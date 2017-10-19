#if INTERACTIVE
#r "../packages/Suave/lib/net40/Suave.dll"
#r "../packages/DynamicInterop/lib/netstandard1.2/DynamicInterop.dll"
#r "../packages/R.NET/lib/net40/RDotNet.dll"
#r "../packages/R.NET.FSharp/lib/net40/RDotNet.FSharp.dll"
#r "../packages/FSharp.Data/lib/net40/FSharp.Data.dll"
#else
module Wrattler.RService
#endif
open Suave
open Suave.Filters
open Suave.Writers
open Suave.Operators
open Suave.Logging

open System
open RDotNet
open FSharp.Data

let dataStoreUrl = "http://localhost:7102"
let logger = Targets.create Verbose [||]
let logf fmt = Printf.kprintf (fun s -> logger.info(Message.eventX s)) fmt

// ------------------------------------------------------------------------------------------------
// Thread-safe R engine access
// ------------------------------------------------------------------------------------------------

let queue = new System.Collections.Concurrent.BlockingCollection<_>()
let worker = System.Threading.Thread(fun () -> 
  let rpath = __SOURCE_DIRECTORY__ + "/../rinstall/R-3.4.1" |> IO.Path.GetFullPath
  let path = System.Environment.GetEnvironmentVariable("PATH")
  System.Environment.SetEnvironmentVariable("PATH", sprintf "%s;%s/bin/x64" path rpath)
  System.Environment.SetEnvironmentVariable("R_HOME", rpath)
  let rengine = REngine.GetInstance(rpath + "/bin/x64/R.dll", AutoPrint=false)
  rengine.Evaluate("dataStore <- list()") |> ignore
  while true do 
    let op = queue.Take() 
    op rengine )
worker.Start()

let withREngine op = 
  Async.FromContinuations(fun (cont, econt, _) ->
    queue.Add(fun reng -> 
       let res = try Choice1Of2(op reng) with e -> Choice2Of2(e)
       match res with Choice1Of2 res -> cont res | Choice2Of2 e -> econt e    
    )
  )

// ------------------------------------------------------------------------------------------------
// Calling data store and caching data
// ------------------------------------------------------------------------------------------------

let storeDataset hash file json = 
  Http.AsyncRequestString
    ( sprintf "%s/%s/%s" dataStoreUrl hash file, 
      httpMethod="PUT", body = HttpRequestBody.TextRequest json)
  |> Async.Ignore

type RetrievedFrame = 
  | Cached of string
  | Downloaded of JsonValue

let retrieveFrames frames = async {
  let! known = withREngine(fun rengine ->
    set (rengine.Evaluate("ls(dataStore)").AsCharacter()))
  let res = ResizeArray<_>()
  for var, url in frames do
    if known.Contains url then res.Add(var, Cached(sprintf "dataStore$`%s`" url)) else
    let! json = Http.AsyncRequestString(url, httpMethod="GET")
    res.Add(var, Downloaded(JsonValue.Parse(json))) 
  return res :> seq<_> }

// ------------------------------------------------------------------------------------------------
// Calling data store
// ------------------------------------------------------------------------------------------------

let getConvertor (rengine:REngine) = function 
  | JsonValue.Boolean _ -> fun data -> 
      data |> Seq.map (function JsonValue.Boolean b -> b | _ -> failwith "Expected boolean") |> rengine.CreateLogicalVector :> SymbolicExpression
  | JsonValue.Number _ -> fun data -> 
      data |> Seq.map (function JsonValue.Number n -> float n | JsonValue.Float n -> n | _ -> failwith "Expected number") |> rengine.CreateNumericVector :> _ 
  | JsonValue.Float _ -> fun data -> 
      data |> Seq.map (function JsonValue.Number n -> float n | JsonValue.Float n -> n | _ -> failwith "Expected number") |> rengine.CreateNumericVector :> _ 
  | JsonValue.String _ -> fun data -> 
      data |> Seq.map (function JsonValue.String s -> s | _ -> failwith "Expected tring") |> rengine.CreateCharacterVector :> _ 
  | _ -> failwith "Unexpected json value"

let evaluate hash frames code (rengine:REngine) = 

  let mainEnv = rengine.Evaluate("new.env()").AsEnvironment()
  let tmpEnv = rengine.Evaluate("new.env()").AsEnvironment()
  rengine.SetSymbol("tmpEnv", tmpEnv)
  rengine.SetSymbol("mainEnv", mainEnv)

  let knownFrames = set [ for name, _ in frames -> name ]
  for name, data in frames do
    let df = 
      match data with 
      | Cached expr ->
          rengine.Evaluate(expr).AsDataFrame()
      | Downloaded data ->
          let data = data.AsArray()
          let props = 
            match Array.head data with 
            | JsonValue.Record props -> props |> Array.map (fun (k, v) -> 
                k, getConvertor rengine v )
            | _ -> failwith "Expected record"
          let cols = props |> Seq.map (fun (p, _) -> p, ResizeArray<_>()) |> dict
          for row in data do
            match row with 
            | JsonValue.Record recd -> for k, v in recd do cols.[k].Add(v)
            | _ -> failwith "Expected record"
          props |> Seq.iteri (fun i (n, conv) ->
            rengine.SetSymbol("tmp" + string i, conv cols.[n], tmpEnv))  
          let assigns = props |> Seq.mapi (fun i (n, _) -> sprintf "'%s'=tmpEnv$tmp%d" n i)
          let df = rengine.Evaluate(sprintf "data.frame(%s)" (String.concat "," assigns)).AsDataFrame()
          df.SetAttribute("names", rengine.CreateCharacterVector (Array.map fst props))
          df
    rengine.SetSymbol(name, df, mainEnv)

  let code = sprintf "with(mainEnv, { %s })" code
  rengine.Evaluate(code) |> ignore
  [ for var in rengine.Evaluate("ls(mainEnv)").AsCharacter() do
      if not (knownFrames.Contains(var)) then
        match rengine.Evaluate("as.data.frame(mainEnv$" + var + ")") with
        | ActivePatterns.DataFrame df -> 
            rengine.Evaluate(sprintf "dataStore$`%s/%s/%s` <- mainEnv$%s" dataStoreUrl hash var var) |> ignore
            yield var, df
        | _ -> () ] 

let formatValue (value:obj) = 
  match value with 
  | :? int as n -> JsonValue.Number(decimal n)
  | :? float as f -> JsonValue.Float(f)
  | :? string as s -> JsonValue.String(s)
  | _ -> failwithf "Unsupported value: %A" value

let formatType typ =
  if typ = typeof<System.Double> then "float"
  elif typ = typeof<System.Int32> then "int"
  elif typ = typeof<System.String> then "string"
  else failwithf "Unsupported type: %s" (typ.FullName)

let evaluateAndParse hash frames code rengine = 
  let results = evaluate hash frames code rengine
  [ for var, df in results ->
      let data = 
        [| for row in df.GetRows() ->
             df.ColumnNames |> Array.map (fun c -> c, formatValue row.[c]) |> JsonValue.Record |] 
        |> JsonValue.Array
      let row = df.GetRows() |> Seq.tryHead
      let tys = 
        match row with
        | Some row -> df.ColumnNames |> Array.map (fun c -> c, formatType(row.[c].GetType()))
        | None -> df.ColumnNames |> Array.map (fun c -> c, "object")
      var, tys, data.ToString() ]

let evaluateAndStore hash frames code = async {
  let! results = withREngine (evaluateAndParse hash frames code)
  for var, _, data in results do do! storeDataset hash var data 
  return [ for var, typ, _ in results -> var, typ ] }

// --------------------------------------------------------------------------------------
// Server that exposes the R functionality
// --------------------------------------------------------------------------------------

type ExportsJson = JsonProvider<"""[
  { "variable":"foo",
    "type":{"kind":"frame", "columns":[["foo", "string"],["bar", "int"]]}} ]""">

type EvalJson = JsonProvider<"""[
  { "name":"foo",
    "url":"http://datastore/123" } ]""">

type RequestJson = JsonProvider<"""{ 
  "code":"a <- b", "hash":"0x123456",
  "frames": [{"name":"b", "url":"http://datastore/123"}, {"name":"c", "url":"http://datastore/123"}] }""">

let app =
  setHeader  "Access-Control-Allow-Origin" "*"
  >=> setHeader "Access-Control-Allow-Headers" "content-type"
  >=> choose [
    OPTIONS >=> 
      Successful.OK "CORS approved"

    GET >=> path "/" >=>  
      Successful.OK "Service is running..."

    POST >=> path "/eval" >=> fun ctx -> async {
      let req = RequestJson.Parse(System.Text.UTF32Encoding.UTF8.GetString(ctx.request.rawForm))
      logf "Evaluating code (%s) with frames: %A" req.Hash [ for f in req.Frames -> f.Name]
      let! frames = retrieveFrames [ for f in req.Frames -> f.Name, f.Url ]
      let! rdata = evaluateAndStore req.Hash frames req.Code
      let res = 
        [| for var, _ in rdata -> EvalJson.Root(var, sprintf "%s/%s/%s" dataStoreUrl req.Hash var).JsonValue |]
        |> JsonValue.Array
      logf "Evaluated code (%s): %A" req.Hash [ for var, _ in rdata -> sprintf "%s/%s" req.Hash var ]
      return! Successful.OK (res.ToString()) ctx }

    POST >=> path "/exports" >=> fun ctx -> async {
      let req = RequestJson.Parse(System.Text.UTF32Encoding.UTF8.GetString(ctx.request.rawForm))
      logf "Getting exports (%s) with frames: %A" req.Hash [ for f in req.Frames -> f.Name]
      let! frames = retrieveFrames [ for f in req.Frames -> f.Name, f.Url ]
      let! rdata = evaluateAndStore req.Hash frames req.Code
      let res = 
        [| for var, typ in rdata -> 
            let cols = [| for k, v in typ -> [|k; v|] |]
            ExportsJson.Root(var, ExportsJson.Type("frame", cols)).JsonValue |]
        |> JsonValue.Array
      logf "Got exports (%s): %A" req.Hash [ for var, _ in rdata -> var ] 
      return! Successful.OK (res.ToString()) ctx } ]

// --------------------------------------------------------------------------------------
// Startup code for Azure hosting
// --------------------------------------------------------------------------------------

// When port was specified, we start the app (in Azure), 
// otherwise we do nothing (it is hosted by 'build.fsx')
match System.Environment.GetCommandLineArgs() |> Seq.tryPick (fun s ->
    if s.StartsWith("port=") then Some(int(s.Substring("port=".Length)))
    else None ) with
| Some port ->
    let serverConfig =
      { Web.defaultConfig with
          bindings = [ HttpBinding.createSimple HTTP "127.0.0.1" port ] }
    Web.startWebServer serverConfig app
| _ -> ()