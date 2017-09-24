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

open System
open RDotNet
open FSharp.Data

// ------------------------------------------------------------------------------------------------
// Thread-safe R engine access
// ------------------------------------------------------------------------------------------------

let queue = new System.Collections.Concurrent.BlockingCollection<_>()
let worker = System.Threading.Thread(fun () -> 
  let rpath = __SOURCE_DIRECTORY__ + "/../rinstall/R-3.4.1" |> IO.Path.GetFullPath
  let path = System.Environment.GetEnvironmentVariable("PATH")
  System.Environment.SetEnvironmentVariable("PATH", sprintf "%s;%s/bin/x64" path rpath)
  System.Environment.SetEnvironmentVariable("R_HOME", rpath)
  let rengine = REngine.GetInstance(rpath + "/bin/x64\R.dll", AutoPrint=false)
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

let evaluate frames code (rengine:REngine) = 

  let mainEnv = rengine.Evaluate("new.env()").AsEnvironment()
  let tmpEnv = rengine.Evaluate("new.env()").AsEnvironment()
  rengine.SetSymbol("tmpEnv", tmpEnv)
  rengine.SetSymbol("mainEnv", mainEnv)

  for name, data in frames do
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
    rengine.SetSymbol(name, df, mainEnv)

  let code = sprintf "with(mainEnv, { %s })" code
  rengine.Evaluate(code) |> ignore
  [ for var in rengine.Evaluate("ls(mainEnv)").AsCharacter() do
      match rengine.Evaluate("as.data.frame(mainEnv$" + var + ")") with
      | ActivePatterns.DataFrame df -> yield var, df
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

let getExports frames code rengine = 
  [ for var, df in evaluate frames code rengine ->      
      let row = df.GetRows() |> Seq.tryHead
      let tys = 
        match row with
        | Some row -> df.ColumnNames |> Array.map (fun c -> c, formatType(row.[c].GetType()))
        | None -> df.ColumnNames |> Array.map (fun c -> c, "object")
      var, List.ofArray tys ]

let getResults frames code rengine =
  [ for var, df in evaluate frames code rengine ->      
      var, 
      [| for row in df.GetRows() ->
           df.ColumnNames |> Array.map (fun c -> c, row.[c]) |] ]

(*
Async.RunSynchronously <| withREngine (fun rengine ->
  let q = rengine.Evaluate("q <- quote({ print('hi')\ndata <- iris\nagg <- aggregate(Petal.Width~Species, data, mean)\ncolnames(agg)[2] <- \"PetalWidth\" })")
  let count = rengine.Evaluate("length(q)").AsNumeric().[0] |> int
  for i in 2 .. count do
    let call = rengine.Evaluate(sprintf "as.list(q[%d][[1]])" i).AsCharacter()
    if call.Length > 0 && call.[0] = "`<-`" then
      if rengine.Evaluate(sprintf "!is.call(as.list(q[%d][[1]])[[2]])" i).AsCharacter().[0] = "TRUE" then
        rengine.Evaluate(sprintf "as.character(as.list(q[%d][[1]])[[2]])" i).AsCharacter().[0]
        |> printfn "%s"  )
*)

//Async.RunSynchronously <| withREngine (getExports "data <- iris\nagg <- aggregate(Petal.Width~Species, data, mean)")
//Async.RunSynchronously <| withREngine (getResults "data <- iris\nagg <- aggregate(Petal.Width~Species, data, mean)")

// --------------------------------------------------------------------------------------
// Server that exposes the R functionality
// --------------------------------------------------------------------------------------

type ExportsJson = JsonProvider<"""[
  { "variable":"foo",
    "type":{"kind":"frame", "columns":[["foo", "string"],["bar", "int"]]}} ]""">

type EvalJson = JsonProvider<"""[
  { "name":"foo",
    "data":[] } ]""">

type RequestJson = JsonProvider<"""{ 
  "code":"a <- b",
  "frames": [{"name":"b", "data":[]}, {"name":"c", "data":[]}] }""">

let app =
  setHeader  "Access-Control-Allow-Origin" "*"
  >=> setHeader "Access-Control-Allow-Headers" "content-type"
  >=> choose [
    OPTIONS >=> 
      Successful.OK "CORS approved"

    GET >=> path "/" >=>  
      Successful.OK "Service is running..."

    POST >=> path "/eval" >=>       
      request (fun req ctx -> async {
        let req = RequestJson.Parse(System.Text.UTF32Encoding.UTF8.GetString(req.rawForm))
        let! res = withREngine (getResults [| for f in req.Frames -> f.Name, [| for d in f.Data -> d.JsonValue |] |] req.Code)
        let res = 
          [| for var, data in res -> 
              let data = Array.map (Array.map (fun (k, v) -> k, formatValue v) >> JsonValue.Record) data
              EvalJson.Root(var, data).JsonValue |]
          |> JsonValue.Array
        return! Successful.OK (res.ToString()) ctx })

    POST >=> path "/exports" >=> 
      request (fun req ctx -> async {
        let req = RequestJson.Parse(System.Text.UTF32Encoding.UTF8.GetString(req.rawForm))
        let! exp = withREngine (getExports [| for f in req.Frames -> f.Name, [| for d in f.Data -> d.JsonValue |] |] req.Code)
        let res = 
          [| for var, cols in exp -> 
              let cols = [| for k, v in cols -> [|k; v|] |]
              ExportsJson.Root(var, ExportsJson.Type("frame", cols)).JsonValue |]
          |> JsonValue.Array
        return! Successful.OK (res.ToString()) ctx }) ]

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