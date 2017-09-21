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
open System
open Suave.Filters
open Suave.Writers
open Suave.Operators
open FSharp.Data
open RDotNet

// ------------------------------------------------------------------------------------------------
// Thread-safe R engine access
// ------------------------------------------------------------------------------------------------

let queue = new System.Collections.Concurrent.BlockingCollection<_>()
let worker = System.Threading.Thread(fun () -> 
  let rengine = REngine.GetInstance(AutoPrint=false)
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

let evaluate code (rengine:REngine) = 
  let code = sprintf "e <- new.env(); with(e, { %s })" code
  rengine.Evaluate(code) |> ignore
  [ for var in rengine.Evaluate("ls(e)").AsCharacter() do
      match rengine.Evaluate("as.data.frame(e$" + var + ")") with
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

let getExports code rengine = 
  [ for var, df in evaluate code rengine ->      
      let row = df.GetRows() |> Seq.tryHead
      let tys = 
        match row with
        | Some row -> df.ColumnNames |> Array.map (fun c -> c, formatType(row.[c].GetType()))
        | None -> df.ColumnNames |> Array.map (fun c -> c, "object")
      var, List.ofArray tys ]

let getResults code rengine =
  [ for var, df in evaluate code rengine ->      
      var, 
      [| for row in df.GetRows() ->
           df.ColumnNames |> Array.map (fun c -> c, row.[c]) |] ]

// --------------------------------------------------------------------------------------
// Server that exposes the R functionality
// --------------------------------------------------------------------------------------

type ExportsJson = JsonProvider<"""[
  { "variable":"foo",
    "type":{"kind":"frame", "columns":[["foo", "string"],["bar", "int"]]}} ]""">
type EvalJson = JsonProvider<"""[
  { "variable":"foo",
    "data":[] } ]""">
    
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
        let code = System.Text.UTF32Encoding.UTF8.GetString(req.rawForm)
        let! res = withREngine (getResults code)
        let res = 
          [| for var, data in res -> 
              let data = Array.map (Array.map (fun (k, v) -> k, formatValue v) >> JsonValue.Record) data
              EvalJson.Root(var, data).JsonValue |]
          |> JsonValue.Array
        return! Successful.OK (res.ToString()) ctx })

    POST >=> path "/exports" >=> 
      request (fun req ctx -> async {
        let code = System.Text.UTF32Encoding.UTF8.GetString(req.rawForm)
        let! exp = withREngine (getExports code)
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