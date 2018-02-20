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

//let dataStoreUrl = "http://localhost:7102"
let dataStoreUrl = "http://wrattler-data-store.azurewebsites.net"
let logger = Targets.create Verbose [||]
let logf fmt = Printf.kprintf (fun s -> logger.info(Message.eventX s)) fmt

// ------------------------------------------------------------------------------------------------
// Thread-safe R engine access
// ------------------------------------------------------------------------------------------------

type RContext = 
  { REngine : REngine
    KnownNames : Set<string> }

let queue = new System.Collections.Concurrent.BlockingCollection<_>()
let worker = System.Threading.Thread(fun () -> 
  let rpath = __SOURCE_DIRECTORY__ + "/../rinstall/R-3.4.1" |> IO.Path.GetFullPath
  let pkgpath = __SOURCE_DIRECTORY__ + "/../rinstall/libraries" |> IO.Path.GetFullPath
  //let rpath = @"C:\Programs\Academic\R\R-3.4.2"
  let path = System.Environment.GetEnvironmentVariable("PATH")
  System.Environment.SetEnvironmentVariable("PATH", sprintf "%s;%s/bin/x64" path rpath)
  System.Environment.SetEnvironmentVariable("R_HOME", rpath)
  let rengine = REngine.GetInstance(rpath + "/bin/x64/R.dll", AutoPrint=false)
  //rengine.Evaluate(".libPaths( c( .libPaths(), \"C:\\\\Users\\\\tomas\\\\Documents\\\\R\\\\win-library\\\\3.4\") )") |> ignore
  rengine.Evaluate(sprintf ".libPaths( c( .libPaths(), \"%s\") )" (pkgpath.Replace("\\","\\\\"))) |> ignore

  rengine.Evaluate("""
    showTree <- function(e, write = cat) {
      w <- makeCodeWalker(call = showTreeCall, leaf = showTreeLeaf,
                          write = write, res = new.env())
      walkCode(e, w)
      w$res
    }
    showTreeCall <- function(e, w0) {
      for (a in as.list(e)) {
        w <- makeCodeWalker(call = w0$call, leaf = w0$leaf, write = w0$write, res = new.env())
        walkCode(a, w)
        assign(toString(length(w0$res)+1), w$res, envir=w0$res)
      }
    }
    showTreeLeaf <- function(e, w) {
      if (typeof(e) == "symbol") w$res$it <- e
      else w$res$it <- deparse(e)
    }  
  """) |> ignore
  rengine.Evaluate("dataStore <- list()") |> ignore
  try 
    rengine.Evaluate("library(tidyverse)") |> ignore
    rengine.Evaluate("library(codetools)") |> ignore
  with e -> printfn "Packages failed: %A" e

  let knownNames = 
    rengine.Evaluate("search()").AsCharacter() 
    |> Seq.collect (fun pkg -> rengine.Evaluate(sprintf "ls(\"%s\")" pkg).AsCharacter()) 
    |> set
  let ctx = { REngine = rengine; KnownNames = knownNames }
  while true do 
    let op = queue.Take() 
    try op ctx
    with e -> printf "Operation failed: %A" e)
worker.Start()

let withRContext op = 
  Async.FromContinuations(fun (cont, econt, _) ->
    queue.Add(fun reng -> 
       let res = try Choice1Of2(op reng) with e -> Choice2Of2(e)
       match res with Choice1Of2 res -> cont res | Choice2Of2 e -> econt e    
    )
  )

let withREngine op = withRContext (fun ctx -> op ctx.REngine)

#load "pivot.fs"
open TheGamma.Services.Pivot

let getConvertor (rengine:REngine) typ =
  let stringConvertor data =
    data |> Array.map (function 
      | JsonValue.String s -> s 
      | v -> v.ToString() ) |> rengine.CreateCharacterVector :> SymbolicExpression
  let withFallback f data : SymbolicExpression =
    try f data with _ -> stringConvertor data 
  match typ with 
  | InferredType.OneZero _ 
  | InferredType.Bool _ -> withFallback (fun data -> 
      data |> Array.map (function 
        | JsonValue.Boolean b -> b 
        | JsonValue.Number 1.0M -> true
        | JsonValue.Number 0.0M -> false
        | v -> failwithf "Expected boolean, but got: %A" v) |> rengine.CreateLogicalVector :> _)
  | InferredType.Int _ 
  | InferredType.Float _ -> withFallback (fun data -> 
      let line = data |> Array.map (function 
        | JsonValue.Number n -> string (float n)
        | JsonValue.Float n -> string n 
        | JsonValue.String "" -> "NA"
        | JsonValue.String s -> string (float s)
        | v -> failwithf "Expected number, but got: %A" v) |> String.concat "," |> sprintf "c(%s)" 
      rengine.Evaluate(line).AsNumeric() :> _)
  | InferredType.Date _
  | InferredType.Any
  | InferredType.Empty
  | InferredType.String _ -> stringConvertor 

let createRDataFrame (rengine:REngine) data = 
  let tmpEnv = rengine.Evaluate("new.env()").AsEnvironment()
  rengine.SetSymbol("tmpEnv", tmpEnv)

  let propNames = (Seq.head data:JsonValue).Properties() |> Seq.map fst
  let propConvs = 
    data
    |> Seq.truncate 500
    |> Seq.map (fun (r:JsonValue) -> 
        r.Properties() |> Array.map (function
          | _, JsonValue.Boolean b -> b.ToString()
          | _, JsonValue.Number n -> n.ToString()
          | _, JsonValue.Float f -> f.ToString()
          | _, JsonValue.String s -> s.ToString()
          | _ -> failwith "createRDataFrame: Unexpected value" ) )
    |> Seq.map (Array.map Inference.inferType)
    |> Seq.reduce (Array.map2 Inference.unifyTypes)
    |> Seq.map (getConvertor rengine)

  let props = Seq.zip propNames propConvs
  let cols = props |> Seq.map (fun (p, _) -> p, ResizeArray<_>()) |> dict
  for row in data do
    match row with 
    | JsonValue.Record recd -> for k, v in recd do cols.[k].Add(v)
    | _ -> failwith "Expected record"
  props |> Seq.iteri (fun i (n, conv) ->
    rengine.SetSymbol("tmp" + string i, conv (cols.[n].ToArray()), tmpEnv))  
  let assigns = props |> Seq.mapi (fun i (n, _) -> sprintf "'%s'=tmpEnv$tmp%d" n i)
  rengine.Evaluate(sprintf "tmpEnv$df <- data.frame(%s)" (String.concat "," assigns)) |> ignore
  rengine.Evaluate(sprintf "colnames(tmpEnv$df) <- c(%s)" (String.concat "," [ for n, _ in props -> "\"" + n + "\"" ])) |> ignore
  "tmpEnv$df"

// ------------------------------------------------------------------------------------------------
// Calling data store and caching data
// ------------------------------------------------------------------------------------------------

let storeDataset hash file json = 
  logf "Uploading data set: %s/%s" hash file
  Http.AsyncRequestString
    ( sprintf "%s/%s/%s" dataStoreUrl hash file, 
      httpMethod="PUT", body = HttpRequestBody.TextRequest json)
  |> Async.Ignore

let retrieveFrames frames = async {
  let! known = withREngine(fun rengine ->
    set (rengine.Evaluate("ls(dataStore)").AsCharacter()))
  let res = ResizeArray<_>()
  for var, url in frames do
    if known.Contains url then res.Add(var, sprintf "dataStore$`%s`" url) else
    logf "Downloading data set: %s" url
    let! json = Http.AsyncRequestString(url, httpMethod="GET")

    let data = JsonValue.Parse(json).AsArray()
    do! withREngine (fun rengine ->
      let expr = createRDataFrame rengine data
      rengine.Evaluate(sprintf "dataStore$`%s` <- %s" url expr) |> ignore )
    res.Add(var, sprintf "dataStore$`%s`" url) 
  return res :> seq<_> }

// ------------------------------------------------------------------------------------------------
// Calling data store
// ------------------------------------------------------------------------------------------------

let evaluate hash frames code (rengine:REngine) = 

  let mainEnv = rengine.Evaluate("new.env()").AsEnvironment()
  let tmpEnv = rengine.Evaluate("new.env()").AsEnvironment()
  rengine.SetSymbol("tmpEnv", tmpEnv)
  rengine.SetSymbol("mainEnv", mainEnv)

  let knownFrames = set [ for name, _ in frames -> name ]
  for name, expr in frames do
    let df = rengine.Evaluate(expr:string).AsDataFrame()
    rengine.SetSymbol(name, df)

//  rengine.Evaluate(code)
  //rengine.Evaluate("1+2")
  let code = sprintf "with(mainEnv, { %s })" code
  rengine.Evaluate(code) |> ignore
  [ for var in rengine.Evaluate("ls(mainEnv)").AsCharacter() ->
      if not (knownFrames.Contains(var)) then
        try
          match rengine.Evaluate("as.data.frame(mainEnv$" + var + ")") with
          | ActivePatterns.DataFrame df -> 
              rengine.Evaluate(sprintf "dataStore$`%s/%s/%s` <- mainEnv$%s" dataStoreUrl hash var var) |> ignore
              [ var, df ]
          | _ -> []
        with _ -> [] (* not a data frame *) 
      else [] ] |> List.concat

let formatValue (value:obj) = 
  match value with 
  | :? int as n -> JsonValue.Number(decimal n)
  | :? float as f when Double.IsNaN f -> JsonValue.String("")
  | :? float as f -> JsonValue.Float(f)
  | :? string as s -> JsonValue.String(s)
  | _ -> failwithf "Unsupported value: %A" value

let formatType typ =
  if typ = typeof<System.Double> then "float"
  elif typ = typeof<System.Int32> then "int"
  elif typ = typeof<System.String> then "string"
  else failwithf "Unsupported type: %s" (typ.FullName)

(*
let code = """
training <- 
  bb2014 %>%
  mutate(`Urban/rural` = ifelse(`Urban/rural`=="Urban", 1, 0)) 
test <- bb2015fix %>%
  mutate(`Urban/rural` = ifelse(`Urban/rural`=="Urban", 1, 0))

# glm(`Urban/rural`~., family=binomial(link='logit'), data=training)
"""
*)
let evaluateAndParse hash frames code rengine = 
  try
    logf "Evaluating: %s" code
    let results = evaluate hash frames code rengine
    logf "Success. Results (%s): %A" (String.concat "," (List.map fst results)) results
    [ for var, df in results ->
        logf "Reading data for frame: %s" var
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
  with e ->
    logf "Failed to evluate! %A" e
    reraise ()

let evaluateAndStore hash frames code = async {
  let! results = withREngine (evaluateAndParse hash frames code)
  logf "Storing resulting data frames"
  for var, _, data in results do do! storeDataset hash var data 
  logf "Resulting data frames uploaded"
  return [ for var, typ, _ in results -> var, typ ] }

let source = """
colnames(bb2014) <- c("Urban","Down","Up","Latency","Web")
colnames(bb2015fix) <- c("Urban","Down","Up","Latency","Web")

training <- 
  bb2014 %>% mutate(Urban = ifelse(Urban=="Urban", 1, 0))

test <- 
  bb2015fix %>% mutate(Urban = ifelse(Urban=="Urban", 1, 0)) 

model <- glm(Urban ~.,family=binomial(link='logit'),data=training)
pred <- predict(model, test, type="response") %>% round
pred[is.na(pred)] <- 0.5

predicted <- data.frame(Urban=pred, ActualUrban=test$Urban)"""

type RLeaf = 
  | Symbol of string
  | Value of obj list
  | Expr of SymbolicExpression

type RTree = 
  | Leaf of RLeaf
  | Node of RTree list

let rec parseRTree (tree:SymbolicExpression) = 
  if tree.Type = Internals.SymbolicExpressionType.Environment then
    let env = tree.AsEnvironment()
    let symbols = env.GetSymbolNames() |> List.ofSeq
    if symbols = ["it"] then
      let it = env.GetSymbol("it")
      match it.Type with
      | Internals.SymbolicExpressionType.Symbol -> Leaf(Symbol(it.AsSymbol().PrintName))
      | Internals.SymbolicExpressionType.CharacterVector -> Leaf(Value [ for c in it.AsCharacter() -> box c])
      | _ -> 
          printfn "******* %A" it.Type
          Leaf(Expr(it))
    else
      let count = symbols |> Seq.map int |> Seq.max
      Node [ for i in 1 .. count -> parseRTree (env.GetSymbol(string i))  ]
  else
    failwithf "Expected environment but got: %A" tree.Type

let collect f tree = 
  let rec loop tree = seq {
    yield! f tree 
    match tree with 
    | Node children -> for c in children do yield! loop c
    | _ -> () }
  loop tree |> List.ofSeq
  
let getBindings availableFrames source = withRContext (fun { REngine = rengine; KnownNames = knownNames } ->
  let availableFrames = set availableFrames
  let tree = rengine.Evaluate("showTree(quote({ " + source + " }))") |> parseRTree
  let exports = tree |> collect (function
    | Node [ Leaf(Symbol("<-")); Leaf(Symbol sym); _ ] -> [sym]
    | _ -> []) 
  let imports = tree |> collect (function
    | Leaf(Symbol name) when availableFrames.Contains(name)   
        // && not(knownNames.Contains(name))  -- running code adds names to this, which breaks things
          -> [name]
    | _ -> [])    
  List.distinct imports, List.distinct exports )

// --------------------------------------------------------------------------------------
// Server that exposes the R functionality
// --------------------------------------------------------------------------------------

type ExportsResponseJson = JsonProvider<"""
  { "exports": ["foo","bar"],
    "imports": ["foo","bar"] }""">

type EvalResponseJson = JsonProvider<"""[
  { "name":"foo",
    "url":"http://datastore/123" } ]""">

type EvalRequestJson = JsonProvider<"""{ 
  "code":"a <- b", "hash":"0x123456",
  "frames": [{"name":"b", "url":"http://datastore/123"}, {"name":"c", "url":"http://datastore/123"}] }""">

type ExportsRequestJson = JsonProvider<"""{ 
  "code":"a <- b", "hash":"0x123456",
  "frames": ["foo","bar"] }""">

let app =
  setHeader  "Access-Control-Allow-Origin" "*"
  >=> setHeader "Access-Control-Allow-Headers" "content-type"
  >=> choose [
    OPTIONS >=> 
      Successful.OK "CORS approved"

    GET >=> path "/" >=>  
      Successful.OK "Service is running..."

    POST >=> path "/eval" >=> fun ctx -> async {
      let req = EvalRequestJson.Parse(System.Text.UTF32Encoding.UTF8.GetString(ctx.request.rawForm))
      logf "Evaluating code (%s) with frames: %A" req.Hash [ for f in req.Frames -> f.Name]
      let! frames = retrieveFrames [ for f in req.Frames -> f.Name, f.Url ]
      let! rdata = evaluateAndStore req.Hash frames req.Code
      let res = 
        [| for var, _ in rdata -> EvalResponseJson.Root(var, sprintf "%s/%s/%s" dataStoreUrl req.Hash var).JsonValue |]
        |> JsonValue.Array
      logf "Evaluated code (%s): %A" req.Hash [ for var, _ in rdata -> sprintf "%s/%s" req.Hash var ]
      return! Successful.OK (res.ToString()) ctx }

    POST >=> path "/exports" >=> fun ctx -> async {
      let req = ExportsRequestJson.Parse(System.Text.UTF32Encoding.UTF8.GetString(ctx.request.rawForm))
      logf "Getting exports (%s) with frames: %A" req.Hash (List.ofSeq req.Frames)
      let! imports, exports = getBindings req.Frames req.Code
      logf "Got imports: %A" imports 
      logf "Got exports: %A" exports
      let res = ExportsResponseJson.Root(Array.ofSeq exports, Array.ofSeq imports) 
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