module RInstall 

open System
open System.IO
open System.Text
open System.Diagnostics

let ensureR () = 
  if Microsoft.Win32.Registry.LocalMachine.OpenSubKey(@"SOFTWARE\R-core") <> null then () else
  let installer = "https://cran.r-project.org/bin/windows/base/R-3.4.1-win.exe"
  printfn "Installing R in local machine."
  use wc = new System.Net.WebClient()
  let tmp = Path.GetTempPath()
  let tmpExe = Path.Combine(tmp, Path.ChangeExtension(Path.GetRandomFileName(),".exe"))
  printfn "Downloading R bits..."
  wc.DownloadFile(Uri installer, tmpExe)
  printf "Installing R..."
  let psi = 
    new ProcessStartInfo
      ( tmpExe, "/COMPONENTS=x64,main,translation", 
        UseShellExecute = false, RedirectStandardError = true, RedirectStandardOutput = true, 
        CreateNoWindow = true, StandardErrorEncoding = Encoding.UTF8, StandardOutputEncoding = Encoding.UTF8)
  let proc = Process.Start(psi)
  proc.WaitForExit()
  while (not (proc.StandardOutput.EndOfStream)) do printfn "out: %s" (proc.StandardOutput.ReadLine())
  while (not (proc.StandardError.EndOfStream)) do printfn "err: %s" (proc.StandardOutput.ReadLine())
  if proc.ExitCode <> 0 then invalidOp "Failed to install R in local context"
  printf "R installation complete."
