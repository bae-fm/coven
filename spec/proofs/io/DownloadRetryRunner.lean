import CovenIO.Downloads
import Lean

open Lean CovenIO

private def retry (j : Json) : Except String Json := do
  let clears ← match ← j.getObjValAs? String "clears" with
    | "byItself" => pure CovenStorelogData.Pending.Clears.byItself
    | "byPerson" => pure CovenStorelogData.Pending.Clears.byPerson
    | "never" => pure CovenStorelogData.Pending.Clears.never
    | other => throw s!"unknown failure class {other}"
  let now ← j.getObjValAs? Nat "now"
  let provider ← j.getObjValAs? Nat "provider"
  let changed ← j.getObjValAs? Bool "changed"
  let reason := Downloads.Reason.storage clears
  let timer : Waiting.Timer := ⟨Waiting.delay 0, provider⟩
  return Json.mkObj [
    ("timed", toJson (Downloads.automatic reason)),
    ("retried", toJson (Downloads.canRetry (.unfinished [] reason) timer now changed))]

def main : IO Unit := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let cases ← (← Json.parse input).getArr?
    cases.mapM retry
  match result with
  | .ok output => (← IO.getStdout).putStrLn (toJson output).compress
  | .error error => throw (IO.userError error)
