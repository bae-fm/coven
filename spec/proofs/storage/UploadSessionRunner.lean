import CovenStorage.UploadSession
import Lean

open Lean CovenStorage.UploadSession

private def parseClass : String → Except String Class
  | "automatic" => pure .automatic
  | "appAction" => pure .appAction
  | "never" => pure .never
  | other => throw s!"unknown class {other}"

private def className : Class → String
  | .automatic => "automatic"
  | .appAction => "appAction"
  | .never => "never"

private def parseFault (fault : Json) : Except String Fault := do
  match ← fault.getObjValAs? String "kind" with
  | "none" => pure .none
  | "expire" => pure .expire
  | "fail" => pure (.fail (← parseClass (← fault.getObjValAs? String "class")))
  | "refuse" => pure (.refuse (← parseClass (← fault.getObjValAs? String "class")))
  | "lose_begin" => pure .loseBegin
  | "lose_completion" => pure .loseCompletion
  | "pause" => pure .pause
  | "parts" =>
    let k ← fault.getObjValAs? Nat "parts"
    let pf ← match ← fault.getObjValAs? String "then" with
      | "lost" => pure PartFault.lost
      | "dropped" => pure .dropped
      | "crash" => pure .crash
      | other => throw s!"unknown part fault {other}"
    pure (.afterParts k pf)
  | other => throw s!"unknown fault {other}"

/-- What the Rust test can see after each event: the queue row and its
recorded parts, the provider's live session and its parts, the sessions it
began that the row never named, the published object, the switched file and
the pending failure's class. -/
private def observe (s : State) : Json :=
  Json.mkObj [
    ("queued", toJson s.row.isSome),
    ("recorded", toJson (s.progress.map List.length)),
    ("held", toJson (s.session.map List.length)),
    ("stray", toJson s.stray),
    ("published", toJson s.published.isSome),
    ("stored", toJson s.stored),
    ("failure", toJson ((s.row.bind (·.failure)).map className))]

private def step (s : State) (event : Json) : Except String State := do
  match ← event.getObjValAs? String "op" with
  | "run" =>
    pure (run s (← event.getObjValAs? Bool "retry") (← parseFault (← event.getObjVal? "fault")))
  | "delete" => pure (Step.abandon.apply s)
  | "elapse" => pure (Step.elapse.apply s)
  | "connect" => pure (Step.connect.apply s)
  | "reopen" => pure (Step.crash.apply s)
  | "taken" => pure (Step.rival.apply s)
  | "end_dead" => pure (Step.endDead.apply s)
  | other => throw s!"unknown event {other}"

/-- One case: the file's parts, whether its place is the device's own, and
the events. -/
private def runCase (input : Json) : Except String Json := do
  let total ← input.getObjValAs? Nat "total"
  let owned ← input.getObjValAs? Bool "owned"
  let mut s := initial total ⟨1, 1, List.range total, owned⟩
  let mut seen := #[]
  for event in ← input.getObjValAs? (Array Json) "events" do
    s ← step s event
    seen := seen.push (observe s)
  pure (Json.arr seen)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= fun cases => do
      let cases ← cases.getArr?
      pure (Json.arr (← cases.mapM runCase)) with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
