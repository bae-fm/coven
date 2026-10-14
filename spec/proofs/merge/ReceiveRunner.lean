import CovenMerge.ReceivePositions
import CovenMerge.Retirement
import Lean

open Lean CovenMerge.Receive

private def position (j : Json) : Except String CovenMerge.Receive.Position := do
  let pair ← j.getArr?
  if pair.size != 2 then throw "a position must have device and number"
  let device ← pair[0]!.getNat?
  let number ← pair[1]!.getNat?
  if number = 0 then throw "positions start at one"
  pure ⟨device, number⟩

/-- Execute the premises of the publication-bound theorem for a completed
observation. A phase beginning at finality sees retirement. -/
private def publication (j : Json) : Except String Bool := do
  let finalAt ← j.getObjValAs? Nat "final"
  let start ← j.getObjValAs? Nat "start"
  let sent ← j.getObjValAs? Nat "sent"
  let stored ← j.getObjValAs? Nat "stored"
  let observed := decide (finalAt ≤ start)
  if allowed : observed = false ∧ sent < start + 300 ∧ stored ≤ sent + 86400 then
    have _bound := send_gate_publication_bound finalAt start sent stored observed
      (fun h => by simp [observed, h]) allowed.1 allowed.2.1 allowed.2.2
    if stored < finalAt + 300 + 86400 then return true
    else throw "publication theorem contradicted"
  else return false

private def retirement (j : Json) : Except String Retirement := do
  pure ⟨← j.getObjValAs? Nat "device", ← j.getObjValAs? Nat "entry", ← j.getObjValAs? Nat "stored"⟩

/-- One write's own retirement window: its device, first publication time and
the retiring entries its recorded past read, against the kept retirements. -/
private def policy (j : Json) (retirements : List Retirement) : Except String (Policy Unit) := do
  let device ← j.getObjValAs? Nat "device"
  let stored ← j.getObjValAs? Nat "stored"
  let read ← j.getObjValAs? (Array Nat) "read"
  pure {
    boundary := ⟨fun _ => false, fun _ => false, by simp⟩
    logs := ⟨fun _ => device, fun _ => 1⟩
    reset := none
    readEntry := fun _ e => read.contains e
    schema := fun _ => .eligible
    retirements
    stored := fun _ => stored }

/-- `withinWindow` under the kept retirements, and, when a later kept set is
given with a final anchor, the same verdict under it (`retirement_verdict_final`). -/
private def window (j : Json) : Except String Json := do
  let kept ← (← j.getObjValAs? (Array Json) "kept").toList.mapM retirement
  let verdict := withinWindow (← policy j kept) ()
  match j.getObjVal? "later" with
  | .ok later =>
    let later ← (← later.getArr?).toList.mapM retirement
    pure (toJson [verdict, withinWindow (← policy j later) ()])
  | .error _ => pure (toJson verdict)

private def run (j : Json) : Except String Json := do
  let cases ← j.getArr?
  let results ← cases.mapM fun c => do
    if let .ok "publication" := c.getObjValAs? String "check" then
      return toJson (← publication c)
    if let .ok "window" := c.getObjValAs? String "check" then
      return ← window c
    let w ← position (← c.getObjVal? "write")
    let past ← (← c.getObjValAs? (Array Json) "past").mapM position
    let passed ← (← c.getObjValAs? (Array Json) "passed").mapM position
    pure (toJson (positionsReady w past.toList passed.toList))
  pure (toJson results)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
