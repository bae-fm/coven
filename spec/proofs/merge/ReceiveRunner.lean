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

private def run (j : Json) : Except String Json := do
  let cases ← j.getArr?
  let results ← cases.mapM fun c => do
    if let .ok "publication" := c.getObjValAs? String "check" then
      return ← publication c
    let w ← position (← c.getObjVal? "write")
    let past ← (← c.getObjValAs? (Array Json) "past").mapM position
    let passed ← (← c.getObjValAs? (Array Json) "passed").mapM position
    pure (positionsReady w past.toList passed.toList)
  pure (toJson results)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
