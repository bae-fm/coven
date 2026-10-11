import CovenMerge.ReceivePositions
import Lean

open Lean CovenMerge.Receive

private def position (j : Json) : Except String CovenMerge.Receive.Position := do
  let pair ← j.getArr?
  if pair.size != 2 then throw "a position must have device and number"
  let device ← pair[0]!.getNat?
  let number ← pair[1]!.getNat?
  if number = 0 then throw "positions start at one"
  pure ⟨device, number⟩

private def run (j : Json) : Except String Json := do
  let cases ← j.getArr?
  let results ← cases.mapM fun c => do
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
