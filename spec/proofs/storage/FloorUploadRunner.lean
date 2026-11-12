import CovenStorage.FloorUpload
import Lean

open Lean CovenStorage.FloorUpload

private def outcome : Outcome → String
  | .stored => "stored"
  | .conflict => "conflict"

private def run (input : Json) : Except String Json := do
  let initial ← input.getObjValAs? (Option (List Nat)) "initial"
  let mut store : Store := ⟨if initial.isSome then 1 else 0, initial⟩
  let condition := if initial.isSome then Condition.observed store.revision else .absent
  let mut results := #[]
  for op in ← input.getObjValAs? (Array Json) "ops" do
    let bytes ← op.getObjValAs? (List Nat) "bytes"
    let (after, result) := publish store ⟨condition, bytes⟩
    store := after
    results := results.push <|
      Json.mkObj [("result", toJson (outcome result)), ("bytes", toJson store.bytes)]
  pure <| Json.arr results

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
