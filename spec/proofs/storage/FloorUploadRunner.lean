import CovenStorage.FloorUpload
import Lean

open Lean CovenStorage.FloorUpload

private def outcome : Outcome → String
  | .stored => "stored"
  | .incomplete => "incomplete"
  | .conflict => "conflict"

private def run (input : Json) : Except String Json := do
  let initial ← input.getObjValAs? (Option (List Nat)) "initial"
  let mut store : Store := ⟨if initial.isSome then 1 else 0, initial⟩
  let condition := if initial.isSome then Condition.observed store.revision else .absent
  let totals ← input.getObjValAs? (Array Nat) "totals"
  let mut uploads := totals.map fun total => Upload.mk condition total []
  let mut results := #[]
  for op in ← input.getObjValAs? (Array Json) "ops" do
    let index ← op.getObjValAs? Nat "session"
    let some upload := uploads[index]? | throw "unknown session"
    let result ← match ← op.getObjValAs? String "op" with
      | "part" => do
          uploads := uploads.set! index (receive upload (← op.getObjValAs? (List Nat) "bytes"))
          pure "received"
      | "publish" => do
          let (after, result) := publish store upload
          store := after
          pure (outcome result)
      | other => throw s!"unknown operation {other}"
    results := results.push <| Json.mkObj [("result", toJson result), ("bytes", toJson store.bytes)]
  pure <| Json.arr results

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
