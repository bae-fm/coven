import CovenStorage.Objects
import Lean

/-! JSON adapter for testing S3 publication against the existing create-once model. -/

open Lean CovenStorage

private def run (j : Json) : Except String Json := do
  let requests ← fromJson? j (α := Array Json)
  let mut storage := emptyStorage
  let mut results := #[]
  for request in requests do
    let number ← request.getObjValAs? Nat "path"
    let bytes ← request.getObjValAs? (List Nat) "bytes"
    let path := Path.write 31 number
    let occupied := (storage path).isSome
    storage := create storage path bytes 0
    match storage path with
    | none => throw "create did not leave an object"
    | some object =>
      results := results.push <| Json.mkObj [
        ("occupied", toJson occupied), ("bytes", toJson object.bytes)]
  pure <| Json.arr results

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
