import CovenStorage.Objects
import CovenStorage.CopyPublication
import Lean

/-! JSON adapter for testing provider publication against the create-once model.

An array of creates runs `create`, or for a copy-publication profile
(`"copy": true`) stages each create as its own session and publishes it with
`CopyPublication.publish`. An object `{"ops": [...]}` runs staged sessions and
publications in the given arrival order through `CopyPublication.run`. -/

open Lean CovenStorage

private def creates (requests : Array Json) : Except String Json := do
  let mut storage := emptyStorage
  let mut bucket := CopyPublication.empty
  let mut results := #[]
  let mut session := 0
  for request in requests do
    let number ← request.getObjValAs? Nat "path"
    let bytes ← request.getObjValAs? (List Nat) "bytes"
    let copy := (request.getObjValAs? Bool "copy").toOption.getD false
    let path := Path.slot number
    if copy then
      session := session + 1
      let (after, outcome) :=
        CopyPublication.publish (CopyPublication.stage bucket session bytes) session path 0
      bucket := after
      match bucket.objects path with
      | none => throw "copy publication did not leave an object"
      | some object =>
        results := results.push <| Json.mkObj [
          ("occupied", toJson (outcome == .refused)), ("bytes", toJson object.bytes),
          ("staged", toJson (bucket.staging session).isSome)]
    else
      let occupied := (storage path).isSome
      storage := create storage path bytes 0
      match storage path with
      | none => throw "create did not leave an object"
      | some object =>
        results := results.push <| Json.mkObj [
          ("occupied", toJson occupied), ("bytes", toJson object.bytes)]
  pure <| Json.arr results

private def outcomeName : CopyPublication.Outcome → String
  | .copied => "copied"
  | .refused => "refused"
  | .missing => "missing"

private def ops (list : Array Json) : Except String Json := do
  let parsed ← list.toList.mapM fun op => do
    let session ← op.getObjValAs? Nat "session"
    match ← op.getObjValAs? String "op" with
    | "stage" => pure (CopyPublication.Op.stage session (← op.getObjValAs? (List Nat) "bytes"))
    | "publish" =>
        pure (CopyPublication.Op.publish session (.slot (← op.getObjValAs? Nat "path")) 0)
    | other => throw s!"unknown copy operation {other}"
  let sessions := parsed.filterMap fun
    | .stage s _ => some s
    | .publish _ _ _ => none
  let (bucket, outcomes) := CopyPublication.run CopyPublication.empty parsed
  let paths := (parsed.filterMap fun
    | .publish _ (.slot n) _ => some n
    | _ => none).eraseDups
  pure <| Json.mkObj [
    ("outcomes", toJson (outcomes.map outcomeName)),
    ("objects", Json.arr (paths.map fun n =>
      Json.mkObj [("path", toJson n),
        ("bytes", toJson ((bucket.objects (.slot n)).map (·.bytes)))]).toArray),
    ("staged", toJson (sessions.filter fun s => (bucket.staging s).isSome).length)]

private def run (j : Json) : Except String Json :=
  match j with
  | .arr requests => creates requests
  | _ => do ops (← j.getObjValAs? (Array Json) "ops")

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
