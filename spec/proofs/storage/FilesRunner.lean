import CovenStorage.Files
import Lean

/-! JSON adapter comparing the Rust attachment, upload completion and the
rows and queue they leave with `Files.attach` and `Files.finish` (§16.1, §16.5).

Each case is a list of events. `attach` fixes a row's reference and queues it;
`delete` removes a row, an environment step the model leaves to ordinary
writes; `begin` marks when the Rust attempt opens its source and is ignored
here; `finish` settles a queued reference against a storage observation.
The output reports each settlement, whether it changed the write count, and
finally every row's reference and the queued references. -/

open Lean CovenStorage CovenStorage.Files

private def reference (j : Json) : Except String Reference := do
  pure ⟨← j.getObjValAs? Nat "uploader", ← j.getObjValAs? Nat "id", ← j.getObjValAs? Nat "key"⟩

private def referenceJson (r : Reference) : Json :=
  Json.mkObj [("uploader", toJson r.uploader), ("id", toJson r.id), ("key", toJson r.key)]

/-- The checked bytes the attempt would publish; the storage observation names
equal or different bytes relative to them. -/
private def expected : Bytes := [0]

private def remote : String → Except String (Except ReadFailure (Option Object))
  | "stored" => pure (.ok (some ⟨expected, 0⟩))
  | "other" => pure (.ok (some ⟨[1], 0⟩))
  | "absent" => pure (.ok none)
  | "error" => pure (.error .network)
  | other => throw s!"unknown storage observation {other}"

private def settlementJson : Settlement → Json
  | .stored => "stored"
  | .pending _ => "pending"
  | .absent => "absent"
  | .reset => "reset"

private def deleteRow (s : State) (row : Nat) : State :=
  { s with rows := fun r => if r = row then none else s.rows r }

private def runCase (j : Json) : Except String Json := do
  let events ← j.getObjValAs? (Array Json) "events"
  let mut state : State := ⟨fun _ => none, [], [], 0⟩
  let mut rows : List Nat := []
  let mut settlements := #[]
  for event in events do
    match ← event.getObjValAs? String "op" with
    | "attach" =>
      let row ← event.getObjValAs? Nat "row"
      let r ← reference (← event.getObjVal? "ref")
      let file : RowFile := ⟨r, ← event.getObjValAs? Nat "size",
        ← event.getObjValAs? Nat "hash", ← event.getObjValAs? Nat "version"⟩
      state := attach state row ⟨file, []⟩
      unless rows.contains row do rows := rows ++ [row]
    | "delete" => state := deleteRow state (← event.getObjValAs? Nat "row")
    | "begin" => pure ()
    | "finish" =>
      let r ← reference (← event.getObjVal? "ref")
      let (next, settlement) := finish state r expected (← remote (← event.getObjValAs? String "remote"))
      settlements := settlements.push <| Json.mkObj [
        ("settlement", settlementJson settlement),
        ("writes_changed", toJson (next.writes != state.writes))]
      state := next
    | other => throw s!"unknown files event {other}"
  let rowJson := rows.map fun row => Json.mkObj [
    ("row", toJson row),
    ("ref", match state.rows row with
      | some file => referenceJson file.reference
      | none => Json.null)]
  pure <| Json.mkObj [
    ("finishes", Json.arr settlements),
    ("rows", Json.arr rowJson.toArray),
    ("queue", Json.arr (state.queue.map (referenceJson ·.row.reference)).toArray)]

private def run (j : Json) : Except String Json := do
  let cases ← fromJson? j (α := Array Json)
  Json.arr <$> cases.mapM runCase

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
