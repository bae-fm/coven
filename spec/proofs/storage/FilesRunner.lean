import CovenStorage.Files
import Lean

/-! JSON adapter comparing the Rust attachment, upload completion and the
rows and queue they leave with `Files.attach` and `Files.finish` (§16.1, §16.5).

Each case is a list of events. `attach` fixes a row's reference and queues it;
`delete` removes a row with `Files.deleteRow`; `begin` marks when the Rust
attempt opens its source and is ignored here; `finish` settles a queued
reference against a storage observation. The output reports each settlement
and deletion, whether it changed the write count, and finally every row's
reference and the queued references.

A `status` case instead judges one storage observation of the given object
length against the file's complete encrypted size, with the uploader's
evidence, using `Files.status`. With `read` ranges it also reads them in order
with `Files.fetch`, stopping at the first failure. -/

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

private def runCase (j : Json) : Except String Json := do
  let events ← j.getObjValAs? (Array Json) "events"
  let mut state : State := ⟨fun _ => none, [], [], 0⟩
  let mut rows : List Nat := []
  let mut settlements := #[]
  let mut deletes := #[]
  for event in events do
    match ← event.getObjValAs? String "op" with
    | "attach" =>
      let row ← event.getObjValAs? Nat "row"
      let r ← reference (← event.getObjVal? "ref")
      let file : RowFile := ⟨r, ← event.getObjValAs? Nat "size",
        ← event.getObjValAs? Nat "hash", ← event.getObjValAs? Nat "version"⟩
      state := attach state row ⟨file, []⟩
      unless rows.contains row do rows := rows ++ [row]
    | "delete" =>
      let next := deleteRow state (← event.getObjValAs? Nat "row")
      deletes := deletes.push <| Json.mkObj [("writes_changed", toJson (next.writes != state.writes))]
      state := next
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
    ("deletes", Json.arr deletes),
    ("rows", Json.arr rowJson.toArray),
    ("queue", Json.arr (state.queue.map (referenceJson ·.row.reference)).toArray)]

private def deviceState : String → Except String DeviceState
  | "active" => pure .active
  | "removed" => pure .removed
  | "replaced" => pure .replaced
  | other => throw s!"unknown device state {other}"

private def sourceFailure : String → Except String SourceFailure
  | "missing" => pure .missing
  | "changed" => pure .changed
  | "integrity" => pure .integrity
  | other => throw s!"unknown source failure {other}"

private def sourceFailureJson : SourceFailure → Json
  | .missing => "missing"
  | .changed => "changed"
  | .integrity => "integrity"

private def verdictJson : Status → Json
  | .available => "available"
  | .uploading device => Json.mkObj [("uploading", toJson device)]
  | .missing device .deviceRemoved =>
    Json.mkObj [("missing", toJson device), ("reason", "removed")]
  | .missing device .deviceReplaced =>
    Json.mkObj [("missing", toJson device), ("reason", "replaced")]
  | .missing device (.source failure) =>
    Json.mkObj [("missing", toJson device), ("reason", sourceFailureJson failure)]

private def fileErrorJson : FileError → Json
  | .failed _ => Json.mkObj [("error", true)]
  | .integrity => "integrity"

private def statusJson : Except FileError Status → Json
  | .error e => fileErrorJson e
  | .ok verdict => verdictJson verdict

private def readJson : Except ReadError Unit → Json
  | .ok () => "ok"
  | .error (.file e) => fileErrorJson e
  | .error (.unavailable verdict) => verdictJson verdict

private def range (j : Json) : Except String (Nat × Nat) := do
  let bounds ← fromJson? j (α := Array Nat)
  match bounds.toList with
  | [start, stop] => pure (start, stop)
  | _ => throw "a range is [start, stop]"

/-- One status judgement: the observation at the fixed path, the file's
complete encrypted size and the uploader's state and report. An observed object
has `length` bytes, the complete size unless given. Unrelated devices' states
and reports are set to differ. -/
private def statusCase (j : Json) : Except String Json := do
  let r ← reference (← j.getObjVal? "ref")
  let size ← j.getObjValAs? Nat "size"
  let observed : Except ReadFailure (Option Object) ←
    match ← j.getObjValAs? String "remote" with
    | "present" =>
      let length := (j.getObjValAs? Nat "length").toOption.getD size
      pure (.ok (some ⟨List.replicate length 0, 0⟩))
    | "absent" => pure (.ok none)
    | "error" => pure (.error .network)
    | other => throw s!"unknown storage observation {other}"
  let state ← deviceState (← j.getObjValAs? String "device")
  let report ← match j.getObjVal? "report" with
    | .ok .null => pure none
    | .ok (.str failure) => some <$> sourceFailure failure
    | _ => pure none
  let reports : Reports := {
    device := fun d => if d = r.uploader then state else .removed
    unavailable := fun d f => if d = r.uploader ∧ f = r.id then report else some .integrity }
  let judged := statusJson (status r size observed reports)
  match j.getObjVal? "read" with
  | .error _ => pure judged
  | .ok ranges =>
    let ranges ← (← fromJson? ranges (α := Array Json)).mapM range
    let read := ranges.toList.forM fun (start, stop) =>
      (fetch r reports size start stop (rangeReply observed start stop)).map fun _ => ()
    pure (Json.mkObj [("status", judged), ("read", readJson read)])

private def run (j : Json) : Except String Json := do
  let cases ← fromJson? j (α := Array Json)
  Json.arr <$> cases.mapM fun c =>
    if (c.getObjVal? "status").isOk then statusCase c else runCase c

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
