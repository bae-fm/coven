import CovenStorage.Files
import Lean

/-! JSON adapter comparing the Rust staging, upload completion and the
references, records and queue they leave with `Files.stage`, `Files.change`
and `Files.finish` (§17.2, §17.9).

Each case is a list of events on device 1. `attach` stages file `file` for
the row, inserting the row if it isn't there, with its upload to `ref`;
`delete` deletes a row with this device's own `Files.change`; `begin` marks
when the Rust attempt opens its source and is ignored here; `finish` settles
the upload to a reference against a storage observation. The output reports
each settlement and deletion, whether it changed the write count, and
finally the file each row references, every record with its stored
reference, if any, and the queued uploads.

A `status` case instead judges one storage observation of the given object
length against the file's complete encrypted size, using `Files.judge`: for a
stored file, or for a file on this device only while it uploads, with or
without a source failure (`pending`). With `read` ranges it also reads them
in order with `Files.fetch`, stopping at the first failure. -/

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
  | .occupied => "refused"

private def runCase (j : Json) : Except String Json := do
  let events ← j.getObjValAs? (Array Json) "events"
  let mut state : State := ⟨1, [], [], [], [], [], [], 0⟩
  let mut rows : List Nat := []
  let mut settlements := #[]
  let mut deletes := #[]
  for event in events do
    match ← event.getObjValAs? String "op" with
    | "attach" =>
      let row ← event.getObjValAs? Nat "row"
      let r ← reference (← event.getObjVal? "ref")
      let upload : Files.Upload := ⟨r, ← event.getObjValAs? Nat "size",
        ← event.getObjValAs? Nat "hash", [], []⟩
      state := stage state row upload (← event.getObjValAs? Nat "file")
      unless rows.contains row do rows := rows ++ [row]
    | "delete" =>
      let next := change state (← event.getObjValAs? Nat "row") none
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
    ("file", match state.file row with
      | some file => toJson file
      | none => Json.null)]
  let records := (state.records.map (·.id)).mergeSort (· ≤ ·)
  let recordJson := records.filterMap fun id => (state.records.find? (·.id == id)).map fun rec =>
    Json.mkObj [("file", toJson id),
      ("ref", match rec.coordinates.stored with
        | some r => referenceJson r
        | none => Json.null)]
  pure <| Json.mkObj [
    ("finishes", Json.arr settlements),
    ("deletes", Json.arr deletes),
    ("rows", Json.arr rowJson.toArray),
    ("records", Json.arr recordJson.toArray),
    ("queue", Json.arr (state.queue.map (referenceJson ·.location)).toArray)]

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
  | .uploading => "uploading"
  | .deviceOnly device => Json.mkObj [("deviceOnly", toJson device)]
  | .missing device .lost =>
    Json.mkObj [("missing", toJson device), ("reason", "lost")]
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
complete encrypted size, and whether the file is on this device only while it
uploads there (`pending`), with its source failure (`report`), or stored at
the reference. An observed object has `length` bytes, the complete size unless
given. -/
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
  let pending ← j.getObjValAs? Bool "pending"
  let report ← match j.getObjVal? "report" with
    | .ok .null => pure none
    | .ok (.str failure) => some <$> sourceFailure failure
    | _ => pure none
  let coordinates : Coordinates := if pending then .device r.uploader else .cloud r
  let here : Local := if pending then some report else none
  let judged := statusJson (judge coordinates size observed here)
  match j.getObjVal? "read" with
  | .error _ => pure judged
  | .ok ranges =>
    let ranges ← (← fromJson? ranges (α := Array Json)).mapM range
    let read := ranges.toList.forM fun (start, stop) =>
      (fetch r size start stop (rangeReply observed start stop)).map fun _ => ()
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
