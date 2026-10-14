import CovenStorage.ReplacementRead
import CovenStorage.IdentityCounters
import Lean

/-! JSON adapter comparing the Rust pre-send identity check, replacement
observation and replacement admission with `checkIdentity` and `ReplacementRead`,
and the reservation sequences and storage-less reset with `IdentityCounters`. -/

open Lean CovenStorage

private def ends (j : Json) : Except String Ends := do
  pure ⟨← j.getObjValAs? Nat "writes", ← j.getObjValAs? Nat "entries"⟩

private def counters (j : Json) : Except String Counters := do
  pure ⟨← ends j, ← j.getObjValAs? Nat "key_copies", ← j.getObjValAs? Nat "snapshot"⟩

private def scan (j : Json) : Except String (Except ReadFailure Evidence) := do
  if (j.getObjVal? "error").isOk then return .error .network
  return .ok {
    listed := ← counters (← j.getObjVal? "listed")
    covered := ← ends (← j.getObjVal? "covered")
    posted := ← ends (← j.getObjVal? "posted")
    replaced := ← j.getObjValAs? Bool "replaced"
    removed := ← j.getObjValAs? Bool "removed" }

private def gateJson : Gate → Json
  | .send => "send"
  | .pending _ => "pending"
  | .stopped => "stopped"
  | .reset .identityMismatch => "reset:identity-mismatch"
  | .reset .storageAhead => "reset:storage-ahead"
  | .reset .slotMismatch => "reset:slot-mismatch"
  | .reset .replaced => "reset:replaced"
  | .reset .damage => "reset:damage"

private def custody (j : Json) : Except String (Option Nat) :=
  match j.getObjVal? "custody" with
  | .ok .null => pure none
  | .ok value => some <$> fromJson? value
  | .error _ => pure none

private def identity (j : Json) : Except String Json := do
  let device ← j.getObjValAs? Nat "device"
  let reserved ← counters (← j.getObjVal? "reserved")
  return gateJson (checkIdentity device reserved (← custody j) (← scan (← j.getObjVal? "scan")))

/-- Observe kept registrations, then query the gate, as an active device does. -/
private def replacement (j : Json) : Except String Json := do
  let device ← j.getObjValAs? Nat "device"
  let reserved ← counters (← j.getObjVal? "reserved")
  let mut state : ReplacementRead.Device := .active ⟨device, reserved, [], []⟩ []
  let mut results := #[]
  for event in ← j.getObjValAs? (Array Json) "events" do
    match ← event.getObjValAs? String "op" with
    | "observe" =>
      let r : ReplacementRead.Registration :=
        ⟨← event.getObjValAs? Nat "old", ← event.getObjValAs? Nat "fresh", 1⟩
      state := ReplacementRead.observe state r (← event.getObjValAs? Nat "own_fresh")
    | "gate" =>
      let g := ReplacementRead.gate state .write (← custody event) (← scan (← event.getObjVal? "scan"))
      results := results.push (gateJson g)
    | other => throw s!"unknown replacement event {other}"
  return Json.arr results

/-- Begin a reset from a working copy, retry it, then finish it: whether the
installation is writable while resetting, and the finished device's id,
queue length and first write number. A store that never set up storage
finishes with `resetUnconnected`. -/
private def reset (j : Json) : Except String Json := do
  let queued ← j.getObjValAs? Nat "queued"
  let copy : Local := ⟨← j.getObjValAs? Nat "old", ⟨⟨queued, 0⟩, 0, 0⟩,
    (List.range queued).map fun n => .untried ⟨.write 0 (n + 1), []⟩, []⟩
  let r : Replacement := ⟨copy.device, ← j.getObjValAs? Nat "fresh", ⟨0, 0⟩, 1, .storageAhead⟩
  let mut state := beginReset copy r
  let mut writable := #[]
  for _ in List.range (← j.getObjValAs? Nat "retries") do
    state := retryReset state
    writable := writable.push (CovenStorage.writable state)
  let connected ← match j.getObjVal? "connected" with
    | .ok value => fromJson? value
    | .error _ => pure true
  let finished :=
    if connected then finishReset r [] else IdentityCounters.resetUnconnected r
  return Json.mkObj [("writable", toJson writable), ("device", toJson finished.device),
    ("queue", toJson finished.queue.length),
    ("first", toJson (commitWrite finished []).reserved.logs.writes)]

/-- From a sequence at `last`, reserve operation ids in order, each a
snapshot operation or another: the last id, and the greatest snapshot
number among them, which is the reservation the check compares. -/
private def sequence (j : Json) : Except String Json := do
  let steps ← j.getObjValAs? (Array Bool) "snapshots"
  let start : IdentityCounters.Sequence := ⟨← j.getObjValAs? Nat "last", []⟩
  let s := start.run steps.toList
  return Json.mkObj [("last", toJson s.last),
    ("snapshot", toJson (IdentityCounters.greatest s.kind))]

private def entryPath (j : Json) : Except String Path := do
  pure (.entry (← j.getObjValAs? Nat "device") (← j.getObjValAs? Nat "number"))

private def registration (j : Json) : Except String ReplacementRead.Registration := do
  pure ⟨← j.getObjValAs? Nat "old", ← j.getObjValAs? Nat "fresh", ← j.getObjValAs? Nat "number"⟩

private def logObject (j : Json) : Except String ReplacementRead.LogObject := do
  let kind ← match ← j.getObjValAs? String "kind" with
    | "write" => pure LogKind.writes
    | "entry" => pure LogKind.entries
    | other => throw s!"unknown log object kind {other}"
  pure ⟨← j.getObjValAs? Nat "device", ← j.getObjValAs? Nat "number", kind,
    ← (← j.getObjValAs? (Array Json) "past").toList.mapM entryPath⟩

/-- Judge old-id objects by their recorded pasts against the kept registrations,
then rebuild the applied set from all of them with `load`. -/
private def admission (j : Json) : Except String Json := do
  let kept ← (← j.getObjValAs? (Array Json) "kept").toList.mapM registration
  let objects ← (← j.getObjValAs? (Array Json) "objects").toList.mapM logObject
  let loaded := ReplacementRead.load kept objects
  return Json.mkObj [
    ("accepts", toJson (objects.map (ReplacementRead.accepts kept))),
    ("loaded", toJson (objects.map fun o => decide (o.path ∈ loaded)))]

private def run (j : Json) : Except String Json := do
  let cases ← fromJson? j (α := Array Json)
  Json.arr <$> cases.mapM fun c => do
    match ← c.getObjValAs? String "check" with
    | "identity" => identity c
    | "replacement" => replacement c
    | "reset" => reset c
    | "admission" => admission c
    | "sequence" => sequence c
    | other => throw s!"unknown identity check {other}"

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
