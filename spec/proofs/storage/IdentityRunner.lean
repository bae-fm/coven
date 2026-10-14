import CovenStorage.ReplacementRead
import Lean

/-! JSON adapter comparing the Rust pre-send identity check and replacement
observation with `checkIdentity` and `ReplacementRead`. -/

open Lean CovenStorage

private def ends (j : Json) : Except String Ends := do
  pure ⟨← j.getObjValAs? Nat "writes", ← j.getObjValAs? Nat "entries"⟩

private def counters (j : Json) : Except String Counters := do
  pure ⟨← ends j, ← j.getObjValAs? Nat "snapshot"⟩

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

private def run (j : Json) : Except String Json := do
  let cases ← fromJson? j (α := Array Json)
  Json.arr <$> cases.mapM fun c => do
    match ← c.getObjValAs? String "check" with
    | "identity" => identity c
    | "replacement" => replacement c
    | other => throw s!"unknown identity check {other}"

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
