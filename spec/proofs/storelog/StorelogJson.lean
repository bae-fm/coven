import CovenStorelog.CurrentReplay
import CovenStorelog.Horizon
import CovenStorelog.ExamplesSupport
import Lean

/-! Checked JSON histories shared by the store-log and data differential runners. -/
namespace StorelogJson
open Lean CovenStorelog

def nat (j : Json) (name : String) : Except String Nat :=
  j.getObjValAs? Nat name

def readRole (j : Json) : Except String Role := do
  match ← nat j "role" with
  | 0 => pure .admin
  | 1 => pure .member
  | _ => throw "unknown role"

def readAudience (j : Json) : Except String Audience := do
  match ← nat j "audience" with
  | 0 => pure .store
  | c + 1 => pure (.circle c)

def readSnapshot (j : Json) : Except String SnapshotId := do
  let snapshot ← j.getObjVal? "snapshot"
  pure ⟨← readAudience snapshot, ← nat snapshot "number"⟩

def readKey (j : Json) : Except String CurrentReplay.KeyCommitment := do
  let hash ← j.getObjValAs? (Array Nat) "key_hash"
  if hash.any (· > 255) then throw "key hash must contain 32 bytes"
  let bytes := hash.map UInt8.ofNat
  if h : bytes.size = 32 then
    pure ⟨← nat j "key", ⟨bytes, h⟩⟩
  else throw "key hash must contain 32 bytes"

def readAction (j : Json) : Except String CurrentReplay.Action := do
  match ← nat j "kind" with
  | 0 => pure (.create (← j.getObjValAs? String "access") (← readKey j))
  | 1 => pure (.addMember (← nat j "member") (← readRole j) (← j.getObjValAs? String "access"))
  | 2 => pure (.removeMember (← nat j "member"))
  | 3 => pure (.changeRole (← nat j "member") (← readRole j))
  | 4 => pure (.addDevice (← nat j "member") (← nat j "device"))
  | 5 => pure (.removeDevice (← nat j "member") (← nat j "device"))
  | 6 => pure (.makeCircle (← nat j "circle") (← j.getObjValAs? String "name") (← readKey j))
  | 7 => pure (.renameCircle (← nat j "circle") (← j.getObjValAs? String "name"))
  | 8 => pure (.deleteCircle (← nat j "circle"))
  | 9 => pure (.addToCircle (← nat j "circle") (← nat j "member"))
  | 10 => pure (.removeFromCircle (← nat j "circle") (← nat j "member"))
  | 11 => pure (.raiseSchema (← nat j "version") (← readSnapshot j))
  | 13 => pure (.reset (← readSnapshot j))
  | 14 => pure (.setAccess (← nat j "member") (← j.getObjValAs? String "access"))
  | 15 => pure (.rotateKey (← readAudience j) (← readKey j))
  | _ => throw "unknown action"

def readEntry (j : Json) : Except String CurrentReplay.Entry := do
  pure ⟨← nat j "author", ← nat j "device", ← j.getObjValAs? (List Nat) "past",
    ← readAction (← j.getObjVal? "action")⟩

def roleNumber : Role → Nat
  | .admin => 0
  | .member => 1

def audienceNumber : Audience → Nat
  | .store => 0
  | .circle c => c + 1

def sorted (xs : List Nat) : List Nat := xs.mergeSort (fun a b => decide (a ≤ b))

def byKey {α : Type} (xs : List (Nat × α)) : List (Nat × α) :=
  xs.mergeSort (fun a b => decide (a.1 ≤ b.1))

def project (r : Result) (causes : List (List Nat)) (late : List Nat) : Json := Id.run do
  let members := (byKey r.state.members).map fun (m, role) => toJson [m, roleNumber role]
  let access := (byKey r.state.access).map fun (m, a) => Json.arr #[toJson m, toJson a]
  let devices := (byKey r.state.devices).map fun (d, m) => toJson [d, m]
  let circles := (byKey r.state.circles).map fun (c, circle) =>
    Json.arr #[toJson c, toJson circle.name, toJson (sorted circle.members)]
  let versions := byKey (r.state.versions.map fun (a, v) => (audienceNumber a, v))
  let versions := versions.map fun (a, v) => toJson [a, v.number, v.snapshot, v.entry]
  let resets := byKey (r.state.resets.map fun (a, s) => (audienceNumber a, s))
  let resets := resets.map fun (a, s) => toJson [a, s]
  return (Json.mkObj [
    ("landed_too_late", toJson (sorted late)), ("created", toJson r.state.created), ("members", toJson members), ("access", toJson access),
    ("devices", toJson devices), ("circles", toJson circles),
    ("versions", toJson versions), ("resets", toJson resets),
    ("kept", toJson (sorted r.kept)), ("dropped", toJson (sorted r.dropped)),
    ("circle_causes", toJson causes)])

structure Input where
  history : CurrentReplay.History
  bound : Nat
  window : Nat

def read (j : Json) : Except String Input := do
  let raw ← j.getObjValAs? (Array Json) "entries"
  let entries ← raw.mapM readEntry
  let some root := entries[0]? | throw "history must contain creation"
  let stored ← raw.mapM (fun e => nat e "stored")
  let attempted ← raw.mapM (fun e => nat e "attempted")
  let window ← nat j "window"
  -- These total functions are inspected only below entries.size. Extending by
  -- the actual root supplies no invented action or cryptographic commitment.
  let H : CurrentReplay.History := ⟨fun e => entries[e]?.getD root,
    fun e => stored[e]?.getD stored[0]!, fun e => attempted[e]?.getD attempted[0]!⟩
  if !validCheck H.membership.log entries.size then throw "history violates Valid"
  for i in List.range entries.size do
    if attempted[i]! > stored[i]! then throw "attempt follows storage time"
    for p in (H.log i).past do
      if stored[p]! > attempted[i]! then throw "recorded read follows attempt time"
  pure ⟨H, entries.size, window⟩

def checkedViews (input : Input) : Except String (Nat → State) := do
  let H := input.history
  let window := input.window
  let views := CurrentReplay.authorViews H window input.bound input.bound
  for i in List.range input.bound do
    match (H.log i).action with
    | .removeDevice m d =>
        if lookup (views i).devices d != some m then
          throw s!"device removal {i} has no matching owner in its author's view"
    | _ => pure ()
  pure views

def received (j : Json) (input : Input) : Except String (Array (List Nat)) := do
  let H := input.history
  let received ← j.getObjValAs? (Array (List Nat)) "received"
  for subset in received do
    if subset.any (· ≥ input.bound) then throw "received entry outside history"
    for e in subset do
      if !(H.log e).past.all (· ∈ subset) then throw "received subset is not causally closed"
  pure received

end StorelogJson
