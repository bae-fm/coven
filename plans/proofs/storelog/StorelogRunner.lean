import CovenStorelog
import Lean

/-! JSON adapter for the Rust differential test. Histories are numbered in
timestamp order; past lists contain all observed entry indices. The adapter
checks `Valid` and observed device ownership, then calls `resolve` directly.
No replay or proof is reimplemented here. -/

open Lean CovenStorelog

private def nat (j : Json) (name : String) : Except String Nat :=
  j.getObjValAs? Nat name

private def readRole (j : Json) : Except String Role := do
  match ← nat j "role" with
  | 0 => pure .admin
  | 1 => pure .member
  | _ => throw "unknown role"

private def readAudience (j : Json) : Except String Audience := do
  match ← nat j "audience" with
  | 0 => pure .store
  | c + 1 => pure (.circle c)

private def readSnapshot (j : Json) : Except String SnapshotId := do
  let snapshot ← j.getObjVal? "snapshot"
  pure ⟨← readAudience snapshot, ← nat snapshot "number"⟩

private def readAction (j : Json) : Except String Action := do
  match ← nat j "kind" with
  | 0 => pure (.create (← j.getObjValAs? String "access"))
  | 1 => pure (.addMember (← nat j "member") (← readRole j) (← j.getObjValAs? String "access"))
  | 2 => pure (.removeMember (← nat j "member") (← j.getObjValAs? (List Nat) "circles"))
  | 3 => pure (.changeRole (← nat j "member") (← readRole j))
  | 4 => pure (.addDevice (← nat j "member") (← nat j "device"))
  | 5 => pure (.removeDevice (← nat j "member") (← nat j "device"))
  | 6 => pure (.makeCircle (← nat j "circle") (← j.getObjValAs? String "name"))
  | 7 => pure (.renameCircle (← nat j "circle") (← j.getObjValAs? String "name"))
  | 8 => pure (.deleteCircle (← nat j "circle"))
  | 9 => pure (.addToCircle (← nat j "circle") (← nat j "member"))
  | 10 => pure (.removeFromCircle (← nat j "circle") (← nat j "member"))
  | 11 => pure (.raiseVersion .schema (← nat j "version") (← readSnapshot j))
  | 12 => pure (.raiseVersion .format (← nat j "version") (← readSnapshot j))
  | 13 => pure (.reset (← readSnapshot j))
  | 14 => pure (.setAccess (← nat j "member") (← j.getObjValAs? String "access"))
  | _ => throw "unknown action"

private def readEntry (j : Json) : Except String Entry := do
  pure ⟨← nat j "author", ← nat j "device", ← j.getObjValAs? (List Nat) "past",
    ← readAction (← j.getObjVal? "action")⟩

private def roleNumber : Role → Nat
  | .admin => 0
  | .member => 1

private def audienceNumber : Audience → Nat
  | .store => 0
  | .circle c => c + 1

private def kindNumber : VersionKind → Nat
  | .schema => 0
  | .format => 1

private def sorted (xs : List Nat) : List Nat := xs.mergeSort (fun a b => decide (a ≤ b))

private def byKey {α : Type} (xs : List (Nat × α)) : List (Nat × α) :=
  xs.mergeSort (fun a b => decide (a.1 ≤ b.1))

private def run (j : Json) : Except String Json := do
  let entries ← (← j.getObjValAs? (Array Json) "entries").mapM readEntry
  -- Log is a total function. Valid and resolve inspect only the finite
  -- prefix, except Valid's root check for an empty history; extend it by creation.
  let M : Log := fun w => entries[w]?.getD ⟨0, 0, [], .create "unused"⟩
  if !validCheck M entries.size then throw "history violates Valid"
  let views := authorViews M entries.size
  for i in List.range entries.size do
    match (M i).action with
    | .removeDevice m d =>
        if lookup (views i).devices d != some m then
          throw s!"device removal {i} has no matching owner in its author's view"
    | _ => pure ()
  let r := resolve M entries.size (fun _ => true)
  let members := (byKey r.state.members).map fun (m, role) => toJson [m, roleNumber role]
  let access := (byKey r.state.access).map fun (m, a) => Json.arr #[toJson m, toJson a]
  let devices := (byKey r.state.devices).map fun (d, m) => toJson [d, m]
  let circles := (byKey r.state.circles).map fun (c, circle) =>
    Json.arr #[toJson c, toJson circle.name, toJson (sorted circle.members)]
  let versions := (r.state.versions.map fun ((k, a), v) =>
    (kindNumber k, audienceNumber a, v)).mergeSort
      (fun x y => decide (x.1 < y.1 ∨ (x.1 = y.1 ∧ x.2.1 ≤ y.2.1)))
  let versions := versions.map fun (k, a, v) => toJson [k, a, v.number, v.snapshot, v.entry]
  let resets := byKey (r.state.resets.map fun (a, s) => (audienceNumber a, s))
  let resets := resets.map fun (a, s) => toJson [a, s]
  pure (Json.mkObj [
    ("created", toJson r.state.created), ("members", toJson members), ("access", toJson access),
    ("devices", toJson devices), ("circles", toJson circles),
    ("versions", toJson versions), ("resets", toJson resets),
    ("kept", toJson (sorted r.kept)), ("dropped", toJson (sorted r.dropped))])

def main : IO UInt32 := do
  let stdin ← IO.getStdin
  let stdout ← IO.getStdout
  let input ← stdin.readToEnd
  let output := do
    let j ← Json.parse input
    let histories ← j.getObjValAs? (Array Json) "histories"
    let results ← histories.mapM run
    pure (toJson results)
  match output with
  | .ok result => stdout.putStrLn result.compress; pure 0
  | .error err =>
    (← IO.getStderr).putStrLn err
    pure 1
