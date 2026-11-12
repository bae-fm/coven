import CovenStorage.FloorPointer
import Lean

open Lean CovenStorage.FloorPointer

private def name (op : Json) : Except String CovenStorage.FloorPointer.Name := do
  pure ⟨← op.getObjValAs? Nat "owner", ← op.getObjValAs? Nat "number"⟩

private def step (op : Json) : Except String Step := do
  match ← op.getObjValAs? String "op" with
  | "upload" => pure (.upload (← name op))
  | "late_create" => pure (.lateCreate (← name op))
  | "replace" => pure (.replace (← name op) (← op.getObjValAs? Nat "observed"))
  | "settle" => pure (.settle (← name op))
  | "delete_own" => pure (.deleteOwn (← name op))
  | "delete_gone" => pure (.deleteGone (← name op))
  | "gone" => pure (.markGone (← op.getObjValAs? Nat "device"))
  | other => throw s!"unknown operation {other}"

private def pair (b : CovenStorage.FloorPointer.Name) : Json := Json.arr #[toJson b.owner, toJson b.number]

/-- What the Rust test sees after each operation: the bodies in storage in name
order, the body the pointer names and the pointer's revision. -/
private def observe (s : State) : Json :=
  let sorted := s.bodies.toArray.qsort fun a b =>
    a.owner < b.owner ∨ (a.owner = b.owner ∧ a.number < b.number)
  Json.mkObj [
    ("bodies", Json.arr (sorted.map pair)),
    ("pointer", match s.pointer with | some b => pair b | none => Json.null),
    ("revision", toJson s.revision)]

private def run (input : Json) : Except String Json := do
  let mut s := initial
  let mut seen := #[]
  for op in ← input.getObjValAs? (Array Json) "ops" do
    s := (← step op).apply s
    seen := seen.push (observe s)
  pure (Json.arr seen)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
