import Lean

/-! Shared JSON scalar and uniqueness checks for sequence runner cases. -/
namespace SequenceInput
open Lean

def nat (j : Json) (key : String) : Except String Nat := j.getObjValAs? Nat key

def positive (j : Json) (key : String) : Except String Nat := do
  let n ← nat j key
  if n = 0 then throw s!"{key} must be positive"
  return n

def distinct [DecidableEq α] (xs : List α) (name : String) : Except String Unit := do
  if xs.eraseDups.length != xs.length then throw s!"duplicate {name}"

def pair (j : Json) : Except String (Nat × Nat) := do
  let xs ← j.getArr?
  match xs.toList with
  | [a, b] => return (← fromJson? a, ← fromJson? b)
  | _ => throw "expected a pair of natural numbers"

end SequenceInput
