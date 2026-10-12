import CovenMerge.OwnerDeleteRule

/-! Indirect effects at the removal-input boundary. These are completed SQL
results supplied to the guard, not an implementation of SQL extraction or
shared triggers. Row 0 is the deleted parent, 1 its affected row, 2 Ana's
protected row, and 3 a missing key, another claimant, or a move destination. -/
namespace CovenMerge.Ownership.DeleteEffectsPaths

inductive Effect where
  | check | uniqueKeyReference | multipleClaims | audienceMove
  deriving DecidableEq, Repr

def policy : Policy Bool Nat Unit Bool where
  table _ := ()
  owned _ := true
  creator r := decide (r = 2 ∨ r = 3)
  author := id

def claim (name : String) (key : Nat × Nat) (stamp : Nat) : Claim (Nat × Nat) :=
  ⟨⟨[name, "scope"], none⟩, key, stamp, false⟩

/-- CHECK loss propagates through a foreign key. A reference to a changed
unique key resolves to absent key 3. The multi-claim contest removes both
row 1 and its rival 2 in the unique pass. A generated audience move removes
source 2 and creates destination 3, with their common original owner. -/
def inputs (effect : Effect) (deleted : Bool) : EntryInputs Nat (Nat × Nat) where
  rows := [0, 1, 2, 3]
  present r := if r = 0 then !deleted else if r = 1 then true else
    match effect with
    | .audienceMove => (r == 2 && !deleted) || (r == 3 && deleted)
    | .multipleClaims => r == 2 || r == 3
    | _ => r == 2
  refs r := if r = 2 then
    match effect with
    | .check => [⟨1, false⟩]
    | .uniqueKeyReference => [⟨if deleted then 3 else 1, false⟩]
    | _ => []
    else []
  checkFails r := decide (effect = .check) && deleted && r == 1
  inDeletedCircle _ := none
  claims r := if effect = .multipleClaims then
    if r = 1 then [claim "a" (if deleted then 0 else 1, 0) 1, claim "b" (0, 0) 1]
    else if r = 2 then [claim "a" (0, 0) 2, claim "b" (1, 0) 2]
    else if r = 3 then [claim "a" (2, 0) 0, claim "b" (0, 0) 0] else []
    else []
  rank := id

theorem indirect_effect_blocked (effect : Effect) :
    (entryView (inputs effect false)).shown 2 = true ∧
    (entryView (inputs effect true)).shown 2 = false ∧
    effectsReason policy false (inputs effect false) (inputs effect true) = some (.delete 2 true) := by
  cases effect <;> decide

/-- The rival loses even though its competing claimant also loses a different
constraint. All unique contests read the same pass-one survivors. -/
theorem multiple_claims_judged_together :
    (entryView (inputs .multipleClaims true)).shown 1 = false ∧
    (entryView (inputs .multipleClaims true)).shown 2 = false ∧
    (entryView (inputs .multipleClaims true)).shown 3 = true := by decide

/-- Moving the row does not evade protection by supplying a destination copy. -/
theorem audience_source_protected :
    owner policy 2 = owner policy 3 ∧
    (entryView (inputs .audienceMove true)).shown 3 = true ∧
    effectsReason policy false (inputs .audienceMove false) (inputs .audienceMove true) =
      some (.delete 2 true) := by decide

end CovenMerge.Ownership.DeleteEffectsPaths
