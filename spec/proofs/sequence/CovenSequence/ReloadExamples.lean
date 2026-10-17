import CovenSequence.Reload
import CovenSequence.ReaderExamples

namespace CovenSequence.ReloadExamples
open ReaderExamples (sig rules part)

/-- A floor may still name slot 2 while a reload selected slot 4 and slot 6
has since superseded that selection. All three announcements are valid. -/
def history : Nat → Slot sig
  | 1 => .writes () 0 (part 10)
  | 2 => .snapshot () () 1 1
  | 3 => .writes () 0 (part 20)
  | 4 => .snapshot () () 3 2
  | 5 => .writes () 0 (part 30)
  | 6 => .snapshot () () 5 3
  | _ => .writes () 0 (fun _ => none)

def before : Store sig where
  slot := fun i => if 1 ≤ i ∧ i ≤ 6 then some (history i) else none
  first := fun i => if 1 ≤ i ∧ i ≤ 6 then some (history i) else none
  head := 6
  floor := ⟨2, fun _ => 1, fun _ => 1⟩
  body := fun p => some (stateAt rules history () (2 * p - 1))

def after : Store sig :=
  { before with body := fun p => if p = 2 then none else before.body p }

theorem before_good : Good rules before history := by
  refine ⟨?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_, ?_⟩
  · intro i hi hh; simp only [before] at hh ⊢; simp [hi, hh]
  · intro i hi; simp only [before] at hi ⊢
    have : ¬ (1 ≤ i ∧ i ≤ 6) := by omega
    simp [this]
  · intro i hi; simp only [before] at hi ⊢
    have : ¬ (1 ≤ i ∧ i ≤ 6) := by omega
    simp [this]
  · intro _ _; rfl
  · intro _; simp [before]
  · intro _; simp [before]
  · intro A; cases A; rfl
  · intro A; cases A
    exact Or.inr ⟨2, (), by decide, by decide, rfl, by unfold AppliedTo; decide⟩
  · intro A r au v c p _ _ hs
    simp only [history] at hs
    split at hs <;> cases hs

theorem deletion_allowed : StoreStep rules history before after := by
  apply StoreStep.deleteBody before 2
  · intro A; simp [before]
  · intro A r au v c _ _ hs
    simp only [history] at hs
    split at hs <;> cases hs

/-- **Counterexample without the replacement check.** Good storage permits
deleting the selected body, with a valid later replacement already announced,
while the floor still names an older snapshot. Blind floor reselection lowers
coverage from 3 to 1 even though keeping the old target still preserves rows.
The checked operation returns NotFound instead; this is not a code failure. -/
theorem unchecked_reselection_goes_backwards :
    Good rules after history ∧
    AppliedTo rules history () 4 ∧ AppliedTo rules history () 6 ∧
    after.body 2 = none ∧ after.floor.covers () < 3 ∧
    Reload.replaces (⟨4, (), 3⟩ : Reload.Selection Unit) ⟨6, (), 5⟩ = true ∧
    Reload.reselect 4 2 [1] (some (⟨4, (), 3⟩ : Reload.Selection Unit)) [⟨2, (), 1⟩] = none ∧
    Reload.target 4 2 [1] = 4 := by
  exact ⟨store_good rules deletion_allowed before_good,
    by unfold AppliedTo; decide, by unfold AppliedTo; decide,
    rfl, by decide, by decide, by decide, by decide⟩

end CovenSequence.ReloadExamples
