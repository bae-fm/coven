import CovenQueue.Conversion
import CovenMerge.OwnerAdmission

/-!
# The checks a change passes before its converter runs

Coven never calls a converter on a change that couldn't count anyway
(coven.md §18.6). Admission at the write's schema version changes no values
(`prepare_body`). Each admitted change then meets the receiver's admission
guard, the merge package's `Admission.guard` and the sync crate's per-change
verdicts: its owner permission under the current table declaration and the
creators coven kept (§9.6), its foreign-key effects against the write's
original schema (§9.1), and its data sources (§9.5), in that order. Only a
change the guard keeps reaches the converter; one it stops stays frozen in its
original shape, with the guard's cause.

`checkedLosses` gives conversion the guard's verdict for each of a write's
changes, the cause recorded as a queue loss cause.
-/

namespace CovenQueue.ConversionChecks

open CovenMerge.Ownership CovenMerge.Ownership.Admission

variable {W Row Table Member Payload : Type}
  [DecidableEq W] [DecidableEq Row] [DecidableEq Member] [DecidableEq Payload]

/-- The guard keeps a change exactly when its owner and foreign-key checks
pass, no earlier verdict dropped it, and none of its sources was dropped. -/
theorem guard_none_iff (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row)) (w : W) (r : Row) :
    guard P E D prior w r = none ↔
      check P E w r = true ∧ prior w r = none ∧ dropped D (seed P E prior) (w, r) = false := by
  unfold Admission.guard
  cases hc : check P E w r <;> cases hp : prior w r <;>
    cases hd : dropped D (seed P E prior) (w, r) <;> simp

/-- The losses conversion takes for write `w`'s changes: the guard's verdict
on each change's row `row r`, its cause recorded by `cause`. -/
def checkedLosses (P : Policy W Row Table Member) (E : Evidence W Row Payload)
    (D : Dependencies W Row) (prior : W → Row → Option (Cause W Member Row)) (w : W)
    (row : Nat → Row) (cause : Cause W Member Row → CovenMerge.LossCause Nat) :
    Nat → Option (CovenMerge.LossCause Nat) :=
  fun r => (guard P E D prior w (row r)).map cause

/-- **A change that fails its owner or foreign-key check never reaches the
converter.** It stays frozen with its original values, naming the member who
owns the row when the owner check failed and the foreign key otherwise. -/
theorem failed_check_frozen (f : List Nat → List Nat) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row)
    (prior : W → Row → Option (Cause W Member Row)) (w : W) (row : Nat → Row)
    (cause : Cause W Member Row → CovenMerge.LossCause Nat) (ch : RowChange) (values : List Nat)
    (original : ch.content = .values values) (failed : check P E w (row ch.row) = false) :
    (convertChange f (checkedLosses P E D prior w row cause) ch).content =
      .frozen (cause (if allowed P w (row ch.row) then .foreignKey
        else .notOwner (owner P (row ch.row)))) values := by
  apply dependent_change_not_converted f _ ch values _ original
  cases ha : allowed P w (row ch.row) <;> simp [checkedLosses, Admission.guard, failed, ha]

/-- **Only a change that passes every check reaches the converter.** A change
the conversion leaves unfrozen passed its owner and foreign-key checks, had no
earlier verdict, and depends on no dropped source; its values are the
converter's output. -/
theorem converted_passed_checks (f : List Nat → List Nat) (P : Policy W Row Table Member)
    (E : Evidence W Row Payload) (D : Dependencies W Row)
    (prior : W → Row → Option (Cause W Member Row)) (w : W) (row : Nat → Row)
    (cause : Cause W Member Row → CovenMerge.LossCause Nat) (ch : RowChange) (values : List Nat)
    (original : ch.content = .values values)
    (unfrozen : (convertChange f (checkedLosses P E D prior w row cause) ch).frozen = false) :
    check P E w (row ch.row) = true ∧ prior w (row ch.row) = none ∧
      dropped D (seed P E prior) (w, row ch.row) = false ∧
      (convertChange f (checkedLosses P E D prior w row cause) ch).content = .values (f values) := by
  have kept : guard P E D prior w (row ch.row) = none := by
    cases hg : guard P E D prior w (row ch.row) with
    | none => rfl
    | some c =>
      have := dependent_change_not_converted f (checkedLosses P E D prior w row cause) ch values
        (cause c) original (by simp [checkedLosses, hg])
      simp [RowChange.frozen, this] at unfrozen
  obtain ⟨checked, noPrior, notDropped⟩ := (guard_none_iff P E D prior w (row ch.row)).mp kept
  exact ⟨checked, noPrior, notDropped,
    independent_change_converted f _ ch values original (by simp [checkedLosses, kept])⟩

end CovenQueue.ConversionChecks
