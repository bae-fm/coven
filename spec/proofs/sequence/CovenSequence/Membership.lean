import CovenSequence.Cutoff

/-! Removed member ids remain retired, independently of the authority policy.
A replacement invitation for an active member does not remove that member. -/
namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- No valid addition can reuse an id retained by removal. -/
theorem retired_member_refused (L : Ledger S) (a : S.Dev) (m : S.Mem) (role : Role)
    (retired : L.retired m = true) :
    ledgerValid R L (.admin a (.addMember m role)) = false := by
  simp [ledgerValid, structural, retired]

/-- Every slot preserves retirement and absence together. -/
theorem step_retired_member (L : Ledger S) (k : Nat) (x : Slot S) (m : S.Mem)
    (retired : L.retired m = true) (absent : L.member m = false) :
    (ledgerStep R L k x).retired m = true ∧ (ledgerStep R L k x).member m = false := by
  unfold ledgerStep
  by_cases valid : ledgerValid R L x = true
  · simp only [valid, ite_true]
    cases x with
    | admin a e =>
      cases e with
      | addMember target role =>
        by_cases same : m = target
        · subst target
          rw [retired_member_refused R L a m role retired] at valid
          contradiction
        · simp [noteClaim, ledgerEffect, applyEntry, same, retired, absent]
      | removeMember target =>
        by_cases same : m = target <;>
          simp [noteClaim, ledgerEffect, applyEntry, same, retired, absent]
      | _ => exact ⟨retired, absent⟩
    | _ => exact ⟨retired, absent⟩
  · simp [valid, noteClaim, refuse, retired, absent]

/-- A retired id stays absent at every later prefix. -/
theorem retired_member_stays (seq : Nat → Slot S) (m : S.Mem) (i : Nat)
    (retired : (ledgerAt R seq i).retired m = true)
    (absent : (ledgerAt R seq i).member m = false) :
    ∀ k, i ≤ k → (ledgerAt R seq k).retired m = true ∧ (ledgerAt R seq k).member m = false
  | k, later => by
    induction k with
    | zero => have same : i = 0 := by omega
              subst i; exact ⟨retired, absent⟩
    | succ k ih =>
      by_cases same : i = k + 1
      · subst i; exact ⟨retired, absent⟩
      · have prior := ih (by omega)
        exact step_retired_member R _ _ _ m prior.1 prior.2

/-- An applied removal permanently ends membership, even across attempted
additions, role changes, registrations, key replacements and arbitrary slots. -/
theorem removed_member_never_active (seq : Nat → Slot S) (i : Nat) (positive : 1 ≤ i)
    (a : S.Dev) (m : S.Mem) (slot : seq i = .admin a (.removeMember m))
    (applied : Applied R seq i) (k : Nat) (later : i ≤ k) :
    (ledgerAt R seq k).retired m = true ∧ (ledgerAt R seq k).member m = false := by
  have effect : (ledgerAt R seq i).retired m = true ∧ (ledgerAt R seq i).member m = false := by
    rw [applied_ledger R seq positive applied, slot]
    simp [noteClaim, ledgerEffect, applyEntry]
  exact retired_member_stays R seq m i effect.1 effect.2 k later

end CovenSequence
