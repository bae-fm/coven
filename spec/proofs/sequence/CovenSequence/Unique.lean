import CovenSequence.Cutoff

/-!
# Unique values and owner rows: first slot wins

A batch's `claims` are the unique values and owner rows it takes. A key goes
to the first counted batch that claims it, and keeps that owner. Owner creation
uses the same rule. The decision reads only slot order, so it is the same on
every device (`prefix_determines_state`), and a long-offline device's late
claim loses to the earlier slot whatever its timestamp.
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- Whether a slot's batch claims `key`. -/
def claimsKey : Slot S → S.Key → Bool
  | .writes _ _ _ cl _, key => decide (key ∈ cl)
  | _, _ => false

/-- Slot `i` is a counted batch that claims `key`. -/
def CountedClaim (seq : Nat → Slot S) (i : Nat) (key : S.Key) : Prop :=
  1 ≤ i ∧ Applied R seq i ∧ claimsKey (seq i) key = true

theorem step_owner (L : Ledger S) (k : Nat) (x : Slot S) (key : S.Key) :
    (ledgerStep R L k x).owner key =
      match L.owner key with
      | some i => some i
      | none => if valid R L k x && claimsKey x key then some k else none := by
  unfold ledgerStep
  by_cases hv : valid R L k x = true
  · simp only [hv, ite_true]
    cases x with
    | writes a g v cl b =>
      simp only [effect, claimAll, claimsKey, Bool.true_and]
      cases L.owner key <;> simp
    | admin a e =>
      cases e <;> simp only [effect, applyEntry, claimsKey] <;> cases L.owner key <;> simp
    | _ => simp only [effect, claimsKey] <;> cases L.owner key <;> simp
  · have hv' : valid R L k x = false := by simpa using hv
    simp only [hv', Bool.false_and, refuse]
    cases h : L.owner key <;> simp [h]

/-- **First slot wins.** Starting with no owners, after `k` slots a key's owner
is `i` exactly when slot `i ≤ k` is the first counted batch claiming it, and it
has none exactly when no counted batch up to `k` claims it. -/
theorem first_slot_wins (seq : Nat → Slot S) (h0 : ∀ key, R.ledger0.owner key = none) (key : S.Key) :
    ∀ k, (∀ i, (ledgerAt R seq k).owner key = some i ↔
            (i ≤ k ∧ CountedClaim R seq i key ∧ ∀ j, j < i → ¬ CountedClaim R seq j key)) ∧
         ((ledgerAt R seq k).owner key = none ↔ ∀ i, i ≤ k → ¬ CountedClaim R seq i key)
  | 0 => by
    refine ⟨fun i => ?_, ?_⟩
    · simp only [ledgerAt, h0, reduceCtorEq, false_iff]
      intro ⟨hi, hc, _⟩
      exact absurd hc.1 (by omega)
    · simp only [ledgerAt, h0, true_iff]
      intro i hi hc
      exact absurd hc.1 (by omega)
  | k + 1 => by
    obtain ⟨ihs, ihn⟩ := first_slot_wins seq h0 key k
    have hcc : CountedClaim R seq (k + 1) key ↔
        (valid R (ledgerAt R seq k) (k + 1) (seq (k + 1)) && claimsKey (seq (k + 1)) key) = true := by
      simp [CountedClaim, Applied]
    rw [ledgerAt_succ, step_owner]
    cases ho : (ledgerAt R seq k).owner key with
    | some i0 =>
      have hi0 := (ihs i0).1 ho
      refine ⟨fun i => ?_, ?_⟩
      · simp only [Option.some.injEq]
        constructor
        · intro he; subst he
          exact ⟨by omega, hi0.2.1, hi0.2.2⟩
        · intro ⟨hi, hc, hfirst⟩
          by_cases hlt : i0 < i
          · exact absurd hi0.2.1 (hfirst i0 hlt)
          · by_cases hgt : i < i0
            · exact absurd hc (hi0.2.2 i hgt)
            · omega
      · simp only [reduceCtorEq, false_iff]
        intro hall
        exact hall i0 (by omega) hi0.2.1
    | none =>
      have hnone := ihn.1 ho
      by_cases hk : (valid R (ledgerAt R seq k) (k + 1) (seq (k + 1)) && claimsKey (seq (k + 1)) key) = true
      · simp only [hk, ite_true]
        refine ⟨fun i => ?_, ?_⟩
        · simp only [Option.some.injEq]
          constructor
          · intro he; subst he
            exact ⟨Nat.le_refl _, hcc.2 hk, fun j hj => hnone j (by omega)⟩
          · intro ⟨hi, hc, hfirst⟩
            by_cases hle : i ≤ k
            · exact absurd hc (hnone i hle)
            · omega
        · simp only [reduceCtorEq, false_iff]
          intro hall
          exact hall (k + 1) (Nat.le_refl _) (hcc.2 hk)
      · have hk' : (valid R (ledgerAt R seq k) (k + 1) (seq (k + 1)) && claimsKey (seq (k + 1)) key) = false := by
          simpa using hk
        simp only [hk', Bool.false_eq_true, ite_false]
        refine ⟨fun i => ?_, ?_⟩
        · simp only [reduceCtorEq, false_iff]
          intro ⟨hi, hc, _⟩
          by_cases hle : i ≤ k
          · exact hnone i hle hc
          · have : i = k + 1 := by omega
            subst this
            exact hk (hcc.1 hc)
        · simp only [true_iff]
          intro i hi hc
          by_cases hle : i ≤ k
          · exact hnone i hle hc
          · have : i = k + 1 := by omega
            subst this
            exact hk (hcc.1 hc)

/-- An owner, once chosen, never changes. -/
theorem owner_stays (seq : Nat → Slot S) (key : S.Key) {i : Nat} (j : Nat)
    (h : (ledgerAt R seq j).owner key = some i) :
    ∀ k, j ≤ k → (ledgerAt R seq k).owner key = some i
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact h
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact h
      · rw [ledgerAt_succ, step_owner, ih (by omega)]

end CovenSequence
