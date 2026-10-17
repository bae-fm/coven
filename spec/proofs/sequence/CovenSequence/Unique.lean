import CovenSequence.Cutoff

/-!
# Unique values and owner rows: first slot wins

A part's `claims` are the unique values and owner rows of its audience that it
takes. A key goes to the first counted part that claims it, and keeps that
owner. Owner creation
uses the same rule. The decision reads only slot order, so it is the same on
every device (`prefix_determines_state`), and a long-offline device's late
claim loses to the earlier slot whatever its timestamp.
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- Whether a slot's part for `A` claims `key`. -/
def claimsKey (A : S.Aud) : Slot S → S.Key → Bool
  | .writes _ _ parts, key =>
    match parts A with
    | some w => decide (key ∈ w.claims)
    | none => false
  | _, _ => false

/-- Slot `i` has a counted part for `A` that claims `key`. -/
def CountedClaim (seq : Nat → Slot S) (A : S.Aud) (i : Nat) (key : S.Key) : Prop :=
  1 ≤ i ∧ AppliedTo R seq A i ∧ claimsKey A (seq i) key = true

theorem step_owner (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud) (x : Slot S) (key : S.Key) :
    (audStep R L P k A x).owner key =
      match P.owner key with
      | some i => some i
      | none => if touches A x && audValid R L P k A x && claimsKey A x key then some k else none := by
  unfold audStep
  by_cases ht : touches A x = true
  · by_cases hv : audValid R L P k A x = true
    · simp only [ht, hv, ite_true, Bool.true_and]
      cases x with
      | writes a g parts =>
        simp only [audEffect, claimsKey]
        split <;> split <;> simp_all [claimAll] <;> cases h : P.owner key <;> simp_all
      | _ => simp only [audEffect, claimsKey] <;> cases P.owner key <;> simp
    · have hv' : audValid R L P k A x = false := by simpa using hv
      simp only [ht, hv', ite_true, Bool.true_and, Bool.false_and, refuseAud]
      cases h : P.owner key <;> simp [h]
  · have ht' : touches A x = false := by simpa using ht
    simp only [ht', Bool.false_and, Bool.false_eq_true, ite_false]
    cases P.owner key <;> simp

/-- **First slot wins.** Starting with no owners in audience `A`, after `k`
slots a key's owner is `i` exactly when slot `i ≤ k` is the first counted `A`
part claiming it, and it has none exactly when no counted `A` part up to `k`
claims it. -/
theorem first_slot_wins (seq : Nat → Slot S) (A : S.Aud) (h0 : ∀ key, (R.aud0 A).owner key = none)
    (key : S.Key) :
    ∀ k, (∀ i, (audAt R seq A k).owner key = some i ↔
            (i ≤ k ∧ CountedClaim R seq A i key ∧ ∀ j, j < i → ¬ CountedClaim R seq A j key)) ∧
         ((audAt R seq A k).owner key = none ↔ ∀ i, i ≤ k → ¬ CountedClaim R seq A i key)
  | 0 => by
    refine ⟨fun i => ?_, ?_⟩
    · simp only [audAt, h0, reduceCtorEq, false_iff]
      intro ⟨hi, hc, _⟩
      exact absurd hc.1 (by omega)
    · simp only [audAt, h0, true_iff]
      intro i hi hc
      exact absurd hc.1 (by omega)
  | k + 1 => by
    obtain ⟨ihs, ihn⟩ := first_slot_wins seq A h0 key k
    have hcc : CountedClaim R seq A (k + 1) key ↔
        (touches A (seq (k + 1)) && audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (seq (k + 1)) &&
          claimsKey A (seq (k + 1)) key) = true := by
      simp [CountedClaim, AppliedTo, and_assoc]
    rw [audAt_succ, step_owner]
    cases ho : (audAt R seq A k).owner key with
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
      by_cases hk : (touches A (seq (k + 1)) && audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (seq (k + 1)) &&
          claimsKey A (seq (k + 1)) key) = true
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
      · have hk' : (touches A (seq (k + 1)) && audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (seq (k + 1)) &&
          claimsKey A (seq (k + 1)) key) = false := by
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
theorem owner_stays (seq : Nat → Slot S) (A : S.Aud) (key : S.Key) {i : Nat} (j : Nat)
    (h : (audAt R seq A j).owner key = some i) :
    ∀ k, j ≤ k → (audAt R seq A k).owner key = some i
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact h
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact h
      · rw [audAt_succ, step_owner, ih (by omega)]

end CovenSequence
