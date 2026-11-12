import CovenSequence.Model

/-!
# Convergence

A device's state for an audience is a function of the prefix of slots it
applied: any two devices that applied the same slots `1..k` have the same
ledger, audience ledger and rows. Readers that follow storage reach exactly
this state (`read_ok`), so devices agree on every prefix they share.
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

theorem ledger_prefix (seq₁ seq₂ : Nat → Slot S) :
    ∀ k, (∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) → ledgerAt R seq₁ k = ledgerAt R seq₂ k
  | 0, _ => rfl
  | k + 1, h => by
    rw [ledgerAt_succ, ledgerAt_succ, h (k + 1) (by omega) (Nat.le_refl _),
      ledger_prefix seq₁ seq₂ k (fun i h1 h2 => h i h1 (by omega))]

theorem aud_prefix (seq₁ seq₂ : Nat → Slot S) (A : S.Aud) :
    ∀ k, (∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) → audAt R seq₁ A k = audAt R seq₂ A k
  | 0, _ => rfl
  | k + 1, h => by
    rw [audAt_succ, audAt_succ, h (k + 1) (by omega) (Nat.le_refl _),
      ledger_prefix R seq₁ seq₂ k (fun i h1 h2 => h i h1 (by omega)),
      aud_prefix seq₁ seq₂ A k (fun i h1 h2 => h i h1 (by omega))]

theorem storeBefore_prefix (seq₁ seq₂ : Nat → Slot S) (v : Nat) :
    ∀ c, (∀ i, 1 ≤ i → i ≤ c → seq₁ i = seq₂ i) → storeBefore R seq₁ v c = storeBefore R seq₂ v c
  | 0, _ => rfl
  | c + 1, h => by
    have := storeBefore_prefix seq₁ seq₂ v c (fun i h1 h2 => h i h1 (by omega))
    simp only [storeBefore, this,
      aud_prefix R seq₁ seq₂ R.store (c + 1) h]

theorem inputSlot_prefix (seq₁ seq₂ : Nat → Slot S) (A : S.Aud) (c : Nat)
    (h : ∀ i, 1 ≤ i → i ≤ c → seq₁ i = seq₂ i) (B : S.Aud) :
    inputSlot R seq₁ A c B = inputSlot R seq₂ A c B := by
  simp only [inputSlot, aud_prefix R seq₁ seq₂ A c h, storeBefore_prefix R seq₁ seq₂ _ c h]

theorem rows_prefix_all (seq₁ seq₂ : Nat → Slot S) :
    ∀ k A, (∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) → rowsAt R seq₁ A k = rowsAt R seq₂ A k := by
  intro k
  induction k using Nat.strongRecOn with
  | ind k ih =>
    intro A h
    cases k with
    | zero => rw [rowsAt_zero, rowsAt_zero]
    | succ k =>
      have hl := ledger_prefix R seq₁ seq₂ k (fun i h1 h2 => h i h1 (by omega))
      have ha := aud_prefix R seq₁ seq₂ A k (fun i h1 h2 => h i h1 (by omega))
      have hs := h (k + 1) (by omega) (Nat.le_refl _)
      have hk := ih k (by omega) A (fun i h1 h2 => h i h1 (by omega))
      cases hx : seq₁ (k + 1) with
      | boundary d a v c p =>
        have hx₂ : seq₂ (k + 1) = .boundary d a v c p := hs ▸ hx
        rw [rowsAt_boundary R seq₁ A k hx, rowsAt_boundary R seq₂ A k hx₂, hl, ha, hk]
        split
        · rename_i hv
          have hc : c < k + 1 := by have := hv.2; simp [audValid] at this; omega
          have hc' : ∀ i, 1 ≤ i → i ≤ c → seq₁ i = seq₂ i := fun i h1 h2 => h i h1 (by omega)
          have view : raiseView R seq₁ A c = raiseView R seq₂ A c := by
            funext B
            unfold raiseView
            rw [inputSlot_prefix R seq₁ seq₂ A c hc' B]
            exact ih _ (Nat.lt_of_le_of_lt (inputSlot_le R seq₂ A c B) hc) B
              (fun i h1 h2 => h i h1 (by have := inputSlot_le R seq₂ A c B; omega))
          rw [view]
          exact finishBoundary_congr R A seq₁ seq₂ _ v _ _ _ _ (fun i h1 h2 => h i (by omega) (by omega))
        · rfl
      | _ =>
        have hnr : ∀ d a v c p, seq₁ (k + 1) ≠ .boundary d a v c p := by intro _ _ _ _ _; simp [hx]
        have hnr₂ : ∀ d a v c p, seq₂ (k + 1) ≠ .boundary d a v c p := hs ▸ hnr
        rw [rowsAt_other R seq₁ A k hnr, rowsAt_other R seq₂ A k hnr₂, hl, ha, hk, hs]

theorem rows_prefix (seq₁ seq₂ : Nat → Slot S) (A : S.Aud) (k : Nat)
    (h : ∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) : rowsAt R seq₁ A k = rowsAt R seq₂ A k :=
  rows_prefix_all R seq₁ seq₂ k A h

/-- **Convergence.** For every audience, the state after `k` slots depends
only on those slots. -/
theorem prefix_determines_state (seq₁ seq₂ : Nat → Slot S) (A : S.Aud) (k : Nat)
    (h : ∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) : stateAt R seq₁ A k = stateAt R seq₂ A k := by
  simp only [stateAt, ledger_prefix R seq₁ seq₂ k h, aud_prefix R seq₁ seq₂ A k h,
    rows_prefix R seq₁ seq₂ A k h]

/-- Whether slot `n` is valid depends only on the slots before it, so an
operation that validated against slots `1..n-1` validated exactly what every
device checks when it applies slot `n`. -/
theorem valid_prefix (seq₁ seq₂ : Nat → Slot S) (n : Nat)
    (h : ∀ i, 1 ≤ i → i < n → seq₁ i = seq₂ i) (x : Slot S) (A : S.Aud) :
    ledgerValid R (ledgerAt R seq₁ (n - 1)) x = ledgerValid R (ledgerAt R seq₂ (n - 1)) x ∧
    audValid R (ledgerAt R seq₁ (n - 1)) (audAt R seq₁ A (n - 1)) n A x =
      audValid R (ledgerAt R seq₂ (n - 1)) (audAt R seq₂ A (n - 1)) n A x := by
  have h' : ∀ i, 1 ≤ i → i ≤ n - 1 → seq₁ i = seq₂ i := fun i h1 h2 => h i h1 (by omega)
  rw [ledger_prefix R seq₁ seq₂ (n - 1) h', aud_prefix R seq₁ seq₂ A (n - 1) h']
  exact ⟨rfl, rfl⟩

end CovenSequence
