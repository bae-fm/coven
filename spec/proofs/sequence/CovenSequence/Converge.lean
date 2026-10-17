import CovenSequence.Model

/-!
# Convergence

A device's state for an audience is a function of the prefix of slots it
applied: any two devices that applied the same slots `1..k` have the same
ledger, audience ledger and rows. Readers that follow storage reach exactly
this state (`read_sound`), so devices agree on every prefix they share.
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

theorem rows_prefix (seq₁ seq₂ : Nat → Slot S) (A : S.Aud) :
    ∀ k, (∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) → rowsAt R seq₁ A k = rowsAt R seq₂ A k := by
  intro k
  induction k using Nat.strongRecOn with
  | ind k ih =>
    intro h
    cases k with
    | zero => rw [rowsAt_zero, rowsAt_zero]
    | succ k =>
      have hl := ledger_prefix R seq₁ seq₂ k (fun i h1 h2 => h i h1 (by omega))
      have ha := aud_prefix R seq₁ seq₂ A k (fun i h1 h2 => h i h1 (by omega))
      have hs := h (k + 1) (by omega) (Nat.le_refl _)
      have hk := ih k (by omega) (fun i h1 h2 => h i h1 (by omega))
      cases hx : seq₁ (k + 1) with
      | raise d a v c p =>
        have hx₂ : seq₂ (k + 1) = .raise d a v c p := hs ▸ hx
        rw [rowsAt_raise R seq₁ A k hx, rowsAt_raise R seq₂ A k hx₂, hl, ha, hk]
        split
        · rename_i hv
          have hc : c < k + 1 := by have := hv.2; simp [audValid] at this; omega
          rw [ih c hc (fun i h1 h2 => h i h1 (by omega))]
          exact replay_congr R A seq₁ seq₂ _ v _ _ _ (fun i h1 h2 => h i (by omega) (by omega))
        · rfl
      | _ =>
        have hnr : ∀ d a v c p, seq₁ (k + 1) ≠ .raise d a v c p := by intro _ _ _ _ _; simp [hx]
        have hnr₂ : ∀ d a v c p, seq₂ (k + 1) ≠ .raise d a v c p := hs ▸ hnr
        rw [rowsAt_other R seq₁ A k hnr, rowsAt_other R seq₂ A k hnr₂, hl, ha, hk, hs]

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
