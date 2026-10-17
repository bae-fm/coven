import CovenSequence.Model

/-!
# Convergence

A device's state is a function of the prefix of slots it applied: any two
devices that applied the same slots `1..k` have the same ledger and rows.
Readers that follow storage reach exactly this state (`Reader.read_sound`), so
devices agree on every prefix they share.
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

/-- The ledger after `k` slots depends only on those slots. -/
theorem ledger_prefix (seq₁ seq₂ : Nat → Slot S) :
    ∀ k, (∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) → ledgerAt R seq₁ k = ledgerAt R seq₂ k
  | 0, _ => rfl
  | k + 1, h => by
    rw [ledgerAt_succ, ledgerAt_succ, h (k + 1) (by omega) (Nat.le_refl _),
      ledger_prefix seq₁ seq₂ k (fun i h1 h2 => h i h1 (by omega))]

/-- **Convergence.** The state after `k` slots depends only on those slots. -/
theorem prefix_determines_state (seq₁ seq₂ : Nat → Slot S) (k : Nat)
    (h : ∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) : stateAt R seq₁ k = stateAt R seq₂ k := by
  have hrows : ∀ k, (∀ i, 1 ≤ i → i ≤ k → seq₁ i = seq₂ i) → rowsAt R seq₁ k = rowsAt R seq₂ k := by
    intro k
    induction k using Nat.strongRecOn with
    | ind k ih =>
      intro h
      cases k with
      | zero => rw [rowsAt_zero, rowsAt_zero]
      | succ k =>
        have hl := ledger_prefix R seq₁ seq₂ k (fun i h1 h2 => h i h1 (by omega))
        have hs := h (k + 1) (by omega) (Nat.le_refl _)
        have hk := ih k (by omega) (fun i h1 h2 => h i h1 (by omega))
        cases hx : seq₁ (k + 1) with
        | raise d v c p =>
          have hx₂ : seq₂ (k + 1) = .raise d v c p := hs ▸ hx
          rw [rowsAt_raise R seq₁ k hx, rowsAt_raise R seq₂ k hx₂, hl, hk]
          by_cases hv : valid R (ledgerAt R seq₂ k) (k + 1) (.raise d v c p) = true
          · have hc : c < k + 1 := by simp [valid] at hv; omega
            simp only [hv, ite_true]
            rw [ih c hc (fun i h1 h2 => h i h1 (by omega))]
            exact replay_congr R seq₁ seq₂ _ v _ _ _ (fun i h1 h2 => h i (by omega) (by omega))
          · simp [hv]
        | _ =>
          have hnr : ∀ d v c p, seq₁ (k + 1) ≠ .raise d v c p := by intro d v c p; simp [hx]
          have hnr₂ : ∀ d v c p, seq₂ (k + 1) ≠ .raise d v c p := hs ▸ hnr
          rw [rowsAt_other R seq₁ k hnr, rowsAt_other R seq₂ k hnr₂, hl, hk, hs]
  exact Prod.ext (ledger_prefix R seq₁ seq₂ k h) (hrows k h)

/-- Whether slot `n` is valid depends only on the slots before it, so an
operation that validated against slots `1..n-1` validated exactly what every
device checks when it applies slot `n`. -/
theorem valid_prefix (seq₁ seq₂ : Nat → Slot S) (n : Nat)
    (h : ∀ i, 1 ≤ i → i < n → seq₁ i = seq₂ i) (x : Slot S) :
    valid R (ledgerAt R seq₁ (n - 1)) n x = valid R (ledgerAt R seq₂ (n - 1)) n x := by
  rw [ledger_prefix R seq₁ seq₂ (n - 1) (fun i h1 h2 => h i h1 (by omega))]

end CovenSequence
