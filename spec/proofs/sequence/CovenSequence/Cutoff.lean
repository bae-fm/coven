import CovenSequence.Converge

/-!
# The removal cut-off

A batch claimed after its device's removal, its member's removal, or the
replacement of the member key it was made under does not count: each of its
parts is consumed with no effect and its refusal is recorded in that part's
audience. The decision reads only the ledger before the slot, so every device
of every audience makes it alike.
-/

namespace CovenSequence

variable {S : Sig} (R : Rules S)

theorem step_deviceOf (L : Ledger S) (k : Nat) (x : Slot S) {d : S.Dev} {m : S.Mem}
    (h : L.deviceOf d = some m) : (ledgerStep R L k x).deviceOf d = some m := by
  unfold ledgerStep
  simp only [noteClaim_deviceOf]
  split
  · rename_i hv
    cases x with
    | admin a e =>
      cases e with
      | addDevice d' m' =>
        simp only [ledgerEffect, applyEntry]
        by_cases hd : d = d'
        · subst hd
          simp [ledgerValid, structural, h] at hv
        · simp [hd, h]
      | _ => simp [ledgerEffect, applyEntry, h]
    | _ => simp [ledgerEffect, h]
  · simp [refuse, h]

theorem step_removed (L : Ledger S) (k : Nat) (x : Slot S) {d : S.Dev}
    (h : L.removed d = true) : (ledgerStep R L k x).removed d = true := by
  unfold ledgerStep
  simp only [noteClaim_removed]
  split
  · cases x with
    | admin a e =>
      cases e with
      | removeDevice d' =>
        simp only [ledgerEffect, applyEntry]
        by_cases hd : d = d' <;> simp [hd, h]
      | _ => simp [ledgerEffect, applyEntry, h]
    | _ => simp [ledgerEffect, h]
  · simp [refuse, h]

theorem step_keyGen (L : Ledger S) (k : Nat) (x : Slot S) (m : S.Mem) :
    L.keyGen m ≤ (ledgerStep R L k x).keyGen m := by
  unfold ledgerStep
  simp only [noteClaim_keyGen]
  split
  · cases x with
    | admin a e =>
      cases e with
      | replaceKey m' =>
        simp only [ledgerEffect, applyEntry]
        by_cases hm : m = m'
        · subst hm; simp
        · simp [hm]
      | _ => simp [ledgerEffect, applyEntry]
    | _ => simp [ledgerEffect]
  · simp [refuse]

theorem deviceOf_stays (seq : Nat → Slot S) {d : S.Dev} {m : S.Mem} (j : Nat)
    (h : (ledgerAt R seq j).deviceOf d = some m) :
    ∀ k, j ≤ k → (ledgerAt R seq k).deviceOf d = some m
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact h
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact h
      · exact step_deviceOf R _ _ _ (ih (by omega))

theorem removed_stays (seq : Nat → Slot S) {d : S.Dev} (j : Nat)
    (h : (ledgerAt R seq j).removed d = true) :
    ∀ k, j ≤ k → (ledgerAt R seq k).removed d = true
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact h
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact h
      · exact step_removed R _ _ _ (ih (by omega))

theorem keyGen_mono (seq : Nat → Slot S) (m : S.Mem) (j : Nat) :
    ∀ k, j ≤ k → (ledgerAt R seq j).keyGen m ≤ (ledgerAt R seq k).keyGen m
  | k, hk => by
    induction k with
    | zero => have : j = 0 := by omega
              subst this; exact Nat.le_refl _
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact Nat.le_refl _
      · exact Nat.le_trans (ih (by omega)) (step_keyGen R _ _ _ m)

/-- An applied slot's ledger is its effect, with its claim number and digest noted. -/
theorem applied_ledger (seq : Nat → Slot S) {j : Nat} (hj : 1 ≤ j) (h : Applied R seq j) :
    ledgerAt R seq j = noteClaim R (ledgerEffect R (ledgerAt R seq (j - 1)) j (seq j)) j (seq j) := by
  have hjeq : j = (j - 1) + 1 := by omega
  unfold Applied at h
  rw [hjeq, ledgerAt_succ, ← hjeq]
  simp [ledgerStep, h]

/-- What cuts off batches by device `d` made under key generation `g`: slot `j`
removes `d`, removes the member `d` was registered to, or replaces that
member's key when `g` was not newer than the replaced key. -/
def Cut (seq : Nat → Slot S) (j : Nat) (d : S.Dev) (g : Nat) : Prop :=
  1 ≤ j ∧ Applied R seq j ∧
    ((∃ a, seq j = .admin a (.removeDevice d)) ∨
     (∃ a m, seq j = .admin a (.removeMember m) ∧ (ledgerAt R seq (j - 1)).deviceOf d = some m) ∨
     (∃ a m, seq j = .admin a (.replaceKey m) ∧ (ledgerAt R seq (j - 1)).deviceOf d = some m ∧
        g ≤ (ledgerAt R seq (j - 1)).keyGen m))

/-- **The removal cut-off.** A batch at slot `k+1` by a device cut off at an
earlier slot `j` is refused in every audience it has a part for: that audience
only records the refusal, so no unique value or owner row is taken, and its
rows are unchanged. The ledger only records the slot's claim number and digest. -/
theorem cutoff (seq : Nat → Slot S) {j k : Nat} (hjk : j ≤ k) {d : S.Dev} {g : Nat}
    {parts : S.Aud → Option (WPart S)} (hw : seq (k + 1) = .writes d g parts) (hcut : Cut R seq j d g)
    (A : S.Aud) {w : WPart S} (hA : parts A = some w) :
    audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (seq (k + 1)) = false ∧
    ledgerAt R seq (k + 1) = noteClaim R (ledgerAt R seq k) (k + 1) (seq (k + 1)) ∧
    audAt R seq A (k + 1) = refuseAud (audAt R seq A k) (k + 1) ∧
    rowsAt R seq A (k + 1) = rowsAt R seq A k := by
  obtain ⟨hj1, happ, hcases⟩ := hcut
  have hcount : counts (ledgerAt R seq k) d g = false := by
    simp only [counts]
    rcases hcases with ⟨a, hs⟩ | ⟨a, m, hs, hdm⟩ | ⟨a, m, hs, hdm, hg⟩
    · have hrj : (ledgerAt R seq j).removed d = true := by
        rw [applied_ledger R seq hj1 happ, hs]
        simp [ledgerEffect, applyEntry]
      have hr := removed_stays R seq j hrj k hjk
      cases hdk : (ledgerAt R seq k).deviceOf d with
      | none => rfl
      | some m => simp [hr]
    · have hrj : (ledgerAt R seq j).removed d = true := by
        rw [applied_ledger R seq hj1 happ, hs]
        simp [ledgerEffect, applyEntry, hdm]
      have hr := removed_stays R seq j hrj k hjk
      cases hdk : (ledgerAt R seq k).deviceOf d with
      | none => rfl
      | some m => simp [hr]
    · have hkj : (ledgerAt R seq j).keyGen m = (ledgerAt R seq (j - 1)).keyGen m + 1 := by
        rw [applied_ledger R seq hj1 happ, hs]
        simp [ledgerEffect, applyEntry]
      have hmono := keyGen_mono R seq m j k hjk
      have hdk := deviceOf_stays R seq (j - 1) hdm k (by omega)
      rw [hdk]
      have : ((ledgerAt R seq k).keyGen m == g) = false := by
        simp only [beq_eq_false_iff_ne]; omega
      simp [this]
  have hinv : audValid R (ledgerAt R seq k) (audAt R seq A k) (k + 1) A (seq (k + 1)) = false := by
    rw [hw]; simp [audValid, hA, hcount]
  have htouch : touches A (seq (k + 1)) = true := by rw [hw]; simp [touches, hA]
  refine ⟨hinv, ?_, ?_, ?_⟩
  · rw [ledgerAt_succ, hw]; simp [ledgerStep, ledgerValid, ledgerEffect]
  · rw [audAt_succ]; simp [audStep, hinv, htouch]
  · have hnr : ∀ d a v c p, seq (k + 1) ≠ .boundary d a v c p := by intro _ _ _ _ _; simp [hw]
    rw [rowsAt_other R seq A k hnr]
    simp [hinv]

end CovenSequence
