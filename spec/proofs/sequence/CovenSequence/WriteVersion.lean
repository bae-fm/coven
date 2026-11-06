import CovenSequence.CircleRaise

/-!
# A part waits for its audience's version

A part counts only at a version its audience has reached (`audValid`'s
`w.version ≤ P.version`). An audience's version never goes down: a raise must
increase it and nothing else changes it (`version_mono`). So a device that sends
a part only once the audience has reached the part's version in the prefix it
has applied sends a part that counts at whatever later slot it lands
(`waiting_part_counts`). One sent earlier is refused: a device that started
reading a circle at version 1 after its app reached version 2 writes parts at
version 2, which the circle refuses until its raise (`ahead_part_refused`).
-/

namespace CovenSequence.WriteVersion

open CovenSequence

section General
variable {S : Sig} (R : Rules S)

/-- One slot never lowers an audience's version. -/
theorem step_version (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud) (x : Slot S) :
    P.version ≤ (audStep R L P k A x).version := by
  unfold audStep
  split
  · split
    · rename_i valid
      cases x with
      | boundary d a kind c p =>
        cases kind with
        | raise v =>
          simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
          simp only [audEffect]
          omega
        | reset => simp [audEffect]
      | writes d g parts =>
        simp only [audEffect]
        split <;> exact Nat.le_refl _
      | _ => simp [audEffect]
    · simp [refuseAud]
  · exact Nat.le_refl _

theorem version_mono (seq : Nat → Slot S) (A : S.Aud) (j : Nat) :
    ∀ k, j ≤ k → (audAt R seq A j).version ≤ (audAt R seq A k).version
  | k, hk => by
    induction k with
    | zero =>
      have : j = 0 := by omega
      subst this; exact Nat.le_refl _
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; exact Nat.le_refl _
      · rw [audAt_succ]
        exact Nat.le_trans (ih (by omega)) (step_version R _ _ _ _ _)

/-- **A part sent once its audience reached its version counts on that
clause wherever it lands.** The device has applied `j` slots, where the
audience is at the part's version or later; the part lands at slot `k`
after them. -/
theorem waiting_part_counts (seq : Nat → Slot S) (A : S.Aud) (j k v : Nat)
    (reached : v ≤ (audAt R seq A j).version) (after : j < k) :
    v ≤ (audAt R seq A (k - 1)).version :=
  Nat.le_trans reached (version_mono R seq A j (k - 1) (by omega))

end General

open ReaderExamples CircleRaise

/-- Slot 1 writes a part into the circle at version 2; `atOne` writes it at
version 1. -/
def ahead : Nat → Slot sig
  | 1 => .writes () 0 (fun _ => some ⟨2, [], 10, 0⟩)
  | _ => .writes () 0 (fun _ => none)

def atOne : Nat → Slot sig
  | 1 => .writes () 0 (fun _ => some ⟨1, [], 10, 0⟩)
  | _ => .writes () 0 (fun _ => none)

/-- **Counterexample: a part ahead of its audience is refused.** A circle at
version 1 refuses a part written at version 2 and counts the same part
written at version 1. A device whose app reached version 2 before it started
reading the circle writes parts at version 2, so sent before the circle's
raise they are refused. -/
theorem ahead_part_refused :
    ¬ AppliedTo baseOne ahead () 1 ∧ AppliedTo baseOne atOne () 1 := by
  unfold AppliedTo
  decide

end CovenSequence.WriteVersion
