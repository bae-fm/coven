import CovenSequence.CircleRaise

/-!
# A part waits for its audience's version

A part counts only at a version its audience has reached (`audValid`'s
`w.version ≤ P.version`). An audience's version never goes down: a raise must
increase it and nothing else changes it (`version_mono`). So a device that sends
a part only once the audience has reached the part's version in the prefix it
has applied sends a part that counts at whatever later slot it lands
(`waiting_part_counts`). In the model one sent earlier doesn't count at its
slot: a part at version 2 in a circle at version 1 is refused there, and the
same part at the circle's version counts (`ahead_part_refused`). Coven's
devices don't refuse such a part, they judge it as a late write, so their raise
body can hold what the model's leaves out (`RaiseBody.ahead_part_differs`). An
audience no one may raise, such as a deleted circle, keeps its version
(`closed_version`), so a part ahead of it never meets the rule however long it
waits (`closed_part_never_counts`).
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

/-- A slot where no boundary of `A` is allowed leaves `A`'s version as it was. -/
theorem closed_step_version (L : Ledger S) (P : AudLedger S) (k : Nat) (A : S.Aud) (x : Slot S)
    (closed : ∀ d kind, R.boundaryAllowed L d A kind = false) :
    (audStep R L P k A x).version = P.version := by
  unfold audStep
  split
  · split
    · rename_i valid
      cases x with
      | boundary d a kind c p =>
        simp only [audValid, closed, Bool.and_false, Bool.false_and] at valid
        exact absurd valid Bool.false_ne_true
      | writes d g parts =>
        simp only [audEffect]
        split <;> rfl
      | _ => simp [audEffect]
    · simp [refuseAud]
  · rfl

/-- **An audience no one may raise keeps its version.** From slot `j` on no
boundary of `A` is allowed, as for a deleted circle (coven.md §15.9), so `A`
stays at its version after slot `j`. -/
theorem closed_version (seq : Nat → Slot S) (A : S.Aud) (j : Nat)
    (closed : ∀ i, j ≤ i → ∀ d kind, R.boundaryAllowed (ledgerAt R seq i) d A kind = false) :
    ∀ k, j ≤ k → (audAt R seq A k).version = (audAt R seq A j).version
  | k, hk => by
    induction k with
    | zero =>
      have : j = 0 := by omega
      subst this; rfl
    | succ k ih =>
      by_cases hj : j = k + 1
      · subst hj; rfl
      · rw [audAt_succ, closed_step_version R _ _ _ _ _ (closed k (by omega))]
        exact ih (by omega)

/-- **A part ahead of an audience no one may raise never meets its version
rule.** Holding it back waits for a version the audience never reaches, and
wherever it lands it is refused alike, so waiting gains the part nothing. -/
theorem closed_part_never_counts (seq : Nat → Slot S) (A : S.Aud) (j k v : Nat)
    (closed : ∀ i, j ≤ i → ∀ d kind, R.boundaryAllowed (ledgerAt R seq i) d A kind = false)
    (ahead : (audAt R seq A j).version < v) (after : j ≤ k) :
    ¬ v ≤ (audAt R seq A k).version := by
  rw [closed_version R seq A j closed k after]
  omega

/-- **A raise that finds its version reached, or its audience closed, never
counts.** The raising operation validates against the `j` slots it has
applied. If `A` has reached `v` there, or no one may raise `A` from `j` on, as
for a deleted circle, a raise of `A` to `v` is refused at whatever later slot
it lands. So that operation adopts the state it applied instead of claiming,
and its preparation is unused (coven.md §15.9, §18.3, §18.8). -/
theorem satisfied_raise_never_counts (seq : Nat → Slot S) (A : S.Aud) (j k v : Nat)
    {d : S.Dev} {c : Nat} {p : S.Path} (slot : seq k = .boundary d A (.raise v) c p)
    (after : j < k)
    (satisfied : v ≤ (audAt R seq A j).version ∨
      ∀ i, j ≤ i → ∀ d kind, R.boundaryAllowed (ledgerAt R seq i) d A kind = false) :
    ¬ AppliedTo R seq A k := by
  intro ⟨_, valid⟩
  rw [slot] at valid
  simp only [audValid, Bool.and_eq_true, decide_eq_true_eq] at valid
  rcases satisfied with reached | closed
  · have := version_mono R seq A j (k - 1) (by omega)
    omega
  · have := closed (k - 1) (by omega) d (.raise v)
    simp_all

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

/-- **Counterexample: a part ahead of its audience doesn't count at its slot.**
A circle at version 1 doesn't count a part written at version 2 and counts the
same part written at version 1. Coven's devices judge such a part as a late
write instead (`RaiseBody.ahead_part_differs`). -/
theorem ahead_part_refused :
    ¬ AppliedTo baseOne ahead () 1 ∧ AppliedTo baseOne atOne () 1 := by
  unfold AppliedTo
  decide

end CovenSequence.WriteVersion
