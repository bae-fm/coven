/-!
# A member's removal never applies while their restore code is offered (§14.1)

When a pass reaches the slot that removes the device's member, it first deletes
the person's saved restore code and S3 key, and applies the slot only if that
succeeded (`follow_person_entries`, then the slot's transaction). From the first
deletion on, the keychain refuses to write the entries again, even when the
deletion failed part way. A device that is not yet at the removal's slot saves
and keeps them as usual. The deletion is tried again by the retried pass, which
reads the same slot.

`removal_applies_only_after_deletion`: in every reachable world, once the removal
is applied the entries are deleted and the keychain refuses to write them.
`removal_applies_after_follow`: a pass at the removal's slot that deletes the
entries then applies it. `apply_first_leaves_code` is the counterexample for
the other order.
-/
namespace CovenSequence.PersonLeaves

/-- The slots applied, the slot that removes the member, whether the saved code
is offered, and whether the keychain refuses to write the person's entries. -/
structure World where
  applied : Nat
  removal : Nat
  offered : Bool
  left : Bool
  deriving DecidableEq

inductive Step : World → World → Prop
  /-- The device saves the restore code (setup, joining, restoring, resetting,
  member-key replacement); the keychain refuses once the person has left. -/
  | save (w : World) (open_ : w.left = false) : Step w { w with offered := true }
  /-- The deletion before the removal's slot fails part way: the keychain now
  refuses writes, the code may still be offered, and nothing is applied. -/
  | followFails (w : World) (atRemoval : w.applied + 1 = w.removal) :
      Step w { w with left := true }
  /-- The deletion succeeds. -/
  | follow (w : World) (atRemoval : w.applied + 1 = w.removal) :
      Step w { w with left := true, offered := false }
  /-- The slot applies. The slot that removes the member applies only after the
  deletion has left the entries deleted. -/
  | commit (w : World)
      (ready : w.applied + 1 = w.removal → w.left = true ∧ w.offered = false) :
      Step w { w with applied := w.applied + 1 }

inductive Steps : World → World → Prop
  | refl (w : World) : Steps w w
  | step {a b c : World} : Step a b → Steps b c → Steps a c

def World.start (removal : Nat) : World := ⟨0, removal, false, false⟩

/-- Once the removal applied, the entries are deleted and stay so. -/
def Inv (w : World) : Prop := w.removal ≤ w.applied → w.left = true ∧ w.offered = false

theorem leaves_step_inv {a b : World} (s : Step a b) (i : Inv a) : Inv b := by
  cases s with
  | save open_ =>
    intro h
    have := i h
    simp [open_] at this ⊢
  | followFails => intro h; exact absurd h (by simp; omega)
  | follow => intro h; exact absurd h (by simp; omega)
  | commit ready =>
    intro h
    by_cases e : a.applied + 1 = a.removal
    · exact ready e
    · exact i (by simp at h ⊢; omega)

theorem leaves_steps_inv {a b : World} (run : Steps a b) (i : Inv a) : Inv b := by
  induction run with
  | refl => exact i
  | step s _ ih => exact ih (leaves_step_inv s i)

theorem inv_start (removal : Nat) (positive : 1 ≤ removal) : Inv (World.start removal) := by
  intro h; simp [World.start] at h; omega

/-- **The removal never applies while the code is offered.** In every reachable
world, once the removal's slot is applied the person's saved entries are deleted
and the keychain refuses to write them. -/
theorem removal_applies_only_after_deletion {removal : Nat} (positive : 1 ≤ removal)
    {w : World} (run : Steps (World.start removal) w) (applied : w.removal ≤ w.applied) :
    w.left = true ∧ w.offered = false :=
  leaves_steps_inv run (inv_start removal positive) applied

/-- **A pass at the removal's slot that deletes the entries then applies it**, from
whatever the failed passes before it left. -/
theorem removal_applies_after_follow {w : World} (at_ : w.applied + 1 = w.removal) :
    Steps w { w with applied := w.removal, left := true, offered := false } := by
  have follow := Step.follow w at_
  have commit := Step.commit { w with left := true, offered := false }
    (fun _ => ⟨rfl, rfl⟩)
  have r := Steps.step follow (Steps.step commit (Steps.refl _))
  obtain ⟨a, r', o, l⟩ := w
  simp only at at_ r ⊢
  subst at_
  exact r

/-- Applying the slot first, and deleting after. -/
def applyFirst (w : World) : World := { w with applied := w.applied + 1 }

/-- **Counterexample: apply, then delete.** A pass that applies the removal and
then fails to delete leaves the code offered with the removal applied, and the
retried pass finds the removal already applied and has no deletion to repeat
(the deletion runs only for the slot that moves the member to removed). -/
theorem apply_first_leaves_code :
    ∃ w : World, w.applied + 1 = w.removal ∧ w.offered = true ∧
      (applyFirst w).removal ≤ (applyFirst w).applied ∧ (applyFirst w).offered = true ∧
      ¬ ((applyFirst w).applied + 1 = (applyFirst w).removal) :=
  ⟨⟨4, 5, true, false⟩, rfl, rfl, by simp [applyFirst], rfl, by simp [applyFirst]⟩

end CovenSequence.PersonLeaves
