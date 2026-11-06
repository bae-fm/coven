import CovenSequence.ReloadFloor

/-!
# Checking a floor from the history a device holds (§5.2, §16.4, §16.6)

A device that has applied the slots through `through` reads a floor another
device wrote. The floor names each audience's snapshot by the slot that
announced it, at or before its `at`; checking the floor needs the history
through `at` (`Settle.FloorAnnounced`).

While the floor's first kept slot is at most `through + 1`, the slots after
`through` through `at` are kept: the device reads them into a copy of its
history (`Settle.settle_reads_through_floor`). Once the floor's first kept slot
has passed `through + 1`, the slots after `through` below it may be deleted.
The device starts instead from the history in the floor's store snapshot, which
covers at least `first - 1`, and reads the kept slots after that coverage
through `at`. A settling claim whose slot the floor passed does this (§5.3), as
does a pass that reloads from the floor (§6.1), and so retention settling a lost
reply (§16.6).

`checkFrom` is the slot the history the floor is checked against starts from.
Every slot the check reads is kept (`check_reads_kept`), so in storage in good
order it is present, and an honest floor is accepted (`check_from_floor`).
Reading forward from `through` instead, past a first kept slot beyond
`through + 1`, needs a deleted slot (`history_needs_deleted_slot`).
-/
namespace CovenSequence.FloorCheck

open Settle

/-- The slot through which the history a floor is checked against holds the
sequence before the kept slots are read: the device's own `through`, or, once
the floor's first kept slot has passed `through + 1`, the coverage `covers` of
the floor's store snapshot. -/
def checkFrom (through covers first : Nat) : Nat :=
  if through + 1 < first then covers else through

/-- **The slots a floor check reads are kept.** With the floor's store snapshot
covering at least `first - 1`, every slot after `checkFrom` is at or after the
floor's first kept slot. -/
theorem check_reads_kept {through covers first a : Nat} (hcov : first ≤ covers + 1)
    (after : checkFrom through covers first < a) : first ≤ a := by
  unfold checkFrom at after
  split at after <;> omega

section Store
variable {S : Sig} (R : Rules S)

/-- **A floor check needs no deleted slot and accepts an honest floor.** In
storage in good order, with `store` the audience whose floor snapshot the device
starts from, every slot the check reads, after `checkFrom` through the floor's
`at` (`reach`), is present and genuine, and the history the check holds names
every snapshot the floor names. The history through `checkFrom` is the device's
own, or the floor's store snapshot's, whose body is honest (`Good.floor_body`). -/
theorem check_from_floor {seq : Nat → Slot S} {st : Store S} (g : Good R st seq) (store : S.Aud)
    {through reach : Nat} (hat : reach ≤ st.head)
    (hnamed : ∀ A, st.floor.covers A = 0 ∨ ∃ a au w, 1 ≤ a ∧ a ≤ reach ∧
      seq a = .snapshot au A (st.floor.covers A) w (st.floor.path A)) :
    let start := checkFrom through (st.floor.covers store) st.floor.start
    (∀ a, start < a → a ≤ reach → st.slot a = some (seq a)) ∧
    FloorAnnounced (heldAt st seq start (max start reach)) st := by
  intro start
  have hcov := g.start_le store
  refine ⟨fun a after within => ?_, ?_⟩
  · have kept := check_reads_kept hcov after
    rw [g.kept_genuine a kept, g.first_genuine a (by omega) (by omega)]
  · have hstart : st.floor.start ≤ start + 1 := by
      simp only [start, checkFrom]
      split <;> omega
    exact settle_reads_through_floor R g hstart hat hnamed

end Store

/-! ## Reading forward past a moved floor

Slot 1 adds member `false`; slot 2 announces `false`'s snapshot covering 1, and
slot 3 its snapshot covering 2. The floor names the one at 3, with `at` 3 and
first kept slot 3: slots 1 and 2 are deleted. A device has applied slot 1. -/

open CovenSequence in
def compactSeq : Nat → Slot adminSig
  | 1 => .admin true (.addMember false .member)
  | 2 => .snapshot false () 1 0 ()
  | 3 => .snapshot false () 2 0 ()
  | _ => .admin false (.other ())

open CovenSequence in
/-- Slots 1 and 2 deleted; the floor's first kept slot is 3, its snapshot covers 2. -/
def compactStore : Store adminSig where
  slot := fun i => if i = 3 then some (compactSeq 3) else none
  first := fun i => if 1 ≤ i ∧ i ≤ 3 then some (compactSeq i) else none
  head := 3
  floor := ⟨3, fun _ => 2, fun _ => ()⟩
  body := fun _ => some (stateAt adminRules compactSeq () 2)

open CovenSequence in
theorem compactStore_good : Good adminRules compactStore compactSeq where
  first_genuine i h1 h2 := by
    have h2 : i ≤ 3 := h2
    simp [compactStore, h1, h2]
  first_out i h := by
    have h : i = 0 ∨ 3 < i := h
    rcases h with h | h <;> simp [compactStore] <;> omega
  slot_out i h := by
    have h : i = 0 ∨ 3 < i := h
    rcases h with h | h <;> simp [compactStore] <;> omega
  kept_genuine i h := by
    have h : 3 ≤ i := h
    by_cases h3 : i = 3
    · subst h3; rfl
    · simp [compactStore, h3]; omega
  start_le A := by cases A; decide
  covers_le A := by cases A; decide
  floor_body _ := rfl
  floor_announced A := by
    cases A
    exact Or.inr ⟨3, false, 0, by decide, by decide, rfl, by unfold AppliedTo; exact ⟨by decide, by decide⟩⟩
  boundary_body _ r au v c p h1 h2 hs := by
    have h2 : r ≤ 3 := h2
    have : r = 1 ∨ r = 2 ∨ r = 3 := by omega
    rcases this with rfl | rfl | rfl <;> simp [compactSeq] at hs

/-- **Counterexample: reading forward from the device's history past a moved
floor.** In a store in good order, a device that has applied slot 1 reads the
floor, whose first kept slot 3 has passed slot 2. Checked against the slots it
holds, the floor is refused. Reading forward from slot 1 through the floor's
`at` needs slot 2, which is deleted. The check from the floor's store snapshot
starts after its coverage, 2, reads only kept slot 3, and accepts the floor. -/
theorem history_needs_deleted_slot :
    Good adminRules compactStore compactSeq ∧ 1 + 1 < compactStore.floor.start ∧
    compactStore.slot 2 = none ∧
    ¬ FloorAnnounced (heldAt compactStore compactSeq 1 1) compactStore ∧
    checkFrom 1 (compactStore.floor.covers ()) compactStore.floor.start = 2 ∧
    compactStore.slot 3 = some (compactSeq 3) ∧
    FloorAnnounced (heldAt compactStore compactSeq 2 (max 2 3)) compactStore := by
  refine ⟨compactStore_good, by decide, rfl, ?_, by decide, rfl, ?_⟩
  · refine settle_held_refuses_floor () (by decide) fun a au w h => ?_
    revert h
    match a with
    | 0 => simp [compactSeq]
    | 1 => simp [compactSeq]
    | 2 => simp [compactSeq]
    | n + 3 => intro _; omega
  · exact (check_from_floor adminRules compactStore_good () (through := 1) (reach := 3)
      (by decide) fun A => by cases A; exact Or.inr ⟨3, false, 0, by decide, by decide, rfl⟩).2

end CovenSequence.FloorCheck
