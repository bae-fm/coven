import CovenSequence.Settle

/-!
# What a pass's read forward reports (§6.1, §6.2)

A pass reads `pos + 1`, `pos + 2`, … until a slot is missing or can't be read
or applied: the slot `stop`. It then reads the floor. The floor's first kept
slot gates the commit: a floor past `pos + 1` sends the device to reload.

The floor's own check needs the history through its `at` (`reach` here): each
snapshot it names is found at the slot that announced it
(`Settle.FloorAnnounced`). `check n` is that check against the history the
device holds through `n`. Applying a slot needs only the floor's first kept
slot (`ReadStep.apply`): the slots before a stop at or below the floor's `at`
apply, and the floor's own check waits for a pass that reads through its `at`.

A missing slot is damage when a later slot is known to exist: one an existence
check found (`known`), or one through the floor's `at`, which the floor's
writer had applied.

Checking the floor before reporting the stop (`checkFirst`) blames an honest
floor for a gap below its `at`: the history through `stop - 1` lacks the
announcement the floor names past the gap (`checkFirst_blames_honest_floor`).
Reporting a stop at or below the floor's `at` first (`pass`) reports the gap
(`gap_report_wins`), and blames a floor only once the device holds the history
through its `at` (`floor_blamed_only_when_held`), which an honest floor passes
(`honest_floor_never_blamed`).
-/
namespace CovenSequence.ReadPass

/-- What the floor read after the slots shows. -/
structure FloorView where
  /-- Its first kept slot. -/
  first : Nat
  /-- The slot its writer had applied through: its `at`. -/
  reach : Nat
  deriving DecidableEq

/-- Why reading stopped at a slot. -/
inductive Stop where
  /-- The provider answered "not found". -/
  | missing
  /-- The read failed, or the slot couldn't be opened or applied. -/
  | fault
  deriving DecidableEq

inductive Outcome where
  /-- The floor has passed the next slot: the device reloads. -/
  | reload
  /-- Applied through `through`, which is the end the device knows. -/
  | synced (through : Nat)
  /-- Applied through `slot - 1`; reports `slot` and stops there. -/
  | stops (slot : Nat)
  /-- Applies nothing; reports the floor as damaged. -/
  | floorDamaged
  deriving DecidableEq

/-- What a stop reports: a fault always; a missing slot when a later slot is
known to exist, through `known`; otherwise the end. -/
def report (stop : Nat) (why : Stop) (known : Nat) : Outcome :=
  match why with
  | .fault => .stops stop
  | .missing => if stop ≤ known then .stops stop else .synced (stop - 1)

theorem report_ne_floorDamaged (stop : Nat) (why : Stop) (known : Nat) :
    report stop why known ≠ .floorDamaged := by
  cases why <;> simp only [report] <;> (try split) <;> simp

/-- The order before the fix: the floor is checked against the history held
through `stop - 1` before the stop is reported. -/
def checkFirst (check : Nat → Bool) (pos stop : Nat) (why : Stop) (known : Nat) :
    Option FloorView → Outcome
  | none => report stop why known
  | some f =>
    if pos + 1 < f.first then .reload
    else if check (stop - 1) then report stop why (max known f.reach)
    else .floorDamaged

/-- A pass's read forward. A stop at or below the floor's `at` leaves the
floor unchecked, since the history through `at` can't be read, and is reported
itself. Otherwise the floor is checked against the history through
`stop - 1`, which then holds its `at`. -/
def pass (check : Nat → Bool) (pos stop : Nat) (why : Stop) (known : Nat) :
    Option FloorView → Outcome
  | none => report stop why known
  | some f =>
    if pos + 1 < f.first then .reload
    else if stop ≤ f.reach then .stops stop
    else if check (stop - 1) then report stop why known
    else .floorDamaged

/-- **The gap's report wins.** Reading that stops at or below the floor's `at`
applies the slots before the stop and reports the stop, whatever the check
against the shorter history says. -/
theorem gap_report_wins (check : Nat → Bool) {pos stop known : Nat} (why : Stop)
    {f : FloorView} (first : f.first ≤ pos + 1) (inside : stop ≤ f.reach) :
    pass check pos stop why known (some f) = .stops stop := by
  simp [pass, show ¬ pos + 1 < f.first by omega, inside]

/-- The floor is blamed only after the device holds the history through its
`at` and the check against that history fails. -/
theorem floor_blamed_only_when_held {check : Nat → Bool} {pos stop known : Nat} {why : Stop}
    {floor : Option FloorView} (blamed : pass check pos stop why known floor = .floorDamaged) :
    ∃ f, floor = some f ∧ f.reach < stop ∧ check (stop - 1) = false := by
  cases floor with
  | none => exact absurd blamed (report_ne_floorDamaged stop why known)
  | some f =>
    simp only [pass] at blamed
    by_cases hr : pos + 1 < f.first
    · simp [hr] at blamed
    by_cases hin : stop ≤ f.reach
    · simp [hr, hin] at blamed
    cases hc : check (stop - 1)
    · exact ⟨f, rfl, by omega, rfl⟩
    · simp only [hr, hin, hc, ite_false, ite_true] at blamed
      exact absurd blamed (report_ne_floorDamaged stop why known)

/-- A stop past the floor's `at`, or no floor, reads as it did before the fix. -/
theorem pass_eq_checkFirst (check : Nat → Bool) (pos stop : Nat) (why : Stop) (known : Nat)
    {floor : Option FloorView} (past : ∀ f, floor = some f → f.reach < stop) :
    pass check pos stop why known floor = checkFirst check pos stop why known floor := by
  cases floor with
  | none => rfl
  | some f =>
    have hp := past f rfl
    simp only [pass, checkFirst, show ¬ stop ≤ f.reach by omega, ite_false]
    have same : report stop why known = report stop why (max known f.reach) := by
      cases why with
      | fault => rfl
      | missing =>
        simp only [report]
        by_cases hk : stop ≤ known
        · simp [hk, show stop ≤ max known f.reach by omega]
        · simp [hk, show ¬ stop ≤ max known f.reach by omega]
    rw [same]

/-! ## In the storage model -/

section Store
variable {S : Sig} (R : Rules S)
open Settle

/-- **A gap below the floor's `at` is damage.** In storage in good order, every
slot from the first kept slot through the head is present, and the floor's
`at` is at most the head. So "not found" there means storage lost the slot. -/
theorem gap_below_at_is_damage {st : Store S} {seq : Nat → Slot S} (g : Good R st seq)
    {stop reach : Nat} (kept : st.floor.start ≤ stop) (positive : 1 ≤ stop)
    (inside : stop ≤ reach) (applied : reach ≤ st.head) :
    st.slot stop = some (seq stop) := by
  rw [g.kept_genuine stop kept, g.first_genuine stop positive (by omega)]

/-- **An honest floor is never blamed.** With the floor check that of
`Settle.FloorAnnounced` against the slots the device holds, storage in good
order, and a floor whose named snapshots were announced through its `at`, the
pass never reports the floor as damaged. -/
theorem honest_floor_never_blamed {st : Store S} {seq : Nat → Slot S} (g : Good R st seq)
    (check : Nat → Bool) {pos stop known reach : Nat} (why : Stop)
    (hcheck : ∀ n, check n = true ↔ FloorAnnounced (heldAt st seq pos n) st)
    (hstart : st.floor.start ≤ pos + 1) (hstop : pos < stop) (held : stop - 1 ≤ st.head)
    (hnamed : ∀ A, st.floor.covers A = 0 ∨ ∃ a au, 1 ≤ a ∧ a ≤ reach ∧
      seq a = .snapshot au A (st.floor.covers A) (st.floor.path A)) :
    pass check pos stop why known (some ⟨st.floor.start, reach⟩) ≠ .floorDamaged := by
  intro blamed
  obtain ⟨f, hf, past, failed⟩ := floor_blamed_only_when_held blamed
  cases hf
  have past : reach < stop := past
  have within : ∀ A, st.floor.covers A = 0 ∨ ∃ a au, 1 ≤ a ∧ a ≤ stop - 1 ∧
      seq a = .snapshot au A (st.floor.covers A) (st.floor.path A) := by
    intro A
    rcases hnamed A with zero | ⟨a, au, h1, h2, hs⟩
    · exact Or.inl zero
    · exact Or.inr ⟨a, au, h1, Nat.le_trans h2 (Nat.le_sub_one_of_lt past), hs⟩
  have announced := settle_reads_through_floor R g (through := pos) (reach := stop - 1) hstart held within
  rw [Nat.max_eq_right (by omega : pos ≤ stop - 1)] at announced
  rw [(hcheck (stop - 1)).2 announced] at failed
  cases failed

end Store

/-! ## The counterexample: a gap below an honest floor's `at`

`Settle.lostStore` with slot 2 lost. Device `true` has applied slot 1. Slot 2,
missing now, held device `false`'s announcement of the snapshot the floor
names; the floor was written after it, with `at` 2. -/

open Settle in
/-- `lostStore`, with slot 2 gone. Its floor is the one the store in good order holds. -/
def gapStore : Store adminSig := { lostStore with slot := fun i => if i = 1 then some (lostSeq 1) else none }

open Settle in
/-- **Counterexample: checking the floor first blames an honest floor.**
Reading from slot 1, the device misses slot 2, below the floor's `at`. The floor
is honest: it is the floor of `lostStore`, which is in good order. Checked
against the slots through 1, it is refused, and the pass reports the floor
instead of the gap. Reporting the stop first reports slot 2. -/
theorem checkFirst_blames_honest_floor (check : Nat → Bool)
    (hcheck : ∀ n, check n = true ↔ FloorAnnounced (heldAt gapStore lostSeq 1 n) gapStore) :
    Good adminRules lostStore lostSeq ∧ gapStore.floor = lostStore.floor ∧
    gapStore.slot 2 = none ∧ lostStore.slot 2 = some (lostSeq 2) ∧
    checkFirst check 1 2 .missing 1 (some ⟨gapStore.floor.start, 2⟩) = .floorDamaged ∧
    pass check 1 2 .missing 1 (some ⟨gapStore.floor.start, 2⟩) = .stops 2 := by
  have held : heldAt gapStore lostSeq 1 1 = heldAt lostStore lostSeq 1 1 := by
    funext a
    unfold heldAt
    by_cases ha : a ≤ 1 <;> simp [ha]
  have refused : check 1 = false := by
    cases hc : check 1
    · rfl
    · have announced := (hcheck 1).1 hc
      rw [held] at announced
      exact absurd announced lost_claim_refuses_honest_floor.2.2.1
  refine ⟨lostStore_good, rfl, rfl, rfl, ?_, ?_⟩
  · simp [checkFirst, gapStore, lostStore, refused]
  · exact gap_report_wins check .missing (by decide) (by decide)

end CovenSequence.ReadPass
