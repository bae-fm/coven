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

The end search (`locateEnd`) finds a slot whose next one is missing. Storage
whose sequence ends at its answer answers every check it makes the same way
(`end_search_sees_an_end`), so a gap at a slot it checked is not known: no slot
after it is known to exist until a later check or a floor's `at` shows one.
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
    (hnamed : ∀ A, st.floor.covers A = 0 ∨ ∃ a au w, 1 ≤ a ∧ a ≤ reach ∧
      seq a = .snapshot au A (st.floor.covers A) w (st.floor.path A)) :
    pass check pos stop why known (some ⟨st.floor.start, reach⟩) ≠ .floorDamaged := by
  intro blamed
  obtain ⟨f, hf, past, failed⟩ := floor_blamed_only_when_held blamed
  cases hf
  have past : reach < stop := past
  have within : ∀ A, st.floor.covers A = 0 ∨ ∃ a au w, 1 ≤ a ∧ a ≤ stop - 1 ∧
      seq a = .snapshot au A (st.floor.covers A) w (st.floor.path A) := by
    intro A
    rcases hnamed A with zero | ⟨a, au, w, h1, h2, hs⟩
    · exact Or.inl zero
    · exact Or.inr ⟨a, au, w, h1, Nat.le_trans h2 (Nat.le_sub_one_of_lt past), hs⟩
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

/-! ## Finding the end

`locateEnd` checks `start + 1`, `start + 2`, `start + 4`, … until one is
missing, then halves the bracket. Checks only test existence. -/

/-- Halve `(low, high)` until the two are adjacent. -/
def narrow (present : Nat → Bool) (low high : Nat) : Nat → Nat
  | 0 => low
  | fuel + 1 =>
    if high ≤ low + 1 then low
    else if present (low + (high - low) / 2) then narrow present (low + (high - low) / 2) high fuel
    else narrow present low (low + (high - low) / 2) fuel

/-- Check `start + step`, doubling `step`, until a check misses. -/
def climb (present : Nat → Bool) (start low step : Nat) : Nat → Nat × Nat
  | 0 => (low, start + step)
  | fuel + 1 =>
    if present (start + step) then climb present start (start + step) (2 * step) fuel
    else (low, start + step)

/-- The end search: 64 doublings and 64 halvings, as for a `u64` slot number. -/
def locateEnd (present : Nat → Bool) (start : Nat) : Nat :=
  narrow present (climb present start start 1 64).1 (climb present start start 1 64).2 64

theorem narrow_bounds (present : Nat → Bool) :
    ∀ fuel low high, low < high →
      low ≤ narrow present low high fuel ∧ narrow present low high fuel < high
  | 0, low, high, h => ⟨Nat.le_refl _, h⟩
  | fuel + 1, low, high, h => by
    simp only [narrow]
    by_cases adjacent : high ≤ low + 1
    · simp only [adjacent, ite_true]; exact ⟨Nat.le_refl _, h⟩
    · simp only [adjacent, ite_false]
      split
      · have := narrow_bounds present fuel (low + (high - low) / 2) high (by omega)
        exact ⟨by omega, by omega⟩
      · have := narrow_bounds present fuel low (low + (high - low) / 2) (by omega)
        exact ⟨by omega, by omega⟩

/-- Halving gives the same answer on any presence that agrees, inside the
bracket, with "at most the answer". -/
theorem narrow_congr (present other : Nat → Bool) :
    ∀ fuel low high, low < high →
      (∀ i, low < i → i < high → (other i = true ↔ i ≤ narrow present low high fuel)) →
      narrow other low high fuel = narrow present low high fuel
  | 0, _, _, _, _ => rfl
  | fuel + 1, low, high, h, agree => by
    by_cases adjacent : high ≤ low + 1
    · simp [narrow, adjacent]
    have lower : low < low + (high - low) / 2 := by omega
    have upper : low + (high - low) / 2 < high := by omega
    cases hp : present (low + (high - low) / 2)
    · have hr : narrow present low high (fuel + 1) =
          narrow present low (low + (high - low) / 2) fuel := by
        simp [narrow, adjacent, hp]
      have bound := (narrow_bounds present fuel low _ lower).2
      have ho : other (low + (high - low) / 2) = false := by
        cases ho : other (low + (high - low) / 2)
        · rfl
        · have := (agree _ lower upper).1 ho
          omega
      rw [hr]
      simp only [narrow, adjacent, ho, ite_false, Bool.false_eq_true]
      exact narrow_congr present other fuel low _ lower fun i h1 h2 => by
        rw [← hr]; exact agree i h1 (by omega)
    · have hr : narrow present low high (fuel + 1) =
          narrow present (low + (high - low) / 2) high fuel := by
        simp [narrow, adjacent, hp]
      have bound := (narrow_bounds present fuel _ high upper).1
      have ho : other (low + (high - low) / 2) = true := (agree _ lower upper).2 (by omega)
      rw [hr]
      simp only [narrow, adjacent, ho, ite_false, ite_true]
      exact narrow_congr present other fuel _ high upper fun i h1 h2 => by
        rw [← hr]; exact agree i (by omega) h2

/-- While `low` precedes the next check, the doubling keeps it so, and its
bracket stays ordered. -/
theorem climb_bounds (present : Nat → Bool) (start : Nat) :
    ∀ fuel low step, 0 < step → low < start + step →
      low ≤ (climb present start low step fuel).1 ∧
        (climb present start low step fuel).1 < (climb present start low step fuel).2
  | 0, low, step, _, h => ⟨Nat.le_refl _, h⟩
  | fuel + 1, low, step, positive, h => by
    simp only [climb]
    split
    · have := climb_bounds present start fuel (start + step) (2 * step) (by omega) (by omega)
      exact ⟨by omega, by omega⟩
    · exact ⟨Nat.le_refl _, h⟩

/-- Doubling gives the same bracket on "at most `r`", for any `r` the bracket holds. -/
theorem climb_congr (present : Nat → Bool) (start r : Nat) :
    ∀ fuel low step, 0 < step → low < start + step →
      (climb present start low step fuel).1 ≤ r → r < (climb present start low step fuel).2 →
      climb (fun i => decide (i ≤ r)) start low step fuel = climb present start low step fuel
  | 0, _, _, _, _, _, _ => rfl
  | fuel + 1, low, step, positive, h, lower, upper => by
    cases hp : present (start + step)
    · have hc : climb present start low step (fuel + 1) = (low, start + step) := by
        simp [climb, hp]
      rw [hc] at upper
      rw [hc]
      simp only at upper
      simp [climb, show ¬ start + step ≤ r by omega]
    · have hc : climb present start low step (fuel + 1) =
          climb present start (start + step) (2 * step) fuel := by
        simp [climb, hp]
      rw [hc] at lower upper
      rw [hc]
      have bound := (climb_bounds present start fuel (start + step) (2 * step) (by omega) (by omega)).1
      simp only [climb, show start + step ≤ r by omega, decide_true, ite_true]
      exact climb_congr present start r fuel (start + step) (2 * step) (by omega) (by omega) lower upper

theorem locateEnd_bounds (present : Nat → Bool) (start : Nat) :
    start ≤ locateEnd present start ∧
      (climb present start start 1 64).1 ≤ locateEnd present start ∧
      locateEnd present start < (climb present start start 1 64).2 := by
  have b := climb_bounds present start 64 start 1 (by omega) (by omega)
  have n := narrow_bounds present 64 _ _ b.2
  exact ⟨by unfold locateEnd; omega, n.1, n.2⟩

/-- **The end search sees an end.** Storage whose sequence ends at the search's
answer answers every check the search makes the same way, so the search
answers the same there. A slot after the answer, past a checked miss, is not
known to exist: a gap at a slot the search checked reads as the end until a
later check or a floor's `at` shows a later slot. -/
theorem end_search_sees_an_end (present : Nat → Bool) (start : Nat) :
    locateEnd (fun i => decide (i ≤ locateEnd present start)) start = locateEnd present start := by
  have b := climb_bounds present start 64 start 1 (by omega) (by omega)
  have e := locateEnd_bounds present start
  have same := climb_congr present start (locateEnd present start) 64 start 1 (by omega) (by omega)
    e.2.1 e.2.2
  unfold locateEnd at same ⊢
  rw [same]
  exact narrow_congr present _ 64 _ _ b.2 fun i _ _ => by simp

/-- Ben applied through 10; Ana's slots run to 20. The gap at 12 is a slot
the search checks: the search answers 11 and the pass reads to the end there.
The gap at 13 is not: the check of 14 finds a later slot, and the pass
reports 13. -/
theorem checked_gap_reads_as_end :
    locateEnd (fun i => decide (i ≤ 20 ∧ i ≠ 12)) 10 = 11 ∧
    pass (fun _ => true) 10 12 .missing (locateEnd (fun i => decide (i ≤ 20 ∧ i ≠ 12)) 10) none =
      .synced 11 ∧
    locateEnd (fun i => decide (i ≤ 20 ∧ i ≠ 13)) 10 = 20 ∧
    pass (fun _ => true) 10 13 .missing (locateEnd (fun i => decide (i ≤ 20 ∧ i ≠ 13)) 10) none =
      .stops 13 := by
  decide

end CovenSequence.ReadPass
