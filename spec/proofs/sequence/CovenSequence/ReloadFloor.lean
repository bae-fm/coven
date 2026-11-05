import CovenSequence.ReadPass

/-!
# The floor a reload checks before it commits (§16.4, §16.5)

A reload loads each audience from its start, a snapshot or the device's own
prefix, and reads the kept slots after the starts up to one target. Before it
commits, it reads the floor again. That floor may be newer than any the reload
began from: its writer had applied through its `at`, which can be past the
target, and it names the snapshots announced there.

A floor whose first kept slot has passed the slot after the lowest start
(`first + 1`) deleted slots the reload needs: the reload starts over from it.
Otherwise the floor's own check is `Settle.FloorAnnounced` against the history
the reload holds: through its target, then the kept slots it reads through the
floor's `at`.

Checking against the history through the target alone (`atTarget`) refuses an
honest floor whose `at` is past the target (`atTarget_refuses_honest_floor`).
That is the case of a device added to a circle: it reloads the circle to the
slot that added it, while the floor, written after a newer snapshot of the
circle was announced, names that snapshot. Reading the kept slots through the
floor's `at` first (`install`) never refuses an honest floor
(`honest_floor_installs`).
-/
namespace CovenSequence.ReloadFloor

open ReadPass Settle

/-- What the reload does with the floor it reads before committing. -/
inductive Outcome where
  /-- The floor deleted slots the starts need: load again from it. -/
  | restart
  /-- The floor checks: commit. -/
  | install
  /-- Commits nothing; reports the floor as damaged. -/
  | floorDamaged
  deriving DecidableEq

/-- The order before the fix: the floor is checked against the history the
reload holds through its `target`. `first` is the lowest start's coverage. -/
def atTarget (check : Nat → Bool) (first target : Nat) (f : FloorView) : Outcome :=
  if first + 1 < f.first then .restart
  else if check target then .install
  else .floorDamaged

/-- The reload's floor check: against the history through its `target`, then
the kept slots it reads through the floor's `at`. -/
def install (check : Nat → Bool) (first target : Nat) (f : FloorView) : Outcome :=
  if first + 1 < f.first then .restart
  else if check (max target f.reach) then .install
  else .floorDamaged

/-- A floor whose `at` is at most the target is checked the same way by both. -/
theorem install_eq_atTarget (check : Nat → Bool) (first target : Nat) {f : FloorView}
    (within : f.reach ≤ target) :
    install check first target f = atTarget check first target f := by
  simp [install, atTarget, Nat.max_eq_left within]

section Store
variable {S : Sig} (R : Rules S)

/-- **An honest floor is never refused by a reload.** In storage in good order,
with the floor check that of `Settle.FloorAnnounced` against the history the
reload holds through its target and the kept slots after it, a reload whose
starts reach its target installs or starts over from a floor whose named
snapshots were announced through its `at`. -/
theorem honest_floor_installs {st : Store S} {seq : Nat → Slot S} (g : Good R st seq)
    (check : Nat → Bool) {first target reach : Nat}
    (hcheck : ∀ n, check n = true ↔ FloorAnnounced (heldAt st seq target n) st)
    (hfirst : first ≤ target) (hat : reach ≤ st.head)
    (hnamed : ∀ A, st.floor.covers A = 0 ∨ ∃ a au, 1 ≤ a ∧ a ≤ reach ∧
      seq a = .snapshot au A (st.floor.covers A) (st.floor.path A)) :
    install check first target ⟨st.floor.start, reach⟩ ≠ .floorDamaged := by
  intro damaged
  simp only [install] at damaged
  by_cases hr : first + 1 < st.floor.start
  · simp [hr] at damaged
  · have announced :=
      settle_reads_through_floor R g (through := target) (reach := reach) (by omega) hat hnamed
    simp [hr, (hcheck _).2 announced] at damaged

end Store

/-- **Counterexample: checking at the target refuses an honest floor.** In
`Settle.lostStore`, slot 1 adds member `false` and slot 2 announces `false`'s
snapshot covering it; the floor, honest in the store in good order, names that
announcement, with `at` 2. A reload activated by slot 1, starting from coverage
1 and targeting slot 1, checks the floor against the slots through 1 and
refuses it; reading slot 2 first, it installs. -/
theorem atTarget_refuses_honest_floor (check : Nat → Bool)
    (hcheck : ∀ n, check n = true ↔ FloorAnnounced (heldAt lostStore lostSeq 1 n) lostStore) :
    Good adminRules lostStore lostSeq ∧
    atTarget check 1 1 ⟨lostStore.floor.start, 2⟩ = .floorDamaged ∧
    install check 1 1 ⟨lostStore.floor.start, 2⟩ = .install := by
  obtain ⟨good, _, refused, accepted⟩ := lost_claim_refuses_honest_floor
  have below : check 1 = false := by
    cases hc : check 1
    · rfl
    · exact absurd ((hcheck 1).1 hc) refused
  have through : check 2 = true := (hcheck 2).2 accepted
  refine ⟨good, ?_, ?_⟩
  · simp [atTarget, lostStore, below]
  · simp [install, lostStore, through]

end CovenSequence.ReloadFloor
