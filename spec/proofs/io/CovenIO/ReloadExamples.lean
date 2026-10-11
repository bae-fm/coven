import CovenIO.Reload
import CovenIO.Examples

namespace CovenIO.ReloadExamples
open SnapshotPublication Examples

theorem fresh_selection_recovers (order : SnapshotOrder) :
    (readLoadingLog (fun _ => snapshotStore afterSelection) (fun _ => afterSelection.landed)
      (fun _ => none) (fun _ => false) 1 1 0 (rightSnapshot.position 1)).stop = .snapshots ∧
    current order afterSelection 0 = some joinedSnapshot ∧ joinedSnapshot.covers otherWrite 0 := by
  cases order <;> decide

theorem stale_selection_recovers (order : SnapshotOrder) :
    (readLoadingLog (fun _ => snapshotStore missedState) (fun _ => missedState.landed)
      (fun _ => none) (fun _ => false) 0 1 0 (loadedSnapshot.position 0)).stop = .snapshots ∧
    current order missedState 0 = some coveringSnapshot ∧ coveringSnapshot.covers laterWrite 0 := by
  cases order <;> decide

/-- A catalog from before the missing GET cannot implement the miss rule. -/
theorem cached_catalog_misses_deletion :
    (readLoadingLog (fun _ => snapshotStore missedState) (fun _ => loadedState.landed)
      (fun _ => none) (fun _ => false) 0 1 0 1).stop = .miss := by decide

/-- Two writes and two concurrently prepared snapshots suffice. The smaller
path wins both count/time ties and also has the later storage time. -/
theorem scheduled_retention_can_stall (order : SnapshotOrder) (delay : Nat) :
    let s := { beforeSelection with time := month + delay }
    Run order [0, 1, 2] s ∧
    leftSnapshot.counts = true ∧ leftSnapshot.covers otherWrite 0 ∧
    (∀ reader ∈ s.writers, otherWrite.number ≤ postedPosition [(0, 1, 1), (1, 1, 1), (2, 1, 1)] reader 1) ∧
    otherWrite ∉ (retainPass order s [(0, 1, 1), (1, 1, 1), (2, 1, 1)] true).deleted ∧
    (retainPass order s [(0, 1, 1), (1, 1, 1), (2, 1, 1)] true).landed = s.landed := by
  dsimp only
  refine ⟨⟨beforeSelectionEvents ++ [(month + delay, .tick)], ?_⟩,
    by decide, by decide, ?_, ?_, rfl⟩
  · rw [run_append, (fresh_selection_race order).1]
    simp only [Option.bind_some, execute]
    rw [tick_preserves_deleted order beforeSelection (month + delay)
      (by dsimp [beforeSelection]; omega)]
    rfl
  · simp [beforeSelection, otherWrite, postedPosition]
  · apply uncovered_pass_keeps order { beforeSelection with time := month + delay }
      [(0, 1, 1), (1, 1, 1), (2, 1, 1)] true otherWrite (by change otherWrite ∉ beforeSelection.deleted; decide)
    change ¬ Covered (catalogCurrent order beforeSelection.landed) otherWrite
    cases order <;> decide

theorem no_order_repairs_coverage (order : SnapshotOrder) :
    eligible order beforeSelection otherWrite [(0, 1, 1), (1, 1, 1), (2, 1, 1)] true = false := by
  cases order <;> decide

def quietPasses (order : SnapshotOrder) : Nat → State
  | 0 => beforeSelection
  | n + 1 => retainPass order { quietPasses order n with time := month + n + 1 }
      [(0, 1, 1), (1, 1, 1), (2, 1, 1)] true

theorem every_pass_retains_uncovered (order : SnapshotOrder) (n : Nat) :
    (quietPasses order n).time = month + n ∧
    (quietPasses order n).landed = beforeSelection.landed ∧
    (quietPasses order n).writes = beforeSelection.writes ∧ otherWrite ∉ (quietPasses order n).deleted := by
  induction n with
  | zero => exact ⟨by simp [quietPasses, beforeSelection], rfl, rfl, by change otherWrite ∉ beforeSelection.deleted; decide⟩
  | succ n ih =>
      refine ⟨by simp [quietPasses, retainPass, retainWrites, Nat.add_assoc], ih.2.1, ih.2.2.1, ?_⟩
      apply uncovered_pass_keeps order { quietPasses order n with time := month + n + 1 }
        [(0, 1, 1), (1, 1, 1), (2, 1, 1)] true otherWrite ih.2.2.2
      change ¬ Covered (catalogCurrent order (quietPasses order n).landed) otherWrite
      rw [ih.2.1]
      cases order <;> decide

/-- Separate audiences can have a deleted prefix hole before a later write
for the audience being loaded. No snapshot for that audience sees the hole. -/
def crossAudienceEvents : List (Nat × Action) :=
  [(0, .write 0 [1]), (0, .beginCatchup 1 0 1), (0, .read 0 0),
    (0, .read 0 0), (0, .prepare 0), (0, .land 1 0 1),
    (month, .delete 0 1 [] true), (month, .write 0 [0])]

def crossAudienceState : State :=
  ⟨month, [0], [⟨0, 2, month, [0]⟩, ⟨0, 1, 0, [1]⟩], [⟨0, 1, 0, [1]⟩],
    [⟨1, 0, 1, 0, 0, [(0, 1)]⟩], [], []⟩

theorem other_audience_hole (order : SnapshotOrder) :
    execute order (initial [0]) crossAudienceEvents = some crossAudienceState ∧
    (readLoadingLog (fun _ => snapshotStore crossAudienceState) (fun _ => crossAudienceState.landed)
      (fun _ => none) (fun _ => false) 0 2 0 0).stop = .snapshots ∧
    current order crossAudienceState 0 = none ∧ live crossAudienceState 0 2 = some ⟨0, 2, month, [0]⟩ := by
  cases order <;> decide

/-- Even a fresh catalog for every audience cannot supply a store snapshot
that has never been published. Retrying this catch-up repeats the same miss. -/
theorem other_audience_reload_stuck (order : SnapshotOrder) :
    execute order crossAudienceState [(month, .beginCatchup 0 0 1), (month, .read 0 0)] = none := by
  cases order <;> decide

/-- Once the deleted number is certified irrelevant to the store audience,
advancing past it lets the actual reader receive the later store write. -/
theorem other_audience_skip_reaches_tail :
    let result := readLoadingLog (fun _ => snapshotStore crossAudienceState)
      (fun _ => crossAudienceState.landed) (fun _ => none) (fun _ => false) 0 2 0 1
    result.stop = .miss ∧ result.position = 2 := by decide

end CovenIO.ReloadExamples
