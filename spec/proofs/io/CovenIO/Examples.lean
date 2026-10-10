import CovenIO.Retired
import CovenIO.Downloads
import CovenIO.Refinement
import CovenIO.Execution

namespace CovenIO.Examples

/-- Ana's covering snapshot remains while an incomparable snapshot wins
the total-position ordering. Deleting Ana 1 is legal after 30 storage days. -/
def ana : Snapshot := ⟨0, 0, 1, [(0, 1)]⟩
def ben : Snapshot := ⟨0, 1, 1, [(1, 2)]⟩
def anaWrite : Write := ⟨0, 1, 86400, [0]⟩

theorem incomparable_latest :
    ana.score < ben.score ∧ ana.covers anaWrite 0 ∧ ¬ ben.covers anaWrite 0 ∧
    Deletable [ana, ben] [0, 1] (fun _ _ => 0) (31 * 86400) True anaWrite := by
  refine ⟨by decide, by decide, by decide, ?_, trivial, Or.inr (by decide)⟩
  intro a ha
  have eq : a = 0 := by simpa [anaWrite] using ha
  subst a
  exact ⟨ana, by simp, by decide⟩

example : ana.score < ben.score ∧ ¬ ben.covers anaWrite 0 :=
  ⟨incomparable_latest.1, incomparable_latest.2.2.1⟩

theorem latest_selected_incomparable : newest [ana, ben] = some ben ∧ advances ben (fun _ => 0) = true := by decide

theorem deletion_history_reachable : RetainRun ⟨[ana, ben], [anaWrite]⟩ := by
  have b := RetainRun.step RetainRun.initial (RetainStep.publish ⟨[], []⟩ ben)
  have a := RetainRun.step b (RetainStep.publish ⟨[ben], []⟩ ana)
  exact RetainRun.step a (RetainStep.deleteWrite ⟨[ana, ben], []⟩ anaWrite [0, 1]
    (fun _ _ => 0) (31 * 86400) True incomparable_latest.2.2.2)

def deletedAna : Store := erase (put empty (.log .write 0 1) [7] 86400) (.log .write 0 1)

theorem newest_snapshot_misses_deleted_write :
    ben.position 0 = 0 ∧ deletedAna (.log .write 0 (ben.position 0 + 1)) = none ∧
      anaWrite.number > ben.position 0 := by decide

example : Scan (fun _ => deletedAna) .write 0 0 (ben.position 0) 0 0
    [⟨0, .log .write 0 1, none⟩] := .miss 0 0 rfl

def snapshotTwo : Snapshot := ⟨0, 0, 2, [(0, 1)]⟩
def snapshotThree : Snapshot := ⟨0, 0, 3, [(0, 2)]⟩

theorem delete_two_after_three : Dominates snapshotThree snapshotTwo := by
  constructor
  · rfl
  · intro w
    by_cases eq : w = 0
    · subst w; decide
    · simp [Snapshot.position, snapshotTwo, snapshotThree, Ne.symm eq]

def snapshotGap : Store := erase
  (put (put empty (.snapshot 0 0 2) [1] 0) (.snapshot 0 0 3) [2] 1) (.snapshot 0 0 2)

theorem snapshot_next_number_wrong :
    snapshotGap (.snapshot 0 0 2) = none ∧
    snapshotGap (.snapshot 0 0 3) = some ⟨⟨[2], 1⟩, 0⟩ := by decide

example : snapshotGap (.snapshot 0 0 2) = none ∧
    snapshotGap (.snapshot 0 0 3) ≠ none := by decide

theorem away_write_miss :
    Deletable [ana] [0, 1] (fun _ _ => 0) (31 * 86400) True anaWrite ∧
    deletedAna (.log .write 0 1) = none := by
  refine ⟨⟨?_, trivial, Or.inr (by decide)⟩, rfl⟩
  intro a ha
  have eq : a = 0 := by simpa [anaWrite] using ha
  subst a
  exact ⟨ana, by simp, by decide⟩

example : needsSnapshots (some 0) month = true ∧ deletedAna (.log .write 0 1) = none := by decide

/-- Starting a pass at day 29 does not protect a day-1 write at day 31. -/
theorem pass_crosses_deadline :
    29 * 86400 < 0 + month ∧
    Deletable [ana] [0, 1] (fun _ _ => 0) (31 * 86400) True
      { anaWrite with storedAt := 86400 } := by
  refine ⟨by decide, ?_, trivial, Or.inr (by decide)⟩
  intro a ha
  have eq : a = 0 := by simpa [anaWrite] using ha
  subst a
  exact ⟨ana, by simp, by decide⟩

example : 29 * 86400 < month ∧ 86400 + month ≤ 31 * 86400 := by decide

def firstEight : Store := (List.range 8).foldl
  (fun store n => put store (.log .write 0 (n + 1)) [n + 1] 0) empty

def writeNine : Store := put
  (put firstEight (.positions 0) [8] 0) (.log .write 0 9) [9] 1

theorem positions_not_index :
    (writeNine (.positions 0)).map (fun o => o.value.bytes) = some [8] ∧
    (writeNine (.log .write 0 9)).map (fun o => o.value.bytes) = some [9] := by decide

example : observe writeNine [] (.get (.log .write 0 9)) = .object (some ⟨⟨[9], 1⟩, 0⟩) := by decide

/-- Two ordered events may have the same provider timestamp. A new writer
publishes after the folder listing but still has a stored time equal to T. -/
def tiedTime : World := fun event =>
  if event < 2 then empty else put empty (.log .entry 7 1) [7] 10

theorem timestamp_not_frontier :
    tiedTime 1 (.log .entry 7 1) = none ∧
    tiedTime 2 (.log .entry 7 1) = some ⟨⟨[7], 10⟩, 0⟩ ∧ (10 : Nat) ≤ 10 := by decide

example : observe (tiedTime 1) [.log .entry 7 1] (.list .deviceEntries) = .listing [] ∧
    (tiedTime 2 (.log .entry 7 1)).isSome = true := by decide

/-- A permanent check failure is a successful GET but stops later reads. -/
theorem no_request_error_insufficient :
    Downloads.first [.ok (), .error .authentication] = some .authentication ∧
    Downloads.automatic .authentication = false := by decide

example : Downloads.first [.ok (), .error .authentication] ≠ none := by decide

def twoEntries : Store := put (put empty (.log .entry 0 1) [1] 0) (.log .entry 0 2) [2] 1

theorem refused_scan_incomplete :
    (readLog (fun _ => twoEntries) (fun _ => none) (fun _ => true) .entry 0 3 0 0).stop = .refused ∧
    (readLog (fun _ => twoEntries) (fun _ => none) (fun _ => true) .entry 0 3 0 0).trace.length = 1 ∧
    (twoEntries (.log .entry 0 2)).isSome = true := by decide

example : (readLog (fun _ => twoEntries) (fun _ => none) (fun _ => true) .entry 0 3 0 0).position = 0 := by decide

theorem successful_execution :
    (readLog (fun _ => twoEntries) (fun _ => none) (fun _ => false) .entry 0 3 0 0).stop = .miss ∧
    (readLog (fun _ => twoEntries) (fun _ => none) (fun _ => false) .entry 0 3 0 0).position = 2 := by decide

theorem idle_three_devices : (idle (fun _ _ => 1) [0, 1, 2]).length = 11 := by decide

def downloaded : Downloads.State := .unfinished [⟨0, [1, 2]⟩, ⟨1, [3, 4]⟩] .network
def diskFull : Downloads.Attempt :=
  ⟨.file 0 7, .ok (), true, true, false, true, ⟨2, [5, 6]⟩, 3, true, true⟩

theorem disk_failure_keeps_progress :
    Downloads.progress (Downloads.apply downloaded diskFull) = 4 ∧
    Downloads.pending (Downloads.apply downloaded diskFull) = some .disk ∧
    Downloads.pinned (Downloads.apply downloaded diskFull) = false ∧
    Downloads.completed (Downloads.apply downloaded diskFull) = false := by decide

example : Waiting.restart 100 1000 600 = 0 := by decide
example : Waiting.restart 100 (-1000) 600 = 600 := by decide

/-- "No request names a retired device" cannot include file reads or
retention deletes: surviving rows retain the original fixed file path. -/
example : Request.names 7 (.range (.file 7 9) 0 100) = true := by decide

/-- Stopping upon reading replacement and a 30-day request duration do not
alone place that read or the last landing before the finality drain. -/
def lateOldCopy : World := fun event =>
  if event < 32 then empty else put empty (.log .write 0 1) [8] 32

theorem replacement_needs_landing_frontier :
    0 + 30 < 31 ∧ 31 < 32 ∧ 32 ≤ 31 + 30 ∧ ¬ Retired.ClosedAfter lateOldCopy 0 31 := by
  refine ⟨by decide, by decide, by decide, ?_⟩
  intro closed
  have missing := closed 32 (by decide) .write 1 ⟨⟨[8], 32⟩, 0⟩ (by decide)
  contradiction

example : ¬ Retired.ClosedAfter lateOldCopy 0 31 := replacement_needs_landing_frontier.2.2.2

end CovenIO.Examples
