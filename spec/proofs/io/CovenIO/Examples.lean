import CovenIO.Retired
import CovenIO.Downloads
import CovenIO.Refinement
import CovenIO.Execution

namespace CovenIO.Examples

def ana : Snapshot := ⟨0, 0, 1, month, [(0, 1)]⟩
def ben : Snapshot := ⟨0, 1, 1, month + 1, [(1, 1)]⟩
def anaWrite : Write := ⟨0, 1, 0, [0]⟩
def only (snap : Snapshot) : Current := fun a => if a = snap.audience then some snap else none

theorem incomparable_latest : ¬ CanSnapshot (only ana) ben := by
  intro allowed
  have bound := (allowed ana rfl).2 0
  change 1 ≤ 0 at bound
  omega

theorem latest_selected_incomparable : newest [ana, ben] = some ben ∧
    advances ben (fun _ => 0) = true ∧ ¬ ben.covers anaWrite 0 := by decide

theorem incomparable_deletion_impossible : ¬ RetainRun ⟨only ben, [anaWrite]⟩ := by
  intro run
  obtain ⟨snap, hs, hc⟩ := deleted_write_covered run anaWrite (by simp) 0 (by simp [anaWrite])
  have eq : snap = ben := Option.some.inj hs.symm
  subst snap
  exact latest_selected_incomparable.2.2 hc

/-- Two writers check an empty audience before either upload lands. Ana's
snapshot lands, permits deletion, then Ben's pending snapshot becomes current.
All checks at send and deletion succeed; publication is not serialized. -/
def concurrentSnapshots : SnapshotPublication.State :=
  ⟨⟨publishCurrent (publishCurrent (fun _ => none) ana) ben, [anaWrite]⟩, []⟩

theorem concurrent_snapshot_history : SnapshotPublication.Run concurrentSnapshots := by
  let initial : SnapshotPublication.State := ⟨⟨fun _ => none, []⟩, []⟩
  let a : SnapshotPublication.State := ⟨initial.retention, [ana]⟩
  let both : SnapshotPublication.State := ⟨initial.retention, [ben, ana]⟩
  let first : SnapshotPublication.State := ⟨⟨publishCurrent (fun _ => none) ana, []⟩, [ben]⟩
  let deleted : SnapshotPublication.State := ⟨⟨first.retention.current, [anaWrite]⟩, [ben]⟩
  have ha : SnapshotPublication.Run a := .step .initial (.prepare initial ana (by intro old h; cases h))
  have hb : SnapshotPublication.Run both := .step ha (.prepare a ben (by intro old h; cases h))
  have landed : SnapshotPublication.Run first := by
    simpa [both, first, a, initial, ana, ben] using
      SnapshotPublication.Run.step hb (.land both ana month (by simp [both]))
  have deletion : Deletable first.retention.current [0, 1] (fun _ _ => 0) month True anaWrite := by
    refine ⟨?_, trivial, Or.inr (by simp [anaWrite])⟩
    intro audience ha
    have eq : audience = 0 := by simpa [anaWrite] using ha
    subst audience
    exact ⟨ana, rfl, by decide⟩
  have hd : SnapshotPublication.Run deleted := .step landed
    (.deleteWrite first anaWrite [0, 1] (fun _ _ => 0) month True deletion)
  simpa [concurrentSnapshots, deleted, first, ben] using
    SnapshotPublication.Run.step hd (.land deleted ben (month + 1) (by simp [deleted]))

theorem concurrent_snapshot_loses_deleted_write : ¬ concurrentSnapshots.retention.Valid := by
  intro valid
  obtain ⟨snap, hs, hc⟩ := valid anaWrite (by simp [concurrentSnapshots]) 0 (by simp [anaWrite])
  have eq : snap = ben := by
    simpa [concurrentSnapshots, publishCurrent, newer, ana, ben] using hs.symm
  subst snap
  exact latest_selected_incomparable.2.2 hc

example : ∃ s, SnapshotPublication.Run s ∧ ¬ s.retention.Valid :=
  ⟨concurrentSnapshots, concurrent_snapshot_history, concurrent_snapshot_loses_deleted_write⟩

def deletedAna : Store := erase (put empty (.log .write 0 1) [7] 0) (.log .write 0 1)

theorem newest_snapshot_misses_deleted_write :
    ben.position 0 = 0 ∧ deletedAna (.log .write 0 (ben.position 0 + 1)) = none ∧
      anaWrite.number > ben.position 0 := by decide

def snapshotTwo : Snapshot := ⟨0, 0, 2, 0, [(0, 1)]⟩
def snapshotThree : Snapshot := ⟨0, 0, 3, 1, [(0, 2)]⟩

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

/-- Even serialized growing snapshots do not preserve a reader's earlier
selection. Write 2 can disappear after it selected the snapshot through 1. -/
theorem loaded_snapshot_can_become_stale :
    ∃ state, RetainRun state ∧ { anaWrite with number := 2 } ∈ state.deleted ∧
      Covered state.current { anaWrite with number := 2 } ∧
      ¬ snapshotTwo.covers { anaWrite with number := 2 } 0 := by
  let initial : Retention := ⟨fun _ => none, []⟩
  let first : Retention := ⟨publishCurrent initial.current snapshotTwo, []⟩
  let second : Retention := ⟨publishCurrent first.current snapshotThree, []⟩
  let write : Write := { anaWrite with number := 2 }
  have hfirst : RetainRun first := .step .initial (.publish initial snapshotTwo (by intro old h; cases h))
  have hsecond : RetainRun second := .step hfirst (.publish first snapshotThree (by
    intro old found
    have eq : old = snapshotTwo := Option.some.inj found.symm
    subst old
    exact delete_two_after_three))
  have cover : Covered second.current write := by
    intro audience ha
    have eq : audience = 0 := by simpa [write, anaWrite] using ha
    subst audience
    exact ⟨snapshotThree, rfl, by decide⟩
  let final : Retention := ⟨second.current, [write]⟩
  have run : RetainRun final := .step hsecond (.deleteWrite second write [1] (fun _ _ => 0)
    month True ⟨cover, trivial, Or.inr (by simp [write, anaWrite])⟩)
  exact ⟨final, run, by simp [final, write], cover, by decide⟩

theorem away_write_miss :
    Deletable (only ana) [0, 1] (fun _ _ => 0) month True anaWrite ∧
    deletedAna (.log .write 0 1) = none := by
  refine ⟨⟨?_, trivial, Or.inr (by simp [anaWrite])⟩, rfl⟩
  intro a ha
  have eq : a = 0 := by simpa [anaWrite] using ha
  subst a
  exact ⟨ana, rfl, by decide⟩

/-- At day 31 the terminal miss is diverted to snapshot discovery even
though the caller began its pass with an earlier sample. -/
theorem pass_crosses_deadline :
    (readRecentLog (fun _ => deletedAna) (fun _ => none) (fun _ => false)
      0 1 0 0 (some 0) (28 * day) (fun _ => 3 * day)).stop = .snapshots ∧
    needsSnapshots (some 0) (29 * day) 0 = true := by decide

/-- A nondecreasing provider clock can cross retention while elapsed time
still accepts a miss. The write was published after checkpoint 0 at time 1. -/
def beforeClockJump : Provider := ⟨put empty (.log .write 0 1) [7] 1, 1⟩
def afterClockJump : Provider :=
  ⟨erase beforeClockJump.objects (.log .write 0 1), month + 1⟩

theorem clock_jump_defeats_recent_miss :
    needsSnapshots (some 0) 1 0 = false ∧
    Deletable (only { ana with storedAt := 1 }) [0, 1] (fun _ _ => 0)
      (month + 1) True { anaWrite with storedAt := 1 } ∧
    ProviderRun beforeClockJump afterClockJump ∧
    (readRecentLog (fun _ => afterClockJump.objects) (fun _ => none) (fun _ => false)
      0 1 0 0 (some 0) 1 (fun _ => 0)).stop = .miss := by
  refine ⟨by decide, ?_, ?_, by decide⟩
  · refine ⟨?_, trivial, Or.inr (by decide)⟩
    intro a ha
    have eq : a = 0 := by simpa [anaWrite] using ha
    subst a
    exact ⟨{ ana with storedAt := 1 }, rfl, by decide⟩
  · exact .step (.step (.refl _) (.tick beforeClockJump (month + 1) (by decide)))
      (.delete { beforeClockJump with time := month + 1 } (.log .write 0 1) rfl)

def firstEight : Store := (List.range 8).foldl
  (fun store n => put store (.log .write 0 (n + 1)) [n + 1] 0) empty

def writeNine : Store := put
  (put firstEight (.positions 0) [8] 0) (.log .write 0 9) [9] 1

theorem positions_not_index :
    (writeNine (.positions 0)).map (fun o => o.value.bytes) = some [8] ∧
    (writeNine (.log .write 0 9)).map (fun o => o.value.bytes) = some [9] := by decide

example : observe writeNine [] (.get (.log .write 0 9)) = .object (some ⟨⟨[9], 1⟩, 0⟩) := by decide

/-- A later entry in the clock's tick is outside the preceding-unit cutoff. -/
def tiedTime : World := fun event =>
  if event < 2 then empty else put empty (.log .entry 7 1) [7] 10

theorem timestamp_not_frontier :
    tiedTime 1 (.log .entry 7 1) = none ∧
    tiedTime 2 (.log .entry 7 1) = some ⟨⟨[7], 10⟩, 0⟩ ∧
    ¬ (10 : Nat) ≤ observationTime 10 1 := by decide

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
  if event < 32 then empty else put empty (.log .write 0 1) [8] (32 * day)

theorem replacement_needs_landing_frontier :
    0 + month < 31 * day ∧ 31 * day < 32 * day ∧ 32 * day ≤ 31 * day + month ∧
    ¬ Retired.ClosedAfter lateOldCopy 0 31 := by
  refine ⟨by decide, by decide, by decide, ?_⟩
  intro closed
  have missing := closed 32 (by decide) .write 1 ⟨⟨[8], 32 * day⟩, 0⟩ (by decide)
  contradiction

example : ¬ Retired.ClosedAfter lateOldCopy 0 31 := replacement_needs_landing_frontier.2.2.2

def removal : Retired.Retirement := ⟨4, 0, [0]⟩

theorem retired_write_boundary_and_restore :
    Retired.writeAllowed [removal] (fun _ => false) { anaWrite with storedAt := month } = true ∧
    Retired.writeAllowed [removal] (fun _ => false) { anaWrite with storedAt := month + 1 } = false ∧
    Retired.writeAllowed [] (fun _ => false) { anaWrite with storedAt := month + 1 } = true ∧
    Retired.writeAllowed [removal] (fun _ => true) anaWrite = false := by decide

theorem fresh_gate_boundaries :
    permitted (some ⟨0, 1, .send, false⟩) 299 = true ∧
    permitted (some ⟨0, 1, .send, false⟩) 300 = false ∧
    permitted (some ⟨0, 300, .send, false⟩) 300 = false ∧
    permitted none 0 = false := by decide

/-- A request sent on the last integer second permitted by a pre-finality
catch-up may take a full day; it still lands before the drain deadline. -/
def lastSend : Retired.Publication 1 :=
  ⟨⟨0, 0, .send, false⟩, 299, 299 + day, by decide, by decide, by decide,
    by intro h; simp at h⟩

theorem last_send_settles : lastSend.landed < 1 + freshFor + day :=
  Retired.publication_before_deadline 1 lastSend

/-- Eager file downloads remain a pass step; only uploads moved out. -/
theorem pass_can_download_file :
    IOTrace ⟨none, none⟩ [.request .pass 0 (.range (.file 7 9) 0 100)] ⟨none, none⟩ :=
  .cons rfl (.nil _)

/-- Retries, parts and completion all expire with the catch-up start. -/
theorem upload_parts_use_gate :
    gateStep ⟨none, some ⟨0, 1, .send, false⟩⟩
      (.request .upload 299 (.uploadPart 0 1 [7])) = some ⟨none, some ⟨0, 1, .send, false⟩⟩ ∧
    gateStep ⟨none, some ⟨0, 1, .send, false⟩⟩
      (.request .upload 300 (.finishUpload 0 1)) = none := by decide

/-- Starting another catch-up does not discard still-fresh completed evidence
or block the independent upload worker while that catch-up is pending. -/
theorem upload_uses_last_completed_catchup :
    IOTrace ⟨none, some ⟨0, 1, .send, false⟩⟩
      [.beginPass 2, .request .upload 3 (.uploadPart 0 1 [7])]
      ⟨some 2, some ⟨0, 1, .send, false⟩⟩ :=
  .cons rfl (.cons rfl (.nil _))

end CovenIO.Examples
