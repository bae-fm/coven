import CovenIO.Retired
import CovenIO.Downloads
import CovenIO.Refinement
import CovenIO.Execution

namespace CovenIO.Examples

open SnapshotPublication

def ana : Snapshot := ⟨0, 0, 1, 0, 0, [(0, 1)]⟩
def ben : Snapshot := ⟨0, 1, 1, 1, 0, [(1, 2)]⟩
def anaWrite : Write := ⟨0, 1, 0, [0]⟩
def only (snap : Snapshot) : Current := fun a => if a = snap.audience then some snap else none

theorem incomparable_latest : ¬ CanSnapshot (only ana) ben := by
  intro allowed
  have bound := (allowed ana rfl).2 0
  change 1 ≤ 0 at bound
  omega

theorem latest_selected_incomparable : newest .writes [ana, ben] = some ben ∧
    advances ben (fun _ => 0) = true ∧ ¬ ben.covers anaWrite 0 := by decide

/-- Each terminal miss observes one writer. Concurrent scans can still
produce incomparable snapshots without an error or unresolved input. -/
def incomparableEvents : List (Nat × Action) :=
  [(0, .beginCatchup 0 0 1), (0, .beginCatchup 0 1 1),
   (0, .read 0 1), (0, .read 1 0),
   (0, .write 0 [0]), (0, .read 0 0), (0, .read 0 0), (0, .prepare 0),
   (0, .write 1 [0]), (0, .write 1 [0]),
   (0, .read 1 1), (0, .read 1 1), (0, .read 1 1), (0, .prepare 1),
   (0, .land 0 0 1), (1, .land 0 1 1)]

def incomparableState : State :=
  ⟨1, [0, 1], [⟨1, 2, 0, [0]⟩, ⟨1, 1, 0, [0]⟩, anaWrite], [], [ben, ana], [], []⟩

theorem incomparable_history : execute .writes (initial [0, 1]) incomparableEvents =
    some incomparableState := by decide

theorem incomparable_deletion_impossible :
    eligible .writes { incomparableState with time := month } anaWrite
      [(0, 0, 1), (1, 0, 1)] true = false := by decide

def deletedAna : Store := erase (put empty (.log .write 0 1) [7] 0) (.log .write 0 1)

def snapshotTwo : Snapshot := ⟨0, 0, 2, 0, 0, [(0, 1)]⟩
def snapshotThree : Snapshot := ⟨0, 0, 3, 1, 1, [(0, 2)]⟩

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

/-- Load coverage through write 1 at zero. Publish write 2 and its snapshot
at second 1. Thirty days later, age permits deletion despite the waiting reader. -/
def loadedSnapshot : Snapshot := ⟨0, 0, 1, 0, 0, [(0, 1)]⟩
def coveringSnapshot : Snapshot := ⟨0, 0, 2, 1, 1, [(0, 2)]⟩
def laterWrite : Write := ⟨0, 2, 1, [0]⟩
def loadedState : State := ⟨0, [0, 1], [anaWrite], [], [loadedSnapshot], [], []⟩
def missedState : State :=
  ⟨month + 1, [0, 1], [laterWrite, anaWrite], [laterWrite],
    [coveringSnapshot, loadedSnapshot], [], []⟩

def loadEvents : List (Nat × Action) :=
  [(0, .write 0 [0]), (0, .beginCatchup 0 0 1), (0, .read 0 0),
   (0, .read 0 0), (0, .read 0 1), (0, .prepare 0), (0, .land 0 0 1)]

def readerRaceEvents : List (Nat × Action) :=
  [(1, .write 0 [0]), (1, .beginCatchup 0 0 2),
   (1, .read 0 0), (1, .read 0 0), (1, .read 0 1), (1, .prepare 0), (1, .land 0 0 2),
   (month + 1, .delete 0 2 [] true)]

theorem loaded_snapshot_can_become_stale (order : SnapshotOrder) :
    execute order (initial [0, 1]) loadEvents = some loadedState ∧
    current order loadedState 0 = some loadedSnapshot ∧
    execute order loadedState readerRaceEvents = some missedState ∧
    current order missedState 0 = some coveringSnapshot ∧
    ¬ loadedSnapshot.covers laterWrite 0 := by
  cases order <;> decide

theorem loaded_snapshot_advances : advances loadedSnapshot (fun _ => 0) = true := by decide

/-- Execute the actual next-number reader against the history's final store. -/
theorem newest_snapshot_misses_deleted_write :
    (readLog (fun _ => snapshotStore missedState) (fun _ => none) (fun _ => false)
      .write 0 1 0 (loadedSnapshot.position 0)).stop = .miss ∧
    (readLog (fun _ => snapshotStore missedState) (fun _ => none) (fun _ => false)
      .write 0 1 0 (loadedSnapshot.position 0)).position < laterWrite.number ∧
    laterWrite ∈ missedState.deleted := by decide

example : ∃ s, Run .writes [0, 1] s ∧ laterWrite ∈ s.deleted ∧
    snapshotStore s (.log .write 0 (loadedSnapshot.position 0 + 1)) = none := by
  refine ⟨missedState, ⟨loadEvents ++ readerRaceEvents, ?_⟩, by decide, by decide⟩
  rw [run_append, (loaded_snapshot_can_become_stale .writes).1]
  exact (loaded_snapshot_can_become_stale .writes).2.2.1

/-- At the covering snapshot's deletion deadline, a competing upload made
from its preceding catch-up is already too late to count. -/
def oldCompetitor : Snapshot := ⟨0, 1, 1, day + 1, 0, [(1, 2)]⟩

theorem in_flight_race_excluded :
    eligible .writes { incomparableState with time := day, landed := [ana] }
      anaWrite [(0, 0, 1), (1, 0, 1)] true = false ∧
    catalogCurrent .writes [oldCompetitor, ana] 0 = some ana := by decide

/-- Local abandonment does not imply remote cancellation. -/
def lateEvents : List (Nat × Action) :=
  [(0, .beginCatchup 0 1 1), (0, .read 1 0), (0, .read 1 1), (0, .prepare 1)]

theorem late_upload_lands_but_does_not_count :
    ∃ s, execute .storage (initial [0, 1])
      (lateEvents ++ loadEvents ++ readerRaceEvents ++ [(month + 2, .land 0 1 1)]) = some s ∧
      current .storage s 0 = some coveringSnapshot ∧
      s.landed.head? = some ⟨0, 1, 1, month + 2, 0, []⟩ := by
  refine ⟨⟨month + 2, [0, 1], [laterWrite, anaWrite], [laterWrite],
    [⟨0, 1, 1, month + 2, 0, []⟩, coveringSnapshot, loadedSnapshot], [], []⟩,
    by decide, by decide, by decide⟩

/-- Two writes and three snapshots suffice without a delayed reader. The
first two snapshots have incomparable coverage; the smaller path wins their
write-count and catch-up-time ties. The losing snapshot's age can later
release its write as soon as a fresh snapshot covering both lands. -/
def leftSnapshot : Snapshot := ⟨0, 1, 1, 1, 0, [(1, 1)]⟩
def rightSnapshot : Snapshot := ⟨0, 0, 1, 2, 0, [(0, 1)]⟩
def joinedSnapshot : Snapshot := ⟨0, 1, 2, month, month, [(1, 1), (0, 1)]⟩
def otherWrite : Write := ⟨1, 1, 0, [0]⟩
def beforeSelection : State :=
  ⟨month, [0, 1, 2], [anaWrite, otherWrite], [], [rightSnapshot, leftSnapshot], [], []⟩
def afterSelection : State :=
  ⟨month, [0, 1, 2], [anaWrite, otherWrite], [otherWrite],
    [joinedSnapshot, rightSnapshot, leftSnapshot], [], []⟩

def beforeSelectionEvents : List (Nat × Action) :=
  [(0, .beginCatchup 0 1 1), (0, .beginCatchup 0 0 1),
   (0, .read 1 0), (0, .read 0 1), (0, .read 1 2), (0, .read 0 2),
   (0, .write 1 [0]), (0, .read 1 1), (0, .read 1 1), (0, .prepare 1),
   (0, .write 0 [0]), (0, .read 0 0), (0, .read 0 0), (0, .prepare 0),
   (1, .land 0 1 1), (2, .land 0 0 1), (month, .tick)]

def afterSelectionEvents : List (Nat × Action) :=
  [(month, .beginCatchup 0 1 2), (month, .read 1 1), (month, .read 1 1),
   (month, .read 1 0), (month, .read 1 2), (month, .prepare 1), (month, .land 0 1 2),
   (month, .delete 1 1 [] true)]

theorem fresh_selection_race (order : SnapshotOrder) :
    execute order (initial [0, 1, 2]) beforeSelectionEvents = some beforeSelection ∧
    current order beforeSelection 0 = some rightSnapshot ∧
    execute order beforeSelection afterSelectionEvents = some afterSelection ∧
    current order afterSelection 0 = some joinedSnapshot ∧
    ¬ rightSnapshot.covers otherWrite 0 ∧
    snapshotStore afterSelection (.log .write 1 (rightSnapshot.position 1 + 1)) = none := by
  cases order <;> decide

theorem fresh_selection_reader_misses :
    advances rightSnapshot (fun _ => 0) = true ∧
    (readLog (fun _ => snapshotStore afterSelection) (fun _ => none) (fun _ => false)
      .write 1 1 0 (rightSnapshot.position 1)).stop = .miss ∧
    otherWrite ∈ afterSelection.deleted := by decide

example : Run .writes [0, 1, 2] afterSelection := by
  refine ⟨beforeSelectionEvents ++ afterSelectionEvents, ?_⟩
  rw [run_append, (fresh_selection_race .writes).1]
  exact (fresh_selection_race .writes).2.2.1

/-- Both incomparable writes cannot be released using only those two
snapshots: the single current snapshot fails one of the coverage checks. -/
theorem symmetric_deletions_excluded (order : SnapshotOrder) :
    !(eligible order beforeSelection anaWrite [] true &&
      eligible order beforeSelection otherWrite [] true) = true := by
  cases order <;> decide

/-- Neither a positive minimum age nor continual snapshot production is
a scheduling obligation to execute DELETE. Any proposed finite deadline
can pass with an eligible object still present. -/
def cleanupReady : State :=
  ⟨day + 1, [0], [anaWrite], [], [ana], [], []⟩

def cleanupReadyEvents : List (Nat × Action) :=
  [(0, .write 0 [0]), (0, .beginCatchup 0 0 1), (0, .read 0 0), (0, .read 0 0),
   (0, .prepare 0), (0, .land 0 0 1), (day + 1, .tick)]

theorem cleanup_ready_reachable (order : SnapshotOrder) :
    execute order (initial [0]) cleanupReadyEvents = some cleanupReady ∧
    eligible order cleanupReady anaWrite [(0, 0, 1)] true = true := by
  cases order <;> decide

theorem no_cleanup_deadline (order : SnapshotOrder) (delay : Nat) :
    ∃ s, Run order [0] s ∧ day + delay < s.time ∧
      live s 0 1 = some anaWrite ∧ s.deleted = [] := by
  let s := { cleanupReady with time := day + 1 + delay }
  refine ⟨s, ⟨cleanupReadyEvents ++ [(day + 1 + delay, .tick)], ?_⟩,
    by dsimp [s]; omega, rfl, rfl⟩
  rw [run_append, (cleanup_ready_reachable order).1]
  simp only [Option.bind_some, execute]
  rw [tick_preserves_deleted order cleanupReady _ (by simp [cleanupReady])]
  rfl

/-- A producer publishes another write and a fresh snapshot each second.
The rule permits this history to omit every eligible deletion. -/
def busySnapshot (n : Nat) : Snapshot := ⟨0, 0, n + 1, n, n, [(0, n + 1)]⟩

def busyWrites : Nat → List Write
  | 0 => [anaWrite]
  | n + 1 => ⟨0, n + 2, n + 1, [0]⟩ :: busyWrites n

def busySnapshots : Nat → List Snapshot
  | 0 => [ana]
  | n + 1 => busySnapshot (n + 1) :: busySnapshots n

def busyState (n : Nat) : State :=
  ⟨n, [0], busyWrites n, [], busySnapshots n, [], []⟩

theorem busy_catalog (n : Nat) :
    (busySnapshots n).filter (fun s => s.audience == 0 && s.counts) = busySnapshots n ∧
    newest .writes (busySnapshots n) = some (busySnapshot n) := by
  induction n with
  | zero => decide
  | succ n ih =>
      constructor
      · simp only [busySnapshots, List.filter_cons]
        rw [ih.1]
        simp [busySnapshot, Snapshot.counts]
      · simp [busySnapshots, newest, ih.2, newer, Snapshot.rank, Snapshot.writeCount, busySnapshot]

theorem busy_write_facts (n : Nat) :
    (busyWrites n).length = n + 1 ∧
    (busyWrites n).all (fun w => w.writer == 0) = true ∧
    ∀ k, n + 1 < k → (busyWrites n).find? (fun w => w.writer == 0 && w.number == k) = none := by
  induction n with
  | zero => simp [busyWrites, anaWrite]; omega
  | succ n ih =>
      refine ⟨by simp [busyWrites, ih.1], by simp [busyWrites, ih.2.1], ?_⟩
      intro k hk
      simp [busyWrites, show n + 2 ≠ k by omega, ih.2.2 k (by omega)]

theorem busy_unused_path (n number : Nat) (later : n + 1 < number) :
    (busySnapshots n).any (fun s => s.audience == 0 && s.writer == 0 && s.number == number) = false := by
  induction n with
  | zero => simp [busySnapshots, ana, show 1 ≠ number by omega]
  | succ n ih => simp [busySnapshots, busySnapshot, show n + 1 + 1 ≠ number by omega, ih (by omega)]

def busyEvents (n : Nat) : List (Nat × Action) :=
  [(n + 1, .write 0 [0]), (n + 1, .beginCatchup 0 0 (n + 2)),
   (n + 1, .read 0 0), (n + 1, .read 0 0),
   (n + 1, .prepare 0), (n + 1, .land 0 0 (n + 2))]

theorem busy_step (n : Nat) :
    execute .writes (busyState n) (busyEvents n) = some (busyState (n + 1)) := by
  have filterWrites : (busyWrites n).filter (fun w => w.writer == 0) = busyWrites n :=
    List.filter_eq_self.mpr (by simpa using (busy_write_facts n).2.1)
  have catalog := busy_catalog n
  have unused := busy_unused_path n (n + 2) (by omega)
  have unusedReverse : ¬ ∃ s, s ∈ busySnapshots n ∧
      (0 = s.audience ∧ 0 = s.writer) ∧ n + 2 = s.number := by
    rintro ⟨snap, member, ⟨audience, writer⟩, number⟩
    have found : (busySnapshots n).any (fun s =>
        s.audience == 0 && s.writer == 0 && s.number == n + 2) = true :=
      List.any_eq_true.mpr ⟨snap, member, by simp [← audience, ← writer, ← number]⟩
    rw [unused] at found
    cases found
  have absent := (busy_write_facts n).2.2 (n + 3) (by omega)
  simp [busyEvents, busyState, execute, step, act, live, current, catalogCurrent,
    filterWrites, (busy_write_facts n).1, catalog.1, catalog.2, samePath, unusedReverse,
    Snapshot.position, busySnapshot, canPrepare, dominates, absent, busyWrites, busySnapshots, Nat.add_assoc]

theorem busy_run (n : Nat) : Run .writes [0] (busyState n) := by
  induction n with
  | zero =>
      exact ⟨[(0, .write 0 [0]), (0, .beginCatchup 0 0 1), (0, .read 0 0),
        (0, .read 0 0), (0, .prepare 0), (0, .land 0 0 1)], by decide⟩
  | succ n ih =>
      obtain ⟨events, run⟩ := ih
      refine ⟨events ++ busyEvents n, ?_⟩
      rw [run_append, run]
      exact busy_step n

theorem busy_never_deletes (n : Nat) : (busyState n).deleted = [] := rfl

theorem busy_retains_first (n : Nat) :
    ana ∈ busySnapshots n ∧ anaWrite ∈ busyWrites n := by
  induction n with
  | zero => simp [busySnapshots, busyWrites]
  | succ n ih => exact ⟨List.mem_cons_of_mem _ ih.1, List.mem_cons_of_mem _ ih.2⟩

theorem busy_eligible (n : Nat) (aged : day < n) :
    eligible .writes (busyState n) anaWrite [(0, 0, 1)] true = true := by
  have catalog := busy_catalog n
  simp only [eligible, decide_eq_true_eq, Eligible, Deletable]
  refine ⟨⟨?_, trivial, Or.inl ?_⟩, ?_⟩
  · simp [Covered, busyState, catalogCurrent, catalog.1, catalog.2,
      Snapshot.covers, Snapshot.position, busySnapshot, anaWrite]
  · simp [busyState, anaWrite, postedPosition]
  · intro audience member
    have eq : audience = 0 := by simpa [anaWrite] using member
    subst audience
    exact ⟨ana, (busy_retains_first n).1, by decide, by decide,
      by simpa [ana, busyState] using aged⟩

theorem busy_cleanup_unbounded (bound : Nat) :
    ∃ s, Run .writes [0] s ∧ bound < s.time ∧ s.deleted = [] ∧
      eligible .writes s anaWrite [(0, 0, 1)] true = true := by
  refine ⟨busyState (day + bound + 1), busy_run _, by dsimp [busyState]; omega,
    rfl, busy_eligible _ (by omega)⟩

/-- Even coverage dominance cannot protect an indefinitely paused reader:
the replacement here dominates everything the earlier selection covers. -/
theorem reader_race_despite_dominance : Dominates coveringSnapshot loadedSnapshot := by
  constructor
  · rfl
  · intro writer
    by_cases same : writer = 0
    · subst writer; decide
    · simp [Snapshot.position, loadedSnapshot, coveringSnapshot, Ne.symm same]

theorem snapshot_age_boundaries :
    ({ ana with storedAt := day }).counts = true ∧
    ({ ana with storedAt := day + 1 }).counts = false ∧
    eligible .writes { cleanupReady with time := day } anaWrite [(0, 0, 1)] true = false ∧
    eligible .writes cleanupReady anaWrite [(0, 0, 1)] true = true := by decide

theorem away_write_miss :
    Deletable (only ana) [0, 1] (fun _ _ => 0) month True anaWrite ∧
    deletedAna (.log .write 0 1) = none := by
  refine ⟨⟨?_, trivial, Or.inr (by simp [anaWrite])⟩, rfl⟩
  intro a ha
  have eq : a = 0 := by simpa [anaWrite] using ha
  subst a
  exact (by decide : ana.covers anaWrite 0)

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
    exact (by decide : ({ ana with storedAt := 1 }).covers { anaWrite with storedAt := 1 } 0)
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
