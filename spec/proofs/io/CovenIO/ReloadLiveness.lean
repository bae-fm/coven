import CovenIO.ReloadExamples

namespace CovenIO.ReloadLiveness
open SnapshotPublication Examples

variable (clock : Nat → Nat)
def write (n : Nat) : Write := ⟨0, n + 1, clock n, [0]⟩
def snap (n : Nat) : Snapshot := ⟨0, 0, n + 1, clock n, clock n, [(0, n + 1)]⟩
def writes : Nat → List Write
  | 0 => [write clock 0]
  | n + 1 => write clock (n + 1) :: writes n
def snapshots : Nat → List Snapshot
  | 0 => [snap clock 0]
  | n + 1 => snap clock (n + 1) :: snapshots n
def state (n : Nat) : State :=
  ⟨clock n + month, [0], writes clock n, writes clock n, snapshots clock n, [], []⟩

theorem catalog (n : Nat) : catalogCurrent .writes (snapshots clock n) 0 = some (snap clock n) := by
  have facts : (snapshots clock n).filter (fun s => s.audience == 0 && s.counts) = snapshots clock n ∧
      newest .writes (snapshots clock n) = some (snap clock n) := by
    induction n with
    | zero => simp [snapshots, snap, newest, Snapshot.counts]
    | succ n ih =>
        constructor
        · simp only [snapshots, List.filter_cons]
          rw [ih.1]
          simp [snap, Snapshot.counts]
        · simp [snapshots, newest, ih.2, newer, Snapshot.rank, Snapshot.writeCount, snap]
  simp [catalogCurrent, facts.1, facts.2]

theorem numbers (n : Nat) :
    (writes clock n).length = n + 1 ∧
    (writes clock n).all (fun w => w.writer == 0) = true ∧
    ∀ w ∈ writes clock n, w.number ≤ n + 1 := by
  induction n with
  | zero => simp [writes, write]
  | succ n ih =>
      refine ⟨by simp [writes, ih.1], by simp [writes, write, ih.2.1], ?_⟩
      intro w hw
      rcases List.mem_cons.mp hw with rfl | old
      · simp [write]
      · have := ih.2.2 w old; omega

theorem snapshot_numbers (n : Nat) : ∀ s ∈ snapshots clock n,
    s.number ≤ n + 1 ∧ s.position 0 ≤ n + 1 := by
  induction n with
  | zero => simp [snapshots, snap, Snapshot.position]
  | succ n ih =>
      intro s hs
      rcases List.mem_cons.mp hs with rfl | old
      · simp [snap, Snapshot.position]
      · obtain ⟨number, position⟩ := ih s old
        exact ⟨by omega, by omega⟩

theorem no_covered_miss (n : Nat) : coveredMiss (snapshots clock n) 0 (n + 2) = false := by
  apply Bool.eq_false_iff.mpr
  intro h
  obtain ⟨s, hs, yes⟩ := List.any_eq_true.mp h
  have bound := (snapshot_numbers clock n s hs).2
  simp only [Bool.and_eq_true, decide_eq_true_eq] at yes
  omega

theorem old_absent (n writer number : Nat) :
    (writes clock n).find? (fun w => w.writer == writer && w.number == number && !decide (w ∈ writes clock n)) = none := by
  apply List.find?_eq_none.mpr
  intro w hw
  simp [hw]

def events (n : Nat) : List (Nat × Action) :=
  [(clock (n + 1), .write 0 [0]), (clock (n + 1), .beginCatchup 0 0 (n + 2)),
    (clock (n + 1), .read 0 0), (clock (n + 1), .read 0 0),
    (clock (n + 1), .prepare 0), (clock (n + 1), .land 0 0 (n + 2)),
    (clock (n + 1) + month, .delete 0 (n + 2) [] true)]

theorem step (n : Nat) (late : clock n + month ≤ clock (n + 1)) : execute .writes (state clock n) (events clock n) = some (state clock (n + 1)) := by
  have filtered : (writes clock n).filter (fun w => w.writer == 0) = writes clock n :=
    List.filter_eq_self.mpr (by simpa using (numbers clock n).2.1)
  have unused : ¬ ∃ s ∈ snapshots clock n, (0 = s.audience ∧ 0 = s.writer) ∧ n + 2 = s.number := by
    rintro ⟨s, hs, _, eq⟩
    have := (snapshot_numbers clock n s hs).1
    omega
  have newWrite : write clock (n + 1) ∉ writes clock n := by
    intro h
    have := (numbers clock n).2.2 _ h
    simp only [write] at this
    omega
  have absentNew : ¬ ⟨0, n + 2, clock (n + 1), [0]⟩ ∈ writes clock n := newWrite
  have latest : catalogCurrent .writes (snap clock (n + 1) :: snapshots clock n) 0 = some (snap clock (n + 1)) := catalog clock (n + 1)
  have maturity : MatureCoverage (snap clock (n + 1) :: snapshots clock n) (clock (n + 1) + month) (write clock (n + 1)) := by
    intro a ha
    have eq : a = 0 := by simpa [write] using ha
    subst a
    refine ⟨snap clock (n + 1), by simp, by simp [snap, Snapshot.counts],
      by simp [snap, write, Snapshot.covers, Snapshot.position], ?_⟩
    have duration : day < month := by unfold month day; decide
    change clock (n + 1) + day < clock (n + 1) + month
    omega
  have due : Eligible .writes (snap clock (n + 1) :: snapshots clock n) [0] (postedPosition [])
      (clock (n + 1) + month) (true = true) (write clock (n + 1)) := by
    refine ⟨⟨?_, rfl, Or.inr (by simp [write])⟩, maturity⟩
    intro a ha
    have eq : a = 0 := by simpa [write] using ha
    subst a
    rw [latest]
    simp [snap, write, Snapshot.covers, Snapshot.position]
  let written : State := ⟨clock (n + 1), [0], write clock (n + 1) :: writes clock n,
    writes clock n, snapshots clock n, [], []⟩
  let c : SnapshotPublication.Catchup := ⟨⟨0, 0, n + 2, clock (n + 1), clock (n + 1), [(0, n + 1)]⟩, [], clock (n + 1)⟩
  let begun := { written with catching := [c] }
  let readOne := { written with catching := [{ c with snapshot := snap clock (n + 1) }] }
  let readMiss := { written with catching := [{ c with snapshot := snap clock (n + 1), missed := [0] }] }
  let prepared := { written with uploading := [snap clock (n + 1)] }
  let landed := { written with landed := snap clock (n + 1) :: snapshots clock n }
  have h1 : SnapshotPublication.step .writes (state clock n) (clock (n + 1)) (.write 0 [0]) = some written := by
    simp only [SnapshotPublication.step, state, late, ↓reduceIte]
    simp [act, filtered, (numbers clock n).1, written, write]
  have h2 : SnapshotPublication.step .writes written (clock (n + 1)) (.beginCatchup 0 0 (n + 2)) = some begun := by
    simp [SnapshotPublication.step, written, act, current, catalog, samePath, unused, begun, c, snap]
  have h3 : SnapshotPublication.step .writes begun (clock (n + 1)) (.read 0 0) = some readOne := by
    simp [SnapshotPublication.step, begun, written, c, act, live, Snapshot.position, write,
      absentNew, readOne, snap]
  have h4 : SnapshotPublication.step .writes readOne (clock (n + 1)) (.read 0 0) = some readMiss := by
    simp [SnapshotPublication.step, readOne, written, c, act, live, Snapshot.position, write,
      absentNew, old_absent, readMiss, snap, no_covered_miss, Nat.add_assoc]
  have h5 : SnapshotPublication.step .writes readMiss (clock (n + 1)) (.prepare 0) = some prepared := by
    simp [SnapshotPublication.step, readMiss, written, c, act, canPrepare, current, catalog,
      dominates, Snapshot.position, snap, prepared]
  have h6 : SnapshotPublication.step .writes prepared (clock (n + 1)) (.land 0 0 (n + 2)) = some landed := by
    simp [SnapshotPublication.step, prepared, written, act, snap, samePath, landed]
  have h7 : SnapshotPublication.step .writes landed (clock (n + 1) + month) (.delete 0 (n + 2) [] true) =
      some (state clock (n + 1)) := by
    have check : eligible .writes { landed with time := clock (n + 1) + month } (write clock (n + 1)) [] true = true := by
      simpa [eligible, landed, written] using due
    have found : live { landed with time := clock (n + 1) + month } 0 (n + 2) = some (write clock (n + 1)) := by
      simp [live, landed, written, write, absentNew]
    simp only [SnapshotPublication.step, show landed.time ≤ clock (n + 1) + month by dsimp [landed, written]; omega,
      ↓reduceIte, act, found]
    simp only [bind, Option.bind, check, ↓reduceIte]
    rfl
  simp only [events, execute]
  rw [h1]
  simp only [Option.bind_some]
  rw [h2]
  simp only [Option.bind_some]
  rw [h3]
  simp only [Option.bind_some]
  rw [h4]
  simp only [Option.bind_some]
  rw [h5]
  simp only [Option.bind_some]
  rw [h6]
  simp only [Option.bind_some]
  rw [h7]
  rfl

theorem reachable (zero : clock 0 = 0) (spaced : ∀ n, clock n + month ≤ clock (n + 1)) (n : Nat) : Run .writes [0] (state clock n) := by
  induction n with
  | zero =>
      exact ⟨[(0, .write 0 [0]), (0, .beginCatchup 0 0 1), (0, .read 0 0),
        (0, .read 0 0), (0, .prepare 0), (0, .land 0 0 1), (month, .delete 0 1 [] true)], by simp only [state, writes, snapshots, write, snap, zero]; decide⟩
  | succ n ih =>
      obtain ⟨history, executed⟩ := ih
      refine ⟨history ++ events clock n, ?_⟩
      rw [run_append, executed]
      exact step clock n (spaced n)

/-- The reader loads n+1, then a further write is published, snapshotted,
aged and deleted before its next GET. Every round has finite successful IO;
storage time grows without bound. No errors or unfair provider are needed. -/
theorem busy_reload_never_finishes (zero : clock 0 = 0)
    (spaced : ∀ n, clock n + month ≤ clock (n + 1)) (n : Nat) :
    Run .writes [0] (state clock (n + 1)) ∧
    (readLoadingLog (fun _ => snapshotStore (state clock (n + 1))) (fun _ => snapshots clock (n + 1))
      (fun _ => none) (fun _ => false) 0 1 0 ((snap clock n).position 0)).stop = .snapshots ∧
    (snap clock (n + 1)).covers (write clock (n + 1)) 0 := by
  refine ⟨reachable clock zero spaced _, ?_, by simp [snap, write, Snapshot.covers, Snapshot.position]⟩
  have covered : coveredMiss (snapshots clock (n + 1)) 0 (n + 1) = true := by
    apply List.any_eq_true.mpr
    exact ⟨snap clock (n + 1), by simp [snapshots], by simp [snap, Snapshot.counts, Snapshot.position]⟩
  simp [readLoadingLog, readLog, snapshotStore, live, state, old_absent,
    snap, Snapshot.position, covered]

/-- Fixing the target at the loaded snapshot's position ends this particular
loop immediately: fetching n+2 is ordinary subsequent sync, not a prerequisite
for committing a snapshot that already realizes n+1. -/
theorem fixed_target_already_realized (n : Nat) : n + 1 ≤ (snap clock n).position 0 := by
  simp [snap, Snapshot.position]

theorem storage_time_unbounded (spaced : ∀ n, clock n + month ≤ clock (n + 1)) (n : Nat) :
    n ≤ clock n := by
  induction n with
  | zero => omega
  | succ n ih =>
      have positive : 0 < month := by decide
      have := spaced n
      omega

/-- Instantiate the schedule, so the infinite-round statement does not
depend on the existence of an unspecified clock. -/
example (n : Nat) :
    (readLoadingLog (fun _ => snapshotStore (state (fun k => month * k) (n + 1)))
      (fun _ => snapshots (fun k => month * k) (n + 1))
      (fun _ => none) (fun _ => false) 0 1 0 n.succ).stop = .snapshots := by
  have loop := busy_reload_never_finishes (fun k => month * k) (by simp)
    (by intro k; simp [Nat.mul_add]) n
  simpa only [snap, Snapshot.position, List.filter_cons, List.filter_nil, List.map_cons,
    List.map_nil, List.foldl_cons, List.foldl_nil, beq_self_eq_true, ↓reduceIte, Nat.zero_max] using loop.2.1

end CovenIO.ReloadLiveness
