import CovenIO.FixedReload
import CovenIO.RetentionDeadline
import CovenIO.AtomicReload
import CovenIO.ReloadLiveness

namespace CovenIO.RepairedExamples
open SnapshotPublication Examples ReloadExamples FixedReload RetentionDeadline

def catalogState (s : State) : SnapshotHistory.State := ⟨s.time, s.landed, s.deleted⟩

theorem selection_races_finish (order : SnapshotOrder) :
    scan (fun _ => snapshotStore afterSelection) (fun _ => catalogState afterSelection) order
      0 (some rightSnapshot) [] [anaWrite, otherWrite] 0 = .retry 1 otherWrite ∧
    scan (fun _ => snapshotStore afterSelection) (fun _ => catalogState afterSelection) order
      0 (current order afterSelection 0) afterSelection.deleted [anaWrite, otherWrite] 1 = .complete [] ∧
    scan (fun _ => snapshotStore missedState) (fun _ => catalogState missedState) order
      0 (some loadedSnapshot) [] [anaWrite, laterWrite] 0 = .retry 1 laterWrite ∧
    scan (fun _ => snapshotStore missedState) (fun _ => catalogState missedState) order
      0 (current order missedState 0) missedState.deleted [anaWrite, laterWrite] 1 = .complete [] := by
  cases order <;> decide

/-- This supplies the catalog certificates for an actual retry, so the
combined termination theorem is instantiated beyond an already-covered cut. -/
def racePublished (w : Write) : Prop := w = anaWrite ∨ w = laterWrite

theorem race_sound_loaded : SnapshotHistory.Sound racePublished loadedSnapshot := by
  constructor
  · intro w pub ha cov
    rcases pub with rfl | rfl <;>
      simp_all [Snapshot.covers, Snapshot.position, anaWrite, laterWrite, loadedSnapshot]
  · intro w pub ha earlier
    rcases pub with rfl | rfl <;> simp [anaWrite, laterWrite, loadedSnapshot] at earlier

theorem race_sound_covering : SnapshotHistory.Sound racePublished coveringSnapshot := by
  constructor
  · intro w pub ha cov
    rcases pub with rfl | rfl <;> decide
  · intro w pub ha earlier
    rcases pub with rfl | rfl
    · decide
    · simp [laterWrite, coveringSnapshot] at earlier

theorem race_catalog_runs (order : SnapshotOrder) :
    SnapshotHistory.Run racePublished order (catalogState loadedState) ∧
    SnapshotHistory.Run racePublished order (catalogState missedState) := by
  have start : SnapshotHistory.Run racePublished order (catalogState loadedState) :=
    .step .initial (.land _ loadedSnapshot rfl race_sound_loaded)
  refine ⟨start, ?_⟩
  have tick : SnapshotHistory.Run racePublished order ⟨1, [loadedSnapshot], []⟩ :=
    .step start (.tick _ 1 (by decide))
  have landed : SnapshotHistory.Run racePublished order ⟨1, [coveringSnapshot, loadedSnapshot], []⟩ :=
    .step tick (.land _ coveringSnapshot rfl race_sound_covering)
  have aged : SnapshotHistory.Run racePublished order ⟨month + 1, [coveringSnapshot, loadedSnapshot], []⟩ :=
    .step landed (.tick _ (month + 1) (by decide))
  exact .step aged (.delete _ laterWrite (Or.inr rfl) [0, 1] (fun _ _ => 0) True
    (by cases order <;> decide))

def raceSource (order : SnapshotOrder) : Source order [anaWrite, laterWrite] where
  world := fun _ => snapshotStore missedState
  history := fun t => if t = 0 then catalogState loadedState else catalogState missedState
  published := racePublished
  required := by intro w hw; simpa [racePublished] using hw
  unique := by
    intro u v pu pv writer number
    rcases pu with rfl | rfl <;> rcases pv with rfl | rfl <;> simp_all [anaWrite, laterWrite]
  runs := by
    intro t
    split
    · exact (race_catalog_runs order).1
    · exact (race_catalog_runs order).2
  deletedMono := by
    intro s t later w member
    by_cases hs : s = 0
    · simp [hs, catalogState, loadedState] at member
    · have ht : t ≠ 0 := by omega
      simpa [hs, ht] using member
  absent := by
    intro t w member missing
    rcases List.mem_cons.mp member with rfl | rest
    · contradiction
    · have eq : w = laterWrite := by simpa using rest
      subst w
      simp [catalogState, missedState]

theorem combined_race_commits (order : SnapshotOrder) :
    Loads (raceSource order) 0 0 1 1 [] := by
  apply Loads.retry 0 1 1 0 laterWrite []
  · cases order <;> decide
  · apply Loads.commit
    cases order <;> decide

theorem other_audience_finishes (order : SnapshotOrder) :
    scan (fun _ => snapshotStore crossAudienceState) (fun _ => catalogState crossAudienceState) order
      0 none [] [⟨0, 1, 0, [1]⟩, ⟨0, 2, month, [0]⟩] 0 = .complete [⟨0, 2, month, [0]⟩] := by
  cases order <;> decide

theorem cached_catalog_cannot_commit (order : SnapshotOrder) :
    scan (fun _ => snapshotStore missedState) (fun _ => catalogState loadedState) order
      0 (some loadedSnapshot) [] [anaWrite, laterWrite] 0 = .uncertified laterWrite := by
  cases order <;> decide

theorem moving_catalog (order : SnapshotOrder) (clock : Nat → Nat)
    (spaced : ∀ n, clock n + month ≤ clock (n + 1)) (n : Nat) :
    catalogCurrent order (ReloadLiveness.snapshots clock n) 0 = some (ReloadLiveness.snap clock n) := by
  have filter : ∀ k, (ReloadLiveness.snapshots clock k).filter
      (fun s => s.audience == 0 && s.counts) = ReloadLiveness.snapshots clock k := by
    intro k
    induction k with
    | zero => simp [ReloadLiveness.snapshots, ReloadLiveness.snap, Snapshot.counts]
    | succ k ih =>
        simp only [ReloadLiveness.snapshots, List.filter_cons]
        rw [ih]
        simp [ReloadLiveness.snap, Snapshot.counts]
  simp only [catalogCurrent, filter]
  induction n with
  | zero => rfl
  | succ n ih =>
      have later := spaced n
      have positive : 0 < month := by decide
      have newerSnap : newer order (ReloadLiveness.snap clock (n + 1)) (ReloadLiveness.snap clock n) = true := by
        cases order <;> simp [newer, Snapshot.rank, Snapshot.writeCount, ReloadLiveness.snap] <;> omega
      simp [ReloadLiveness.snapshots, newest, ih, newerSnap]

/-- Even arbitrary future storage cannot delay this commit: its moving tail
is outside the fixed target. The selected snapshot is newest in all orders. -/
theorem moving_tail_finishes (order : SnapshotOrder) (clock : Nat → Nat)
    (spaced : ∀ n, clock n + month ≤ clock (n + 1)) (n : Nat) (world : World)
    (catalog : Nat → SnapshotHistory.State) :
    scan world catalog order 0
      (catalogCurrent order (ReloadLiveness.snapshots clock n) 0) []
      (ReloadLiveness.writes clock n) 0 = .complete [] := by
  rw [moving_catalog order clock spaced n]
  apply scan_covered
  intro w hw
  have zero : w.writer = 0 := by
    have all := (ReloadLiveness.numbers clock n).2.1
    have := List.all_eq_true.mp all w hw
    simpa using this
  simpa [position, ReloadLiveness.snap, Snapshot.position, zero] using (ReloadLiveness.numbers clock n).2.2 w hw

theorem quiet_stall_repaired (order : SnapshotOrder) :
    refreshDue order beforeSelection [] true true otherWrite 0 = true ∧
    execute order beforeSelection afterSelectionEvents = some afterSelection ∧
    otherWrite ∈ (retainPass order afterSelection [] true).deleted := by
  cases order <;> decide

def repairedPass : State := { afterSelection with time := month + 3, deleted := [] }

set_option maxRecDepth 4096 in
theorem repaired_pass_reachable (order : SnapshotOrder) : Run order [0, 1, 2] repairedPass := by
  refine ⟨beforeSelectionEvents ++ afterSelectionEvents.take 7 ++ [(month + 3, .tick)], ?_⟩
  cases order <;> decide

theorem joined_dominates : Dominates joinedSnapshot rightSnapshot := by
  constructor
  · rfl
  · intro writer
    by_cases h : writer = 0
    · subst writer; decide
    · simp [Snapshot.position, joinedSnapshot, rightSnapshot, Ne.symm h]

def quiet_service (order : SnapshotOrder) :
    Service order repairedPass.writers repairedPass.landed otherWrite 0 month 1 0 where
  base := beforeSelection.landed
  added := [joinedSnapshot]
  inventory := rfl
  previous := rightSnapshot
  selected := by cases order <;> decide
  afterReady := by
    intro snap member
    have eq : snap = joinedSnapshot := by simpa using member
    subst snap
    exact Nat.le_refl _
  outcome := Or.inr ⟨joinedSnapshot, by simp, rfl, by decide, by unfold SnapshotRefresh.Normalized; decide,
    joined_dominates, Nat.le_refl _, Nat.le_refl _, by decide⟩

theorem quiet_deadline (order : SnapshotOrder) :
    ∃ time, time ≤ month + 7 ∧ month + 7 ≤ time ∧
      otherWrite ∈ (retainPass order repairedPass [] true).deleted := by
  have sound : ∀ snap ∈ repairedPass.landed,
      SnapshotHistory.Sound (fun w => w = otherWrite) snap := by
    intro snap member
    simp [repairedPass, afterSelection] at member
    rcases member with rfl | rfl | rfl
    · constructor
      · intro w pub _ _; subst w; decide
      · intro w pub _ _; subst w; decide
    · constructor
      · intro w pub _ _; subst w; decide
      · intro w pub _ earlier; subst w; change 0 < 0 at earlier; omega
    · constructor
      · intro w pub _ _; subst w; decide
      · intro w pub _ earlier; subst w; change 0 < 0 at earlier; omega
  have form : ∀ snap ∈ repairedPass.landed, SnapshotRefresh.Normalized repairedPass.writers snap := by
    intro snap member
    simp [repairedPass, afterSelection] at member
    rcases member with rfl | rfl | rfl <;> unfold SnapshotRefresh.Normalized <;> decide
  have result := bounded_witnessed_storage order repairedPass [] (fun _ => true) otherWrite (fun w => w = otherWrite)
    rfl rfl (by decide) (by decide) 1 month 1 0 4 sound form
    (by
      intro a ha
      have eq : a = 0 := by simpa [otherWrite] using ha
      subst a
      exact ⟨leftSnapshot, by decide, by decide, by decide, by decide⟩)
    ⟨0, by decide⟩
    (by
      intro a ha
      have eq : a = 0 := by simpa [otherWrite] using ha
      subst a
      exact quiet_service order)
    (Or.inr (by decide)) (by decide)
    (fun time w => month + 7 ≤ time ∧ w ∈ (retainPass order repairedPass [] true).deleted)
    (by intro v hv; exact ⟨Nat.le_refl _, hv⟩)
  exact result

theorem mixed_finality_retention (order : SnapshotOrder) :
    anaWrite ∈ (retainWrites order repairedPass [] (fun w => w.writer == 0)).deleted ∧
    otherWrite ∉ (retainWrites order repairedPass [] (fun w => w.writer == 0)).deleted := by
  cases order <;> decide

theorem scheduled_cleanup_runs (order : SnapshotOrder) :
    anaWrite ∈ (retainPass order cleanupReady [(0, 0, 1)] true).deleted := by
  cases order <;> decide

theorem busy_selected (order : SnapshotOrder) (n : Nat) :
    catalogCurrent order (busyState n).landed 0 = some (busySnapshot n) := by
  simp only [catalogCurrent, busyState, (busy_catalog n).1]
  induction n with
  | zero => rfl
  | succ n ih =>
      have next : newer order (busySnapshot (n + 1)) (busySnapshot n) = true := by
        cases order <;> simp [newer, Snapshot.rank, Snapshot.writeCount, busySnapshot]
      simp [busySnapshots, newest, ih, next]

theorem busy_cleanup_runs (order : SnapshotOrder) (n : Nat) (aged : day < n) :
    anaWrite ∈ (retainPass order (busyState n) [(0, 0, 1)] true).deleted := by
  apply due_pass_deletes order (busyState n) [(0, 0, 1)] true anaWrite (busy_retains_first n).2
  simp only [eligible, decide_eq_true_eq, Eligible, Deletable]
  refine ⟨⟨?_, trivial, Or.inl ?_⟩, ?_⟩
  · simp [Covered, anaWrite, busy_selected, Snapshot.covers, busySnapshot, Snapshot.position]
  · simp [busyState, anaWrite, postedPosition]
  · intro a ha
    have eq : a = 0 := by simpa [anaWrite] using ha
    subst a
    exact ⟨ana, (busy_retains_first n).1, by decide, by decide,
      by simpa [ana, busyState] using aged⟩

/-- One write with no snapshot is final and aged, but the proposed repair
never requests its first witness, even with an online reader every pass. -/
def withoutWitness : State := ⟨month, [0], [anaWrite], [], [], [], []⟩

def unwitnessedPasses (order : SnapshotOrder) : Nat → State
  | 0 => withoutWitness
  | n + 1 => retainPass order { unwitnessedPasses order n with time := month + n + 1 } [] true

theorem no_first_witness (order : SnapshotOrder) (n : Nat) :
    unwitnessedPasses order n = { withoutWitness with time := month + n } ∧
    refreshDue order (unwitnessedPasses order n) [] true true anaWrite 0 = false := by
  have keeps : ∀ time, retainPass order { withoutWitness with time := time } [] true =
      { withoutWitness with time := time } := by
    intro time
    simp [retainPass, retainWrites, withoutWitness, eligible, Eligible, Deletable, Covered, catalogCurrent, newest, anaWrite]
  induction n with
  | zero => constructor <;> rfl
  | succ n ih =>
      have eq : unwitnessedPasses order (n + 1) = { withoutWitness with time := month + (n + 1) } := by
        simp [unwitnessedPasses, ih.1, keeps, Nat.add_assoc]
      refine ⟨eq, ?_⟩
      rw [eq]
      simp [refreshDue, coverageDue, historical, withoutWitness]

example (order : SnapshotOrder) (deadline : Nat) :
    ∃ s, Run order [0] s ∧ deadline < s.time ∧
      anaWrite ∉ s.deleted ∧ refreshDue order s [] true true anaWrite 0 = false ∧
      s = unwitnessedPasses order (deadline + 1) := by
  refine ⟨{ withoutWitness with time := month + (deadline + 1) }, ?_, by dsimp; omega,
    by simp [withoutWitness], by simp [refreshDue, coverageDue, historical, withoutWitness], (no_first_witness order _).1.symm⟩
  refine ⟨[(0, .write 0 [0]), (month + (deadline + 1), .tick)], ?_⟩
  simp [execute, step, act, initial, withoutWitness, anaWrite]

/-- Removing only the historical-witness precondition requests the missing
first snapshot in the counterexample. This extension is not part of the proposal. -/
example (order : SnapshotOrder) :
    refreshDue order withoutWitness [] true true anaWrite 0 = false ∧
    coverageDue order withoutWitness [] true true anaWrite 0 = true := by
  cases order <;> decide

/-- The old quiet-store history is also permitted forever when nobody who
can read its missing audience is online. Retention itself still runs. -/
example (order : SnapshotOrder) (n : Nat) :
    otherWrite ∉ (quietPasses order n).deleted ∧
    refreshDue order (quietPasses order n) [] true false otherWrite 0 = false := by
  exact ⟨(every_pass_retains_uncovered order n).2.2.2, by simp [refreshDue, coverageDue]⟩

/-- A checked publication history behind the multi-audience overrun. At the
initial selection only audience 1 has a snapshot, through write 1. -/
def firstPart : Write := ⟨0, 1, 0, [0, 1]⟩
def secondPart : Write := ⟨0, 2, 1, [0, 1]⟩
def audienceOne : Snapshot := ⟨1, 0, 1, 0, 0, [(0, 1)]⟩
def audienceZero : Snapshot := ⟨0, 0, 1, 1, 1, [(0, 2)]⟩
def firstCut : State := ⟨0, [0], [firstPart], [], [audienceOne], [], []⟩
def laterCut : State :=
  ⟨month + 1, [0], [secondPart, firstPart], [firstPart], [audienceZero, audienceOne], [], []⟩
def firstCutEvents : List (Nat × Action) :=
  [(0, .write 0 [0, 1]), (0, .beginCatchup 1 0 1), (0, .read 0 0),
    (0, .read 0 0), (0, .prepare 0), (0, .land 1 0 1)]
def laterCutEvents : List (Nat × Action) :=
  [(1, .write 0 [0, 1]), (1, .beginCatchup 0 0 1), (1, .read 0 0),
    (1, .read 0 0), (1, .read 0 0), (1, .prepare 0), (1, .land 0 0 1),
    (month + 1, .delete 0 1 [] true)]

set_option maxRecDepth 4096 in
theorem replacement_exceeds_target (order : SnapshotOrder) :
    execute order (initial [0]) firstCutEvents = some firstCut ∧
    execute order firstCut laterCutEvents = some laterCut ∧
    scan (fun _ => snapshotStore laterCut) (fun _ => catalogState laterCut) order
      0 none [] [firstPart] 0 = .retry 1 firstPart ∧
    scan (fun _ => snapshotStore laterCut) (fun _ => catalogState laterCut) order
      0 (current order laterCut 0) laterCut.deleted [firstPart] 1 = .complete [] ∧
    scan (fun _ => snapshotStore laterCut) (fun _ => catalogState laterCut) order
      1 (current order laterCut 1) laterCut.deleted [firstPart] 1 = .complete [] ∧
    audienceZero.position 0 = 2 ∧ audienceOne.position 0 = 1 ∧
    secondPart ∉ laterCut.deleted := by
  cases order <;> decide

set_option maxRecDepth 4096 in
example (order : SnapshotOrder) :
    Run order [0] laterCut ∧
    ¬ AtomicReload.CommonPoint [0, 1] [0]
      (fun a writer => position (current order laterCut a) writer) := by
  constructor
  · refine ⟨firstCutEvents ++ laterCutEvents, ?_⟩
    cases order <;> decide
  · intro aligned
    have equal := aligned 0 (by simp) 1 (by simp) 0 (by simp)
    cases order <;> (change 2 = 1 at equal; contradiction)

/-- Independent publication remains possible; symmetric deletion and late
uploads remain excluded. These checks do not depend on the chosen ordering. -/
theorem old_races_preserved (order : SnapshotOrder) :
    execute order (initial [0, 1]) incomparableEvents = some incomparableState ∧
    !(eligible order beforeSelection anaWrite [] true && eligible order beforeSelection otherWrite [] true) = true ∧
    eligible order { incomparableState with time := day, landed := [ana] }
      anaWrite [(0, 0, 1), (1, 0, 1)] true = false ∧
    catalogCurrent order [oldCompetitor, ana] 0 = some ana := by
  cases order <;> decide

end CovenIO.RepairedExamples
