import CovenIO.CommonReload
import CovenIO.AudienceRetention
import CovenIO.RepairedExamples
import CovenIO.SnapshotWriteExamples

namespace CovenIO.CommonReloadExamples
open RepairedExamples SnapshotPublication FixedReload

def published (w : Write) : Prop := w = firstPart ∨ w = secondPart

theorem snapshots_sound : SnapshotHistory.Sound published audienceOne ∧
    SnapshotHistory.Sound published audienceZero := by
  constructor <;> constructor
  all_goals
    intro w pub ha last
    rcases pub with rfl | rfl
    all_goals simp_all [firstPart, secondPart, audienceOne, audienceZero, Snapshot.covers, Snapshot.position]

theorem catalog_runs (order : SnapshotOrder) :
    SnapshotHistory.Run published order (catalogState firstCut) ∧
    SnapshotHistory.Run published order (catalogState laterCut) := by
  have first : SnapshotHistory.Run published order (catalogState firstCut) :=
    .step .initial (.land _ audienceOne rfl snapshots_sound.1)
  refine ⟨first, ?_⟩
  have tick : SnapshotHistory.Run published order ⟨1, [audienceOne], []⟩ :=
    .step first (.tick _ 1 (by decide))
  have landed : SnapshotHistory.Run published order ⟨1, [audienceZero, audienceOne], []⟩ :=
    .step tick (.land _ audienceZero rfl snapshots_sound.2)
  have aged : SnapshotHistory.Run published order ⟨month + 1, [audienceZero, audienceOne], []⟩ :=
    .step landed (.tick _ (month + 1) (by decide))
  exact .step aged (.delete _ firstPart (Or.inl rfl) [0] (fun _ _ => 0) True (by cases order <;> decide))

def source (order : SnapshotOrder) : CommonReload.Source order [0] where
  world := fun _ => snapshotStore laterCut
  history := fun t => if t = 0 then catalogState firstCut else catalogState laterCut
  published := published
  inventory := fun cut => [firstPart, secondPart].filter (fun w => decide (w.number ≤ cut w.writer))
  required := by intro cut w member; simpa [published] using (List.mem_filter.mp member).1
  within := by
    intro cut w member
    have facts := List.mem_filter.mp member
    rcases List.mem_cons.mp facts.1 with rfl | rest
    · exact ⟨by simp [firstPart], by decide, by simpa using facts.2⟩
    · have eq : w = secondPart := by simpa using rest
      subst w
      exact ⟨by simp [secondPart], by decide, by simpa using facts.2⟩
  enumerated := by
    intro cut w pub _ _ within
    exact List.mem_filter.mpr ⟨by simpa [published] using pub, by simpa using within⟩
  unique := by
    intro u v pu pv writer number
    rcases pu with rfl | rfl <;> rcases pv with rfl | rfl <;> simp_all [firstPart, secondPart]
  runs := by
    intro t
    split
    · exact (catalog_runs order).1
    · exact (catalog_runs order).2
  deletedMono := by
    intro s t later w member
    by_cases hs : s = 0
    · simp [hs, catalogState, firstCut] at member
    · have ht : t ≠ 0 := by omega
      simpa [hs, ht] using member
  publication := fun t => if t = 0 then SnapshotWriteExamples.first else SnapshotWriteExamples.later
  publicationRuns := by
    intro t
    split
    · exact SnapshotWriteExamples.first_run
    · exact SnapshotWriteExamples.later_run
  catalogPublished := by
    intro t snap member
    by_cases ht : t = 0 <;>
      simpa [ht, catalogState, firstCut, laterCut, SnapshotWriteExamples.first,
        SnapshotWriteExamples.later, SnapshotWriteExamples.state] using member
  frontierMono := by
    intro s t later w member
    have eq : w = 0 := by simpa using member
    subst w
    by_cases hs : s = 0 <;> by_cases ht : t = 0
    all_goals simp [hs, ht, SnapshotWriteExamples.first, SnapshotWriteExamples.later,
      SnapshotWriteExamples.state, SnapshotWriteExamples.oneWriter,
      SnapshotWriteExamples.logOne, SnapshotWriteExamples.logTwo]
    omega
  decoded := by
    intro t writer hw number bytes positive found
    have writerEq : writer = 0 := by simpa using hw
    subst writer
    have within : number ≤ (if t = 0 then 1 else 2) := by
      obtain ⟨bound, _⟩ := List.getElem?_eq_some_iff.mp found
      by_cases ht : t = 0 <;>
        simp [ht, SnapshotWriteExamples.first, SnapshotWriteExamples.later,
          SnapshotWriteExamples.state, SnapshotWriteExamples.oneWriter,
          SnapshotWriteExamples.logOne, SnapshotWriteExamples.logTwo] at bound ⊢ <;> omega
    by_cases one : number = 1
    · exact ⟨firstPart, Or.inl rfl, rfl, one.symm⟩
    · have two : number = 2 := by split at within <;> omega
      exact ⟨secondPart, Or.inr rfl, rfl, two.symm⟩
  absent := by
    intro start cut ready t w _ member missing
    have choices := (List.mem_filter.mp member).1
    rcases List.mem_cons.mp choices with rfl | rest
    · simp [catalogState, laterCut]
    · have eq : w = secondPart := by simpa using rest
      subst w
      have hit : snapshotStore laterCut (.log .write secondPart.writer secondPart.number) ≠ none := by decide
      exact False.elim (hit missing)

def initialLoader : CommonReload.State := ⟨0, fun _ => 0, fun _ => none⟩
def replacement : CommonReload.State := CommonReload.replace initialLoader 0 1

/-- The 2/1 history raises the common target to 2, obtains audience
1's second part from the log, and stages both audiences at 2. -/
theorem replacement_fills_other_audience (order : SnapshotOrder) :
    CommonReload.round (source order) [0, 1] (fun _ => 1) initialLoader = .retry 0 1 firstPart ∧
    CommonReload.target (source order) [0, 1] (fun _ => 1) replacement 0 = 2 ∧
    CommonReload.round (source order) [0, 1] (fun _ => 1) replacement =
      .complete [(0, []), (1, [secondPart])] ∧
    CommonReload.Loads (source order) [0, 1] (fun _ => 1) initialLoader 1 replacement
      [(0, []), (1, [secondPart])] := by
  have first : CommonReload.round (source order) [0, 1] (fun _ => 1) initialLoader = .retry 0 1 firstPart := by
    cases order <;> decide
  have last : CommonReload.round (source order) [0, 1] (fun _ => 1) replacement =
      .complete [(0, []), (1, [secondPart])] := by cases order <;> decide
  exact ⟨first, by cases order <;> decide, last,
    .retry initialLoader replacement 0 1 0 firstPart _ first (.commit replacement _ last)⟩

example (order : SnapshotOrder) :
    CommonReload.Quiescent (source order) 1 ∧
    CommonReload.Valid (source order) [0, 1] (fun _ => 1) replacement := by
  constructor
  · intro t ht; simp [source, show t ≠ 0 by omega]
  · refine ⟨?_, ?_, ?_⟩
    · intro a ha; simp [replacement, CommonReload.replace, initialLoader]; split <;> omega
    · intro w hw; simp [CommonReload.Source.frontier, source, replacement, CommonReload.replace,
        SnapshotWriteExamples.later, SnapshotWriteExamples.state, SnapshotWriteExamples.oneWriter,
        SnapshotWriteExamples.logTwo]
      split <;> simp_all
    · intro a _ positions kept
      simp [replacement, CommonReload.replace, initialLoader] at kept

/-- The store keeps its existing state, which holds none of Ana's writes,
while Gifts is selected after write 1 was deleted. A selection from that
catalog would already account for the deletion; the kept state knows none,
so it reads write 1, finds it deleted, and is replaced by the store's current
snapshot through 2. The common target becomes 2 and Gifts reads write 2. -/
def keptLoader : CommonReload.State := ⟨1, fun _ => 1, fun a => if a = 0 then some [] else none⟩
def keptReplaced : CommonReload.State := CommonReload.replace keptLoader 0 2

theorem kept_state_replaced (order : SnapshotOrder) :
    CommonReload.Valid (source order) [0, 1] (fun _ => 1) keptLoader ∧
    CommonReload.round (source order) [0, 1] (fun _ => 1) keptLoader = .retry 0 2 firstPart ∧
    CommonReload.round (source order) [0, 1] (fun _ => 1) keptReplaced =
      .complete [(0, []), (1, [secondPart])] ∧
    CommonReload.Loads (source order) [0, 1] (fun _ => 1) keptLoader 1 keptReplaced
      [(0, []), (1, [secondPart])] := by
  have first : CommonReload.round (source order) [0, 1] (fun _ => 1) keptLoader = .retry 0 2 firstPart := by
    cases order <;> decide
  have last : CommonReload.round (source order) [0, 1] (fun _ => 1) keptReplaced =
      .complete [(0, []), (1, [secondPart])] := by cases order <;> decide
  refine ⟨?_, first, last, .retry keptLoader keptReplaced 0 2 0 firstPart _ first (.commit keptReplaced _ last)⟩
  refine ⟨?_, ?_, ?_⟩
  · intro a _; simp [keptLoader]
  · intro w hw; simp [CommonReload.Source.frontier, source, keptLoader,
      SnapshotWriteExamples.later, SnapshotWriteExamples.state, SnapshotWriteExamples.oneWriter,
      SnapshotWriteExamples.logTwo]
    split <;> simp_all
  · intro a _ positions kept w _
    simp only [keptLoader] at kept
    split at kept
    · cases kept; simp [CommonReload.keptStart, Snapshot.position]
    · cases kept

/-- Bundles are sufficient when their common cut also reaches the frozen
past. Independent circle readers need not provide any all-circle bundle;
their shared store audience is omitted from these readable sets. -/
example : (∀ a ∈ [0, 1], ∃ publisher : Nat, a ∈ ([publisher] : List Nat)) ∧
    ¬ ∃ publisher : Nat, ∀ a ∈ [0, 1], a ∈ ([publisher] : List Nat) := by
  constructor
  · intro a _; exact ⟨a, by simp⟩
  · rintro ⟨publisher, all⟩
    have h0 := all 0 (by simp)
    have h1 := all 1 (by simp)
    simp at h0 h1
    omega

/-- Device 0 wrote both parts and went offline. Device 1 reads only the store
(audience 1), so its complete bundle cannot cover the offline circle (0). -/
def offlineSnapshot : Snapshot := { audienceOne with writer := 1 }

def offlinePart (time : Nat) : State :=
  ⟨time, [0, 1], [firstPart], [], [offlineSnapshot], [], []⟩

/-- The store-only publisher completes a catch-up over both writers. Its one
snapshot is a complete bundle of the audiences that publisher can read. -/
theorem offline_reachable (order : SnapshotOrder) (time : Nat) : Run order [0, 1] (offlinePart time) := by
  let events : List (Nat × Action) :=
    [(0, .write 0 [0, 1]), (0, .beginCatchup 1 1 1), (0, .read 1 0),
      (0, .read 1 0), (0, .read 1 1), (0, .prepare 1), (0, .land 1 1 1)]
  have start : execute order (initial [0, 1]) events = some (offlinePart 0) := by
    cases order <;> decide
  refine ⟨events ++ [(time, .tick)], ?_⟩
  rw [run_append, start]
  simp [execute, step, act, offlinePart]

/-- One write and two part audiences defeat an independent physical storage
bound. Bundling every audience of the online publisher does not change this. -/
example (order : SnapshotOrder) (deadline : Nat) :
    Run order [0, 1] (offlinePart (month + deadline + 1)) ∧
    (∀ a ∈ ([1] : List Nat), ∃ snap ∈ (offlinePart (month + deadline + 1)).landed,
      snap.writer = 1 ∧ snap.audience = a ∧ snap.counts = true ∧ snap.position 0 = 1) ∧
    firstPart ∉ (retainPass order (offlinePart (month + deadline + 1)) [] true).deleted ∧
    RetentionDeadline.refreshDue order (offlinePart (month + deadline + 1)) [] true false firstPart 0 = false ∧
    RetentionDeadline.refreshDue order (offlinePart (month + deadline + 1)) [] true true firstPart 1 = false ∧
    (offlinePart (month + deadline + 1)).time > deadline := by
  refine ⟨offline_reachable order _, ?_, ?_, by simp [RetentionDeadline.refreshDue], ?_, by dsimp [offlinePart]; omega⟩
  · intro a ha
    have eq : a = 1 := by simpa using ha
    subst a
    exact ⟨offlineSnapshot, by simp [offlinePart], rfl, rfl, by decide, by decide⟩
  · cases order <;> simp [retainPass, retainWrites, offlinePart, offlineSnapshot, eligible, Eligible, Deletable,
      Covered, catalogCurrent, newest, firstPart, audienceOne, Snapshot.covers, Snapshot.position, Snapshot.counts]
  · cases order <;> simp [RetentionDeadline.refreshDue, offlinePart, offlineSnapshot,
      catalogCurrent, newest, firstPart, audienceOne, Snapshot.covers, Snapshot.position, Snapshot.counts]

open Examples AudienceRetention

def firstSnapshot : Snapshot := ⟨0, 0, 1, month, month, [(0, 1)]⟩
def firstCovered : State :=
  ⟨month + day + 5, [0], [anaWrite], [], [firstSnapshot], [], []⟩

theorem first_coverage_sound : SnapshotHistory.Sound (fun w => w = anaWrite) firstSnapshot := by
  constructor <;> intro w pub _ _ <;> subst w <;> decide

/-- A readable audience with no snapshot requests and publishes its first
witness. Retention executes after the strict one-day age, in every order. -/
theorem first_snapshot_deleted (order : SnapshotOrder) :
    RetentionDeadline.refreshDue order withoutWitness [] true true anaWrite 0 = true ∧
    execute order withoutWitness
      [(month, .beginCatchup 0 0 1), (month, .read 0 0), (month, .read 0 0),
        (month, .prepare 0), (month, .land 0 0 1), (month + day + 5, .tick)] = some firstCovered ∧
    anaWrite ∈ (retainPass order firstCovered [] true).deleted := by
  cases order <;> decide

def firstService (order : SnapshotOrder) :
    WitnessService order firstCovered.landed anaWrite 0 (ready anaWrite month month) 1 0 where
  base := []
  added := [firstSnapshot]
  inventory := rfl
  observedAt := month
  observed := by decide
  stored := by simp
  outcome := Or.inr ⟨firstSnapshot, by simp, rfl, by decide, by decide, by decide⟩

def afterFirstService (order : SnapshotOrder) :
    RetentionDeadline.Service order [0] firstCovered.landed anaWrite 0 (month + day + 3) 1 0 where
  base := [firstSnapshot]
  added := []
  inventory := rfl
  previous := firstSnapshot
  selected := by cases order <;> decide
  afterReady := by simp
  outcome := Or.inl (by decide)

set_option maxRecDepth 4096 in
/-- Instantiate the all-write theorem, including an initially empty catalog,
with successful audience and deletion services rather than assumed witnesses. -/
theorem first_snapshot_deadline (order : SnapshotOrder) :
    ∃ time, time ≤ month + day + 6 ∧
      time = firstCovered.time ∧ anaWrite ∈ (retainWrites order firstCovered [] (fun _ => true)).deleted := by
  have sound : ∀ snap ∈ firstCovered.landed, SnapshotHistory.Sound (fun w => w = anaWrite) snap := by
    intro snap member
    have eq : snap = firstSnapshot := by simpa [firstCovered] using member
    exact eq ▸ first_coverage_sound
  have normalized : ∀ snap ∈ firstCovered.landed, SnapshotRefresh.Normalized firstCovered.writers snap := by
    intro snap member
    have eq : snap = firstSnapshot := by simpa [firstCovered] using member
    subst snap
    unfold SnapshotRefresh.Normalized
    decide
  have boundEq : witnessBound anaWrite month (fun _ => month) 1 0 = month + 2 := by decide
  have maxEq : max month (month + 2 + day + 1) = month + day + 3 := by decide
  have result := bounded_storage order firstCovered [] (fun _ => true) anaWrite (fun w => w = anaWrite)
    rfl rfl (by decide) (by decide) month 1 0 0 (fun _ => month) sound normalized
    ⟨0, by decide⟩
    (by
      intro a ha
      have eq : a = 0 := by simpa [anaWrite] using ha
      subst a
      exact firstService order)
    (by
      intro a ha
      have eq : a = 0 := by simpa [anaWrite] using ha
      subst a
      simp only [boundEq, maxEq]
      exact afterFirstService order)
    (Or.inr (by decide)) (by simp only [boundEq, maxEq]; decide)
    (fun time w => time = firstCovered.time ∧ w ∈ (retainWrites order firstCovered [] (fun _ => true)).deleted)
    (by intro v hv; exact ⟨rfl, hv⟩)
  simpa only [boundEq, maxEq, Nat.add_zero] using result

end CovenIO.CommonReloadExamples
