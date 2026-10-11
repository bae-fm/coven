import CovenIO.Publication
import CovenIO.Reload

namespace CovenIO.SnapshotWrites

/-- Per-writer historical publication, including objects since deleted by
retention. The same request rule serves ordinary and raise snapshots. -/
structure State where
  logs : Nat → Publication.Writer
  captured : List Snapshot
  requested : List Snapshot
  landed : List Snapshot

def initial : State := ⟨fun _ => Publication.initial, [], [], []⟩

def changeLog (s : State) (writer : Nat) (next : Publication.Writer) : State :=
  { s with logs := fun w => if w = writer then next else s.logs w }

/-- Local capture can include pending own writes. Other devices' positions
must already have publication evidence from reads or a loaded snapshot. -/
def Captured (s : State) (snap : Snapshot) : Prop :=
  snap.position snap.writer ≤ (s.logs snap.writer).reserved.length ∧
    ∀ writer, writer ≠ snap.writer → snap.position writer ≤ (s.logs writer).published.length

def Published (s : State) (snap : Snapshot) : Prop :=
  ∀ writer, snap.position writer ≤ (s.logs writer).published.length

inductive Step : State → State → Prop
  | log (s : State) (writer : Nat) (next : Publication.Writer)
      (event : Publication.Step (s.logs writer) next) : Step s (changeLog s writer next)
  | capture (s : State) (snap : Snapshot) (positions : Captured s snap) :
      Step s { s with captured := snap :: s.captured }
  | request (s : State) (snap : Snapshot) (captured : snap ∈ s.captured)
      (confirmed : snap.position snap.writer ≤ (s.logs snap.writer).confirmed) :
      Step s { s with requested := snap :: s.requested }
  | land (s : State) (snap : Snapshot) (requested : snap ∈ s.requested) (time : Nat) :
      Step s { s with landed := { snap with storedAt := time } :: s.landed }
  | crash (s : State) : Step s s

/-- Publication and reservations never retreat, even after cancellation. -/
theorem log_monotone {a b : Publication.Writer} (step : Publication.Step a b) :
    a.reserved.length ≤ b.reserved.length ∧ a.published.length ≤ b.published.length := by
  cases step <;> simp

theorem step_monotone {a b : State} (step : Step a b) (writer : Nat) :
    (a.logs writer).reserved.length ≤ (b.logs writer).reserved.length ∧
      (a.logs writer).published.length ≤ (b.logs writer).published.length := by
  cases step with
  | log w next event =>
      by_cases same : writer = w
      · subst writer; simpa [changeLog] using log_monotone event
      · simp [changeLog, same]
  | capture | request | land | crash => exact ⟨Nat.le_refl _, Nat.le_refl _⟩

def Valid (s : State) : Prop :=
  (∀ writer, Publication.Valid (s.logs writer)) ∧
    (∀ snap ∈ s.captured, Captured s snap) ∧
    (∀ snap ∈ s.requested, Published s snap) ∧
    ∀ snap ∈ s.landed, Published s snap

theorem captured_monotone {a b : State} (step : Step a b) (snap : Snapshot)
    (captured : Captured a snap) : Captured b snap :=
  ⟨Nat.le_trans captured.1 (step_monotone step snap.writer).1,
    fun writer other => Nat.le_trans (captured.2 writer other) (step_monotone step writer).2⟩

theorem published_monotone {a b : State} (step : Step a b) (snap : Snapshot)
    (published : Published a snap) : Published b snap :=
  fun writer => Nat.le_trans (published writer) (step_monotone step writer).2

theorem step_valid {a b : State} (step : Step a b) (valid : Valid a) : Valid b := by
  obtain ⟨logs, captured, requested, landed⟩ := valid
  cases step with
  | log writer next event =>
      refine ⟨?_, ?_, ?_, ?_⟩
      · intro w
        by_cases same : w = writer
        · subst w; simpa [changeLog] using Publication.step_valid event (logs writer)
        · simpa [changeLog, same] using logs w
      · intro snap member
        exact captured_monotone (.log _ _ _ event) snap (captured snap member)
      · intro snap member
        exact published_monotone (.log _ _ _ event) snap (requested snap member)
      · intro snap member
        exact published_monotone (.log _ _ _ event) snap (landed snap member)
  | capture snap positions =>
      refine ⟨logs, ?_, requested, landed⟩
      intro other member
      rcases List.mem_cons.mp member with rfl | old
      · exact positions
      · exact captured other old
  | request snap member confirmed =>
      refine ⟨logs, captured, ?_, landed⟩
      intro other member'
      rcases List.mem_cons.mp member' with rfl | old
      · intro writer
        by_cases own : writer = other.writer
        · subst writer; exact Nat.le_trans confirmed (logs _).1
        · exact (captured other member).2 writer own
      · exact requested other old
  | land snap member time =>
      refine ⟨logs, captured, requested, ?_⟩
      intro other member'
      rcases List.mem_cons.mp member' with rfl | old
      · exact requested snap member
      · exact landed other old
  | crash => exact ⟨logs, captured, requested, landed⟩

inductive Run : State → Prop
  | initial : Run initial
  | step {a b : State} : Run a → Step a b → Run b

theorem run_valid {s : State} (run : Run s) : Valid s := by
  induction run with
  | initial => simp [Valid, initial, Publication.Valid, Publication.initial]
  | step run step ih => exact step_valid step ih

/-- The log model supplies real bytes at every positive covered number.
Counting admission only restricts this stronger all-landings invariant. -/
theorem snapshot_covers_published {s : State} (run : Run s) (snap : Snapshot)
    (landed : snap ∈ s.landed) (writer number : Nat)
    (positive : 0 < number) (covered : number ≤ snap.position writer) :
    ∃ bytes, (s.logs writer).published[number - 1]? = some bytes := by
  have bound := Nat.le_trans covered ((run_valid run).2.2.2 snap landed writer)
  have index : number - 1 < (s.logs writer).published.length := by omega
  exact ⟨(s.logs writer).published[number - 1], by simp [index]⟩

theorem selected_bound {s : State} (run : Run s) (order : SnapshotOrder)
    (audience writer : Nat) (snap : Snapshot)
    (selected : catalogCurrent order s.landed audience = some snap) :
    snap.position writer ≤ (s.logs writer).published.length :=
  (run_valid run).2.2.2 snap (current_member order s.landed audience snap selected).1 writer

/-- Loading a remote snapshot supplies the capture rule's remote bound.
There is no assumption that positions themselves prove publication. -/
theorem inherited_position_published {s : State} (run : Run s) (snap : Snapshot)
    (member : snap ∈ s.landed) (writer : Nat) :
    snap.position writer ≤ (s.logs writer).published.length :=
  (run_valid run).2.2.2 snap member writer

/-- Coverage of a missed next number cannot be manufactured from an
unuploaded local write, even when only another audience covers that number. -/
theorem covered_miss_published {s : State} (run : Run s) (writer position : Nat)
    (covered : coveredMiss s.landed writer position = true) :
    ∃ bytes, (s.logs writer).published[position]? = some bytes := by
  obtain ⟨snap, member, covered⟩ := List.any_eq_true.mp covered
  have facts : snap.counts = true ∧ position < snap.position writer := by simpa using covered
  have within := facts.2
  simpa using snapshot_covers_published run snap member writer (position + 1) (by omega) (by omega)

end CovenIO.SnapshotWrites
