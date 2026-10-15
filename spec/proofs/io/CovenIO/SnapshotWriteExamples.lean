import CovenIO.SnapshotWrites
import CovenIO.RepairedExamples

namespace CovenIO.SnapshotWriteExamples
open RepairedExamples

/-- One writer, with bytes for every confirmed historical number. Other
writers remain empty; these bytes stand for the decoded two-part writes. -/
def logOne : Publication.Writer := ⟨[[1]], [[1]], 1⟩
def logTwo : Publication.Writer := ⟨[[1], [2]], [[1], [2]], 2⟩

def oneWriter (log : Publication.Writer) : Nat → Publication.Writer :=
  fun writer => if writer = 0 then log else Publication.initial

def state (log : Publication.Writer) (captured requested landed : List Snapshot) : SnapshotWrites.State :=
  ⟨oneWriter log, captured, requested, landed⟩

theorem log_step (a b : Publication.Writer) (captured requested landed : List Snapshot)
    (step : Publication.Step a b) :
    SnapshotWrites.Step (state a captured requested landed) (state b captured requested landed) := by
  have change := SnapshotWrites.Step.log (state a captured requested landed) 0 b
    (by simpa [state, oneWriter] using step)
  have eq : SnapshotWrites.changeLog (state a captured requested landed) 0 b =
      state b captured requested landed := by
    simp only [SnapshotWrites.changeLog, state, oneWriter]
    congr 1
    funext w
    by_cases own : w = 0 <;> simp [oneWriter, own]
  exact eq ▸ change

def first : SnapshotWrites.State := state logOne [audienceOne] [audienceOne] [audienceOne]
def later : SnapshotWrites.State :=
  state logTwo [audienceZero, audienceOne] [audienceZero, audienceOne] [audienceZero, audienceOne]

theorem initial_run : SnapshotWrites.Run (state Publication.initial [] [] []) := by
  have eq : state Publication.initial [] [] [] = SnapshotWrites.initial := by
    simp only [state, SnapshotWrites.initial]
    congr 1
    funext w
    simp [oneWriter]
  rw [eq]; exact .initial

theorem first_run : SnapshotWrites.Run first := by
  have start := initial_run
  have reserved : SnapshotWrites.Run (state ⟨[[1]], [], 0⟩ [] [] []) :=
    .step start (log_step _ _ _ _ _ (.reserve _ [1]))
  have landed : SnapshotWrites.Run (state ⟨[[1]], [[1]], 0⟩ [] [] []) :=
    .step reserved (log_step _ _ _ _ _ (.land _ [1] [] rfl rfl))
  have confirmed : SnapshotWrites.Run (state logOne [] [] []) :=
    .step landed (log_step _ _ _ _ _ (.confirm _ rfl))
  have captured : SnapshotWrites.Run (state logOne [audienceOne] [] []) :=
    .step confirmed (.capture _ audienceOne (by
      refine ⟨by decide, ?_⟩
      intro writer other
      change writer ≠ 0 at other
      simp [state, oneWriter, audienceOne, Snapshot.position, Ne.symm other, other]))
  have requested : SnapshotWrites.Run (state logOne [audienceOne] [audienceOne] []) :=
    .step captured (.request _ audienceOne (by simp [state]) (by decide))
  exact .step requested (.land _ audienceOne (by simp [state]) 0)

theorem later_run : SnapshotWrites.Run later := by
  have reserved : SnapshotWrites.Run (state ⟨[[1], [2]], [[1]], 1⟩ [audienceOne] [audienceOne] [audienceOne]) :=
    .step first_run (log_step _ _ _ _ _ (.reserve _ [2]))
  have landed : SnapshotWrites.Run (state ⟨[[1], [2]], [[1], [2]], 1⟩ [audienceOne] [audienceOne] [audienceOne]) :=
    .step reserved (log_step _ _ _ _ _ (.land _ [2] [] rfl rfl))
  have confirmed : SnapshotWrites.Run (state logTwo [audienceOne] [audienceOne] [audienceOne]) :=
    .step landed (log_step _ _ _ _ _ (.confirm _ rfl))
  have captured : SnapshotWrites.Run (state logTwo [audienceZero, audienceOne] [audienceOne] [audienceOne]) :=
    .step confirmed (.capture _ audienceZero (by
      refine ⟨by decide, ?_⟩
      intro writer other
      change writer ≠ 0 at other
      simp [state, oneWriter, audienceZero, Snapshot.position, Ne.symm other, other]))
  have requested : SnapshotWrites.Run (state logTwo [audienceZero, audienceOne]
      [audienceZero, audienceOne] [audienceOne]) :=
    .step captured (.request _ audienceZero (by simp [state]) (by decide))
  exact .step requested (.land _ audienceZero (by simp [state]) 1)

/-- Ana's old write has not landed. Capture can contain it; sending the
snapshot is forbidden, whether it is ordinary or belongs to a raise. -/
def pending : SnapshotWrites.State := state ⟨[[1]], [], 0⟩ [audienceOne] [] []

theorem pending_run : SnapshotWrites.Run pending := by
  have reserved : SnapshotWrites.Run (state ⟨[[1]], [], 0⟩ [] [] []) :=
    .step initial_run (log_step _ _ _ _ _ (.reserve _ [1]))
  exact .step reserved (.capture _ audienceOne (by
    refine ⟨by decide, ?_⟩
    intro writer other
    change writer ≠ 0 at other
    simp [state, oneWriter, audienceOne, Snapshot.position, Ne.symm other, other]))

/-- Captured, but not confirmed, so not publishable. -/
theorem pending_unpublished : SnapshotWrites.Captured pending audienceOne ∧
    ¬ audienceOne.position audienceOne.writer ≤ (pending.logs audienceOne.writer).confirmed ∧
    ¬ SnapshotWrites.Published pending audienceOne := by
  refine ⟨⟨by decide, ?_⟩, by decide, ?_⟩
  · intro writer other
    change writer ≠ 0 at other
    simp [pending, state, oneWriter, audienceOne, Snapshot.position, Ne.symm other, other]
  · intro published
    have bad := published 0
    change 1 ≤ 0 at bad
    omega

/-- Without the confirmation gate, that locally captured store snapshot
would label the absent two-part write as covered, losing its circle part. -/
example : (pending.logs 0).published[0]? = none ∧
    coveredMiss [audienceOne] 0 0 = true ∧
    catalogCurrent .writes [audienceOne] 0 = none := by decide

/-- The store snapshot is publishable only with write 1 already present.
The other audience obtains its part from that log, rather than skipping it. -/
theorem two_audience_raise_keeps_circle_part (order : SnapshotOrder) :
    SnapshotWrites.Run first ∧
    (first.logs 0).published[0]? = some [1] ∧
    FixedReload.scan
      (fun _ => Publication.objects (first.logs 0) .write 0 (fun _ => 0))
      (fun _ => ⟨0, first.landed, []⟩) order 0 none [] [firstPart] 0 =
        .complete [firstPart] := by
  exact ⟨first_run, by decide, by cases order <;> decide⟩

end CovenIO.SnapshotWriteExamples
