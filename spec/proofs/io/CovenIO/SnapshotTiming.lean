import CovenIO.Retention

/-! The provider sample belongs to the catch-up session. These results check
the executable publication actions, independently of position coverage.
Signing, byte retries and local upload abandonment are outside this model. -/

namespace CovenIO.SnapshotPublication

/-- Sampling starts a session before any of its read actions. Provider ticks
may be equal; event order, rather than a strict clock inequality, orders IO. -/
theorem begin_samples_time (order : SnapshotOrder) (s t : State)
    (audience writer number : Nat)
    (begun : act order s (.beginCatchup audience writer number) = some t) :
    ∃ c, t.catching = c :: s.catching ∧ c.snapshot.caughtAt = s.time ∧
      c.lastRead = s.time ∧ c.missed = [] := by
  simp only [act] at begun
  split at begun
  · split at begun <;> split at begun <;> cases begun <;>
      exact ⟨_, rfl, rfl, rfl, rfl⟩
  · cases begun

/-- Reads, including snapshot reloads, keep the session's original sample. -/
theorem read_preserves_sample (order : SnapshotOrder) (s t : State)
    (writer logWriter : Nat) (read : act order s (.read writer logWriter) = some t) :
    ∃ c ∈ s.catching, ∃ next,
      t.catching = next :: s.catching.filter (fun c => c.snapshot.writer != writer) ∧
      next.snapshot.caughtAt = c.snapshot.caughtAt ∧ next.lastRead = s.time := by
  simp only [act, Option.bind_eq_bind] at read
  cases found : s.catching.find? (fun c => c.snapshot.writer == writer) with
  | none => simp [found] at read
  | some c =>
    have member := List.mem_of_find?_eq_some found
    simp only [found, Option.bind_some] at read
    split at read
    · cases read
    · cases seen : live s logWriter (c.snapshot.position logWriter + 1) <;>
        simp only [seen, pure, Option.bind_some] at read
      · split at read
        · cases selected : current order s c.snapshot.audience <;>
            simp [selected] at read
          cases read
          exact ⟨c, member, _, rfl, rfl, rfl⟩
        · cases read
          exact ⟨c, member, _, rfl, rfl, rfl⟩
      · cases read
        exact ⟨c, member, _, rfl, rfl, rfl⟩

/-- Preparation copies the sample into the pending remote request. -/
theorem prepare_preserves_sample (order : SnapshotOrder) (s t : State) (writer : Nat)
    (prepared : act order s (.prepare writer) = some t) :
    ∃ c ∈ s.catching,
      t.uploading = { c.snapshot with storedAt := s.time } :: s.uploading ∧
      t.catching = s.catching.filter (fun c => c.snapshot.writer != writer) := by
  simp only [act, Option.bind_eq_bind] at prepared
  cases found : s.catching.find? (fun c => c.snapshot.writer == writer) with
  | none => simp [found] at prepared
  | some c =>
    simp only [found, Option.bind_some] at prepared
    split at prepared
    · cases prepared
      exact ⟨c, List.mem_of_find?_eq_some found, rfl, rfl⟩
    · cases prepared

/-- Remote completion changes publication time, never the captured sample. -/
theorem land_preserves_sample (order : SnapshotOrder) (s t : State)
    (audience writer number : Nat)
    (landed : act order s (.land audience writer number) = some t) :
    ∃ snap ∈ s.uploading,
      t.landed = { snap with storedAt := s.time } :: s.landed := by
  simp only [act, Option.bind_eq_bind] at landed
  cases found : s.uploading.find? (fun snap =>
      snap.audience == audience && snap.writer == writer && snap.number == number) with
  | none => simp [found] at landed
  | some snap =>
    simp only [found, Option.bind_some, Option.some.injEq] at landed
    subst t
    exact ⟨snap, List.mem_of_find?_eq_some found, rfl⟩

private theorem act_time (order : SnapshotOrder) (s t : State) (action : Action)
    (taken : act order s action = some t) : t.time = s.time := by
  cases action <;> simp only [act, Option.bind_eq_bind, Option.bind, pure] at taken
  all_goals
    repeat' first | split at taken | dsimp only at taken
    all_goals cases taken <;> rfl

/-- Reachable catch-up sessions carry a sample no later than provider time. -/
def SamplesBeforeTime (s : State) : Prop :=
  ∀ c ∈ s.catching, c.snapshot.caughtAt ≤ s.time

private theorem act_samples_before_time (order : SnapshotOrder) (s t : State)
    (action : Action) (taken : act order s action = some t)
    (before : SamplesBeforeTime s) : SamplesBeforeTime t := by
  have time := act_time order s t action taken
  intro c member
  rw [time]
  cases action with
  | beginCatchup audience writer number =>
    obtain ⟨fresh, cs, sample, _, _⟩ := begin_samples_time order s t audience writer number taken
    rw [cs] at member
    rcases List.mem_cons.mp member with rfl | old
    · omega
    · exact before c old
  | read writer logWriter =>
    obtain ⟨old, oldMember, next, cs, sample, _⟩ := read_preserves_sample order s t writer logWriter taken
    rw [cs] at member
    rcases List.mem_cons.mp member with rfl | retained
    · rw [sample]; exact before old oldMember
    · exact before c (List.mem_filter.mp retained).1
  | prepare writer =>
    obtain ⟨_, _, _, cs⟩ := prepare_preserves_sample order s t writer taken
    rw [cs] at member
    exact before c (List.mem_filter.mp member).1
  | write writer audiences | land audience writer number | delete writer number posted final | tick =>
    simp only [act, Option.bind_eq_bind, Option.bind] at taken
    repeat' first | split at taken | dsimp only at taken
    all_goals cases taken <;> exact before c member

private theorem execute_samples_before_time (order : SnapshotOrder) (s t : State)
    (events : List (Nat × Action)) (run : execute order s events = some t)
    (before : SamplesBeforeTime s) : SamplesBeforeTime t := by
  induction events generalizing s with
  | nil => cases run; exact before
  | cons event rest ih =>
    simp only [execute] at run
    cases taken : step order s event.1 event.2 with
    | none => simp [taken] at run
    | some next =>
      apply ih next (by simpa [taken] using run)
      simp only [step] at taken
      split at taken
      · rename_i later
        apply act_samples_before_time order _ next event.2 taken
        intro c member
        exact Nat.le_trans (before c member) later
      · cases taken

theorem run_samples_before_time (order : SnapshotOrder) (writers : List Nat) (s : State)
    (run : Run order writers s) : SamplesBeforeTime s := by
  obtain ⟨events, run⟩ := run
  exact execute_samples_before_time order _ s events run (by simp [SamplesBeforeTime, initial])

/-- Every successful read in a reachable execution uses the preceding
session sample, including reads performed after a snapshot reload. -/
theorem read_uses_prior_sample (order : SnapshotOrder) (writers : List Nat) (s t : State)
    (run : Run order writers s) (time writer logWriter : Nat)
    (read : step order s time (.read writer logWriter) = some t) :
    ∃ c ∈ s.catching, ∃ next,
      t.catching = next :: s.catching.filter (fun c => c.snapshot.writer != writer) ∧
      next.snapshot.caughtAt = c.snapshot.caughtAt ∧ next.snapshot.caughtAt ≤ time := by
  simp only [step] at read
  split at read
  · rename_i later
    obtain ⟨c, member, next, cs, sample, _⟩ := read_preserves_sample order _ t writer logWriter read
    refine ⟨c, member, next, cs, sample, ?_⟩
    rw [sample]
    exact Nat.le_trans (run_samples_before_time order writers s run c member) later
  · cases read

end CovenIO.SnapshotPublication
