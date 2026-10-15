import CovenIO.FixedReload

namespace CovenIO.AwayRead

/-! §15's away-device read of one writer's log. Retention deletes a write
only once the store's current newest counting snapshot covers it
(`Write.holders`), and every member reads the store. So a miss whose number
the store's current newest snapshot, in a catalog listed after the missing
GET, reaches is answered by loading that snapshot, and any other miss is of a
write not yet stored at its GET. The read never passes a number. -/

inductive Result where
  /-- A terminal miss at `position + 1`, GET at `time`, after receiving the
  listed numbers. -/
  | miss (time position : Nat) (received : List Nat)
  /-- The store's current newest snapshot in the catalog listed at `time`
  reaches the missing `number`: the device loads it. -/
  | reload (time number : Nat)
  | exhausted
  deriving DecidableEq, Repr

/-- How far the store's current newest counting snapshot in the catalog
listed at `time` reaches on `writer`. -/
def storeReach (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (time writer : Nat) : Nat :=
  FixedReload.position (catalogCurrent order (history time).landed store) writer

/-- GET `position + 1` at `time`; on a miss, list the store's snapshots at
`time + 1`. Fuel bounds this finite execution, not the log. -/
def scan (world : World) (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (writer : Nat) : Nat → Nat → Nat → Result
  | 0, _, _ => .exhausted
  | fuel + 1, time, position =>
      match world time (.log .write writer (position + 1)) with
      | some _ =>
          match scan world history order writer fuel (time + 1) (position + 1) with
          | .miss t q received => .miss t q ((position + 1) :: received)
          | other => other
      | none =>
          if position < storeReach history order (time + 1) writer then .reload (time + 1) (position + 1)
          else .miss time position []

/-- The storage the away read observes, with the `SnapshotHistory` catalogs
and deletion ledger. -/
structure Source (order : SnapshotOrder) (writer : Nat) where
  world : World
  history : Nat → SnapshotHistory.State
  published : Write → Prop
  runs : ∀ t, SnapshotHistory.Run published order (history t)
  /-- A write stored before a GET is missing only once deleted, and the
  catalog listed after that GET includes the deletion. -/
  absent : ∀ t w, published w → w.writer = writer → w.storedAt < t →
    world t (.log .write writer w.number) = none → w ∈ (history (t + 1)).deleted

/-- The store's current newest snapshot reaches every deleted write. -/
theorem store_reaches_deleted (source : Source order writer) (t : Nat) (w : Write)
    (deleted : w ∈ (source.history t).deleted) :
    w.number ≤ storeReach source.history order t w.writer := by
  have covered := SnapshotHistory.deleted_stays_covered (source.runs t) w deleted store (store_holds w)
  unfold storeReach
  cases selected : catalogCurrent order (source.history t).landed store with
  | none => simp [selected] at covered
  | some snap =>
      have hc : snap.covers w store := by simpa [selected] using covered
      simpa [FixedReload.position] using hc.2

/-- What a terminal miss establishes. -/
structure MissFacts (source : Source order writer) (time position t q : Nat) (received : List Nat) : Prop where
  after : time ≤ t
  advanced : position ≤ q
  terminal : source.world t (.log .write writer (q + 1)) = none
  unreached : storeReach source.history order (t + 1) writer ≤ q
  read : ∀ n ∈ received, position < n ∧ n ≤ q ∧
    ∃ t' o, time ≤ t' ∧ source.world t' (.log .write writer n) = some o
  accounted : ∀ n, position < n → n ≤ q → n ∈ received

/-- Every number up to a terminal miss was received by an actual GET. -/
theorem miss_spec (source : Source order writer) (fuel : Nat) :
    ∀ time position t q received,
      scan source.world source.history order writer fuel time position = .miss t q received →
      MissFacts source time position t q received := by
  induction fuel with
  | zero => intro _ _ _ _ _ result; cases result
  | succ fuel ih =>
      intro time position t q received result
      simp only [scan] at result
      cases hw : source.world time (.log .write writer (position + 1)) with
      | some o =>
          simp only [hw] at result
          cases rest : scan source.world source.history order writer fuel (time + 1) (position + 1) with
          | miss t' q' received' =>
              simp only [rest, Result.miss.injEq] at result
              obtain ⟨rfl, rfl, rfl⟩ := result
              have facts := ih (time + 1) (position + 1) _ _ _ rest
              refine ⟨by have := facts.after; omega, by have := facts.advanced; omega,
                facts.terminal, facts.unreached, ?_, ?_⟩
              · intro n member
                rcases List.mem_cons.mp member with rfl | tail
                · exact ⟨by omega, facts.advanced, time, o, Nat.le_refl _, hw⟩
                · obtain ⟨low, high, t', o', when, found⟩ := facts.read n tail
                  exact ⟨by omega, high, t', o', by omega, found⟩
              · intro n low high
                by_cases first : n = position + 1
                · simp [first]
                · exact List.mem_cons_of_mem _ (facts.accounted n (by omega) high)
          | reload t' n => simp [rest] at result
          | exhausted => simp [rest] at result
      | none =>
          simp only [hw] at result
          by_cases reach : position < storeReach source.history order (time + 1) writer
          · simp [reach] at result
          · simp only [reach, ↓reduceIte, Result.miss.injEq] at result
            obtain ⟨rfl, rfl, rfl⟩ := result
            exact ⟨Nat.le_refl _, Nat.le_refl _, hw, by omega, by simp, fun n low high => by omega⟩

/-- A reload is asked for only by the store's current newest snapshot,
listed after the missing read, reaching the missing number. -/
theorem reload_spec (source : Source order writer) (fuel : Nat) :
    ∀ time position t n,
      scan source.world source.history order writer fuel time position = .reload t n →
      position < n ∧ n ≤ storeReach source.history order t writer := by
  induction fuel with
  | zero => intro _ _ _ _ result; cases result
  | succ fuel ih =>
      intro time position t n result
      simp only [scan] at result
      cases hw : source.world time (.log .write writer (position + 1)) with
      | some o =>
          simp only [hw] at result
          cases rest : scan source.world source.history order writer fuel (time + 1) (position + 1) with
          | miss t' q' received' => simp [rest] at result
          | reload t' n' =>
              simp only [rest] at result
              cases result
              have facts := ih (time + 1) (position + 1) t n rest
              exact ⟨by omega, facts.2⟩
          | exhausted => simp [rest] at result
      | none =>
          simp only [hw] at result
          by_cases reach : position < storeReach source.history order (time + 1) writer
          · simp only [reach, ↓reduceIte, Result.reload.injEq] at result
            obtain ⟨rfl, rfl⟩ := result
            exact ⟨by omega, by omega⟩
          · simp [reach] at result

/-- The terminal number was not yet stored at its missing GET: had it been,
it would be deleted, and the store's snapshot would reach it. -/
theorem terminal_unstored (source : Source order writer) (fuel time position t q : Nat)
    (received : List Nat)
    (result : scan source.world source.history order writer fuel time position = .miss t q received)
    (w : Write) (pub : source.published w) (writerEq : w.writer = writer) (numberEq : w.number = q + 1) :
    t ≤ w.storedAt := by
  have facts := miss_spec source fuel time position t q received result
  apply Nat.le_of_not_gt
  intro stored
  have deleted := source.absent t w pub writerEq stored (by rw [numberEq]; exact facts.terminal)
  have reached := store_reaches_deleted source (t + 1) w deleted
  have short := facts.unreached
  rw [writerEq] at reached
  omega

/-- Publication order: gap-free numbers stored in number order (§6). -/
structure Ordered (source : Source order writer) : Prop where
  prefixClosed : ∀ v, source.published v → v.writer = writer → ∀ n, 0 < n → n ≤ v.number →
    ∃ u, source.published u ∧ u.writer = writer ∧ u.number = n
  inOrder : ∀ u v, source.published u → source.published v → u.writer = writer → v.writer = writer →
    u.number ≤ v.number → u.storedAt ≤ v.storedAt

/-- The read passes no number and leaves no stored write behind: every number
up to the terminal miss was received by an actual GET, and every write past
it was stored no earlier than that miss, so a checkpoint sampled before the
read protects them all (§15). -/
theorem store_covered_never_passes (source : Source order writer) (ordered : Ordered source)
    (fuel time position t q : Nat) (received : List Nat)
    (result : scan source.world source.history order writer fuel time position = .miss t q received) :
    (∀ n, position < n → n ≤ q → n ∈ received ∧
      ∃ t' o, time ≤ t' ∧ source.world t' (.log .write writer n) = some o) ∧
    ∀ w, source.published w → w.writer = writer → q < w.number → t ≤ w.storedAt := by
  have facts := miss_spec source fuel time position t q received result
  refine ⟨fun n low high => ⟨facts.accounted n low high, (facts.read n (facts.accounted n low high)).2.2⟩, ?_⟩
  intro w pub writerEq beyond
  obtain ⟨u, pu, uWriter, uNumber⟩ := ordered.prefixClosed w pub writerEq (q + 1) (by omega) (by omega)
  have first := terminal_unstored source fuel time position t q received result u pu uWriter uNumber
  have order := ordered.inOrder u w pu pub uWriter writerEq (by omega)
  omega

/-- The away read receives every write of this writer past its start that
was stored before the terminal miss. -/
theorem away_complete (source : Source order writer) (ordered : Ordered source)
    (fuel time position t q : Nat) (received : List Nat)
    (result : scan source.world source.history order writer fuel time position = .miss t q received)
    (w : Write) (pub : source.published w) (writerEq : w.writer = writer) (beyond : position < w.number)
    (stored : w.storedAt < t) :
    w.number ∈ received ∧ ∃ t' o, time ≤ t' ∧ source.world t' (.log .write writer w.number) = some o := by
  have never := store_covered_never_passes source ordered fuel time position t q received result
  apply never.1 w.number beyond
  apply Nat.le_of_not_gt
  intro beyondMiss
  have := never.2 w pub writerEq beyondMiss
  omega

end CovenIO.AwayRead
