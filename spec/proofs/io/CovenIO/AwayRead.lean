import CovenIO.FixedReload

namespace CovenIO.AwayRead

/-! §15's away-device read of one writer's log, past deleted numbers the
device has no part in. A miss is used only once a complete catalog of every
audience, listed after it, has no counting snapshot through that number. A
number a catalog listed *before* the missing read covers was published before
it, so its absence is a deletion: a readable audience whose current newest
counting snapshot, in the catalog listed after the read, reaches it reloads;
otherwise the write has no part in any readable audience and the read passes
the number. A number only the later catalog covers is read again, with that
catalog as the earlier one. -/

inductive Result where
  /-- A terminal miss at `position + 1`, GET at `time`, after receiving and
  passing the listed numbers. -/
  | miss (time position : Nat) (received skipped : List Nat)
  /-- A readable audience's current newest snapshot in the catalog listed at
  `time` reaches the deleted `number`: the device reloads it. -/
  | reload (time audience number : Nat)
  | exhausted
  deriving DecidableEq, Repr

/-- The first readable audience whose current newest counting snapshot in
`catalog` reaches past `position` on `writer`. -/
def reaching (order : SnapshotOrder) (catalog : List Snapshot) (readable : List Nat)
    (writer position : Nat) : Option Nat :=
  readable.find? fun a =>
    decide (position < FixedReload.position (catalogCurrent order catalog a) writer)

/-- GET `position + 1` at `time`. `evidence` is a complete catalog listed at
or before that GET; `history (time + 1)` is the complete catalog listed after
it. Fuel bounds this finite execution, not the log. -/
def scan (world : World) (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (readable : List Nat) (writer : Nat) : Nat → Nat → Nat → List Snapshot → Result
  | 0, _, _, _ => .exhausted
  | fuel + 1, time, position, evidence =>
      match world time (.log .write writer (position + 1)) with
      | some _ =>
          match scan world history order readable writer fuel (time + 1) (position + 1) evidence with
          | .miss t q received skipped => .miss t q ((position + 1) :: received) skipped
          | other => other
      | none =>
          if coveredMiss evidence writer position then
            match reaching order (history (time + 1)).landed readable writer position with
            | some a => .reload (time + 1) a (position + 1)
            | none =>
                match scan world history order readable writer fuel (time + 1) (position + 1) evidence with
                | .miss t q received skipped => .miss t q received ((position + 1) :: skipped)
                | other => other
          else if coveredMiss (history (time + 1)).landed writer position then
            scan world history order readable writer fuel (time + 1) position (history (time + 1)).landed
          else .miss time position [] []

/-- The rule that judges a miss by the catalog listed after it alone, which
`AwayReadExamples.after_catalog_skips_live_write` refutes. -/
def scanAfterOnly (world : World) (history : Nat → SnapshotHistory.State) (order : SnapshotOrder)
    (readable : List Nat) (writer : Nat) : Nat → Nat → Nat → Result
  | 0, _, _ => .exhausted
  | fuel + 1, time, position =>
      match world time (.log .write writer (position + 1)) with
      | some _ =>
          match scanAfterOnly world history order readable writer fuel (time + 1) (position + 1) with
          | .miss t q received skipped => .miss t q ((position + 1) :: received) skipped
          | other => other
      | none =>
          if coveredMiss (history (time + 1)).landed writer position then
            match reaching order (history (time + 1)).landed readable writer position with
            | some a => .reload (time + 1) a (position + 1)
            | none =>
                match scanAfterOnly world history order readable writer fuel (time + 1) (position + 1) with
                | .miss t q received skipped => .miss t q received ((position + 1) :: skipped)
                | other => other
          else .miss time position [] []

/-- The storage the away read observes. Catalogs and the deletion ledger are
the `SnapshotHistory` ones; checked publication supplies `listedStored`
(`SnapshotWrites.snapshot_covers_published`, strict for equal ticks). -/
structure Source (order : SnapshotOrder) (writer : Nat) where
  world : World
  history : Nat → SnapshotHistory.State
  published : Write → Prop
  unique : ∀ u v, published u → published v → u.writer = v.writer → u.number = v.number → u = v
  runs : ∀ t, SnapshotHistory.Run published order (history t)
  /-- A counting snapshot in the catalog listed at `t` covers only writes
  stored before `t`. -/
  listedStored : ∀ t s, s ∈ (history t).landed → s.counts = true → ∀ n, 0 < n →
    n ≤ s.position writer → ∃ w, published w ∧ w.writer = writer ∧ w.number = n ∧ w.storedAt < t
  /-- A write stored before a GET is missing only once deleted, and the
  catalog listed after that GET includes the deletion. -/
  absent : ∀ t w, published w → w.writer = writer → w.storedAt < t →
    world t (.log .write writer w.number) = none → w ∈ (history (t + 1)).deleted

/-- The earlier catalog was listed at or before the GET at `time`. -/
def Listed (history : Nat → SnapshotHistory.State) (evidence : List Snapshot) (time : Nat) : Prop :=
  ∃ e, e ≤ time ∧ evidence = (history e).landed

theorem reaching_some (order : SnapshotOrder) (catalog : List Snapshot) (readable : List Nat)
    (writer position a : Nat) (found : reaching order catalog readable writer position = some a) :
    a ∈ readable ∧ position < FixedReload.position (catalogCurrent order catalog a) writer := by
  unfold reaching at found
  exact ⟨List.mem_of_find?_eq_some found, by simpa using List.find?_some found⟩

theorem reaching_none (order : SnapshotOrder) (catalog : List Snapshot) (readable : List Nat)
    (writer position : Nat) (none : reaching order catalog readable writer position = none) :
    ∀ a ∈ readable, FixedReload.position (catalogCurrent order catalog a) writer ≤ position := by
  intro a member
  unfold reaching at none
  have := List.find?_eq_none.mp none a member
  simp at this
  exact this

/-- Coverage listed before the missing GET proves publication; the absence is
then a deletion that the catalog listed after the GET records. -/
theorem covered_deleted (source : Source order writer) (evidence : List Snapshot) (time position : Nat)
    (listed : Listed source.history evidence time)
    (covered : coveredMiss evidence writer position = true)
    (missing : source.world time (.log .write writer (position + 1)) = none) :
    ∃ w, source.published w ∧ w.writer = writer ∧ w.number = position + 1 ∧
      w ∈ (source.history (time + 1)).deleted := by
  obtain ⟨e, before, rfl⟩ := listed
  obtain ⟨s, member, check⟩ := List.any_eq_true.mp covered
  have facts : s.counts = true ∧ position < s.position writer := by simpa using check
  obtain ⟨w, pub, writerEq, numberEq, stored⟩ :=
    source.listedStored e s member facts.1 (position + 1) (by omega) (by omega)
  refine ⟨w, pub, writerEq, numberEq, source.absent time w pub writerEq (by omega) ?_⟩
  rw [numberEq]
  exact missing

/-- A deleted write that no readable audience's current newest snapshot
reaches has no part in any readable audience. -/
theorem passed_no_part (source : Source order writer) (readable : List Nat) (time position : Nat)
    (w : Write) (deleted : w ∈ (source.history time).deleted) (writerEq : w.writer = writer)
    (numberEq : w.number = position + 1)
    (none : reaching order (source.history time).landed readable writer position = none) :
    ∀ a ∈ readable, a ∉ w.audiences := by
  intro a member
  have bound := reaching_none order _ readable writer position none a member
  apply SnapshotHistory.deleted_outside_uncovered_audience (source.runs time) w deleted a
  intro snap selected
  rw [selected] at bound
  simp only [FixedReload.position] at bound
  rw [writerEq]
  omega

/-- What a terminal miss establishes. -/
structure MissFacts (source : Source order writer) (readable : List Nat)
    (time position t q : Nat) (received skipped : List Nat) : Prop where
  after : time ≤ t
  advanced : position ≤ q
  terminal : source.world t (.log .write writer (q + 1)) = none
  uncovered : coveredMiss (source.history (t + 1)).landed writer q = false
  read : ∀ n ∈ received, position < n ∧ n ≤ q ∧
    ∃ t' o, time ≤ t' ∧ source.world t' (.log .write writer n) = some o
  passed : ∀ n ∈ skipped, position < n ∧ n ≤ q ∧ ∃ w t', source.published w ∧
    w.writer = writer ∧ w.number = n ∧ w ∈ (source.history t').deleted ∧
    (∀ a ∈ readable, a ∉ w.audiences) ∧
    ∀ a ∈ readable, FixedReload.position (catalogCurrent order (source.history t').landed a) writer < n
  accounted : ∀ n, position < n → n ≤ q → n ∈ received ∨ n ∈ skipped

/-- Every number up to a terminal miss was received by an actual GET, or is
a deleted write with no part in any readable audience. -/
theorem miss_spec (source : Source order writer) (readable : List Nat) (fuel : Nat) :
    ∀ time position evidence t q received skipped, Listed source.history evidence time →
      scan source.world source.history order readable writer fuel time position evidence =
        .miss t q received skipped →
      MissFacts source readable time position t q received skipped := by
  induction fuel with
  | zero => intro _ _ _ _ _ _ _ _ result; cases result
  | succ fuel ih =>
      intro time position evidence t q received skipped listed result
      have later : Listed source.history evidence (time + 1) := by
        obtain ⟨e, before, eq⟩ := listed
        exact ⟨e, by omega, eq⟩
      simp only [scan] at result
      cases hw : source.world time (.log .write writer (position + 1)) with
      | some o =>
          simp only [hw] at result
          cases rest : scan source.world source.history order readable writer fuel (time + 1)
              (position + 1) evidence with
          | miss t' q' received' skipped' =>
              simp only [rest, Result.miss.injEq] at result
              obtain ⟨rfl, rfl, rfl, rfl⟩ := result
              have facts := ih (time + 1) (position + 1) evidence _ _ _ _ later rest
              refine ⟨by have := facts.after; omega, by have := facts.advanced; omega,
                facts.terminal, facts.uncovered, ?_, ?_, ?_⟩
              · intro n member
                rcases List.mem_cons.mp member with rfl | tail
                · exact ⟨by omega, facts.advanced, time, o, Nat.le_refl _, hw⟩
                · obtain ⟨low, high, t', o', when, found⟩ := facts.read n tail
                  exact ⟨by omega, high, t', o', by omega, found⟩
              · intro n member
                obtain ⟨low, high, rest⟩ := facts.passed n member
                exact ⟨by omega, high, rest⟩
              · intro n low high
                by_cases first : n = position + 1
                · exact Or.inl (by simp [first])
                · rcases facts.accounted n (by omega) high with got | gone
                  · exact Or.inl (List.mem_cons_of_mem _ got)
                  · exact Or.inr gone
          | reload t' a n => simp [rest] at result
          | exhausted => simp [rest] at result
      | none =>
          simp only [hw] at result
          by_cases covered : coveredMiss evidence writer position = true
          · simp only [covered, ↓reduceIte] at result
            cases reach : reaching order (source.history (time + 1)).landed readable writer position with
            | some a => simp [reach] at result
            | none =>
                simp only [reach] at result
                cases rest : scan source.world source.history order readable writer fuel (time + 1)
                    (position + 1) evidence with
                | miss t' q' received' skipped' =>
                    simp only [rest, Result.miss.injEq] at result
                    obtain ⟨rfl, rfl, rfl, rfl⟩ := result
                    have facts := ih (time + 1) (position + 1) evidence _ _ _ _ later rest
                    obtain ⟨w, pub, writerEq, numberEq, deleted⟩ :=
                      covered_deleted source evidence time position listed covered hw
                    have noPart := passed_no_part source readable (time + 1) position w deleted writerEq
                      numberEq reach
                    have short : ∀ a ∈ readable, FixedReload.position
                        (catalogCurrent order (source.history (time + 1)).landed a) writer < position + 1 :=
                      fun a member => Nat.lt_succ_of_le (reaching_none order _ readable writer position reach a member)
                    refine ⟨by have := facts.after; omega, by have := facts.advanced; omega,
                      facts.terminal, facts.uncovered, ?_, ?_, ?_⟩
                    · intro n member
                      obtain ⟨low, high, t', o', when, found⟩ := facts.read n member
                      exact ⟨by omega, high, t', o', by omega, found⟩
                    · intro n member
                      rcases List.mem_cons.mp member with rfl | tail
                      · exact ⟨by omega, facts.advanced, w, time + 1, pub, writerEq, numberEq, deleted, noPart, short⟩
                      · obtain ⟨low, high, rest⟩ := facts.passed n tail
                        exact ⟨by omega, high, rest⟩
                    · intro n low high
                      by_cases first : n = position + 1
                      · exact Or.inr (by simp [first])
                      · rcases facts.accounted n (by omega) high with got | gone
                        · exact Or.inl got
                        · exact Or.inr (List.mem_cons_of_mem _ gone)
                | reload t' a n => simp [rest] at result
                | exhausted => simp [rest] at result
          · simp only [covered, Bool.false_eq_true, ↓reduceIte] at result
            by_cases again : coveredMiss (source.history (time + 1)).landed writer position = true
            · simp only [again, ↓reduceIte] at result
              have facts := ih (time + 1) position (source.history (time + 1)).landed t q received skipped
                ⟨time + 1, Nat.le_refl _, rfl⟩ result
              refine ⟨by have := facts.after; omega, facts.advanced, facts.terminal, facts.uncovered,
                ?_, facts.passed, facts.accounted⟩
              intro n member
              obtain ⟨low, high, t', o', when, found⟩ := facts.read n member
              exact ⟨low, high, t', o', by omega, found⟩
            · simp only [again, Bool.false_eq_true, ↓reduceIte, Result.miss.injEq] at result
              obtain ⟨rfl, rfl, rfl, rfl⟩ := result
              refine ⟨Nat.le_refl _, Nat.le_refl _, hw, by simpa using again, ?_, ?_, ?_⟩
              · intro n member; cases member
              · intro n member; cases member
              · intro n low high; omega

/-- A reload is asked for only by a readable audience whose current newest
snapshot, listed after the missing read, reaches that deleted write. -/
theorem reload_spec (source : Source order writer) (readable : List Nat) (fuel : Nat) :
    ∀ time position evidence t a n, Listed source.history evidence time →
      scan source.world source.history order readable writer fuel time position evidence =
        .reload t a n →
      a ∈ readable ∧ position < n ∧
        n ≤ FixedReload.position (catalogCurrent order (source.history t).landed a) writer ∧
        ∃ w, source.published w ∧ w.writer = writer ∧ w.number = n ∧ w ∈ (source.history t).deleted := by
  induction fuel with
  | zero => intro _ _ _ _ _ _ _ result; cases result
  | succ fuel ih =>
      intro time position evidence t a n listed result
      have later : Listed source.history evidence (time + 1) := by
        obtain ⟨e, before, eq⟩ := listed
        exact ⟨e, by omega, eq⟩
      simp only [scan] at result
      cases hw : source.world time (.log .write writer (position + 1)) with
      | some o =>
          simp only [hw] at result
          cases rest : scan source.world source.history order readable writer fuel (time + 1)
              (position + 1) evidence with
          | miss t' q' received' skipped' => simp [rest] at result
          | reload t' a' n' =>
              simp only [rest] at result
              cases result
              have facts := ih (time + 1) (position + 1) evidence t a n later rest
              exact ⟨facts.1, by omega, facts.2.2⟩
          | exhausted => simp [rest] at result
      | none =>
          simp only [hw] at result
          by_cases covered : coveredMiss evidence writer position = true
          · simp only [covered, ↓reduceIte] at result
            cases reach : reaching order (source.history (time + 1)).landed readable writer position with
            | some b =>
                simp only [reach, Result.reload.injEq] at result
                obtain ⟨rfl, rfl, rfl⟩ := result
                have found := reaching_some order _ readable writer position _ reach
                obtain ⟨w, pub, writerEq, numberEq, deleted⟩ :=
                  covered_deleted source evidence time position listed covered hw
                exact ⟨found.1, by omega, by omega, w, pub, writerEq, numberEq, deleted⟩
            | none =>
                simp only [reach] at result
                cases rest : scan source.world source.history order readable writer fuel (time + 1)
                    (position + 1) evidence with
                | miss t' q' received' skipped' => simp [rest] at result
                | reload t' a' n' =>
                    simp only [rest] at result
                    cases result
                    have facts := ih (time + 1) (position + 1) evidence t a n later rest
                    exact ⟨facts.1, by omega, facts.2.2⟩
                | exhausted => simp [rest] at result
          · simp only [covered, Bool.false_eq_true, ↓reduceIte] at result
            by_cases again : coveredMiss (source.history (time + 1)).landed writer position = true
            · simp only [again, ↓reduceIte] at result
              exact ih (time + 1) position _ t a n ⟨time + 1, Nat.le_refl _, rfl⟩ result
            · simp [again] at result

/-- The terminal number was not yet stored at its missing GET, unless the
write has no part in any audience (a migration marker), which no catalog
need cover. -/
theorem terminal_unstored (source : Source order writer) (readable : List Nat) (fuel time position : Nat)
    (evidence : List Snapshot) (t q : Nat) (received skipped : List Nat)
    (listed : Listed source.history evidence time)
    (result : scan source.world source.history order readable writer fuel time position evidence =
      .miss t q received skipped)
    (w : Write) (pub : source.published w) (writerEq : w.writer = writer) (numberEq : w.number = q + 1)
    (parts : w.audiences ≠ []) : t ≤ w.storedAt := by
  have facts := miss_spec source readable fuel time position evidence t q received skipped listed result
  apply Nat.le_of_not_gt
  intro stored
  have deleted := source.absent t w pub writerEq stored (by rw [numberEq]; exact facts.terminal)
  obtain ⟨a, ha⟩ := List.exists_mem_of_ne_nil _ parts
  have detected := SnapshotHistory.deleted_miss_detected (source.runs (t + 1)) w deleted a ha q (by omega)
  rw [writerEq, facts.uncovered] at detected
  cases detected

/-- Publication order: gap-free numbers stored in number order (§6). -/
structure Ordered (source : Source order writer) : Prop where
  prefixClosed : ∀ v, source.published v → v.writer = writer → ∀ n, 0 < n → n ≤ v.number →
    ∃ u, source.published u ∧ u.writer = writer ∧ u.number = n
  inOrder : ∀ u v, source.published u → source.published v → u.writer = writer → v.writer = writer →
    u.number ≤ v.number → u.storedAt ≤ v.storedAt

/-- Every write of this writer past the terminal miss was stored no earlier
than that miss: a checkpoint sampled before the read protects them all. -/
theorem away_checkpoint (source : Source order writer) (ordered : Ordered source) (readable : List Nat)
    (fuel time position : Nat) (evidence : List Snapshot) (t q : Nat) (received skipped : List Nat)
    (listed : Listed source.history evidence time)
    (result : scan source.world source.history order readable writer fuel time position evidence =
      .miss t q received skipped)
    (parts : ∀ w, source.published w → w.writer = writer → w.audiences ≠ [])
    (w : Write) (pub : source.published w) (writerEq : w.writer = writer) (beyond : q < w.number) :
    t ≤ w.storedAt := by
  obtain ⟨u, pu, uWriter, uNumber⟩ := ordered.prefixClosed w pub writerEq (q + 1) (by omega) (by omega)
  have first := terminal_unstored source readable fuel time position evidence t q received skipped listed
    result u pu uWriter uNumber (parts u pu uWriter)
  have order := ordered.inOrder u w pu pub uWriter writerEq (by omega)
  omega

/-- The away read receives every write of this writer past its start that
was stored before the terminal miss and has a part in a readable audience. -/
theorem away_complete (source : Source order writer) (ordered : Ordered source) (readable : List Nat)
    (fuel time position : Nat) (evidence : List Snapshot) (t q : Nat) (received skipped : List Nat)
    (listed : Listed source.history evidence time)
    (result : scan source.world source.history order readable writer fuel time position evidence =
      .miss t q received skipped)
    (parts : ∀ w, source.published w → w.writer = writer → w.audiences ≠ [])
    (w : Write) (pub : source.published w) (writerEq : w.writer = writer) (beyond : position < w.number)
    (stored : w.storedAt < t) (relevant : ∃ a ∈ readable, a ∈ w.audiences) :
    w.number ∈ received ∧ ∃ t' o, time ≤ t' ∧ source.world t' (.log .write writer w.number) = some o := by
  have facts := miss_spec source readable fuel time position evidence t q received skipped listed result
  have within : w.number ≤ q := by
    apply Nat.le_of_not_gt
    intro beyondMiss
    have := away_checkpoint source ordered readable fuel time position evidence t q received skipped
      listed result parts w pub writerEq beyondMiss
    omega
  rcases facts.accounted w.number beyond within with got | gone
  · obtain ⟨_, _, rest⟩ := facts.read w.number got
    exact ⟨got, rest⟩
  · obtain ⟨_, _, v, t', pv, vWriter, vNumber, _, noPart, _⟩ := facts.passed w.number gone
    have eq := source.unique v w pv pub (vWriter.trans writerEq.symm) vNumber
    subst eq
    obtain ⟨a, member, part⟩ := relevant
    exact False.elim (noPart a member part)

/-- Were every deleted write also covered by the store's current newest
counting snapshot, a readable audience every member reads, the away read
would never pass a number: each deleted miss reloads instead. -/
theorem store_covered_never_passes (source : Source order writer) (readable : List Nat)
    (store : Nat) (member : store ∈ readable)
    (storeCovers : ∀ t w, w ∈ (source.history t).deleted →
      w.number ≤ FixedReload.position (catalogCurrent order (source.history t).landed store) w.writer)
    (fuel time position : Nat) (evidence : List Snapshot) (t q : Nat) (received skipped : List Nat)
    (listed : Listed source.history evidence time)
    (result : scan source.world source.history order readable writer fuel time position evidence =
      .miss t q received skipped) :
    skipped = [] := by
  have facts := miss_spec source readable fuel time position evidence t q received skipped listed result
  cases skipped with
  | nil => rfl
  | cons n rest =>
      obtain ⟨_, _, w, t', _, writerEq, numberEq, deleted, _, short⟩ := facts.passed n (by simp)
      have covered := storeCovers t' w deleted
      have below := short store member
      rw [writerEq] at covered
      omega

end CovenIO.AwayRead
