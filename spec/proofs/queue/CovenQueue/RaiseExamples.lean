import CovenQueue.Examples
import CovenQueue.RaiseCaptures

namespace CovenQueue.RaiseExamples
open Examples

def ben (n : Nat) : CheckedWrite :=
  ⟨record 1 (n + 9) (if n < 2 then 1 else 2) [] (if n < 2 then [] else [10]), none⟩
def noConversion : Conversion := ⟨2, none⟩
def benBoundary : View := ⟨some ⟨⟨10, [⟨1, 8⟩]⟩, 2⟩, none, []⟩

theorem ben_valid : DataValid ben where
  uniqueRows w := by simp [ben, record]
  earlier w r a ha := by
    by_cases row : 0 = r <;>
      simp [provenance, rowChange, ben, record, row,
        CovenMerge.DataDependency.Provenance.sources] at ha
  sourcePresent w r a ha := by
    by_cases row : 0 = r <;>
      simp [provenance, rowChange, ben, record, row,
        CovenMerge.DataDependency.Provenance.sources] at ha

theorem ben_nine_without_conversion :
    receive noConversion benBoundary ben ben_valid 0 =
      ({ (ben 0).record with disposition := .lost 2 }, .excluded 10 2) := by decide

theorem ben_ten_without_conversion :
    receive noConversion benBoundary ben ben_valid 1 =
      ({ (ben 1).record with disposition := .lost 2 }, .excluded 10 2) := by decide

/-- With no callback both original records upload unchanged and retain losses.
After adoption, write 11 uses the winning state and applies. -/
theorem ben_losses_and_new_write :
    receivedLoss ben 0 (receive noConversion benBoundary ben ben_valid 0).2 = some (10, (ben 0).record) ∧
    receivedLoss ben 1 (receive noConversion benBoundary ben ben_valid 1).2 = some (10, (ben 1).record) ∧
    (receive noConversion benBoundary ben ben_valid 2).2 = .applied := by decide

theorem ben_nine_converted :
    receive rename benBoundary ben ben_valid 0 =
      ({ (ben 0).record with version := 2, body := [⟨0, 0, .ins, ⟨none, []⟩, .values [99, 42]⟩] }, .applied) := by
  have kept : CovenMerge.DataDependency.retained (preparedState rename benBoundary ben ben_valid) 0 0 = true := by
    rw [CovenMerge.DataDependency.retained_eq]; rfl
  have loss : conversionLosses rename benBoundary ben ben_valid 0 0 = none := by
    change (if CovenMerge.DataDependency.retained (preparedState rename benBoundary ben ben_valid) 0 0
      then none else some _) = none
    rw [kept]; rfl
  change ({ (ben 0).record with version := 2, body := [convertChange (fun xs => 99 :: xs)
    (conversionLosses rename benBoundary ben ben_valid 0)
      ⟨0, 0, .ins, ⟨none, []⟩, .values [42]⟩] }, Verdict.applied) = _
  simp [convertChange, loss]


/-- Both late records convert on receive independently of their upload attempts.
Recorded reads alone cannot block the second record's unrelated row change. -/
theorem ben_ten_converted :
    receive rename benBoundary ben ben_valid 1 =
      ({ (ben 1).record with version := 2, body := [⟨0, 0, .ins, ⟨none, []⟩, .values [99, 42]⟩] }, .applied) := by
  have kept : CovenMerge.DataDependency.retained (preparedState rename benBoundary ben ben_valid) 1 0 = true := by
    rw [CovenMerge.DataDependency.retained_eq]; rfl
  have loss : conversionLosses rename benBoundary ben ben_valid 1 0 = none := by
    change (if CovenMerge.DataDependency.retained (preparedState rename benBoundary ben ben_valid) 1 0
      then none else some _) = none
    rw [kept]; rfl
  change ({ (ben 1).record with version := 2, body := [convertChange (fun xs => 99 :: xs)
    (conversionLosses rename benBoundary ben ben_valid 1)
      ⟨0, 0, .ins, ⟨none, []⟩, .values [42]⟩] }, Verdict.applied) = _
  simp [convertChange, loss]


/-- Ana's migration captures old write 1 plus empty marker 2. New app write 3
is committed behind both. The snapshot and entry wait only through 2. -/
def migratedQueue : Queue := RaiseUpload.migrate committed.queue 2 (record 0 2 2).identity

def afterMigration : World := { committed with
  queue := migratedQueue.commit (record 0 3 2) }

def plan : RaiseUpload.Plan := ⟨2, [⟨0, 1⟩, ⟨0, 2⟩], [90], [10]⟩

theorem capture_retains_old_record : afterMigration.queue.waiting.map Pending.record =
    [firstRecord, { record 0 2 2 with disposition := .migration, body := [] }, record 0 3 2] := by
  decide

example : ¬ RaiseUpload.Confirmed (fun _ => False) plan afterMigration ∧
    Uploadable 1 2 firstRecord ∧
    Uploadable 1 2 ({ record 0 2 2 with disposition := .migration, body := [] }) ∧
    ¬ Uploadable 1 2 (record 0 3 2) := by
  refine ⟨?_, by decide, by decide, by decide⟩
  intro h
  have impossible : ¬ plan.through ≤ afterMigration.queue.settled := by decide
  exact impossible h.1

/-- There is an actual queue/send/landing/confirmation schedule, with the
store still on 1, that publishes the raise while app write 3 remains queued. -/
theorem raise_publishes_with_new_tail :
    ∃ t, RaiseUpload.Run encode (fun _ => False) plan afterMigration ⟨t, .entryStored⟩ ∧
      t.queue.settled = 2 ∧ t.queue.waiting = [.untried (record 0 3 2)] := by
  let front : List Pending := [.untried firstRecord,
    .untried { record 0 2 2 with disposition := .migration, body := [] }]
  exact RaiseUpload.no_raise_deadlock encode (fun _ => False) plan afterMigration 2 1 2 2
    [request] [key] front [.untried (record 0 3 2)]
    (by change Numbered 0 1 _; exact .cons rfl (.cons rfl (.cons rfl (.nil _)))) (by decide) (by decide)
    (by intro p hp; simp [front] at hp
        rcases hp with rfl | rfl <;> decide)
    (by decide) ⟨request, [], rfl, rfl, rfl⟩
    (by intro q _
        exact ⟨⟨q.reserved, 1, 1⟩, some ⟨some q.device, false, ⟨q.settled, 0, 0⟩⟩,
          rfl, by simp [checkIdentity, Queue.reserved, Nat.not_lt_of_ge (Nat.le_add_right q.settled q.waiting.length)]⟩)
    (by intro p _; exact Or.inl rfl) (by decide) (by decide)

/-- Ana writes 1, migrates to 2 with marker 2, writes 3, then migrates to 3
with marker 4 before raise 2 ever published, and writes 5. Her second capture
replaces the first and runs through 4. -/
def twoMigrations : RaiseUpload.Local :=
  (((((⟨.empty 0, 1, 0, 0⟩ : RaiseUpload.Local).appWrite (record 0 0 1)).migrated 2
    (record 0 0 2).identity).appWrite (record 0 0 2)).migrated 3
    (record 0 0 3).identity).appWrite (record 0 0 3)

theorem two_migrations_reach : RaiseUpload.LocalReach encode 0 1 twoMigrations :=
  .next (.next (.next (.next (.next .start (.write _ _)) (.migrate _ 2 _ (by decide)))
    (.write _ _)) (.migrate _ 3 _ (by decide))) (.write _ _)

/-- Write 3 is newer than store version 1 and no raise to 2 will be kept,
so a gate comparing it with the kept version alone would hold it, and with
it the raise to 3, forever. It belongs to the later capture: with the store
still at 1, records 1 to 4 pass the gate and 5 waits for raise 3. -/
theorem between_write_joins_later_prefix :
    twoMigrations.through = 4 ∧ twoMigrations.publication = 3 ∧
    (RaiseUpload.records twoMigrations.world.queue).map (·.version) = [1, 2, 2, 3, 3] ∧
    (RaiseUpload.records twoMigrations.world.queue).map
      (fun r => decide (Uploadable 1 twoMigrations.publication r)) =
      [true, true, true, true, false] := by decide

/-- And the raise to 3 publishes after confirming 1 to 4, with write 5 left. -/
theorem between_write_raise_publishes :
    ∃ t, RaiseUpload.Run encode (fun _ => False) ⟨4, [⟨0, 1⟩, ⟨0, 2⟩, ⟨0, 3⟩, ⟨0, 4⟩], [], []⟩
      twoMigrations.world ⟨t, .entryStored⟩ ∧ 4 ≤ t.queue.settled :=
  RaiseUpload.later_capture_never_deadlocks encode (fun _ => False) two_migrations_reach
    _ 1 2 [request] [key] (by decide) (by decide) ⟨request, [], rfl, rfl, rfl⟩
    (by intro q _
        exact ⟨⟨q.reserved, 1, 1⟩, some ⟨some q.device, false, ⟨q.settled, 0, 0⟩⟩,
          rfl, by simp [checkIdentity, Queue.reserved, Nat.not_lt_of_ge (Nat.le_add_right q.settled q.waiting.length)]⟩)
    (by intro p _; exact Or.inl rfl) (by decide) (by decide)

/-- If a competing raise wins first, Ana keeps the same old upload bytes.
Her reload and another receiver use the same selected boundary and callback. -/
theorem competing_raise_converges (c : Conversion) (v : View) (h : PendingHistory)
    (valid : DataValid (checkedHistory h)) (w : Nat)
    (good : ∀ n, (checkedHistory h n).refusal = none) :
    Received c v (checkedHistory h) valid w (receive c v (checkedHistory h) valid w) ∧
    (rebuild c v ⟨h, checkedHistory h, valid⟩).uploads = h :=
  ⟨every_write_received c v (checkedHistory h) valid good w, rfl⟩

/-- Another device's raise is already kept when Ana starts her old upload.
That remains an old-schema attempt; absence/presence of the callback decides
loss versus receive conversion, identically on Ana and her peers. -/
theorem concurrent_raise_old_write :
    Uploadable 2 0 firstRecord ∧
    receive noConversion raised (checkedHistory untriedHistory) untried_valid 0 =
      ({ firstRecord with disposition := .lost 2 }, .excluded 10 2) ∧
    receive rename raised (checkedHistory untriedHistory) untried_valid 0 =
      ({ firstRecord with version := 2, body := [⟨0, 0, .ins, ⟨none, []⟩, .values [99, 42]⟩] }, .applied) ∧
    (rebuild rename raised ⟨untriedHistory, checkedHistory untriedHistory, untried_valid⟩).uploads 0 =
      (.untried firstRecord, none) := by
  refine ⟨by decide, by decide, ?_, by decide⟩
  have kept : CovenMerge.DataDependency.retained
      (preparedState rename raised (checkedHistory untriedHistory) untried_valid) 0 0 = true := by
    rw [CovenMerge.DataDependency.retained_eq]; rfl
  have loss : conversionLosses rename raised (checkedHistory untriedHistory) untried_valid 0 0 = none := by
    change (if CovenMerge.DataDependency.retained
      (preparedState rename raised (checkedHistory untriedHistory) untried_valid) 0 0 then none else some _) = none
    rw [kept]; rfl
  change ({ firstRecord with version := 2, body := [convertChange (fun xs => 99 :: xs)
    (conversionLosses rename raised (checkedHistory untriedHistory) untried_valid 0)
      ⟨0, 0, .ins, ⟨none, []⟩, .values [42]⟩] }, Verdict.applied) = _
  simp [convertChange, loss]

end CovenQueue.RaiseExamples
