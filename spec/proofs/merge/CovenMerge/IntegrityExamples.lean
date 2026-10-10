import CovenMerge.Boundary
import CovenMerge.Examples

namespace CovenMerge.IntegrityExamples

/-! §17.1: a non-final deletion hides a parent and its child; a third row
also fails CHECK. Independent removal must leave the first two intact. -/

def circleInputs (deleted : Option Nat) : EntryInputs Nat Nat where
  rows := [7, 8, 9]
  present r := r ∈ [7, 8, 9]
  refs r := if r = 9 then [⟨7, false⟩] else []
  checkFails r := r = 8
  inDeletedCircle r := if r = 7 ∨ r = 8 then deleted else none
  claims _ := []
  rank := id

theorem nonfinal_parent_and_child :
    (entryView (circleInputs (some 4))).removed 7 = true ∧
    (entryView (circleInputs (some 4))).rules 9 = [.foreignKey] ∧
    migrationRemoves (circleInputs (some 4)) (fun _ => false) 7 = false ∧
    migrationRemoves (circleInputs (some 4)) (fun _ => false) 9 = false ∧
    migrationRemoves (circleInputs (some 4)) (fun _ => false) 8 = true ∧
    migrationRemoves (circleInputs (some 4)) (fun _ => true) 7 = true ∧
    migrationRemoves (circleInputs (some 4)) (fun _ => true) 9 = true := by decide

def circleState : St Nat Nat Nat := [1, 5].foldl (step DeletedCircle.M) St.init

def migratedCircle := migrateHidden DeletedCircle.M circleState
  (circleInputs (some 4)) (fun _ => false) 6 2

def afterMigration (deletion : Option Nat) : EntryInputs Nat Nat :=
  { circleInputs deletion with present := fun r => migratedCircle.merged.gen r % 2 == 1 }

theorem migration_retains_reversible_rows :
    migratedCircle.merged.gen 7 = 1 ∧ migratedCircle.merged.cell 7 0 = some 1 ∧
    migratedCircle.merged.gen 9 = 1 ∧ migratedCircle.merged.cell 9 0 = some 5 ∧
    migratedCircle.merged.gen 8 = 2 ∧ migratedCircle.merged.cell 8 0 = none ∧
    migratedCircle.merged.genWrite 8 2 = some 6 ∧
    (migratedCircle.frozen 8).map (fun loss =>
      (loss.record.column, loss.record.values 0, loss.record.cause)) =
      some (none, some 1, LossCause.schemaChange 2 6) ∧
    (migratedCircle.frozen 7).isNone = true ∧
    (migratedCircle.frozen 9).isNone = true ∧
    (entryView (afterMigration none)).shown 7 = true ∧
    (entryView (afterMigration none)).shown 9 = true ∧
    (entryView (afterMigration none)).shown 8 = false := by decide

/-! Three writes are sufficient: insert, migration delete, concurrent edit.
The migration read the insert; the later-stamped edit did not read migration. -/

def migrationWrites : Writes (Fin 3) Unit Unit where
  ts w := w.val
  past w a := w != 0 && a == 0
  chg w _ := if w = 0 then some ⟨.ins, 0, fun _ => true⟩
    else if w = 1 then some ⟨.del, 1, fun _ => false⟩
    else some ⟨.upd, 1, fun _ => true⟩

theorem migration_writes_valid : Valid migrationWrites where
  ts_inj _ _ h := Fin.ext h
  past_ts w a h := by
    simp [migrationWrites] at h ⊢
    omega
  gen_seen w r ch h := by
    simp only [migrationWrites] at h
    split at h
    · cases h; exact Or.inl rfl
    · rename_i hw
      have hp : migrationWrites.past w 0 = true := by simp [migrationWrites, hw]
      split at h <;> cases h <;>
        exact Or.inr ⟨0, ⟨.ins, 0, fun _ => true⟩, hp, rfl, by decide, rfl⟩
  parity w r ch h := by
    simp only [migrationWrites] at h
    split at h
    · cases h; decide
    · split at h <;> cases h <;> decide

def beforeMigration : St (Fin 3) Unit Unit := step migrationWrites St.init 0

def hidden : EntryInputs Unit Unit where
  rows := [()]
  present _ := true
  refs _ := []
  checkFails _ := true
  inDeletedCircle _ := none
  claims _ := []
  rank _ := 0

def captured := migrateHidden migrationWrites beforeMigration hidden (fun _ => false) 1 2

theorem migration_and_late_edit :
    let early := ([0, 2, 1] : List (Fin 3)).foldl (step migrationWrites) (St.init : St (Fin 3) Unit Unit)
    let late := step migrationWrites captured.merged 2
    early.gen () = 2 ∧ late.gen () = 2 ∧
    early.cell () () = none ∧ late.cell () () = none ∧
    early.lost () () 2 = some (1, 1) ∧ late.lost () () 2 = some (1, 1) ∧
    late.lost () () 0 = none ∧
    (captured.frozen ()).map (fun loss => (loss.record.values (), loss.record.cause)) =
      some (some 0, LossCause.schemaChange 2 1) := by decide

def migrationBoundary : SnapshotBoundary (Fin 3) :=
  ⟨fun w => w != 2, fun w => w != 2, fun _ h => h⟩

def migrationSnapshot : MergeSnapshot (Fin 3) Unit Unit :=
  ⟨captured.merged, (captured.frozen ()).toList⟩

def migratedInputs (st : St (Fin 3) Unit Unit) : EntryInputs Unit Unit :=
  { hidden with present := fun r => st.gen r % 2 == 1 }

def migrationDelivery (schema : SchemaDisposition) :=
  boundaryDevice migrationWrites migrationBoundary none (fun _ _ => false)
    (fun _ => schema) (fun w _ _ => some w) migratedInputs migrationSnapshot [2]

/-- No reset exists: a converted late edit applies and loses to the migration
delete. An excluded edit keeps a frozen loss instead. A losing migration's
computed value is replaced by the selected snapshot without a new loss. -/
theorem migration_without_reset :
    (migrationDelivery .eligible).current.merged.lost () () 2 = some (1, 1) ∧
    ((migrationDelivery .eligible).excluded 2 ()).isNone = true ∧
    (migrationDelivery (.excluded 2)).current.merged.lost () () 2 = none ∧
    ((migrationDelivery (.excluded 2)).excluded 2 ()).map (fun loss =>
      (loss.record.values (), loss.record.cause)) = some (some 2, .schemaChange 2 2) ∧
    (migrationDelivery .losingMigration).current.merged.cell () () = none ∧
    (migrationDelivery .losingMigration).current.merged.lost () () 2 = none ∧
    ((migrationDelivery .losingMigration).excluded 2 ()).isNone = true ∧
    (migrationDelivery .losingMigration).snapshotFrozen.length = 1 := by decide

/-- Dropping a column keeps a pending cell loss's cause; dropping a table
keeps every column of a whole-row loss. Neither needs a current schema. -/
def blueLoss : LossRecord Nat Nat String :=
  ⟨7, 1, some "color", fun c => if c = "color" then some 2 else none, .write 3⟩

def wholeLoss : LossRecord Nat Nat String :=
  ⟨7, 1, none, fun c => if c = "color" then some 2 else if c = "title" then some 1 else none,
    .rules [.check]⟩

theorem dropped_schema_keeps_losses :
    (freezeDropped (fun _ => true) (.active blueLoss)).record.column = some "color" ∧
    (freezeDropped (fun _ => true) (.active blueLoss)).record.values "color" = some 2 ∧
    (freezeDropped (fun _ => true) (.active blueLoss)).record.cause = .write 3 ∧
    (freezeDropped (fun _ => true) (.active wholeLoss)).record.values "title" = some 1 ∧
    (freezeDropped (fun _ => true) (.active wholeLoss)).record.cause = .rules [.check] := by decide

/-! §8: put the removed circle row into the store under the same key. The
store copy remains the only shown row when the deletion drops. -/

def copies (deleted : Option Nat) : EntryInputs Nat Nat where
  rows := [0, 1]
  present _ := true
  refs _ := []
  checkFails _ := false
  inDeletedCircle r := if r = 1 then deleted else none
  claims r := [⟨⟨[], none⟩, 7, r, true⟩]
  rank := id

theorem same_key_after_return :
    (entryView (copies (some 4))).shown 0 = true ∧
    (entryView (copies (some 4))).shown 1 = false ∧
    (entryView (copies none)).shown 0 = true ∧
    (entryView (copies none)).rules 1 = [.otherAudience] := by decide

/-! §19.3: snapshot covers 0, writes 1 and 2 read only old history, and write
3 read the kept reset as well as write 2. The author can have applied 1 and 2
before learning its own entry; installing the reset discards both. -/

def resetM : Writes (Fin 4) Unit Unit where
  ts w := w.val
  past w a := a.val < w.val
  chg w _ := if w = 0 then some ⟨.ins, 0, fun _ => true⟩
    else some ⟨.upd, 1, fun _ => true⟩

def resetBoundary : ResetBoundary (Fin 4) := ⟨⟨fun w => w == 0, fun w => w == 0, fun _ h => h⟩, 7⟩
def readReset (w : Fin 4) (e : Nat) : Bool := w == 3 && e == 7
def resetSnapshot : St (Fin 4) Unit Unit := step resetM St.init 0
def authorBefore : St (Fin 4) Unit Unit := [1, 2].foldl (step resetM) resetSnapshot
def resetAfter := reloadReset resetM resetBoundary readReset resetSnapshot [1, 2, 3]

theorem reset_writes_valid : Valid resetM where
  ts_inj _ _ h := Fin.ext h
  past_ts _ _ h := by simpa [resetM] using h
  gen_seen w r ch h := by
    simp only [resetM] at h
    split at h
    · cases h; exact Or.inl rfl
    · rename_i hw
      cases h
      refine Or.inr ⟨0, ⟨.ins, 0, fun _ => true⟩, ?_, rfl, by decide, rfl⟩
      simp [resetM]
      omega
  parity w r ch h := by
    simp only [resetM] at h
    split at h <;> cases h <;> decide

theorem reset_generations : ResetGenerations resetM resetBoundary readReset := by
  intro w r ch _ h
  simp only [resetM] at h
  split at h
  · cases h; exact Or.inl rfl
  · rename_i hw
    cases h
    refine Or.inr ⟨0, ⟨.ins, 0, fun _ => true⟩, ?_, by decide, rfl, by decide, rfl⟩
    simp [resetM]
    omega

theorem reset_snapshot_correct :
    IsSpec (resetWrites resetM resetBoundary readReset)
      (fun w => resetBoundary.inputs w = true) resetSnapshot := by
  have causal : CausalOrder (resetWrites resetM resetBoundary readReset) [0] := by
    apply CausalOrder.snoc .nil (by simp)
    intro a h
    simp [resetWrites, snapshotWrites, resetM] at h
  have spec := run_isSpec (reset_valid reset_writes_valid resetBoundary readReset reset_generations) causal
  apply isSpec_congr (fun w => by simp [resetBoundary]) spec

/-- Coverage can include writes that an earlier reset already ignored. Their
effects and dependency edges remain absent from the next selected snapshot. -/
def consumedBoundary : ResetBoundary (Fin 4) where
  entry := 7
  covered w := w.val < 3
  inputs w := w == 0
  inputs_covered w h := by simp at h; subst w; decide

theorem consumed_is_not_input :
    consumedBoundary.covered 2 = true ∧ consumedBoundary.inputs 2 = false ∧
    (resetWrites resetM consumedBoundary readReset).chg 2 () = none ∧
    (resetWrites resetM consumedBoundary readReset).past 3 2 = false ∧
    (reloadReset resetM consumedBoundary readReset resetSnapshot [1, 2, 3]).cell () () = some 3 ∧
    (reloadReset resetM consumedBoundary readReset resetSnapshot [1, 2, 3]).lost () () 2 = none := by decide

theorem reset_author_and_remote :
    authorBefore.cell () () = some 2 ∧
    (reloadReset resetM resetBoundary readReset resetSnapshot [1, 2]).cell () () = some 0 ∧
    resetAfter.cell () () = some 3 ∧ resetAfter.gen () = 1 ∧
    resetAfter.lost () () 1 = none ∧ resetAfter.lost () () 2 = none ∧
    (replaceAudience (fun (_ : Unit) => ()) () authorBefore resetAfter).cell () () = some 3 ∧
    (replaceAudience (fun (_ : Unit) => ()) () St.init resetAfter).cell () () = some 3 ∧
    (resetWrites resetM resetBoundary readReset).past 3 2 = false := by decide

theorem reset_before_schema_exclusion :
    disposition resetBoundary.toSnapshotBoundary (some resetBoundary.entry) readReset (fun _ => .excluded 2) 1 = .resetIgnored ∧
    (boundaryLoss resetBoundary.toSnapshotBoundary (some resetBoundary.entry) readReset (fun _ => .excluded 2) 1 () 1
      (fun (_ : Unit) => some 1)).isNone = true ∧
    disposition resetBoundary.toSnapshotBoundary (some resetBoundary.entry) readReset (fun _ => .eligible) 3 = .apply := by decide

end CovenMerge.IntegrityExamples
