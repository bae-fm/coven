import CovenMerge.Model
import CovenQueue.Model

namespace CovenQueue

inductive HiddenBy where
  | visible
  | provisionalCircleOnly
  | independentOrFinal
  deriving DecidableEq, Repr

/-- Removal causes are evaluated without provisional deleted-circle causes
before this choice (§17.1). Their computation belongs to the merge model. -/
def retireHidden (cause : HiddenBy) : Bool :=
  match cause with
  | .independentOrFinal => true
  | _ => false

theorem provisional_circle_not_retired : retireHidden .provisionalCircleOnly = false := rfl

structure FrozenCell where
  column : String
  scalar : Nat
  setter : WriteId
  deriving DecidableEq, Repr

/-- No live reference is retained in old-shape values (D7, §17.1). -/
structure FrozenRow where
  generation : Nat
  migration : WriteId
  version : Nat
  cells : List FrozenCell
  deriving DecidableEq, Repr

def freezeRow (generation : Nat) (migration : WriteId) (version : Nat)
    (values : List FrozenCell) : FrozenRow := ⟨generation, migration, version, values⟩

theorem frozen_values_keep_setters (g : Nat) (m : WriteId) (v : Nat) (cells : List FrozenCell) :
    (freezeRow g m v cells).cells = cells := rfl

/-- The hidden-row part of a breaking migration authors ordinary deletes.
Its row identities include table, key and audience, as in the merge model. -/
def migrationWrites {W Row Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Row Col) (s : CovenMerge.St W Row Col)
    (migration : W) (hidden : Row → HiddenBy) : CovenMerge.Writes W Row Col :=
  { M with chg := fun w row => if w = migration then
      if retireHidden (hidden row) then some ⟨.del, s.gen row, fun _ => false⟩ else none
    else M.chg w row }

/-- Deletion and the whole frozen row are one result of the atomic migration.
All pre-migration values are captured, including values read by the migration. -/
def migrateHiddenRows {W Row Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Row Col) (s : CovenMerge.St W Row Col)
    (migration : W) (migrationId : WriteId) (version : Nat)
    (hidden : Row → HiddenBy) (values : Row → List FrozenCell) :
    CovenMerge.St W Row Col × (Row → Option FrozenRow) :=
  (CovenMerge.step (migrationWrites M s migration hidden) s migration,
    fun row => if retireHidden (hidden row) then
      some (freezeRow (s.gen row) migrationId version (values row)) else none)

theorem hidden_row_retired_and_frozen {W Row Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Row Col) (s : CovenMerge.St W Row Col)
    (migration : W) (migrationId : WriteId) (version : Nat)
    (hidden : Row → HiddenBy) (values : Row → List FrozenCell)
    (row : Row) (column : Col) (n : Nat)
    (h : retireHidden (hidden row) = true) (live : s.gen row = 2 * n + 1)
    (future : s.genWrite row (2 * n + 2) = none) :
    let result := migrateHiddenRows M s migration migrationId version hidden values
    result.1.gen row = 2 * n + 2 ∧ result.1.cell row column = none ∧
      result.1.genWrite row (2 * n + 2) = some migration ∧
      result.2 row = some ⟨2 * n + 1, migrationId, version, values row⟩ := by
  simp [migrateHiddenRows, migrationWrites, h, freezeRow,
    CovenMerge.step, CovenMerge.genStep, CovenMerge.cellStep,
    CovenMerge.genWriteStep, CovenMerge.minBy, live, future, Nat.add_assoc]

theorem provisional_row_preserved {W Row Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Row Col) (s : CovenMerge.St W Row Col)
    (migration : W) (migrationId : WriteId) (version : Nat)
    (hidden : Row → HiddenBy) (values : Row → List FrozenCell)
    (row : Row) (h : hidden row = .provisionalCircleOnly) :
    let result := migrateHiddenRows M s migration migrationId version hidden values
    result.1.gen row = s.gen row ∧ result.1.cell row = s.cell row ∧
      result.1.genWrite row = s.genWrite row ∧ result.1.lost row = s.lost row ∧
      result.2 row = none := by
  simp [migrateHiddenRows, migrationWrites, h, retireHidden,
    CovenMerge.step, CovenMerge.genStep, CovenMerge.cellStep, CovenMerge.genWriteStep]

/-- The queue's converted edit keeps its old generation. This theorem uses
the existing merge's actual `step`, not a second implementation of merge.
The deletion keeps its generation record and the late value loses to it. -/
theorem retired_edit_loses {W Row Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Row Col) (s : CovenMerge.St W Row Col)
    (migration edit : W) (row : Row) (column : Col) (g : Nat)
    (hg : s.gen row = g)
    (future : s.genWrite row (g + 1) = none)
    (deleted : M.chg migration row = some ⟨.del, g, fun _ => false⟩)
    (changed : M.chg edit row = some ⟨.upd, g, fun _ => true⟩) :
    let retired := CovenMerge.step M s migration
    let after := CovenMerge.step M retired edit
    retired.gen row = g + 1 ∧ after.gen row = g + 1 ∧
      after.cell row column = none ∧ after.lost row column edit = some (g, migration) := by
  simp [CovenMerge.step, CovenMerge.genStep, CovenMerge.genWriteStep,
    CovenMerge.cellStep, CovenMerge.lostStep, CovenMerge.minBy, CovenMerge.Change.inc,
    hg, future, deleted, changed]

theorem migrated_hidden_row_rejects_late_edit {W Row Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Row Col) (s : CovenMerge.St W Row Col)
    (migration edit : W) (migrationId : WriteId) (version : Nat)
    (hidden : Row → HiddenBy) (values : Row → List FrozenCell)
    (row : Row) (column : Col) (g : Nat)
    (h : retireHidden (hidden row) = true) (distinct : edit ≠ migration)
    (hg : s.gen row = g) (future : s.genWrite row (g + 1) = none)
    (changed : M.chg edit row = some ⟨.upd, g, fun _ => true⟩) :
    let retired := (migrateHiddenRows M s migration migrationId version hidden values).1
    let after := CovenMerge.step (migrationWrites M s migration hidden) retired edit
    retired.gen row = g + 1 ∧ after.gen row = g + 1 ∧
      after.cell row column = none ∧ after.lost row column edit = some (g, migration) := by
  exact retired_edit_loses (migrationWrites M s migration hidden) s migration edit row column g
    hg future (by simp [migrationWrites, h, hg])
    (by simp [migrationWrites, distinct, changed])

/-- The row may be re-added later; an edit at the retired generation still
cannot change the new incarnation, however large its timestamp (§8.3). -/
theorem retired_edit_cannot_touch_readded {W Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Unit Col) (old : Option W) (edit : W) (column : Col)
    (g current : Nat) (h : g < current) :
    CovenMerge.cellStep M current old column edit (some ⟨.upd, g, fun _ => true⟩) = old := by
  simp [CovenMerge.cellStep, CovenMerge.Change.inc, Nat.ne_of_lt h]

end CovenQueue
