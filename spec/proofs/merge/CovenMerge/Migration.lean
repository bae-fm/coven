import CovenMerge.Circles

/-! Breaking migrations (§17.1). Re-evaluate all removal rules without
non-final circle deletions, then use the ordinary generation delete. Capture
the whole written row even though the migration had read its setters. -/

namespace CovenMerge

section
variable {W Row Col K : Type} [DecidableEq W] [DecidableEq Row] [DecidableEq K]

def independentInputs (I : EntryInputs Row K) (final : Nat → Bool) : EntryInputs Row K :=
  { I with inDeletedCircle := fun r => (I.inDeletedCircle r).filter final }

def migrationRemoves (I : EntryInputs Row K) (final : Nat → Bool) : Row → Bool :=
  (entryView (independentInputs I final)).removed

theorem independent_rule_order (I : EntryInputs Row K) (final : Nat → Bool)
    {D : Row → Bool}
    (h : let J := (independentInputs I final).erase
      Stratified J.rows (FiresP (fires J)) (rivalBefore J) (start J) D) :
    D = removal (independentInputs I final).erase := entry_rule_order _ h

/-- Inputs differing only in non-final deletions yield the same migration
decision, including children and unique claims, because all three passes rerun. -/
theorem nonfinal_does_not_decide (I : EntryInputs Row K) (final : Nat → Bool) :
    migrationRemoves (independentInputs I final) final = migrationRemoves I final := by
  have he : independentInputs (independentInputs I final) final = independentInputs I final := by
    cases I
    simp only [independentInputs, RemovalInputs.mk.injEq]
    refine ⟨trivial, trivial, trivial, trivial, ?_, trivial, trivial⟩
    funext r
    simp [Option.filter_filter]
  unfold migrationRemoves
  rw [he]

/-- The immutable row-change record authored by the migration. Other changed
cells use the supplied migration write; hidden rows become ordinary deletes. -/
def withHiddenDeletes (M : Writes W Row Col) (st : St W Row Col)
    (I : EntryInputs Row K) (final : Nat → Bool) (migration : W) : Writes W Row Col :=
  { M with chg := fun w r =>
      if w = migration then
        if migrationRemoves I final r then some ⟨.del, st.gen r, fun _ => false⟩
        else if (entryView I).removed r then none
        else M.chg w r
      else M.chg w r }

/-- A frozen loss owns its record, with no live parent links. `Col` keeps the
old column names; values identify the original setters, as in the core model.
Its owner never feeds it back into `step` or the current removal rules. -/
structure FrozenLoss (W Row Col : Type) where
  record : LossRecord W Row Col

/-- Freezing preserves the entire record, including its original cause. -/
def freeze (record : LossRecord W Row Col) : FrozenLoss W Row Col := ⟨record⟩

theorem freeze_preserves (record : LossRecord W Row Col) :
    (freeze record).record = record := rfl

/-- Any interpretation of the captured setter as a scalar also stays equal. -/
theorem freeze_preserves_values (value : W → Row → Col → α)
    (record : LossRecord W Row Col) (c : Col) :
    ((freeze record).record.values c).map (fun w => value w record.row c) =
      (record.values c).map (fun w => value w record.row c) := rfl

inductive PendingLoss (W Row Col : Type) where
  | active (record : LossRecord W Row Col)
  | frozen (loss : FrozenLoss W Row Col)

def PendingLoss.record : PendingLoss W Row Col → LossRecord W Row Col
  | .active r => r
  | .frozen r => r.record

/-- Dropping any part of a pending row loss freezes its whole captured row;
the caller's affected predicate comes from table/column identity (§17.1). -/
def freezeDropped (affected : LossRecord W Row Col → Bool) :
    PendingLoss W Row Col → PendingLoss W Row Col
  | .active r => if affected r then .frozen (freeze r) else .active r
  | .frozen r => .frozen r

theorem drop_preserves_pending (affected : LossRecord W Row Col → Bool)
    (loss : PendingLoss W Row Col) : (freezeDropped affected loss).record = loss.record := by
  cases loss with
  | active r => simp only [freezeDropped]; split <;> rfl
  | frozen r => rfl

theorem drop_freezes_affected (affected : LossRecord W Row Col → Bool)
    (r : LossRecord W Row Col) (h : affected r = true) :
    freezeDropped affected (.active r) = .frozen (freeze r) := by simp [freezeDropped, h]

theorem drop_preserves_all_pending (affected : LossRecord W Row Col → Bool)
    (losses : List (PendingLoss W Row Col)) :
    (losses.map (freezeDropped affected)).map PendingLoss.record =
      losses.map PendingLoss.record := by
  simp only [List.map_map]
  congr 1
  funext loss
  exact drop_preserves_pending affected loss

def captureHidden (st : St W Row Col) (I : EntryInputs Row K) (final : Nat → Bool)
    (migration : W) (version : Nat) (r : Row) : Option (FrozenLoss W Row Col) :=
  if migrationRemoves I final r then
    some (freeze ⟨r, st.gen r, none, st.cell r, .schemaChange version migration⟩)
  else none

structure Migrated (W Row Col : Type) where
  merged : St W Row Col
  frozen : Row → Option (FrozenLoss W Row Col)

def migrateHidden (M : Writes W Row Col) (st : St W Row Col)
    (I : EntryInputs Row K) (final : Nat → Bool) (migration : W) (version : Nat) :
    Migrated W Row Col :=
  ⟨step (withHiddenDeletes M st I final migration) st migration,
    captureHidden st I final migration version⟩

/-- A row hidden only by reversible causes is excluded from every part of
the migration write, not merely from its generated deletes (§17.1). -/
theorem nonfinal_only_untouched (M : Writes W Row Col) (st : St W Row Col)
    (I : EntryInputs Row K) (final : Nat → Bool) (m : W) (v : Nat) {r : Row}
    (hidden : (entryView I).removed r = true) (independent : migrationRemoves I final r = false) :
    atRow (migrateHidden M st I final m v).merged r = atRow st r ∧
    (migrateHidden M st I final m v).frozen r = none := by
  simp [migrateHidden, withHiddenDeletes, captureHidden, hidden, independent,
    atRow, step, genStep, genWriteStep, cellStep]

theorem hidden_generation_delete (M : Writes W Row Col) (st : St W Row Col)
    (I : EntryInputs Row K) (final : Nat → Bool) (m : W) (v : Nat) {r : Row}
    (h : migrationRemoves I final r = true) :
    (migrateHidden M st I final m v).merged.gen r = st.gen r + 1 ∧
    (∀ c, (migrateHidden M st I final m v).merged.cell r c = none) ∧
    (migrateHidden M st I final m v).frozen r =
      some (freeze ⟨r, st.gen r, none, st.cell r, .schemaChange v m⟩) := by
  simp [migrateHidden, withHiddenDeletes, captureHidden, h, step, genStep, cellStep]

theorem migration_keeps_generation_record (M : Writes W Row Col) (st : St W Row Col)
    (I : EntryInputs Row K) (final : Nat → Bool) (m : W) (v : Nat) {r : Row}
    (h : migrationRemoves I final r = true) (fresh : st.genWrite r (st.gen r + 1) = none) :
    (migrateHidden M st I final m v).merged.genWrite r (st.gen r + 1) = some m := by
  simp [migrateHidden, step, withHiddenDeletes, h, genWriteStep, fresh, minBy]

theorem hidden_delete_even (M : Writes W Row Col) (st : St W Row Col)
    (I : EntryInputs Row K) (final : Nat → Bool) (m : W) (v : Nat) {r : Row}
    (h : migrationRemoves I final r = true) (odd : st.gen r % 2 = 1) :
    (migrateHidden M st I final m v).merged.gen r % 2 = 0 := by
  rw [(hidden_generation_delete M st I final m v h).1]
  omega

/-- No live removed-row record survives an even generation. -/
theorem migration_removes_live_loss [DecidableEq Col]
    (M : Writes W Row Col) (st : St W Row Col) (I : EntryInputs Row K)
    (final : Nat → Bool) (m : W) (v : Nat) {r : Row}
    (h : migrationRemoves I final r = true) (odd : st.gen r % 2 = 1)
    (after : EntryInputs Row K)
    (present : after.present r = decide
      ((migrateHidden M st I final m v).merged.gen r % 2 = 1)) :
    lossRecord (migrateHidden M st I final m v).merged (entryView after) r none = none := by
  have he := hidden_delete_even M st I final m v h odd
  simp [lossRecord, entryView, view, EntryInputs.erase, present, he]

/-- A late, eligible edit of an already deleted incarnation leaves the row
unchanged and records its value against that incarnation's generation delete.
An old-schema write excluded at the boundary uses a frozen schema loss instead. -/
theorem late_edit_recorded (M : Writes W Row Col) (st : St W Row Col)
    (w d : W) (r : Row) (c : Col) (g : Nat) (sets : Col → Bool)
    (change : M.chg w r = some ⟨.upd, g, sets⟩) (hc : sets c = true)
    (older : g < st.gen r) (deleted : st.genWrite r (g + 1) = some d) :
    (step M st w).gen r = st.gen r ∧
    (step M st w).cell r c = st.cell r c ∧
    (step M st w).lost r c w = some (g, d) := by
  have ne : g ≠ st.gen r := by omega
  simp [step, change, genStep, cellStep, lostStep, Change.inc, ne, older, deleted, hc]

/-- The migration record is immutable: devices apply its authored changes,
not deletes recomputed from each arrival prefix. Thus the core convergence
theorem includes its deletes and every concurrent/late edit. -/
theorem migration_converges (M : Writes W Row Col) (st : St W Row Col)
    (I : EntryInputs Row K) (final : Nat → Bool) (m : W)
    (valid : Valid (withHiddenDeletes M st I final m)) {a b : List W}
    (ha : CausalOrder (withHiddenDeletes M st I final m) a)
    (hb : CausalOrder (withHiddenDeletes M st I final m) b)
    (same : ∀ w, w ∈ a ↔ w ∈ b) :
    a.foldl (step (withHiddenDeletes M st I final m)) St.init =
      b.foldl (step (withHiddenDeletes M st I final m)) St.init :=
  merge_converges valid ha hb same

/-- Capturing a migration's snapshot also agrees: equal author input sets
produce equal generation deletes and equal frozen whole-row records. -/
theorem migration_capture_converges (M : Writes W Row Col) (valid : Valid M)
    (inputs : St W Row Col → EntryInputs Row K) (final : Nat → Bool) (m : W) (v : Nat)
    {a b : List W} (ha : CausalOrder M a) (hb : CausalOrder M b)
    (same : ∀ w, w ∈ a ↔ w ∈ b) :
    migrateHidden M (a.foldl (step M) St.init) (inputs (a.foldl (step M) St.init)) final m v =
      migrateHidden M (b.foldl (step M) St.init) (inputs (b.foldl (step M) St.init)) final m v := by
  rw [merge_converges valid ha hb same]

end
end CovenMerge
