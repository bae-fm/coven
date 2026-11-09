import CovenMerge.Migration

/-!
# A column read in full was a reference when its setter wrote it (D7.3)

A device keeps an accepted change's record small: a column's value in full
only when the column was a reference under the schema the change's write was
made with, and every other value as a digest. Judging a later write reads
values in full only for the columns that are references under the schema
that write was made with: the cells its author's merge state named, through
their setters. So the kept records serve the judgement exactly when every
cell of a state at schema version `v` that `v` reads as a reference was
written as a reference by its setter: `Kept`.

This file proves the schema changes keep `Kept`: an addition (§18.1) and a
breaking migration's write (§18.4, §18.7) leave it true, given it held before,
and so does every write a device counts at its version whose columns `Fits`
says were references when written: its own and its peers' writes at that
version, writes made before additions only, and late writes whose converter
keeps a reference only from a column that was one (api.md E13). A device that
loads a snapshot takes the author's state, which `Kept` held on.

Columns are identities that renames carry (§18.4), so a column keeps its `Col`
across versions. `ref v r c` says column `c` of row `r`'s table is a reference
under schema version `v`, and `table v r` that the table exists at `v`;
`version w` is the schema version write `w` was made with.
-/

namespace CovenMerge.ReferenceSetters

open CovenMerge

section
variable {W Row Col : Type} [DecidableEq W]

/-- Every schema version, as the references it declares. -/
structure Schemas (Row Col : Type) where
  ref : Nat → Row → Col → Bool
  table : Nat → Row → Bool

variable (S : Schemas Row Col) (version : W → Nat)

/-- **The rule.** Every cell a state at schema version `v` holds, in a
column `v` reads as a reference, was set by a write that wrote it as a
reference. -/
def Kept (v : Nat) (st : St W Row Col) : Prop :=
  ∀ r c w, st.cell r c = some w → S.ref v r c = true → S.ref (version w) r c = true

/-- The empty database keeps the rule. -/
theorem initial_kept (v : Nat) : Kept S version v (St.init : St W Row Col) := by
  intro r c w h
  simp [St.init] at h

/-- A cell after one write is the cell before it, or the write's own, set by
its change to that row. -/
theorem cell_after_step (M : Writes W Row Col) (st : St W Row Col) (w : W) {r : Row} {c : Col}
    {x : W} (h : (step M st w).cell r c = some x) :
    st.cell r c = some x ∨ (x = w ∧ ∃ ch, M.chg w r = some ch ∧ ch.sets c = true) := by
  simp only [step] at h
  cases hch : M.chg w r with
  | none =>
    rw [hch] at h
    exact Or.inl h
  | some ch =>
    rw [hch] at h
    simp only [cellStep] at h
    split at h
    · split at h
      · next hs => exact Or.inr ⟨(Option.some.inj h).symm, ch, rfl, hs.2⟩
      · cases h
    · split at h
      · next hs =>
        simp only [maxBy] at h
        cases hold : st.cell r c with
        | none =>
          rw [hold] at h
          exact Or.inr ⟨(Option.some.inj h).symm, ch, rfl, hs.2.1⟩
        | some y =>
          rw [hold] at h
          by_cases later : M.ts y < M.ts w
          · simp only [later, ite_true] at h
            exact Or.inr ⟨(Option.some.inj h).symm, ch, rfl, hs.2.1⟩
          · simp only [later, ite_false] at h
            exact Or.inl h
      · exact Or.inl h

/-- A write fits a state at version `v` when every column it sets that `v`
reads as a reference was one under the write's own version. A write made at
`v` fits; so does one made before additions alone (`addition_fits`), and a
converted late write, whose converter refuses to make a column a reference
that its source column wasn't (api.md E13, Rust `migration_change`). -/
def Fits (M : Writes W Row Col) (v : Nat) (w : W) : Prop :=
  ∀ r ch c, M.chg w r = some ch → ch.sets c = true → S.ref v r c = true →
    S.ref (version w) r c = true

/-- A write made at the state's own version fits it. -/
theorem own_version_fits (M : Writes W Row Col) (v : Nat) (w : W) (made : version w = v) :
    Fits S version M v w := by
  intro r ch c _ _ hr
  rw [made]
  exact hr

/-- **Counting a fitting write keeps the rule.** -/
theorem write_keeps (M : Writes W Row Col) {v : Nat} {st : St W Row Col} {w : W}
    (kept : Kept S version v st) (fits : Fits S version M v w) :
    Kept S version v (step M st w) := by
  intro r c x h hr
  rcases cell_after_step M st w h with old | ⟨rfl, ch, hch, hs⟩
  · exact kept r c x old hr
  · exact fits r ch c hch hs hr

/-- §18.1: an addition adds tables, and columns without constraints, so a
column of a table that existed is a reference after it only if it was one
before. -/
def Addition (v : Nat) : Prop :=
  ∀ r c, S.table v r = true → S.ref (v + 1) r c = true → S.ref v r c = true

/-- A state holds cells only in tables its schema has. -/
def InSchema (v : Nat) (st : St W Row Col) : Prop :=
  ∀ r c w, st.cell r c = some w → S.table v r = true

/-- **An addition keeps the rule.** -/
theorem addition_keeps {v : Nat} {st : St W Row Col} (kept : Kept S version v st)
    (held : InSchema S v st) (addition : Addition S v) : Kept S version (v + 1) st := by
  intro r c w h hr
  exact kept r c w h (addition r c (held r c w h) hr)

/-- A write made before additions alone fits the state after them: each
column it sets, of a table its version has, that is a reference now, was one
then. -/
theorem addition_fits (M : Writes W Row Col) {v : Nat} {w : W}
    (fits : Fits S version M v w)
    (tables : ∀ r ch, M.chg w r = some ch → S.table v r = true)
    (addition : Addition S v) : Fits S version M (v + 1) w :=
  fun r ch c hch hs hr => fits r ch c hch hs (addition r c (tables r ch hch) hr)

/-- A breaking migration's write, one change per row (§18.4, §18.7): a row
removal rules hide is deleted at its generation; every other present row is
updated, setting each column whose reference status differs between the
schemas before (`v`) and after (`v'`), a cell whose references change counting
as changed, and each column whose value it changes (`changes`). -/
def migrationChange (st : St W Row Col) (v v' : Nat) (hidden : Row → Bool)
    (changes : Row → Col → Bool) (r : Row) : Option (Change Col) :=
  if st.gen r % 2 = 1 then
    if hidden r then some ⟨.del, st.gen r, fun _ => false⟩
    else some ⟨.upd, st.gen r, fun c => S.ref v' r c != S.ref v r c || changes r c⟩
  else none

/-- The writes with the migration write `m` making its changes. -/
def withMigration (M : Writes W Row Col) (st : St W Row Col) (v v' : Nat)
    (hidden : Row → Bool) (changes : Row → Col → Bool) (m : W) : Writes W Row Col :=
  { M with chg := fun w r => if w = m then migrationChange S st v v' hidden changes r else M.chg w r }

/-- A cell whose reference status the migration changes is the migration
write's afterwards, or gone with its hidden row: no earlier setter keeps it. -/
theorem migrated_cell (M : Writes W Row Col) (st : St W Row Col) {v v' : Nat}
    {hidden : Row → Bool} {changes : Row → Col → Bool} {m : W} {r : Row} {c : Col} {x : W}
    (present : st.gen r % 2 = 1) (newer : ∀ y, st.cell r c = some y → M.ts y < M.ts m)
    (differs : S.ref v' r c ≠ S.ref v r c)
    (h : (step (withMigration S M st v v' hidden changes m) st m).cell r c = some x) :
    x = m := by
  simp only [step, withMigration, migrationChange, present, ite_true] at h
  cases hh : hidden r
  · simp only [hh, Bool.false_eq_true, ite_false, cellStep] at h
    have sets : (S.ref v' r c != S.ref v r c || changes r c) = true := by
      simp [differs]
    simp only [ne_eq, not_true_eq_false, false_and, ite_false, reduceCtorEq,
      not_false_eq_true, sets, Change.inc, true_and, ite_true] at h
    simp only [maxBy] at h
    cases hold : st.cell r c with
    | none =>
      rw [hold] at h
      exact (Option.some.inj h).symm
    | some y =>
      rw [hold] at h
      have older := newer y hold
      simp only [older, ite_true] at h
      exact (Option.some.inj h).symm
  · simp only [hh, ite_true, cellStep] at h
    simp at h

/-- **A breaking migration keeps the rule.** The migration write is made at
the new version `v'`, after every cell's setter: its device had read them
all. Afterwards the rule holds at `v'`. -/
theorem migration_keeps (M : Writes W Row Col) {st : St W Row Col} {v v' : Nat}
    {hidden : Row → Bool} {changes : Row → Col → Bool} {m : W}
    (kept : Kept S version v st)
    (present : ∀ r c w, st.cell r c = some w → st.gen r % 2 = 1)
    (newer : ∀ r c y, st.cell r c = some y → M.ts y < M.ts m)
    (made : version m = v') :
    Kept S version v' (step (withMigration S M st v v' hidden changes m) st m) := by
  intro r c x h hr
  rcases cell_after_step _ st m h with old | ⟨rfl, _⟩
  · by_cases same : S.ref v' r c = S.ref v r c
    · exact kept r c x old (same ▸ hr)
    · have := migrated_cell S M st (present r c x old) (newer r c) same h
      subst this
      rw [made]
      exact hr
  · rw [made]
    exact hr

/-- Dropping tables and columns keeps the rule: it only removes cells. -/
theorem drop_keeps {v : Nat} {st : St W Row Col} (kept : Kept S version v st)
    (table : Row → Bool) (column : Row → Col → Bool) :
    Kept S version v (dropSchema st table column) := by
  intro r c w h hr
  simp only [dropSchema] at h
  split at h
  · cases h
  · exact kept r c w h hr

end

end CovenMerge.ReferenceSetters
