import SqliveNarrowing.Plan

/-! Whether a write reruns a live query, given each table's scope
(`read_set.rs`): some row change of a table the statement reads inserts or
deletes a row, or changes a column the statement reads, and the row before
or after passes the table's scope. A join's lookups read the database the
write leaves, as sqlive judges a write in the write's own transaction before
it commits. -/
namespace SqliveNarrowing

/-- One row operation of a write: the row before and after, `none` where
there is none (`RowChange`). -/
structure Change where
  table : Nat
  old : Option Row
  new : Option Row
  deriving Repr

/-- Whether a change touches what the statement reads of its row: a row
inserted or deleted, or a read column changed. -/
def changedIn (columns : List Nat) : Option Row → Option Row → Bool
  | some a, some b => columns.any fun c => a.cell c != b.cell c
  | none, none => false
  | _, _ => true

/-- The rows an item's lookups read in `db`: a table's, or a derived item's
query run alone. -/
def lookupRows (s : Statement) (db : Db) (i : Nat) : List Row :=
  match (s.item i).kind with
  | .table t => db.rows t
  | .derived q _ => evalQuery s db (s.queries.length + 1) Bind.empty q

/-- The columns a result column names. -/
def OutCol.refs : OutCol → List (Nat × Nat)
  | .expr e | .max e => e.refs
  | .count => []

/-- The columns a core's clauses name. -/
def Core.refs (c : Core) : List (Nat × Nat) :=
  c.conds.flatMap (·.1.refs) ++ c.output.flatMap OutCol.refs ++ c.groupBy.flatMap Expr.refs

/-- Each table the statement reads, with the columns of it any of its
clauses names: what its rows can depend on. -/
def Statement.reads (s : Statement) : List (Nat × List Nat) :=
  let refs := s.cores.flatMap Core.refs
  (s.itemNumbers.filterMap s.table?).eraseDups.map fun t =>
    (t, (refs.filterMap fun (i, c) => if s.table? i = some t then some c else none).eraseDups)

/-- Whether a write's `changes`, leaving `db'`, rerun the statement, which
reads the columns `reads` names of each table, given each table's scope. -/
def reruns (s : Statement) (scopeOf : Nat → Option RScope)
    (reads : List (Nat × List Nat)) (changes : List Change) (db' : Db) : Bool :=
  changes.any fun ch =>
    match reads.find? (·.1 == ch.table) with
    | none => false
    | some (_, columns) =>
      let scope := (scopeOf ch.table).getD .all
      let passes (r : Row) := scope.passes (lookupRows s db') r
      changedIn columns ch.old ch.new && (ch.old.any passes || ch.new.any passes)

end SqliveNarrowing
