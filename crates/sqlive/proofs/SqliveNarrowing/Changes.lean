import SqliveNarrowing.Plan

/-! Whether a write reruns a live query, given each table's scope
(`read_set.rs`): some row change of a table the statement reads inserts or
deletes a row, or changes a column the statement reads, and the row before
or after passes the table's scope. The columns a statement reads are those
its clauses name, and every column of a table it reads in a query whose rows
SQLite picks (`unreported_columns.rs`). A join's lookups read the database the
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

/-- Whether query `q`'s ORDER BY tells every two of its rows apart. The query
is one SELECT, and either it aggregates with no GROUP BY, so it returns one
row, or it neither groups nor aggregates, each of its FROM items is a table,
and the ORDER BY sorts by each one's rowid, its first column (`Json.readDb`),
as a result column. Two rows of such a query come from bindings that differ in
some item's row, and so in its rowid (`Pick.fullyOrders_separates`). -/
def Statement.fullyOrders (s : Statement) (q : Nat) : Bool :=
  match (s.query q).arms with
  | [k] =>
    let c := s.core k
    (c.aggregated && c.groupBy.isEmpty) ||
    (!c.aggregated && c.items.all fun i => (s.table? i).isSome &&
      (s.query q).orderBy.any fun (p, _) =>
        match c.output[p]? with
        | some (.expr (.col j 0)) => j == i
        | _ => false)
  | _ => false

/-- The subqueries whose first row a clause of the statement takes. -/
def Statement.firstRowQueries (s : Statement) : List Nat := s.cores.flatMap Core.firstRows

/-- Whether SQLite picks which rows query `q` returns by the order it reads
its rows in: the query keeps some of its rows, by a LIMIT or an OFFSET or as a
subquery whose first row a clause takes, and its ORDER BY doesn't tell every
two of them apart. That order follows the plan SQLite picks, as an index on a
column no clause names (`Pick.post_picks_any`). -/
def Statement.picks (s : Statement) (q : Nat) : Bool :=
  let query := s.query q
  (query.limit.isSome || query.offset.isSome || s.firstRowQueries.contains q) && !s.fullyOrders q

/-- The items query `q` reads: those of its arms' cores, and of the queries
those cores read, FROM items' and subqueries' alike. -/
def Statement.queryItems (s : Statement) (q : Nat) : List Nat :=
  go (s.queries.length + 1) q
where
  go : Nat → Nat → List Nat
    | 0, _ => []
    | f + 1, q => (s.query q).arms.flatMap fun k =>
      let c := s.core k
      let derived := c.items.flatMap fun i =>
        match (s.item i).kind with
        | .derived q' _ => go f q'
        | .table _ => []
      c.items ++ derived ++ c.subqueries.flatMap (go f)

/-- The tables that a query whose rows SQLite picks reads. -/
def Statement.pickedTables (s : Statement) : List Nat :=
  ((List.range s.queries.length).filter s.picks).flatMap fun q =>
    (s.queryItems q).filterMap s.table?

/-- How many columns table `t` has, as the items that read it list them. -/
def Statement.width (s : Statement) (t : Nat) : Nat :=
  ((s.itemNumbers.find? (s.table? · = some t)).map fun i => (s.item i).columns.length).getD 0

/-- Each table the statement reads, with the columns of it any of its
clauses names, and every column of a table a query whose rows SQLite picks
reads: what its rows can depend on. -/
def Statement.reads (s : Statement) : List (Nat × List Nat) :=
  let refs := s.cores.flatMap Core.refs
  (s.itemNumbers.filterMap s.table?).eraseDups.map fun t =>
    let named := refs.filterMap fun (i, c) => if s.table? i = some t then some c else none
    let picked := if s.pickedTables.contains t then List.range (s.width t) else []
    (t, (named ++ picked).eraseDups)

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
