import SqliveNarrowing.Plan

/-! Whether a write reruns a live query, given each table's scope
(`read_set.rs`): some row change of a table the statement reads inserts or
deletes a row, or changes a column the statement reads, and the row before
or after passes the table's scope. The columns a statement reads are those
its clauses name, and every column of a table it reads in a query whose rows
SQLite picks or a compound or core whose rows follow the order SQLite reads
its rows in (`unreported_columns.rs`). A join's lookups read the database the
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
def OutCol.refs (o : OutCol) : List (Nat × Nat) := o.exprs.flatMap Expr.refs

/-- The columns a core's clauses name. -/
def Core.refs (c : Core) : List (Nat × Nat) :=
  c.conds.flatMap (·.1.refs) ++ c.output.flatMap OutCol.refs ++ c.groupBy.flatMap Expr.refs

/-- Whether the expressions `sorted` sort the bindings of core `k` by the
rowid of each of its items, each a table, its first column (`Json.readDb`):
two bindings of such a core differ in some item's row, and so in its rowid
(`Pick.bindings_eq_of_rowids`). -/
def Statement.sortsByRowids (s : Statement) (k : Nat) (sorted : List Expr) : Bool :=
  (s.core k).items.all fun i => (s.table? i).isSome && sorted.contains (.col i 0)

/-- The result columns' expressions that query `q`'s ORDER BY sorts by. -/
def Statement.orderExprs (s : Statement) (q k : Nat) : List Expr :=
  (s.query q).orderBy.filterMap fun (p, _) =>
    match (s.core k).output[p]? with
    | some (.expr e) => some e
    | _ => none

/-- Whether query `q`'s ORDER BY tells every two of its rows apart. The query
is one SELECT, and either it aggregates with no GROUP BY, so it returns one
row, or it neither groups, aggregates, calls a window function nor is
DISTINCT, as sqlive refuses, and its ORDER BY sorts by the rowid of each of
its items as result columns (`Pick.fullyOrders_separates`). -/
def Statement.fullyOrders (s : Statement) (q : Nat) : Bool :=
  match (s.query q).arms with
  | [k] =>
    let c := s.core k
    (c.aggregated && c.groupBy.isEmpty) ||
      (!c.aggregated && !c.windowed && !c.distinct && s.sortsByRowids k (s.orderExprs q k))
  | _ => false

/-- Whether `e` can take a different value in each binding of a group of a
core that aggregates by `groupBy`: it names a column, or holds a subquery,
outside every GROUP BY term. -/
def Expr.bare (groupBy : List Expr) (e : Expr) : Bool :=
  !groupBy.contains e && match e with
    | .col .. | .sub _ => true
    | .lit _ => false
    | .add l r | .greatest l r | .coalesce l r => l.bare groupBy || r.bare groupBy

/-- The GROUP BY terms among `groupBy` whose values `e` shows: `e` itself
when it is one, or those its operands show. -/
def Expr.groupTerms (groupBy : List Expr) (e : Expr) : List Expr :=
  if groupBy.contains e then [e] else match e with
    | .add l r | .greatest l r | .coalesce l r => l.groupTerms groupBy ++ r.groupTerms groupBy
    | .col .. | .lit _ | .sub _ => []

/-- Whether two values of `e` in bindings of core `k` that compare equal are
the same value: `e` is a column of one of the core's tables that sqlive gives
an affinity other than BLOB, so it compares by BINARY and holds no integer
and real that compare equal, as an untyped column can hold `1` and `1.0`. -/
def Statement.comparesExactly (s : Statement) (k : Nat) : Expr → Bool
  | .col i c => (s.core k).items.contains i && (s.table? i).isSome &&
      (s.affinity i c).any (· != .blob)
  | _ => false

/-- Whether core `k`'s rows follow the order SQLite reads its bindings in: it
aggregates, and a result column takes its value from one binding of a group,
which that order decides, as a column outside every GROUP BY term
(`Expr.bare`) does, or joins its values in that order, with no ORDER BY of its
own that tells the bindings apart, or keeps the greatest value it reads first
among values that compare equal and can differ, or shows a GROUP BY term that
can hold such values, of which the group keeps the one it reads first; or it
numbers its bindings in a window whose ORDER BY doesn't tell them apart, or
numbers its groups. That order follows the plan SQLite picks
(`Pick.bare_picks_any`, `Pick.concat_follows_order`, `Pick.max_keeps_first`,
`Pick.distinct_keeps_first`, `Pick.rowNumber_follows_order`). An aggregate's
call implies that the core aggregates. -/
def Statement.followsOrder (s : Statement) (k : Nat) : Bool :=
  let c := s.core k
  c.output.any fun
    | .expr e => c.aggregated &&
        (e.bare c.groupBy || (e.groupTerms c.groupBy).any (!s.comparesExactly k ·))
    | .concat _ orderBy => !s.sortsByRowids k (orderBy.map (·.1))
    | .max e => !s.comparesExactly k e
    | .rowNumber _ orderBy => c.aggregated || !s.sortsByRowids k (orderBy.map (·.1))
    | .count => false

/-- Whether core `k`'s rows follow the order SQLite reads its rows in: it is
DISTINCT, which keeps one of each set of rows that compare equal, the first it
reads, and a result column can hold values that compare equal and differ, as
any but a column whose equal values are the same can
(`Pick.distinct_keeps_first`). -/
def Statement.distinctFollowsOrder (s : Statement) (k : Nat) : Bool :=
  let c := s.core k
  c.distinct && c.output.any fun
    | .expr e => !s.comparesExactly k e
    | .count | .max _ | .concat .. | .rowNumber .. => true

/-- Whether query `q`'s rows follow the order SQLite reads its arms' rows in:
it combines its arms by UNION, INTERSECT or EXCEPT, which keep one of each set
of rows that compare equal, the one that order decides, and a result column
of an arm can hold values that compare equal and differ, as any but a column
whose equal values are the same can (`Pick.distinct_keeps_first`). -/
def Statement.compoundFollowsOrder (s : Statement) (q : Nat) : Bool :=
  let query := s.query q
  query.ops.any (· != .unionAll) && query.arms.any fun k =>
    (s.core k).output.any fun
      | .expr e => !s.comparesExactly k e
      | .count | .max _ | .concat .. | .rowNumber .. => true

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

/-- The items core `k` reads, with `read` giving the items a query reads:
its own, and those of the queries of its derived items and subqueries. -/
def Statement.coreItemsWith (s : Statement) (read : Nat → List Nat) (k : Nat) : List Nat :=
  let c := s.core k
  let derived := c.items.flatMap fun i =>
    match (s.item i).kind with
    | .derived q _ => read q
    | .table _ => []
  c.items ++ derived ++ c.subqueries.flatMap read

/-- The items query `q` reads: those of its arms' cores, and of the queries
those cores read, FROM items' and subqueries' alike. -/
def Statement.queryItems (s : Statement) (q : Nat) : List Nat :=
  go (s.queries.length + 1) q
where
  go : Nat → Nat → List Nat
    | 0, _ => []
    | f + 1, q => (s.query q).arms.flatMap (s.coreItemsWith (go f))

/-- The tables that a query whose rows SQLite picks, a compound whose rows
follow the order SQLite reads its arms' rows in, or a core whose rows follow
the order SQLite reads its bindings in, reads. -/
def Statement.pickedTables (s : Statement) : List Nat :=
  let picked := ((List.range s.queries.length).filter fun q =>
    s.picks q || s.compoundFollowsOrder q).flatMap s.queryItems
  let followed := ((List.range s.cores.length).filter fun k =>
    s.followsOrder k || s.distinctFollowsOrder k).flatMap (s.coreItemsWith s.queryItems)
  (picked ++ followed).filterMap s.table?

/-- How many columns table `t` has, as the items that read it list them. -/
def Statement.width (s : Statement) (t : Nat) : Nat :=
  ((s.itemNumbers.find? (s.table? · = some t)).map fun i => (s.item i).columns.length).getD 0

/-- Each table the statement reads, with the columns of it any of its
clauses names, and every column of a table that a query whose rows SQLite
picks, or a compound or core whose rows follow the order SQLite reads its
rows in, reads: what its rows can depend on. -/
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
