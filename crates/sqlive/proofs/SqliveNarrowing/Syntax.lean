import SqliveNarrowing.Value

/-! A statement as sqlive's scope planner reads it: SELECT cores, the FROM
items of each, the queries cores are arms of, and the conditions on their
rows. The statement is flat and numbered as sqlive's `Planner` numbers it, so
an item or a core here has sqlive's number. A subquery is a query number, so
expressions and conditions refer to it without nesting. -/
namespace SqliveNarrowing

/-- An expression over the columns of the items in scope. -/
inductive Expr where
  /-- Column `column` of item `item`'s row. -/
  | col (item column : Nat)
  | lit (value : Value)
  | add (left right : Expr)
  /-- `max(left, right)`: SQLite's scalar max of two values, NULL when either
  is, which reads one row as `+` does and is no aggregate. -/
  | greatest (left right : Expr)
  /-- A scalar subquery: the first column of its first row, or NULL. -/
  | sub (query : Nat)
  /-- `COALESCE(first, rest)`: `first`'s value, or `rest`'s when it is NULL.
  A COALESCE of more arguments nests in `rest`. -/
  | coalesce (first rest : Expr)
  /-- `CASE WHEN test THEN yes ELSE no END`, or `iif(test, yes, no)`: `yes`'s
  value where `test` holds a value other than 0, else `no`'s, so `no`'s where
  `test` is NULL. -/
  | iif (test yes no : Expr)
  /-- `e IS NULL` as a value: 1 where `e` is NULL, else 0, never NULL. -/
  | isNull (e : Expr)
  deriving Repr, Inhabited, DecidableEq

/-- A condition, in SQL's three-valued logic. -/
inductive Cond where
  | cmp (op : Op) (left right : Expr)
  /-- `left op right` under a collation other than BINARY, as SQLite compares a
  column that declares one: by the two values' keys, as `Dir.key` states a
  collation. The planner reads it as any condition SQLite judges. -/
  | collated (key : Int → Int) (op : Op) (left right : Expr)
  /-- `e IS NULL`, or `e IS NOT NULL` when `not`. -/
  | null (e : Expr) (not : Bool)
  | inList (e : Expr) (values : List Expr)
  /-- `e IN (query)`, or `NOT IN` when `not`: the query's first column. -/
  | inQuery (e : Expr) (query : Nat) (not : Bool)
  /-- `EXISTS (query)`, or `NOT EXISTS` when `not`. -/
  | exists (query : Nat) (not : Bool)
  /-- A row value compared with a subquery's row: `(e1, ..., en) = (query)`
  with the subquery on either side, or `(e1, ..., en) IN (query)` when
  `isIn`, `NOT IN` when `not`. -/
  | rowSub (values : List Expr) (query : Nat) (isIn : Bool) (not : Bool)
  /-- `(l1, ..., ln) = (r1, ..., rn)`. -/
  | rowEq (left right : List Expr)
  /-- A row value ordered against another, `(left, ...) op (right, ...)`, where
  `op` is `<`, `<=`, `>` or `>=`: its first pair, compared by the keys of
  `collation` when it names one, and `rest`, the same comparison of the other
  pairs, which for the last pair is `.cmp op` or `.collated`. -/
  | rowOrder (collation : Option (Int → Int)) (op : Op) (left right : Expr) (rest : Cond)
  | and (left right : Cond)
  | or (left right : Cond)
  | not (c : Cond)
  deriving Repr, Inhabited

/-- A column's affinity, when sqlive narrows by the column: a stored column
of BINARY collation. -/
inductive Affinity where
  | numeric | blob
  deriving DecidableEq, Repr, Inhabited

/-- What a FROM item reads rows from. -/
inductive ItemKind where
  | table (t : Nat)
  /-- A FROM subquery, a CTE or one of the app's views (`view`), read as
  query `query`'s cores. -/
  | derived (query : Nat) (view : Bool)
  deriving Repr, Inhabited, DecidableEq

structure Item where
  /-- The core whose FROM holds it. -/
  core : Nat
  kind : ItemKind
  /-- Whether a LEFT JOIN adds it. -/
  left : Bool
  /-- Each column's affinity, or `none` where a condition on it can't narrow. -/
  columns : List (Option Affinity)
  /-- Whether each column is a stored column of a collation other than
  BINARY, whose values sqlive compares in an ORDER BY term only by asking
  SQLite, and by no other condition (`ColumnValue::Collated`). A list that
  ends early says `false` for each column past its end. -/
  collated : List Bool := []
  /-- Whether sqlive can read its rows alone, in a lookup: a table, a view,
  and a FROM subquery or CTE outside any expression subquery or whose query
  names nothing outside it (`View.readable`). -/
  readable : Bool
  /-- Whether a parenthesized join of two tables or more, after the first
  item of its FROM, holds it: SQLite reads such a join through a `SELECT *`
  of its own, which reads every column of its tables. -/
  grouped : Bool := false
  /-- Whether sqlive knows its first column holds its table's rowid, as an
  INTEGER PRIMARY KEY does: a contentful FTS5 table's first column is a
  column of its own, whatever values it holds (`Virtual`). -/
  rowidFirst : Bool := true
  /-- The columns of each unique index of its table over plain columns, with
  no WHERE, each column in the collation the index compares it by, BINARY or
  the column's own. -/
  keys : List (List Nat) := []
  /-- Its table's columns declared NOT NULL. -/
  notNull : List Nat := []
  /-- Whether it is a FROM subquery written where it is read, not a CTE or one
  of the app's views. -/
  written : Bool := false
  deriving Repr, Inhabited

/-- Where a condition is written: the WHERE or an inner join's ON, or the ON
of the LEFT JOIN that adds `item`. -/
inductive Place where
  | inner
  | leftOn (item : Nat)
  deriving DecidableEq, Repr, Inhabited

/-- A result column. -/
inductive OutCol where
  | expr (e : Expr)
  /-- `count(*)`. -/
  | count
  /-- `max(e)`. -/
  | max (e : Expr)
  /-- `CAST(group_concat(e, '' ORDER BY ...) AS INTEGER)`: the values of `e`
  joined in the order SQLite reads the group's rows in, or in the order its
  own ORDER BY sorts them, each term descending when `true`. The model's
  values are integers, so the differential writes the joined text read back
  as one, which keeps the order. -/
  | concat (e : Expr) (orderBy : List (Expr × Dir))
  /-- `row_number() OVER (PARTITION BY ... ORDER BY ...)`, each ORDER BY term
  descending when `true`, in a core that doesn't aggregate: a binding's place
  among the bindings of its partition, in the order the window's ORDER BY
  sorts them and, among those that sort the same, in the order SQLite reads
  them in. -/
  | rowNumber (partition : List Expr) (orderBy : List (Expr × Dir))
  deriving Repr, Inhabited

/-- The expressions a result column holds. -/
def OutCol.exprs : OutCol → List Expr
  | .expr e | .max e => [e]
  | .count => []
  | .concat e orderBy => e :: orderBy.map (·.1)
  | .rowNumber partition orderBy => partition ++ orderBy.map (·.1)

/-- One SELECT core: the statement's, an arm of a compound, or a subquery's. -/
structure Core where
  /-- The core a subquery sits in, whose items its conditions can name. -/
  parent : Option Nat
  items : List Nat
  /-- Its conditions, each one conjunct, in the order sqlive reads them: each
  item's ON in FROM order, then the WHERE. -/
  conds : List (Cond × Place)
  output : List OutCol
  groupBy : List Expr
  /-- `SELECT DISTINCT`: it keeps one of each set of its rows that compare
  equal. -/
  distinct : Bool := false
  deriving Repr, Inhabited

/-- Whether a core makes one row of many: it groups or calls an aggregate. -/
def Core.aggregated (c : Core) : Bool :=
  !c.groupBy.isEmpty || c.output.any fun
    | .count | .max _ | .concat .. => true
    | .expr _ | .rowNumber .. => false

/-- Whether a core calls a window function, whose value in a row depends on
the core's other rows. -/
def Core.windowed (c : Core) : Bool :=
  c.output.any fun
    | .rowNumber .. => true
    | _ => false

/-- How a compound combines the rows so far with an arm's. -/
inductive Combine where
  | unionAll | union | except | intersect
  deriving DecidableEq, Repr, Inhabited

structure Query where
  /-- Its arms' cores, in order. -/
  arms : List Nat
  /-- The operator before each arm after the first. -/
  ops : List Combine
  /-- ORDER BY: result column positions, each with its direction and collation. -/
  orderBy : List (Nat × Dir)
  /-- Whether each ORDER BY term writes its result column's column or
  expression, which sqlive's LIMIT bound needs, rather than its position. -/
  orderNamed : Bool
  limit : Option Nat
  /-- OFFSET: how many of the ordered rows the query skips first. -/
  offset : Option Nat
  deriving Repr, Inhabited

/-- A shape that makes sqlive read every table of the statement whole. A
USING join is none: the JSON reader reads it as the equalities it stands for
(`Json.readCore`). Nor are two CTEs of one name: the JSON reader reads each
FROM item that names a CTE as the CTE SQLite resolves the name to
(`CteNames.resolve`). Nor is a parenthesized join: the JSON reader reads its
tables as items of the core, joined one after another, where that gives
SQLite's rows (`Parenthesized`), and refuses one where it doesn't. Nor is a
contentful FTS5 table: the schema lists it as a table, every column of no
affinity (`Virtual`); `virtual` is any other virtual table, and a hidden
column of an FTS5 table. -/
inductive WholeShape where
  | natural | right | recursive | inTable | virtual
  deriving DecidableEq, Repr, Inhabited

structure Statement where
  items : List Item
  cores : List Core
  queries : List Query
  root : Nat
  /-- The shapes in it that sqlive reads whole. -/
  whole : List WholeShape
  /-- The columns, each of a table, the statement reads that no clause of
  these cores names, as `Statement.pruned` keeps those of the statement it
  prunes (`Statement.unclaused`). -/
  reported : List (Nat × Nat) := []
  deriving Repr, Inhabited

def Statement.item (s : Statement) (i : Nat) : Item := s.items.getD i default
def Statement.core (s : Statement) (k : Nat) : Core := s.cores.getD k default
def Statement.query (s : Statement) (q : Nat) : Query := s.queries.getD q default

/-- The item numbers, from zero. -/
def Statement.itemNumbers (s : Statement) : List Nat := List.range s.items.length

/-- Whether item `i` reads a table, and which. -/
def Statement.table? (s : Statement) (i : Nat) : Option Nat :=
  match (s.item i).kind with
  | .table t => some t
  | .derived .. => none

/-- Whether every binding of item `i`'s core holds a row of `i`: no LEFT JOIN
adds it, whose left row no right row joins gives NULLs in its place. -/
def Statement.alwaysBound (s : Statement) (i : Nat) : Bool := !(s.item i).left

/-- The affinity sqlive narrows column `c` of item `i` by. -/
def Statement.affinity (s : Statement) (i c : Nat) : Option Affinity :=
  ((s.item i).columns[c]?).join

/-- The columns `e` names, as item and column, in order, with repeats. -/
def Expr.refs : Expr → List (Nat × Nat)
  | .col i c => [(i, c)]
  | .lit _ => []
  | .add l r | .greatest l r | .coalesce l r => l.refs ++ r.refs
  | .iif t y n => t.refs ++ y.refs ++ n.refs
  | .isNull e => e.refs
  | .sub _ => []

/-- The items `e` names, in order, with repeats. -/
def Expr.items (e : Expr) : List Nat := e.refs.map (·.1)

/-- The subqueries `e` holds. -/
def Expr.queries : Expr → List Nat
  | .col .. | .lit _ => []
  | .add l r | .greatest l r | .coalesce l r => l.queries ++ r.queries
  | .iif t y n => t.queries ++ y.queries ++ n.queries
  | .isNull e => e.queries
  | .sub q => [q]

/-- `e` over item `target`, whose column `rename c` stands for each column
`c` it names. -/
def Expr.over (target : Nat) (rename : Nat → Nat) : Expr → Expr
  | .col _ c => .col target (rename c)
  | .lit v => .lit v
  | .add l r => .add (l.over target rename) (r.over target rename)
  | .greatest l r => .greatest (l.over target rename) (r.over target rename)
  | .sub q => .sub q
  | .coalesce l r => .coalesce (l.over target rename) (r.over target rename)
  | .iif t y n => .iif (t.over target rename) (y.over target rename) (n.over target rename)
  | .isNull e => .isNull (e.over target rename)

/-- Whether `e` is NULL wherever every column it names is NULL and its
subqueries return NULL: each operation in it is NULL where an operand is.
COALESCE returns a later argument where an earlier one is NULL, a CASE the
ELSE's value where its test is NULL, and `IS NULL` 1 where its operand is
NULL, so none of them is. -/
def Expr.strict : Expr → Bool
  | .col .. | .lit _ | .sub _ => true
  | .add l r | .greatest l r => l.strict && r.strict
  | .coalesce .. | .iif .. | .isNull _ => false

def Cond.refs : Cond → List (Nat × Nat)
  | .cmp _ l r | .collated _ _ l r => l.refs ++ r.refs
  | .null e _ => e.refs
  | .inList e vs => e.refs ++ vs.flatMap Expr.refs
  | .inQuery e _ _ => e.refs
  | .exists .. => []
  | .rowSub vs _ _ _ => vs.flatMap Expr.refs
  | .rowEq ls rs => ls.flatMap Expr.refs ++ rs.flatMap Expr.refs
  | .rowOrder _ _ l r rest => l.refs ++ r.refs ++ rest.refs
  | .and l r | .or l r => l.refs ++ r.refs
  | .not c => c.refs

def Cond.items (c : Cond) : List Nat := c.refs.map (·.1)

/-- `c` over item `target`, as `Expr.over` gives its expressions. -/
def Cond.over (target : Nat) (rename : Nat → Nat) : Cond → Cond
  | .cmp op l r => .cmp op (l.over target rename) (r.over target rename)
  | .collated key op l r => .collated key op (l.over target rename) (r.over target rename)
  | .null e n => .null (e.over target rename) n
  | .inList e vs => .inList (e.over target rename) (vs.map (Expr.over target rename))
  | .inQuery e q n => .inQuery (e.over target rename) q n
  | .exists q n => .exists q n
  | .rowSub vs q isIn n => .rowSub (vs.map (Expr.over target rename)) q isIn n
  | .rowEq ls rs => .rowEq (ls.map (Expr.over target rename)) (rs.map (Expr.over target rename))
  | .rowOrder key op l r rest =>
    .rowOrder key op (l.over target rename) (r.over target rename) (rest.over target rename)
  | .and l r => .and (l.over target rename) (r.over target rename)
  | .or l r => .or (l.over target rename) (r.over target rename)
  | .not c => .not (c.over target rename)

def Cond.queries : Cond → List Nat
  | .cmp _ l r | .collated _ _ l r => l.queries ++ r.queries
  | .null e _ => e.queries
  | .inList e vs => e.queries ++ vs.flatMap Expr.queries
  | .inQuery e q _ => e.queries ++ [q]
  | .exists q _ => [q]
  | .rowSub vs q _ _ => vs.flatMap Expr.queries ++ [q]
  | .rowEq ls rs => ls.flatMap Expr.queries ++ rs.flatMap Expr.queries
  | .rowOrder _ _ l r rest => l.queries ++ r.queries ++ rest.queries
  | .and l r | .or l r => l.queries ++ r.queries
  | .not c => c.queries

/-- The subqueries whose first row `c` takes: each scalar subquery in it, and
the subquery a row value is compared with by `=`. An IN and an EXISTS take
none, as they read every row. -/
def Cond.firstRows : Cond → List Nat
  | .cmp _ l r | .collated _ _ l r => l.queries ++ r.queries
  | .null e _ => e.queries
  | .inList e vs => e.queries ++ vs.flatMap Expr.queries
  | .inQuery e _ _ => e.queries
  | .exists .. => []
  | .rowSub vs q isIn _ => vs.flatMap Expr.queries ++ if isIn then [] else [q]
  | .rowEq ls rs => ls.flatMap Expr.queries ++ rs.flatMap Expr.queries
  | .rowOrder _ _ l r rest => l.queries ++ r.queries ++ rest.firstRows
  | .and l r | .or l r => l.firstRows ++ r.firstRows
  | .not c => c.firstRows

/-- The queries a core's clauses hold, in the order sqlive reads them: the
result columns', then the conditions', then GROUP BY's. -/
def Core.subqueries (c : Core) : List Nat :=
  let outs := c.output.flatMap fun o => o.exprs.flatMap Expr.queries
  (outs ++ c.conds.flatMap (·.1.queries) ++ c.groupBy.flatMap Expr.queries).eraseDups

/-- The subqueries whose first row a core's clauses take. -/
def Core.firstRows (c : Core) : List Nat :=
  let outs := c.output.flatMap fun o => o.exprs.flatMap Expr.queries
  outs ++ c.conds.flatMap (·.1.firstRows) ++ c.groupBy.flatMap Expr.queries

end SqliveNarrowing
