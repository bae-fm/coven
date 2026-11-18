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
  deriving Repr, Inhabited, DecidableEq

/-- A condition, in SQL's three-valued logic. -/
inductive Cond where
  | cmp (op : Op) (left right : Expr)
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
  /-- Whether sqlive can read its rows alone, in a lookup: a table, a view,
  and a FROM subquery or CTE outside any expression subquery or whose query
  names nothing outside it (`View.readable`). -/
  readable : Bool
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
  | concat (e : Expr) (orderBy : List (Expr × Bool))
  /-- `row_number() OVER (PARTITION BY ... ORDER BY ...)`, each ORDER BY term
  descending when `true`, in a core that doesn't aggregate: a binding's place
  among the bindings of its partition, in the order the window's ORDER BY
  sorts them and, among those that sort the same, in the order SQLite reads
  them in. -/
  | rowNumber (partition : List Expr) (orderBy : List (Expr × Bool))
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
  /-- ORDER BY: result column positions, each descending when `true`. -/
  orderBy : List (Nat × Bool)
  /-- Whether each ORDER BY term names its result column's column, which
  sqlive's LIMIT bound needs, rather than its position. -/
  orderNamed : Bool
  limit : Option Nat
  /-- OFFSET: how many of the ordered rows the query skips first. -/
  offset : Option Nat
  deriving Repr, Inhabited

/-- A shape that makes sqlive read every table of the statement whole. A
USING join is none: the JSON reader reads it as the equalities it stands for
(`Json.readCore`). -/
inductive WholeShape where
  | natural | right | recursive | inTable | virtual | parenthesized | repeatedCte
  deriving DecidableEq, Repr, Inhabited

structure Statement where
  items : List Item
  cores : List Core
  queries : List Query
  root : Nat
  /-- The shapes in it that sqlive reads whole. -/
  whole : List WholeShape
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

/-- The affinity sqlive narrows column `c` of item `i` by. -/
def Statement.affinity (s : Statement) (i c : Nat) : Option Affinity :=
  ((s.item i).columns[c]?).join

/-- The columns `e` names, as item and column, in order, with repeats. -/
def Expr.refs : Expr → List (Nat × Nat)
  | .col i c => [(i, c)]
  | .lit _ => []
  | .add l r | .greatest l r => l.refs ++ r.refs
  | .sub _ => []

/-- The items `e` names, in order, with repeats. -/
def Expr.items (e : Expr) : List Nat := e.refs.map (·.1)

/-- The subqueries `e` holds. -/
def Expr.queries : Expr → List Nat
  | .col .. | .lit _ => []
  | .add l r | .greatest l r => l.queries ++ r.queries
  | .sub q => [q]

def Cond.refs : Cond → List (Nat × Nat)
  | .cmp _ l r => l.refs ++ r.refs
  | .null e _ => e.refs
  | .inList e vs => e.refs ++ vs.flatMap Expr.refs
  | .inQuery e _ _ => e.refs
  | .exists .. => []
  | .rowSub vs _ _ _ => vs.flatMap Expr.refs
  | .rowEq ls rs => ls.flatMap Expr.refs ++ rs.flatMap Expr.refs
  | .and l r | .or l r => l.refs ++ r.refs
  | .not c => c.refs

def Cond.items (c : Cond) : List Nat := c.refs.map (·.1)

def Cond.queries : Cond → List Nat
  | .cmp _ l r => l.queries ++ r.queries
  | .null e _ => e.queries
  | .inList e vs => e.queries ++ vs.flatMap Expr.queries
  | .inQuery e q _ => e.queries ++ [q]
  | .exists q _ => [q]
  | .rowSub vs q _ _ => vs.flatMap Expr.queries ++ [q]
  | .rowEq ls rs => ls.flatMap Expr.queries ++ rs.flatMap Expr.queries
  | .and l r | .or l r => l.queries ++ r.queries
  | .not c => c.queries

/-- The subqueries whose first row `c` takes: each scalar subquery in it, and
the subquery a row value is compared with by `=`. An IN and an EXISTS take
none, as they read every row. -/
def Cond.firstRows : Cond → List Nat
  | .cmp _ l r => l.queries ++ r.queries
  | .null e _ => e.queries
  | .inList e vs => e.queries ++ vs.flatMap Expr.queries
  | .inQuery e _ _ => e.queries
  | .exists .. => []
  | .rowSub vs q isIn _ => vs.flatMap Expr.queries ++ if isIn then [] else [q]
  | .rowEq ls rs => ls.flatMap Expr.queries ++ rs.flatMap Expr.queries
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
