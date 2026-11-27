import SqliveNarrowing.Plan

/-! Whether the expressions a sort compares, with the equalities of a core's
conditions, tell every two of the core's bindings apart (`told_apart.rs`).
A table item is told apart by a unique key of its table, its rowid among
them, whose every column the sort compares or an equality with the items told
apart before it fixes, so that item adds one row at most to each binding of
those. A FROM subquery is told apart when the columns such equalities fix let
its query return one row at most. A core's bindings are told apart when every
one of its items is (`Pick.tellsApart_separates`). -/
namespace SqliveNarrowing

/-- The queries core `k` reads: its derived items' and its subqueries. -/
def Statement.nestedQueries (s : Statement) (k : Nat) : List Nat :=
  (s.core k).items.filterMap (fun i =>
    match (s.item i).kind with
    | .derived q _ => some q
    | .table _ => none) ++ (s.core k).subqueries

/-- The columns a result column names. -/
def OutCol.refs (o : OutCol) : List (Nat × Nat) := o.exprs.flatMap Expr.refs

/-- The columns a core's clauses name. -/
def Core.refs (c : Core) : List (Nat × Nat) :=
  c.conds.flatMap (·.1.refs) ++ c.output.flatMap OutCol.refs ++ c.groupBy.flatMap Expr.refs

/-- The cores of query `q` and of the queries they read, `fuel` deep. -/
def Statement.queryCores (s : Statement) : Nat → Nat → List Nat
  | 0, _ => []
  | f + 1, q => (s.query q).arms.flatMap fun k => k :: (s.nestedQueries k).flatMap (s.queryCores f)

/-- The items query `q`, its subqueries and its derived items' queries name
outside their own cores. -/
def Statement.outerNames (s : Statement) (q : Nat) : List Nat :=
  let cores := s.queryCores (s.queries.length + 1) q
  (cores.flatMap fun k => (s.core k).refs.map (·.1)).filter fun j => !cores.contains (s.item j).core

/-- The items `e`'s value depends on: those it names, and those its
subqueries name outside them. -/
def Statement.exprDeps (s : Statement) (e : Expr) : List Nat :=
  e.items ++ e.queries.flatMap s.outerNames

/-- The items a condition's truth depends on. -/
def Statement.condDeps (s : Statement) (cd : Cond) : List Nat :=
  cd.items ++ cd.queries.flatMap s.outerNames

/-- Whether every item of core `k` among `deps` is one of `known`: the others
hold the row of the binding around the core. -/
def Statement.within (s : Statement) (k : Nat) (known deps : List Nat) : Bool :=
  deps.all fun j => !(s.core k).items.contains j || known.contains j

/-- Whether comparing column `c` of item `i` with `e` converts none of the
column's values: the column is numeric, whose values SQLite converts for no
comparison, or it is untyped and `e` gives SQLite no numeric affinity to
convert them by. -/
def Statement.keepsValues (s : Statement) (i c : Nat) (e : Expr) : Bool :=
  match s.affinity i c with
  | some .numeric => true
  | some .blob => match e with
    | .col j d => s.affinity j d == some .blob
    | .sub _ => false
    | _ => true
  | none => false

/-- Whether `e` is a column of a collation other than BINARY. -/
def Statement.collatedColumn (s : Statement) : Expr → Bool
  | .col j d => ((s.item j).collated[d]?).getD false
  | _ => false

/-- Whether SQLite compares `left = right` by BINARY: by the left side's
collation when it is a column, else by the right side's. A subquery's value
carries its column's collation, which this doesn't follow. -/
def Statement.comparesBinary (s : Statement) : Expr → Expr → Bool
  | .col i c, _ => !s.collatedColumn (.col i c)
  | .sub _, _ => false
  | _, .sub _ => false
  | _, right => !s.collatedColumn right

/-- Whether the condition `cd` is an equality of column `c` of item `i` with a
value that the items `known` of core `k` and the binding around it give. -/
def Statement.equates (s : Statement) (k : Nat) (known : List Nat) (i c : Nat) : Cond → Bool
  | .cmp .eq l r =>
    let by_ (e : Expr) := s.within k known (s.exprDeps e) && s.keepsValues i c e &&
      s.comparesBinary l r
    (l == .col i c && by_ r) || (r == .col i c && by_ l)
  | _ => false

/-- Whether an equality of the WHERE or an inner join's ON fixes column `c` of
item `i`: it holds, not NULL, in every binding the core keeps. -/
def Statement.innerEquated (s : Statement) (k : Nat) (known : List Nat) (i c : Nat) : Bool :=
  (s.core k).conds.any fun (cd, p) => p == .inner && s.equates k known i c cd

/-- Whether an equality of the ON of the LEFT JOIN that adds item `i` fixes its
column `c` in every binding that holds a row of it. -/
def Statement.onEquated (s : Statement) (k : Nat) (known : List Nat) (i c : Nat) : Bool :=
  (s.core k).conds.any fun (cd, p) => p == .leftOn i && s.equates k known i c cd

/-- Whether column `c` of item `i` holds no NULL: the rowid, or a column
declared NOT NULL. -/
def Statement.neverNull (s : Statement) (i c : Nat) : Bool :=
  (c == 0 && (s.item i).rowidFirst) || (s.item i).notNull.contains c

/-- Whether the sort `sorted` compares column `c` of item `i`, which holds no
NULL, so two rows of the item that tie in the sort hold one value there, and
so do two bindings of which only one holds a row. -/
def Statement.sortedColumn (s : Statement) (sorted : List Expr) (i c : Nat) : Bool :=
  sorted.contains (.col i c) && s.neverNull i c

/-- Whether column `c` of item `i` takes one value in two bindings that tie in
`sorted`, agree on `known`, and hold the values `fixed` names alike. -/
def Statement.covers (s : Statement) (k : Nat) (sorted : List Expr) (fixed : List (Nat × Nat))
    (known : List Nat) (i c : Nat) : Bool :=
  s.sortedColumn sorted i c || fixed.contains (i, c) || s.innerEquated k known i c ||
    s.onEquated k known i c

/-- Whether column `c` of item `i` tells a binding that holds a row of the
item from one that holds none. -/
def Statement.anchors (s : Statement) (k : Nat) (sorted : List Expr) (fixed : List (Nat × Nat))
    (known : List Nat) (i c : Nat) : Bool :=
  s.sortedColumn sorted i c || fixed.contains (i, c) || s.innerEquated k known i c

/-- Whether the items `known` and the binding around core `k` decide whether
the LEFT JOIN that adds item `i` finds it a row. -/
def Statement.onKnown (s : Statement) (k : Nat) (known : List Nat) (i : Nat) : Bool :=
  ((s.core k).onOf i).all fun cd => s.within k (i :: known) (s.condDeps cd)

/-- The unique keys of table item `i`: its rowid, where its first column holds
it, and each unique index of its table. -/
def Statement.keysOf (s : Statement) (i : Nat) : List (List Nat) :=
  (if (s.item i).rowidFirst then [[0]] else []) ++ (s.item i).keys

/-- The columns of derived item `i` that equalities fix. -/
def Statement.fixedColumns (s : Statement) (k : Nat) (fixed : List (Nat × Nat)) (known : List Nat)
    (i : Nat) : List Nat :=
  (List.range (s.item i).columns.length).filter fun c =>
    fixed.contains (i, c) || s.innerEquated k known i c || s.onEquated k known i c

/-- Whether item `i` of core `k` adds one row at most, or the one NULL row of
a LEFT JOIN, to a binding of the items `known`, among bindings that tie in
`sorted` and hold the values `fixed` names alike. `one q columns` says
whether query `q` returns one row at most for any values of its columns
`columns`. -/
def Statement.determines (s : Statement) (one : Nat → List Nat → Bool) (k : Nat)
    (sorted : List Expr) (fixed : List (Nat × Nat)) (known : List Nat) (i : Nat) : Bool :=
  match (s.item i).kind with
  | .table _ => (s.keysOf i).any fun key =>
      key.all (s.covers k sorted fixed known i) &&
        (!(s.item i).left || key.any (s.anchors k sorted fixed known i) || s.onKnown k known i)
  | .derived q _ =>
    let columns := s.fixedColumns k fixed known i
    (s.item i).written && one q columns &&
      (!(s.item i).left || columns.any (s.anchors k [] fixed known i) || s.onKnown k known i)

/-- The items `known`, and those each round of `rounds` adds that the ones
before it determine. -/
def toldRounds (determines : List Nat → Nat → Bool) (items : List Nat) : Nat → List Nat → List Nat
  | 0, known => known
  | rounds + 1, known =>
    toldRounds determines items rounds
      (known ++ items.filter fun i => !known.contains i && determines known i)

/-- Whether the items of core `k` are each determined, in turn. -/
def Statement.allTold (s : Statement) (one : Nat → List Nat → Bool) (k : Nat)
    (sorted : List Expr) (fixed : List (Nat × Nat)) : Bool :=
  let items := (s.core k).items
  let known := toldRounds (s.determines one k sorted fixed) items items.length []
  items.all known.contains

/-- Whether two values of `e` in bindings of core `k` that compare equal are
the same value: `e` is a column of one of the core's tables that sqlive gives
an affinity other than BLOB, so it compares by BINARY and holds no integer
and real that compare equal, as an untyped column can hold `1` and `1.0`. -/
def Statement.comparesExactly (s : Statement) (k : Nat) : Expr → Bool
  | .col i c => (s.core k).items.contains i && (s.table? i).isSome &&
      (s.affinity i c).any (· != .blob)
  | _ => false

/-- The expression of the core's result column at position `p`, where it is
one's own expression. -/
def Core.passes (c : Core) (p : Nat) : Option Expr :=
  match c.output[p]? with
  | some (.expr e) => some e
  | _ => none

/-- The columns of core `k`'s items that the result columns `columns` pass on,
each as its item and column. -/
def Statement.passedColumns (s : Statement) (k : Nat) (columns : List Nat) : List (Nat × Nat) :=
  columns.filterMap fun p => match (s.core k).passes p with
    | some (.col i c) => if (s.core k).items.contains i then some (i, c) else none
    | _ => none

/-- Whether query `q` returns one row at most for any values of its columns
`columns`, none of them NULL: it is one SELECT that aggregates with no GROUP
BY, or groups by columns that `columns` return, or neither aggregates nor
calls a window function, and equalities with the columns it returns there tell
its bindings apart. -/
def Statement.oneRow (s : Statement) : Nat → Nat → List Nat → Bool
  | 0, _, _ => false
  | f + 1, q, columns =>
    match (s.query q).arms with
    | [k] =>
      let c := s.core k
      (c.aggregated && c.groupBy.isEmpty) ||
        (!c.groupBy.isEmpty && c.groupBy.all fun g =>
          s.comparesExactly k g && columns.any fun p => c.passes p == some g) ||
        (!c.aggregated && !c.windowed &&
          s.allTold (s.oneRow f) k [] (s.passedColumns k columns))
    | _ => false

/-- Whether the expressions `sorted`, with the equalities of core `k`'s
conditions, tell every two of its bindings apart. -/
def Statement.tellsApart (s : Statement) (k : Nat) (sorted : List Expr) : Bool :=
  s.allTold (s.oneRow (s.queries.length + 1)) k sorted []

end SqliveNarrowing
