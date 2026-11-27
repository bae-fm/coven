import SqliveNarrowing.Semantics

/-! What sqlive's planner builds from a statement: terms on one item's row,
edges between items, the state it narrows a core's items in, and the scopes a
run binds them to (`scope_plan.rs`, `scope_term.rs`, `row_scope.rs`). -/
namespace SqliveNarrowing

/-- A condition on one item's own row (`ScopeTerm`), over its columns. -/
inductive STerm where
  /-- `column op value`, `op` being `eq`, `lt`, `le`, `gt` or `ge`. -/
  | compare (column : Nat) (op : Op) (value : Int)
  | inList (column : Nat) (values : List Int)
  /-- `column IS NULL`, or `IS NOT NULL` when `not`. -/
  | null (column : Nat) (not : Bool)
  /-- A condition on item `item`'s columns that SQLite judges row by row. -/
  | row (item : Nat) (cond : Cond)
  | and (left right : STerm)
  | or (left right : STerm)
  deriving Repr, Inhabited

/-- The binding where every item has the row `r`, to judge a condition that
names one item's columns. -/
def rowBind (r : Option Row) : Bind := fun _ => r

/-- No subquery: a term holds none. -/
def noSubs : Subs := fun _ => []

/-- Whether a term holds of an item's row, NULLs for a missing row. -/
def STerm.holds (r : Option Row) : STerm → Bool
  | .compare c op v =>
    match cellOf r c with
    | some x => op.compare x v
    | none => false
  | .inList c vs =>
    match cellOf r c with
    | some x => vs.contains x
    | none => false
  | .null c n => (cellOf r c).isNone != n
  | .row _ cd => isTrue (cd.eval (rowBind r) noSubs)
  | .and l t => l.holds r && t.holds r
  | .or l t => l.holds r || t.holds r

/-- Whether a row of NULLs can pass the term: a NULL test that isn't `IS NOT
NULL` and a condition SQLite judges can (`ScopeTerm::may_hold_on_nulls`). -/
def STerm.mayHoldOnNulls : STerm → Bool
  | .compare .. | .inList .. => false
  | .null _ n => !n
  | .row .. => true
  | .and l r => l.mayHoldOnNulls && r.mayHoldOnNulls
  | .or l r => l.mayHoldOnNulls || r.mayHoldOnNulls

/-- The columns a term tests, in order, each once. -/
def STerm.columns : STerm → List Nat
  | .compare c .. | .inList c _ | .null c _ => [c]
  | .row i cd => (cd.refs.filterMap fun (j, c) => if j = i then some c else none).eraseDups
  | .and l r | .or l r => (l.columns ++ r.columns).eraseDups

/-- Whether each column `refs` names is one of item `i`'s that `rename` gives
a column for. -/
def renames (i : Nat) (rename : Nat → Option Nat) (refs : List (Nat × Nat)) : Bool :=
  refs.all fun (j, c) => j == i && (rename c).isSome

/-- The condition `cd` on item `i`'s columns, with no subquery, over item
`target`, whose column `rename c` holds column `c`'s value; `none` when
`rename` gives no column for one it names (`RowTerm::over`). -/
def Cond.passed (i target : Nat) (rename : Nat → Option Nat) (cd : Cond) : Option Cond :=
  if cd.queries.isEmpty && renames i rename cd.refs then
    some (cd.over target fun c => (rename c).getD c)
  else none

/-- A condition SQLite judges, passed to item `target` (`Cond.passed`). -/
def STerm.judgedOn (target : Nat) (rename : Nat → Option Nat) (i : Nat) (cd : Cond) :
    Option STerm :=
  (cd.passed i target rename).map (.row target)

/-- The term over another source whose column `rename c` holds column `c`'s
value: a part testing a column it gives none for is left out of an AND and
leaves an OR with nothing; a condition SQLite judges becomes what `judged`
gives for it (`ScopeTerm::renamed`). -/
def STerm.renamed (rename : Nat → Option Nat) (judged : Nat → Cond → Option STerm) :
    STerm → Option STerm
  | .compare c op v => (rename c).map fun c => .compare c op v
  | .inList c vs => (rename c).map fun c => .inList c vs
  | .null c n => (rename c).map fun c => .null c n
  | .row i cd => judged i cd
  | .and l r =>
    match l.renamed rename judged, r.renamed rename judged with
    | some l, some r => some (.and l r)
    | some t, none | none, some t => some t
    | none, none => none
  | .or l r =>
    match l.renamed rename judged, r.renamed rename judged with
    | some l, some r => some (.or l r)
    | _, _ => none

/-- A value of one item's row: one of its columns, or an expression over its
columns. An edge compares such a value of its member's row with such a value
of its partner's (`MemberValue`, `PartnerValue`). -/
inductive PValue where
  | column (c : Nat)
  | expr (e : Expr)
  deriving Repr, Inhabited, DecidableEq

/-- The value a row gives. -/
def PValue.eval (r : Row) : PValue → Value
  | .column c => r.cell c
  | .expr e => e.eval (rowBind (some r)) noSubs

/-- The value an item's row in a binding gives, NULLs for a missing row. -/
def PValue.on (r : Option Row) : PValue → Value
  | .column c => cellOf r c
  | .expr e => e.eval (rowBind r) noSubs

theorem PValue.on_some (r : Row) (v : PValue) : v.on (some r) = v.eval r := by
  cases v <;> rfl

/-- What column `pc` of a partner item `p`, of affinity `pa`, gives a member
column of affinity `affinity` to compare with: the column, when the two
share an affinity, and the column as an expression when the partner's has
none (BLOB), as a column declared with no type: SQLite gives its values the
member's affinity, as a lookup gives an expression's
(`Planner::partner_column`). Columns of two other affinities give nothing:
SQLite converts their values in ways sqlive doesn't follow. -/
def columnValue (affinity pa : Affinity) (p pc : Nat) : Option PValue :=
  if pa = affinity then some (.column pc) else if pa = .blob then some (.expr (.col p pc)) else none

/-- A partner's column read either way gives a row the value it holds. -/
theorem columnValue_eval {affinity pa : Affinity} {p pc : Nat} {v : PValue}
    (h : columnValue affinity pa p pc = some v) (r : Row) : v.eval r = r.cell pc := by
  unfold columnValue at h
  split at h
  · cases h; rfl
  · split at h
    · cases h; simp [PValue.eval, Expr.eval, rowBind, cellOf]
    · cases h

/-- The columns of its item, `i`, that the value reads. -/
def PValue.columns : PValue → List Nat
  | .column c => [c]
  | .expr e => e.refs.map (·.2)

/-- The value of item `i`'s row over item `target`, whose column `rename c`
holds column `c`'s value; `none` when `rename` gives no column for one it
reads (`Member::over`). -/
def PValue.passed (i target : Nat) (rename : Nat → Option Nat) : PValue → Option PValue
  | .column c => (rename c).map .column
  | .expr e =>
    if !e.refs.isEmpty && e.queries.isEmpty && renames i rename e.refs then
      some (.expr (e.over target fun c => (rename c).getD c))
    else none

/-- Whether the value is NULL where every column of its item is: a column
is, and so is a strict expression. -/
def PValue.strict : PValue → Bool
  | .column _ => true
  | .expr e => e.strict

/-- A value of `member`'s row equals a value of `partner`'s rows (`Edge`). -/
structure Edge where
  member : Nat
  /-- What it compares of the member's row: a column, or an expression over
  the member's columns, which SQLite evaluates on each row a write changes. -/
  memberValue : PValue
  /-- The member column's affinity; BLOB, which converts nothing, for an
  expression. -/
  affinity : Affinity
  partner : Nat
  value : PValue
  /-- Whether the partner sits in a core inside the member's: the source of a
  subquery's value, or the item a derived item's arm passes the compared
  column on from (`via`). -/
  nested : Bool
  /-- The derived item, which a lookup can't read alone, and its column that
  the equality compares, when its one arm passes that column on unchanged
  from the partner (`Passed`). -/
  via : Option (Nat × Nat) := none
  /-- Whether the edge compares by `IS`, which matches a NULL with a NULL. -/
  nulls : Bool := false
  /-- Whether the planner passed the edge into a derived item's arm from an
  edge of the derived item. -/
  pushed : Bool := false
  deriving Repr, Inhabited, DecidableEq

/-- One side of an OR as it narrows one item: terms on the item's row and
edges from it, which all hold where the side does (`Side`). -/
structure Side where
  terms : List STerm := []
  edges : List Edge := []
  deriving Repr, Inhabited

/-- Whether a row of NULLs in the item's place can pass the side. -/
def Side.mayHoldOnNulls (side : Side) : Bool :=
  side.terms.any (·.mayHoldOnNulls) || side.edges.any (·.nulls)

/-- Whether a side narrows its item: it has a term or an edge. -/
def Side.narrows (side : Side) : Bool := !side.terms.isEmpty || !side.edges.isEmpty

/-- An OR that narrows `item`: wherever the OR holds, one of its sides does
(`Disjunction`). -/
structure Disjunction where
  item : Nat
  sides : List Side
  deriving Repr, Inhabited

def Disjunction.mayHoldOnNulls (d : Disjunction) : Bool := d.sides.any Side.mayHoldOnNulls

/-- Terms on items, edges between them and ORs over both, which conditions
give. -/
structure Narrowing where
  terms : List (Nat × STerm) := []
  edges : List Edge := []
  ors : List Disjunction := []
  deriving Repr, Inhabited

/-- Whether a row of NULLs in an item's place can pass some of `n`: a term
that can, an `IS` edge, whose member's NULL matches a NULL, or an OR with a
side that can. -/
def Narrowing.mayHoldOnNulls (n : Narrowing) : Bool :=
  n.terms.any (fun (_, t) => t.mayHoldOnNulls) || n.edges.any (·.nulls) ||
    n.ors.any (·.mayHoldOnNulls)

/-- Whether an edge narrows a LEFT JOIN's item `i` from the join's ON while
the join is a LEFT JOIN's: an edge from `i` that reads through no derived
item. -/
def Edge.ownOn (i : Nat) (e : Edge) : Bool := e.member == i && e.via.isNone

/-- Whether an OR narrows a LEFT JOIN's item `i` from the join's ON while the
join is a LEFT JOIN's: an OR of `i` none of whose sides' edges reads through
a derived item. -/
def Disjunction.ownOn (i : Nat) (d : Disjunction) : Bool :=
  d.item == i && d.sides.all fun side => side.edges.all (·.via.isNone)

/-- What of `n`, the ON of the LEFT JOIN adding `i`, narrows `i` while the
join is a LEFT JOIN's, and the rest, which waits for the join to be an inner
join's. An edge from `i` through a derived item, and an OR of `i` with a side
that holds one, wait with the rest: the proofs find such an edge's partner
row only in a binding its core keeps, and the ON holds of bindings of the
items before the join's, which the core may not keep (`plan_left_passes`). -/
def Narrowing.split (n : Narrowing) (i : Nat) : Narrowing × Narrowing :=
  (⟨n.terms.filter (·.1 == i), n.edges.filter (Edge.ownOn i), n.ors.filter (Disjunction.ownOn i)⟩,
   ⟨n.terms.filter (·.1 != i), n.edges.filter (!Edge.ownOn i ·), n.ors.filter (!Disjunction.ownOn i ·)⟩)

def Narrowing.append (a b : Narrowing) : Narrowing :=
  ⟨a.terms ++ b.terms, a.edges ++ b.edges, a.ors ++ b.ors⟩

/-- A run's scope of an item's rows (`RowScope`). -/
inductive RScope where
  | all
  | compare (column : Nat) (op : Op) (value : Int)
  | null (column : Nat) (not : Bool)
  | inVals (column : Nat) (values : List Int)
  | row (item : Nat) (cond : Cond)
  /-- The rows whose value of `e` is not NULL and compares with `value` as
  `op` says by `key`, the key of the collation an ORDER BY term compares by
  (`Dir`): an ORDER BY term's bound, which a write judges by SQLite's own
  comparison, as the connection's collations give it. -/
  | ordered (key : Int → Int) (e : Expr) (op : Op) (value : Int)
  /-- The row's `member` value equals `value` over a row of `partner` in
  `scope`; a NULL matches a NULL when `nulls`, as `IS` compares. -/
  | joined (member : PValue) (partner : Nat) (value : PValue) (scope : RScope) (nulls : Bool)
  /-- The row's values equal those of one row of `partner` in `scope`, each
  edge's `member` value against its `value` (a NULL matching a NULL when the
  edge's `nulls`): the edges are equalities of one join, and one partner row
  gives all their values. -/
  | joinedAll (edges : List Edge) (partner : Nat) (scope : RScope)
  /-- The rows of `item` that sort at or before the `limit`-th row of the
  window they sit in: a subquery's rows for one binding around it, which are
  the rows of `item` that its `own` conditions pass and that hold one value in
  each `partition` column, sorted by `keys`, each descending when `true`
  (`Window`). A row with a NULL in a partition column sits in no window. -/
  | window (item : Nat) (own : List Cond) (partition : List Nat) (keys : List (Expr × Dir))
      (limit : Nat)
  | and (left right : RScope)
  | or (left right : RScope)
  deriving Repr, Inhabited

/-- Both, either alone when the other is every row (`RowScope::and`). -/
def RScope.both : RScope → RScope → RScope
  | .all, s | s, .all => s
  | l, r => .and l r

/-- Either, every row when either is (`RowScope::or`). -/
def RScope.either : RScope → RScope → RScope
  | .all, _ | _, .all => .all
  | l, r => .or l r

/-- The scope a term gives a run (`ScopeTerm::bind`): the differential binds
no parameter, so every term binds. -/
def STerm.bind : STerm → RScope
  | .compare c op v => .compare c op v
  | .inList c vs => .inVals c vs.eraseDups
  | .null c n => .null c n
  | .row i cd => .row i cd
  | .and l r => l.bind.both r.bind
  | .or l r => l.bind.either r.bind

/-- The values every row in the scope holds in `column`, when the scope says
(`RowScope::equal_values`). -/
def RScope.equalValues (column : Nat) : RScope → Option (List Int)
  | .inVals c vs => if c = column then some vs else none
  | .compare c .eq v => if c = column then some [v] else none
  | .and l r => (l.equalValues column).orElse fun _ => r.equalValues column
  | .or l r => do
    let a ← l.equalValues column
    let b ← r.equalValues column
    pure (a ++ b)
  | _ => none

/-- The values of the ORDER BY terms `keys`, expressions over one item's
columns, in its row `r`. -/
def keyValues (keys : List (Expr × Dir)) (r : Row) : List Value :=
  keys.map fun (e, _) => e.eval (rowBind (some r)) noSubs

/-- Whether `r'` is a row of the window `r` sits in that sorts before `r`:
its `own` conditions hold, it holds `r`'s value in each `partition` column,
and its values of `keys` sort before `r`'s. -/
def windowBefore (own : List Cond) (partition : List Nat) (keys : List (Expr × Dir)) (r r' : Row) :
    Bool :=
  own.all (fun cd => isTrue (cd.eval (rowBind (some r')) noSubs)) &&
    partition.all (fun c => r'.cell c == r.cell c) &&
    keyCompare (keys.map (·.2)) (keyValues keys r') (keyValues keys r) == .lt

/-- Whether row `r` passes, looking a join's value up among `rowsOf partner`
(`RowScope::matches`, and the SQL `scope_lookup.rs` renders). -/
def RScope.passes (rowsOf : Nat → List Row) (r : Row) : RScope → Bool
  | .all => true
  | .compare c op v =>
    match r.cell c with
    | some x => op.compare x v
    | none => false
  | .null c n => (r.cell c).isNone != n
  | .inVals c vs =>
    match r.cell c with
    | some x => vs.contains x
    | none => false
  | .row _ cd => isTrue (cd.eval (rowBind (some r)) noSubs)
  | .ordered key e op v =>
    match e.eval (rowBind (some r)) noSubs with
    | some x => op.compare (key x) (key v)
    | none => false
  | .joined m p v s nulls =>
    ((m.eval r).isSome || nulls) && (rowsOf p).any fun r' => v.eval r' == m.eval r && s.passes rowsOf r'
  | .joinedAll es p s =>
    (rowsOf p).any fun r' =>
      es.all (fun e => ((e.memberValue.eval r).isSome || e.nulls) && e.value.eval r' == e.memberValue.eval r) &&
        s.passes rowsOf r'
  | .window i own part keys n =>
    part.all (fun c => (r.cell c).isSome) &&
      decide (((rowsOf i).filter (windowBefore own part keys r)).length < n)
  | .and l t => l.passes rowsOf r && t.passes rowsOf r
  | .or l t => l.passes rowsOf r || t.passes rowsOf r

/-! Static facts about items, which the readers check. -/

/-- Whether conditions can narrow item `i`: a table, or a derived item. -/
def Statement.narrowable (s : Statement) (i : Nat) : Bool :=
  match (s.item i).kind with
  | .table _ | .derived .. => true

/-- The cores a core sits in, nearest first, itself included. -/
def Statement.scopeChain (s : Statement) (k : Nat) : List Nat :=
  go s.cores.length k
where
  go : Nat → Nat → List Nat
    | 0, k => [k]
    | f + 1, k =>
      match (s.core k).parent with
      | some p => k :: go f p
      | none => [k]

/-- The source, and its column, that derived item `d`'s column `pc` passes
on unchanged, when a lookup can't read `d` alone and can read the source:
`d`'s query is one SELECT that neither aggregates nor calls a window
function, whose result column `pc` is a bare column of one of its own items
(`derived::passed_source`). `d` is an item of its core's FROM, as every item
of a statement the reader reads is. -/
def passedSource (s : Statement) (d pc : Nat) : Option (Nat × Nat) :=
  match (s.item d).kind with
  | .table _ => none
  | .derived q _ =>
    match (s.query q).arms with
    | [a] =>
      if (s.item d).readable || (s.core a).aggregated || (s.core a).windowed ||
        !(s.core (s.item d).core).items.contains d then none
      else
        match (s.core a).output[pc]? with
        | some (.expr (.col x xc)) =>
          if (s.item x).core = a ∧ (s.item x).readable then some (x, xc) else none
        | _ => none
    | _ => none

/-- Whether the items `placed` hold an edge's partner, and the derived item
it reads through, if any: then its member can take the partner's values
(`Edge::placed`). -/
def Edge.placedIn (placed : List Nat) (e : Edge) : Bool :=
  placed.contains e.partner && match e.via with
    | some (d, _) => placed.contains d
    | none => true

/-! The state sqlive narrows a statement's items in (`Planner`). -/

structure PItem where
  /-- What the core's conditions, and those passed into a derived item, say
  of the item's own row. -/
  term : Option STerm := none
  /-- What the edges between the item and the other items of its core carry
  to it from their own terms (`Transfer.carry`): it narrows the item's
  scope, and decides nothing of the order the planner places items in. -/
  carried : Option STerm := none
  /-- The edges the item took as memberships, in order. -/
  memberships : List Edge := []
  /-- The sides of each OR that narrows the item. -/
  ors : List (List Side) := []
  /-- The ORs among `ors` whose sides' edges all take values from items
  placed before it, which narrow its scope. -/
  either : List (List Side) := []
  /-- For a LEFT JOIN's right side no condition outside its ON has made an
  inner join's yet, what its ON says of the other items. -/
  leftOn : Option Narrowing := none
  deriving Repr, Inhabited

structure PState where
  items : Nat → PItem
  edges : List Edge
  /-- The edges `PushDown.pushDown` passes into derived items' arms, each with
  its target as member. They are among `edges` too; no step of the planner
  reads this list, which the proofs read to tell a pushed edge from one a
  condition gives. -/
  pushed : List Edge := []

def PState.update (st : PState) (i : Nat) (f : PItem → PItem) : PState :=
  { st with items := fun j => if j = i then f (st.items j) else st.items j }

/-- The state before any condition is read: each LEFT JOIN item holds an
empty narrowing. -/
def PState.initial (s : Statement) : PState :=
  { items := fun i => { leftOn := if (s.item i).left then some {} else none }, edges := [] }

/-- Add `term` to item `i`'s term, with AND. -/
def PState.addTerm (st : PState) (i : Nat) (t : STerm) : PState :=
  st.update i fun p => { p with term := some (match p.term with | some e => .and e t | none => t) }

/-- Add `n`'s terms, edges and ORs, and the items it narrows
(`Planner::add`). -/
def PState.add (st : PState) (n : Narrowing) : PState × List Nat :=
  let st := n.terms.foldl (fun st (i, t) => st.addTerm i t) st
  let st := n.ors.foldl (fun st d => st.update d.item fun p => { p with ors := p.ors ++ [d.sides] }) st
  ({ st with edges := st.edges ++ n.edges },
   n.terms.map (·.1) ++ n.edges.map (·.member) ++ n.ors.map (·.item))

/-- Narrow by `n`, which conditions on the joined rows give, and make each
LEFT JOIN item it narrows an inner join's, releasing what its ON said of the
other items (`Planner::narrow`, `Planner::join_inner`). A term a row of NULLs
can pass, an `IS` edge and an OR with a side that can, narrow nothing on a
right side that is still a LEFT JOIN's. -/
def PState.narrow : Nat → PState → Narrowing → PState
  | 0, st, _ => st
  | fuel + 1, st, n =>
    let terms := n.terms.filter fun (i, t) => !(t.mayHoldOnNulls && (st.items i).leftOn.isSome)
    let edges := n.edges.filter fun e => !(e.nulls && (st.items e.member).leftOn.isSome)
    let ors := n.ors.filter fun d => !(d.mayHoldOnNulls && (st.items d.item).leftOn.isSome)
    let (st, narrowed) := st.add { terms, edges, ors }
    narrowed.foldl (fun st i =>
      match (st.items i).leftOn with
      | some held => PState.narrow fuel (st.update i fun p => { p with leftOn := none }) held
      | none => st) st

/-- Make item `i` an inner join's right side (`Planner::join_inner`). -/
def PState.joinInner (fuel : Nat) (st : PState) (i : Nat) : PState :=
  match (st.items i).leftOn with
  | some held => PState.narrow fuel (st.update i fun p => { p with leftOn := none }) held
  | none => st

/-- `n`, from the ON of the LEFT JOIN adding `i`: what it says of `i` narrows
it, and the rest waits for the join to be an inner join's (`narrow_on`). -/
def PState.narrowOn (fuel : Nat) (st : PState) (i : Nat) (n : Narrowing) : PState :=
  let (own, others) := n.split i
  let st := (st.add own).1
  match (st.items i).leftOn with
  | some held => st.update i fun p => { p with leftOn := some (held.append others) }
  | none => st.narrow fuel others

/-- A condition reader: the narrowing condition `cd`, written in core `k`,
gives, or `none` when the reader doesn't read it. -/
abbrev ReadCondition := Statement → Nat → Cond → Option Narrowing

end SqliveNarrowing
