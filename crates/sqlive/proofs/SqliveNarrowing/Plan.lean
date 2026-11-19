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

/-- The term over another source whose column `rename c` holds column `c`'s
value: a part testing a column it gives none for is left out of an AND and
leaves an OR with nothing; a condition SQLite judges stays where it is
(`ScopeTerm::renamed`). -/
def STerm.renamed (rename : Nat → Option Nat) : STerm → Option STerm
  | .compare c op v => (rename c).map fun c => .compare c op v
  | .inList c vs => (rename c).map fun c => .inList c vs
  | .null c n => (rename c).map fun c => .null c n
  | .row .. => none
  | .and l r =>
    match l.renamed rename, r.renamed rename with
    | some l, some r => some (.and l r)
    | some t, none | none, some t => some t
    | none, none => none
  | .or l r =>
    match l.renamed rename, r.renamed rename with
    | some l, some r => some (.or l r)
    | _, _ => none

/-- What a membership compares with: a partner's column of the member
column's affinity, or an expression over the partner's columns. -/
inductive PValue where
  | column (c : Nat)
  | expr (e : Expr)
  deriving Repr, Inhabited

/-- The value a partner row gives. -/
def PValue.eval (r : Row) : PValue → Value
  | .column c => r.cell c
  | .expr e => e.eval (rowBind (some r)) noSubs

/-- `member.column` equals a value of `partner`'s rows (`Edge`). -/
structure Edge where
  member : Nat
  column : Nat
  affinity : Affinity
  partner : Nat
  value : PValue
  /-- Whether the partner is the source of a subquery's value. -/
  nested : Bool
  /-- Whether the edge compares by `IS`, which matches a NULL with a NULL. -/
  nulls : Bool := false
  deriving Repr, Inhabited

/-- One side of an OR as it narrows one item: terms on the item's row and
edges from it, which all hold where the side does (`Side`). -/
structure Side where
  terms : List STerm := []
  edges : List Edge := []
  deriving Repr, Inhabited

/-- Whether a row of NULLs in the item's place can pass the side. -/
def Side.mayHoldOnNulls (side : Side) : Bool :=
  side.terms.any (·.mayHoldOnNulls) || side.edges.any (·.nulls)

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

/-- What of `n` narrows `i`, and the rest. -/
def Narrowing.split (n : Narrowing) (i : Nat) : Narrowing × Narrowing :=
  (⟨n.terms.filter (·.1 == i), n.edges.filter (·.member == i), n.ors.filter (·.item == i)⟩,
   ⟨n.terms.filter (·.1 != i), n.edges.filter (·.member != i), n.ors.filter (·.item != i)⟩)

def Narrowing.append (a b : Narrowing) : Narrowing :=
  ⟨a.terms ++ b.terms, a.edges ++ b.edges, a.ors ++ b.ors⟩

/-- A run's scope of an item's rows (`RowScope`). -/
inductive RScope where
  | all
  | compare (column : Nat) (op : Op) (value : Int)
  | null (column : Nat) (not : Bool)
  | inVals (column : Nat) (values : List Int)
  | row (item : Nat) (cond : Cond)
  /-- `column` equals `value` over a row of `partner` in `scope`; a NULL
  matches a NULL when `nulls`, as `IS` compares. -/
  | joined (column : Nat) (partner : Nat) (value : PValue) (scope : RScope) (nulls : Bool)
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
  | .joined c p v s nulls =>
    ((r.cell c).isSome || nulls) && (rowsOf p).any fun r' => v.eval r' == r.cell c && s.passes rowsOf r'
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
