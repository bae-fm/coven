import SqliveNarrowing.Rules

/-! sqlive's scope planner over a statement (`scope_plan.rs`): it reads each
core's conditions with the rules, orders the items so each takes values from
items placed before it, and binds the scopes of a run. -/
namespace SqliveNarrowing

/-- The cores in the order sqlive finishes reading them: a core after the
cores of its derived items and of its subqueries. -/
def Statement.readOrder (s : Statement) : List Nat :=
  (s.query s.root).arms.flatMap (go (s.cores.length + 1))
where
  go : Nat → Nat → List Nat
    | 0, _ => []
    | f + 1, k =>
      let c := s.core k
      let derived := c.items.flatMap fun i =>
        match (s.item i).kind with
        | .derived q _ => (s.query q).arms
        | .table _ => []
      let nested := c.subqueries.flatMap fun q => (s.query q).arms
      (derived ++ nested).flatMap (go f) ++ [k]

/-- Fuel for the nested releases of LEFT JOINs and passes into derived
items: one for each item. -/
def Statement.fuel (s : Statement) : Nat := s.items.length + 1

/-- Read core `k`'s conditions: those a row of NULLs fails first, as they
make LEFT JOINs inner, which the others see; then carry each item's terms on
a compared column across the core's edges; then pass each item's term into
it, when it is derived (`read_core`). -/
def readCore (s : Statement) (st : PState) (k : Nat) : PState :=
  let c := s.core k
  let read := c.conds.map fun (cd, p) => (readCondition s k cd, p)
  let rejecting := read.filter fun (n, _) => !n.mayHoldOnNulls
  let others := read.filter fun (n, _) => n.mayHoldOnNulls
  let st := (rejecting ++ others).foldl (fun st (n, p) =>
    match p with
    | .inner => st.narrow s.fuel n
    | .leftOn i => st.narrowOn s.fuel i n) st
  let st := Transfer.carry s st k
  c.items.foldl (fun st i => PushDown.pushDown s s.fuel s.fuel st i (st.items i).term []) st

/-- The items `pushDown` passes a condition on item `i`'s column `c` into,
at every depth. -/
def PushDown.reached (s : Statement) : Nat → Nat → Nat → List Nat
  | 0, _, _ => []
  | fuel + 1, i, c =>
    match PushDown.arms s i with
    | none => []
    | some found => found.flatMap fun arm =>
      match (arm[c]?).join with
      | some (t, c') => t :: PushDown.reached s fuel t c'
      | none => []

/-- The items derived item `d` can pass the edges it takes into, once it is
placed: those an arm passes on a column an edge of `d` compares, at every
depth (`derived::receivers`). -/
def receivers (s : Statement) (st : PState) (d : Nat) : List Nat :=
  (st.edges.filter (·.member == d)).flatMap fun e =>
    e.memberValue.columns.flatMap (PushDown.reached s s.fuel d)

/-- `found` with each item of `items` that `adds` takes, given those found,
added one at a time until it takes none, in at most `fuel` steps. -/
def closure (items : List Nat) (adds : List Nat → Nat → Bool) : Nat → List Nat → List Nat
  | 0, found => found
  | fuel + 1, found =>
    match items.find? fun i => !found.contains i && adds found i with
    | some i => closure items adds fuel (found ++ [i])
    | none => found

/-- Whether item `i` takes its values from items that all wait. A derived item
that an edge from its partner reads through gives that partner no values, so
the partner counts as none of its partners. -/
def starved (st : PState) (waiting : List Nat) (i : Nat) : Bool :=
  let counted := st.edges.filter fun e => e.member == i &&
    !st.edges.any fun back => back.member == e.partner && back.via.map (·.1) == some i
  let partners := counted.map (·.partner)
  !partners.isEmpty && partners.all waiting.contains

/-- Whether a waiting derived item could pass edges into item `i`: placed
before that item, it would take none of them. -/
def received (s : Statement) (st : PState) (waiting : List Nat) (i : Nat) : Bool :=
  waiting.any fun d => (receivers s st d).contains i

/-- Whether item `i` has values to give before any item is placed: it has
conditions of its own, or it is a derived item that no condition passes into,
whose rows nothing around it narrows, or one that reads an item that has
(`Planner::gives_alone`). -/
def givesAlone (s : Statement) (st : PState) : Nat → Nat → Bool
  | 0, _ => false
  | fuel + 1, i =>
    (st.items i).term.isSome || match (s.item i).kind with
      | .table _ => false
      | .derived q _ => (PushDown.arms s i == some []) ||
          (s.query q).arms.any fun k => (s.core k).items.any (givesAlone s st fuel)

/-- The items of `remaining` that items giving values reach by edges, either
way, with none of `waiting` among them: a giver has values to give on its own
or a partner placed already. -/
def givenValues (s : Statement) (st : PState) (order remaining waiting : List Nat) : List Nat :=
  let gives (i : Nat) : Bool := !waiting.contains i &&
    (givesAlone s st s.fuel i ||
      st.edges.any fun e => e.member == i && order.contains e.partner)
  closure remaining
    (fun reached i => !waiting.contains i && st.edges.any fun e =>
      (e.member == i && reached.contains e.partner) ||
        (e.partner == i && reached.contains e.member))
    remaining.length (remaining.filter gives)

/-- The items of `remaining` that wait (`Planner::waiting`): those taking a
subquery's value, or a value a derived item's arm passes on, from an item not
placed yet, or through a derived item not placed yet; those whose every
partner waits; those a waiting derived item could pass edges into; and, among
the items that no giver of values reaches, those that take values from one
that waits. -/
def waiting (s : Statement) (st : PState) (order remaining : List Nat) : List Nat :=
  let first := remaining.filter fun i =>
    st.edges.any (fun e => e.nested && e.member == i && !e.placedIn order) ||
      (st.items i).ors.any fun sides => sides.any fun side =>
        side.edges.any fun e => e.nested && !e.placedIn order
  let alone := closure remaining (fun w i => starved st w i || received s st w i)
    remaining.length first
  let given := givenValues s st order remaining alone
  closure remaining
    (fun w i => (!given.contains i && st.edges.any fun e => e.member == i && w.contains e.partner)
      || received s st w i)
    remaining.length alone

/-- Place the items one by one, each taking as memberships its edges to
items placed before it, through derived items placed before it, and passing
them into it when it is derived (`Planner::ordered`). -/
def ordered (s : Statement) (st : PState) : PState × List Nat :=
  let all := s.cores.length |> List.range |>.flatMap fun k => (s.core k).items
  go all.length all st [] []
where
  go : Nat → List Nat → PState → List Nat → List Nat → PState × List Nat
    | 0, _, st, order, _ => (st, order)
    | f + 1, all, st, order, waited =>
      let remaining := all.filter fun i => !order.contains i
      match remaining with
      | [] => (st, order)
      | r :: _ =>
        let w := waiting s st order remaining
        let waited := waited ++ w.filter fun i => !waited.contains i
        let released := remaining.find? fun i => waited.contains i && !w.contains i
        let free := remaining.filter fun i => !w.contains i
        let next := match released, free with
          | some i, _ => i
          | none, first :: _ =>
            let core := (s.item first).core
            let candidates := free.filter fun i => (s.item i).core == core
            let takesUnplacedValues (i : Nat) : Bool := st.edges.any fun e =>
              e.member == i && candidates.contains e.partner
                && (s.table? e.partner).isNone && (st.items e.partner).term.isNone
                && !st.edges.any (·.member == e.partner)
            (candidates.find? fun i => st.edges.any fun e => e.member == i && order.contains e.partner)
              |>.orElse (fun _ => candidates.find? fun i =>
                (st.items i).term.isSome && !takesUnplacedValues i)
              |>.orElse (fun _ => candidates.find? fun i => (s.table? i).isNone)
              |>.getD first
          | none, [] => r
        let taken := st.edges.filter fun e => e.member == next && e.placedIn order
        let either := (st.items next).ors.filter fun sides =>
          sides.all fun side => side.edges.all fun e => e.placedIn order
        let st := st.update next fun p => { p with memberships := p.memberships ++ taken, either }
        let st := PushDown.pushDown s s.fuel s.fuel st next none taken
        go f all st (order ++ [next]) waited

/-- The planner's state once every core is read and every item placed, with
the order, the root's LIMIT bound and the subqueries' windows. -/
structure Planned where
  state : PState
  order : List Nat
  sorted : Option Limit.SortedLimit
  windows : List Window.Window

def plan (s : Statement) : Planned :=
  let st := s.readOrder.foldl (readCore s) (PState.initial s)
  let sorted := Limit.sortedLimit s st
  let (st, order) := ordered s st
  { state := st, order, sorted, windows := Window.windows s }

/-- The scope item `i`'s window gives it: every row for an item no window
sorts. -/
def Planned.windowScope (p : Planned) (i : Nat) : RScope :=
  match p.windows.find? (·.item == i) with
  | some w => w.scope
  | none => .all

/-- The scope a membership gives its member, with `bound` giving each item
placed before it its scope: the member's values among those its partner's
scope holds, when the scope says, or the rows whose value a row of the
partner in its scope gives. -/
def Planned.membershipScope (bound : Nat → RScope) (m : Edge) : RScope :=
  let partner := bound m.partner
  match m.memberValue, m.value with
  | .column mc, .column c =>
    match partner.equalValues c with
    | some vs => .inVals mc vs.eraseDups
    | none => .joined m.memberValue m.partner m.value partner m.nulls
  | _, v => .joined m.memberValue m.partner v partner m.nulls

/-- Whether a membership gives its member the rows whose value a row of its
partner gives, as a lookup finds them, not those of a value its partner's
scope holds: it takes values from a partner of its core, compares by `=`,
is no edge passed into a derived item's arm, and its member is a column that compares with a
column of the partner no scope fixes, or with an expression of the partner. -/
def Planned.listsPartner (bound : Nat → RScope) (m : Edge) : Bool :=
  !m.nested && !m.pushed && !m.nulls &&
    match m.memberValue, m.value with
    | .column _, .column c => (bound m.partner).equalValues c |>.isNone
    | .column _, .expr _ => true
    | .expr _, _ => false

/-- The scopes the memberships of one item that join one partner give
together: the rows one row of the partner gives every value of. A join that
compares several columns with a partner finds one partner row, so the
partner's rows are looked up once for all of them (`Planned.listsPartner`
takes the memberships it covers). -/
def Planned.groupScopes (bound : Nat → RScope) (ms : List Edge) : List RScope :=
  let listed := ms.filter (Planned.listsPartner bound)
  (listed.map (·.partner)).eraseDups.filterMap fun p =>
    match listed.filter (·.partner == p) with
    | [] | [_] => none
    | es => some (.joinedAll es p (bound p))

/-- The scope one side of an OR gives its item: its terms, and the rows its
edges join of their partners' scopes. -/
def Planned.sideScope (bound : Nat → RScope) (side : Side) : RScope :=
  side.edges.foldl (fun acc m => acc.both (.joined m.memberValue m.partner m.value (bound m.partner) m.nulls))
    (side.terms.foldl (fun acc t => acc.both t.bind) .all)

/-- How a run's item `i` looks up the rows of an earlier run's item `j`:
through the first of its memberships that takes values from `j`. -/
def Planned.lookupOf (p : Planned) (bound : Nat → RScope) (i j : Nat) : Option (RScope → RScope) :=
  ((p.state.items i).memberships.find? (·.partner == j)).map fun m before =>
    .joined m.memberValue m.partner m.value ((bound m.partner).both before) m.nulls

/-- Item `i`'s scope for a run whose last row's ORDER BY values are `last`,
with `bound` giving each item placed before it its scope: its terms, what
its edges carry, its memberships, its ORs, and the LIMIT bound when the run
returned as many rows as its LIMIT. -/
def Planned.scopeAt (p : Planned) (last : Option (List (List Value))) (bound : Nat → RScope)
    (i : Nat) : RScope :=
  let item := p.state.items i
  let scope := ((item.term.map STerm.bind).getD .all).both
    ((item.carried.map STerm.bind).getD .all)
  let scope := item.memberships.foldl (fun scope m => scope.both (membershipScope bound m)) scope
  let scope := (Planned.groupScopes bound item.memberships).foldl RScope.both scope
  let scope := item.either.foldl (fun scope sides =>
    match sides.map (sideScope bound) with
    | [] => scope
    | first :: rest => scope.both (rest.foldl RScope.either first)) scope
  match p.sorted, last with
  | some sorted, some values =>
    match Limit.runBound i (p.lookupOf bound i) (sorted.runs.zip values) with
    | some found => scope.both found
    | none => scope
  | _, _ => scope

/-- Each item's scope for a run whose last row's ORDER BY values are `last`,
run by run, when it returned as many rows as its LIMIT
(`scope_lookup::scopes`). -/
def Planned.scopes (p : Planned) (last : Option (List (List Value))) : Nat → RScope :=
  p.order.foldl (fun bound i =>
    let scope := p.scopeAt last bound i
    fun j => if j = i then scope else bound j) fun _ => .all

/-- Each item's scope as its table's scope reads it: its scope among
`Planned.scopes`, which lookups read, and its window's
(`scope_lookup::scopes`). -/
def Planned.itemScopes (p : Planned) (last : Option (List (List Value))) (i : Nat) : RScope :=
  (p.scopes last i).both (p.windowScope i)

/-- Each table's scope: the scopes of the items reading it, joined with OR;
`none` for a table no item reads. -/
def tableScope (s : Statement) (scopes : Nat → RScope) (t : Nat) : Option RScope :=
  s.itemNumbers.foldl (fun found i =>
    if s.table? i = some t then
      some (match found with
        | some other => other.either (scopes i)
        | none => scopes i)
    else found) none

end SqliveNarrowing
