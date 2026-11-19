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

/-- The items of `remaining` that wait: those taking a subquery's value from
an item not placed yet, and those whose every partner waits
(`Planner::waiting`). -/
def waiting (st : PState) (order remaining : List Nat) : List Nat :=
  let first := remaining.filter fun i =>
    st.edges.any (fun e => e.nested && e.member == i && !order.contains e.partner) ||
      (st.items i).ors.any fun sides => sides.any fun side =>
        side.edges.any fun e => e.nested && !order.contains e.partner
  go remaining.length first
where
  go : Nat → List Nat → List Nat
    | 0, w => w
    | f + 1, w =>
      let starved := remaining.find? fun i =>
        let partners := (st.edges.filter (·.member == i)).map (·.partner)
        !w.contains i && !partners.isEmpty && partners.all w.contains
      match starved with
      | some i => go f (w ++ [i])
      | none => w

/-- Place the items one by one, each taking as memberships its edges to
items placed before it, and passing them into it when it is derived
(`Planner::ordered`). -/
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
        let w := waiting st order remaining
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
        let taken := st.edges.filter fun e => e.member == next && order.contains e.partner
        let either := (st.items next).ors.filter fun sides =>
          sides.all fun side => side.edges.all fun e => order.contains e.partner
        let st := st.update next fun p => { p with memberships := p.memberships ++ taken, either }
        let st := PushDown.pushDown s s.fuel s.fuel st next none taken
        go f all st (order ++ [next]) waited

/-- The planner's state once every core is read and every item placed, with
the order and the root's LIMIT bound. -/
structure Planned where
  state : PState
  order : List Nat
  sorted : Option Limit.SortedLimit

def plan (s : Statement) : Planned :=
  let st := s.readOrder.foldl (readCore s) (PState.initial s)
  let sorted := Limit.sortedLimit s st
  let (st, order) := ordered s st
  { state := st, order, sorted }

/-- Each item's scope for a run whose last row's ORDER BY values are `last`,
run by run, when it returned as many rows as its LIMIT
(`scope_lookup::scopes`). -/
def Planned.scopes (p : Planned) (last : Option (List (List Value))) : Nat → RScope :=
  p.order.foldl (fun bound i =>
    let item := p.state.items i
    let scope := ((item.term.map STerm.bind).getD .all).both
      ((item.carried.map STerm.bind).getD .all)
    let scope := item.memberships.foldl (fun scope m =>
      let partner := bound m.partner
      match m.memberValue, m.value with
      | .column mc, .column c =>
        match partner.equalValues c with
        | some vs => scope.both (.inVals mc vs.eraseDups)
        | none => scope.both (.joined m.memberValue m.partner m.value partner m.nulls)
      | _, v => scope.both (.joined m.memberValue m.partner v partner m.nulls)) scope
    let sideScope (side : Side) : RScope :=
      side.edges.foldl (fun acc m => acc.both (.joined m.memberValue m.partner m.value (bound m.partner) m.nulls))
        (side.terms.foldl (fun acc t => acc.both t.bind) .all)
    let scope := item.either.foldl (fun scope sides =>
      match sides.map sideScope with
      | [] => scope
      | first :: rest => scope.both (rest.foldl RScope.either first)) scope
    -- A run's item looks up the rows of an earlier run's item through the
    -- first of its memberships that takes values from it.
    let lookup (j : Nat) : Option (RScope → RScope) :=
      (item.memberships.find? (·.partner == j)).map fun m before =>
        .joined m.memberValue m.partner m.value ((bound m.partner).both before) m.nulls
    let scope := match p.sorted, last with
      | some sorted, some values =>
        match Limit.runBound i lookup (sorted.runs.zip values) with
        | some found => scope.both found
        | none => scope
      | _, _ => scope
    fun j => if j = i then scope else bound j) fun _ => .all

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
