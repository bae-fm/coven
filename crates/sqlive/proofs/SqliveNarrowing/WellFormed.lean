import SqliveNarrowing.Rules.View

/-! What the runner's JSON reader guarantees of every statement it reads
(`Json.readStatement`), as one check the runner reports and the differential
asserts: queries numbered after the queries their arms read, items in range
and in the core that lists them, derived items readable alone where the
planner reads them so, LEFT JOIN ONs that name earlier items, column
references to items in scope, and the tree the queries and cores make.
`whole_narrowing_sound` uses it for its items in range, which make the
masked database give the statement's rows (`result_mask`), `evalQuery_fuel`
for its query numbers, and the restriction for the outer items each query
names (`Outer.lean`). -/
namespace SqliveNarrowing

/-- Whether derived query `q` is readable alone where the statement reads it:
its arms sit in no other core, or the queries from some number up to `q`
name nothing outside them, as the reader checks (`Json.closedSince`). -/
def Statement.readsAlone (s : Statement) (q : Nat) : Bool :=
  (s.query q).arms.all (fun k => (s.core k).parent.isNone) ||
    (List.range (q + 1)).any fun first =>
      View.closed s ((List.range (q + 1 - first)).map (first + ·))

/-- Whether the conditions of a LEFT JOIN's ON in core `k` name only the item
the join adds, items before it in the core's FROM, and items of the cores `k`
sits in. -/
def Statement.onsNameEarlier (s : Statement) (k : Nat) : Bool :=
  let c := s.core k
  c.conds.all fun
    | (_, .inner) => true
    | (cd, .leftOn i) => (s.item i).left && c.items.contains i &&
        cd.items.all fun j => j == i || (c.items.takeWhile (· != i)).contains j || !c.items.contains j

/-- The queries the statement reads, each where it reads it: its own, then
each core's derived items' and subqueries'. -/
def Statement.readQueries (s : Statement) : List Nat :=
  s.root :: (List.range s.cores.length).flatMap s.nestedQueries

/-- The cores the queries list as arms. -/
def Statement.armCores (s : Statement) : List Nat :=
  (List.range s.queries.length).flatMap fun q => (s.query q).arms

/-- The statement's own query sits in no core. -/
def Statement.rootOutside (s : Statement) : Bool :=
  (s.query s.root).arms.all fun k => (s.core k).parent.isNone

/-- Each core sits in an arm of a query numbered after its own. -/
def Statement.parentsAfter (s : Statement) : Bool :=
  (List.range s.cores.length).all fun k =>
    match (s.core k).parent with
    | none => true
    | some p => (List.range s.queries.length).any fun qp => (s.query qp).arms.contains p &&
        (List.range s.queries.length).all fun qk => !(s.query qk).arms.contains k || decide (qk < qp)

/-- A subquery's arms sit in the core that holds it, and a derived item's
query's arms in no core or in the core its own core sits in. -/
def Statement.parentsMatch (s : Statement) : Bool :=
  (List.range s.cores.length).all fun k =>
    ((s.core k).subqueries.all fun q => (s.query q).arms.all fun a => (s.core a).parent == some k) &&
    (s.core k).items.all fun i =>
      match (s.item i).kind with
      | .derived q _ => (s.query q).arms.all fun a =>
          (s.core a).parent.isNone || (s.core a).parent == (s.core k).parent
      | .table _ => true

/-- A LEFT JOIN's ON's subqueries name, of the ON's own core, only the item
the join adds and items before it. -/
def Statement.onSubqueriesNameEarlier (s : Statement) : Bool :=
  (List.range s.cores.length).all fun k => (s.core k).conds.all fun
    | (_, .inner) => true
    | (cd, .leftOn i) => cd.queries.all fun q => (s.outerNames q).all fun j =>
        (s.item j).core != k || ((s.core k).items.takeWhile (· != i) ++ [i]).contains j

/-- What the runner's JSON reader guarantees: the root is a query; each query's
arms are cores, and the queries each arm reads have lower numbers, as the
reader numbers a query after everything it holds; each item a core lists is an
item of that core; a readable derived item's query is readable alone; a LEFT
JOIN's ON names only its own item, items before it and outer items; every
column a core's clauses name is of an item of the core or of a core it sits
in; and the statement is a tree: each core lists each item once, one place
reads each query, the statement's own query included, which no core reads,
and each core is an arm of one query. The reader gives each FROM item, CTE
reference and subquery a query of its own, and each query arms of its own.
The cores sit in one another as the queries hold one another: the
statement's own query sits in no core, each core sits in an arm of a query
numbered after its own, a subquery's arms sit in the core that holds it, and
a derived item's query's arms in no core (a view) or in the core around the
item's core (a FROM subquery or CTE); and a LEFT JOIN's ON's subqueries name
no item after the join's of the ON's core. -/
def Statement.wellFormed (s : Statement) : Bool :=
  s.root < s.queries.length &&
  (List.range s.queries.length).all (fun q => (s.query q).arms.all fun k =>
    k < s.cores.length && (s.nestedQueries k).all (· < q)) &&
  (List.range s.cores.length).all (fun k => (s.core k).items.all fun i =>
    i < s.items.length && (s.item i).core == k) &&
  (List.range s.items.length).all (fun i =>
    match (s.item i).kind with
    | .derived q _ => !(s.item i).readable || s.readsAlone q
    | .table _ => true) &&
  (List.range s.cores.length).all s.onsNameEarlier &&
  ((List.range s.cores.length).all fun k => (s.core k).refs.all fun (i, _) =>
    (s.scopeChain k).any fun k' => (s.core k').items.contains i) &&
  (List.range s.cores.length).all (fun k => decide (s.core k).items.Nodup) &&
  decide s.readQueries.Nodup &&
  decide s.armCores.Nodup &&
  s.rootOutside && s.parentsAfter && s.parentsMatch && s.onSubqueriesNameEarlier

/-- The parts of `wellFormed` the proofs take, each stated on its own. -/
structure Statement.WellFormedParts (s : Statement) : Prop where
  root : s.root < s.queries.length
  arms : ∀ q < s.queries.length, ∀ k ∈ (s.query q).arms,
    k < s.cores.length ∧ ∀ q' ∈ s.nestedQueries k, q' < q
  items : ∀ k < s.cores.length, ∀ i ∈ (s.core k).items, i < s.items.length ∧ (s.item i).core = k
  refs : ∀ k < s.cores.length, ∀ p ∈ (s.core k).refs, ∃ k' ∈ s.scopeChain k, p.1 ∈ (s.core k').items
  nodup : ∀ k < s.cores.length, (s.core k).items.Nodup
  readQueries : s.readQueries.Nodup
  armCores : s.armCores.Nodup
  rootOutside : ∀ k ∈ (s.query s.root).arms, (s.core k).parent = none
  parentsAfter : ∀ k < s.cores.length, ∀ p, (s.core k).parent = some p →
    ∃ qp < s.queries.length, p ∈ (s.query qp).arms ∧
      ∀ qk < s.queries.length, k ∈ (s.query qk).arms → qk < qp
  subqueryParents : ∀ k < s.cores.length, ∀ q ∈ (s.core k).subqueries, ∀ a ∈ (s.query q).arms,
    (s.core a).parent = some k
  derivedParents : ∀ k < s.cores.length, ∀ i ∈ (s.core k).items, ∀ q v, (s.item i).kind = .derived q v →
    ∀ a ∈ (s.query q).arms, (s.core a).parent = none ∨ (s.core a).parent = (s.core k).parent
  onNames : ∀ k < s.cores.length, ∀ cd i, (cd, Place.leftOn i) ∈ (s.core k).conds →
    ∀ q ∈ cd.queries, ∀ j ∈ s.outerNames q, (s.item j).core = k →
      j ∈ (s.core k).items.takeWhile (· ≠ i) ++ [i]

theorem Statement.wellFormed_parts {s : Statement} (h : s.wellFormed = true) : s.WellFormedParts := by
  simp only [Statement.wellFormed, Statement.rootOutside, Statement.parentsAfter,
    Statement.parentsMatch, Statement.onSubqueriesNameEarlier, Bool.and_eq_true, List.all_eq_true,
    List.mem_range, decide_eq_true_eq, beq_iff_eq, List.any_eq_true, List.contains_iff_mem,
    Option.isNone_iff_eq_none] at h
  obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨hroot, harms⟩, hitems⟩, _⟩, _⟩, hrefs⟩, hnodup⟩, hread⟩, harmCores⟩,
    hout⟩, hafter⟩, hmatch⟩, hon⟩ := h
  refine ⟨hroot, harms, hitems, fun k hk p hp => hrefs k hk p hp, hnodup, hread, harmCores, hout,
    fun k hk p hp => ?_, fun k hk q hq a ha => (hmatch k hk).1 q hq a ha,
    fun k hk i hi q v hkind a ha => ?_, fun k hk cd i hcd q hq j hj hcore => ?_⟩
  · have := hafter k hk
    rw [hp] at this
    simp only [List.any_eq_true, List.mem_range, Bool.and_eq_true, List.contains_iff_mem,
      List.all_eq_true, Bool.or_eq_true, Bool.not_eq_true', decide_eq_true_eq] at this
    obtain ⟨qp, hqp, hpq, hlt⟩ := this
    refine ⟨qp, hqp, hpq, fun qk hqk hkq => (hlt qk hqk).resolve_left ?_⟩
    simpa using hkq
  · have := (hmatch k hk).2 i hi
    rw [hkind] at this
    simp only [List.all_eq_true, Bool.or_eq_true, Option.isNone_iff_eq_none, beq_iff_eq] at this
    exact this a ha
  · have := hon k hk (cd, .leftOn i) hcd
    simp only [List.all_eq_true, Bool.or_eq_true, bne_iff_ne, ne_eq, List.contains_iff_mem,
      List.mem_append, List.mem_singleton] at this
    rcases this q hq j hj with h | h
    · exact absurd hcore h
    · have he : (fun x : Nat => x != i) = (fun x => decide (x ≠ i)) := by
        funext x
        simp only [bne, ne_eq, decide_not]
        rfl
      simpa [he] using h

/-- A well-formed statement's cores list only items in range. -/
theorem Statement.wellFormed_items {s : Statement} (h : s.wellFormed = true) {k i : Nat}
    (hi : i ∈ (s.core k).items) : i < s.items.length := by
  by_cases hk : k < s.cores.length
  · exact ((s.wellFormed_parts h).items k hk i hi).1
  · have : s.core k = default := by
      simp only [Statement.core, List.getD_eq_getElem?_getD]
      rw [List.getElem?_eq_none (Nat.le_of_not_lt hk)]
      rfl
    rw [this] at hi
    cases hi

end SqliveNarrowing
