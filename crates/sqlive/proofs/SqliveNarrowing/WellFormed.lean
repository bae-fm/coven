import SqliveNarrowing.Rules.View

/-! What the runner's JSON reader guarantees of every statement it reads
(`Json.readStatement`), as one check the runner reports and the differential
asserts. The composed proof assumes it: a statement the reader can't build
is no statement sqlive runs. -/
namespace SqliveNarrowing

/-- The queries core `k` reads: its derived items' and its subqueries. -/
def Statement.nestedQueries (s : Statement) (k : Nat) : List Nat :=
  (s.core k).items.filterMap (fun i =>
    match (s.item i).kind with
    | .derived q _ => some q
    | .table _ => none) ++ (s.core k).subqueries

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

/-- What the runner's JSON reader guarantees: the root is a query; each query's
arms are cores, and the queries each arm reads have lower numbers, as the
reader numbers a query after everything it holds; each item a core lists is an
item of that core; a readable derived item's query is readable alone; a LEFT
JOIN's ON names only its own item, items before it and outer items; and every
column a core's clauses name is of an item of the core or of a core it sits
in. -/
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
  (List.range s.cores.length).all fun k => (s.core k).refs.all fun (i, _) =>
    (s.scopeChain k).any fun k' => (s.core k').items.contains i

/-- A well-formed statement's cores list only items in range. -/
theorem Statement.wellFormed_items {s : Statement} (h : s.wellFormed = true) {k i : Nat}
    (hi : i ∈ (s.core k).items) : i < s.items.length := by
  by_cases hk : k < s.cores.length
  · simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range,
      decide_eq_true_eq, beq_iff_eq] at h
    exact (h.1.1.1.2 k hk i hi).1
  · have : s.core k = default := by
      simp only [Statement.core, List.getD_eq_getElem?_getD]
      rw [List.getElem?_eq_none (Nat.le_of_not_lt hk)]
      rfl
    rw [this] at hi
    cases hi

end SqliveNarrowing
