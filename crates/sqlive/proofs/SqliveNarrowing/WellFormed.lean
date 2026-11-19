import SqliveNarrowing.Rules.View

/-! What the runner's JSON reader guarantees of every statement it reads
(`Json.readStatement`), as one check the runner reports and the differential
asserts: queries numbered after the queries their arms read, items in range
and in the core that lists them, derived items readable alone where the
planner reads them so, LEFT JOIN ONs that name earlier items, and column
references to items in scope. `whole_narrowing_sound` uses it for its items
in range, which make the masked database give the statement's rows
(`result_mask`), and `evalQuery_fuel` for its query numbers. -/
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

/-- The queries the statement reads, each where it reads it: its own, then
each core's derived items' and subqueries'. -/
def Statement.readQueries (s : Statement) : List Nat :=
  s.root :: (List.range s.cores.length).flatMap s.nestedQueries

/-- The cores the queries list as arms. -/
def Statement.armCores (s : Statement) : List Nat :=
  (List.range s.queries.length).flatMap fun q => (s.query q).arms

/-- What the runner's JSON reader guarantees: the root is a query; each query's
arms are cores, and the queries each arm reads have lower numbers, as the
reader numbers a query after everything it holds; each item a core lists is an
item of that core; a readable derived item's query is readable alone; a LEFT
JOIN's ON names only its own item, items before it and outer items; every
column a core's clauses name is of an item of the core or of a core it sits
in; and the statement is a tree: each core lists each item once, one place
reads each query, the statement's own query included, which no core reads,
and each core is an arm of one query. The reader gives each FROM item, CTE
reference and subquery a query of its own, and each query arms of its own. -/
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
  decide s.armCores.Nodup

/-- A well-formed statement's cores list only items in range. -/
theorem Statement.wellFormed_items {s : Statement} (h : s.wellFormed = true) {k i : Nat}
    (hi : i ∈ (s.core k).items) : i < s.items.length := by
  by_cases hk : k < s.cores.length
  · simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range,
      decide_eq_true_eq, beq_iff_eq] at h
    exact (h.1.1.1.1.1.1.2 k hk i hi).1
  · have : s.core k = default := by
      simp only [Statement.core, List.getD_eq_getElem?_getD]
      rw [List.getElem?_eq_none (Nat.le_of_not_lt hk)]
      rfl
    rw [this] at hi
    cases hi

/-- A core out of range is the default one, which lists nothing. -/
theorem Statement.core_out {s : Statement} {k : Nat} (hk : ¬k < s.cores.length) :
    s.core k = default := by
  simp only [Statement.core, List.getD_eq_getElem?_getD]
  rw [List.getElem?_eq_none (Nat.le_of_not_lt hk)]
  rfl

/-- A query out of range is the default one, with no arms. -/
theorem Statement.query_out {s : Statement} {q : Nat} (hq : ¬q < s.queries.length) :
    s.query q = default := by
  simp only [Statement.query, List.getD_eq_getElem?_getD]
  rw [List.getElem?_eq_none (Nat.le_of_not_lt hq)]
  rfl

/-- A well-formed statement's cores list each item once. -/
theorem Statement.wellFormed_nodup {s : Statement} (h : s.wellFormed = true) (k : Nat) :
    (s.core k).items.Nodup := by
  by_cases hk : k < s.cores.length
  · simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range,
      decide_eq_true_eq] at h
    exact h.1.1.2 k hk
  · rw [s.core_out hk]
    exact List.nodup_nil

/-- One place reads each query of a well-formed statement. -/
theorem Statement.wellFormed_readQueries {s : Statement} (h : s.wellFormed = true) :
    s.readQueries.Nodup := by
  simp only [Statement.wellFormed, Bool.and_eq_true, decide_eq_true_eq] at h
  exact h.1.2

/-- Each core of a well-formed statement is an arm of one query. -/
theorem Statement.wellFormed_armCores {s : Statement} (h : s.wellFormed = true) :
    s.armCores.Nodup := by
  simp only [Statement.wellFormed, Bool.and_eq_true, decide_eq_true_eq] at h
  exact h.2

/-- The lists a list without repeats joins hold no element in common. -/
theorem disjoint_of_nodup_flatMap {α β : Type} {f : α → List β} :
    ∀ {l : List α}, (l.flatMap f).Nodup → ∀ {a a'}, a ∈ l → a' ∈ l → a ≠ a' →
      ∀ {x}, x ∈ f a → x ∈ f a' → False
  | [], _, _, _, ha, _, _, _, _, _ => by cases ha
  | c :: l, h, a, a', ha, ha', hne, x, hx, hx' => by
    rw [List.flatMap_cons, List.nodup_append] at h
    obtain ⟨_, hl, hdis⟩ := h
    rcases List.mem_cons.mp ha with rfl | hal
    · rcases List.mem_cons.mp ha' with rfl | hal'
      · exact hne rfl
      · exact hdis x hx x (List.mem_flatMap.mpr ⟨a', hal', hx'⟩) rfl
    · rcases List.mem_cons.mp ha' with rfl | hal'
      · exact hdis x hx' x (List.mem_flatMap.mpr ⟨a, hal, hx⟩) rfl
      · exact disjoint_of_nodup_flatMap hl hal hal' hne hx hx'

/-- A core out of range reads no query. -/
theorem Statement.nestedQueries_out {s : Statement} {k : Nat} (hk : ¬k < s.cores.length) :
    s.nestedQueries k = [] := by
  unfold Statement.nestedQueries
  rw [s.core_out hk]
  rfl

/-- No core of a well-formed statement reads the statement's own query. -/
theorem Statement.wellFormed_root {s : Statement} (h : s.wellFormed = true) (k : Nat) :
    s.root ∉ s.nestedQueries k := by
  intro hmem
  by_cases hk : k < s.cores.length
  · have hnd := s.wellFormed_readQueries h
    simp only [Statement.readQueries, List.nodup_cons, List.mem_flatMap, List.mem_range] at hnd
    exact hnd.1 ⟨k, hk, hmem⟩
  · rw [s.nestedQueries_out hk] at hmem
    cases hmem

/-- Two cores of a well-formed statement that read one query are one core. -/
theorem Statement.wellFormed_reader {s : Statement} (h : s.wellFormed = true) {k k' q : Nat}
    (hq : q ∈ s.nestedQueries k) (hq' : q ∈ s.nestedQueries k') : k = k' := by
  have hk : k < s.cores.length := by
    refine Decidable.byContradiction fun hk => ?_
    rw [s.nestedQueries_out hk] at hq
    cases hq
  have hk' : k' < s.cores.length := by
    refine Decidable.byContradiction fun hk' => ?_
    rw [s.nestedQueries_out hk'] at hq'
    cases hq'
  have hnd := (List.nodup_cons.mp (s.wellFormed_readQueries h)).2
  exact Decidable.byContradiction fun hne =>
    disjoint_of_nodup_flatMap hnd (List.mem_range.mpr hk) (List.mem_range.mpr hk') hne hq hq'

/-- Two queries of a well-formed statement with a common arm are one query. -/
theorem Statement.wellFormed_armOf {s : Statement} (h : s.wellFormed = true) {q q' k : Nat}
    (hk : k ∈ (s.query q).arms) (hk' : k ∈ (s.query q').arms) : q = q' := by
  have hq : q < s.queries.length := by
    refine Decidable.byContradiction fun hq => ?_
    rw [s.query_out hq] at hk
    cases hk
  have hq' : q' < s.queries.length := by
    refine Decidable.byContradiction fun hq' => ?_
    rw [s.query_out hq'] at hk'
    cases hk'
  exact Decidable.byContradiction fun hne =>
    disjoint_of_nodup_flatMap (s.wellFormed_armCores h) (List.mem_range.mpr hq)
      (List.mem_range.mpr hq') hne hk hk'

end SqliveNarrowing
