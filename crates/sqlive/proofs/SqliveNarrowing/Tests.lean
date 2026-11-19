import SqliveNarrowing.Restrict
import SqliveNarrowing.Scopes
import SqliveNarrowing.PlannerState

/-! The tests the planner's scopes give the restriction (`Restriction`):
an item's row passes its test when it passes the item's scope, with lookups
in what sqlive keeps of the database a write leaves, each edge the planner
pushed into the item, and, for an item a lookup reads alone, is among the rows the lookup reads. A query's
row matters to the derived item that reads the query when it passes the
derived item's scope and pushed edges; every row of a query no derived item
reads, or with a LIMIT or an OFFSET, matters. `plan_restriction` proves that
a write the judge lets pass leaves the restriction these tests: each table
holds the same rows passing each item's test before and after the write,
and a derived item's test keeps only rows that matter to its query. -/
namespace SqliveNarrowing

/-- Whether item `i`'s row `r` passes its scope among `scopes`, with lookups
`rowsOf`, and each edge the planner pushed into `i`. -/
def Planned.scopeTest (p : Planned) (scopes : Nat → RScope) (rowsOf : Nat → List Row)
    (i : Nat) (r : Row) : Bool :=
  (scopes i).passes rowsOf r && p.state.pushed.all fun e =>
    e.member != i || (Planned.membershipScope scopes e).passes rowsOf r

/-- Item `i`'s test: its row passes its scope and pushed edges, and is among
the rows a lookup reads when one reads `i` alone. -/
def Planned.itemTest (s : Statement) (p : Planned) (scopes : Nat → RScope) (rowsOf : Nat → List Row)
    (i : Nat) (r : Row) : Bool :=
  p.scopeTest scopes rowsOf i r && (!(s.item i).readable || (rowsOf i).contains r)

/-- Query `q`'s test: its row passes the scope and pushed edges of each
derived item a core lists that reads `q`, unless `q` has a LIMIT or an
OFFSET. -/
def Planned.queryTest (s : Statement) (p : Planned) (scopes : Nat → RScope) (rowsOf : Nat → List Row)
    (q : Nat) (r : Row) : Bool :=
  (s.query q).limit.isSome || (s.query q).offset.isSome ||
    (List.range s.cores.length).all fun k => (s.core k).items.all fun d =>
      match (s.item d).kind with
      | .derived q' _ => q' != q || p.scopeTest scopes rowsOf d r
      | .table _ => true

/-- The LIMIT values of the run that read `db`, run by run, when it returned
as many rows as its LIMIT. -/
def Planned.lastOf (s : Statement) (p : Planned) (db : Db) : Option (List (List Value)) :=
  p.sorted.bind fun sorted => Limit.lastValues sorted (s.result db)

/-! One place reads each query. -/

/-- A list a list without repeats maps to with no repeats maps no two of its
elements to one value. -/
theorem eq_of_filterMap_nodup {α β : Type} {f : α → Option β} :
    ∀ {l : List α}, (l.filterMap f).Nodup → l.Nodup → ∀ {a b x}, a ∈ l → b ∈ l → f a = some x →
      f b = some x → a = b
  | [], _, _, _, _, _, ha, _, _, _ => by cases ha
  | c :: l, h, hl, a, b, x, ha, hb, hfa, hfb => by
    obtain ⟨hc, hl⟩ := List.nodup_cons.mp hl
    have hout : ∀ {y}, y ∈ l → f c = some x → f y = some x → False := by
      intro y hy hfc hfy
      rw [List.filterMap_cons, hfc] at h
      exact (List.nodup_cons.mp h).1 (List.mem_filterMap.mpr ⟨y, hy, hfy⟩)
    rcases List.mem_cons.mp ha with hac | hal
    · rcases List.mem_cons.mp hb with hbc | hbl
      · rw [hac, hbc]
      · rw [hac] at hfa
        exact (hout hbl hfa hfb).elim
    rcases List.mem_cons.mp hb with hbc | hbl
    · rw [hbc] at hfb
      exact (hout hal hfb hfa).elim
    have h' : (l.filterMap f).Nodup := by
      rw [List.filterMap_cons] at h
      split at h
      · exact h
      · exact (List.nodup_cons.mp h).2
    exact eq_of_filterMap_nodup h' hl hal hbl hfa hfb

/-- Each list a list without repeats joins has no repeats. -/
theorem nodup_of_flatMap {α β : Type} {f : α → List β} {x : α} :
    ∀ {l : List α}, (l.flatMap f).Nodup → x ∈ l → (f x).Nodup
  | [], _, hx => by cases hx
  | c :: l, h, hx => by
    rw [List.flatMap_cons, List.nodup_append] at h
    rcases List.mem_cons.mp hx with rfl | hx
    · exact h.1
    · exact nodup_of_flatMap h.2.1 hx

/-- The queries a core of a well-formed statement reads hold no repeats. -/
theorem Statement.nestedQueries_nodup {s : Statement} (h : s.wellFormed = true) {k : Nat}
    (hk : k < s.cores.length) : (s.nestedQueries k).Nodup := by
  have hnd := (List.nodup_cons.mp (s.wellFormed_parts h).readQueries).2
  exact nodup_of_flatMap hnd (List.mem_range.mpr hk)

/-- Two cores of a well-formed statement that read one query are one core. -/
theorem Statement.wellFormed_reader {s : Statement} (h : s.wellFormed = true) {k k' q : Nat}
    (hq : q ∈ s.nestedQueries k) (hq' : q ∈ s.nestedQueries k') : k = k' := by
  have hlt : ∀ {k}, q ∈ s.nestedQueries k → k < s.cores.length := by
    intro k hq
    refine Decidable.byContradiction fun hk => ?_
    unfold Statement.nestedQueries at hq
    rw [s.core_out hk] at hq
    cases hq
  have hnd := (List.nodup_cons.mp (s.wellFormed_parts h).readQueries).2
  exact Decidable.byContradiction fun hne =>
    disjoint_of_nodup_flatMap hnd (List.mem_range.mpr (hlt hq)) (List.mem_range.mpr (hlt hq')) hne hq hq'

/-- The query a derived item reads. -/
def Statement.derivedQuery (s : Statement) (i : Nat) : Option Nat :=
  match (s.item i).kind with
  | .derived q _ => some q
  | .table _ => none

/-- Two derived items the cores of a well-formed statement list that read
one query are one item. -/
theorem Statement.derived_unique {s : Statement} (h : s.wellFormed = true) {k k' i d q : Nat} {v v' : Bool}
    (hi : i ∈ (s.core k).items) (hd : d ∈ (s.core k').items) (hki : (s.item i).kind = .derived q v)
    (hkd : (s.item d).kind = .derived q v') : i = d := by
  have hmem : ∀ {k j v}, j ∈ (s.core k).items → (s.item j).kind = .derived q v → q ∈ s.nestedQueries k :=
    fun hj hkj => List.mem_append_left _ (List.mem_filterMap.mpr ⟨_, hj, by simp [hkj]⟩)
  obtain rfl := s.wellFormed_reader h (hmem hi hki) (hmem hd hkd)
  have hklt := s.core_lt hi
  exact eq_of_filterMap_nodup (List.nodup_append.mp (s.nestedQueries_nodup h hklt)).1
    ((s.wellFormed_parts h).nodup k hklt) hi hd (x := q) (by simp [hki]) (by simp [hkd])

/-- No derived item a core of a well-formed statement lists reads a
subquery. -/
theorem Statement.subquery_not_derived {s : Statement} (h : s.wellFormed = true) {k k' d q : Nat} {v : Bool}
    (hq : q ∈ (s.core k).subqueries) (hd : d ∈ (s.core k').items) (hkd : (s.item d).kind = .derived q v) :
    False := by
  have hd' : q ∈ (s.core k').items.filterMap (fun i =>
      match (s.item i).kind with
      | .derived q _ => some q
      | .table _ => none) := List.mem_filterMap.mpr ⟨d, hd, by simp [hkd]⟩
  have hsub : q ∈ s.nestedQueries k := List.mem_append_right _ hq
  obtain rfl := s.wellFormed_reader h hsub (List.mem_append_left _ hd')
  have hklt := s.core_lt hd
  exact (List.nodup_append.mp (s.nestedQueries_nodup h hklt)).2.2 q hd' q hq rfl

/-- The restriction the planner's tests make: a write the judge lets pass,
on a statement it doesn't read whole, leaves each table holding the same
rows that pass the test of each item reading it, in what sqlive keeps of
the database before and after, and a derived item's test keeps only rows
that matter to its query. -/
theorem plan_restriction {s : Statement} (hwf : s.wellFormed = true) {db db' : Db}
    {changes : List Change} (hw : Writes db changes db') (hr : s.reruns db changes db' = false)
    (hwhole : Whole.readsWhole s = false) :
    let p := plan s
    let scopes := p.scopes (p.lastOf s db)
    let rowsOf := lookupRows s (db'.mask s.reads)
    Restriction s (db.mask s.reads) (db'.mask s.reads) (p.itemTest s scopes rowsOf)
      (p.queryTest s scopes rowsOf) := by
  intro p scopes rowsOf
  have hscopes : s.tableScopes db = some (tableScope s scopes) := by
    simp [Statement.tableScopes, hwhole, scopes, p, Planned.lastOf]
  refine ⟨hwf, fun k i hi t ht => ?_, fun k i hi q v hkind r hr => ?_, fun q k hk q' hq' r => ?_,
    fun q _ hcut r => ?_⟩
  · refine scoped_rows hw hr hscopes (s.wellFormed_items hwf hi) ht fun r hg => ?_
    simp only [Planned.itemTest, Planned.scopeTest, Bool.and_eq_true] at hg
    exact hg.1.1
  · simp only [Planned.queryTest, Bool.or_eq_true, List.all_eq_true, List.mem_range]
    refine Or.inr fun k' _ d hd => ?_
    split
    · rename_i q' v' hkd
      by_cases hq : q' = q
      · subst hq
        obtain rfl := s.derived_unique hwf hi hd hkind hkd
        simp only [Planned.itemTest, Bool.and_eq_true] at hr
        simp [hr.1]
      · simp [hq]
    · rfl
  · simp only [Planned.queryTest, Bool.or_eq_true, List.all_eq_true, List.mem_range]
    refine Or.inr fun k' _ d hd => ?_
    split
    · rename_i q'' v' hkd
      by_cases hq : q'' = q'
      · subst hq
        exact absurd (s.subquery_not_derived hwf hq' hd hkd) id
      · simp [hq]
    · rfl
  · simp [Planned.queryTest, hcut]

end SqliveNarrowing
