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
and a derived item's test keeps only rows that matter to its query.
`Planned.bound_passes` proves that the LIMIT bound passes each sorted item's
row in a binding of the statement's own core that sorts at or before the
last row. -/
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


/-! The LIMIT bound. The ORDER BY sorts items of the statement's own core,
which sits in no core, so no membership between two of them takes a
subquery's value (`plan_nestedOK`), and each lookup the bound makes is a
membership's (`Limit.joined_passes`). The runs keep the ORDER BY's terms in
order, so a binding's values run by run are its ORDER BY values. -/

/-- Grouping the ORDER BY's terms into runs keeps them in order: each run's
terms, each with the run's item, give the terms back. -/
theorem Limit.group_flatten :
    ∀ terms : List (Nat × Limit.SortKey × Bool × Nat),
      (Limit.SortRun.group terms).flatMap (fun run => run.terms.map (run.item, ·)) = terms
  | [] => rfl
  | (i, term) :: rest => by
    have ih := Limit.group_flatten rest
    simp only [Limit.SortRun.group]
    split
    · rename_i run runs hgroup
      rw [hgroup] at ih
      split
      · rename_i hi
        simp only [List.flatMap_cons, List.map_cons, List.cons_append] at ih ⊢
        rw [← hi, ih]
      · simp only [List.flatMap_cons, List.map_cons, List.map_nil, List.cons_append,
          List.nil_append] at ih ⊢
        rw [ih]
    · rename_i hgroup
      rw [hgroup] at ih
      simp only [List.flatMap_cons, List.map_cons, List.map_nil, List.flatMap_nil,
        List.cons_append, List.nil_append] at ih ⊢
      rw [← ih]

/-- A function of each run's terms with the run's item, joined over the
runs, is the function of each term. -/
theorem Limit.group_flatMap {γ : Type} (F : Nat → Limit.SortKey × Bool × Nat → γ)
    (terms : List (Nat × Limit.SortKey × Bool × Nat)) :
    (Limit.SortRun.group terms).flatMap (fun run => run.terms.map (F run.item)) =
      terms.map fun t => F t.1 t.2 := by
  conv => rhs; rw [← Limit.group_flatten terms]
  rw [List.map_flatMap]
  simp [List.map_map, Function.comp_def]

/-- Each run's item is the item of one of the terms. -/
theorem Limit.group_item :
    ∀ {terms : List (Nat × Limit.SortKey × Bool × Nat)} {run : Limit.SortRun},
      run ∈ Limit.SortRun.group terms → ∃ t ∈ terms, t.1 = run.item
  | [], _, h => by cases h
  | (i, term) :: rest, run, h => by
    simp only [Limit.SortRun.group] at h
    split at h
    · rename_i r runs hgroup
      split at h
      · rename_i hi
        rcases List.mem_cons.mp h with rfl | h
        · exact ⟨(i, term), by simp, by simp [hi]⟩
        · obtain ⟨t, ht, he⟩ := Limit.group_item (hgroup ▸ List.mem_cons_of_mem r h)
          exact ⟨t, List.mem_cons_of_mem _ ht, he⟩
      · rcases List.mem_cons.mp h with rfl | h
        · exact ⟨(i, term), by simp, rfl⟩
        · obtain ⟨t, ht, he⟩ := Limit.group_item (hgroup ▸ h)
          exact ⟨t, List.mem_cons_of_mem _ ht, he⟩
    · rename_i hgroup
      simp only [List.mem_singleton] at h
      subst h
      exact ⟨(i, term), by simp, rfl⟩

/-- Each term's item is a run's item. -/
theorem Limit.term_item {terms : List (Nat × Limit.SortKey × Bool × Nat)} {t : Nat × Limit.SortKey × Bool × Nat}
    (ht : t ∈ terms) : ∃ run ∈ Limit.SortRun.group terms, run.item = t.1 := by
  rw [← Limit.group_flatten terms] at ht
  obtain ⟨run, hrun, hmem⟩ := List.mem_flatMap.mp ht
  obtain ⟨term, _, rfl⟩ := List.mem_map.mp hmem
  exact ⟨run, hrun, rfl⟩

/-- An item `runBound` bounds is a run's item. -/
theorem Limit.runBound_item {i : Nat} {lookup : Nat → Option (RScope → RScope)} :
    ∀ {runs : List (Limit.SortRun × List Value)} {found : RScope},
      Limit.runBound i lookup runs = some found → ∃ p ∈ runs, p.1.item = i
  | [], _, h => by simp [Limit.runBound] at h
  | (run, last) :: rest, found, h => by
    by_cases hi : run.item = i
    · exact ⟨(run, last), by simp, hi⟩
    · simp only [Limit.runBound, hi, ↓reduceIte] at h
      cases hlater : Limit.runBound i lookup rest with
      | none => simp [hlater] at h
      | some later =>
        obtain ⟨p, hp, he⟩ := Limit.runBound_item hlater
        exact ⟨p, List.mem_cons_of_mem _ hp, he⟩

/-- `mapM` gives as many elements as it maps, each the image of the element
it pairs with. -/
theorem mapM_zip {α β : Type} {g : α → Option β} :
    ∀ {l : List α} {l' : List β}, l.mapM g = some l' →
      l'.length = l.length ∧ ∀ x ∈ l.zip l', g x.1 = some x.2
  | [], l', h => by simp at h; subst h; simp
  | a :: as, l', h => by
    simp only [List.mapM_cons, Option.bind_eq_bind, Option.bind_eq_some_iff, Option.pure_def,
      Option.some.injEq] at h
    obtain ⟨b, hb, bs, hbs, rfl⟩ := h
    obtain ⟨h1, h2⟩ := mapM_zip hbs
    refine ⟨by simp [h1], fun x hx => ?_⟩
    simp only [List.zip_cons_cons, List.mem_cons] at hx
    rcases hx with rfl | hx
    · exact hb
    · exact h2 x hx

/-- Two lists of one length whose paired elements map to one value map to
one list. -/
theorem map_eq_of_zip {α β γ : Type} {F : α → γ} {F' : β → γ} :
    ∀ {l : List α} {l' : List β}, l'.length = l.length → (∀ x ∈ l.zip l', F x.1 = F' x.2) →
      l.map F = l'.map F'
  | [], [], _, _ => rfl
  | a :: as, b :: bs, hlen, h => by
    simp only [List.map_cons, List.cons.injEq]
    exact ⟨h (a, b) (by simp), map_eq_of_zip (by simpa using hlen) (fun x hx => h x (by simp [hx]))⟩
  | [], _ :: _, hlen, _ => by simp at hlen
  | _ :: _, [], hlen, _ => by simp at hlen

/-- What `sortedLimit` reads of an ORDER BY term `pd` as the term `t` of
item `t.1`: result column `pd.1` holds an expression `e` over `t.1`, an item
of the arm `k` that the state `st` holds no ON narrowing back for, which
`sortKey` sorts by `t.2.1`, in the term's direction, and result column
`t.2.2.2` holds `e` too. -/
def Limit.SortedTerm (s : Statement) (st : PState) (k : Nat) (pd : Nat × Bool)
    (t : Nat × Limit.SortKey × Bool × Nat) : Prop :=
  ∃ e, (s.core k).output[pd.1]? = some (.expr e) ∧ (s.item t.1).core = k ∧
    (st.items t.1).leftOn = none ∧ Limit.sortKey s t.1 e = some t.2.1 ∧ t.2.2.1 = pd.2 ∧
    (s.core k).output[t.2.2.2]? = some (.expr e)

/-- What `sortedLimit` reads: the statement's own query has one arm, which
neither aggregates nor calls a window function, and its ORDER BY's terms,
grouped into runs, each as `SortedTerm` says. -/
theorem Limit.sortedLimit_facts {s : Statement} {st : PState} {sorted : Limit.SortedLimit}
    (h : Limit.sortedLimit s st = some sorted) :
    ∃ k ts, (s.query s.root).arms = [k] ∧ (s.core k).aggregated = false ∧
      (s.core k).windowed = false ∧ sorted.runs = Limit.SortRun.group ts ∧
      ts.length = (s.query s.root).orderBy.length ∧
      ∀ x ∈ (s.query s.root).orderBy.zip ts, Limit.SortedTerm s st k x.1 x.2 := by
  unfold Limit.sortedLimit at h
  cases harms : (s.query s.root).arms with
  | nil => simp [harms] at h
  | cons k rest =>
    cases rest with
    | cons _ _ => simp [harms] at h
    | nil =>
      simp only [harms, Option.bind_eq_bind, Option.bind_eq_some_iff] at h
      obtain ⟨n, _, h⟩ := h
      split at h
      · simp at h
      split at h
      · simp at h
      rename_i _ hmany
      simp only [Bool.or_eq_true, not_or, Bool.not_eq_true] at hmany
      simp only [Option.bind_eq_some_iff, Option.pure_def, Option.some.injEq] at h
      obtain ⟨ts, hts, rfl⟩ := h
      obtain ⟨hlen, hpairs⟩ := mapM_zip hts
      refine ⟨k, ts, rfl, hmany.1, hmany.2, rfl, hlen, fun x hx => ?_⟩
      have hx' := hpairs x hx
      obtain ⟨⟨p, desc⟩, t⟩ := x
      simp only [Option.bind_eq_some_iff] at hx'
      obtain ⟨o, ho, hx'⟩ := hx'
      split at hx'
      · rename_i e
        simp only [Option.bind_eq_some_iff] at hx'
        obtain ⟨⟨i, c⟩, _, hx'⟩ := hx'
        split at hx'
        · simp at hx'
        rename_i hok
        simp only [not_or, Bool.not_eq_true, Option.isSome_eq_false_iff,
          Option.isNone_iff_eq_none, ne_eq, Decidable.not_not] at hok
        simp only [Option.bind_eq_some_iff, Option.some.injEq] at hx'
        obtain ⟨pos, hpos, key, hkey, rfl⟩ := hx'
        obtain ⟨hlt, hmatch, _⟩ := List.findIdx?_eq_some_iff_getElem.mp hpos
        refine ⟨e, ho, hok.1, by simpa using hok.2.2, hkey, rfl, ?_⟩
        rw [List.getElem?_eq_getElem hlt]
        split at hmatch
        · rename_i e' heq
          simp only [beq_iff_eq] at hmatch
          rw [heq, hmatch]
        · cases hmatch
      · cases hx'

/-- A result column that holds `e` holds `e`'s value in a core's row of a
binding. -/
theorem Core.outRow_cell {c : Core} {p : Nat} {e : Expr} (h : c.output[p]? = some (.expr e))
    (E : Bind → Nat → List Row) (b : Bind) : (c.outRow E b).cell p = e.eval b (E b) := by
  simp [Core.outRow, Row.cell, List.getElem?_map, h]

/-- The planned state's `sorted` is what `sortedLimit` reads of the state the
statement's conditions leave. -/
theorem plan_sorted (s : Statement) :
    (plan s).sorted = Limit.sortedLimit s (s.readOrder.foldl (readCore s) (PState.initial s)) := rfl

/-- The LIMIT bound's part of the planner's promise: in a binding `b` of the
statement's own core whose ORDER BY values sort at or before those of the
core's row of `b'`, the LIMIT bound of each item the ORDER BY sorts passes
the item's row, when `b` holds a row of each item the ORDER BY sorts, and
each membership of the item to another such item holds of `b` and has its
partner's row among `rowsOf` and passing its scope among `bound`: the order
places those partners before the item. -/
theorem Planned.bound_passes {s : Statement} (hwf : s.wellFormed = true)
    {sorted : Limit.SortedLimit} (hs : (plan s).sorted = some sorted) {k : Nat}
    (hk : k ∈ (s.query s.root).arms) {E E' : Bind → Nat → List Row} {b b' : Bind}
    (hle : keyLe ((s.query s.root).orderBy.map (·.2))
      (Limit.orderKey (s.query s.root) ((s.core k).outRow E b))
      (Limit.orderKey (s.query s.root) ((s.core k).outRow E' b')) = true)
    {bound : Nat → RScope} {rowsOf : Nat → List Row} {row : Nat → Row}
    (hrows : ∀ run ∈ sorted.runs, b run.item = some (row run.item))
    {Z : Db} {f : Nat} {i : Nat}
    (hmembers : ∀ m ∈ ((plan s).state.items i).memberships, (∃ run ∈ sorted.runs, run.item = m.partner) →
      m.HoldsIn s Z f b ∧ row m.partner ∈ rowsOf m.partner ∧
        (bound m.partner).passes rowsOf (row m.partner) = true)
    {found : RScope}
    (h : Limit.runBound i ((plan s).lookupOf bound i) (sorted.runs.zip (sorted.runs.map fun run =>
      run.terms.map fun (_, _, p) => ((s.core k).outRow E' b').cell p)) = some found) :
    found.passes rowsOf (row i) = true := by
  have hs' := hs
  rw [plan_sorted] at hs'
  obtain ⟨k0, ts, harms, _, _, hruns, hlen, hts⟩ := Limit.sortedLimit_facts hs'
  rw [harms, List.mem_singleton] at hk
  subst hk
  -- Each term's item is an item of the statement's own core, which sits in no core.
  have hcore : ∀ run ∈ sorted.runs, (s.item run.item).core = k := by
    intro run hrun
    rw [hruns] at hrun
    obtain ⟨t, ht, he⟩ := Limit.group_item hrun
    obtain ⟨⟨pd, t'⟩, hx, rfl⟩ : ∃ x ∈ (s.query s.root).orderBy.zip ts, x.2 = t := by
      obtain ⟨n, hn, rfl⟩ := List.getElem_of_mem ht
      exact ⟨((s.query s.root).orderBy[n]'(by omega), ts[n]), List.mem_iff_getElem.mpr
        ⟨n, by simp only [List.length_zip]; omega, by simp⟩, rfl⟩
    obtain ⟨_, _, hc, _⟩ := hts _ hx
    rw [← he]
    exact hc
  have hparent : (s.core k).parent = none :=
    (s.wellFormed_parts hwf).rootOutside k (by rw [harms]; simp)
  have hzip : ∀ {α β : Type} (l : List α) (g : α → β), l.zip (l.map g) = l.map fun x => (x, g x) := by
    intro α β l g
    induction l with
    | nil => rfl
    | cons x l ih => simp [ih]
  rw [hzip] at h
  obtain ⟨⟨run_i, _⟩, hpi, hii⟩ := Limit.runBound_item h
  obtain ⟨run_i', hrun_i, hpair⟩ := List.mem_map.mp hpi
  simp only [Prod.mk.injEq] at hpair
  obtain ⟨rfl, _⟩ := hpair
  apply Limit.runBound_passes rowsOf row i _ _ _ _ _ h
  · -- Each lookup is a membership from `i` to a run's item, which holds of `b`.
    intro p hp look hlook scope hscope
    obtain ⟨run_j, hrun_j, rfl⟩ := List.mem_map.mp hp
    simp only [Planned.lookupOf, Option.map_eq_some_iff] at hlook
    obtain ⟨m, hfind, rfl⟩ := hlook
    have hm := List.mem_of_find?_eq_some hfind
    have hpart : m.partner = run_j.item := by simpa using List.find?_some hfind
    obtain ⟨hmember, hnestOK⟩ := (plan_nestedOK s).2.1 i m hm
    have hnested : m.nested = false := by
      cases hn : m.nested with
      | false => rfl
      | true =>
        obtain ⟨k', q, hq, harm⟩ := hnestOK hn
        have hk' : k' < s.cores.length := by
          refine Decidable.byContradiction fun hk' => ?_
          rw [s.core_out hk'] at hq
          cases hq
        have := (s.wellFormed_parts hwf).subqueryParents k' hk' q hq _ harm
        rw [hpart, hcore run_j hrun_j, hparent] at this
        cases this
    obtain ⟨hholds, hrj, hboundj⟩ := hmembers m hm ⟨run_j, hrun_j, hpart.symm⟩
    have hbi := hrows run_i' hrun_i
    have hbj := hrows run_j hrun_j
    rw [hii] at hbi
    rw [← hpart] at hbj
    have hscope' : scope.passes rowsOf (row m.partner) = true := by rw [hpart]; exact hscope
    exact Limit.joined_passes hnested hholds (by rw [hmember]; exact hbi) hbj rowsOf hrj
      (by rw [Limit.passes_both, hboundj, hscope']; rfl)
  · intro p hp
    obtain ⟨run, _, rfl⟩ := List.mem_map.mp hp
    simp
  · -- The runs' values are the ORDER BY's values.
    have hfst : (sorted.runs.map fun x => (x, x.terms.map fun (_, _, p) =>
        ((s.core k).outRow E' b').cell p)).map (·.1) = sorted.runs := by simp [Function.comp_def]
    rw [hfst, List.flatMap_map]
    simp only
    rw [hruns]
    have hdirs : Limit.runDirections (Limit.SortRun.group ts) = (s.query s.root).orderBy.map (·.2) := by
      unfold Limit.runDirections Limit.SortRun.keys
      simp only [List.map_map, Function.comp_def]
      rw [Limit.group_flatMap (fun _ t => t.2.1)]
      exact (map_eq_of_zip hlen fun x hx => (hts x hx).choose_spec.2.2.2.2.1.symm).symm
    have hkey : Limit.runKey row (Limit.SortRun.group ts) =
        Limit.orderKey (s.query s.root) ((s.core k).outRow E b) := by
      unfold Limit.runKey Limit.SortRun.keys
      simp only [List.map_map, Function.comp_def]
      rw [Limit.group_flatMap (fun j t => t.1.value (row j))]
      unfold Limit.orderKey
      refine (map_eq_of_zip hlen fun x hx => ?_).symm
      obtain ⟨e, he, _, _, hsk, _, _⟩ := hts x hx
      obtain ⟨⟨p, d⟩, t⟩ := x
      simp only at he hsk ⊢
      obtain ⟨run, hrun, hitem⟩ := Limit.term_item (List.of_mem_zip hx).2
      rw [← hruns] at hrun
      have hb := hrows run hrun
      rw [hitem] at hb
      rw [Core.outRow_cell he, Limit.sortKey_value hsk hb]
    have hlast : (Limit.SortRun.group ts).flatMap (fun run => run.terms.map fun (_, _, p) =>
        ((s.core k).outRow E' b').cell p) = Limit.orderKey (s.query s.root) ((s.core k).outRow E' b') := by
      rw [Limit.group_flatMap (fun _ t => ((s.core k).outRow E' b').cell t.2.2)]
      unfold Limit.orderKey
      refine (map_eq_of_zip hlen fun x hx => ?_).symm
      obtain ⟨e, he, _, _, _, _, hpos⟩ := hts x hx
      obtain ⟨⟨p, d⟩, t⟩ := x
      simp only at he hpos ⊢
      rw [Core.outRow_cell he, Core.outRow_cell hpos]
    rw [hdirs, hkey, hlast]
    exact hle

end SqliveNarrowing
