import SqliveNarrowing.Filter

/-! The bindings a run of a query reads around it. A query's cores, and the
cores of the queries they read, name items of their own and items of the
cores they sit in (`outerNames`). A run of the query reads the rows of those
outer items from the binding around it, which holds a row of each that no
LEFT JOIN adds (`Statement.Outer`). The statement's own query names no outer
item (`outer_root`), and a core of a query run under such a binding runs its
derived items' queries and its subqueries under such bindings too
(`outer_derived`, `outer_sub`, `outer_subOn`), because a well-formed
statement's cores sit in one another as its queries hold one another. -/
namespace SqliveNarrowing

/-- Core `b` encloses core `a`: it is `a`, or encloses the core `a` sits in. -/
inductive Statement.Encloses (s : Statement) : Nat → Nat → Prop
  | refl (k : Nat) : s.Encloses k k
  | parent {b a p : Nat} : (s.core a).parent = some p → s.Encloses b p → s.Encloses b a

theorem Statement.encloses_of_go {s : Statement} :
    ∀ (f k c : Nat), c ∈ Statement.scopeChain.go s f k → s.Encloses c k
  | 0, k, c, h => by
    simp only [Statement.scopeChain.go, List.mem_singleton] at h
    subst h
    exact .refl _
  | f + 1, k, c, h => by
    simp only [Statement.scopeChain.go] at h
    split at h
    · rename_i p hp
      rcases List.mem_cons.mp h with rfl | h
      · exact .refl _
      · exact .parent hp (Statement.encloses_of_go f p c h)
    · simp only [List.mem_singleton] at h
      subst h
      exact .refl _

/-- A core of the chain a core sits in encloses it. -/
theorem Statement.encloses_of_scopeChain {s : Statement} {c k : Nat} (h : c ∈ s.scopeChain k) :
    s.Encloses c k :=
  Statement.encloses_of_go _ k c h

/-- Whether binding `b` holds a row of each of `S`'s items that no LEFT JOIN
adds, as a join does once it has joined them. -/
def Statement.BoundIn (s : Statement) (S : List Nat) (b : Bind) : Prop :=
  ∀ j ∈ S, s.alwaysBound j = true → b j ≠ none

/-- The binding a core's items join onto under the binding `o` around it:
none for a core in no other (`coreBindings`). -/
def Statement.outerOf (s : Statement) (k : Nat) (o : Bind) : Bind :=
  if (s.core k).parent.isNone then Bind.empty else o

/-- Whether binding `b` holds, outside core `k`'s items, the binding that
core `k`'s items join onto under `o`: as each binding a join of the core
builds does. -/
def Statement.Within (s : Statement) (k : Nat) (o b : Bind) : Prop :=
  ∀ j, j ∉ (s.core k).items → b j = s.outerOf k o j

/-- A binding a run of query `q` reads around it: it holds a row of each item
outside `q` that `q` names and that no LEFT JOIN adds. -/
def Statement.Outer (s : Statement) (q : Nat) (o : Bind) : Prop :=
  ∀ j ∈ s.outerNames q, s.alwaysBound j = true → o j ≠ none

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

/-- A query with an arm is in range. -/
theorem Statement.query_lt {s : Statement} {q k : Nat} (hk : k ∈ (s.query q).arms) :
    q < s.queries.length := by
  refine Decidable.byContradiction fun hq => ?_
  rw [s.query_out hq] at hk
  cases hk

/-- A core that lists an item is in range. -/
theorem Statement.core_lt {s : Statement} {k i : Nat} (hi : i ∈ (s.core k).items) :
    k < s.cores.length := by
  refine Decidable.byContradiction fun hk => ?_
  rw [s.core_out hk] at hi
  cases hi

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

/-- Two queries of a well-formed statement with a common arm are one query. -/
theorem Statement.wellFormed_armOf {s : Statement} (h : s.wellFormed = true) {q q' k : Nat}
    (hk : k ∈ (s.query q).arms) (hk' : k ∈ (s.query q').arms) : q = q' :=
  Decidable.byContradiction fun hne =>
    disjoint_of_nodup_flatMap (s.wellFormed_parts h).armCores (List.mem_range.mpr (s.query_lt hk))
      (List.mem_range.mpr (s.query_lt hk')) hne hk hk'

/-- No core of a well-formed statement reads the statement's own query. -/
theorem Statement.wellFormed_root {s : Statement} (h : s.wellFormed = true) (k : Nat) :
    s.root ∉ s.nestedQueries k := by
  intro hmem
  by_cases hk : k < s.cores.length
  · have hnd := (s.wellFormed_parts h).readQueries
    simp only [Statement.readQueries, List.nodup_cons, List.mem_flatMap, List.mem_range] at hnd
    exact hnd.1 ⟨k, hk, hmem⟩
  · unfold Statement.nestedQueries at hmem
    rw [s.core_out hk] at hmem
    cases hmem

/-- The queries of a LEFT JOIN's ON are subqueries of its core. -/
theorem Core.onOf_queries {c : Core} {i : Nat} {cd : Cond} {q : Nat} (hcd : cd ∈ c.onOf i)
    (hq : q ∈ cd.queries) : q ∈ c.subqueries := by
  simp only [Core.onOf, List.mem_filterMap] at hcd
  obtain ⟨⟨cd', p⟩, hmem, hp⟩ := hcd
  split at hp
  · obtain rfl := Option.some.inj hp
    unfold Core.subqueries
    exact List.mem_eraseDups.mpr (List.mem_append_left _ (List.mem_append_right _
      (List.mem_flatMap.mpr ⟨(_, p), hmem, hq⟩)))
  · cases hp

/-- An ON condition of item `i` is one of its core's conditions, placed in
`i`'s LEFT JOIN. -/
theorem Core.mem_onOf {c : Core} {i : Nat} {cd : Cond} (hcd : cd ∈ c.onOf i) :
    (cd, Place.leftOn i) ∈ c.conds := by
  simp only [Core.onOf, List.mem_filterMap] at hcd
  obtain ⟨⟨cd', p⟩, hmem, hp⟩ := hcd
  split at hp
  · rename_i hpl
    obtain rfl := Option.some.inj hp
    simp only [beq_iff_eq] at hpl
    rw [← hpl]
    exact hmem
  · cases hp

/-! The cores of a query. -/

theorem Statement.mem_queryCores {s : Statement} {f q c : Nat} :
    c ∈ s.queryCores (f + 1) q ↔ ∃ k ∈ (s.query q).arms,
      c = k ∨ ∃ q' ∈ s.nestedQueries k, c ∈ s.queryCores f q' := by
  simp [Statement.queryCores, List.mem_flatMap]

/-- A core of a query is an arm of it or of a query it reads, which has a
lower number. -/
theorem Statement.queryCores_arm {s : Statement} (h : s.wellFormed = true) :
    ∀ (f q c : Nat), c ∈ s.queryCores f q → ∃ qc ≤ q, c ∈ (s.query qc).arms
  | 0, _, _, hc => by simp [Statement.queryCores] at hc
  | f + 1, q, c, hc => by
    obtain ⟨k, hk, rfl | ⟨q', hq', hc⟩⟩ := Statement.mem_queryCores.mp hc
    · exact ⟨q, Nat.le_refl _, hk⟩
    · obtain ⟨qc, hle, hqc⟩ := Statement.queryCores_arm h f q' c hc
      exact ⟨qc, Nat.le_trans hle (Nat.le_of_lt (s.wellFormed_nested h (s.query_lt hk) hk hq')), hqc⟩

/-- A core of a query is in range. -/
theorem Statement.queryCores_lt {s : Statement} (h : s.wellFormed = true) {f q c : Nat}
    (hc : c ∈ s.queryCores f q) : c < s.cores.length := by
  obtain ⟨qc, _, hqc⟩ := s.queryCores_arm h f q c hc
  exact ((s.wellFormed_parts h).arms qc (s.query_lt hqc) c hqc).1

/-- With fuel past its number, a query's cores are the same at every fuel. -/
theorem Statement.queryCores_fuel {s : Statement} (h : s.wellFormed = true) :
    ∀ q f f', q < f → q < f' → s.queryCores f q = s.queryCores f' q := by
  intro q
  induction q using Nat.strongRecOn with
  | ind q ih =>
    intro f f' hf hf'
    obtain ⟨g, rfl⟩ : ∃ g, f = g + 1 := ⟨f - 1, by omega⟩
    obtain ⟨g', rfl⟩ : ∃ g', f' = g' + 1 := ⟨f' - 1, by omega⟩
    simp only [Statement.queryCores]
    apply flatMap_congr_mem
    intro k hk
    congr 1
    apply flatMap_congr_mem
    intro q' hq'
    have hlt := s.wellFormed_nested h (s.query_lt hk) hk hq'
    exact ih q' hlt g g' (by omega) (by omega)

/-- The cores of a query that an arm of `q` reads are cores of `q`. -/
theorem Statement.queryCores_nested {s : Statement} (h : s.wellFormed = true) {q k q' c : Nat}
    (hk : k ∈ (s.query q).arms) (hq' : q' ∈ s.nestedQueries k)
    (hc : c ∈ s.queryCores (s.queries.length + 1) q') : c ∈ s.queryCores (s.queries.length + 1) q := by
  have hlt := s.wellFormed_nested h (s.query_lt hk) hk hq'
  have hql := s.query_lt hk
  rw [s.queryCores_fuel h q' (s.queries.length + 1) s.queries.length (by omega) (by omega)] at hc
  exact Statement.mem_queryCores.mpr ⟨k, hk, Or.inr ⟨q', hq', hc⟩⟩

/-- An arm of a query is one of its cores. -/
theorem Statement.arm_mem_queryCores {s : Statement} {q k f : Nat} (hk : k ∈ (s.query q).arms) :
    k ∈ s.queryCores (f + 1) q :=
  Statement.mem_queryCores.mpr ⟨k, hk, Or.inl rfl⟩

/-- The core a core of query `q` sits in is a core of `q`, or the core an
arm of `q` sits in. -/
theorem Statement.queryCores_parent {s : Statement} (h : s.wellFormed = true) :
    ∀ (f q c p : Nat), c ∈ s.queryCores f q → (s.core c).parent = some p →
      p ∈ s.queryCores f q ∨ ∃ a ∈ (s.query q).arms, (s.core a).parent = some p
  | 0, _, _, _, hc, _ => by simp [Statement.queryCores] at hc
  | f + 1, q, c, p, hc, hp => by
    obtain ⟨k, hk, rfl | ⟨q', hq', hc⟩⟩ := Statement.mem_queryCores.mp hc
    · exact Or.inr ⟨c, hk, hp⟩
    · have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
      rcases Statement.queryCores_parent h f q' c p hc hp with hin | ⟨a, ha, hap⟩
      · exact Or.inl (Statement.mem_queryCores.mpr ⟨k, hk, Or.inr ⟨q', hq', hin⟩⟩)
      · unfold Statement.nestedQueries at hq'
        rcases List.mem_append.mp hq' with hd | hsub
        · obtain ⟨i, hi, hkind⟩ := List.mem_filterMap.mp hd
          split at hkind
          · rename_i q'' v hkind'
            obtain rfl := Option.some.inj hkind
            rcases (s.wellFormed_parts h).derivedParents k hklt i hi _ v hkind' a ha with hn | hpk
            · rw [hn] at hap
              cases hap
            · exact Or.inr ⟨k, hk, hpk ▸ hap⟩
          · cases hkind
        · have := (s.wellFormed_parts h).subqueryParents k hklt q' hsub a ha
          rw [this] at hap
          obtain rfl := Option.some.inj hap
          exact Or.inl (s.arm_mem_queryCores hk)

/-- A core enclosing a core of query `q` is a core of `q`, or encloses the
core an arm of `q` sits in. -/
theorem Statement.encloses_queryCores {s : Statement} (h : s.wellFormed = true) {e c : Nat}
    (he : s.Encloses e c) :
    ∀ {f q}, c ∈ s.queryCores f q →
      e ∈ s.queryCores f q ∨ ∃ a ∈ (s.query q).arms, ∃ p, (s.core a).parent = some p ∧ s.Encloses e p := by
  induction he with
  | refl => exact fun hc => Or.inl hc
  | @parent a p hp hbp ih =>
    intro f q hc
    rcases s.queryCores_parent h f q a p hc hp with hin | ⟨a', ha', hap⟩
    · exact ih hin
    · exact Or.inr ⟨a', ha', p, hap, hbp⟩

/-- A core enclosing the core an arm of query `q` sits in is an arm of a
query numbered after `q`. -/
theorem Statement.encloses_after {s : Statement} (h : s.wellFormed = true) {e c : Nat}
    (he : s.Encloses e c) :
    ∀ {q k}, k ∈ (s.query q).arms → (s.core k).parent = some c → ∃ qe, q < qe ∧ e ∈ (s.query qe).arms := by
  have after : ∀ {q k c}, k ∈ (s.query q).arms → (s.core k).parent = some c →
      ∃ qc, q < qc ∧ c ∈ (s.query qc).arms := by
    intro q k c hk hp
    have hql := s.query_lt hk
    obtain ⟨qc, _, hc, hlt⟩ := (s.wellFormed_parts h).parentsAfter k
      ((s.wellFormed_parts h).arms q hql k hk).1 c hp
    exact ⟨qc, hlt q hql hk, hc⟩
  induction he with
  | refl => exact fun hk hp => after hk hp
  | @parent a p hp _ ih =>
    intro q k hk hka
    obtain ⟨qa, hlt, ha⟩ := after hk hka
    obtain ⟨qe, hlt', he⟩ := ih ha hp
    exact ⟨qe, Nat.lt_trans hlt hlt', he⟩

/-- An arm of a query numbered after `q` is no core of `q`. -/
theorem Statement.not_mem_queryCores {s : Statement} (h : s.wellFormed = true) {e qe q f : Nat}
    (he : e ∈ (s.query qe).arms) (hlt : q < qe) : e ∉ s.queryCores f q := by
  intro hc
  obtain ⟨qc, hle, hqc⟩ := s.queryCores_arm h f q e hc
  have := s.wellFormed_armOf h he hqc
  omega

/-- An item query `q` names outside its cores is an item of a core that
encloses the core an arm of `q` sits in, named by a core of `q`. -/
theorem Statement.outerName_encloses {s : Statement} (h : s.wellFormed = true) {q j : Nat}
    (hj : j ∈ s.outerNames q) :
    ∃ c ∈ s.queryCores (s.queries.length + 1) q, j ∈ (s.core c).refs.map (·.1) ∧
      ∃ e, j ∈ (s.core e).items ∧ (s.item j).core = e ∧
        ∃ a ∈ (s.query q).arms, ∃ p, (s.core a).parent = some p ∧ s.Encloses e p := by
  simp only [Statement.outerNames, List.mem_filter, List.mem_flatMap, Bool.not_eq_true'] at hj
  obtain ⟨⟨c, hc, hjc⟩, hout⟩ := hj
  refine ⟨c, hc, hjc, ?_⟩
  obtain ⟨⟨j', col⟩, hp, rfl⟩ := List.mem_map.mp hjc
  obtain ⟨e, he, hje⟩ := (s.wellFormed_parts h).refs c (s.queryCores_lt h hc) _ hp
  have hcore := ((s.wellFormed_parts h).items e (s.core_lt hje) _ hje).2
  refine ⟨e, hje, hcore, ?_⟩
  rcases s.encloses_queryCores h (s.encloses_of_scopeChain he) hc with hin | hout'
  · rw [hcore] at hout
    rw [List.contains_iff_mem.mpr hin] at hout
    cases hout
  · exact hout'

/-- An item named by a core of query `q`, of a core that is no core of `q`,
is one `q` names outside its cores. -/
theorem Statement.mem_outerNames {s : Statement} {q c j : Nat}
    (hc : c ∈ s.queryCores (s.queries.length + 1) q) (hj : j ∈ (s.core c).refs.map (·.1))
    (hout : (s.item j).core ∉ s.queryCores (s.queries.length + 1) q) : j ∈ s.outerNames q := by
  simp only [Statement.outerNames, List.mem_filter, List.mem_flatMap, Bool.not_eq_true']
  exact ⟨⟨c, hc, hj⟩, by simpa using hout⟩

/-! The outer bindings of each query a run reads. -/

/-- The statement's own query names no item outside its cores. -/
theorem Statement.outer_root {s : Statement} (h : s.wellFormed = true) : s.Outer s.root Bind.empty := by
  intro j hj
  obtain ⟨_, _, _, _, _, _, a, ha, _, hap, _⟩ := s.outerName_encloses h hj
  rw [(s.wellFormed_parts h).rootOutside a ha] at hap
  cases hap

/-- A derived item's query runs under the binding its core's items join
onto, which holds a row of each outer item the query names that no LEFT JOIN
adds. -/
theorem Statement.outer_derived {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {o : Bind} (ho : s.Outer q o) {i : Nat} (hi : i ∈ (s.core k).items)
    {q' : Nat} {v : Bool} (hkind : (s.item i).kind = .derived q' v) : s.Outer q' (s.outerOf k o) := by
  intro j hj hab
  obtain ⟨c, hc, hjc, e, _, hcore, a, ha, p, hap, hep⟩ := s.outerName_encloses h hj
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  have hq' : q' ∈ s.nestedQueries k :=
    List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩)
  rcases (s.wellFormed_parts h).derivedParents k hklt i hi q' v hkind a ha with hn | hpk
  · rw [hn] at hap
    cases hap
  · rw [hap] at hpk
    obtain ⟨qe, hlt, he⟩ := s.encloses_after h hep hk hpk.symm
    have hout : (s.item j).core ∉ s.queryCores (s.queries.length + 1) q := by
      rw [hcore]
      exact s.not_mem_queryCores h he hlt
    have := ho j (s.mem_outerNames (s.queryCores_nested h hk hq' hc) hjc hout) hab
    simpa [Statement.outerOf, ← hpk] using this

/-- What a subquery of core `k` names outside its cores, of `k`'s items or of
items outside `k`'s query, a binding of `k` that holds `S`'s rows holds,
when `S` holds every item of `k` that the subquery names. -/
theorem Statement.outer_subquery {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {o : Bind} (ho : s.Outer q o) {b : Bind} (hw : s.Within k o b)
    {q' : Nat} (hq' : q' ∈ (s.core k).subqueries) {S : List Nat}
    (hS : ∀ j ∈ s.outerNames q', (s.item j).core = k → j ∈ S) (hb : s.BoundIn S b) :
    s.Outer q' b := by
  intro j hj hab
  obtain ⟨c, hc, hjc, e, hje, hcore, a, ha, p, hap, hep⟩ := s.outerName_encloses h hj
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  rw [(s.wellFormed_parts h).subqueryParents k hklt q' hq' a ha] at hap
  obtain rfl := Option.some.inj hap
  cases hep with
  | refl => exact hb j (hS j hj hcore) hab
  | @parent _ p2 hkp hep2 =>
    obtain ⟨qe, hlt, he⟩ := s.encloses_after h hep2 hk hkp
    have hnot := s.not_mem_queryCores (f := s.queries.length + 1) h he hlt
    have hout : (s.item j).core ∉ s.queryCores (s.queries.length + 1) q := by
      rw [hcore]
      exact s.not_mem_queryCores h he hlt
    have hjk : j ∉ (s.core k).items := by
      intro hjk
      rw [((s.wellFormed_parts h).items k hklt j hjk).2] at hcore
      subst hcore
      exact hnot (s.arm_mem_queryCores hk)
    have hnested : q' ∈ s.nestedQueries k := List.mem_append_right _ hq'
    have := ho j (s.mem_outerNames (s.queryCores_nested h hk hnested hc) hjc hout) hab
    rw [hw j hjk]
    simpa [Statement.outerOf, hkp] using this

/-- A subquery of a core's WHERE or result columns runs under a binding of
the core that holds a row of each of its items that no LEFT JOIN adds. -/
theorem Statement.outer_sub {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {o : Bind} (ho : s.Outer q o) {b : Bind} (hw : s.Within k o b)
    (hb : s.BoundIn (s.core k).items b) {q' : Nat} (hq' : q' ∈ (s.core k).subqueries) :
    s.Outer q' b :=
  s.outer_subquery h hk ho hw hq' (fun j hj hcore => by
    obtain ⟨_, _, _, e, hje, hcore', _⟩ := s.outerName_encloses h hj
    rw [hcore'] at hcore
    subst hcore
    exact hje) hb

/-- A subquery of a LEFT JOIN's ON runs under a binding of the core that
holds a row of each item up to the join's that no LEFT JOIN adds. -/
theorem Statement.outer_subOn {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {o : Bind} (ho : s.Outer q o) {i : Nat} {b : Bind}
    (hw : s.Within k o b) (hb : s.BoundIn ((s.core k).items.takeWhile (· ≠ i) ++ [i]) b)
    {cd : Cond} (hcd : cd ∈ (s.core k).onOf i) {q' : Nat} (hq' : q' ∈ cd.queries) : s.Outer q' b :=
  s.outer_subquery h hk ho hw (Core.onOf_queries hcd hq')
    (fun j hj hcore => (s.wellFormed_parts h).onNames k
      ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1 cd i (Core.mem_onOf hcd) q' hq' j hj hcore) hb

end SqliveNarrowing
