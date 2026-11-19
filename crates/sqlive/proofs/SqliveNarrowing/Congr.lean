import SqliveNarrowing.Restrict
import SqliveNarrowing.PlannerState

/-! A query's rows depend, of the binding it runs under, only on the rows of
the items it names outside its cores (`Statement.outerNames`):
`evalQuery_outer_congr`. A run binds a core's own items as it joins them, so
whatever the binding around it holds for them changes nothing, and a core
names, outside its own items, only items its query names outside its cores,
as do its derived items' queries and, beside its own items, its subqueries.
So a LEFT JOIN's ON, which names only its own item, items before it and
items outside the core, holds of a binding a core keeps as it held when the
join added the item. -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-! The items each part of a query names outside it. -/

/-- A core of query `q` names its own items and items `q` names outside its
cores. -/
theorem Statement.refs_outer {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {j col : Nat} (hp : (j, col) ∈ (s.core k).refs) :
    j ∈ (s.core k).items ∨ j ∈ s.outerNames q := by
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  obtain ⟨e, he, hje⟩ := (s.wellFormed_parts h).refs k hklt _ hp
  have hcore := ((s.wellFormed_parts h).items e (s.core_lt hje) _ hje).2
  have henc := s.encloses_of_scopeChain he
  cases henc with
  | refl => exact Or.inl hje
  | @parent _ p hp' hbp =>
    obtain ⟨qe, hlt, he'⟩ := s.encloses_after h hbp hk hp'
    exact Or.inr (s.mem_outerNames (s.arm_mem_queryCores hk) (List.mem_map.mpr ⟨(j, col), hp, rfl⟩)
      (by rw [hcore]; exact s.not_mem_queryCores h he' hlt))

/-- A derived item's query names outside its cores only items the query of
the item's core names outside its cores. -/
theorem Statement.outerNames_derived {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {i : Nat} (hi : i ∈ (s.core k).items) {q' : Nat} {v : Bool}
    (hkind : (s.item i).kind = .derived q' v) {j : Nat} (hj : j ∈ s.outerNames q') :
    j ∈ s.outerNames q := by
  obtain ⟨c, hc, hjc, e, _, hcore, a, ha, p, hap, hep⟩ := s.outerName_encloses h hj
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  have hq' : q' ∈ s.nestedQueries k :=
    List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩)
  rcases (s.wellFormed_parts h).derivedParents k hklt i hi q' v hkind a ha with hn | hpk
  · rw [hn] at hap
    cases hap
  · rw [hap] at hpk
    obtain ⟨qe, hlt, he⟩ := s.encloses_after h hep hk hpk.symm
    exact s.mem_outerNames (s.queryCores_nested h hk hq' hc) hjc
      (by rw [hcore]; exact s.not_mem_queryCores h he hlt)

/-- A subquery of a core names outside its cores the core's items and items
the core's query names outside its cores. -/
theorem Statement.outerNames_sub {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {q' : Nat} (hq' : q' ∈ (s.core k).subqueries) {j : Nat}
    (hj : j ∈ s.outerNames q') : j ∈ (s.core k).items ∨ j ∈ s.outerNames q := by
  obtain ⟨c, hc, hjc, e, hje, hcore, a, ha, p, hap, hep⟩ := s.outerName_encloses h hj
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  rw [(s.wellFormed_parts h).subqueryParents k hklt q' hq' a ha] at hap
  obtain rfl := Option.some.inj hap
  cases hep with
  | refl => exact Or.inl hje
  | @parent _ p2 hkp hep2 =>
    obtain ⟨qe, hlt, he⟩ := s.encloses_after h hep2 hk hkp
    have hnested : q' ∈ s.nestedQueries k := List.mem_append_right _ hq'
    exact Or.inr (s.mem_outerNames (s.queryCores_nested h hk hnested hc) hjc
      (by rw [hcore]; exact s.not_mem_queryCores h he hlt))

/-- An item a query names outside its cores is no item of one of its arms. -/
theorem Statement.outer_not_item {s : Statement} (h : s.wellFormed = true) {q k : Nat}
    (hk : k ∈ (s.query q).arms) {j : Nat} (hj : j ∈ s.outerNames q) : j ∉ (s.core k).items := by
  intro hjk
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  have hcore := ((s.wellFormed_parts h).items k hklt j hjk).2
  simp only [Statement.outerNames, List.mem_filter, Bool.not_eq_true'] at hj
  have h2 := hj.2
  rw [hcore, show (s.queryCores (s.queries.length + 1) q).contains k = true from
    List.contains_iff_mem.mpr (s.arm_mem_queryCores hk)] at h2
  cases h2

/-- A LEFT JOIN's ON names only the item the join adds, items before it and
items outside the core. -/
theorem Statement.wellFormed_onNamesEarlier {s : Statement} (h : s.wellFormed = true) {k : Nat}
    (hk : k < s.cores.length) {cd : Cond} {i : Nat} (hcd : (cd, Place.leftOn i) ∈ (s.core k).conds)
    {j : Nat} (hj : j ∈ cd.items) :
    j = i ∨ j ∈ (s.core k).items.takeWhile (· ≠ i) ∨ j ∉ (s.core k).items := by
  simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range] at h
  obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨_, _⟩, _⟩, _⟩, hons⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩ := h
  have := hons k hk
  simp only [Statement.onsNameEarlier, List.all_eq_true] at this
  have hc := this _ hcd
  simp only [Bool.and_eq_true, List.all_eq_true, Bool.or_eq_true, beq_iff_eq, List.contains_iff_mem,
    Bool.not_eq_true'] at hc
  have he : (fun x : Nat => x != i) = (fun x => decide (x ≠ i)) := by
    funext x
    simp only [bne, ne_eq, decide_not]
    rfl
  rcases hc.2 j hj with (h1 | h1) | h1
  · exact Or.inl h1
  · exact Or.inr (Or.inl (by simpa [he] using h1))
  · exact Or.inr (Or.inr (by simpa using h1))

/-- Of a list without repeats that splits at `i`, the elements before `i`
are those `takeWhile` takes. -/
theorem takeWhile_split {pre rest : List Nat} {i : Nat} (hnd : (pre ++ i :: rest).Nodup) :
    (pre ++ i :: rest).takeWhile (· ≠ i) = pre := by
  induction pre with
  | nil => simp
  | cons a pre ih =>
    have ha : a ≠ i := fun e => by subst e; simp at hnd
    simp only [List.cons_append, List.nodup_cons] at hnd
    have := ih hnd.2
    simp only [ne_eq, decide_not] at this
    simp [ha, this]

theorem all_congr_mem {α : Type} {l : List α} {f g : α → Bool} (h : ∀ a ∈ l, f a = g a) :
    l.all f = l.all g := by
  induction l with
  | nil => rfl
  | cons a l ih =>
    simp only [List.all_cons]
    rw [h a List.mem_cons_self, ih (fun b hb => h b (List.mem_cons_of_mem a hb))]

/-! Joins that differ only in what their bindings hold before an item joins. -/

/-- A binding's rows of the items of `items` outside `rest`, and no row of
any other item. -/
def joinedPart (items rest : List Nat) (x : Bind) : Bind :=
  fun j => if j ∈ items ∧ j ∉ rest then x j else none

/-- Two joins of the items `is` whose bindings hold the same rows of the
items of `items` before `is` keep bindings that hold the same rows of
`items`, when each item's rows are the same and its ON holds of both or
neither binding that hold the same rows of the items before it. -/
theorem joinItems_agree {s : Statement} {c : Core} {rowsOf rowsOf' : Nat → List Row}
    {subs subs' : Bind → Subs} {V V' : Bind → Prop} {items : List Nat} :
    ∀ (is : List Nat), (∀ i ∈ is, i ∈ items) → is.Nodup →
      (∀ x i r, i ∈ is → V x → V (x.set i r)) → (∀ x i r, i ∈ is → V' x → V' (x.set i r)) →
      (∀ i ∈ is, rowsOf i = rowsOf' i) →
      (∀ i rest, i :: rest <:+ is → ∀ y y' r, V y → V' y' → (∀ j ∈ items, j ∉ i :: rest → y j = y' j) →
        onHolds c subs i (y.set i (some r)) = onHolds c subs' i (y'.set i (some r))) →
      ∀ (L L' : List Bind), (∀ y ∈ L, V y) → (∀ y ∈ L', V' y) →
        L.map (joinedPart items is) = L'.map (joinedPart items is) →
        (joinItems s c rowsOf subs is L).map (joinedPart items []) =
          (joinItems s c rowsOf' subs' is L').map (joinedPart items [])
  | [], _, _, _, _, _, _, L, L', _, _, h => by simpa [joinItems] using h
  | i :: rest, hin, hnd, hV, hV', hrows, hon, L, L', hL, hL', heq => by
    rw [View.joinItems_cons, View.joinItems_cons]
    have hi : i ∈ items := hin i List.mem_cons_self
    have hir : i ∉ rest := (List.nodup_cons.mp hnd).1
    have hstep : ∀ y y', V y → V' y' →
        joinedPart items (i :: rest) y = joinedPart items (i :: rest) y' →
        (View.joinStep s c rowsOf subs i y).map (joinedPart items rest) =
          (View.joinStep s c rowsOf' subs' i y').map (joinedPart items rest) := by
      intro y y' hy hy' hyy
      have hagree : ∀ j ∈ items, j ∉ i :: rest → y j = y' j := by
        intro j hj hjn
        have := congrFun hyy j
        simpa [joinedPart, hj, hjn] using this
      have hset : ∀ x, joinedPart items rest (y.set i x) = joinedPart items rest (y'.set i x) := by
        intro x
        funext j
        by_cases hji : j = i
        · subst hji; simp [joinedPart, Bind.set]
        · simp only [joinedPart, Bind.set, hji, ↓reduceIte]
          split
          · rename_i hj
            exact hagree j hj.1 (by simp [hji, hj.2])
          · rfl
      have hon' : ∀ r, onHolds c subs i (y.set i (some r)) = onHolds c subs' i (y'.set i (some r)) :=
        fun r => hon i rest List.suffix_rfl y y' r hy hy' hagree
      unfold View.joinStep
      simp only
      rw [hrows i List.mem_cons_self]
      have hj : ((rowsOf' i).filterMap fun r =>
            if (c.onOf i).all (fun cd => isTrue (cd.eval (y.set i (some r)) (subs (y.set i (some r)))))
            then some (y.set i (some r)) else none).map (joinedPart items rest) =
          ((rowsOf' i).filterMap fun r =>
            if (c.onOf i).all (fun cd => isTrue (cd.eval (y'.set i (some r)) (subs' (y'.set i (some r)))))
            then some (y'.set i (some r)) else none).map (joinedPart items rest) := by
        rw [List.map_filterMap, List.map_filterMap]
        congr 1
        funext r
        have := hon' r
        unfold onHolds at this
        rw [this]
        split <;> simp [hset]
      have hempty : ∀ l : List Bind, (l.map (joinedPart items rest)).isEmpty = l.isEmpty := by
        intro l; cases l <;> rfl
      have he := congrArg List.isEmpty hj
      rw [hempty, hempty] at he
      rw [he]
      split
      · simp [hset]
      · exact hj
    refine joinItems_agree rest (fun j hj => hin j (List.mem_cons_of_mem i hj)) (List.nodup_cons.mp hnd).2
      (fun x j r hj hx => hV x j r (List.mem_cons_of_mem i hj) hx)
      (fun x j r hj hx => hV' x j r (List.mem_cons_of_mem i hj) hx)
      (fun j hj => hrows j (List.mem_cons_of_mem i hj))
      (fun j rest' hs => hon j rest' (hs.trans (List.suffix_cons i rest)))
      _ _ ?_ ?_ ?_
    · intro y hy
      obtain ⟨b, hb, hy⟩ := List.mem_flatMap.mp hy
      obtain ⟨x, rfl⟩ := mem_joinStep hy
      exact hV b i x List.mem_cons_self (hL b hb)
    · intro y hy
      obtain ⟨b, hb, hy⟩ := List.mem_flatMap.mp hy
      obtain ⟨x, rfl⟩ := mem_joinStep hy
      exact hV' b i x List.mem_cons_self (hL' b hb)
    · clear hon
      induction L generalizing L' with
      | nil =>
        cases L' with
        | nil => rfl
        | cons _ _ => simp at heq
      | cons y L ih =>
        cases L' with
        | nil => simp at heq
        | cons y' L' =>
          simp only [List.map_cons, List.cons.injEq] at heq
          simp only [List.flatMap_cons, List.map_append]
          rw [hstep y y' (hL y List.mem_cons_self) (hL' y' List.mem_cons_self) heq.1,
            ih L' (fun z hz => hL z (List.mem_cons_of_mem y hz))
              (fun z hz => hL' z (List.mem_cons_of_mem y' hz)) heq.2]

/-! The rows a query returns under two bindings that agree on its outer
items. -/

/-- A core of query `q` keeps, under two bindings that hold the same rows of
the items `q` names outside its cores, bindings that differ only outside its
items, where each holds the binding it ran under, and makes the same rows of
them, when its derived items' queries and its subqueries return the same rows
under two bindings that agree on what they name outside their cores. -/
theorem Statement.core_outer_congr {s : Statement} (h : s.wellFormed = true) {Z : Db}
    {E : Bind → Nat → List Row}
    (hE : ∀ q' b b', (∀ j ∈ s.outerNames q', b j = b' j) → E b q' = E b' q')
    {q k : Nat} (hk : k ∈ (s.query q).arms) {b b' : Bind} (hagree : ∀ j ∈ s.outerNames q, b j = b' j) :
    (s.core k).result E (coreBindings s Z E b k) = (s.core k).result E (coreBindings s Z E b' k) := by
  let c := s.core k
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  have hnd := (s.wellFormed_parts h).nodup k hklt
  cases hpar : c.parent with
  | none => simp [coreBindings, c, hpar] at *
  | some p =>
    have hb : ∀ x : Bind, (if (s.core k).parent.isNone then Bind.empty else x) = x := by
      intro x; simp [c] at hpar; simp [hpar]
    -- What the core's clauses name outside its items, both bindings hold alike.
    have hrefs : ∀ j col, (j, col) ∈ c.refs → j ∉ c.items → b j = b' j := by
      intro j col hj hjn
      rcases s.refs_outer h hk hj with hj' | hj'
      · exact absurd hj' hjn
      · exact hagree j hj'
    let V : Bind → Prop := fun y => ∀ j ∉ c.items, y j = b j
    let V' : Bind → Prop := fun y => ∀ j ∉ c.items, y j = b' j
    have hVset : ∀ (W : Bind → Prop), (W = V ∨ W = V') → ∀ x i r, i ∈ c.items → W x → W (x.set i r) := by
      intro W hW x i r hi hx
      rcases hW with rfl | rfl <;>
      · intro j hj
        have hji : j ≠ i := fun e => hj (e ▸ hi)
        simp only [Bind.set, hji, ↓reduceIte]
        exact hx j hj
    -- Two bindings that hold the same rows of the core's items that a
    -- subquery names, each the binding it ran under elsewhere, give the
    -- subquery the same rows.
    have hsubs : ∀ y y', V y → V' y' → ∀ q' ∈ c.subqueries,
        (∀ j ∈ c.items, j ∈ s.outerNames q' → y j = y' j) → E y q' = E y' q' := by
      intro y y' hy hy' q' hq' hS
      apply hE
      intro j hj
      rcases s.outerNames_sub h hk hq' hj with hjk | hjq
      · exact hS j hjk hj
      · have hjn : j ∉ c.items := s.outer_not_item h hk hjq
        rw [hy j hjn, hy' j hjn]
        exact hagree j hjq
    have hcond : ∀ cd p, (cd, p) ∈ c.conds → ∀ y y', V y → V' y' → (∀ j ∈ c.items, y j = y' j) →
        cd.eval y (E y) = cd.eval y' (E y') := by
      intro cd p hcd y y' hy hy' hyy
      apply Cond.eval_congr
      · intro j col hj
        by_cases hjk : j ∈ c.items
        · rw [hyy j hjk]
        · rw [hy j hjk, hy' j hjk, hrefs j col (by
            simp only [Core.refs, List.mem_append, List.mem_flatMap]
            exact Or.inl (Or.inl ⟨(cd, p), hcd, hj⟩)) hjk]
      · intro q' hq'
        exact hsubs y y' hy hy' q' (Core.cond_queries hcd hq') (fun j hj _ => hyy j hj)
    -- Each item's ON holds of both or neither of two bindings that hold the
    -- same rows of the items up to it.
    have hon : ∀ i rest, i :: rest <:+ c.items → ∀ y y' r, V y → V' y' →
        (∀ j ∈ c.items, j ∉ i :: rest → y j = y' j) →
        onHolds c E i (y.set i (some r)) = onHolds c E i (y'.set i (some r)) := by
      intro i rest hsuf y y' r hy hy' hagr
      obtain ⟨pre, hpre⟩ := hsuf
      have hndi : (pre ++ i :: rest).Nodup := hpre ▸ hnd
      have hi : i ∈ c.items := by rw [← hpre]; simp
      have htw : c.items.takeWhile (· ≠ i) = pre := by rw [← hpre]; exact takeWhile_split hndi
      have hpre_out : ∀ j ∈ pre, j ∉ i :: rest := fun j hj hjr =>
        (List.nodup_append.mp hndi).2.2 j hj j hjr rfl
      have hpre_in : ∀ j ∈ pre, j ∈ c.items := fun j hj => by rw [← hpre]; exact List.mem_append_left _ hj
      have hset : ∀ j, (j = i ∨ j ∈ pre) → (y.set i (some r)) j = (y'.set i (some r)) j := by
        intro j hj
        by_cases hji : j = i
        · subst hji; simp [Bind.set]
        · have hjp : j ∈ pre := hj.resolve_left hji
          simp only [Bind.set, hji, ↓reduceIte]
          exact hagr j (hpre_in j hjp) (hpre_out j hjp)
      have hVs := hVset V (Or.inl rfl) y i (some r) hi hy
      have hV's := hVset V' (Or.inr rfl) y' i (some r) hi hy'
      have hcd : ∀ cd ∈ c.onOf i, cd.eval (y.set i (some r)) (E (y.set i (some r))) =
          cd.eval (y'.set i (some r)) (E (y'.set i (some r))) := by
        intro cd hcdi
        have hcd := Core.mem_onOf hcdi
        apply Cond.eval_congr
        · intro j col hj
          have hjc : j ∈ cd.items := List.mem_map.mpr ⟨(j, col), hj, rfl⟩
          rcases s.wellFormed_onNamesEarlier h hklt hcd hjc with rfl | hjp | hjk
          · rw [hset _ (Or.inl rfl)]
          · rw [htw] at hjp
            rw [hset j (Or.inr hjp)]
          · rw [hVs j hjk, hV's j hjk, hrefs j col (by
              simp only [Core.refs, List.mem_append, List.mem_flatMap]
              exact Or.inl (Or.inl ⟨(cd, Place.leftOn i), hcd, hj⟩)) hjk]
        · intro q' hq'
          refine hsubs _ _ hVs hV's q' (Core.onOf_queries hcdi hq') (fun j hj hjn => ?_)
          have hcore := ((s.wellFormed_parts h).items k hklt j hj).2
          have := (s.wellFormed_parts h).onNames k hklt cd i hcd q' hq' j hjn hcore
          rw [htw] at this
          rcases List.mem_append.mp this with hjp | hji
          · exact hset j (Or.inr hjp)
          · exact hset j (Or.inl (List.mem_singleton.mp hji))
      unfold onHolds
      rw [Bool.eq_iff_iff]
      simp only [List.all_eq_true]
      exact ⟨fun hh cd hc => by rw [← hcd cd hc]; exact hh cd hc,
        fun hh cd hc => by rw [hcd cd hc]; exact hh cd hc⟩
    -- The two joins keep bindings that hold the same rows of the core's items.
    let rowsOf (o : Bind) (i : Nat) : List Row :=
      match (s.item i).kind with
      | .table t => Z.rows t
      | .derived q' _ => E o q'
    have hrows : ∀ i ∈ c.items, rowsOf b i = rowsOf b' i := by
      intro i hi
      simp only [rowsOf]
      cases hkind : (s.item i).kind with
      | table t => rfl
      | derived q' v =>
        exact hE q' b b' (fun j hj => hagree j (s.outerNames_derived h hk hi hkind hj))
    have hJ := joinItems_agree (s := s) (c := c) (rowsOf := rowsOf b) (rowsOf' := rowsOf b')
      (subs := E) (subs' := E) (V := V) (V' := V') (items := c.items) c.items (fun _ h => h) hnd
      (fun x i r hi hx => hVset V (Or.inl rfl) x i r hi hx)
      (fun x i r hi hx => hVset V' (Or.inr rfl) x i r hi hx) hrows hon [b] [b']
      (fun y hy => by simp only [List.mem_singleton] at hy; subst hy; exact fun _ _ => rfl)
      (fun y hy => by simp only [List.mem_singleton] at hy; subst hy; exact fun _ _ => rfl)
      (by
        simp only [List.map_cons, List.map_nil, List.cons.injEq, and_true]
        funext j
        unfold joinedPart
        by_cases hj : j ∈ c.items <;> simp [hj])
    let N : Bind → Bind := fun x j => if j ∈ c.items then x j else b j
    have hJV : ∀ y ∈ joinItems s c (rowsOf b) E c.items [b], V y :=
      joinItems_preserves c.items [b]
        (fun y hy => by simp only [List.mem_singleton] at hy; subst hy; exact fun _ _ => rfl)
        (fun x i r hx hi => hVset V (Or.inl rfl) x i r hi hx)
    have hJV' : ∀ y ∈ joinItems s c (rowsOf b') E c.items [b'], V' y :=
      joinItems_preserves c.items [b']
        (fun y hy => by simp only [List.mem_singleton] at hy; subst hy; exact fun _ _ => rfl)
        (fun x i r hx hi => hVset V' (Or.inr rfl) x i r hi hx)
    have hNjp : ∀ x, N (joinedPart c.items [] x) = N x := by
      intro x; funext j; by_cases hj : j ∈ c.items <;> simp [N, joinedPart, hj]
    have hNV : ∀ y, V y → N y = y := by
      intro y hy; funext j; by_cases hj : j ∈ c.items <;> simp [N, hj, hy j]
    have hJN : joinItems s c (rowsOf b) E c.items [b] = (joinItems s c (rowsOf b') E c.items [b']).map N := by
      have h1 : joinItems s c (rowsOf b) E c.items [b] =
          ((joinItems s c (rowsOf b) E c.items [b]).map (joinedPart c.items [])).map N := by
        rw [List.map_map]
        conv => lhs; rw [← List.map_id (joinItems s c (rowsOf b) E c.items [b])]
        exact List.map_congr_left fun y hy => by simp [hNjp, hNV y (hJV y hy)]
      rw [h1, hJ, List.map_map]
      exact List.map_congr_left fun y _ => by simp [hNjp]
    have hVN : ∀ x, V (N x) := fun x j hj => by simp [N, hj]
    have hNagree : ∀ x, ∀ j ∈ c.items, N x j = x j := fun x j hj => by simp [N, hj]
    -- The WHERE keeps the same bindings, and the core makes the same rows.
    have hbind : coreBindings s Z E b k = (coreBindings s Z E b' k).map N := by
      unfold coreBindings
      simp only [hb]
      change (joinItems s c (rowsOf b) E c.items [b]).filter _ =
        ((joinItems s c (rowsOf b') E c.items [b']).filter _).map N
      rw [hJN, List.filter_map]
      congr 1
      apply List.filter_congr
      intro y hy
      show (c.innerConds.all fun cd => isTrue (cd.eval (N y) (E (N y)))) =
        c.innerConds.all fun cd => isTrue (cd.eval y (E y))
      apply all_congr_mem
      intro cd hcd
      simp only [Core.innerConds, List.mem_filterMap] at hcd
      obtain ⟨⟨cd', p'⟩, hmem, hp'⟩ := hcd
      split at hp'
      · obtain rfl := Option.some.inj hp'
        rw [hcond cd' p' hmem (N y) y (hVN y) (hJV' y hy) (hNagree y)]
      · cases hp'
    let R : Bind → Bind := fun x => if V' x then N x else x
    have hR : ∀ L : List Bind, (∀ x ∈ L, V' x) → L.map R = L.map N := fun L hL =>
      List.map_congr_left fun x hx => by simp [R, hL x hx]
    have hkept' : ∀ x ∈ coreBindings s Z E b' k, V' x := by
      intro x hx
      unfold coreBindings at hx
      simp only [hb] at hx
      exact hJV' x (List.mem_filter.mp hx).1
    have hrowsR : c.rows E ((coreBindings s Z E b' k).map R) = c.rows E (coreBindings s Z E b' k) := by
      apply View.rows_restrict
      intro e he x
      simp only [R]
      split
      · rename_i hx
        apply e.eval_congr
        · intro j col hj
          by_cases hjk : j ∈ c.items
          · rw [hNagree x j hjk]
          · simp only [N, hjk, ↓reduceIte]
            rw [hx j hjk, hrefs j col ?_ hjk]
            simp only [View.outExprs, List.mem_append, List.mem_flatMap] at he
            simp only [Core.refs, List.mem_append, List.mem_flatMap]
            rcases he with ⟨o, ho, he⟩ | he
            · exact Or.inl (Or.inr ⟨o, ho, by
                simp only [OutCol.refs, List.mem_flatMap]; exact ⟨e, he, hj⟩⟩)
            · exact Or.inr ⟨e, he, hj⟩
        · intro q' hq'
          refine hsubs (N x) x (hVN x) hx q' ?_ (fun j hj _ => hNagree x j hj)
          unfold Core.subqueries
          simp only [View.outExprs, List.mem_append, List.mem_flatMap] at he
          refine List.mem_eraseDups.mpr ?_
          rcases he with ⟨o, ho, he⟩ | he
          · exact List.mem_append_left _ (List.mem_append_left _
              (List.mem_flatMap.mpr ⟨o, ho, List.mem_flatMap.mpr ⟨e, he, hq'⟩⟩))
          · exact List.mem_append_right _ (List.mem_flatMap.mpr ⟨e, he, hq'⟩)
      · rfl
    rw [hbind, ← hR _ hkept']
    unfold Core.result
    rw [hrowsR]

/-- A query returns the same rows under two bindings that hold the same rows
of the items it names outside its cores. -/
theorem evalQuery_outer_congr {s : Statement} (h : s.wellFormed = true) (Z : Db) :
    ∀ (f q : Nat) (b b' : Bind), (∀ j ∈ s.outerNames q, b j = b' j) →
      evalQuery s Z f b q = evalQuery s Z f b' q
  | 0, _, _, _, _ => rfl
  | f + 1, q, b, b', hagree => by
    simp only [evalQuery, queryRows]
    congr 2
    apply List.map_congr_left
    intro k hk
    exact s.core_outer_congr h (evalQuery_outer_congr h Z f) hk hagree

/-! A LEFT JOIN's ON holds of each binding a core keeps that holds a row of
the join's item. -/

/-- A binding a join keeps holds, outside the items it joins, a binding it
started from. -/
theorem joinItems_origin {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs} :
    ∀ (is : List Nat) (L : List Bind), ∀ b ∈ joinItems s c rowsOf subs is L,
      ∃ z ∈ L, ∀ j ∉ is, b j = z j
  | [], L, b, hb => ⟨b, hb, fun _ _ => rfl⟩
  | a :: rest, L, b, hb => by
    rw [View.joinItems_cons] at hb
    obtain ⟨z', hz', hbz⟩ := joinItems_origin rest _ b hb
    obtain ⟨y0, hy0, hz⟩ := List.mem_flatMap.mp hz'
    obtain ⟨x, rfl⟩ := mem_joinStep hz
    refine ⟨y0, hy0, fun j hj => ?_⟩
    have hja : j ≠ a := fun e => hj (e ▸ List.mem_cons_self)
    rw [hbz j (fun h => hj (List.mem_cons_of_mem a h))]
    simp [Bind.set, hja]

/-- A binding one item's step of a join adds with a row of the item is one
the item's ON holds of. -/
theorem joinStep_on {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs}
    {i : Nat} {y z : Bind} (hz : z ∈ View.joinStep s c rowsOf subs i y) {r : Row} (hzi : z i = some r) :
    onHolds c subs i z = true := by
  unfold View.joinStep at hz
  simp only at hz
  split at hz
  · simp only [List.mem_singleton] at hz
    subst hz
    simp [Bind.set] at hzi
  · obtain ⟨r', _, hr'⟩ := List.mem_filterMap.mp hz
    split at hr'
    · rename_i hon
      obtain rfl := Option.some.inj hr'
      exact hon
    · cases hr'

/-- A binding a join keeps that holds a row of an item it joins agrees,
outside the items joined after that item, with a binding the item's ON holds
of. -/
theorem joinItems_onAt {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs} :
    ∀ (is : List Nat) (L : List Bind), is.Nodup → ∀ b ∈ joinItems s c rowsOf subs is L,
      ∀ i ∈ is, ∀ r, b i = some r →
        ∃ pre rest, is = pre ++ i :: rest ∧ ∃ y, onHolds c subs i y = true ∧ ∀ j ∉ rest, y j = b j
  | [], _, _, _, _, i, hi, _, _ => by cases hi
  | a :: rest, L, hnd, b, hb, i, hi, r, hbi => by
    rw [View.joinItems_cons] at hb
    have har : a ∉ rest := (List.nodup_cons.mp hnd).1
    by_cases hia : i = a
    · subst hia
      obtain ⟨z, hz, hbz⟩ := joinItems_origin rest _ b hb
      obtain ⟨y0, _, hzs⟩ := List.mem_flatMap.mp hz
      have hzi : z i = some r := by rw [← hbz i har]; exact hbi
      exact ⟨[], rest, rfl, z, joinStep_on hzs hzi, fun j hj => (hbz j hj).symm⟩
    · have hir : i ∈ rest := (List.mem_cons.mp hi).resolve_left hia
      obtain ⟨pre, rest', heq, hy⟩ := joinItems_onAt rest _ (List.nodup_cons.mp hnd).2 b hb i hir r hbi
      exact ⟨a :: pre, rest', by rw [heq]; rfl, hy⟩

/-- Each ON condition of an item holds of a binding its core keeps that holds
a row of the item: the ON names only the item, items before it and items
outside the core, so the binding holds them as it did when the join added
the item. -/
theorem Statement.on_kept {s : Statement} (h : s.wellFormed = true) (Z : Db) {f q k : Nat}
    (hk : k ∈ (s.query q).arms) {o b : Bind} (hb : b ∈ coreBindings s Z (evalQuery s Z f) o k)
    {i : Nat} (hi : i ∈ (s.core k).items) {r : Row} (hbi : b i = some r) :
    ∀ cd ∈ (s.core k).onOf i, isTrue (cd.eval b (evalQuery s Z f b)) = true := by
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  have hnd := (s.wellFormed_parts h).nodup k hklt
  unfold coreBindings at hb
  obtain ⟨pre, rest, heq, y, hon, hyb⟩ :=
    joinItems_onAt _ _ hnd b (List.mem_filter.mp hb).1 i hi r hbi
  have hndi : (pre ++ i :: rest).Nodup := heq ▸ hnd
  have htw : (s.core k).items.takeWhile (· ≠ i) = pre := by rw [heq]; exact takeWhile_split hndi
  have hout : ∀ j, (j = i ∨ j ∈ pre ∨ j ∉ (s.core k).items) → j ∉ rest := by
    intro j hj hjr
    rcases hj with rfl | hjp | hjk
    · exact (List.nodup_append.mp hndi).2.1 |> fun hn => (List.nodup_cons.mp hn).1 hjr
    · exact (List.nodup_append.mp hndi).2.2 j hjp j (List.mem_cons_of_mem i hjr) rfl
    · exact hjk (by rw [heq]; exact List.mem_append_right _ (List.mem_cons_of_mem i hjr))
  intro cd hcdi
  have hcd := Core.mem_onOf hcdi
  have hy := List.all_eq_true.mp hon cd hcdi
  rw [← hy]
  congr 1
  apply Cond.eval_congr
  · intro j col hj
    have hjc : j ∈ cd.items := List.mem_map.mpr ⟨(j, col), hj, rfl⟩
    rw [hyb j (hout j (by
      rcases s.wellFormed_onNamesEarlier h hklt hcd hjc with e | hjp | hjk
      · exact Or.inl e
      · rw [htw] at hjp; exact Or.inr (Or.inl hjp)
      · exact Or.inr (Or.inr hjk)))]
  · intro q' hq'
    apply evalQuery_outer_congr h Z
    intro j hj
    refine (hyb j (hout j ?_)).symm
    by_cases hcore : (s.item j).core = k
    · have := (s.wellFormed_parts h).onNames k hklt cd i hcd q' hq' j hj hcore
      rw [htw] at this
      rcases List.mem_append.mp this with hjp | hji
      · exact Or.inr (Or.inl hjp)
      · exact Or.inl (List.mem_singleton.mp hji)
    · exact Or.inr (Or.inr fun hjk => hcore ((s.wellFormed_parts h).items k hklt j hjk).2)

end SqliveNarrowing
