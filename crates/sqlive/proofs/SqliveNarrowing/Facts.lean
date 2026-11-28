import SqliveNarrowing.Congr

/-! What the planner's state says of the bindings a core keeps. A binding a
core keeps, under a binding around it that holds a row of each outer item
its query names that no LEFT JOIN adds (`Statement.KeptIn`), holds of each
of the core's conditions: the WHERE's always, and a LEFT JOIN's ON where it
holds a row of the join's item (`Statement.on_kept`). It holds a row of each
item no LEFT JOIN adds, and of each such item its conditions name. So each
term, edge and OR a condition gives holds of the binding itself
(`Narrowing.Facts`), and an edge's fact carries a term true of its partner's
row to its member's (`PState.Sem.carry`). -/
namespace SqliveNarrowing


/-- Binding `b` is one core `k` keeps, on `Z` with its subqueries read at
fuel `f`, under a binding around it that its query reads. -/
def Statement.KeptIn (s : Statement) (Z : Db) (f k : Nat) (b : Bind) : Prop :=
  ∃ q o, k ∈ (s.query q).arms ∧ s.Outer q o ∧ b ∈ coreBindings s Z (evalQuery s Z f) o k

/-- A binding one item's step of a join adds with no row of the item is one
a LEFT JOIN adds. -/
theorem joinStep_none {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs}
    {i : Nat} {y z : Bind} (hz : z ∈ View.joinStep s c rowsOf subs i y) (hzi : z i = none) :
    (s.item i).left = true := by
  unfold View.joinStep at hz
  simp only at hz
  split at hz
  · rename_i hl
    simp only [Bool.and_eq_true] at hl
    exact hl.1
  · obtain ⟨r', _, hr'⟩ := List.mem_filterMap.mp hz
    split at hr'
    · obtain rfl := Option.some.inj hr'
      simp [Bind.set] at hzi
    · cases hr'

/-- A join holds a row of each item it joins that no LEFT JOIN adds. -/
theorem joinItems_bound {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs} :
    ∀ (is : List Nat) (L : List Bind), is.Nodup → ∀ b ∈ joinItems s c rowsOf subs is L,
      ∀ i ∈ is, s.alwaysBound i = true → b i ≠ none
  | [], _, _, _, _, i, hi, _ => by cases hi
  | a :: rest, L, hnd, b, hb, i, hi, hab => by
    rw [View.joinItems_cons] at hb
    have har : a ∉ rest := (List.nodup_cons.mp hnd).1
    by_cases hia : i = a
    · subst hia
      obtain ⟨z, hz, hbz⟩ := joinItems_origin rest _ b hb
      obtain ⟨y0, _, hzs⟩ := List.mem_flatMap.mp hz
      rw [hbz i har]
      intro hzi
      have := joinStep_none hzs hzi
      simp [Statement.alwaysBound, this] at hab
    · exact joinItems_bound rest _ (List.nodup_cons.mp hnd).2 b hb i
        ((List.mem_cons.mp hi).resolve_left hia) hab

/-- A core with no core around it names only its own items. -/
theorem Statement.refs_parent {s : Statement} (h : s.wellFormed = true) {k : Nat}
    (hk : k < s.cores.length) {j col : Nat} (hp : (j, col) ∈ (s.core k).refs)
    (hjk : j ∉ (s.core k).items) : (s.core k).parent ≠ none := by
  intro hpar
  obtain ⟨e, he, hje⟩ := (s.wellFormed_parts h).refs k hk _ hp
  have hchain : s.scopeChain k = [k] := by
    unfold Statement.scopeChain
    cases s.cores.length with
    | zero => rfl
    | succ n => simp [Statement.scopeChain.go, hpar]
  rw [hchain, List.mem_singleton] at he
  subst he
  exact hjk hje

/-- What a binding a core keeps holds: a row of each item no LEFT JOIN adds
that the core lists or its clauses name, each WHERE condition, and each ON
condition of an item it holds a row of. -/
theorem Statement.KeptIn.facts {s : Statement} (h : s.wellFormed = true) {Z : Db} {f k : Nat}
    {b : Bind} (hb : s.KeptIn Z f k b) :
    s.BoundIn (s.core k).items b ∧
    (∀ j col, (j, col) ∈ (s.core k).refs → s.alwaysBound j = true → b j ≠ none) ∧
    (∀ cd, (cd, Place.inner) ∈ (s.core k).conds → isTrue (cd.eval b (evalQuery s Z f b)) = true) ∧
    (∀ cd l, (cd, Place.leftOn l) ∈ (s.core k).conds → b l ≠ none →
      isTrue (cd.eval b (evalQuery s Z f b)) = true) := by
  obtain ⟨q, o, hk, hout, hkept⟩ := hb
  have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
  have hnd := (s.wellFormed_parts h).nodup k hklt
  have hjoin := (List.mem_filter.mp (show b ∈ _ from hkept)).1
  have hbound : s.BoundIn (s.core k).items b := fun j hj hab =>
    joinItems_bound _ _ hnd b hjoin j hj hab
  refine ⟨hbound, fun j col hj hab => ?_, fun cd hcd => ?_, fun cd l hcd hl => ?_⟩
  · by_cases hjk : j ∈ (s.core k).items
    · exact hbound j hjk hab
    · rcases s.refs_outer h hk hj with hj' | hj'
      · exact absurd hj' hjk
      · obtain ⟨z, hz, hbz⟩ := joinItems_origin _ _ b hjoin
        simp only [List.mem_singleton] at hz
        subst hz
        rw [hbz j hjk]
        have hpar := s.refs_parent h hklt hj hjk
        simp only [Option.ne_none_iff_isSome] at hpar
        simp only [Option.isSome_iff_ne_none] at hpar
        have : ((s.core k).parent.isNone) = false := by
          cases hp : (s.core k).parent with
          | none => exact absurd hp hpar
          | some _ => rfl
        simp only [this]
        exact hout j hj' hab
  · have hb' := (List.mem_filter.mp (show b ∈ _ from hkept)).2
    have hin : cd ∈ (s.core k).innerConds :=
      List.mem_filterMap.mpr ⟨(cd, .inner), hcd, by simp⟩
    exact List.all_eq_true.mp hb' cd hin
  · obtain ⟨r, hr⟩ := Option.ne_none_iff_exists'.mp hl
    have hlk : l ∈ (s.core k).items := by
      simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range] at h
      obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨_, _⟩, _⟩, _⟩, hons⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩ := h
      have := List.all_eq_true.mp (hons k hklt) _ hcd
      simp only [Bool.and_eq_true, List.contains_iff_mem] at this
      exact this.1.2
    exact s.on_kept h Z hk hkept hlk hr cd
      (List.mem_filterMap.mpr ⟨(cd, .leftOn l), hcd, by simp⟩)

/-! The facts a narrowing states of a binding. -/

/-- An edge's fact at binding `b` of core `k`: it holds of `b`, its member
value is NULL for a missing row unless every binding holds the member's row,
a partner outside a subquery that no LEFT JOIN adds has a row in `b`, a
partner inside one sits in an arm of one of `k`'s subqueries, an edge
through a derived item reads through one of `k` or of an arm of one of its
subqueries (`Edge.NestedIn`), and a partner outside a subquery is an item
its own core lists. -/
def Edge.Fact (s : Statement) (Z : Db) (f k : Nat) (b : Bind) (e : Edge) : Prop :=
  e.HoldsIn s Z f b ∧ e.memberStrict s ∧
    (e.nested = false → e.via = none → s.alwaysBound e.partner = true → b e.partner ≠ none) ∧
    e.NestedIn s (s.core k).subqueries ∧
    (e.nested = false → e.via = none → e.partner ∈ (s.core (s.item e.partner).core).items)

/-- An OR's fact at binding `b` of core `k`: it holds of `b`, each side
narrows its item, and each side's edges are from its item with the
properties `Edge.Fact` states beside holding. -/
def Disjunction.Fact (s : Statement) (Z : Db) (f k : Nat) (b : Bind) (d : Disjunction) : Prop :=
  d.HoldsIn s Z f b ∧ (∀ side ∈ d.sides, side.narrows = true) ∧
    ∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧ e.memberStrict s ∧
      (e.nested = false → e.via = none → s.alwaysBound e.partner = true → b e.partner ≠ none) ∧
      e.NestedIn s (s.core k).subqueries

/-- Every term, edge and OR of `n` is on an item of core `k`. -/
def Narrowing.In (s : Statement) (k : Nat) (n : Narrowing) : Prop :=
  (∀ p ∈ n.terms, (s.item p.1).core = k) ∧ (∀ e ∈ n.edges, (s.item e.member).core = k) ∧
    ∀ d ∈ n.ors, (s.item d.item).core = k

/-- Every term of `n` holds of its item's row in `b`, and every edge's and
OR's fact holds at `b`. -/
def Narrowing.Facts (s : Statement) (Z : Db) (f k : Nat) (b : Bind) (n : Narrowing) : Prop :=
  (∀ p ∈ n.terms, p.2.holds (b p.1) = true) ∧ (∀ e ∈ n.edges, e.Fact s Z f k b) ∧
    ∀ d ∈ n.ors, d.Fact s Z f k b

/-- A core that lists conditions is in range. -/
theorem Statement.cond_core_lt {s : Statement} {k : Nat} {x : Cond × Place}
    (hx : x ∈ (s.core k).conds) : k < s.cores.length := by
  refine Decidable.byContradiction fun hk => ?_
  rw [s.core_out hk] at hx
  cases hx

/-- The narrowing a condition of core `k` gives is on items of `k`, and its
facts hold at each binding `k` keeps that the condition holds of. -/
theorem readCondition_facts {s : Statement} (h : s.wellFormed = true) {k : Nat} {cd : Cond}
    {pl : Place} (hcd : (cd, pl) ∈ (s.core k).conds) :
    (readCondition s k cd).In s k ∧ ∀ Z f b, s.KeptIn Z f k b →
      isTrue (cd.eval b (evalQuery s Z f b)) = true → (readCondition s k cd).Facts s Z f k b := by
  obtain ⟨hterms, hedges, hors⟩ := readCondition_sound s k cd
  refine ⟨⟨fun p hp => (hterms p.1 p.2 hp).1, fun e he => (hedges e he).1,
    fun d hd => (hors d hd).1⟩, fun Z f b hb hholds => ?_⟩
  have hnamed := (hb.facts h).2.1
  have hbound : ∀ j, j ∈ cd.items → s.alwaysBound j = true → b j ≠ none := by
    intro j hj hab
    obtain ⟨⟨j', col⟩, hjc, rfl⟩ := List.mem_map.mp hj
    exact hnamed j' col (by
      simp only [Core.refs, List.mem_append, List.mem_flatMap]
      exact Or.inl (Or.inl ⟨(cd, pl), hcd, hjc⟩)) hab
  have hlisted : ∀ j, j ∈ cd.items → j ∈ (s.core (s.item j).core).items := by
    intro j hj
    obtain ⟨⟨j', col⟩, hjc, rfl⟩ := List.mem_map.mp hj
    have hp : (j', col) ∈ (s.core k).refs := by
      simp only [Core.refs, List.mem_append, List.mem_flatMap]
      exact Or.inl (Or.inl ⟨(cd, pl), hcd, hjc⟩)
    obtain ⟨k', _, hjk'⟩ := (s.wellFormed_parts h).refs k (s.cond_core_lt hcd) _ hp
    rw [((s.wellFormed_parts h).items k' (s.core_lt hjk') j' hjk').2]
    exact hjk'
  have hnest : ∀ e : Edge, e.NestedIn s cd.queries → e.NestedIn s (s.core k).subqueries :=
    fun e hn => hn.mono fun q hq => Core.cond_queries hcd hq
  refine ⟨fun p hp => (hterms p.1 p.2 hp).2 Z f b hholds, fun e he => ?_, fun d hd => ?_⟩
  · obtain ⟨_, _, hpart, _, hstrict, hn, hh⟩ := hedges e he
    exact ⟨hh Z f b hholds, hstrict, fun hnf hvn hab => hbound _ (hpart.1 hnf hvn) hab, hnest e hn,
      fun hnf hvn => hlisted _ (hpart.1 hnf hvn)⟩
  · obtain ⟨_, hnar, hsides, hh⟩ := hors d hd
    refine ⟨hh Z f b hholds, hnar, fun side hside e he => ?_⟩
    obtain ⟨hmem, _, hpart, _, hstrict, hn⟩ := hsides side hside e he
    exact ⟨hmem, hstrict, fun hnf hvn hab => hbound _ (hpart.1 hnf hvn) hab, hnest e hn⟩

/-! The invariant. -/

/-- A class of bindings: database, fuel, core and binding. -/
abbrev Cls : Type := Db → Nat → Nat → Bind → Prop

/-- What state `st` says of the bindings of class `P`, each item judged in
bindings of its own core: an item a core lists that the state holds no ON
narrowing back for has a row; each item's term and carried term hold of its
row; each edge from an item with a row was pushed into a derived item's arm
or its fact holds, and so each OR of such an item; and an item
the state holds an ON narrowing back for is one a LEFT JOIN adds, the held
narrowing is on items of its core, and its facts hold where the item has a
row. -/
structure PState.Sem (s : Statement) (P : Cls) (st : PState) : Prop where
  rows : ∀ Z f k b, P Z f k b → ∀ i ∈ (s.core k).items, (st.items i).leftOn = none → b i ≠ none
  terms : ∀ Z f b i r, P Z f (s.item i).core b → b i = some r →
    ∀ t, (st.items i).term = some t → t.holds (some r) = true
  carried : ∀ Z f b i r, P Z f (s.item i).core b → b i = some r →
    ∀ t, (st.items i).carried = some t → t.holds (some r) = true
  edges : ∀ Z f b, ∀ e ∈ st.edges, P Z f (s.item e.member).core b → b e.member ≠ none →
    e ∈ st.pushed ∨ e.Fact s Z f (s.item e.member).core b
  ors : ∀ Z f b i, P Z f (s.item i).core b → b i ≠ none → ∀ sides ∈ (st.items i).ors,
    (i, sides) ∈ st.pushedOrs ∨ Disjunction.Fact s Z f (s.item i).core b ⟨i, sides⟩
  held : ∀ i n, (st.items i).leftOn = some n → s.alwaysBound i = false ∧ n.In s (s.item i).core ∧
    ∀ Z f b, P Z f (s.item i).core b → b i ≠ none → n.Facts s Z f (s.item i).core b

/-- The state before any condition is read keeps the invariant: an item no
LEFT JOIN adds has a row in every binding its core keeps, and a LEFT JOIN's
item holds back nothing yet. -/
theorem PState.Sem.initial {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) : (PState.initial s).Sem s P := by
  refine ⟨fun Z f k b hb i hi hl => ?_, fun _ _ _ _ _ _ _ _ ht => by simp [PState.initial] at ht,
    fun _ _ _ _ _ _ _ _ ht => by simp [PState.initial] at ht,
    fun _ _ _ e he => by simp [PState.initial] at he,
    fun _ _ _ _ _ _ sides hsides => by simp [PState.initial] at hsides, fun i n hn => ?_⟩
  · have hab : s.alwaysBound i = true := by
      simp only [PState.initial] at hl
      split at hl
      · cases hl
      · rename_i hleft
        simp [Statement.alwaysBound, hleft]
    exact ((hP Z f k b hb).facts h).1 i hi hab
  · simp only [PState.initial] at hn
    split at hn
    · rename_i hleft
      obtain rfl := Option.some.inj hn
      have hnil : ∀ Z f k b, Narrowing.Facts s Z f k b ({} : Narrowing) :=
        fun _ _ _ _ => ⟨fun p hp => (List.not_mem_nil hp).elim, fun e he => (List.not_mem_nil he).elim,
          fun d hd => (List.not_mem_nil hd).elim⟩
      exact ⟨by simp [Statement.alwaysBound, hleft], ⟨fun p hp => (List.not_mem_nil hp).elim,
        fun e he => (List.not_mem_nil he).elim, fun d hd => (List.not_mem_nil hd).elim⟩,
        fun Z f b _ _ => hnil Z f _ b⟩
    · cases hn

/-- An update of one item keeps the invariant when the item's new fields
keep it. -/
theorem PState.Sem.update {s : Statement} {P : Cls} {st : PState} (hs : st.Sem s P) {i : Nat}
    {g : PItem → PItem}
    (hrows : (g (st.items i)).leftOn = none → ∀ Z f k b, P Z f k b → i ∈ (s.core k).items → b i ≠ none)
    (hterm : ∀ Z f b r, P Z f (s.item i).core b → b i = some r →
      ∀ t, (g (st.items i)).term = some t → t.holds (some r) = true)
    (hcarried : ∀ Z f b r, P Z f (s.item i).core b → b i = some r →
      ∀ t, (g (st.items i)).carried = some t → t.holds (some r) = true)
    (hors : ∀ Z f b, P Z f (s.item i).core b → b i ≠ none → ∀ sides ∈ (g (st.items i)).ors,
      (i, sides) ∈ st.pushedOrs ∨ Disjunction.Fact s Z f (s.item i).core b ⟨i, sides⟩)
    (hheld : ∀ n, (g (st.items i)).leftOn = some n → s.alwaysBound i = false ∧ n.In s (s.item i).core ∧
      ∀ Z f b, P Z f (s.item i).core b → b i ≠ none → n.Facts s Z f (s.item i).core b) :
    (st.update i g).Sem s P := by
  refine ⟨fun Z f k b hb j hj hl => ?_, fun Z f b j r hb hr t ht => ?_, fun Z f b j r hb hr t ht => ?_,
    fun Z f b e he hb hm => hs.edges Z f b e he hb hm, fun Z f b j hb hr sides hsides => ?_,
    fun j n hn => ?_⟩
  all_goals simp only [PState.update] at *
  · split at hl
    · rename_i hji; subst hji; exact hrows hl Z f k b hb hj
    · exact hs.rows Z f k b hb j hj hl
  · split at ht
    · rename_i hji; subst hji; exact hterm Z f b r hb hr t ht
    · exact hs.terms Z f b j r hb hr t ht
  · split at ht
    · rename_i hji; subst hji; exact hcarried Z f b r hb hr t ht
    · exact hs.carried Z f b j r hb hr t ht
  · split at hsides
    · rename_i hji; subst hji; exact hors Z f b hb hr sides hsides
    · exact hs.ors Z f b j hb hr sides hsides
  · split at hn
    · rename_i hji; subst hji; exact hheld n hn
    · exact hs.held j n hn

/-- Facts that hold of every binding of their item's core with a row of the
item: what adding a narrowing to the state needs. -/
def Narrowing.Where (s : Statement) (P : Cls) (n : Narrowing) : Prop :=
  (∀ p ∈ n.terms, ∀ Z f b r, P Z f (s.item p.1).core b → b p.1 = some r → p.2.holds (some r) = true) ∧
  (∀ e ∈ n.edges, ∀ Z f b, P Z f (s.item e.member).core b → b e.member ≠ none →
    e.Fact s Z f (s.item e.member).core b) ∧
  (∀ d ∈ n.ors, ∀ Z f b, P Z f (s.item d.item).core b → b d.item ≠ none →
    d.Fact s Z f (s.item d.item).core b)

theorem PState.Sem.addTerm {s : Statement} {P : Cls} {st : PState} (hs : st.Sem s P) {i : Nat}
    {t : STerm} (ht : ∀ Z f b r, P Z f (s.item i).core b → b i = some r → t.holds (some r) = true) :
    (st.addTerm i t).Sem s P := by
  unfold PState.addTerm
  refine hs.update (fun hl Z f k b hb hi => hs.rows Z f k b hb i hi hl) (fun Z f b r hb hr u hu => ?_)
    (fun Z f b r hb hr u hu => hs.carried Z f b i r hb hr u hu)
    (fun Z f b hb hr sides hsides => hs.ors Z f b i hb hr sides hsides)
    (fun n hn => hs.held i n hn)
  simp only [Option.some.injEq] at hu
  subst hu
  split
  · rename_i e he
    simp [STerm.holds, hs.terms Z f b i r hb hr e he, ht Z f b r hb hr]
  · exact ht Z f b r hb hr

theorem PState.Sem.add {s : Statement} {P : Cls} {st : PState} (hs : st.Sem s P) {n : Narrowing}
    (hn : n.Where s P) : (st.add n).1.Sem s P := by
  simp only [PState.add]
  have hterms := foldl_invariant (P := PState.Sem s P) (g := fun st (x : Nat × STerm) => st.addTerm x.1 x.2)
    n.terms (fun st x hx hst => hst.addTerm (hn.1 x hx)) st hs
  have hors := foldl_invariant (P := PState.Sem s P) (g := fun st (d : Disjunction) =>
    st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
    n.ors (fun st d hd hst => hst.update (fun hl Z f k b hb hi => hst.rows Z f k b hb d.item hi hl)
      (fun Z f b r hb hr u hu => hst.terms Z f b d.item r hb hr u hu)
      (fun Z f b r hb hr u hu => hst.carried Z f b d.item r hb hr u hu)
      (fun Z f b hb hr sides hsides => by
        rcases List.mem_append.mp hsides with hsides | hsides
        · exact hst.ors Z f b d.item hb hr sides hsides
        · simp only [List.mem_singleton] at hsides
          subst hsides
          exact Or.inr (hn.2.2 d hd Z f b hb hr))
      (fun m hm => hst.held d.item m hm)) _ hterms
  refine ⟨hors.rows, hors.terms, hors.carried, fun Z f b e he hb hm => ?_, hors.ors, hors.held⟩
  rcases List.mem_append.mp (mem_addOnce.mp he) with he | he
  · exact hors.edges Z f b e he hb hm
  · exact Or.inr (hn.2.1 e he Z f b hb hm)

/-- An item a core lists is an item of that core. -/
theorem Statement.KeptIn.core_of {s : Statement} (h : s.wellFormed = true) {Z : Db} {f k : Nat}
    {b : Bind} (hb : s.KeptIn Z f k b) {i : Nat} (hi : i ∈ (s.core k).items) : (s.item i).core = k := by
  obtain ⟨q, _, hk, _, _⟩ := hb
  exact ((s.wellFormed_parts h).items k ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1 i hi).2

/-- Releasing an item that every binding of its core holds a row of keeps
the invariant, and the facts it held back hold at each such binding. -/
theorem PState.Sem.release {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) {st : PState} (hs : st.Sem s P) {i : Nat}
    (hrow : ∀ Z f b, P Z f (s.item i).core b → b i ≠ none) :
    (st.update i fun p => { p with leftOn := none }).Sem s P :=
  hs.update (fun _ Z f k b hb hi => by
      rw [← (hP Z f k b hb).core_of h hi] at hb
      exact hrow Z f b hb)
    (fun Z f b r hb hr u hu => hs.terms Z f b i r hb hr u hu)
    (fun Z f b r hb hr u hu => hs.carried Z f b i r hb hr u hu)
    (fun Z f b hb hr sides hsides => hs.ors Z f b i hb hr sides hsides)
    (fun n hn => by cases hn)

/-- What of `n` the planner adds as it narrows by `n`: a term a row of NULLs
can pass, an `IS` edge and an OR with a side a row of NULLs can pass narrow
nothing on an item the state still holds an ON narrowing back for. -/
def PState.kept (st : PState) (n : Narrowing) : Narrowing :=
  ⟨n.terms.filter (fun p => !(p.2.mayHoldOnNulls && (st.items p.1).leftOn.isSome)),
   n.edges.filter (fun e => !(e.nulls && (st.items e.member).leftOn.isSome)),
   n.ors.filter (fun d => !(d.mayHoldOnNulls && (st.items d.item).leftOn.isSome))⟩

/-- An item that holds a fact a row of NULLs fails, and that the state
holds an ON narrowing back for, has a row in every binding of its core where
the fact holds. -/
theorem PState.Sem.row_of_fact {s : Statement} {P : Cls} {st : PState} (hs : st.Sem s P)
    {i : Nat} {held : Narrowing} (hl : (st.items i).leftOn = some held) {Z : Db} {f k : Nat}
    {b : Bind} {n : Narrowing} (hfacts : n.Facts s Z f k b)
    (hin : i ∈ (st.add (st.kept n)).2) :
    b i ≠ none := by
  have hab := (hs.held i held hl).1
  have hsome : (st.items i).leftOn.isSome = true := by rw [hl]; rfl
  intro hbi
  simp only [PState.add, PState.kept, List.mem_append, List.mem_map, List.mem_filter] at hin
  rcases hin with (⟨⟨j, t⟩, ⟨ht, hkeep⟩, rfl⟩ | ⟨e, ⟨he, hkeep⟩, rfl⟩) | ⟨d, ⟨hd, hkeep⟩, rfl⟩
  · simp only [hsome, Bool.and_true, Bool.not_eq_true'] at hkeep
    have := hfacts.1 (j, t) ht
    simp only at this
    rw [hbi, STerm.holds_none hkeep] at this
    cases this
  · simp only [hsome, Bool.and_true, Bool.not_eq_true'] at hkeep
    obtain ⟨hh, hstrict, hpart, _⟩ := hfacts.2.1 e he
    rcases hh.bound hkeep hstrict hbi with hab' | ⟨hnf, hvn, habp, hbp⟩
    · rw [hab] at hab'; cases hab'
    · exact hpart hnf hvn habp hbp
  · simp only [hsome, Bool.and_true, Bool.not_eq_true'] at hkeep
    obtain ⟨hh, hnar, hsides⟩ := hfacts.2.2 d hd
    rcases hh.bound hkeep hnar (fun side hside e he => ⟨(hsides side hside e he).1,
        (hsides side hside e he).2.1⟩) hbi with hab' | ⟨side, hside, e, he, hnf, hvn, habp, hbp⟩
    · rw [hab] at hab'; cases hab'
    · exact (hsides side hside e he).2.2.1 hnf hvn habp hbp

/-- Making an item inner, when every binding of its core holds its row, is
releasing it and narrowing by what its ON held back. -/
theorem PState.Sem.joinInner_of {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) {fuel : Nat}
    (hN : ∀ (st : PState) (n : Narrowing) (k : Nat), st.Sem s P → n.In s k →
      (∀ Z f b, P Z f k b → n.Facts s Z f k b) → (PState.narrow fuel st n).Sem s P)
    {st : PState} (hs : st.Sem s P) {i : Nat}
    (hrow : ∀ Z f b, P Z f (s.item i).core b → (st.items i).leftOn ≠ none → b i ≠ none) :
    (st.joinInner fuel i).Sem s P := by
  unfold PState.joinInner
  split
  · rename_i held hheld
    have hrow' : ∀ Z f b, P Z f (s.item i).core b → b i ≠ none :=
      fun Z f b hb => hrow Z f b hb (by rw [hheld]; simp)
    obtain ⟨_, hin, hfacts⟩ := hs.held i held hheld
    exact hN _ held _ (hs.release h hP hrow') hin fun Z f b hb => hfacts Z f b hb (hrow' Z f b hb)
  · exact hs

/-- Narrowing by facts that hold at every binding of a core keeps the
invariant: each item it makes inner holds a fact a row of NULLs fails, so
every binding of its core holds its row. -/
theorem PState.Sem.narrow {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) : ∀ (fuel : Nat) (st : PState) (n : Narrowing)
    (k : Nat), st.Sem s P → n.In s k → (∀ Z f b, P Z f k b → n.Facts s Z f k b) →
      (PState.narrow fuel st n).Sem s P
  | 0, _, _, _, hs, _, _ => hs
  | fuel + 1, st, n, k, hs, hin, hfacts => by
    have ihN := PState.Sem.narrow h hP fuel
    show (List.foldl (fun st i => st.joinInner fuel i) (st.add (st.kept n)).1
      (st.add (st.kept n)).2).Sem s P
    have hwhere : (st.kept n).Where s P := by
      refine ⟨fun p hp Z f b r hb hr => ?_, fun e he Z f b hb _ => ?_, fun d hd Z f b hb _ => ?_⟩
      · have hp' := (List.mem_filter.mp hp).1
        rw [hin.1 p hp'] at hb
        have := (hfacts Z f b hb).1 p hp'
        rwa [hr] at this
      · have he' := (List.mem_filter.mp he).1
        rw [hin.2.1 e he'] at hb ⊢
        exact (hfacts Z f b hb).2.1 e he'
      · have hd' := (List.mem_filter.mp hd).1
        rw [hin.2.2 d hd'] at hb ⊢
        exact (hfacts Z f b hb).2.2 d hd'
    have hik : ∀ i ∈ (st.add (st.kept n)).2, (s.item i).core = k := by
      intro i hi
      simp only [PState.add, PState.kept, List.mem_append, List.mem_map, List.mem_filter] at hi
      rcases hi with (⟨p, ⟨hp, _⟩, rfl⟩ | ⟨e, ⟨he, _⟩, rfl⟩) | ⟨d, ⟨hd, _⟩, rfl⟩
      · exact hin.1 p hp
      · exact hin.2.1 e he
      · exact hin.2.2 d hd
    refine (foldl_invariant (P := fun x => x.Sem s P ∧ PState.Keeps s st x) _ (fun x i hi hx => ?_) _
      ⟨hs.add hwhere, PState.add_keeps s st _⟩).1
    refine ⟨hx.1.joinInner_of h hP ihN fun Z f b hb hl => ?_,
      hx.2.trans (PState.joinInner_keeps s fuel x i)⟩
    have hsome := hx.2.1.1 i (by
      cases hxl : (x.items i).leftOn with
      | none => exact absurd hxl hl
      | some _ => rfl)
    obtain ⟨held0, hheld0⟩ := Option.isSome_iff_exists.mp hsome
    rw [hik i hi] at hb
    exact hs.row_of_fact hheld0 (hfacts Z f b hb) hi

/-- Narrowing by a condition of the ON of the LEFT JOIN that adds `i` keeps
the invariant: what it says of `i` holds where `i` has a row, the rest waits
with what the state holds back for `i`, or narrows once `i` is inner. -/
theorem PState.Sem.narrowOn {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) {k : Nat} {cd : Cond} {i : Nat}
    (hcd : (cd, Place.leftOn i) ∈ (s.core k).conds) (fuel : Nat) {st : PState} (hs : st.Sem s P) :
    (st.narrowOn fuel i (readCondition s k cd)).Sem s P := by
  have hklt := s.cond_core_lt hcd
  obtain ⟨hin, hfacts⟩ := readCondition_facts h hcd
  have hik : i ∈ (s.core k).items := by
    simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range] at h
    obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨_, _⟩, _⟩, _⟩, hons⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩ := h
    have := List.all_eq_true.mp (hons k hklt) _ hcd
    simp only [Bool.and_eq_true, List.contains_iff_mem] at this
    exact this.1.2
  have hcore : (s.item i).core = k := ((s.wellFormed_parts h).items k hklt i hik).2
  -- The condition's facts hold at each binding of the core with a row of `i`.
  have hon : ∀ Z f b, P Z f k b → b i ≠ none → (readCondition s k cd).Facts s Z f k b :=
    fun Z f b hb hbi => hfacts Z f b (hP Z f k b hb) (((hP Z f k b hb).facts h).2.2.2 cd i hcd hbi)
  let n := readCondition s k cd
  have hown : (n.split i).1.Where s P := by
    refine ⟨fun p hp Z f b r hb hr => ?_, fun e he Z f b hb hm => ?_, fun d hd Z f b hb hm => ?_⟩
    · obtain ⟨hp', hpi⟩ := List.mem_filter.mp hp
      simp only [beq_iff_eq] at hpi
      rw [hpi, hcore] at hb
      have := (hon Z f b hb (by rw [← hpi, hr]; simp)).1 p hp'
      rwa [hr] at this
    · obtain ⟨he', hei⟩ := List.mem_filter.mp he
      simp only [Edge.ownOn, beq_iff_eq] at hei
      rw [hei, hcore] at hb ⊢
      exact (hon Z f b hb (hei ▸ hm)).2.1 e he'
    · obtain ⟨hd', hdi⟩ := List.mem_filter.mp hd
      simp only [Disjunction.ownOn, beq_iff_eq] at hdi
      rw [hdi, hcore] at hb ⊢
      exact (hon Z f b hb (hdi ▸ hm)).2.2 d hd'
  have hothersIn : (n.split i).2.In s k :=
    ⟨fun p hp => hin.1 p (List.mem_filter.mp hp).1, fun e he => hin.2.1 e (List.mem_filter.mp he).1,
      fun d hd => hin.2.2 d (List.mem_filter.mp hd).1⟩
  have hothersFacts : ∀ Z f b, P Z f k b → b i ≠ none → (n.split i).2.Facts s Z f k b := by
    intro Z f b hb hbi
    have hF := hon Z f b hb hbi
    exact ⟨fun p hp => hF.1 p (List.mem_filter.mp hp).1, fun e he => hF.2.1 e (List.mem_filter.mp he).1,
      fun d hd => hF.2.2 d (List.mem_filter.mp hd).1⟩
  have hs1 := hs.add hown
  unfold PState.narrowOn
  show (match ((st.add (n.split i).1).1.items i).leftOn with
    | some held => (st.add (n.split i).1).1.update i fun p => { p with leftOn := some (held.append (n.split i).2) }
    | none => PState.narrow fuel (st.add (n.split i).1).1 (n.split i).2).Sem s P
  split
  · rename_i held hheld
    obtain ⟨hab, hheldIn, hheldFacts⟩ := hs1.held i held hheld
    refine hs1.update (fun hl => by cases hl) (fun Z f b r hb hr u hu => hs1.terms Z f b i r hb hr u hu)
      (fun Z f b r hb hr u hu => hs1.carried Z f b i r hb hr u hu)
      (fun Z f b hb hr sides hsides => hs1.ors Z f b i hb hr sides hsides) (fun m hm => ?_)
    simp only [Option.some.injEq] at hm
    subst hm
    rw [hcore] at hheldIn hheldFacts ⊢
    refine ⟨hab, ⟨fun p hp => ?_, fun e he => ?_, fun d hd => ?_⟩, fun Z f b hb hbi => ?_⟩
    · rcases List.mem_append.mp hp with hp | hp
      · exact hheldIn.1 p hp
      · exact hothersIn.1 p hp
    · rcases List.mem_append.mp he with he | he
      · exact hheldIn.2.1 e he
      · exact hothersIn.2.1 e he
    · rcases List.mem_append.mp hd with hd | hd
      · exact hheldIn.2.2 d hd
      · exact hothersIn.2.2 d hd
    · have hA := hheldFacts Z f b hb hbi
      have hB := hothersFacts Z f b hb hbi
      refine ⟨fun p hp => ?_, fun e he => ?_, fun d hd => ?_⟩
      · rcases List.mem_append.mp hp with hp | hp
        · exact hA.1 p hp
        · exact hB.1 p hp
      · rcases List.mem_append.mp he with he | he
        · exact hA.2.1 e he
        · exact hB.2.1 e he
      · rcases List.mem_append.mp hd with hd | hd
        · exact hA.2.2 d hd
        · exact hB.2.2 d hd
  · rename_i hheld
    exact PState.Sem.narrow h hP fuel _ _ k hs1 hothersIn fun Z f b hb =>
      hothersFacts Z f b hb (hs1.rows Z f k b hb i hik hheld)

/-- Reading a core's conditions keeps the invariant. -/
theorem PState.Sem.readConds {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) (k : Nat)
    (l : List (Narrowing × Place))
    (hl : ∀ x ∈ l, ∃ cd, (cd, x.2) ∈ (s.core k).conds ∧ x.1 = readCondition s k cd)
    {st : PState} (hs : st.Sem s P) :
    (l.foldl (fun st (n, p) =>
      match p with
      | .inner => st.narrow s.fuel n
      | .leftOn i => st.narrowOn s.fuel i n) st).Sem s P := by
  refine foldl_invariant (P := PState.Sem s P) l (fun st x hx hst => ?_) st hs
  obtain ⟨cd, hcd, hn⟩ := hl x hx
  obtain ⟨n, p⟩ := x
  simp only at hcd hn
  subst hn
  cases p with
  | inner =>
    obtain ⟨hin, hfacts⟩ := readCondition_facts h hcd
    exact PState.Sem.narrow h hP s.fuel _ _ k hst hin fun Z f b hb =>
      hfacts Z f b (hP Z f k b hb) (((hP Z f k b hb).facts h).2.2.1 cd hcd)
  | leftOn i => exact hst.narrowOn h hP hcd s.fuel

/-! Carrying terms across a core's edges. -/

/-- What `Transfer.carried` lists: a term from an edge of the state between
two items of core `k`, outside a subquery, that compares the partner's column
`y` with the member's column `mc`, renamed from `y` to `mc` from the
partner's own term or, for an edge that compares by `=`, from what the
partner's arms say of `y`. -/
theorem Transfer.mem_carried {s : Statement} {st : PState} {k : Nat} {x : Nat × STerm}
    (hx : x ∈ Transfer.carried s st k) :
    ∃ e ∈ st.edges, (s.item e.member).core = k ∧ (s.item e.partner).core = k ∧ e.nested = false ∧
      e.via = none ∧ ∃ y mc, e.value = .column y ∧ e.memberValue = .column mc ∧ x.1 = e.member ∧
        ∃ u, u.renamed (fun c => if c = y then some mc else none) (fun _ _ => none) = some x.2 ∧
          ((st.items e.partner).term = some u ∨
            (e.nulls = false ∧ Transfer.pulled s st e.partner y = some u)) := by
  obtain ⟨e, he, hx⟩ := List.mem_flatMap.mp hx
  split at hx
  · rename_i hc
    obtain ⟨hcm, hcp, hn, hvn⟩ := hc
    split at hx
    · rename_i y mc hv hm
      refine ⟨e, he, hcm, hcp, hn, hvn, y, mc, hv, hm, ?_⟩
      rcases List.mem_append.mp hx with hx | hx
      · obtain ⟨t', ht', rfl⟩ := List.mem_map.mp hx
        simp only [Option.mem_toList, Option.bind_eq_some_iff] at ht'
        obtain ⟨u, hu, hr⟩ := ht'
        exact ⟨rfl, u, hr, Or.inl hu⟩
      · obtain ⟨t', ht', rfl⟩ := List.mem_map.mp hx
        simp only [Option.mem_toList, Option.bind_eq_some_iff] at ht'
        obtain ⟨u, hu, hr⟩ := ht'
        cases hnull : e.nulls with
        | true => simp [hnull] at hu
        | false =>
          simp only [hnull, Bool.false_eq_true, ↓reduceIte] at hu
          exact ⟨rfl, u, hr, Or.inr ⟨rfl, hu⟩⟩
    · cases hx
  · cases hx

theorem PState.Sem.addCarried {s : Statement} {P : Cls} {st : PState} (hs : st.Sem s P) {i : Nat}
    {t : STerm} (ht : ∀ Z f b r, P Z f (s.item i).core b → b i = some r → t.holds (some r) = true) :
    (Transfer.addCarried st i t).Sem s P := by
  unfold Transfer.addCarried
  refine hs.update (fun hl Z f k b hb hi => hs.rows Z f k b hb i hi hl)
    (fun Z f b r hb hr u hu => hs.terms Z f b i r hb hr u hu) (fun Z f b r hb hr u hu => ?_)
    (fun Z f b hb hr sides hsides => hs.ors Z f b i hb hr sides hsides)
    (fun n hn => hs.held i n hn)
  simp only [Option.some.injEq] at hu
  subst hu
  split
  · rename_i e he
    simp [STerm.holds, hs.carried Z f b i r hb hr e he, ht Z f b r hb hr]
  · exact ht Z f b r hb hr

/-- Carrying terms across core `k`'s edges keeps the invariant, when the
state has pushed no edge into a derived item's arm, and a row of a derived
item of `k` passes what its arms say of each of its columns that isn't NULL: each edge
an item with a row holds has its fact, whose partner then has a row, and a
term true of the partner's row is true, renamed, of the member's. -/
theorem PState.Sem.carry {s : Statement} {P : Cls} {st : PState} (hs : st.Sem s P)
    (hpushed : st.pushed = []) (k : Nat)
    (hpull : ∀ Z f b i r y t, (s.item i).core = k → i ∈ (s.core k).items → P Z f k b →
      b i = some r → r.cell y ≠ none → Transfer.pulled s st i y = some t → t.holds (some r) = true) :
    (Transfer.carry s st k).Sem s P := by
  unfold Transfer.carry
  refine foldl_invariant (P := PState.Sem s P) _ (fun x c hc hx => hx.addCarried ?_) st hs
  obtain ⟨e, he, hcm, hcp, hn, hvn, y, mc, hv, hm, hi, u, hr, hu⟩ := Transfer.mem_carried hc
  intro Z f b r hb hbr
  rw [hi] at hb hbr
  have hmem : b e.member ≠ none := by rw [hbr]; simp
  rcases hs.edges Z f b e he hb hmem with hp | ⟨hh, _, hpart, _, hlist⟩
  · rw [hpushed] at hp
    cases hp
  have hpk : P Z f (s.item e.partner).core b := by rw [hcp, ← hcm]; exact hb
  have hh' := hh
  obtain ⟨rp, hrp, hcell⟩ : ∃ rp, b e.partner = some rp ∧ (e.nulls = false → rp.cell y ≠ none) := by
    rcases hh' with ⟨x, _, hx⟩ | ⟨hnul, _, _, hab, _, _⟩ | ⟨hst, _, _, _⟩ | ⟨_, _, _, _, hnt, _⟩ |
      ⟨_, _, _, hv', _⟩
    · simp only [hn, Bool.false_eq_true, ↓reduceIte] at hx
      obtain ⟨rp, hrp, hval⟩ := hx
      refine ⟨rp, hrp, fun _ => ?_⟩
      rw [hv] at hval
      simp only [PValue.eval] at hval
      rw [hval]
      simp
    · obtain ⟨rp, hrp⟩ := Option.ne_none_iff_exists'.mp (hpart hn hvn hab)
      exact ⟨rp, hrp, fun hnf => by rw [hnf] at hnul; cases hnul⟩
    · simp [hv, PValue.strict] at hst
    · rw [hn] at hnt
      cases hnt
    · rw [hvn] at hv'
      cases hv'
  have hut : u.holds (b e.partner) = true := by
    rw [hrp]
    rcases hu with hu | ⟨hnf, hu⟩
    · exact hs.terms Z f b e.partner rp hpk hrp u hu
    · have hlist' := hlist hn hvn
      rw [hcp] at hlist'
      exact hpull Z f b e.partner rp y u hcp hlist' (by rw [← hcm]; exact hb) hrp (hcell hnf) hu
  have := Transfer.transfer_sound hv hm hn hvn hh hut hr
  rwa [hbr] at this

/-- Passing a derived item's ORs into its arms keeps the invariant: each OR it
gives an item is among the pushed ones. -/
theorem PState.Sem.pushOrs {s : Statement} {P : Cls} {st : PState} (hs : st.Sem s P) (item : Nat)
    (ors : List (List Side)) : (PushDown.pushOrs s st item ors).Sem s P := by
  refine PushDown.pushOrs_induct (P := fun x : PState => x.Sem s P) hs
    (fun _ _ arm _ sides _ target _ x hx => ?_)
  rcases PushDown.passOr_cases s target (PushDown.renameInto arm target) sides x with h | ⟨_, _, h⟩
  · rw [h]; exact hx
  · rw [h]
    refine ⟨fun Z f k b hb j hj hl => ?_, fun Z f b j r hb hr t ht => ?_,
      fun Z f b j r hb hr t ht => ?_, fun Z f b e he hb hm => hx.edges Z f b e he hb hm,
      fun Z f b j hb hr found hfound => ?_, fun j n hn => ?_⟩
    all_goals simp only [PState.update] at *
    · split at hl
      · rename_i hji; subst hji; exact hx.rows Z f k b hb j hj hl
      · exact hx.rows Z f k b hb j hj hl
    · split at ht
      · rename_i hji; subst hji; exact hx.terms Z f b j r hb hr t ht
      · exact hx.terms Z f b j r hb hr t ht
    · split at ht
      · rename_i hji; subst hji; exact hx.carried Z f b j r hb hr t ht
      · exact hx.carried Z f b j r hb hr t ht
    · split at hfound
      · rename_i hji
        subst hji
        rcases List.mem_append.mp hfound with hfound | hfound
        · exact (hx.ors Z f b j hb hr found hfound).imp_left (List.mem_append_left _)
        · simp only [List.mem_singleton] at hfound
          subst hfound
          exact Or.inl (List.mem_append_right _ (List.mem_singleton_self _))
      · exact (hx.ors Z f b j hb hr found hfound).imp_left (List.mem_append_left _)
    · split at hn
      · rename_i hji; subst hji; exact hx.held j n hn
      · exact hx.held j n hn

end SqliveNarrowing
