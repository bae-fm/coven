import SqliveNarrowing.OrderPhase

/-! Where the planned state's edges take their values from. A condition's
reader gives only edges whose partner a lookup reads alone (`Item.readable`),
and the planner moves an edge's member only, so every edge of the planned
state, every side of each OR and every narrowing an ON holds back takes its
values from such an item; each OR an item keeps is one of its ORs; and each
edge the planner passes into a derived item's arms has a member its core
lists (`plan_lookups`). A derived item a lookup reads alone runs its query
under no row around it (`evalQuery_readable`), and a query every row of which
matters makes every binding of its arms matter (`relevant_of_all`). -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-- Whether every edge of `n`, and every edge of each side of its ORs, takes
values from an item a lookup reads alone. -/
def Narrowing.Readable (s : Statement) (n : Narrowing) : Prop :=
  (∀ e ∈ n.edges, (s.item e.partner).readable = true) ∧
    ∀ d ∈ n.ors, ∀ side ∈ d.sides, ∀ e ∈ side.edges, (s.item e.partner).readable = true

/-- What the planner's state keeps of where its edges take values from: each
edge's partner, and each edge's of a side of each OR, is an item a lookup
reads alone, as is each partner of a narrowing an ON holds back; each OR an
item keeps is one of its ORs; and each edge passed into a derived item's arm
has a member its core lists. -/
def PState.Lookups (s : Statement) (st : PState) : Prop :=
  (∀ e ∈ st.edges, (s.item e.partner).readable = true) ∧
  (∀ i, ∀ sides ∈ (st.items i).ors, ∀ side ∈ sides, ∀ e ∈ side.edges,
    (s.item e.partner).readable = true) ∧
  (∀ i, ∀ sides ∈ (st.items i).either, sides ∈ (st.items i).ors) ∧
  (∀ i n, (st.items i).leftOn = some n → n.Readable s) ∧
  ∀ e ∈ st.pushed, e.member ∈ (s.core (s.item e.member).core).items

theorem PState.Lookups.update {s : Statement} {st : PState} (h : st.Lookups s) {i : Nat}
    {f : PItem → PItem}
    (hors : ∀ sides ∈ (f (st.items i)).ors, ∀ side ∈ sides, ∀ e ∈ side.edges,
      (s.item e.partner).readable = true)
    (heither : ∀ sides ∈ (f (st.items i)).either, sides ∈ (f (st.items i)).ors)
    (hl : ∀ n, (f (st.items i)).leftOn = some n → n.Readable s) : (st.update i f).Lookups s := by
  refine ⟨h.1, fun j sides hs => ?_, fun j sides hs => ?_, fun j n hn => ?_, h.2.2.2.2⟩
  all_goals simp only [PState.update] at *
  · split at hs
    · rename_i hji; subst hji; exact hors sides hs
    · exact h.2.1 j sides hs
  · split
    · rename_i hji; subst hji; simp only [↓reduceIte] at hs; exact heither sides hs
    · rename_i hji; simp only [hji, ↓reduceIte] at hs; exact h.2.2.1 j sides hs
  · split at hn
    · rename_i hji; subst hji; exact hl n hn
    · exact h.2.2.2.1 j n hn

/-- An update that keeps an item's ORs and kept ORs, and keeps or drops its
held narrowing, keeps the invariant. -/
theorem PState.Lookups.update_same {s : Statement} {st : PState} (h : st.Lookups s) {i : Nat}
    {f : PItem → PItem} (hors : (f (st.items i)).ors = (st.items i).ors)
    (heither : (f (st.items i)).either = (st.items i).either)
    (hl : ∀ n, (f (st.items i)).leftOn = some n → (st.items i).leftOn = some n) :
    (st.update i f).Lookups s :=
  h.update (fun sides hs => h.2.1 i sides (hors ▸ hs))
    (fun sides hs => by rw [hors]; exact h.2.2.1 i sides (heither ▸ hs))
    (fun n hn => h.2.2.2.1 i n (hl n hn))

theorem PState.Lookups.addTerm {s : Statement} {st : PState} (h : st.Lookups s) (i : Nat) (t : STerm) :
    (st.addTerm i t).Lookups s :=
  h.update_same rfl rfl (fun _ hn => hn)

theorem PState.Lookups.add {s : Statement} {st : PState} (h : st.Lookups s) {n : Narrowing}
    (hn : n.Readable s) : (st.add n).1.Lookups s := by
  simp only [PState.add]
  have hterms := foldl_invariant (P := PState.Lookups s) (g := fun st (x : Nat × STerm) => st.addTerm x.1 x.2)
    n.terms (fun st x _ hst => hst.addTerm x.1 x.2) st h
  have hors := foldl_invariant (P := PState.Lookups s) (g := fun st (d : Disjunction) =>
    st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
    n.ors (fun st d hd hst => hst.update
      (fun sides hs side hside e he => by
        rcases List.mem_append.mp hs with hs | hs
        · exact hst.2.1 d.item sides hs side hside e he
        · simp only [List.mem_singleton] at hs
          subst hs
          exact hn.2 d hd side hside e he)
      (fun sides hs => List.mem_append_left _ (hst.2.2.1 d.item sides hs))
      (fun m hm => hst.2.2.2.1 d.item m hm)) _ hterms
  refine ⟨fun e he => ?_, hors.2.1, hors.2.2.1, hors.2.2.2.1, hors.2.2.2.2⟩
  rcases List.mem_append.mp he with he | he
  · exact hors.1 e he
  · exact hn.1 e he

/-- A narrowing's parts that a filter keeps take values where the
narrowing's do. -/
theorem Narrowing.Readable.filter {s : Statement} {n : Narrowing} (h : n.Readable s)
    (pt : Nat × STerm → Bool) (pe : Edge → Bool) (pd : Disjunction → Bool) :
    Narrowing.Readable s ⟨n.terms.filter pt, n.edges.filter pe, n.ors.filter pd⟩ :=
  ⟨fun e he => h.1 e (List.mem_filter.mp he).1, fun d hd => h.2 d (List.mem_filter.mp hd).1⟩

theorem PState.Lookups.narrow {s : Statement} :
    ∀ (fuel : Nat) {st : PState} {n : Narrowing}, st.Lookups s → n.Readable s →
      (PState.narrow fuel st n).Lookups s
  | 0, _, _, h, _ => h
  | fuel + 1, st, n, h, hn => by
    simp only [PState.narrow]
    refine foldl_invariant _ (fun st' i _ hst' => ?_) _ (h.add (hn.filter _ _ _))
    split
    · rename_i held hheld
      exact PState.Lookups.narrow fuel (hst'.update_same rfl rfl (by simp))
        (hst'.2.2.2.1 i held hheld)
    · exact hst'

theorem PState.Lookups.joinInner {s : Statement} {st : PState} (h : st.Lookups s) (fuel i : Nat) :
    (st.joinInner fuel i).Lookups s := by
  unfold PState.joinInner
  split
  · rename_i held hheld
    exact PState.Lookups.narrow fuel (h.update_same rfl rfl (by simp)) (h.2.2.2.1 i held hheld)
  · exact h

theorem PState.Lookups.narrowOn {s : Statement} {st : PState} (h : st.Lookups s) (fuel i : Nat)
    {n : Narrowing} (hn : n.Readable s) : (st.narrowOn fuel i n).Lookups s := by
  unfold PState.narrowOn
  simp only [Narrowing.split]
  have hown := h.add (hn.filter (·.1 == i) (·.member == i) (·.item == i))
  have hothers := hn.filter (·.1 != i) (·.member != i) (·.item != i)
  split
  · rename_i held hheld
    refine hown.update (fun sides hs => hown.2.1 i sides hs)
      (fun sides hs => hown.2.2.1 i sides hs) ?_
    intro n' hn'
    simp only [Option.some.injEq] at hn'
    subst hn'
    have hh := hown.2.2.2.1 i held hheld
    refine ⟨fun e he => ?_, fun d hd => ?_⟩
    · rcases List.mem_append.mp he with he | he
      · exact hh.1 e he
      · exact hothers.1 e he
    · rcases List.mem_append.mp hd with hd | hd
      · exact hh.2 d hd
      · exact hothers.2 d hd
  · exact PState.Lookups.narrow fuel hown hothers

theorem PState.Lookups.carry {s : Statement} {st : PState} (h : st.Lookups s) (k : Nat) :
    (Transfer.carry s st k).Lookups s :=
  foldl_invariant _ (fun st x _ hst => by
    unfold Transfer.addCarried
    exact hst.update_same rfl rfl (fun _ hl => hl)) _ h

theorem PState.Lookups.pushDown {s : Statement} (hwf : s.wellFormed = true) (narrowFuel : Nat) :
    ∀ (fuel : Nat) {st : PState} (item : Nat) (term : Option STerm) {edges : List Edge},
      st.Lookups s → (∀ e ∈ edges, (s.item e.partner).readable = true) →
      (PushDown.pushDown s narrowFuel fuel st item term edges).Lookups s
  | 0, _, _, _, _, h, _ => h
  | fuel + 1, st, item, term, edges, h, hedges => by
    simp only [PushDown.pushDown]
    split
    · exact h
    · rename_i found hfound
      split
      · exact h
      · refine foldl_invariant _ (fun st0 arm harm hst0 => ?_) _ h
        refine foldl_invariant _ (fun st1 target _ hst1 => ?_) _ hst0
        obtain ⟨q, v, k, _, _, _, hk, _, harmk⟩ := PushDown.mem_arms hfound harm
        have hpassed : ∀ e ∈ (edges.filter fun e => !((st1.items target).leftOn.isSome && e.nulls)).filterMap
            (fun e => match e.memberValue with
              | .column c => ((match arm[c]?.join with
                  | some (t, c') => if t = target then some c' else none
                  | none => none).map fun c' => { e with member := target, memberValue := .column c', pushed := true })
              | .expr _ => none),
            (s.item e.partner).readable = true ∧ e.member ∈ (s.core (s.item e.member).core).items := by
          intro e he
          obtain ⟨e0, he0, c, c', _, hthrough, rfl⟩ := PushDown.mem_passed he
          obtain ⟨hcore, _, hout⟩ := harmk c target c' hthrough
          have hklt := ((s.wellFormed_parts hwf).arms q (s.query_lt hk) k hk).1
          exact ⟨hedges e0 he0, by
            show target ∈ (s.core (s.item target).core).items
            rw [hcore]
            exact s.output_listed hwf hklt hcore hout⟩
        apply PState.Lookups.pushDown hwf narrowFuel fuel
        · have hst2 : ∀ (renamed : Option STerm), (match renamed with
              | some r => st1.addTerm target r
              | none => st1).Lookups s := by
            intro renamed
            cases renamed
            · exact hst1
            · exact hst1.addTerm target _
          have hst3 : ∀ (renamed : Option STerm), PState.Lookups s { (match renamed with
              | some r => st1.addTerm target r
              | none => st1) with
              edges := (match renamed with
                | some r => st1.addTerm target r
                | none => st1).edges ++ (edges.filter fun e => !((st1.items target).leftOn.isSome && e.nulls)).filterMap
                  (fun e => match e.memberValue with
                    | .column c => ((match arm[c]?.join with
                        | some (t, c') => if t = target then some c' else none
                        | none => none).map fun c' => { e with member := target, memberValue := .column c', pushed := true })
                    | .expr _ => none),
              pushed := (match renamed with
                | some r => st1.addTerm target r
                | none => st1).pushed ++ (edges.filter fun e => !((st1.items target).leftOn.isSome && e.nulls)).filterMap
                  (fun e => match e.memberValue with
                    | .column c => ((match arm[c]?.join with
                        | some (t, c') => if t = target then some c' else none
                        | none => none).map fun c' => { e with member := target, memberValue := .column c', pushed := true })
                    | .expr _ => none) } := by
            intro renamed
            refine ⟨fun e he => (List.mem_append.mp he).elim ((hst2 _).1 e) (fun he => (hpassed e he).1),
              (hst2 _).2.1, (hst2 _).2.2.1, (hst2 _).2.2.2.1, fun e he => ?_⟩
            rcases List.mem_append.mp he with he | he
            · exact (hst2 _).2.2.2.2 e he
            · exact (hpassed e he).2
          split
          · exact (hst3 _).joinInner _ _
          · exact hst3 _
        · intro e he
          exact (hpassed e he).1

theorem PState.Lookups.readCore {s : Statement} (hwf : s.wellFormed = true) {st : PState}
    (h : st.Lookups s) (k : Nat) : (SqliveNarrowing.readCore s st k).Lookups s := by
  unfold SqliveNarrowing.readCore
  simp only
  refine foldl_invariant _ (fun st i _ hst => PState.Lookups.pushDown hwf _ _ i _ hst (by simp)) _ ?_
  refine PState.Lookups.carry ?_ k
  refine foldl_invariant _ (fun st x hx hst => ?_) _ h
  obtain ⟨n, p⟩ := x
  have hread : ∀ x ∈ (s.core k).conds.map fun (cd, p) => (readCondition s k cd, p),
      x.1.Readable s := by
    intro x hx
    obtain ⟨⟨cd, p'⟩, _, rfl⟩ := List.mem_map.mp hx
    obtain ⟨_, hedges, hors⟩ := readCondition_sound s k cd
    exact ⟨fun e he => (hedges e he).2.2.2.1,
      fun d hd side hside e he => ((hors d hd).2.2.1 side hside e he).2.2.2.1⟩
  have hn : n.Readable s := by
    rcases List.mem_append.mp hx with hx | hx
    · exact hread _ (List.mem_filter.mp hx).1
    · exact hread _ (List.mem_filter.mp hx).1
  cases p
  · exact PState.Lookups.narrow _ hst hn
  · exact hst.narrowOn _ _ hn

theorem PState.Lookups.ordered {s : Statement} (hwf : s.wellFormed = true) :
    ∀ (fuel : Nat) (all : List Nat) {st : PState} (order waited : List Nat), st.Lookups s →
      (SqliveNarrowing.ordered.go s fuel all st order waited).1.Lookups s
  | 0, _, _, _, _, h => h
  | fuel + 1, all, st, order, waited, h => by
    rcases ordered_go_place s fuel all st order waited with ⟨_, heq⟩ | ⟨next, waited', _, heq⟩
    · rw [heq]; exact h
    · rw [heq]
      apply PState.Lookups.ordered hwf fuel
      unfold ordered.placed
      simp only
      refine PState.Lookups.pushDown hwf _ _ _ _ (h.update (fun sides hs => h.2.1 next sides hs)
        (fun sides hs => (List.mem_filter.mp hs).1) (fun n hn => h.2.2.2.1 next n hn)) ?_
      exact fun e he => h.1 e (List.mem_filter.mp he).1

/-- Every edge, OR side and held narrowing of the planned state takes values
from items a lookup reads alone, each OR an item keeps is one of its ORs, and
each edge passed into a derived item's arm has a member its core lists. -/
theorem plan_lookups {s : Statement} (hwf : s.wellFormed = true) : (plan s).state.Lookups s := by
  unfold plan ordered
  simp only
  refine PState.Lookups.ordered hwf _ _ _ _ ?_
  refine foldl_invariant _ (fun st k _ hst => hst.readCore hwf k) _ ?_
  refine ⟨fun e he => by simp [PState.initial] at he, fun i sides hs => by simp [PState.initial] at hs,
    fun i sides hs => by simp [PState.initial] at hs, fun i n hn => ?_,
    fun e he => by simp [PState.initial] at he⟩
  simp only [PState.initial] at hn
  split at hn
  · cases hn
    exact ⟨fun e he => (by cases he), fun d hd => (by cases hd)⟩
  · cases hn

/-! The order places only items cores list. -/

theorem ordered_go_sub (s : Statement) : ∀ (fuel : Nat) (all : List Nat) (st : PState)
    (order waited : List Nat), (∀ i ∈ order, i ∈ all) →
    ∀ i ∈ (ordered.go s fuel all st order waited).2, i ∈ all
  | 0, _, _, _, _, h => h
  | fuel + 1, all, st, order, waited, h => by
    rcases ordered_go_place s fuel all st order waited with ⟨_, heq⟩ | ⟨next, waited', hnext, heq⟩
    · rw [heq]; exact h
    · rw [heq]
      refine ordered_go_sub s fuel all _ _ _ fun i hi => ?_
      rcases List.mem_append.mp hi with hi | hi
      · exact h i hi
      · rw [List.mem_singleton.mp hi]
        exact (List.mem_filter.mp hnext).1

/-- Each item the planner places is an item its core lists. -/
theorem plan_order_listed {s : Statement} (hwf : s.wellFormed = true) {i : Nat}
    (hi : i ∈ (plan s).order) : i ∈ (s.core (s.item i).core).items := by
  have hall : i ∈ (List.range s.cores.length).flatMap fun k => (s.core k).items := by
    unfold plan ordered at hi
    simp only at hi
    exact ordered_go_sub s _ _ _ [] [] (fun _ h => by cases h) i hi
  obtain ⟨k, hk, hik⟩ := List.mem_flatMap.mp hall
  rw [((s.wellFormed_parts hwf).items k (List.mem_range.mp hk) i hik).2]
  exact hik

/-! Derived items a lookup reads alone. -/

/-- Every core of a query among queries that name nothing outside them is an
arm of one of them. -/
theorem View.closed_queryCores {s : Statement} {qs : List Nat} (h : View.closed s qs = true) :
    ∀ (f q : Nat), q ∈ qs → ∀ c ∈ s.queryCores f q, c ∈ View.armsOf s qs
  | 0, _, _, _, hc => by simp [Statement.queryCores] at hc
  | f + 1, q, hq, c, hc => by
    obtain ⟨k, hk, rfl | ⟨q', hq', hc⟩⟩ := Statement.mem_queryCores.mp hc
    · exact List.mem_flatMap.mpr ⟨q, hq, hk⟩
    · have hkin : k ∈ View.armsOf s qs := List.mem_flatMap.mpr ⟨q, hq, hk⟩
      obtain ⟨hder, hcl, _⟩ := View.closed_core h hkin
      refine View.closed_queryCores h f q' ?_ c hc
      unfold Statement.nestedQueries at hq'
      rcases List.mem_append.mp hq' with hd | hsub
      · obtain ⟨i, hi, hkind⟩ := List.mem_filterMap.mp hd
        split at hkind
        · rename_i q'' v hkind'
          obtain rfl := Option.some.inj hkind
          exact hder i hi _ v hkind'
        · cases hkind
      · apply hcl
        unfold Core.subqueries at hsub
        simpa [View.clauseQueries] using List.mem_eraseDups.mp hsub

/-- A query readable alone names no item outside its cores. -/
theorem Statement.outerNames_readsAlone {s : Statement} (hwf : s.wellFormed = true) {q : Nat}
    (h : s.readsAlone q = true) : ∀ j, j ∉ s.outerNames q := by
  intro j hj
  obtain ⟨c, hc, hjc, e, _, hcore, a, ha, p, hap, hep⟩ := s.outerName_encloses hwf hj
  unfold Statement.readsAlone at h
  rcases Bool.or_eq_true_iff.mp h with h | h
  · have := List.all_eq_true.mp h a ha
    rw [hap] at this
    cases this
  · obtain ⟨first, hfirst, hclosed⟩ := List.any_eq_true.mp h
    have hfq := List.mem_range.mp hfirst
    let qs := (List.range (q + 1 - first)).map (first + ·)
    have hqs : q ∈ qs := List.mem_map.mpr ⟨q - first, List.mem_range.mpr (by omega), by omega⟩
    have hcin := View.closed_queryCores hclosed _ q hqs c hc
    obtain ⟨⟨j', col⟩, hp, rfl⟩ := List.mem_map.mp hjc
    have hinside := (View.closed_core hclosed hcin).2.2 _ hp
    simp only [View.inside, List.contains_iff_mem] at hinside
    obtain ⟨q'', hq'', he⟩ := List.mem_flatMap.mp hinside
    obtain ⟨x, hx, rfl⟩ := List.mem_map.mp hq''
    have hxlt := List.mem_range.mp hx
    rw [hcore] at he
    obtain ⟨qe, hlt, he'⟩ := s.encloses_after hwf hep ha hap
    have := s.wellFormed_armOf hwf he he'
    omega

/-- A derived item a lookup reads alone, of a well-formed statement, runs its
query under any binding as under the binding of no row. -/
theorem evalQuery_readable {s : Statement} (hwf : s.wellFormed = true) {i q : Nat} {v : Bool}
    (hi : i < s.items.length) (hkind : (s.item i).kind = .derived q v)
    (hr : (s.item i).readable = true) (Z : Db) (f : Nat) (b : Bind) :
    evalQuery s Z f b q = evalQuery s Z f Bind.empty q := by
  have halone : s.readsAlone q = true := by
    simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range] at hwf
    obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨_, _⟩, _⟩, hderived⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩ := hwf
    have := hderived i hi
    rw [hkind] at this
    simpa [hr] using this
  exact evalQuery_outer_congr hwf Z f q b Bind.empty fun j hj =>
    absurd hj (s.outerNames_readsAlone hwf halone j)

/-- A derived item a lookup reads alone runs its query under the binding of
no row, which holds a row of every outer item it names. -/
theorem outer_readable {s : Statement} (hwf : s.wellFormed = true) {i q : Nat} {v : Bool}
    (hi : i < s.items.length) (hkind : (s.item i).kind = .derived q v)
    (hr : (s.item i).readable = true) : s.Outer q Bind.empty := by
  have halone : s.readsAlone q = true := by
    simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range] at hwf
    obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨_, _⟩, _⟩, hderived⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩ := hwf
    have := hderived i hi
    rw [hkind] at this
    simpa [hr] using this
  exact fun j hj => absurd hj (s.outerNames_readsAlone hwf halone j)

/-! Bindings that matter. -/

/-- Every binding of an arm matters to a query whose every row matters: in
an arm that aggregates, the binding's own row holds its GROUP BY values. -/
theorem relevant_of_all {s : Statement} {rel : Nat → Row → Bool} {q k : Nat}
    (hall : ∀ r, rel q r = true) (E : Bind → Nat → List Row) (b : Bind) :
    relevant s rel q k E b = true := by
  unfold relevant
  split
  · unfold keyMatters
    refine Bool.or_eq_true_iff.mpr (Or.inr (decide_eq_true ⟨(s.core k).outRow E b, hall _,
      fun c n j c' hc hn => ?_⟩))
    simp [Statement.groupKey, List.getElem?_map, hn, Core.outRow_cell hc]
  · exact Bool.or_eq_true_iff.mpr (Or.inr (hall _))

/-- No derived item reads the statement's own query, so the planner's test
of it keeps every row. -/
theorem queryTest_root {s : Statement} (hwf : s.wellFormed = true) (p : Planned)
    (scopes : Nat → RScope) (rowsOf : Nat → List Row) (r : Row) :
    p.queryTest s scopes rowsOf s.root r = true := by
  simp only [Planned.queryTest, Bool.or_eq_true, List.all_eq_true, List.mem_range]
  refine Or.inr fun k _ d hd => ?_
  split
  · rename_i q v hkind
    have hq : q ≠ s.root := fun e => s.wellFormed_root hwf k (e ▸
      List.mem_append_left _ (List.mem_filterMap.mpr ⟨d, hd, by simp [hkind]⟩))
    simp [hq]
  · rfl

/-- A membership's scope reads only its partner's scope. -/
theorem membershipScope_congr {bound bound' : Nat → RScope} {m : Edge}
    (h : bound m.partner = bound' m.partner) :
    Planned.membershipScope bound m = Planned.membershipScope bound' m := by
  unfold Planned.membershipScope
  rw [h]

/-- A side's scope reads only its edges' partners' scopes. -/
theorem sideScope_congr {bound bound' : Nat → RScope} {side : Side}
    (h : ∀ e ∈ side.edges, bound e.partner = bound' e.partner) :
    Planned.sideScope bound side = Planned.sideScope bound' side := by
  have go : ∀ (l : List Edge) (init : RScope), (∀ e ∈ l, bound e.partner = bound' e.partner) →
      l.foldl (fun acc m => acc.both (.joined m.memberValue m.partner m.value (bound m.partner) m.nulls)) init =
        l.foldl (fun acc m => acc.both (.joined m.memberValue m.partner m.value (bound' m.partner) m.nulls))
          init := by
    intro l
    induction l with
    | nil => intro _ _; rfl
    | cons e es ih =>
      intro init hl
      simp only [List.foldl_cons]
      rw [hl e (by simp)]
      exact ih _ (fun e' he' => hl e' (List.mem_cons_of_mem _ he'))
  unfold Planned.sideScope
  exact go _ _ h

end SqliveNarrowing
