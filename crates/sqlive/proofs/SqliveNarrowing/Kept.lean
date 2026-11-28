import SqliveNarrowing.LimitTests
import SqliveNarrowing.Partners

/-! The planner's promise for the bindings that matter. Each item's test
(`planTest`) asks of its row what the item's scope asks, with the scopes of
the items placed before it, and each edge pushed into it. A binding a core
keeps that matters holds what the planned state says (`plan_semTests`): each
term holds, and each membership and side of a kept OR holds, with a partner
whose row sits in the binding or, for a subquery's source, in a binding of
the subquery's core under it. The planner places each partner before its
member (`plan_placedBefore`), so by induction on the order, the partner's row
passes its test, and so is a row of the lookup and passes the partner's scope
(`plan_kept_rows`). A pushed edge passes because the binding's row is one the
derived item's test keeps, and a row of an item a lookup reads alone is a row
of the lookup because the two databases hold the same rows that pass the
item's test. -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-- Whether binding `b` of the statement's own core `k` sorts at or before a
row of the core, which gives the LIMIT values `last` of the run, when the
run has them. -/
def Planned.AtOrBefore (s : Statement) (p : Planned) (last : Option (List (List Value))) (k : Nat)
    (E : Bind → Nat → List Row) (b : Bind) : Prop :=
  ∀ sorted values, p.sorted = some sorted → last = some values →
    ∃ (E' : Bind → Nat → List Row) (b' : Bind),
      values = sorted.runs.map (fun run => run.terms.map fun (_, _, pos) => ((s.core k).outRow E' b').cell pos) ∧
      keyLe ((s.query s.root).orderBy.map (·.2)) (Limit.orderKey (s.query s.root) ((s.core k).outRow E b))
        (Limit.orderKey (s.query s.root) ((s.core k).outRow E' b')) = true

/-! What an item's test asks. -/

theorem planTest_parts {s : Statement} {Y : Db} {last : Option (List (List Value))} {i : Nat} {r : Row}
    (h : planTest s Y last i r = true) :
    ((plan s).scopes last i).passes (lookupRows s Y) r = true ∧
      ((s.item i).readable = true → r ∈ lookupRows s Y i) := by
  unfold planTest Planned.itemTest Planned.scopeTest at h
  simp only [Bool.and_eq_true] at h
  refine ⟨h.1.1.1.1, fun hr => ?_⟩
  have := h.2
  rw [hr] at this
  simpa using this

/-- A placed item's final scope asks of a row what its terms and carried
terms say, and that each of its memberships' scopes passes it. -/
theorem scopes_parts {s : Statement} (last : Option (List (List Value))) (rowsOf : Nat → List Row)
    {d : Nat} {r : Row} (hd : d ∈ (plan s).order)
    (h : ((plan s).scopes last d).passes rowsOf r = true) :
    (∀ t, ((plan s).state.items d).term = some t → t.holds (some r) = true) ∧
      (∀ t, ((plan s).state.items d).carried = some t → t.holds (some r) = true) ∧
      ∀ m ∈ ((plan s).state.items d).memberships,
        (Planned.membershipScope ((plan s).scopes last) m).passes rowsOf r = true := by
  obtain ⟨bound, hbound, hbefore⟩ := (plan s).scopes_eq last (plan_order s).1 hd
  rw [hbound] at h
  obtain ⟨ht, hc, hm, _⟩ := scopeAt_parts h
  refine ⟨ht, hc, fun m hmm => ?_⟩
  rw [← membershipScope_congr (hbefore m.partner ((plan_placedBefore s d).1 m hmm))]
  exact hm m hmm

/-! Each query's test. -/

/-- No derived item reads a subquery, so its test keeps every row. -/
theorem queryTest_sub {s : Statement} (hwf : s.wellFormed = true) (p : Planned)
    (scopes inner : Nat → RScope)
    (rowsOf : Nat → List Row) {k q : Nat} (hq : q ∈ (s.core k).subqueries) (r : Row) :
    p.queryTest s scopes inner rowsOf q r = true := by
  simp only [Planned.queryTest, Bool.or_eq_true, List.all_eq_true, List.mem_range]
  refine Or.inr fun k' _ d hd => ?_
  split
  · rename_i q' v hkd
    by_cases hqq : q' = q
    · subst hqq
      exact absurd (s.subquery_not_derived hwf hq hd hkd) id
    · simp [hqq]
  · rfl

/-- A row that passes a derived item's scope and pushed edges passes the
test of its query. -/
theorem queryTest_of_scope {s : Statement} (hwf : s.wellFormed = true) {p : Planned}
    {scopes inner : Nat → RScope} {rowsOf : Nat → List Row} {k i q : Nat} {v : Bool}
    (hi : i ∈ (s.core k).items) (hkind : (s.item i).kind = .derived q v) {r : Row}
    (hr : p.scopeTest scopes inner rowsOf i r = true) :
    p.queryTest s scopes inner rowsOf q r = true := by
  simp only [Planned.queryTest, Bool.or_eq_true, List.all_eq_true, List.mem_range]
  refine Or.inr fun k' _ d hd => ?_
  split
  · rename_i q' v' hkd
    by_cases hq : q' = q
    · subst hq
      obtain rfl := s.derived_unique hwf hi hd hkind hkd
      simp [hr]
    · simp [hq]
  · rfl

/-! Bindings a core keeps. -/

/-- A binding a core keeps holds, outside the core's items, the binding its
items join onto. -/
theorem coreBindings_within {s : Statement} {Z : Db} {E : Bind → Nat → List Row} {o : Bind} {k : Nat}
    {b : Bind} (hb : b ∈ coreBindings s Z E o k) : s.Within k o b := by
  intro j hj
  exact joinItems_preserves (P := s.Within k o) (s.core k).items [s.outerOf k o]
    (fun b hb => by simp only [List.mem_singleton] at hb; subst hb; exact fun _ _ => rfl)
    (fun b j' x hb hj' j'' hj'' => by
      have hne : j'' ≠ j' := fun e => hj'' (e ▸ hj')
      simp only [Bind.set, hne, ↓reduceIte]
      exact hb j'' hj'')
    b (List.mem_filter.mp hb).1 j hj

/-! Pushed edges. -/

/-- A pushed edge's scope passes the target's row when the derived item's
edge's scope passes the derived row, whose columns the target's row holds in
the columns `rename` gives. -/
theorem membershipScope_pushed {bound : Nat → RScope} {rowsOf : Nat → List Row} {e0 e : Edge}
    {rename : Nat → Option Nat} {r0 r : Row}
    (h : e0.memberValue.passed e0.member e.member rename = some e.memberValue)
    (hp : e0.partner = e.partner) (hv : e0.value = e.value) (hn : e0.nulls = e.nulls)
    (hcols : ∀ c c', rename c = some c' → r0.cell c = cellOf (some r) c')
    (hpass : (Planned.membershipScope bound e0).passes rowsOf r0 = true) :
    (Planned.membershipScope bound e).passes rowsOf r = true := by
  have hsame : e.memberValue.eval r = e0.memberValue.eval r0 := by
    have := PushDown.value_passed_sound h hcols
    rwa [PValue.on_some, PValue.on_some] at this
  unfold Planned.membershipScope at hpass ⊢
  rw [← hp, ← hv, ← hn]
  cases h0 : e0.memberValue with
  | column c =>
    rw [h0] at hpass h hsame
    simp only [PValue.passed, Option.map_eq_some_iff] at h
    obtain ⟨c', _, hc'⟩ := h
    rw [← hc'] at hsame ⊢
    simp only [PValue.eval] at hsame
    cases hval : e0.value with
    | column y =>
      rw [hval] at hpass
      simp only at hpass ⊢
      cases hvs : RScope.equalValues y (bound e0.partner) <;> rw [hvs] at hpass <;>
        simpa [RScope.passes, PValue.eval, hsame] using hpass
    | expr x =>
      rw [hval] at hpass
      simpa [RScope.passes, PValue.eval, hsame] using hpass
  | expr m =>
    rw [h0] at hpass h hsame
    simp only [PValue.passed] at h
    split at h
    · cases h' : e.memberValue with
      | column c' => rw [h'] at h; cases h
      | expr m' =>
        rw [h'] at hsame
        cases hval : e0.value <;> rw [hval] at hpass <;> simp only at hpass ⊢ <;>
          simp only [RScope.passes] at hpass ⊢ <;> rw [hsame] <;> exact hpass
    · cases h

/-- The derived item a pushed edge comes from is an item its core lists. -/
theorem pushed_source_listed {s : Statement} (hwf : s.wellFormed = true) {d : Nat} {e0 : Edge}
    (he0 : e0 ∈ ((plan s).state.items d).memberships ∨ (e0 ∈ (plan s).state.pushed ∧ e0.member = d)) :
    d ∈ (s.core (s.item d).core).items := by
  rcases he0 with he0 | ⟨he0, hm0⟩
  · obtain ⟨pre, post, horder, _⟩ := (plan_placedBefore s d).1 e0 he0
    exact plan_order_listed hwf (by rw [horder]; simp)
  · rw [← hm0]
    exact (plan_lookups hwf).2.2.2.2 e0 he0

/-- No edge is pushed into an item of the statement's own core: no derived
item reads the statement's own query. -/
theorem pushed_not_root {s : Statement} (hwf : s.wellFormed = true) {e : Edge}
    (he : e ∈ (plan s).state.pushed) (hroot : (s.item e.member).core ∈ (s.query s.root).arms) : False := by
  obtain ⟨_, _, d, q0, v, k0, _, hd, _, _, hk0, _, hcore0, _, e0, he0, _⟩ := plan_pushed s he
  have hdl := pushed_source_listed hwf he0
  have hq0 : q0 ∈ s.nestedQueries (s.item d).core :=
    List.mem_append_left _ (List.mem_filterMap.mpr ⟨d, hdl, by simp [hd]⟩)
  rw [hcore0] at hroot
  have := s.wellFormed_armOf hwf hk0 hroot
  exact s.wellFormed_root hwf _ (this ▸ hq0)

/-- A pushed edge's scope passes its target's row in a binding that matters:
the binding holds, in the column its arm passes on, the value of a row the
derived item's test keeps, which the derived item's edge passes. -/
theorem pushed_passes {s : Statement} (hwf : s.wellFormed = true) {Y : Db}
    {last : Option (List (List Value))} {Z : Db} {g q k i : Nat} {b : Bind} {r : Row} {e : Edge}
    (hk : k ∈ (s.query q).arms) (hi : i ∈ (s.core k).items)
    (hrel : relevant s (planRel s Y last) q k (evalQuery s Z g) b = true)
    (hbi : b i = some r) (he : e ∈ (plan s).state.pushed) (hm : e.member = i) :
    (Planned.membershipScope ((plan s).scopes last) e).passes (lookupRows s Y) r = true := by
  obtain ⟨_, _, d, q0, v, k0, rename, hd, hlim, hoff, hk0, hwin, hcore0, hout, e0, he0, hpassed, hp,
    hv, hn⟩ := plan_pushed s he
  have hcorei : (s.item i).core = k := ((s.wellFormed_parts hwf).items k (s.core_lt hi) i hi).2
  rw [hm, hcorei] at hcore0
  subst hcore0
  obtain rfl := s.wellFormed_armOf hwf hk0 hk
  obtain ⟨r0, hr0, hcells⟩ := relevant_cells hrel hwin
  have hcols : ∀ c c', rename c = some c' → r0.cell c = cellOf (some r) c' := by
    intro c c' hc
    obtain ⟨hagg, hout'⟩ := hout c c' hc
    rw [hm] at hagg hout'
    rw [hcells c i c' hout' hagg, hbi]
  have htest := queryTest_scope (pushed_source_listed hwf he0) hd hlim hoff hr0
  simp only [Planned.scopeTest, Bool.and_eq_true, List.all_eq_true, Bool.or_eq_true, bne_iff_ne,
    ne_eq] at htest
  have h0 : (Planned.membershipScope ((plan s).scopes last) e0).passes (lookupRows s Y) r0 = true := by
    rcases he0 with he0 | ⟨he0, hm0⟩
    · obtain ⟨pre, post, horder, _⟩ := (plan_placedBefore s d).1 e0 he0
      exact (scopes_parts last _ (by rw [horder]; simp) htest.1.1.1).2.2 e0 he0
    · rcases htest.2 e0 he0 with h | h
      · exact absurd hm0 h
      · exact h
  exact membershipScope_pushed hpassed hp hv hn hcols h0

/-- A pushed OR's scope passes its target's row in a binding that matters: the
binding holds, in each column its arm passes on, the value of a row the
derived item's test keeps, which a side of the derived item's OR passes. -/
theorem pushedOr_passes {s : Statement} (hwf : s.wellFormed = true) {Y : Db}
    {last : Option (List (List Value))} {Z : Db} {g q k i : Nat} {b : Bind} {r : Row}
    {sides : List Side} (hk : k ∈ (s.query q).arms) (hi : i ∈ (s.core k).items)
    (hrel : relevant s (planRel s Y last) q k (evalQuery s Z g) b = true)
    (hbi : b i = some r) (he : (i, sides) ∈ (plan s).state.pushedOrs) {bound : Nat → RScope}
    (hbound : ∀ side ∈ sides, ∀ e ∈ side.edges, bound e.partner = (plan s).scopes last e.partner) :
    ∃ side ∈ sides, (Planned.sideScope bound side).passes (lookupRows s Y) r = true := by
  obtain ⟨d, q0, v, k0, rename, sides0, hd, hlim, hoff, hk0, hwin, hcore0, hne, hout, hdo, hs0,
    heq⟩ := plan_pushedOrs s (i, sides) he
  have hcorei : (s.item i).core = k := ((s.wellFormed_parts hwf).items k (s.core_lt hi) i hi).2
  simp only at hcore0 hout heq
  rw [hcorei] at hcore0
  subst hcore0
  obtain rfl := s.wellFormed_armOf hwf hk0 hk
  obtain ⟨r0, hr0, hcells⟩ := relevant_cells hrel hwin
  have hcols : ∀ c c', rename c = some c' → r0.cell c = cellOf (some r) c' := by
    intro c c' hc
    obtain ⟨hagg, hout'⟩ := hout c c' hc
    rw [hcells c i c' hout' hagg, hbi]
  have hdl := plan_order_listed hwf hdo
  have htest := queryTest_scope hdl hd hlim hoff hr0
  simp only [Planned.scopeTest, Bool.and_eq_true] at htest
  obtain ⟨boundd, hboundd, hbefore⟩ := (plan s).scopes_eq last (plan_order s).1 hdo
  have hscope := htest.1.1.1
  rw [hboundd] at hscope
  obtain ⟨side0, hside0, hpass0⟩ := (scopeAt_parts hscope).2.2.2 sides0 hs0 hne
  rw [sideScope_congr fun e he' => hbefore e.partner
    ((plan_placedBefore s d).2 sides0 hs0 side0 hside0 e he')] at hpass0
  obtain ⟨hterms, hedges⟩ := sideScope_parts hpass0
  have hmem : PushDown.sideInto s i rename side0 ∈ sides := by
    rw [heq]
    exact List.mem_map.mpr ⟨side0, hside0, rfl⟩
  refine ⟨_, hmem, sideScope_passes (fun t ht => ?_) (fun m hm => ?_)⟩
  · obtain ⟨t0, ht0, hrenamed⟩ := List.mem_filterMap.mp ht
    exact PushDown.renamed_sound hrenamed hcols (PushDown.judgedOn_sound hcols) (hterms t0 ht0)
  · rw [hbound _ hmem m hm]
    obtain ⟨e0, he0, value, hvalue, rfl⟩ := PushDown.mem_passed hm
    have hsame : value.eval r = e0.memberValue.eval r0 := by
      have := PushDown.value_passed_sound hvalue hcols
      rwa [PValue.on_some, PValue.on_some] at this
    have h0 := hedges e0 he0
    simp only [RScope.passes] at h0 ⊢
    rw [hsame]
    exact h0

/-! Partners. -/

/-- What `aroundSource` checks of the derived item and the item around it. -/
theorem aroundSource_facts {s : Statement} {d pc x xc : Nat} (h : aroundSource s d pc = some (x, xc)) :
    ∃ q v a, (s.item d).kind = .derived q v ∧ (s.query q).arms = [a] ∧
      (s.core a).aggregated = false ∧ (s.core a).windowed = false ∧ (s.core a).items = [] ∧
      d ∈ (s.core (s.item d).core).items ∧ (s.core a).output[pc]? = some (.expr (.col x xc)) ∧
      (s.item x).core ≠ (s.item d).core ∧ (s.item x).readable = true := by
  unfold aroundSource at h
  split at h
  · cases h
  · rename_i q v hkind
    split at h
    · rename_i a harms
      split at h
      · cases h
      · rename_i hok
        simp only [Bool.or_eq_true, Bool.not_eq_true', not_or, Bool.not_eq_true,
          List.isEmpty_eq_false_iff, ne_eq, Decidable.not_not] at hok
        split at h
        · rename_i x' xc' hout
          split at h
          · rename_i hx
            cases h
            exact ⟨q, v, a, hkind, harms, hok.1.1.1.2, hok.1.1.2, hok.1.2,
              by simpa using hok.2, hout, hx.2.1, hx.2.2⟩
          · cases h
        · cases h
    · cases h

/-- A row of a relation `d` of the row around it, a query its core runs
under the binding around the core, holds in column `pc` the value of the
column of the item around it that `aroundSource` names, in the row of that
item that each binding of the core holds. -/
theorem around_row {s : Statement} (hwf : s.wellFormed = true) {Z : Db} {g k d pc x xc : Nat}
    {o b : Bind} {rd : Row} {v : Int} (hps : aroundSource s d pc = some (x, xc))
    (hdk : (s.item d).core = k)
    (hrd : ∀ qd w, (s.item d).kind = .derived qd w → rd ∈ evalQuery s Z g (s.outerOf k o) qd)
    (hw : s.Within k o b) (hcell : rd.cell pc = some v) :
    ∃ rp, b x = some rp ∧ rp.cell xc = some v := by
  obtain ⟨qd, w, a, hkind, harms, _, _, hitems, _, hout, hxd, _⟩ := aroundSource_facts hps
  have hrow := hrd qd w hkind
  cases g with
  | zero => simp [evalQuery] at hrow
  | succ g' =>
    rcases query_row_at harms hout hrow with hnone | ⟨b', hb', hc, _⟩
    · simp [Row.cell, hnone] at hcell
    · simp only [Row.cell, hc, Option.join_some, Expr.eval] at hcell
      have hb'x : b' x = s.outerOf a (s.outerOf k o) x :=
        coreBindings_within hb' x (by rw [hitems]; simp)
      have hxk : x ∉ (s.core k).items := by
        intro hx
        have := ((s.wellFormed_parts hwf).items k (s.core_lt hx) x hx).2
        exact hxd (by rw [this, hdk])
      have hbx : b x = s.outerOf k o x := hw x hxk
      rw [hb'x] at hcell
      rw [hbx]
      generalize s.outerOf k o = ob at hcell ⊢
      unfold Statement.outerOf at hcell
      split at hcell
      · simp [Bind.empty, cellOf] at hcell
      · cases hox : ob x with
        | none => rw [hox] at hcell; simp [cellOf] at hcell
        | some rp =>
          rw [hox] at hcell
          exact ⟨rp, rfl, hcell⟩

/-- An edge from item `i`, holding of a binding with `i`'s row `r`, finds a
row of its partner's lookup in its partner's scope that gives `r`'s value,
when the partner's rows that the binding holds, or a binding of the
subquery's core under it holds, pass their test. -/
theorem partner_found {s : Statement} {Y : Db} {last : Option (List (List Value))} {Z : Db} {g i : Nat}
    {b : Bind} {r : Row} {e : Edge} (hm : e.member = i) (hbi : b i = some r) (hh : e.HoldsIn s Z g b)
    (hbnd : e.nested = false → e.via = none → s.alwaysBound e.partner = true → b e.partner ≠ none)
    (hplain : ∀ r', b e.partner = some r' → planTest s Y last e.partner r' = true)
    (hnested : e.nested = true → e.via = none → ∀ g', g = g' + 1 →
      ∀ b' ∈ coreBindings s Z (evalQuery s Z g') b (s.item e.partner).core,
      s.GivesRow Z g' b (s.item e.partner).core b' →
      ∀ r', b' e.partner = some r' → planTest s Y last e.partner r' = true)
    (hvia : ∀ d pc x, e.via = some (d, pc) → e.nested = true → e.memberValue.on (b e.member) = some x →
      (((s.item d).core = (s.item e.member).core ∧ ∃ rd, b d = some rd ∧ pc.eval rd = some x) ∨
        (s.item d).core ≠ (s.item e.member).core ∧
        ∃ g', g = g' + 1 ∧ ∃ b' ∈ coreBindings s Z (evalQuery s Z g') b (s.item d).core,
          s.GivesRow Z g' b (s.item d).core b' ∧ ∃ rd, b' d = some rd ∧ pc.eval rd = some x) →
      ∃ r', planTest s Y last e.partner r' = true ∧ e.value.eval r' = some x)
    (hviaBound : ∀ d pc, e.via = some (d, pc) → e.nested = true →
      (s.item d).core = (s.item e.member).core → s.alwaysBound d = true → b d ≠ none)
    (haround : ∀ d pc x, e.via = some (d, pc) → e.nested = false → e.memberValue.on (b e.member) = some x →
      (∃ rd, b d = some rd ∧ pc.eval rd = some x) → ∃ r', b e.partner = some r' ∧ e.value.eval r' = some x)
    (hread : (s.item e.partner).readable = true) :
    ∃ r', r' ∈ lookupRows s Y e.partner ∧
      ((plan s).scopes last e.partner).passes (lookupRows s Y) r' = true ∧
      e.value.eval r' = e.memberValue.eval r ∧ ((e.memberValue.eval r).isSome = true ∨ e.nulls = true) ∧
      (e.nested = false → b e.partner = some r') := by
  have hon : e.memberValue.on (b e.member) = e.memberValue.eval r := by rw [hm, hbi, PValue.on_some]
  have fin : ∀ r', planTest s Y last e.partner r' = true → e.value.eval r' = e.memberValue.eval r →
      ((e.memberValue.eval r).isSome = true ∨ e.nulls = true) →
      (e.nested = false → b e.partner = some r') →
      ∃ r', r' ∈ lookupRows s Y e.partner ∧
        ((plan s).scopes last e.partner).passes (lookupRows s Y) r' = true ∧
        e.value.eval r' = e.memberValue.eval r ∧
        ((e.memberValue.eval r).isSome = true ∨ e.nulls = true) ∧
        (e.nested = false → b e.partner = some r') := fun r' ht hv hn hb => by
    obtain ⟨h1, h2⟩ := planTest_parts ht
    exact ⟨r', h2 hread, h1, hv, hn, hb⟩
  rcases hh with ⟨x, hx, hrest⟩ | ⟨hnul, hnf, hvn, hab, hx, hval⟩ | ⟨_, hnf, hvn, hab, hp⟩ |
    ⟨d, pc, x, hv, hnt, hx, hrow⟩ | ⟨d, pc, x, hv, hnf, hx, hrow⟩
  · rw [hon] at hx
    cases hn : e.nested
    · simp only [hn, Bool.false_eq_true, ↓reduceIte] at hrest
      obtain ⟨r', hr', hv⟩ := hrest
      have := fin r' (hplain r' hr') (by rw [hv, hx]) (Or.inl (by rw [hx]; rfl)) (fun _ => hr')
      rw [hn] at this
      exact this
    · simp only [hn, ↓reduceIte] at hrest
      obtain ⟨hvn, g', hg, b', hb', hgr, r', hr', hv⟩ := hrest
      have := fin r' (hnested hn hvn g' hg b' hb' hgr r' hr') (by rw [hv, hx]) (Or.inl (by rw [hx]; rfl))
        (fun h => absurd h (by simp [hn]))
      rw [hn] at this
      exact this
  · obtain ⟨r', hr'⟩ := Option.ne_none_iff_exists'.mp (hbnd hnf hvn hab)
    rw [hon] at hx
    exact fin r' (hplain r' hr') (by rw [hval r' hr', hx]) (Or.inr hnul) (fun _ => hr')
  · exact absurd hp (hbnd hnf hvn hab)
  · have hrow' : ((s.item d).core = (s.item e.member).core ∧ ∃ rd, b d = some rd ∧ pc.eval rd = some x) ∨
        (s.item d).core ≠ (s.item e.member).core ∧
        ∃ g', g = g' + 1 ∧ ∃ b' ∈ coreBindings s Z (evalQuery s Z g') b (s.item d).core,
          s.GivesRow Z g' b (s.item d).core b' ∧ ∃ rd, b' d = some rd ∧ pc.eval rd = some x := by
      rcases hrow with hrow | hrow | ⟨hdc, hab, hbd⟩
      · exact Or.inl hrow
      · exact Or.inr hrow
      · exact absurd hbd (hviaBound d pc hv hnt hdc hab)
    obtain ⟨r', ht, hval⟩ := hvia d pc x hv hnt hx hrow'
    rw [hon] at hx
    exact fin r' ht (by rw [hval, hx]) (Or.inl (by rw [hx]; rfl)) (fun h => by rw [hnt] at h; cases h)
  · obtain ⟨r', hr', hval⟩ := haround d pc x hv hnf hx hrow
    rw [hon] at hx
    exact fin r' (hplain r' hr') (by rw [hval, hx]) (Or.inl (by rw [hx]; rfl)) (fun _ => hr')

/-! The induction on the order. -/

/-- What the restriction's tables and the earlier fuels give the induction:
a table row of `X` that passes an item's scope and pushed edges is a row of
`Y`, and the runs on `X` and `Y` return the same rows that matter at each fuel
up to `f`. -/
structure KeptBase (s : Statement) (X Y : Db) (last : Option (List (List Value))) (f : Nat) : Prop where
  wf : s.wellFormed = true
  tables : ∀ i t, s.table? i = some t → i < s.items.length →
    (X.rows t).filter ((plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y) i) =
      (Y.rows t).filter ((plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y) i)
  agree : ∀ g ≤ f, Agree s X Y (planTest s Y last) (planRel s Y last) g

/-- A row of `X` that passes its item's scope test is a row of `Y`. -/
theorem KeptBase.mem {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) {i t : Nat} (ht : s.table? i = some t) (hi : i < s.items.length)
    {r : Row} (hr : r ∈ X.rows t)
    (hst : (plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y) i r = true) : r ∈ Y.rows t := by
  have hmem := List.mem_filter.mpr ⟨hr, hst⟩
  rw [base.tables i t ht hi] at hmem
  exact (List.mem_filter.mp hmem).1

/-- A binding of a subquery's core, under a binding `b` of a core run on `X`
or `Y`, that gives a row the subquery returns passes the planner's windows,
when each binding of that core under `b`, in either database, that passes
its windows holds rows of the items placed before `i` that pass their
tests, and the subquery's source is one of those. -/
theorem nested_windowPasses {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) {Z : Db} (hZ : Z = X ∨ Z = Y) {pre : List Nat} {p g' k' : Nat}
    {b b' : Bind} (hp : p ∈ pre) (hpo : p ∈ (plan s).order) (hpk : (s.item p).core = k')
    (hsub : ∀ Z', (Z' = X ∨ Z' = Y) → ∀ b'' ∈ coreBindings s Z' (evalQuery s Z' g') b k',
      (plan s).windowPasses s Y k' b'' = true →
      ∀ j r', j ∈ pre → b'' j = some r' → planTest s Y last j r' = true)
    (hb' : b' ∈ coreBindings s Z (evalQuery s Z g') b k') (hgr : s.GivesRow Z g' b k' b') :
    (plan s).windowPasses s Y k' b' = true := by
  have hwf := base.wf
  refine givesRow_windowPasses hwf hb' hgr fun w hw hwk t ht => ?_
  obtain ⟨_, hf⟩ := plan_window_read hw
  have hpi : p = w.item := by
    have hpm : p ∈ (s.core k').items := hpk ▸ plan_order_listed hwf hpo
    rw [← hwk, hf.items] at hpm
    simpa using hpm
  rw [hpi] at hp
  refine ⟨(plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y) w.item, ?_,
    fun b'' hb'' hwin r hr => ?_⟩
  · rcases hZ with rfl | rfl
    · exact base.tables w.item t ht (s.wellFormed_items hwf (k := w.core) (by rw [hf.items]; simp))
    · rfl
  · have := hsub Y (Or.inr rfl) b'' hb'' hwin w.item r hp hr
    unfold planTest Planned.itemTest at this
    simp only [Bool.and_eq_true] at this
    exact this.1

/-- A derived item's query has no window: a window sorts a subquery's
item, and one place reads each query. -/
theorem windowPasses_derived {s : Statement} (hwf : s.wellFormed = true) {kd d qd a : Nat} {v : Bool}
    (hd : d ∈ (s.core kd).items) (hkind : (s.item d).kind = .derived qd v) (ha : a ∈ (s.query qd).arms)
    (Y : Db) (b : Bind) : (plan s).windowPasses s Y a b = true := by
  cases h : (plan s).windowPasses s Y a b with
  | true => rfl
  | false =>
    obtain ⟨w, hw, hwi⟩ := windowPasses_false h
    obtain ⟨_, hf⟩ := plan_window_read hw
    have h1 : w.item ∈ (s.core w.core).items := by rw [hf.items]; simp
    have hcore : w.core = a := by
      rw [← ((s.wellFormed_parts hwf).items _ (s.core_lt h1) _ h1).2,
        ((s.wellFormed_parts hwf).items _ (s.core_lt hwi) _ hwi).2]
    subst hcore
    have hq : w.query = qd := s.wellFormed_armOf hwf (by rw [hf.arms]; simp) ha
    obtain ⟨p, _, hp⟩ := hf.parent
    rw [hq] at hp
    exact (s.subquery_not_derived hwf hp hd hkind).elim

/-- An edge through a derived item reads through an item its core lists. -/
theorem Edge.Passed.listed {s : Statement} {e : Edge} {d : Nat} {dv : PValue} (h : e.Passed s d dv) :
    d ∈ (s.core (s.item d).core).items := by
  obtain ⟨_, _, rename, ⟨c, c', hc⟩, hrename, _⟩ := h
  obtain ⟨_, _, _, _, _, _, _, _, hd, _, _, _⟩ := Passed.passedSource_facts (hrename c c' hc)
  exact hd

/-- The source a derived item's arm passes a column on from is an item of
the arm's core. -/
theorem passedSource_listed {s : Statement} (hwf : s.wellFormed = true) {d pc x xc : Nat}
    (h : passedSource s d pc = some (x, xc)) : x ∈ (s.core (s.item x).core).items := by
  obtain ⟨q, _, a, _, harms, _, _, _, _, hout, hcore, _⟩ := Passed.passedSource_facts h
  have hk : a ∈ (s.query q).arms := by rw [harms]; simp
  have hlt := ((s.wellFormed_parts hwf).arms q (s.query_lt hk) a hk).1
  have href : (x, xc) ∈ (s.core a).refs := by
    unfold Core.refs
    refine List.mem_append_left _ (List.mem_append_right _ (List.mem_flatMap.mpr
      ⟨.expr (.col x xc), List.mem_of_getElem? hout, ?_⟩))
    simp [OutCol.refs, OutCol.exprs, Expr.refs]
  obtain ⟨k', _, hx⟩ := (s.wellFormed_parts hwf).refs a hlt _ href
  rw [((s.wellFormed_parts hwf).items k' (s.core_lt hx) x hx).2]
  exact hx

/-- A row of a derived item whose value `dv` is `x` gives, for an edge
through the item (`Edge.Passed`), a row of the edge's partner that passes
its test and whose value is `x`, when the row is one the derived item's
query returns under `ob`, the row passes the item's test, and the partner's
rows of each binding of the arm under `ob` that matters pass their tests. -/
theorem derived_row_source_passes {s : Statement} (hwf : s.wellFormed = true) {Y : Db}
    {last : Option (List (List Value))} {Z : Db} {g kd d : Nat} {e : Edge} {dv : PValue} {ob : Bind}
    {rd : Row} {x : Int} (hd : d ∈ (s.core kd).items)
    (hrd : ∀ qd v, (s.item d).kind = .derived qd v → rd ∈ evalQuery s Z g ob qd)
    (hcell : dv.eval rd = some x) (hpassed : e.Passed s d dv) (htest : planTest s Y last d rd = true)
    (hA : ∀ qd v a, (s.item d).kind = .derived qd v → a ∈ (s.query qd).arms → ∀ g', g = g' + 1 →
      ∀ b'' ∈ coreBindings s Z (evalQuery s Z g') ob a,
      relevant s (planRel s Y last) qd a (evalQuery s Z g') b'' = true →
      ∀ rp, b'' e.partner = some rp → planTest s Y last e.partner rp = true) :
    ∃ rp, planTest s Y last e.partner rp = true ∧ e.value.eval rp = some x := by
  obtain ⟨_, hnone, rename, ⟨c0, c0', hc0⟩, hrename, hvalue⟩ := hpassed
  obtain ⟨qd, v, a, hkind, harms, _, hagg, hwin, _, _, _, _⟩ :=
    Passed.passedSource_facts (hrename c0 c0' hc0)
  have hrow := hrd qd v hkind
  cases g with
  | zero => simp [evalQuery] at hrow
  | succ g' =>
    obtain ⟨b'', hb'', hrdb⟩ := query_row_plain harms hagg hwin hrow
    -- Each column the value names holds the partner's column in `b''`.
    have hcells : ∀ c c', rename c = some c' → rd.cell c = cellOf (b'' e.partner) c' := by
      intro c c' hc
      obtain ⟨qd', v', a', hkind', harms', _, _, _, _, hout, _, _⟩ :=
        Passed.passedSource_facts (hrename c c' hc)
      rw [hkind] at hkind'
      cases hkind'
      rw [harms] at harms'
      cases harms'
      rw [hrdb, Core.outRow_cell hout]
      rfl
    have hval := hvalue rd (b'' e.partner) hcells
    rw [PValue.on_some, hcell] at hval
    cases hp : b'' e.partner with
    | none =>
      rcases hnone with hnone | hab
      · rw [hp, hnone] at hval; cases hval
      · -- A binding of the arm holds a row of each item no LEFT JOIN adds.
        have hx := passedSource_listed hwf (hrename c0 c0' hc0)
        obtain ⟨qd', v', a', hkind', harms', _, _, _, _, _, hxc, _⟩ :=
          Passed.passedSource_facts (hrename c0 c0' hc0)
        rw [hkind] at hkind'
        cases hkind'
        rw [harms] at harms'
        cases harms'
        rw [hxc] at hx
        exact absurd hp (joinItems_bound _ _ ((s.wellFormed_parts hwf).nodup a (s.core_lt hx)) b''
          (List.mem_filter.mp hb'').1 e.partner hx hab)
    | some rp =>
      rw [hp, PValue.on_some] at hval
      refine ⟨rp, hA qd v a hkind (by rw [harms]; simp) g' rfl b'' hb'' ?_ rp hp, hval⟩
      have hst : (plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y) d rd = true := by
        unfold planTest Planned.itemTest at htest
        simp only [Bool.and_eq_true] at htest
        exact htest.1
      unfold relevant
      simp only [hwin, hagg, Bool.false_or, Bool.false_eq_true, ↓reduceIte]
      rw [hrdb] at hst
      exact queryTest_of_scope hwf hd hkind hst

/-- A row of a derived item, in a binding a core keeps, whose value `dv` is
`x` gives, for an edge through the item, a row of the edge's partner that
passes its test and whose value is `x`, when the row passes the item's test
and the partner's rows of each binding of the arm that matters pass their
tests. -/
theorem derived_source_passes {s : Statement} (hwf : s.wellFormed = true) {Y : Db}
    {last : Option (List (List Value))} {Z : Db} {g kd d : Nat} {e : Edge} {dv : PValue} {od bd : Bind}
    {rd : Row} {x : Int}
    (hbd : bd ∈ coreBindings s Z (evalQuery s Z g) od kd) (hd : d ∈ (s.core kd).items)
    (hrd : bd d = some rd) (hcell : dv.eval rd = some x)
    (hpassed : e.Passed s d dv) (htest : planTest s Y last d rd = true)
    (hA : ∀ qd v a, (s.item d).kind = .derived qd v → a ∈ (s.query qd).arms → ∀ g', g = g' + 1 →
      ∀ b'' ∈ coreBindings s Z (evalQuery s Z g') (s.outerOf kd od) a,
      relevant s (planRel s Y last) qd a (evalQuery s Z g') b'' = true →
      ∀ rp, b'' e.partner = some rp → planTest s Y last e.partner rp = true) :
    ∃ rp, planTest s Y last e.partner rp = true ∧ e.value.eval rp = some x :=
  derived_row_source_passes hwf hd (fun _ _ hkind => s.derived_row hwf hbd hd hkind hrd) hcell hpassed
    htest hA

/-- An item's row in a binding that matters, of a core run on `X` or `Y` at a
fuel up to `f`, passes its test, when the rows of the items placed before it
pass theirs in this binding, in each binding of a subquery's core under it,
and in each binding of a derived item's arm, of the core or of a subquery's
core, under the binding that arm reads. -/
theorem item_passes {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) {Z : Db} (hZ : Z = X ∨ Z = Y) {g q k : Nat} {o b : Bind}
    (hg : g ≤ f) (hq : q ≤ g) (ho : s.Outer q o) (hk : k ∈ (s.query q).arms)
    (hb : b ∈ coreBindings s Z (evalQuery s Z g) o k)
    (hrel : relevant s (planRel s Y last) q k (evalQuery s Z g) b = true)
    (hroot : q = s.root → (plan s).AtOrBefore s last k (evalQuery s Z g) b)
    (hwin : (plan s).windowPasses s Y k b = true)
    {pre post : List Nat} {i : Nat} (horder : (plan s).order = pre ++ i :: post)
    (hi : i ∈ (s.core k).items) {r : Row} (hbi : b i = some r)
    (hpre : ∀ j r', j ∈ pre → b j = some r' → planTest s Y last j r' = true)
    (hsub : ∀ Z', (Z' = X ∨ Z' = Y) → ∀ q' ∈ (s.core k).subqueries, ∀ g', g = g' + 1 →
      ∀ k' ∈ (s.query q').arms, ∀ b' ∈ coreBindings s Z' (evalQuery s Z' g') b k',
        (plan s).windowPasses s Y k' b' = true →
        ∀ j r', j ∈ pre → b' j = some r' → planTest s Y last j r' = true)
    (hder : ∀ Z', (Z' = X ∨ Z' = Y) → ∀ d ∈ (s.core k).items, ∀ qd v, (s.item d).kind = .derived qd v →
      ∀ g', g = g' + 1 → ∀ a ∈ (s.query qd).arms,
      ∀ b'' ∈ coreBindings s Z' (evalQuery s Z' g') (s.outerOf k o) a,
        relevant s (planRel s Y last) qd a (evalQuery s Z' g') b'' = true →
        ∀ j r', j ∈ pre → b'' j = some r' → planTest s Y last j r' = true)
    (hsubder : ∀ Z', (Z' = X ∨ Z' = Y) → ∀ q' ∈ (s.core k).subqueries, ∀ k' ∈ (s.query q').arms,
      ∀ d ∈ (s.core k').items, ∀ qd v, (s.item d).kind = .derived qd v →
      ∀ g', g = g' + 2 → ∀ a ∈ (s.query qd).arms,
      ∀ b'' ∈ coreBindings s Z' (evalQuery s Z' g') (s.outerOf k' b) a,
        relevant s (planRel s Y last) qd a (evalQuery s Z' g') b'' = true →
        ∀ j r', j ∈ pre → b'' j = some r' → planTest s Y last j r' = true) :
    planTest s Y last i r = true := by
  have hwf := base.wf
  have hS := plan_semTests hwf last (lookupRows s Y)
  have hklt := s.core_lt hi
  have hcorei : (s.item i).core = k := ((s.wellFormed_parts hwf).items k hklt i hi).2
  have hcls0 : endCls s (planRel s Y last) Z g k b :=
    ⟨⟨q, o, hk, ho, hb⟩, fun q' hq' => by obtain rfl := s.wellFormed_armOf hwf hq' hk; exact hrel⟩
  have hcls : endCls s (planRel s Y last) Z g (s.item i).core b := by rw [hcorei]; exact hcls0
  have hbi' : b i ≠ none := by rw [hbi]; simp
  have hnd := (plan_order s).1
  have hio : i ∈ (plan s).order := by rw [horder]; simp
  obtain ⟨bound, hbound, hbefore⟩ := (plan s).scopes_eq last hnd hio
  have hinpre : ∀ j, (∃ pre' post', (plan s).order = pre' ++ i :: post' ∧ j ∈ pre') → j ∈ pre := by
    rintro j ⟨pre', post', horder', hj⟩
    have hnd' := hnd
    rw [horder] at hnd'
    rw [split_unique hnd' (horder.symm.trans horder')]
    exact hj
  have hbefore' : ∀ j ∈ pre, bound j = (plan s).scopes last j :=
    fun j hj => hbefore j ⟨pre, post, horder, hj⟩
  -- An edge from `i` to a partner placed before it finds the partner's row.
  have hpartner : ∀ e : Edge, e.member = i → e.partner ∈ pre →
      (∀ d pc, e.via = some (d, pc) → e.nested = true → d ∈ pre) →
      e.HoldsIn s Z g b →
      (e.nested = false → e.via = none → s.alwaysBound e.partner = true → b e.partner ≠ none) →
      e.NestedIn s (s.core k).subqueries →
      (s.item e.partner).readable = true →
      ∃ r', r' ∈ lookupRows s Y e.partner ∧
        ((plan s).scopes last e.partner).passes (lookupRows s Y) r' = true ∧
        e.value.eval r' = e.memberValue.eval r ∧
        ((e.memberValue.eval r).isSome = true ∨ e.nulls = true) ∧
        (e.nested = false → b e.partner = some r') := by
    intro e hm hp hvp hh hbnd hnest hread
    refine partner_found hm hbi hh hbnd (fun r' hr' => hpre _ r' hp hr')
      (fun hn hvn g' hg' b' hb' hgr r' hr' => ?_) (fun d pc x hv hnt hx hrow => ?_)
      (fun d pc hv hnt hdc hab => ?_) (fun d pc x hv hnf _ hrow => ?_) hread
    · obtain ⟨q', hq', harm⟩ := hnest.1 hn hvn
      exact hsub Z hZ q' hq' g' hg' _ harm b' hb'
        (nested_windowPasses base hZ hp (by rw [horder]; exact List.mem_append_left _ hp) rfl
          (fun Z' hZ' => hsub Z' hZ' q' hq' g' hg' _ harm) hb' hgr) _ r' hp hr'
    · -- An edge through a derived item finds the row of the source its arm
      -- passes the column on from.
      obtain ⟨hps, hloc⟩ := (hnest.2 d pc hv).1 hnt
      have hdp := hvp d pc hv hnt
      have hdo : d ∈ (plan s).order := by rw [horder]; exact List.mem_append_left _ hdp
      have hdl := hps.listed
      rw [hm, hcorei] at hrow hloc
      rcases hrow with ⟨hdk, rd, hrd, hcell⟩ | ⟨hdk, g', hg', b', hb', hgr, rd, hrd, hcell⟩
      · rw [hdk] at hdl
        exact derived_source_passes hwf hb hdl hrd hcell hps (hpre d rd hdp hrd)
          fun qd v a hkind ha g' hg' b'' hb'' hrel' rp hrp =>
            hder Z hZ d hdl qd v hkind g' hg' a ha b'' hb'' hrel' _ rp hp hrp
      · obtain ⟨q', hq', harm⟩ := hloc.resolve_left hdk
        have hwd := nested_windowPasses base hZ hdp hdo rfl
          (fun Z' hZ' => hsub Z' hZ' q' hq' g' hg' _ harm) hb' hgr
        have htd := hsub Z hZ q' hq' g' hg' _ harm b' hb' hwd d rd hdp hrd
        exact derived_source_passes hwf hb' hdl hrd hcell hps htd
          fun qd v a hkind ha g'' hg'' b'' hb'' hrel' rp hrp =>
            hsubder Z hZ q' hq' _ harm d hdl qd v hkind g'' (by omega) a ha b'' hb'' hrel' _ rp hp hrp
    · -- A binding the core keeps holds a row of `d`, an item of the core that
      -- no LEFT JOIN adds.
      obtain ⟨hps, _⟩ := (hnest.2 d pc hv).1 hnt
      have hdl := hps.listed
      rw [hdc, hm, hcorei] at hdl
      exact joinItems_bound _ _ ((s.wellFormed_parts hwf).nodup k hklt) b (List.mem_filter.mp hb).1 d
        hdl hab
    · -- An edge around a relation of the row around it finds the row of the
      -- item around it that the binding holds.
      obtain ⟨⟨_, pc', xc, rfl, hps, hval⟩, hdc⟩ := (hnest.2 d pc hv).2 hnf
      obtain ⟨rd, hrd, hcell⟩ := hrow
      obtain ⟨_, _, _, _, _, _, _, _, hdl, _, _, _⟩ := aroundSource_facts hps
      rw [hm, hcorei] at hdc
      rw [hdc] at hdl
      obtain ⟨rp, hrp, hcp⟩ := around_row hwf hps hdc
        (fun _ _ hkind => s.derived_row hwf hb hdl hkind hrd) (coreBindings_within hb) hcell
      exact ⟨rp, hrp, by rw [hval, hcp]⟩
  -- Each membership and each side of a kept OR reads through derived items
  -- placed before the item.
  have hvm : ∀ m ∈ ((plan s).state.items i).memberships, ∀ d pc, m.via = some (d, pc) →
      m.nested = true → d ∈ pre :=
    fun m hmm d pc hv hn => by
      obtain ⟨pre', post', h', hb'⟩ := (plan_before s i).1 m hmm
      exact hinpre d ⟨pre', post', h', hb'.2 d pc hv hn⟩
  have hvs : ∀ sides ∈ ((plan s).state.items i).either, ∀ side ∈ sides, ∀ e ∈ side.edges,
      ∀ d pc, e.via = some (d, pc) → e.nested = true → d ∈ pre :=
    fun sides hs side hside e he d pc hv hn => by
      obtain ⟨pre', post', h', hb'⟩ := (plan_before s i).2 sides hs side hside e he
      exact hinpre d ⟨pre', post', h', hb'.2 d pc hv hn⟩
  -- A membership the planner found in the item's core finds the partner's row.
  have hinfo : ∀ m ∈ ((plan s).state.items i).memberships, m.pushed = false →
      ∃ r', r' ∈ lookupRows s Y m.partner ∧
        ((plan s).scopes last m.partner).passes (lookupRows s Y) r' = true ∧
        m.value.eval r' = m.memberValue.eval r ∧
        ((m.memberValue.eval r).isSome = true ∨ m.nulls = true) ∧
        (m.nested = false → b m.partner = some r') := by
    intro m hmm hpf
    have hme := plan_membersEdges s i m hmm
    have hmi := ((plan_nestedOK s).2.1 i m hmm).1
    have hp := hinpre _ ((plan_placedBefore s i).1 m hmm)
    rcases hS.edges Z g b m hme (by rw [hmi]; exact hcls) (by rw [hmi]; exact hbi') with
      hpush | ⟨hh, _, hbnd, hnest, _⟩
    · have := (plan_pushed s hpush).2.1
      rw [hpf] at this
      cases this
    · rw [hmi, hcorei] at hnest
      exact hpartner m hmi hp (hvm m hmm) hh hbnd hnest ((plan_lookups hwf).1 m hme)
  -- The item's scope.
  have hscope : ((plan s).scopes last i).passes (lookupRows s Y) r = true := by
    rw [hbound]
    refine scopeAt_passes (hS.terms Z g b i r hcls hbi) (hS.carried Z g b i r hcls hbi)
      (fun m hmm => ?_) (fun q e0 he0 hq hn0 hp0 _ => ?_) (fun sides hsides => ?_)
      (fun sorted values found hs hl hf => ?_)
    · have hme := plan_membersEdges s i m hmm
      have hmi := ((plan_nestedOK s).2.1 i m hmm).1
      have hp := hinpre _ ((plan_placedBefore s i).1 m hmm)
      rw [membershipScope_congr (hbefore' _ hp)]
      rcases hS.edges Z g b m hme (by rw [hmi]; exact hcls) (by rw [hmi]; exact hbi') with
        hpush | ⟨hh, _, hbnd, hnest, _⟩
      · exact pushed_passes hwf hk hi hrel hbi hpush hmi
      · rw [hmi, hcorei] at hnest
        obtain ⟨r', hr', hsc, hv, hn, _⟩ := hpartner m hmi hp (hvm m hmm) hh hbnd hnest
          ((plan_lookups hwf).1 m hme)
        exact membershipScope_passes hr' hsc hv hn
    · subst hq
      have hp := hinpre _ ((plan_placedBefore s i).1 e0 he0)
      obtain ⟨r0, hr0, hsc0, _, _, hb0⟩ := hinfo e0 he0 hp0
      have hbq := hb0 hn0
      refine ⟨r0, hr0, by rw [hbefore' _ hp]; exact hsc0, fun e he hqe hne hpe _ => ?_⟩
      obtain ⟨r1, _, _, hv1, hn1, hb1⟩ := hinfo e he hpe
      have := hb1 hne
      rw [hqe, hbq] at this
      obtain rfl := Option.some.inj this
      exact ⟨hv1, hn1⟩
    · have hors := (plan_lookups hwf).2.2.1 i sides hsides
      rcases hS.ors Z g b i hcls hbi' sides hors with
        hpushed | ⟨⟨side, hside, hterms, hedges⟩, _, hsd⟩
      · exact pushedOr_passes hwf hk hi hrel hbi hpushed fun side hside e he =>
          hbefore' _ (hinpre _ ((plan_placedBefore s i).2 sides hsides side hside e he))
      refine ⟨side, hside, sideScope_passes (fun t ht => by have := hterms t ht; rwa [hbi] at this)
        fun e he => ?_⟩
      obtain ⟨hmem, _, hbnd, hnest⟩ := hsd side hside e he
      have hp := hinpre _ ((plan_placedBefore s i).2 sides hsides side hside e he)
      rw [hbefore' _ hp]
      rw [hcorei] at hnest
      obtain ⟨r', hr', hsc, hv, hn, _⟩ := hpartner e hmem hp (hvs sides hsides side hside e he)
        (hedges e he) hbnd hnest ((plan_lookups hwf).2.1 i sides hors side hside e he)
      exact joined_passes_of hr' hsc hv hn
    · -- The LIMIT bound bounds only items of the statement's own core.
      obtain ⟨⟨run, _⟩, hpr, hri⟩ := Limit.runBound_item hf
      have hrun : run ∈ sorted.runs := (List.of_mem_zip hpr).1
      obtain ⟨k0, harms, hrk, _⟩ := Limit.run_facts hwf hs hrun
      have hrcore := ((s.wellFormed_parts hwf).items k0 (s.core_lt hrk) _ hrk).2
      simp only at hri
      rw [hri, hcorei] at hrcore
      subst hrcore
      have hkroot : k ∈ (s.query s.root).arms := by rw [harms]; simp
      obtain rfl := s.wellFormed_armOf hwf hk hkroot
      obtain ⟨E', b', rfl, hle⟩ := hroot rfl sorted values hs hl
      have hrows : ∀ run ∈ sorted.runs, b run.item = some ((b run.item).getD default) := by
        intro run hrun
        obtain ⟨k1, harms1, hrk1, hleft1⟩ := Limit.run_facts hwf hs hrun
        rw [harms, List.cons.injEq] at harms1
        rw [← harms1.1] at hrk1
        obtain ⟨x, hx⟩ := Option.ne_none_iff_exists'.mp (hS.rows Z g k b hcls0 run.item hrk1 hleft1)
        rw [hx]
        rfl
      have := Planned.bound_passes hwf hs hkroot hle (bound := bound) (rowsOf := lookupRows s Y)
        (row := fun j => (b j).getD default) hrows (Z := Z) (f := g) (i := i) (fun m hmm hrunm => ?_) hf
      · simpa [hbi] using this
      · have hme := plan_membersEdges s i m hmm
        have hmi := ((plan_nestedOK s).2.1 i m hmm).1
        have hp := hinpre _ ((plan_placedBefore s i).1 m hmm)
        rcases hS.edges Z g b m hme (by rw [hmi]; exact hcls) (by rw [hmi]; exact hbi') with
          hpush | ⟨hh, _, _, hnest, _⟩
        · exact (pushed_not_root hwf hpush (by rw [hmi, hcorei]; exact hkroot)).elim
        · obtain ⟨runm, hrunm, hitem⟩ := hrunm
          have hbp := hrows runm hrunm
          rw [hitem] at hbp
          obtain ⟨h1, h2⟩ := planTest_parts (hpre _ _ hp hbp)
          refine ⟨hh, h2 ((plan_lookups hwf).1 m hme), by rw [hbefore' _ hp]; exact h1,
            fun d pc x hv hnf _ hrow => ?_⟩
          -- A membership around a relation of the row around it takes the
          -- value of the row of the item around it that the binding holds.
          obtain ⟨⟨_, pc', xc, rfl, hps, hval⟩, hdc⟩ := (hnest.2 d pc hv).2 hnf
          obtain ⟨rd, hrd, hcell⟩ := hrow
          obtain ⟨_, _, _, _, _, _, _, _, hdl, _, _, _⟩ := aroundSource_facts hps
          rw [hmi, hcorei] at hdc
          rw [hdc] at hdl
          obtain ⟨rp, hrp, hcp⟩ := around_row hwf hps hdc
            (fun _ _ hkind => s.derived_row hwf hb hdl hkind hrd) (coreBindings_within hb) hcell
          simp only [hrp, Option.getD_some]
          rw [hval, hcp]
  -- Each edge pushed into the item.
  have hwini : ((plan s).windowScope i).passes (lookupRows s Y) r = true := by
    have := List.all_eq_true.mp hwin i hi
    simpa [hbi] using this
  have hst : (plan s).scopeTest ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y) i r = true := by
    unfold Planned.scopeTest
    rw [hscope, hwini, inner_passes hwf hk hi hrel hbi, Bool.true_and, Bool.true_and,
      Bool.true_and]
    refine List.all_eq_true.mpr fun e he => ?_
    by_cases hm : e.member = i
    · simp [pushed_passes hwf hk hi hrel hbi he hm]
    · simp [hm]
  -- A row of an item a lookup reads alone is a row of the lookup.
  have hlook : (s.item i).readable = true → r ∈ lookupRows s Y i := by
    intro hread
    have hilt : i < s.items.length := s.wellFormed_items hwf hi
    have hjoin := joinItems_rowOf _ _ ((s.wellFormed_parts hwf).nodup k hklt) b
      (List.mem_filter.mp hb).1 i hi r hbi
    cases hkind : (s.item i).kind with
    | table t =>
      have hr : r ∈ Z.rows t := by simpa [hkind] using hjoin
      have hL : lookupRows s Y i = Y.rows t := by simp [lookupRows, hkind]
      rw [hL]
      rcases hZ with hZ | hZ <;> subst hZ
      · exact base.mem (by simp [Statement.table?, hkind]) hilt hr hst
      · exact hr
    | derived qi v =>
      have hr := s.derived_row hwf hb hi hkind hbi
      rw [evalQuery_readable hwf hilt hkind hread] at hr
      have hqi : qi ∈ s.nestedQueries k :=
        List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩)
      have hqlt := s.wellFormed_nested hwf (s.query_lt hk) hk hqi
      have hqlen : qi < s.queries.length := Nat.lt_trans hqlt (s.query_lt hk)
      have hL : lookupRows s Y i = evalQuery s Y g Bind.empty qi := by
        simp only [lookupRows, hkind]
        rw [evalQuery_fuel hwf Y qi hqlen g (by omega),
          evalQuery_fuel hwf Y qi hqlen (s.queries.length + 1) (by omega)]
      rw [hL]
      rcases hZ with hZ | hZ <;> subst hZ
      · have hne : qi ≠ s.root := fun e => s.wellFormed_root hwf k (e ▸ hqi)
        have hagree := base.agree g hg qi (by omega) hne Bind.empty
          (outer_readable hwf hilt hkind hread) (Passes.empty _)
        have hmem : r ∈ (evalQuery s _ g Bind.empty qi).filter (planRel s Y last qi) :=
          List.mem_filter.mpr ⟨hr, queryTest_of_scope hwf hi hkind hst⟩
        rw [hagree] at hmem
        exact (List.mem_filter.mp hmem).1
      · exact hr
  unfold planTest Planned.itemTest
  rw [hst, Bool.true_and]
  cases hread : (s.item i).readable
  · rfl
  · simp [hlook hread]

/-- The rows of the items the order places first `n`, in a binding that
matters of a core run on `X` or `Y` at a fuel up to `f`, under a binding
around it whose rows of those items pass their tests, pass their tests. -/
theorem plan_kept_rows {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) :
    ∀ n, ∀ Z, (Z = X ∨ Z = Y) → ∀ g ≤ f, ∀ q ≤ g, ∀ o, s.Outer q o →
      (∀ j r, j ∈ (plan s).order.take n → o j = some r → planTest s Y last j r = true) →
      ∀ k ∈ (s.query q).arms, ∀ b ∈ coreBindings s Z (evalQuery s Z g) o k,
      relevant s (planRel s Y last) q k (evalQuery s Z g) b = true →
      (q = s.root → (plan s).AtOrBefore s last k (evalQuery s Z g) b) →
      (plan s).windowPasses s Y k b = true →
      ∀ j r, j ∈ (plan s).order.take n → b j = some r → planTest s Y last j r = true := by
  have hwf := base.wf
  intro n
  induction n with
  | zero => intro _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ j r hj; simp at hj
  | succ n ih =>
    intro Z hZ g hg q hq o ho hoG k hk b hb hrel hroot hwin j r hj hbj
    have hoG' : ∀ j r, j ∈ (plan s).order.take n → o j = some r → planTest s Y last j r = true :=
      fun j r hj => hoG j r (by rw [List.take_add_one]; exact List.mem_append_left _ hj)
    have hbn := ih Z hZ g hg q hq o ho hoG' k hk b hb hrel hroot hwin
    have hj' := hj
    rw [List.take_add_one] at hj'
    rcases List.mem_append.mp hj' with hj' | hj'
    · exact hbn j r hj' hbj
    · obtain ⟨hlt, rfl⟩ : ∃ h : n < (plan s).order.length, j = (plan s).order[n] := by
        cases hget : (plan s).order[n]? with
        | none => rw [hget] at hj'; cases hj'
        | some x =>
          rw [hget] at hj'
          simp only [Option.toList_some, List.mem_singleton] at hj'
          subst hj'
          obtain ⟨h, hx⟩ := List.getElem?_eq_some_iff.mp hget
          exact ⟨h, hx.symm⟩
      by_cases hik : (plan s).order[n] ∈ (s.core k).items
      · have horder : (plan s).order =
            (plan s).order.take n ++ (plan s).order[n] :: (plan s).order.drop (n + 1) := by
          rw [← List.drop_eq_getElem_cons hlt, List.take_append_drop]
        have hbd : s.BoundIn (s.core k).items b := fun j hj hab =>
          joinItems_bound _ _ ((s.wellFormed_parts hwf).nodup k (s.core_lt hik)) b
            (List.mem_filter.mp hb).1 j hj hab
        -- The binding a core's items join onto holds rows that pass their
        -- tests where the binding around the core does.
        have houterOf : ∀ (k0 : Nat) (o0 : Bind),
            (∀ j r, j ∈ (plan s).order.take n → o0 j = some r → planTest s Y last j r = true) →
            ∀ j r, j ∈ (plan s).order.take n → s.outerOf k0 o0 j = some r → planTest s Y last j r = true := by
          intro k0 o0 h j r hj hr
          unfold Statement.outerOf at hr
          split at hr
          · simp [Bind.empty] at hr
          · exact h j r hj hr
        refine item_passes base hZ hg hq ho hk hb hrel hroot hwin horder hik hbj hbn ?_ ?_ ?_
        · intro Z' hZ' q' hq' g' hg' k' hk' b' hb' hwb' j' r' hj' hbj'
          have hnest : q' ∈ s.nestedQueries k := List.mem_append_right _ hq'
          have hqq := s.wellFormed_nested hwf (s.query_lt hk) hk hnest
          exact ih Z' hZ' g' (by omega) q' (by omega) b
            (s.outer_sub hwf hk ho (coreBindings_within hb) hbd hq') hbn k' hk' b' hb'
            (relevant_of_all (queryTest_sub hwf (plan s) ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y) hq') _ _)
            (fun he => absurd (he ▸ hnest) (s.wellFormed_root hwf k)) hwb' j' r' hj' hbj'
        · intro Z' hZ' d hd qd v hkind g' hg' a ha b'' hb'' hrel'' j' r' hj' hbj'
          have hqd : qd ∈ s.nestedQueries k :=
            List.mem_append_left _ (List.mem_filterMap.mpr ⟨d, hd, by simp [hkind]⟩)
          have hqq := s.wellFormed_nested hwf (s.query_lt hk) hk hqd
          exact ih Z' hZ' g' (by omega) qd (by omega) (s.outerOf k o) (s.outer_derived hwf hk ho hd hkind)
            (houterOf k o hoG') a ha b'' hb'' hrel'' (fun he => absurd (he ▸ hqd) (s.wellFormed_root hwf k))
            (windowPasses_derived hwf hd hkind ha Y b'') j' r' hj' hbj'
        · intro Z' hZ' q' hq' k' hk' d hd qd v hkind g' hg' a ha b'' hb'' hrel'' j' r' hj' hbj'
          have hnest : q' ∈ s.nestedQueries k := List.mem_append_right _ hq'
          have hqq := s.wellFormed_nested hwf (s.query_lt hk) hk hnest
          have hqd : qd ∈ s.nestedQueries k' :=
            List.mem_append_left _ (List.mem_filterMap.mpr ⟨d, hd, by simp [hkind]⟩)
          have hqq' := s.wellFormed_nested hwf (s.query_lt hk') hk' hqd
          have hout' := s.outer_sub hwf hk ho (coreBindings_within hb) hbd hq'
          exact ih Z' hZ' g' (by omega) qd (by omega) (s.outerOf k' b)
            (s.outer_derived hwf hk' hout' hd hkind) (houterOf k' b hbn) a ha b'' hb'' hrel''
            (fun he => absurd (he ▸ hqd) (s.wellFormed_root hwf k')) (windowPasses_derived hwf hd hkind ha Y b'')
            j' r' hj' hbj'
      · rw [coreBindings_within hb _ hik] at hbj
        unfold Statement.outerOf at hbj
        split at hbj
        · simp [Bind.empty] at hbj
        · exact hoG _ r hj hbj

/-- Whether the planned state holds an ON narrowing back for item `j`: an
item a LEFT JOIN adds whose binding may hold no row. -/
def planLeft (s : Statement) (j : Nat) : Prop := ((plan s).state.items j).leftOn.isSome = true

/-- A binding that matters, of a core run on `X` or `Y` at a fuel up to `f`,
under a binding around it whose rows pass their tests, holds rows that pass
their tests, and holds no row only of an item the planned state holds an ON
narrowing back for. -/
theorem plan_restricted {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) {Z : Db} (hZ : Z = X ∨ Z = Y) {g q k : Nat} {o b : Bind}
    (hg : g ≤ f) (hq : q ≤ g) (ho : s.Outer q o) (hoG : Passes (planTest s Y last) o)
    (hk : k ∈ (s.query q).arms) (hb : b ∈ coreBindings s Z (evalQuery s Z g) o k)
    (hrel : relevant s (planRel s Y last) q k (evalQuery s Z g) b = true)
    (hroot : q = s.root → (plan s).AtOrBefore s last k (evalQuery s Z g) b)
    (hwin : (plan s).windowPasses s Y k b = true) :
    Restricted (planTest s Y last) (planLeft s) (s.core k).items b := by
  have hwf := base.wf
  have hklt := ((s.wellFormed_parts hwf).arms q (s.query_lt hk) k hk).1
  have hall := plan_kept_rows base (plan s).order.length Z hZ g hg q hq o ho
    (fun j r _ hr => hoG j r hr) k hk b hb hrel hroot hwin
  rw [List.take_length] at hall
  refine ⟨fun j r hr => ?_, fun j hj hn => ?_⟩
  · by_cases hj : j ∈ (s.core k).items
    · exact hall j r ((plan_order s).2 k hklt j hj) hr
    · rw [coreBindings_within hb _ hj] at hr
      unfold Statement.outerOf at hr
      split at hr
      · simp [Bind.empty] at hr
      · exact hoG j r hr
  · have hcls : endCls s (planRel s Y last) Z g k b :=
      ⟨⟨q, o, hk, ho, hb⟩, fun q' hq' => by obtain rfl := s.wellFormed_armOf hwf hq' hk; exact hrel⟩
    unfold planLeft
    cases hl : ((plan s).state.items j).leftOn with
    | none => exact absurd hn ((plan_semTests hwf last (lookupRows s Y)).rows Z g k b hcls j hj hl)
    | some _ => rfl

end SqliveNarrowing
