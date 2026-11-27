import SqliveNarrowing.Partners

/-! Each run's tests, and what the LIMIT bound asks of them: each run of the
bound is over an item of the statement's own core, and a bound from inside a
run's derived item (`Planned.innerScope`) passes its item's row in each
binding that matters (`inner_passes`), and narrows no item a LEFT JOIN adds
(`innerScope_left`). -/
namespace SqliveNarrowing

/-- Item `i`'s test for a run whose last row's LIMIT values are `last`, with
lookups in `Y`. -/
def planTest (s : Statement) (Y : Db) (last : Option (List (List Value))) : Nat → Row → Bool :=
  (plan s).itemTest s ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y)

/-- Query `q`'s test for the same run. -/
def planRel (s : Statement) (Y : Db) (last : Option (List (List Value))) : Nat → Row → Bool :=
  (plan s).queryTest s ((plan s).scopes last) ((plan s).innerScope last) (lookupRows s Y)

/-! Each query's test. -/

/-- A query with no LIMIT and no OFFSET keeps only rows that pass the scope
and pushed edges of each derived item a core lists that reads it. -/
theorem queryTest_scope {s : Statement} {p : Planned} {scopes inner : Nat → RScope}
    {rowsOf : Nat → List Row}
    {d q : Nat} {v : Bool} (hd : d ∈ (s.core (s.item d).core).items)
    (hkind : (s.item d).kind = .derived q v) (hlim : (s.query q).limit = none)
    (hoff : (s.query q).offset = none) {r : Row} (hr : p.queryTest s scopes inner rowsOf q r = true) :
    p.scopeTest scopes inner rowsOf d r = true := by
  simp only [Planned.queryTest, hlim, hoff, Option.isSome_none, Bool.or_false, Bool.false_or,
    List.all_eq_true, List.mem_range] at hr
  have := hr _ (s.core_lt hd) d hd
  simpa only [hkind, bne_self_eq_false, Bool.false_or] using this

/-! The LIMIT bound's runs. -/

/-- The planned state holds no ON narrowing back for an item that held none
when the statement's conditions were read: the order of the items adds none. -/
theorem plan_keeps_leftOn {s : Statement} {i : Nat}
    (hleft : ((s.readOrder.foldl (readCore s) (PState.initial s)).items i).leftOn = none) :
    ((plan s).state.items i).leftOn = none := by
  cases hfin : ((plan s).state.items i).leftOn with
  | none => rfl
  | some _ =>
    have h1 : ((plan s).state.items i).leftOn.isSome = true := by rw [hfin]; rfl
    unfold plan ordered at h1
    simp only at h1
    have h2 := (ordered_keeps s _ _ _ [] []).1.1 i h1
    rw [hleft] at h2
    cases h2

/-- Each run of the LIMIT bound is over an item the statement's own core,
its only arm, lists, which the planned state holds no ON narrowing back
for. -/
theorem Limit.run_facts {s : Statement} (hwf : s.wellFormed = true) {sorted : Limit.SortedLimit}
    (hs : (plan s).sorted = some sorted) {run : Limit.SortRun} (hrun : run ∈ sorted.runs) :
    ∃ k, (s.query s.root).arms = [k] ∧ run.item ∈ (s.core k).items ∧
      ((plan s).state.items run.item).leftOn = none := by
  have hs' := hs
  rw [plan_sorted] at hs'
  obtain ⟨k, ts, harms, _, _, hruns, hlen, hts⟩ := Limit.sortedLimit_facts hs'
  rw [hruns] at hrun
  obtain ⟨grouped, hg, hitem, _⟩ := Limit.mem_attach hrun
  rw [← hitem]
  obtain ⟨t, ht, he⟩ := Limit.group_item hg
  obtain ⟨⟨pd, t'⟩, hx, rfl⟩ : ∃ x ∈ (s.query s.root).orderBy.zip ts, x.2 = t := by
    obtain ⟨n, hn, rfl⟩ := List.getElem_of_mem ht
    exact ⟨((s.query s.root).orderBy[n]'(by omega), ts[n]), List.mem_iff_getElem.mpr
      ⟨n, by simp only [List.length_zip]; omega, by simp⟩, rfl⟩
  obtain ⟨e, ho, hc, hleft, hsk, _, _⟩ := hts _ hx
  simp only at he ho hc hleft hsk
  rw [← he]
  refine ⟨k, harms, ?_, ?_⟩
  · -- The sort key names the item, so the core's clauses name it.
    obtain ⟨c, hjc⟩ : ∃ c, (t'.1, c) ∈ e.refs := by
      unfold Limit.termKey at hsk
      cases hs : Limit.sortKey s t'.1 e with
      | some k0 =>
        unfold Limit.sortKey at hs
        split at hs
        · rename_i hcond
          simp only [Bool.and_eq_true, Bool.not_eq_true', List.isEmpty_eq_false_iff,
            List.all_eq_true, beq_iff_eq] at hcond
          obtain ⟨⟨_, hne⟩, hall⟩ := hcond
          obtain ⟨⟨j, c⟩, hjc⟩ := List.exists_mem_of_ne_nil _ hne
          have hj : j = t'.1 := (hall _ hjc).1
          rw [hj] at hjc
          exact ⟨c, hjc⟩
        · cases hs
      | none =>
        rw [hs] at hsk
        cases e <;> simp [Option.orElse_none] at hsk
        rename_i it
        obtain ⟨⟨hj, _⟩, _⟩ := hsk
        subst hj
        exact ⟨_, List.mem_singleton_self _⟩
    have hklt := ((s.wellFormed_parts hwf).arms s.root (s.wellFormed_parts hwf).root k
      (by rw [harms]; simp)).1
    have hp : (t'.1, c) ∈ (s.core k).refs := by
      unfold Core.refs
      refine List.mem_append_left _ (List.mem_append_right _ (List.mem_flatMap.mpr
        ⟨_, List.mem_of_getElem? ho, ?_⟩))
      simpa [OutCol.refs, OutCol.exprs] using hjc
    obtain ⟨k', _, hjk'⟩ := (s.wellFormed_parts hwf).refs k hklt _ hp
    have := ((s.wellFormed_parts hwf).items k' (s.core_lt hjk') _ hjk').2
    rw [hc] at this
    subst this
    exact hjk'
  · exact plan_keeps_leftOn hleft

/-! The LIMIT bound inside a run's derived item. -/

/-- A join's scope reads its member's value of the row alone. -/
theorem joined_value {rowsOf : Nat → List Row} {rp rc : Row} {mv mv' v : PValue} {p : Nat}
    {sc sc' : RScope} {nulls : Bool} (hsame : mv'.eval rc = mv.eval rp)
    (hscope : (RScope.joined mv p v sc nulls).passes rowsOf rp = true)
    (hsc : sc' = sc := by rfl) : (RScope.joined mv' p v sc' nulls).passes rowsOf rc = true := by
  subst hsc
  simpa only [RScope.passes, hsame] using hscope

/-- A scope that takes a run's LIMIT bound passes only rows the bound
passes. -/
theorem scopeAt_bound {p : Planned} {values : List (List Value)} {bound : Nat → RScope}
    {rowsOf : Nat → List Row} {i : Nat} {r : Row} {sorted : Limit.SortedLimit} {found : RScope}
    (hs : p.sorted = some sorted)
    (hfound : Limit.runBound i (p.lookupOf bound i) (sorted.runs.zip values) = some found)
    (h : (p.scopeAt (some values) bound i).passes rowsOf r = true) :
    found.passes rowsOf r = true := by
  unfold Planned.scopeAt at h
  simp only [hs, hfound, Limit.passes_both, Bool.and_eq_true] at h
  exact h.2

theorem zip_fst {α β : Type} :
    ∀ (a : List α) (b : List β), (a.zip b).map Prod.fst = a.take b.length
  | [], _ => by simp
  | _ :: _, [] => by simp
  | x :: a, y :: b => by
    simp only [List.zip_cons_cons, List.map_cons, List.length_cons, List.take_succ_cons, zip_fst a b]

/-- Where `innerScope` bounds an item: inside a run's derived item, after the
items `pre` around it, in the run whose item no run before it is over. -/
theorem Limit.candidate_facts {s : Statement} {sorted : Limit.SortedLimit}
    (hs : (plan s).sorted = some sorted) {values : List (List Value)} {i : Nat}
    {x : Nat × List Limit.Level × List (Limit.SortRun × List Value)}
    (hx : x ∈ Limit.innerRuns i [] (sorted.runs.zip values)) :
    ∃ before run last post pre level more,
      sorted.runs.zip values = before ++ (run, last) :: post ∧ run ∈ sorted.runs ∧
      run.inner = pre ++ level :: more ∧ level.item = i ∧
      x = (run.item, pre ++ [level], before ++ [({ item := i, terms := level.terms }, last)]) ∧
      (∀ p ∈ before, p.1 ∈ sorted.runs ∧ p.1.item ≠ run.item) ∧
      Limit.Chain s (s.readOrder.foldl (readCore s) (PState.initial s)) run.item run.terms
        run.inner := by
  obtain ⟨before, run, last, post, hzip, hx⟩ := Limit.mem_innerRuns.mp hx
  obtain ⟨pre, level, more, hinner, hitem, hx⟩ := Limit.mem_levelRuns.mp hx
  simp only [List.nil_append] at hx
  have hs' := hs
  rw [plan_sorted] at hs'
  obtain ⟨_, ts, _, _, _, hruns, _, _⟩ := Limit.sortedLimit_facts hs'
  have hsplit : ∃ after, sorted.runs = before.map (·.1) ++ run :: after := by
    have h := congrArg (List.map Prod.fst) hzip
    rw [zip_fst] at h
    refine ⟨post.map (·.1) ++ sorted.runs.drop values.length, ?_⟩
    conv => lhs; rw [← List.take_append_drop values.length sorted.runs, h]
    simp
  obtain ⟨after, hsplit⟩ := hsplit
  have hne : run.inner ≠ [] := by rw [hinner]; simp
  obtain ⟨_, hfirst, hlevels⟩ := Limit.attach_split (hruns.symm.trans hsplit) hne
  have hchain : Limit.Chain s (s.readOrder.foldl (readCore s) (PState.initial s)) run.item
      run.terms run.inner := by
    rw [hlevels]
    exact Limit.innerLevels_chain _ _ _ _ _
  refine ⟨before, run, last, post, pre, level, more, hzip, by rw [hsplit]; simp, hinner, hitem, hx,
    fun p hp => ⟨?_, hfirst _ (List.mem_map_of_mem hp)⟩, hchain⟩
  rw [hsplit]
  exact List.mem_append_left _ (List.mem_map_of_mem hp)

/-- A run's item and each item inside it is one its core lists, and the
inner ones sit in no run's core. -/
theorem Limit.around_listed {s : Statement} (hwf : s.wellFormed = true)
    {sorted : Limit.SortedLimit} (hs : (plan s).sorted = some sorted) {run : Limit.SortRun}
    (hrun : run ∈ sorted.runs) {st : PState} {pre more : List Limit.Level}
    (hchain : Limit.Chain s st run.item run.terms (pre ++ more)) :
    (Limit.around run.item run.terms pre).1 ∈
        (s.core (s.item (Limit.around run.item run.terms pre).1).core).items ∧
      (pre = [] ∨ ∀ k0, (s.query s.root).arms = [k0] →
        (s.item (Limit.around run.item run.terms pre).1).core ≠ k0) := by
  obtain ⟨k, harms, hlisted, _⟩ := Limit.run_facts hwf hs hrun
  have hcore := ((s.wellFormed_parts hwf).items k (s.core_lt hlisted) _ hlisted).2
  rcases List.eq_nil_or_concat pre with rfl | ⟨pre', level, rfl⟩
  · exact ⟨by rw [Limit.around, List.foldl_nil, hcore]; exact hlisted, Or.inl rfl⟩
  · rw [List.concat_eq_append] at hchain ⊢
    rw [List.append_assoc, List.singleton_append] at hchain
    have link := Limit.Chain.link pre' hchain
    obtain ⟨c, d, passes⟩ := link.found
    obtain ⟨q, v, a, hkind, _, _, hqarms, _, _, hmem, _⟩ := passes.shape
    have ha : a ∈ (s.query q).arms := by rw [hqarms]; simp
    have hlevel := ((s.wellFormed_parts hwf).items a (s.core_lt hmem) _ hmem).2
    rw [Limit.around_append]
    refine ⟨by rw [hlevel]; exact hmem, Or.inr fun k0 harms0 hk0 => ?_⟩
    rw [hlevel] at hk0
    have hroot : a ∈ (s.query s.root).arms := by rw [harms0, hk0]; simp
    obtain rfl := s.wellFormed_armOf hwf ha hroot
    -- No derived item reads the statement's own query.
    have hne : (Limit.around run.item run.terms pre').1 ∈
        (s.core (s.item (Limit.around run.item run.terms pre').1).core).items := by
      have := (Limit.around_listed hwf hs hrun (st := st) (pre := pre')
        (more := level :: more) hchain).1
      exact this
    exact s.wellFormed_root hwf _
      (List.mem_append_left _ (List.mem_filterMap.mpr ⟨_, hne, by simp [hkind]⟩))
  termination_by pre.length

/-- No bound from inside a run's derived item narrows an item a LEFT JOIN
adds. -/
theorem innerScope_left {s : Statement} {last : Option (List (List Value))} {i : Nat}
    (hl : ((plan s).state.items i).leftOn.isSome = true) (rowsOf : Nat → List Row) (r : Row) :
    ((plan s).innerScope last i).passes rowsOf r = true := by
  unfold Planned.innerScope
  cases hs : (plan s).sorted with
  | none => rfl
  | some sorted =>
    cases last with
    | none => rfl
    | some values =>
      simp only [Limit.foldl_both_passes, Bool.and_eq_true, List.all_eq_true]
      refine ⟨rfl, fun x hx => ?_⟩
      obtain ⟨_, run, _, _, pre, level, more, _, _, hinner, hitem, _, _, hchain⟩ :=
        Limit.candidate_facts hs hx
      rw [hinner] at hchain
      obtain ⟨_, _, passes⟩ := (Limit.Chain.link pre hchain).found
      have := plan_keeps_leftOn passes.inner
      rw [hitem] at this
      rw [this] at hl
      cases hl

/-- The bound inside a run's derived item passes the row an inner item has in
a binding that matters: the binding holds, in the columns the arm passes on,
the values of a row the derived item's test keeps, which the derived item's
bound passes. -/
theorem inner_passes {s : Statement} (hwf : s.wellFormed = true) {Y : Db}
    {last : Option (List (List Value))} {E : Bind → Nat → List Row} {q k i : Nat} {b : Bind}
    {r : Row} (hk : k ∈ (s.query q).arms) (hi : i ∈ (s.core k).items)
    (hrel : relevant s (planRel s Y last) q k E b = true) (hbi : b i = some r) :
    ((plan s).innerScope last i).passes (lookupRows s Y) r = true := by
  unfold Planned.innerScope
  cases hs : (plan s).sorted with
  | none => rfl
  | some sorted =>
    cases hlast : last with
    | none => rfl
    | some values =>
      simp only [Limit.foldl_both_passes, Bool.and_eq_true, List.all_eq_true]
      refine ⟨rfl, fun x hx => ?_⟩
      obtain ⟨before, run, lv, post, pre, level, more, hzip, hrun, hinner, hitem, rfl, hbefore,
        hchain⟩ := Limit.candidate_facts hs hx
      simp only
      cases hfound : Limit.runBound i ((plan s).lookupThrough ((plan s).scopes (some values))
          run.item (pre ++ [level])) (before ++ [({ item := i, terms := level.terms }, lv)]) with
      | none => rfl
      | some bc =>
        simp only [Option.getD_some]
        have hchain' := hchain
        rw [hinner] at hchain'
        have link := Limit.Chain.link pre hchain'
        obtain ⟨hlisted, hnested⟩ := Limit.around_listed hwf hs hrun hchain'
        obtain ⟨c0, d0, passes0⟩ := link.found
        obtain ⟨qp, vp, a, hkind, hlim, hoff, hqarms, hwin, _, hmem, _⟩ := passes0.shape
        rw [hitem] at hmem
        have hcorei : (s.item i).core = k := ((s.wellFormed_parts hwf).items k (s.core_lt hi) i hi).2
        have hak : a = k := by
          rw [← hcorei]
          exact ((s.wellFormed_parts hwf).items a (s.core_lt hmem) i hmem).2.symm
        subst hak
        have ha : a ∈ (s.query qp).arms := by rw [hqarms]; simp
        obtain rfl := s.wellFormed_armOf hwf ha hk
        obtain ⟨rp, hrp, hcells⟩ := relevant_cells hrel hwin
        have htest := queryTest_scope hlisted hkind hlim hoff hrp
        simp only [Planned.scopeTest, Bool.and_eq_true] at htest
        rw [hlast] at htest
        have hcols : ∀ c d, level.rename c = some d → rp.cell c = r.cell d := by
          intro c d hc
          obtain ⟨q', v', a', hkind', _, _, harms', _, hout, _, hagg⟩ := (link.passes c d hc).shape
          rw [hkind] at hkind'
          cases hkind'
          rw [hqarms] at harms'
          cases harms'
          rw [hcells c level.item d hout hagg, hitem, hbi]
          rfl
        obtain ⟨k0, harms0, _, _⟩ := Limit.run_facts hwf hs hrun
        have hrootcore : ∀ other ∈ sorted.runs, (s.item other.item).core = k0 := by
          intro other hother
          obtain ⟨k1, harms1, hl1, _⟩ := Limit.run_facts hwf hs hother
          rw [harms0, List.cons.injEq] at harms1
          rw [← harms1.1] at hl1
          exact ((s.wellFormed_parts hwf).items k0 (s.core_lt hl1) _ hl1).2
        have hicore : (s.item i).core ≠ k0 := by
          intro h
          rw [hcorei] at h
          have hroot : a ∈ (s.query s.root).arms := by rw [harms0, h]; simp
          obtain rfl := s.wellFormed_armOf hwf ha hroot
          exact s.wellFormed_root hwf _
            (List.mem_append_left _ (List.mem_filterMap.mpr ⟨_, hlisted, by simp [hkind]⟩))
        have hitems : ∀ p ∈ before, p.1.item ≠ (Limit.around run.item run.terms pre).1 ∧
            p.1.item ≠ i := by
          intro p hp
          refine ⟨?_, fun h => hicore (h ▸ hrootcore _ (hbefore p hp).1)⟩
          rcases hnested with hpre | hne
          · have hfirst := (hbefore p hp).2
            rw [hpre]
            exact hfirst
          · exact fun h => hne k0 harms0 (h ▸ hrootcore _ (hbefore p hp).1)
        have hparent : ∀ found : Option Edge, (pre.foldl Planned.inside (found, run.item)).2 =
            (Limit.around run.item run.terms pre).1 := by
          intro found
          rcases List.eq_nil_or_concat pre with rfl | ⟨pre', l', rfl⟩
          · rfl
          · simp [Limit.around, Planned.inside]
        obtain ⟨bp, hbp, hstep⟩ := Limit.inner_step (lookupRows s Y) rp r
          (Limit.around run.item run.terms pre).1 i
          ((plan s).lookupThrough ((plan s).scopes (some values)) run.item pre)
          ((plan s).lookupThrough ((plan s).scopes (some values)) run.item (pre ++ [level]))
          (Limit.around run.item run.terms pre).2 level.terms lv link.length link.dirs
          (link.values rp r hcols) before hitems (fun j lc hlc => by
            simp only [Planned.lookupThrough, List.foldl_append, List.foldl_cons, List.foldl_nil,
              Planned.inside, Option.map_eq_some_iff, Option.bind_eq_some_iff] at hlc
            obtain ⟨mc, ⟨m, hm, v, hv, rfl⟩, rfl⟩ := hlc
            refine ⟨_, by simp only [Planned.lookupThrough]; rw [hm]; rfl, fun scope hscope => ?_⟩
            rw [hparent] at hv
            have hsame : v.eval r = m.memberValue.eval rp := by
              have := PushDown.value_passed_sound (row := some r) hv hcols
              rwa [PValue.on_some, PValue.on_some] at this
            exact joined_value hsame hscope) bc hfound
        refine hstep ?_
        rcases List.eq_nil_or_concat pre with rfl | ⟨pre', lp, rfl⟩
        · -- The run's item takes the bound in its own scope.
          simp only [Limit.around, List.foldl_nil] at hbp htest hlisted
          have hdo : run.item ∈ (plan s).order :=
            (plan_order s).2 _ (s.core_lt hlisted) _ hlisted
          obtain ⟨boundd, hboundd, hplaced⟩ :=
            (plan s).scopes_eq (some values) (plan_order s).1 hdo
          have hlook : (plan s).lookupThrough ((plan s).scopes (some values)) run.item [] =
              (plan s).lookupOf boundd run.item := by
            funext j
            simp only [Planned.lookupThrough, Planned.lookupOf, List.foldl_nil]
            cases hm : ((plan s).state.items run.item).memberships.find? (·.partner == j) with
            | none => rfl
            | some m =>
              have := hplaced m.partner
                ((plan_placedBefore s run.item).1 m (List.mem_of_find?_eq_some hm))
              simp only [Option.map_some, this]
          have hruns : Limit.runBound run.item ((plan s).lookupOf boundd run.item)
              (sorted.runs.zip values) = some bp := by
            rw [hzip, Limit.runBound_first rfl fun p hp => (hbefore p hp).2, ← hlook]
            exact hbp
          have hscope := htest.1.1.1
          rw [hboundd] at hscope
          exact scopeAt_bound hs hruns hscope
        · -- An inner item takes it as one of its bounds from inside.
          rw [List.concat_eq_append] at hbp htest hinner
          have hinner' := htest.1.2
          simp only [Planned.innerScope, hs, Limit.foldl_both_passes, Bool.and_eq_true,
            List.all_eq_true] at hinner'
          rw [Limit.around_append] at hbp hinner'
          have := hinner'.2 (run.item, pre' ++ [lp],
              before ++ [({ item := lp.item, terms := lp.terms }, lv)])
            (Limit.mem_innerRuns.mpr ⟨before, run, lv, post, hzip,
              Limit.mem_levelRuns.mpr ⟨pre', lp, level :: more, by rw [hinner]; simp, rfl,
                by simp⟩⟩)
          rw [hbp] at this
          exact this

end SqliveNarrowing
