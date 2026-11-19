import SqliveNarrowing.ReadPhase

/-! What the planner's state says once it has placed every item. As the
planner places an item, it takes the item's edges to items placed before it
as memberships, which the invariant doesn't speak of, and passes them into
the item's arms when the item is derived. A passed edge joins the state's
pushed edges, which the invariant leaves alone, and makes its target an inner
join's where a row of NULLs fails it: a binding of the arm that matters holds,
in the column the arm passes on, the value of a row the derived item's test
keeps, which the test keeps only where the edge's member column holds a
value, so the target has a row there. So the planned state says only what
holds of each binding a core keeps that matters (`plan_sem`), and a row
a derived item's query's test keeps passes the derived item's term and
holds a value in each member column its memberships and passed edges
compare by `=`, so the tests the planner's scopes give are such tests
(`plan_semTests`). Each item's memberships are edges of the planned state
(`plan_membersEdges`), so the invariant speaks of them. -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-- A binding a core keeps that matters to the test of its query, for each
query it is an arm of. -/
noncomputable def endCls (s : Statement) (rel : Nat → Row → Bool) : Cls :=
  fun Z f k b => s.KeptIn Z f k b ∧
    ∀ q, k ∈ (s.query q).arms → relevant s rel q k (evalQuery s Z f) b = true

/-- A fold whose steps relate each state to the next by a relation that is
reflexive and transitive keeps a property each step keeps when what it
leaves is related to `Fin`, from a state whose fold is. -/
theorem foldl_until_rel {α : Type} {R : PState → PState → Prop} (hrefl : ∀ a, R a a)
    (htrans : ∀ {a b c}, R a b → R b c → R a c) {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), R st (g st x)) {Fin : PState} {Q : PState → Prop} :
    ∀ (l : List α), (∀ st x, x ∈ l → Q st → R (g st x) Fin → Q (g st x)) →
      ∀ st, Q st → R (l.foldl g st) Fin → Q (l.foldl g st)
  | [], _, _, h, _ => h
  | x :: l, hstep, st, h, hfin => by
    have hgrow : ∀ (l : List α) (st : PState), R st (l.foldl g st) := by
      intro l
      induction l with
      | nil => exact hrefl
      | cons y l ih => exact fun st => htrans (hg st y) (ih _)
    exact foldl_until_rel hrefl htrans hg l (fun st' y hy => hstep st' y (List.mem_cons_of_mem x hy)) _
      (hstep st x List.mem_cons_self h (htrans (hgrow l (g st x)) hfin)) hfin

theorem PState.Grows.refl (st : PState) : st.Grows st :=
  ⟨fun _ h => h, fun _ _ h => h, fun _ h => h⟩

theorem PState.Grows.foldl {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), st.Grows (g st x)) :
    ∀ (l : List α) (st : PState), st.Grows (l.foldl g st)
  | [], st => PState.Grows.refl st
  | x :: l, st => (hg st x).trans (PState.Grows.foldl hg l (g st x))

/-- One target's step of passing edges, and no term, into an arm keeps the
invariant, when the target has a row in every binding of the class where it
is still a LEFT JOIN's and some edge passes. -/
theorem PState.Sem.pushStepEdges {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) {narrowFuel fuel : Nat} {Fin : PState}
    {st : PState} (hs : st.Sem s P) {target : Nat} {renamed : Option STerm} {passed : List Edge}
    (hren : renamed = none)
    (hrow : passed ≠ [] → (st.items target).leftOn.isSome = true →
      ∀ Z f b, P Z f (s.item target).core b → b target ≠ none)
    (ih : ∀ X : PState, X.Sem s P → (∀ e ∈ passed, e ∈ X.pushed) →
      (PushDown.pushDown s narrowFuel fuel X target renamed passed).Grows Fin →
      (PushDown.pushDown s narrowFuel fuel X target renamed passed).Sem s P)
    (hfin : (PushDown.pushDown s narrowFuel fuel
      (if renamed.isSome || !passed.isEmpty then
        PState.joinInner narrowFuel
          { (match renamed with
              | some r => st.addTerm target r
              | none => st) with
            edges := (match renamed with
              | some r => st.addTerm target r
              | none => st).edges ++ passed,
            pushed := (match renamed with
              | some r => st.addTerm target r
              | none => st).pushed ++ passed } target
      else
        { (match renamed with
            | some r => st.addTerm target r
            | none => st) with
          edges := (match renamed with
            | some r => st.addTerm target r
            | none => st).edges ++ passed,
          pushed := (match renamed with
            | some r => st.addTerm target r
            | none => st).pushed ++ passed }) target renamed passed).Grows Fin) :
    (PushDown.pushDown s narrowFuel fuel
      (if renamed.isSome || !passed.isEmpty then
        PState.joinInner narrowFuel
          { (match renamed with
              | some r => st.addTerm target r
              | none => st) with
            edges := (match renamed with
              | some r => st.addTerm target r
              | none => st).edges ++ passed,
            pushed := (match renamed with
              | some r => st.addTerm target r
              | none => st).pushed ++ passed } target
      else
        { (match renamed with
            | some r => st.addTerm target r
            | none => st) with
          edges := (match renamed with
            | some r => st.addTerm target r
            | none => st).edges ++ passed,
          pushed := (match renamed with
            | some r => st.addTerm target r
            | none => st).pushed ++ passed }) target renamed passed).Sem s P := by
  subst hren
  simp only [Option.isSome_none, Bool.false_or] at hfin ⊢
  have hs3 : ({ st with edges := st.edges ++ passed, pushed := st.pushed ++ passed } : PState).Sem s P :=
    ⟨hs.rows, hs.terms, hs.carried, fun Z f b e he hb hm => by
      rcases List.mem_append.mp he with he | he
      · exact (hs.edges Z f b e he hb hm).imp_left (List.mem_append_left _)
      · exact Or.inl (List.mem_append_right _ he), hs.ors, hs.held⟩
  have hp3 : ∀ e ∈ passed, e ∈ ({ st with edges := st.edges ++ passed, pushed := st.pushed ++ passed } :
      PState).pushed := fun e he => List.mem_append_right _ he
  cases hemp : passed.isEmpty with
  | false =>
    simp only [hemp, Bool.not_false, ↓reduceIte] at hfin ⊢
    have hne' : passed ≠ [] := by
      intro he
      rw [he] at hemp
      simp at hemp
    refine ih _ (hs3.joinInner_of h hP (PState.Sem.narrow h hP narrowFuel) fun Z f b hb hl => ?_)
      (fun e he => by rw [PState.joinInner_pushed]; exact hp3 e he) hfin
    refine hrow hne' ?_ Z f b hb
    cases hx : (st.items target).leftOn with
    | none => exact absurd hx hl
    | some _ => rfl
  | true =>
    simp only [hemp, Bool.not_true, Bool.false_eq_true, ↓reduceIte] at hfin ⊢
    exact ih _ hs3 hp3 hfin

/-- Passing edges, and no term, into a derived item's arms keeps the
invariant, when every binding of the class of an arm's core that calls no
window function holds, in the columns the arm passes on, the values of a row
that matters to the derived item, which holds a value in the member column
of each edge passed that a row of NULLs fails. -/
theorem PState.Sem.pushEdges {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) {Fin : PState} {Src : Nat → Prop}
    {Good : Nat → Row → Prop}
    (hGood : ∀ i r, Good i r → ∀ e ∈ Fin.pushed, e.member = i → e.nulls = false →
      ∀ c, e.memberValue = .column c → r.cell c ≠ none)
    (hcls : ∀ Z f k b d q v, Src d → (s.item d).kind = .derived q v → k ∈ (s.query q).arms →
      (s.core k).windowed = false → (s.query q).limit = none → (s.query q).offset = none →
      P Z f k b → ∃ r, Good d r ∧ s.PassedCells k r b)
    (hsrc : ∀ d q v k t, Src d → (s.item d).kind = .derived q v → k ∈ (s.query q).arms →
      t ∈ (s.core k).items → Src t) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (edges : List Edge), Src item → st.Sem s P →
      st.Passes item edges → (PushDown.pushDown s narrowFuel fuel st item none edges).Grows Fin →
      (∀ e ∈ edges, e.nulls = false → ∀ c, e.memberValue = .column c → ∀ r, Good item r →
        r.cell c ≠ none) →
      (PushDown.pushDown s narrowFuel fuel st item none edges).Sem s P
  | 0, st, _, _, _, hs, _, _, _ => hs
  | fuel + 1, st, item, edges, hsrc0, hs, hpass, hfin, hedge => by
    have ih := PState.Sem.pushEdges h hP hGood hcls hsrc narrowFuel fuel
    have hgrow : ∀ (X : PState) (t : Nat) (passed : List Edge), (∀ e ∈ passed, e ∈ X.pushed ∧ e.member = t) →
        X.Grows (PushDown.pushDown s narrowFuel fuel X t none passed) :=
      fun X t passed hp => (PushDown.pushDown_keeps s narrowFuel fuel X t none passed
        fun e he => Or.inr (hp e he)).1
    revert hfin
    simp only [PushDown.pushDown]
    split
    · intro _; exact hs
    · rename_i found hfound
      split
      · intro _; exact hs
      · intro hfin
        have hstep : ∀ (st1 : PState) (target : Nat) (renamed : Option STerm) (passed : List Edge),
            renamed = none → (∀ e ∈ passed, e.member = target) →
            st1.Grows (PushDown.pushDown s narrowFuel fuel
              (if renamed.isSome || !passed.isEmpty then
                PState.joinInner narrowFuel
                  { (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1) with
                    edges := (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1).edges ++ passed,
                    pushed := (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1).pushed ++ passed } target
              else
                { (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1) with
                  edges := (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1).edges ++ passed,
                  pushed := (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1).pushed ++ passed }) target renamed passed) := by
          intro st1 target renamed passed hren hpm
          subst hren
          simp only [Option.isSome_none, Bool.false_or]
          have h13 : st1.Grows ({ st1 with edges := st1.edges ++ passed, pushed := st1.pushed ++ passed } :
              PState) := ⟨fun _ h => h, fun _ _ h => h, fun _ h => List.mem_append_left _ h⟩
          split
          · have h34 := (PState.joinInner_keeps s narrowFuel
              ({ st1 with edges := st1.edges ++ passed, pushed := st1.pushed ++ passed } : PState) target).1
            exact (h13.trans h34).trans (hgrow _ _ _ fun e he =>
              ⟨by rw [PState.joinInner_pushed]; exact List.mem_append_right _ he, hpm e he⟩)
          · exact h13.trans (hgrow _ _ _ fun e he => ⟨List.mem_append_right _ he, hpm e he⟩)
        have hpm : ∀ {target : Nat} {through : Nat → Option (Nat × Nat)} {keep : Edge → Bool} {e : Edge},
            e ∈ (edges.filter keep).filterMap (fun e =>
              match e.memberValue with
              | .column c => (match through c with
                  | some (t, c') => if t = target then some c' else none
                  | none => none).map fun c' => { e with member := target, memberValue := .column c' }
              | .expr _ => none) → e.member = target := by
          intro target through keep e he
          obtain ⟨e0, _, c, c', _, _, rfl⟩ := PushDown.mem_passed he
          rfl
        refine foldl_until_rel (R := PState.Grows) PState.Grows.refl (fun hab hbc => hab.trans hbc)
          (fun st0 arm => PState.Grows.foldl (fun st1 target => hstep st1 target _ _ rfl fun e he => hpm he) _ st0)
          _ (fun st0 arm harm hs0 hfin0 => ?_) st hs hfin
        obtain ⟨q, v, k, hkind, hlim, hoff, hk, hwin, hthrough⟩ := PushDown.mem_arms hfound harm
        have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
        refine foldl_until_rel (R := PState.Grows) PState.Grows.refl (fun hab hbc => hab.trans hbc)
          (fun st1 target => hstep st1 target _ _ rfl fun e he => hpm he) _
          (fun st1 target htarget hs1 hfin1 => ?_) st0 hs0 hfin0
        obtain ⟨c0, _, hc0⟩ := List.mem_filterMap.mp (List.mem_eraseDups.mp htarget)
        obtain ⟨⟨t0, c0'⟩, hc0', ht0⟩ := Option.map_eq_some_iff.mp hc0
        simp only at ht0
        subst ht0
        obtain ⟨hcore, _, hout⟩ := hthrough c0 t0 c0' hc0'
        have hlisted := s.output_listed h hklt hcore hout
        refine hs1.pushStepEdges h hP rfl (fun hne hl Z f b hb => ?_) (fun X hX hXp hXfin => ?_) hfin1
        · obtain ⟨e, he⟩ := List.exists_mem_of_ne_nil _ hne
          obtain ⟨e0, he0f, hmap⟩ := List.mem_filterMap.mp he
          obtain ⟨he0, hkeep⟩ := List.mem_filter.mp he0f
          have hnull : e0.nulls = false := by
            simp only [hl, Bool.true_and, Bool.not_eq_true'] at hkeep
            exact hkeep
          split at hmap
          · rename_i c hmv
            obtain ⟨c', hc', _⟩ := Option.map_eq_some_iff.mp hmap
            split at hc'
            · rename_i t c'' hthc
              split at hc'
              · rename_i htt
                subst htt
                obtain ⟨hcore', hagg, hout'⟩ := hthrough c t c'' hthc
                rw [hcore'] at hb
                obtain ⟨r, hgood, hcells⟩ := hcls Z f k b item q v hsrc0 hkind hk hwin hlim hoff hb
                have hval := hedge e0 he0 hnull c hmv r hgood
                rw [hcells c t c'' hout' hagg] at hval
                intro hbt
                rw [hbt] at hval
                exact hval rfl
              · cases hc'
            · cases hc'
          · cases hmap
        · have hXpass : ∀ e ∈ _, e ∈ X.pushed ∧ e.member = t0 := fun e he => ⟨hXp e he, hpm he⟩
          refine ih X t0 _ (hsrc item q v k t0 hsrc0 hkind hk hlisted) hX
            (fun e he => Or.inr (hXpass e he)) hXfin fun e he hnull c hmv r hg => ?_
          exact hGood t0 r hg e (hXfin.2.2 e ((hgrow X t0 _ hXpass).2.2 e (hXp e he))) (hpm he) hnull c hmv

/-! Placing every item. -/

theorem ordered_placed_termGrows (s : Statement) (st : PState) (order : List Nat) (next : Nat) :
    st.TermGrows (ordered.placed s st order next) := by
  unfold ordered.placed
  exact (PState.termGrows_update rfl).trans (PushDown.termGrows_pushDown s _ _ _ _ _ _)

theorem ordered_termGrows (s : Statement) :
    ∀ (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat),
      st.TermGrows (ordered.go s fuel all st order waited).1
  | 0, _, st, _, _ => PState.TermGrows.refl st
  | fuel + 1, all, st, order, waited => by
    rcases ordered_go_place s fuel all st order waited with ⟨_, heq⟩ | ⟨next, waited', _, heq⟩
    · rw [heq]; exact PState.TermGrows.refl st
    · rw [heq]
      exact (ordered_placed_termGrows s st order next).trans (ordered_termGrows s fuel all _ _ _)

/-- What a row that matters to derived item `d` holds in the planned state
`Fin`: its term, and a value in the member column of each of its memberships
and passed edges that a row of NULLs fails. -/
def Planned.Good (Fin : PState) (d : Nat) (r : Row) : Prop :=
  (∀ u, (Fin.items d).term = some u → u.holds (some r) = true) ∧
  ∀ e, (e ∈ (Fin.items d).memberships ∨ (e ∈ Fin.pushed ∧ e.member = d)) → e.nulls = false →
    ∀ c, e.memberValue = .column c → r.cell c ≠ none

/-- Placing an item keeps the invariant for the bindings that matter. -/
theorem PState.Sem.placed {s : Statement} (h : s.wellFormed = true) {rel : Nat → Row → Bool}
    {Fin : PState}
    (hrel : ∀ d q v, d ∈ (s.core (s.item d).core).items → (s.item d).kind = .derived q v →
      (s.query q).limit = none → (s.query q).offset = none → ∀ r, rel q r = true → Planned.Good Fin d r)
    {st : PState} (hs : st.Sem s (endCls s rel)) (order : List Nat) {next : Nat}
    (hnext : next ∈ (s.core (s.item next).core).items)
    (hfin : (ordered.placed s st order next).Grows Fin) :
    (ordered.placed s st order next).Sem s (endCls s rel) := by
  unfold ordered.placed at hfin ⊢
  simp only at hfin ⊢
  have hs' := hs.update (i := next) (g := fun p => { p with
      memberships := p.memberships ++ st.edges.filter fun e => e.member == next && order.contains e.partner,
      either := (st.items next).ors.filter fun sides =>
        sides.all fun side => side.edges.all fun e => order.contains e.partner })
    (fun hl Z f k b hb hi => hs.rows Z f k b hb next hi hl)
    (fun Z f b r hb hr u hu => hs.terms Z f b next r hb hr u hu)
    (fun Z f b r hb hr u hu => hs.carried Z f b next r hb hr u hu)
    (fun Z f b hb hr sides hsides => hs.ors Z f b next hb hr sides hsides)
    (fun n hn => hs.held next n hn)
  have hpass : (st.update next fun p => { p with
      memberships := p.memberships ++ st.edges.filter fun e => e.member == next && order.contains e.partner,
      either := (st.items next).ors.filter fun sides =>
        sides.all fun side => side.edges.all fun e => order.contains e.partner }).Passes next
      (st.edges.filter fun e => e.member == next && order.contains e.partner) :=
    fun e he => Or.inl (by simp only [PState.update, ↓reduceIte]; exact List.mem_append_right _ he)
  have hgrow := (PushDown.pushDown_keeps s s.fuel s.fuel _ next none _ hpass).1
  refine PState.Sem.pushEdges h (fun _ _ _ _ hb => hb.1) (Fin := Fin)
    (Src := fun d => d ∈ (s.core (s.item d).core).items) (Good := Planned.Good Fin)
    (fun i r hg e he hm => hg.2 e (Or.inr ⟨he, hm⟩)) ?_ ?_ s.fuel s.fuel _ next _ hnext hs' hpass hfin ?_
  · intro Z f k b d q v hdl hkind hk hwin hlim hoff hb
    obtain ⟨r, hr, hcells⟩ := relevant_cells (hb.2 q hk) hwin
    exact ⟨r, hrel d q v hdl hkind hlim hoff r hr, hcells⟩
  · intro d q v k t _ _ _ ht
    rw [((s.wellFormed_parts h).items k (s.core_lt ht) t ht).2]
    exact ht
  · intro e he hnull c hmv r hg
    rcases hpass e he with hm | ⟨_, hm⟩
    · exact hg.2 e (Or.inl ((hgrow.trans hfin).2.1 next e hm)) hnull c hmv
    · exact hg.2 e (Or.inl ((hgrow.trans hfin).2.1 next e (by
        simp only [PState.update, ↓reduceIte]; exact List.mem_append_right _ he))) hnull c hmv

theorem PState.Sem.ordered_go {s : Statement} (h : s.wellFormed = true) {rel : Nat → Row → Bool}
    {Fin : PState}
    (hrel : ∀ d q v, d ∈ (s.core (s.item d).core).items → (s.item d).kind = .derived q v →
      (s.query q).limit = none → (s.query q).offset = none → ∀ r, rel q r = true → Planned.Good Fin d r) :
    ∀ (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat),
      (∀ i ∈ all, i ∈ (s.core (s.item i).core).items) → st.Sem s (endCls s rel) →
      (ordered.go s fuel all st order waited).1.Grows Fin →
      (ordered.go s fuel all st order waited).1.Sem s (endCls s rel)
  | 0, _, _, _, _, _, hs, _ => hs
  | fuel + 1, all, st, order, waited, hall, hs, hfin => by
    rcases ordered_go_place s fuel all st order waited with ⟨_, heq⟩ | ⟨next, waited', hnext, heq⟩
    · rw [heq]; exact hs
    · rw [heq] at hfin ⊢
      exact PState.Sem.ordered_go h hrel fuel all _ _ _ hall
        (hs.placed h hrel order (hall next (List.mem_filter.mp hnext).1)
          ((ordered_keeps s fuel all _ _ _).1.trans hfin)) hfin

/-- The planned state says only what holds of each binding a core keeps that
matters to its query's test, when a row each derived item's query's test
keeps is one that matters to the derived item in the planned state. -/
theorem plan_sem {s : Statement} (h : s.wellFormed = true) {rel : Nat → Row → Bool}
    (hrel : ∀ d q v, d ∈ (s.core (s.item d).core).items → (s.item d).kind = .derived q v →
      (s.query q).limit = none → (s.query q).offset = none → ∀ r, rel q r = true → Planned.Good (plan s).state d r) :
    (plan s).state.Sem s (endCls s rel) := by
  have hread := plan_readSem h (rel := rel) (Fin := (plan s).state)
    (fun d q v hd hk hl ho r hr u hu => (hrel d q v hd hk hl ho r hr).1 u hu)
    (ordered_termGrows s _ _ _ _ _)
  unfold plan ordered
  simp only
  refine PState.Sem.ordered_go h (Fin := (plan s).state) hrel _ _ _ _ _ (fun i hi => ?_)
    (hread.weaken fun _ _ _ _ hb => ⟨hb.1, fun _ => hb.2⟩) (PState.Grows.refl _)
  obtain ⟨k, hk, hik⟩ := List.mem_flatMap.mp hi
  rw [((s.wellFormed_parts h).items k (List.mem_range.mp hk) i hik).2]
  exact hik

/-! A row the planner's tests keep. -/

theorem foldl_both_passes_acc {α : Type} {rowsOf : Nat → List Row} {r : Row} (g : α → RScope) :
    ∀ (l : List α) (acc : RScope), (l.foldl (fun acc a => acc.both (g a)) acc).passes rowsOf r = true →
      acc.passes rowsOf r = true
  | [], _, h => h
  | a :: l, acc, h => by
    have := foldl_both_passes_acc g l _ h
    rw [Limit.passes_both, Bool.and_eq_true] at this
    exact this.1

theorem foldl_both_passes_each {α : Type} {rowsOf : Nat → List Row} {r : Row} (g : α → RScope) :
    ∀ (l : List α) (acc : RScope), (l.foldl (fun acc a => acc.both (g a)) acc).passes rowsOf r = true →
      ∀ a ∈ l, (g a).passes rowsOf r = true
  | [], _, _, a, ha => by cases ha
  | b :: l, acc, h, a, ha => by
    rcases List.mem_cons.mp ha with rfl | ha
    · have := foldl_both_passes_acc g l _ h
      rw [Limit.passes_both, Bool.and_eq_true] at this
      exact this.2
    · exact foldl_both_passes_each g l _ h a ha

/-- A row an item's scope passes passes what the item's terms and carried
terms say of it, and each of its memberships' scopes. -/
theorem scopeAt_parts {p : Planned} {last : Option (List (List Value))} {bound : Nat → RScope}
    {rowsOf : Nat → List Row} {i : Nat} {r : Row}
    (h : (p.scopeAt last bound i).passes rowsOf r = true) :
    (∀ t, (p.state.items i).term = some t → t.holds (some r) = true) ∧
      (∀ t, (p.state.items i).carried = some t → t.holds (some r) = true) ∧
      ∀ m ∈ (p.state.items i).memberships, (Planned.membershipScope bound m).passes rowsOf r = true := by
  have heither : ∀ (l : List (List Side)) (acc : RScope),
      (l.foldl (fun scope sides =>
        match sides.map (Planned.sideScope bound) with
        | [] => scope
        | first :: rest => scope.both (rest.foldl RScope.either first)) acc).passes rowsOf r = true →
      acc.passes rowsOf r = true := by
    intro l
    induction l with
    | nil => exact fun _ h => h
    | cons sides l ih =>
      intro acc h
      have := ih _ h
      simp only at this
      split at this
      · exact this
      · rw [Limit.passes_both, Bool.and_eq_true] at this
        exact this.1
  unfold Planned.scopeAt at h
  simp only at h
  have hall : ∀ found : RScope, ∀ sc : RScope, (sc.both found).passes rowsOf r = true →
      sc.passes rowsOf r = true := by
    intro found sc h
    rw [Limit.passes_both, Bool.and_eq_true] at h
    exact h.1
  have hm := heither _ _ (by
    split at h
    · split at h
      · exact hall _ _ h
      · exact h
    · exact h)
  have hbase := foldl_both_passes_acc _ _ _ hm
  rw [Limit.passes_both, Bool.and_eq_true] at hbase
  refine ⟨fun t ht => ?_, fun t ht => ?_, foldl_both_passes_each _ _ _ hm⟩
  · have := hbase.1
    rw [ht] at this
    simpa [STerm.bind_passes] using this
  · have := hbase.2
    rw [ht] at this
    simpa [STerm.bind_passes] using this

/-- A row a membership's scope passes holds a value in the member's value,
unless the membership compares by `IS`. -/
theorem membershipScope_value {bound : Nat → RScope} {rowsOf : Nat → List Row} {m : Edge} {r : Row}
    (h : (Planned.membershipScope bound m).passes rowsOf r = true) (hn : m.nulls = false) :
    (m.memberValue.eval r).isSome = true := by
  unfold Planned.membershipScope at h
  simp only at h
  split at h
  · rename_i mc c hmv _
    split at h
    · simp only [RScope.passes] at h
      rw [hmv]
      simp only [PValue.eval]
      cases hx : r.cell mc with
      | none => rw [hx] at h; cases h
      | some _ => rfl
    · simp only [RScope.passes, Bool.and_eq_true, Bool.or_eq_true, hn] at h
      rcases h.1 with h1 | h1
      · exact h1
      · cases h1
  · simp only [RScope.passes, Bool.and_eq_true, Bool.or_eq_true, hn] at h
    rcases h.1 with h1 | h1
    · exact h1
    · cases h1

/-- A row of a derived item's query, with no LIMIT and no OFFSET, that the
query's test keeps is one that matters to the derived item in the planned
state: the derived item's scope and pushed edges pass it. -/
theorem queryTest_good {s : Statement} (last : Option (List (List Value)))
    (rowsOf : Nat → List Row) {d q : Nat} {v : Bool} (hd : d ∈ (s.core (s.item d).core).items)
    (hkind : (s.item d).kind = .derived q v) (hlim : (s.query q).limit = none)
    (hoff : (s.query q).offset = none) {r : Row}
    (hr : (plan s).queryTest s ((plan s).scopes last) rowsOf q r = true) :
    Planned.Good (plan s).state d r := by
  have hdk : (s.item d).core < s.cores.length := s.core_lt hd
  have htest : (plan s).scopeTest ((plan s).scopes last) rowsOf d r = true := by
    simp only [Planned.queryTest, hlim, hoff, Option.isSome_none, Bool.or_false, Bool.false_or,
      List.all_eq_true, List.mem_range] at hr
    have := hr _ hdk d hd
    simp only [hkind, bne_self_eq_false, Bool.false_or] at this
    exact this
  simp only [Planned.scopeTest, Bool.and_eq_true, List.all_eq_true, Bool.or_eq_true, bne_iff_ne,
    ne_eq] at htest
  obtain ⟨⟨hsc, _⟩, hpushed⟩ := htest
  obtain ⟨hnd, hall⟩ := plan_order s
  obtain ⟨bound, hbound, _⟩ := (plan s).scopes_eq last hnd (hall _ hdk d hd)
  rw [hbound] at hsc
  obtain ⟨hterm, _, hmem⟩ := scopeAt_parts hsc
  refine ⟨hterm, fun e he hn c hc => ?_⟩
  rcases he with he | ⟨he, hm⟩
  · have := membershipScope_value (hmem e he) hn
    rw [hc] at this
    simp only [PValue.eval] at this
    exact Option.isSome_iff_ne_none.mp this
  · rcases hpushed e he with hne | hp
    · exact absurd hm hne
    · have := membershipScope_value hp hn
      rw [hc] at this
      simp only [PValue.eval] at this
      exact Option.isSome_iff_ne_none.mp this

/-- The planned state says only what holds of each binding a core keeps that
matters to its query's test, the test the planner's scopes give. -/
theorem plan_semTests {s : Statement} (h : s.wellFormed = true) (last : Option (List (List Value)))
    (rowsOf : Nat → List Row) :
    (plan s).state.Sem s (endCls s ((plan s).queryTest s ((plan s).scopes last) rowsOf)) :=
  plan_sem h fun _ _ _ hd hkind hlim hoff _ hr => queryTest_good last rowsOf hd hkind hlim hoff hr

/-! Memberships are edges of the state. -/

/-- `st'` holds every edge `st` holds. -/
def PState.EdgesGrow (st st' : PState) : Prop := ∀ e ∈ st.edges, e ∈ st'.edges

theorem PState.EdgesGrow.refl (st : PState) : st.EdgesGrow st := fun _ h => h

theorem PState.EdgesGrow.trans {a b c : PState} (hab : a.EdgesGrow b) (hbc : b.EdgesGrow c) :
    a.EdgesGrow c := fun e h => hbc e (hab e h)

theorem PState.EdgesGrow.foldl {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), st.EdgesGrow (g st x)) :
    ∀ (l : List α) (st : PState), st.EdgesGrow (l.foldl g st)
  | [], st => PState.EdgesGrow.refl st
  | x :: l, st => (hg st x).trans (PState.EdgesGrow.foldl hg l (g st x))

theorem PState.edgesGrow_update (st : PState) (i : Nat) (f : PItem → PItem) :
    st.EdgesGrow (st.update i f) := fun _ h => h

theorem PState.edgesGrow_add (st : PState) (n : Narrowing) : st.EdgesGrow (st.add n).1 := by
  have hterms : ∀ (l : List (Nat × STerm)) (st : PState),
      (l.foldl (fun st (x : Nat × STerm) => st.addTerm x.1 x.2) st).edges = st.edges := by
    intro l
    induction l with
    | nil => intro st; rfl
    | cons x l ih => intro st; rw [List.foldl_cons, ih]; rfl
  have hors : ∀ (l : List Disjunction) (st : PState),
      (l.foldl (fun st (d : Disjunction) =>
        st.update d.item fun p => { p with ors := p.ors ++ [d.sides] }) st).edges = st.edges := by
    intro l
    induction l with
    | nil => intro st; rfl
    | cons x l ih => intro st; rw [List.foldl_cons, ih]; rfl
  intro e he
  simp only [PState.add]
  rw [hors, hterms]
  exact List.mem_append_left _ he

theorem PState.edgesGrow_narrow : ∀ (fuel : Nat) (st : PState) (n : Narrowing),
    st.EdgesGrow (PState.narrow fuel st n)
  | 0, st, _ => PState.EdgesGrow.refl st
  | fuel + 1, st, n => by
    simp only [PState.narrow]
    refine (PState.edgesGrow_add st _).trans (PState.EdgesGrow.foldl (fun st' i => ?_) _ _)
    split
    · exact (PState.edgesGrow_update _ _ _).trans (PState.edgesGrow_narrow fuel _ _)
    · exact PState.EdgesGrow.refl _

theorem PState.edgesGrow_joinInner (fuel : Nat) (st : PState) (i : Nat) :
    st.EdgesGrow (st.joinInner fuel i) := by
  unfold PState.joinInner
  split
  · exact (PState.edgesGrow_update _ _ _).trans (PState.edgesGrow_narrow fuel _ _)
  · exact PState.EdgesGrow.refl _

theorem PState.edgesGrow_narrowOn (fuel : Nat) (st : PState) (i : Nat) (n : Narrowing) :
    st.EdgesGrow (st.narrowOn fuel i n) := by
  unfold PState.narrowOn
  generalize n.split i = x
  obtain ⟨own, others⟩ := x
  simp only
  refine (PState.edgesGrow_add st own).trans ?_
  split
  · exact PState.edgesGrow_update _ _ _
  · exact PState.edgesGrow_narrow fuel _ _

theorem PushDown.edgesGrow_pushDown (s : Statement) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm) (edges : List Edge),
      st.EdgesGrow (PushDown.pushDown s narrowFuel fuel st item term edges)
  | 0, st, _, _, _ => PState.EdgesGrow.refl st
  | fuel + 1, st, item, term, edges => by
    simp only [PushDown.pushDown]
    split
    · exact PState.EdgesGrow.refl st
    · split
      · exact PState.EdgesGrow.refl st
      · refine PState.EdgesGrow.foldl (fun st0 arm => ?_) _ st
        refine PState.EdgesGrow.foldl (fun st1 target => ?_) _ st0
        refine PState.EdgesGrow.trans ?_ (PushDown.edgesGrow_pushDown s narrowFuel fuel _ _ _ _)
        have h2 : ∀ (renamed : Option STerm), st1.EdgesGrow (match renamed with
            | some r => st1.addTerm target r
            | none => st1) := by
          intro renamed
          cases renamed
          · exact PState.EdgesGrow.refl st1
          · exact PState.edgesGrow_update _ _ _
        have hrec : ∀ (st2 : PState) (passed : List Edge),
            st2.EdgesGrow { st2 with edges := st2.edges ++ passed, pushed := st2.pushed ++ passed } :=
          fun _ _ _ h => List.mem_append_left _ h
        split
        · exact (h2 _).trans ((hrec _ _).trans (PState.edgesGrow_joinInner _ _ _))
        · exact (h2 _).trans (hrec _ _)

theorem edgesGrow_readCore (s : Statement) (st : PState) (k : Nat) :
    st.EdgesGrow (readCore s st k) := by
  unfold readCore
  simp only
  refine PState.EdgesGrow.trans ?_ (PState.EdgesGrow.foldl
    (fun st i => PushDown.edgesGrow_pushDown s _ _ st i _ _) _ _)
  refine PState.EdgesGrow.trans ?_ (PState.EdgesGrow.foldl (fun st (x : Nat × STerm) => by
    unfold Transfer.addCarried
    exact PState.edgesGrow_update _ _ _) _ _)
  refine PState.EdgesGrow.foldl (fun st x => ?_) _ st
  obtain ⟨n, p⟩ := x
  cases p
  · exact PState.edgesGrow_narrow _ _ _
  · exact PState.edgesGrow_narrowOn _ _ _ _

/-- Whether each item's memberships are edges of the state. -/
def PState.MembersEdges (st : PState) : Prop :=
  ∀ i, ∀ m ∈ (st.items i).memberships, m ∈ st.edges

/-- A step that keeps each item's memberships and every edge keeps
memberships edges. -/
theorem PState.MembersEdges.of_same {st st' : PState} (h : st.MembersEdges) (hl : st.SameLinks st')
    (he : st.EdgesGrow st') : st'.MembersEdges := fun i m hm =>
  he m (h i m (by rw [← (hl i).1]; exact hm))

theorem ordered_go_membersEdges (s : Statement) :
    ∀ (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat), st.MembersEdges →
      (ordered.go s fuel all st order waited).1.MembersEdges
  | 0, _, _, _, _, h => h
  | fuel + 1, all, st, order, waited, h => by
    rcases ordered_go_place s fuel all st order waited with ⟨_, heq⟩ | ⟨next, waited', _, heq⟩
    · rw [heq]; exact h
    · rw [heq]
      refine ordered_go_membersEdges s fuel all _ _ _ ?_
      unfold ordered.placed
      simp only
      refine PState.MembersEdges.of_same (fun i m hm => ?_)
        (PushDown.sameLinks_pushDown s _ _ _ _ _ _) (PushDown.edgesGrow_pushDown s _ _ _ _ _ _)
      simp only [PState.update] at hm ⊢
      split at hm
      · rename_i hi
        subst hi
        rcases List.mem_append.mp hm with hm | hm
        · exact h _ m hm
        · exact (List.mem_filter.mp hm).1
      · exact h i m hm

/-- Each item's memberships are edges of the planned state. -/
theorem plan_membersEdges (s : Statement) : (plan s).state.MembersEdges := by
  unfold plan ordered
  simp only
  refine ordered_go_membersEdges s _ _ _ _ _ ?_
  refine PState.MembersEdges.of_same (st := PState.initial s) (fun i m hm => by
    simp [PState.initial] at hm) ?_ ?_
  · exact PState.SameLinks.foldl (fun st k => sameLinks_readCore s st k) _ _
  · exact PState.EdgesGrow.foldl (fun st k => edgesGrow_readCore s st k) _ _

end SqliveNarrowing
