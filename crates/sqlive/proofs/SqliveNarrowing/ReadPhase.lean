import SqliveNarrowing.Facts
import SqliveNarrowing.ReadOrder

/-! What the planner's state says once it has read every core. The planner
only adds to an item's term, with AND (`PState.TermGrows`), so a term true of
a row in the final state was true of it in every state before. A term the
planner passes into a derived item's arm holds of the arm's item's row in each
binding of the arm whose row matters (`relevant`): the row passes the derived
item's final term, which implies the term passed in, and holds the item's
value in the column the arm passes on. The planner reads a core after every
core below it (`readOrder_postorder`), so when it carries terms across a
core's edges, nothing has passed a term into its derived items' arms yet, and
their items' terms hold of every binding the arms keep. So the state after
the planner reads every core says only what holds of each binding a core
keeps that matters (`plan_readSem`). -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-! Terms only grow. -/

/-- `st'` holds each item `st` holds a term for to a term at least as
strong. -/
def PState.TermGrows (st st' : PState) : Prop :=
  ∀ i t, (st.items i).term = some t → ∃ t', (st'.items i).term = some t' ∧
    ∀ r, t'.holds r = true → t.holds r = true

theorem PState.TermGrows.refl (st : PState) : st.TermGrows st :=
  fun _ t h => ⟨t, h, fun _ hr => hr⟩

theorem PState.TermGrows.trans {a b c : PState} (hab : a.TermGrows b) (hbc : b.TermGrows c) :
    a.TermGrows c := by
  intro i t ht
  obtain ⟨t1, h1, i1⟩ := hab i t ht
  obtain ⟨t2, h2, i2⟩ := hbc i t1 h1
  exact ⟨t2, h2, fun r hr => i1 r (i2 r hr)⟩

theorem PState.TermGrows.foldl {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), st.TermGrows (g st x)) :
    ∀ (l : List α) (st : PState), st.TermGrows (l.foldl g st)
  | [], st => PState.TermGrows.refl st
  | x :: l, st => (hg st x).trans (PState.TermGrows.foldl hg l (g st x))

/-- A fold whose steps grow keeps a property each step keeps when what it
leaves grows to `Fin`, from a state whose fold grows to `Fin`. -/
theorem PState.TermGrows.foldl_until {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), st.TermGrows (g st x)) {Fin : PState} {Q : PState → Prop} :
    ∀ (l : List α), (∀ st x, x ∈ l → Q st → (g st x).TermGrows Fin → Q (g st x)) →
      ∀ st, Q st → (l.foldl g st).TermGrows Fin → Q (l.foldl g st)
  | [], _, _, h, _ => h
  | x :: l, hstep, st, h, hfin =>
    PState.TermGrows.foldl_until hg l (fun st' y hy => hstep st' y (List.mem_cons_of_mem x hy)) _
      (hstep st x List.mem_cons_self h ((PState.TermGrows.foldl hg l (g st x)).trans hfin)) hfin

theorem PState.termGrows_update {st : PState} {i : Nat} {f : PItem → PItem}
    (hf : (f (st.items i)).term = (st.items i).term) : st.TermGrows (st.update i f) := by
  intro j t ht
  refine ⟨t, ?_, fun _ hr => hr⟩
  simp only [PState.update]
  split
  · rename_i hji
    subst hji
    rw [hf]
    exact ht
  · exact ht

/-- Adding a term to an item leaves it a term that implies the one added. -/
theorem PState.addTerm_holds (st : PState) (i : Nat) (t : STerm) :
    ∃ x, ((st.addTerm i t).items i).term = some x ∧ ∀ r, x.holds r = true → t.holds r = true := by
  unfold PState.addTerm
  simp only [PState.update, ↓reduceIte]
  split
  · exact ⟨_, rfl, fun r hr => by simp only [STerm.holds, Bool.and_eq_true] at hr; exact hr.2⟩
  · exact ⟨_, rfl, fun _ hr => hr⟩

theorem PState.termGrows_addTerm (st : PState) (i : Nat) (t : STerm) :
    st.TermGrows (st.addTerm i t) := by
  intro j u hu
  unfold PState.addTerm
  simp only [PState.update]
  split
  · rename_i hji
    subst hji
    rw [hu]
    exact ⟨_, rfl, fun r hr => by simp only [STerm.holds, Bool.and_eq_true] at hr; exact hr.1⟩
  · exact ⟨u, hu, fun _ hr => hr⟩

theorem PState.termGrows_add (st : PState) (n : Narrowing) : st.TermGrows (st.add n).1 := by
  simp only [PState.add]
  exact (PState.TermGrows.foldl (fun st (x : Nat × STerm) => PState.termGrows_addTerm st x.1 x.2) _ _).trans
    ((PState.TermGrows.foldl (g := fun st (d : Disjunction) =>
      st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
      (fun st d => PState.termGrows_update rfl) _ _).trans (fun _ t h => ⟨t, h, fun _ hr => hr⟩))

theorem PState.termGrows_narrow : ∀ (fuel : Nat) (st : PState) (n : Narrowing),
    st.TermGrows (PState.narrow fuel st n)
  | 0, st, _ => PState.TermGrows.refl st
  | fuel + 1, st, n => by
    simp only [PState.narrow]
    refine (PState.termGrows_add st _).trans (PState.TermGrows.foldl (fun st' i => ?_) _ _)
    split
    · exact (PState.termGrows_update rfl).trans (PState.termGrows_narrow fuel _ _)
    · exact PState.TermGrows.refl _

theorem PState.termGrows_joinInner (fuel : Nat) (st : PState) (i : Nat) :
    st.TermGrows (st.joinInner fuel i) := by
  unfold PState.joinInner
  split
  · exact (PState.termGrows_update rfl).trans (PState.termGrows_narrow fuel _ _)
  · exact PState.TermGrows.refl _

theorem PState.termGrows_narrowOn (fuel : Nat) (st : PState) (i : Nat) (n : Narrowing) :
    st.TermGrows (st.narrowOn fuel i n) := by
  unfold PState.narrowOn
  generalize n.split i = x
  obtain ⟨own, others⟩ := x
  simp only
  refine (PState.termGrows_add st own).trans ?_
  split
  · exact PState.termGrows_update rfl
  · exact PState.termGrows_narrow fuel _ _

theorem Transfer.termGrows_carry (s : Statement) (st : PState) (k : Nat) :
    st.TermGrows (Transfer.carry s st k) :=
  PState.TermGrows.foldl (fun st (x : Nat × STerm) => by
    unfold Transfer.addCarried
    exact PState.termGrows_update rfl) _ st

theorem PushDown.termGrows_pushDown (s : Statement) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm) (edges : List Edge),
      st.TermGrows (PushDown.pushDown s narrowFuel fuel st item term edges)
  | 0, st, _, _, _ => PState.TermGrows.refl st
  | fuel + 1, st, item, term, edges => by
    simp only [PushDown.pushDown]
    split
    · exact PState.TermGrows.refl st
    · split
      · exact PState.TermGrows.refl st
      · refine PState.TermGrows.foldl (fun st0 arm => ?_) _ st
        refine PState.TermGrows.foldl (fun st1 target => ?_) _ st0
        refine PState.TermGrows.trans ?_ (PushDown.termGrows_pushDown s narrowFuel fuel _ _ _ _)
        have h2 : ∀ (renamed : Option STerm), st1.TermGrows (match renamed with
            | some r => st1.addTerm target r
            | none => st1) := by
          intro renamed
          cases renamed
          · exact PState.TermGrows.refl st1
          · exact PState.termGrows_addTerm st1 target _
        have hrec : ∀ (st2 : PState) (passed : List Edge),
            st2.TermGrows { st2 with edges := st2.edges ++ passed, pushed := st2.pushed ++ passed } :=
          fun _ _ _ t h => ⟨t, h, fun _ hr => hr⟩
        split
        · exact (h2 _).trans ((hrec _ _).trans (PState.termGrows_joinInner _ _ _))
        · exact (h2 _).trans (hrec _ _)

theorem termGrows_readCore (s : Statement) (st : PState) (k : Nat) :
    st.TermGrows (readCore s st k) := by
  unfold readCore
  simp only
  refine PState.TermGrows.trans ?_ (PState.TermGrows.foldl
    (fun st i => PushDown.termGrows_pushDown s _ _ st i _ _) _ _)
  refine PState.TermGrows.trans ?_ (Transfer.termGrows_carry s _ k)
  refine PState.TermGrows.foldl (fun st x => ?_) _ st
  obtain ⟨n, p⟩ := x
  cases p
  · exact PState.termGrows_narrow _ _ _
  · exact PState.termGrows_narrowOn _ _ _ _

/-! No edge passes into a derived item's arm while the planner reads cores. -/

theorem PState.add_pushed (st : PState) (n : Narrowing) : (st.add n).1.pushed = st.pushed := by
  simp only [PState.add]
  have hterms : ∀ (l : List (Nat × STerm)) (st : PState),
      (l.foldl (fun st (x : Nat × STerm) => st.addTerm x.1 x.2) st).pushed = st.pushed := by
    intro l
    induction l with
    | nil => intro st; rfl
    | cons x l ih => intro st; rw [List.foldl_cons, ih]; rfl
  have hors : ∀ (l : List Disjunction) (st : PState),
      (l.foldl (fun st (d : Disjunction) =>
        st.update d.item fun p => { p with ors := p.ors ++ [d.sides] }) st).pushed = st.pushed := by
    intro l
    induction l with
    | nil => intro st; rfl
    | cons x l ih => intro st; rw [List.foldl_cons, ih]; rfl
  rw [hors, hterms]

theorem PState.narrow_pushed : ∀ (fuel : Nat) (st : PState) (n : Narrowing),
    (PState.narrow fuel st n).pushed = st.pushed
  | 0, _, _ => rfl
  | fuel + 1, st, n => by
    simp only [PState.narrow]
    refine foldl_invariant (P := fun x : PState => x.pushed = st.pushed) _ (fun x i _ hx => ?_) _
      (PState.add_pushed st _)
    split
    · rw [PState.narrow_pushed]; exact hx
    · exact hx

theorem PState.joinInner_pushed (fuel : Nat) (st : PState) (i : Nat) :
    (st.joinInner fuel i).pushed = st.pushed := by
  unfold PState.joinInner
  split
  · rw [PState.narrow_pushed]; rfl
  · rfl

theorem PState.narrowOn_pushed (fuel : Nat) (st : PState) (i : Nat) (n : Narrowing) :
    (st.narrowOn fuel i n).pushed = st.pushed := by
  unfold PState.narrowOn
  generalize n.split i = x
  obtain ⟨own, others⟩ := x
  simp only
  split
  · show ((st.add own).1).pushed = st.pushed
    exact PState.add_pushed st own
  · rw [PState.narrow_pushed, PState.add_pushed]

theorem Transfer.carry_pushed (s : Statement) (st : PState) (k : Nat) :
    (Transfer.carry s st k).pushed = st.pushed := by
  unfold Transfer.carry
  generalize Transfer.carried s st k = l
  induction l generalizing st with
  | nil => rfl
  | cons x l ih => rw [List.foldl_cons, ih]; rfl

/-- Passing a term, and no edge, into a derived item's arms passes no
edge. -/
theorem PushDown.pushDown_pushed (s : Statement) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm),
      (PushDown.pushDown s narrowFuel fuel st item term []).pushed = st.pushed
  | 0, _, _, _ => rfl
  | fuel + 1, st, item, term => by
    simp only [PushDown.pushDown]
    split
    · rfl
    · split
      · rfl
      · rename_i found _ _
        have hfold : ∀ {α : Type} (l : List α) (g : PState → α → PState),
            (∀ st x, (g st x).pushed = st.pushed) → ∀ st, (l.foldl g st).pushed = st.pushed := by
          intro α l g hg
          induction l with
          | nil => intro st; rfl
          | cons x l ih => intro st; rw [List.foldl_cons, ih, hg]
        refine hfold _ _ (fun st0 arm => hfold _ _ (fun st1 target => ?_) st0) st
        simp only [List.filter_nil, List.filterMap_nil, List.append_nil, List.isEmpty_nil,
          Bool.not_true, Bool.or_false]
        rw [PushDown.pushDown_pushed]
        split
        · rw [PState.joinInner_pushed]
          split <;> rfl
        · split <;> rfl

theorem readCore_pushed (s : Statement) (st : PState) (k : Nat) :
    (readCore s st k).pushed = st.pushed := by
  unfold readCore
  simp only
  have hfold : ∀ {α : Type} (l : List α) (g : PState → α → PState),
      (∀ st x, (g st x).pushed = st.pushed) → ∀ st, (l.foldl g st).pushed = st.pushed := by
    intro α l g hg
    induction l with
    | nil => intro st; rfl
    | cons x l ih => intro st; rw [List.foldl_cons, ih, hg]
  rw [hfold _ _ (fun st i => PushDown.pushDown_pushed s _ _ st i _), Transfer.carry_pushed,
    hfold _ _ (fun st x => by
      obtain ⟨n, p⟩ := x
      cases p
      · exact PState.narrow_pushed _ _ _
      · exact PState.narrowOn_pushed _ _ _ _)]

/-! A term passed into a derived item's arms. -/

/-- The cells of row `r` that arm `k` passes on from binding `b`: each result
column that is a bare column of an item, and in an arm that aggregates one
the GROUP BY lists, holds that column's value in `b`. -/
def Statement.PassedCells (s : Statement) (k : Nat) (r : Row) (b : Bind) : Prop :=
  ∀ c j c', (s.core k).output[c]? = some (.expr (.col j c')) →
    ((s.core k).aggregated = false ∨ (s.core k).groupBy.contains (.col j c') = true) →
    r.cell c = cellOf (b j) c'

/-- A binding that matters, of an arm that calls no window function, holds
the values of a row its query's test keeps in the columns the arm passes
on. -/
theorem relevant_cells {s : Statement} {rel : Nat → Row → Bool} {q k : Nat}
    {E : Bind → Nat → List Row} {b : Bind} (h : relevant s rel q k E b = true)
    (hwin : (s.core k).windowed = false) : ∃ r, rel q r = true ∧ s.PassedCells k r b := by
  unfold relevant at h
  rw [hwin, Bool.false_or] at h
  split at h
  · rename_i hagg
    unfold keyMatters at h
    obtain ⟨r, hr, hcells⟩ := of_decide_eq_true h
    refine ⟨r, hr, fun c j c' hc hg => ?_⟩
    have hmem : Expr.col j c' ∈ (s.core k).groupBy := by
      rcases hg with hg | hg
      · rw [hagg] at hg; cases hg
      · exact List.contains_iff_mem.mp hg
    obtain ⟨n, hn, hget⟩ := List.getElem_of_mem hmem
    have := hcells c n j c' hc (by rw [List.getElem?_eq_getElem hn, hget])
    simp only [Statement.groupKey, List.getElem?_map, List.getElem?_eq_getElem hn, hget,
      Option.map_some, Option.some.injEq] at this
    exact this.symm
  · exact ⟨_, h, fun c j c' hc _ => by rw [Core.outRow_cell hc]; rfl⟩

/-- An item a core's result column passes on unchanged, of the core's own,
is one the core lists: a core's clauses name only items of the core and of
the cores it sits in. -/
theorem Statement.output_listed {s : Statement} (h : s.wellFormed = true) {k j c c' : Nat}
    (hk : k < s.cores.length) (hcore : (s.item j).core = k)
    (hout : (s.core k).output[c]? = some (.expr (.col j c'))) : j ∈ (s.core k).items := by
  have hp : (j, c') ∈ (s.core k).refs := by
    unfold Core.refs
    refine List.mem_append_left _ (List.mem_append_right _ (List.mem_flatMap.mpr
      ⟨_, List.mem_of_getElem? hout, ?_⟩))
    simp [OutCol.refs, OutCol.exprs, Expr.refs]
  obtain ⟨k', _, hjk'⟩ := (s.wellFormed_parts h).refs k hk _ hp
  have := ((s.wellFormed_parts h).items k' (s.core_lt hjk') j hjk').2
  rw [hcore] at this
  subst this
  exact hjk'

theorem PState.Sem.weaken {s : Statement} {P P' : Cls} {st : PState} (hs : st.Sem s P)
    (h : ∀ Z f k b, P' Z f k b → P Z f k b) : st.Sem s P' :=
  ⟨fun Z f k b hb => hs.rows Z f k b (h _ _ _ _ hb), fun Z f b i r hb => hs.terms Z f b i r (h _ _ _ _ hb),
    fun Z f b i r hb => hs.carried Z f b i r (h _ _ _ _ hb),
    fun Z f b e he hb => hs.edges Z f b e he (h _ _ _ _ hb),
    fun Z f b i hb => hs.ors Z f b i (h _ _ _ _ hb), fun i n hn => ⟨(hs.held i n hn).1,
      (hs.held i n hn).2.1, fun Z f b hb => (hs.held i n hn).2.2 Z f b (h _ _ _ _ hb)⟩⟩

/-- One target's step of passing a term, and no edge, into an arm keeps the
invariant, when the term it passes holds of the target's row in every binding
of the class and a row of NULLs fails it where the target is still a LEFT
JOIN's: the target then has a row in every such binding. -/
theorem PState.Sem.pushStep {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) {narrowFuel fuel : Nat} {Fin : PState}
    {st : PState} (hs : st.Sem s P) {target : Nat} {renamed : Option STerm}
    (hleft : ∀ r, renamed = some r → (st.items target).leftOn.isSome = true → r.mayHoldOnNulls = false)
    (hholds : ∀ r, renamed = some r → ∀ Z f b, P Z f (s.item target).core b → r.holds (b target) = true)
    (ih : ∀ X : PState, X.Sem s P → (PushDown.pushDown s narrowFuel fuel X target renamed []).TermGrows Fin →
      (∀ u, renamed = some u → ∀ row, (∀ t, (Fin.items target).term = some t → t.holds (some row) = true) →
        u.holds (some row) = true) →
      (PushDown.pushDown s narrowFuel fuel X target renamed []).Sem s P)
    (hfin : (PushDown.pushDown s narrowFuel fuel
      (if renamed.isSome || !([] : List Edge).isEmpty then
        PState.joinInner narrowFuel
          { (match renamed with
              | some r => st.addTerm target r
              | none => st) with
            edges := (match renamed with
              | some r => st.addTerm target r
              | none => st).edges ++ ([] : List Edge),
            pushed := (match renamed with
              | some r => st.addTerm target r
              | none => st).pushed ++ ([] : List Edge) } target
      else
        { (match renamed with
            | some r => st.addTerm target r
            | none => st) with
          edges := (match renamed with
            | some r => st.addTerm target r
            | none => st).edges ++ ([] : List Edge),
          pushed := (match renamed with
            | some r => st.addTerm target r
            | none => st).pushed ++ ([] : List Edge) }) target renamed []).TermGrows Fin) :
    (PushDown.pushDown s narrowFuel fuel
      (if renamed.isSome || !([] : List Edge).isEmpty then
        PState.joinInner narrowFuel
          { (match renamed with
              | some r => st.addTerm target r
              | none => st) with
            edges := (match renamed with
              | some r => st.addTerm target r
              | none => st).edges ++ ([] : List Edge),
            pushed := (match renamed with
              | some r => st.addTerm target r
              | none => st).pushed ++ ([] : List Edge) } target
      else
        { (match renamed with
            | some r => st.addTerm target r
            | none => st) with
          edges := (match renamed with
            | some r => st.addTerm target r
            | none => st).edges ++ ([] : List Edge),
          pushed := (match renamed with
            | some r => st.addTerm target r
            | none => st).pushed ++ ([] : List Edge) }) target renamed []).Sem s P := by
  have heta : ∀ x : PState, ({ x with edges := x.edges ++ [], pushed := x.pushed ++ [] } : PState) = x := by
    intro x; cases x; simp
  simp only [heta, List.isEmpty_nil, Bool.not_true, Bool.or_false] at hfin ⊢
  cases renamed with
  | none =>
    simp only [Option.isSome_none, Bool.false_eq_true, ↓reduceIte] at hfin ⊢
    exact ih st hs hfin fun u hu => by cases hu
  | some r =>
    simp only [Option.isSome_some, ↓reduceIte] at hfin ⊢
    have hs2 : (st.addTerm target r).Sem s P := hs.addTerm fun Z f b r0 hb hr0 => by
      have := hholds r rfl Z f b hb
      rwa [hr0] at this
    have hX := hs2.joinInner_of h hP (PState.Sem.narrow h hP narrowFuel) fun Z f b hb hl hnone => by
      have hl' : (st.items target).leftOn.isSome = true := by
        have : ((st.addTerm target r).items target).leftOn = (st.items target).leftOn := by
          simp [PState.addTerm, PState.update]
        rw [← this]
        cases hx : ((st.addTerm target r).items target).leftOn with
        | none => exact absurd hx hl
        | some _ => rfl
      have := hholds r rfl Z f b hb
      rw [hnone, STerm.holds_none (hleft r rfl hl')] at this
      cases this
    refine ih _ hX hfin fun u hu row hrow => ?_
    cases hu
    obtain ⟨x, hx, hxr⟩ := PState.addTerm_holds st target r
    have hg : (st.addTerm target r).TermGrows Fin := (PState.termGrows_joinInner _ _ _).trans
      ((PushDown.termGrows_pushDown s narrowFuel fuel _ _ _ _).trans hfin)
    obtain ⟨t', ht', hti⟩ := hg target x hx
    exact hxr _ (hti _ (hrow t' ht'))

/-- Passing a term, and no edge, into a derived item's arms keeps the
invariant, when every binding of the class of an arm's core that calls no
window function holds, in the columns the arm passes on, the values of a row
that matters to the derived item, of which the derived item's final term,
and so the term passed in, holds. -/
theorem PState.Sem.pushDown {s : Statement} (h : s.wellFormed = true) {P : Cls}
    (hP : ∀ Z f k b, P Z f k b → s.KeptIn Z f k b) {Fin : PState} {Src : Nat → Prop}
    {Good : Nat → Row → Prop}
    (hGood : ∀ i r, Good i r → ∀ u, (Fin.items i).term = some u → u.holds (some r) = true)
    (hcls : ∀ Z f k b d q v, Src d → (s.item d).kind = .derived q v → k ∈ (s.query q).arms →
      (s.core k).windowed = false → P Z f k b → ∃ r, Good d r ∧ s.PassedCells k r b)
    (hsrc : ∀ d q v k t, Src d → (s.item d).kind = .derived q v → k ∈ (s.query q).arms →
      t ∈ (s.core k).items → Src t) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm), Src item → st.Sem s P →
      (PushDown.pushDown s narrowFuel fuel st item term []).TermGrows Fin →
      (∀ u, term = some u → ∀ r, (∀ t, (Fin.items item).term = some t → t.holds (some r) = true) →
        u.holds (some r) = true) →
      (PushDown.pushDown s narrowFuel fuel st item term []).Sem s P
  | 0, st, _, _, _, hs, _, _ => hs
  | fuel + 1, st, item, term, hsrc0, hs, hfin, hterm => by
    have ih := PState.Sem.pushDown h hP hGood hcls hsrc narrowFuel fuel
    revert hfin
    simp only [PushDown.pushDown]
    split
    · intro _; exact hs
    · rename_i found hfound
      split
      · intro _; exact hs
      · intro hfin
        have hstep : ∀ (st1 : PState) (target : Nat) (renamed : Option STerm),
            st1.TermGrows (PushDown.pushDown s narrowFuel fuel
              (if renamed.isSome || !([] : List Edge).isEmpty then
                PState.joinInner narrowFuel
                  { (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1) with
                    edges := (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1).edges ++ ([] : List Edge),
                    pushed := (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1).pushed ++ ([] : List Edge) } target
              else
                { (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1) with
                  edges := (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1).edges ++ ([] : List Edge),
                  pushed := (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1).pushed ++ ([] : List Edge) }) target renamed []) := by
          intro st1 target renamed
          refine PState.TermGrows.trans ?_ (PushDown.termGrows_pushDown s narrowFuel fuel _ _ _ _)
          have h2 : st1.TermGrows (match renamed with
              | some r => st1.addTerm target r
              | none => st1) := by
            cases renamed
            · exact PState.TermGrows.refl st1
            · exact PState.termGrows_addTerm st1 target _
          have hrec : ∀ (st2 : PState), st2.TermGrows
              { st2 with edges := st2.edges ++ ([] : List Edge), pushed := st2.pushed ++ ([] : List Edge) } :=
            fun _ _ t h => ⟨t, h, fun _ hr => hr⟩
          split
          · exact h2.trans ((hrec _).trans (PState.termGrows_joinInner _ _ _))
          · exact h2.trans (hrec _)
        simp only [List.filter_nil, List.filterMap_nil] at hfin hstep ⊢
        refine PState.TermGrows.foldl_until (Q := fun x => x.Sem s P)
          (fun st0 arm => PState.TermGrows.foldl (fun st1 target => hstep st1 target _) _ st0) _
          (fun st0 arm harm hs0 hfin0 => ?_) st hs hfin
        obtain ⟨q, v, k, hkind, _, _, hk, hwin, hthrough⟩ := PushDown.mem_arms hfound harm
        have hklt := ((s.wellFormed_parts h).arms q (s.query_lt hk) k hk).1
        refine PState.TermGrows.foldl_until (Q := fun x => x.Sem s P)
          (fun st1 target => hstep st1 target _) _ (fun st1 target htarget hs1 hfin1 => ?_) st0 hs0 hfin0
        -- The target is an item of the arm the arm passes a column on from.
        have hthr : ∀ c c', (arm[c]?).join = some (target, c') → (s.item target).core = k ∧
            ((s.core k).aggregated = false ∨ (s.core k).groupBy.contains (.col target c') = true) ∧
            (s.core k).output[c]? = some (OutCol.expr (Expr.col target c')) :=
          fun c c' hc => hthrough c target c' hc
        obtain ⟨c0, _, hc0⟩ := List.mem_filterMap.mp (List.mem_eraseDups.mp htarget)
        obtain ⟨⟨t0, c0'⟩, hc0', ht0⟩ := Option.map_eq_some_iff.mp hc0
        simp only at ht0
        subst ht0
        obtain ⟨hcore, _, hout⟩ := hthr c0 c0' hc0'
        have hlisted := s.output_listed h hklt hcore hout
        refine hs1.pushStep h hP (fun r hr hl => ?_) (fun r hr Z f b hb => ?_) (fun X hX hXfin hXterm =>
          ih X t0 _ (hsrc item q v k t0 hsrc0 hkind hk hlisted) hX hXfin hXterm) hfin1
        · simp only [Option.filter, Option.bind] at hr
          split at hr
          · split at hr
            · rename_i hkeep
              obtain rfl := Option.some.inj hr
              simp only [hl, Bool.true_and, Bool.not_eq_true'] at hkeep
              exact hkeep
            · cases hr
          · cases hr
        · rw [hcore] at hb
          obtain ⟨row, hgood, hcells⟩ := hcls Z f k b item q v hsrc0 hkind hk hwin hb
          simp only [Option.filter, Option.bind] at hr
          split at hr
          · rename_i u' hren
            split at hr
            · obtain rfl := Option.some.inj hr
              cases hterm0 : term with
              | none => rw [hterm0] at hren; cases hren
              | some u =>
                rw [hterm0] at hren
                simp only at hren
                have hu := hterm u hterm0 row (hGood item row hgood)
                refine PushDown.renamed_sound hren (fun c c' hc => ?_) hu
                split at hc
                · rename_i t c'' hthc
                  split at hc
                  · rename_i htt
                    subst htt
                    obtain rfl := Option.some.inj hc
                    obtain ⟨_, hagg, hout'⟩ := hthr c c'' hthc
                    exact hcells c t c'' hout' hagg
                  · cases hc
                · cases hc
            · cases hr
          · cases hr

/-- A binding a join keeps holds, of each item it joins, a row of the item's
rows or none. -/
theorem joinItems_rowOf {s : Statement} {c : Core} {rowsOf : Nat → List Row} {subs : Bind → Subs} :
    ∀ (is : List Nat) (L : List Bind), is.Nodup → ∀ b ∈ joinItems s c rowsOf subs is L,
      ∀ i ∈ is, ∀ r, b i = some r → r ∈ rowsOf i
  | [], _, _, _, _, i, hi, _, _ => by cases hi
  | a :: rest, L, hnd, b, hb, i, hi, r, hbi => by
    rw [View.joinItems_cons] at hb
    have har : a ∉ rest := (List.nodup_cons.mp hnd).1
    by_cases hia : i = a
    · subst hia
      obtain ⟨z, hz, hbz⟩ := joinItems_origin rest _ b hb
      obtain ⟨y0, _, hzs⟩ := List.mem_flatMap.mp hz
      rw [hbz i har] at hbi
      unfold View.joinStep at hzs
      simp only at hzs
      split at hzs
      · simp only [List.mem_singleton] at hzs
        subst hzs
        simp [Bind.set] at hbi
      · obtain ⟨r', hr', hz'⟩ := List.mem_filterMap.mp hzs
        split at hz'
        · obtain rfl := Option.some.inj hz'
          simp only [Bind.set, ↓reduceIte, Option.some.injEq] at hbi
          subst hbi
          exact hr'
        · cases hz'
    · exact joinItems_rowOf rest _ (List.nodup_cons.mp hnd).2 b hb i
        ((List.mem_cons.mp hi).resolve_left hia) r hbi

/-- A binding core `k` keeps holds, of a derived item of `k`, a row its
query returns under the binding `k`'s items join onto. -/
theorem Statement.derived_row {s : Statement} (h : s.wellFormed = true) {Z : Db}
    {E : Bind → Nat → List Row} {o : Bind} {k : Nat} {b : Bind} (hb : b ∈ coreBindings s Z E o k)
    {i q : Nat} {v : Bool} (hi : i ∈ (s.core k).items) (hkind : (s.item i).kind = .derived q v)
    {r : Row} (hr : b i = some r) : r ∈ E (s.outerOf k o) q := by
  have hnd := (s.wellFormed_parts h).nodup k (s.core_lt hi)
  have := joinItems_rowOf _ _ hnd b (List.mem_filter.mp hb).1 i hi r hr
  simpa [hkind, Statement.outerOf] using this

/-! Reading a core, and every core. -/

/-- The bindings the planner's state speaks of once it has read the cores
`done`: bindings a core keeps, which matter when they sit below a core of
`done`, whose reading passed terms into the arms below it. -/
noncomputable def readCls (s : Statement) (rel : Nat → Row → Bool) (done : List Nat) : Cls :=
  fun Z f k b => s.KeptIn Z f k b ∧ ((∃ a ∈ done, s.Below k a) →
    ∀ q, k ∈ (s.query q).arms → relevant s rel q k (evalQuery s Z f) b = true)

/-- A derived item's row, in a binding its core keeps that holds a row of
the item, passes what its arms say of each of its columns that isn't NULL,
when the state says only what holds of every binding its arms keep. -/
theorem PState.Sem.pull {s : Statement} (h : s.wellFormed = true) {rel : Nat → Row → Bool}
    {done : List Nat} {st : PState} (hs : st.Sem s (readCls s rel done)) {k : Nat}
    (hk : k ∉ done ∧ ∀ a ∈ done, ¬s.Below k a) :
    ∀ Z f b i r y t, (s.item i).core = k → i ∈ (s.core k).items → readCls s rel done Z f k b →
      b i = some r → r.cell y ≠ none → Transfer.pulled s st i y = some t → t.holds (some r) = true := by
  intro Z f b i r y t _ hi hb hbi hy hp
  obtain ⟨⟨q, o, hkq, ho, hkept⟩, _⟩ := hb
  cases hkind : (s.item i).kind with
  | table _ => simp [Transfer.pulled, hkind] at hp
  | derived q' v =>
    have hr := s.derived_row h hkept hi hkind hbi
    cases f with
    | zero => simp [evalQuery] at hr
    | succ f' =>
      refine Transfer.pulled_sound hkind hp hr (fun k' hk' b' hb' j u r0 hj hu hr0 => ?_) hy
      refine hs.terms Z f' b' j r0 ?_ hr0 u hu
      rw [hj]
      refine ⟨⟨q', s.outerOf k o, hk', s.outer_derived h hkq ho hi hkind, hb'⟩, fun ⟨a, ha, hba⟩ => ?_⟩
      have harm : s.ArmOf k' k :=
        ⟨q', List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩), hk'⟩
      rcases hba.chain h harm with rfl | hka
      · exact absurd ha hk.1
      · exact absurd hka (hk.2 a ha)

/-- Reading core `k`, after the cores `done` and before any core it sits
below, keeps the invariant for the bindings that matter below `done` and
`k`, when no edge has passed into a derived item's arm, the state it leaves
grows to `Fin`, and a row a derived item's query's test keeps passes the
derived item's term in `Fin`. -/
theorem PState.Sem.readCore {s : Statement} (h : s.wellFormed = true) {rel : Nat → Row → Bool}
    {Fin : PState}
    (hrel : ∀ d q v, d ∈ (s.core (s.item d).core).items → (s.item d).kind = .derived q v →
      ∀ r, rel q r = true → ∀ u, (Fin.items d).term = some u → u.holds (some r) = true)
    {done : List Nat} {st : PState} (hs : st.Sem s (readCls s rel done)) (hpushed : st.pushed = [])
    {k : Nat} (hk : k ∉ done ∧ ∀ a ∈ done, ¬s.Below k a) (hfin : (readCore s st k).TermGrows Fin) :
    (readCore s st k).Sem s (readCls s rel (done ++ [k])) := by
  have hP : ∀ (done : List Nat) Z f k b, readCls s rel done Z f k b → s.KeptIn Z f k b :=
    fun _ _ _ _ _ hb => hb.1
  unfold SqliveNarrowing.readCore at hfin ⊢
  simp only at hfin ⊢
  refine PState.TermGrows.foldl_until (Q := fun x => x.Sem s (readCls s rel (done ++ [k])))
    (fun st0 i => PushDown.termGrows_pushDown s _ _ st0 i _ _) _ (fun st0 i hi hs0 hfin0 => ?_) _ ?_ hfin
  · -- Each item's term passes into its arms.
    have hcore : (s.item i).core = k := ((s.wellFormed_parts h).items k (s.core_lt hi) i hi).2
    refine PState.Sem.pushDown h (hP _)
      (Src := fun d => d ∈ (s.core (s.item d).core).items ∧
        ∃ a ∈ done ++ [k], (s.item d).core = a ∨ s.Below (s.item d).core a)
      (Good := fun d r => ∀ u, (Fin.items d).term = some u → u.holds (some r) = true)
      (fun _ _ hg => hg) ?_ ?_ s.fuel s.fuel st0 i _ ⟨by rw [hcore]; exact hi, k, by simp, Or.inl hcore⟩
      hs0 hfin0 fun u hu r hr => ?_
    · intro Z f k' b d q v ⟨hdl, a, ha, hda⟩ hkind hk' hwin hb
      have harm : s.ArmOf k' (s.item d).core :=
        ⟨q, List.mem_append_left _ (List.mem_filterMap.mpr ⟨d, hdl, by simp [hkind]⟩), hk'⟩
      have hbelow : s.Below k' a := by
        rcases hda with hda | hda
        · rw [← hda]; exact .arm harm
        · exact (Statement.Below.arm harm).trans hda
      obtain ⟨r, hr, hcells⟩ := relevant_cells (hb.2 ⟨a, ha, hbelow⟩ q hk') hwin
      exact ⟨r, hrel d q v hdl hkind r hr, hcells⟩
    · intro d q v k' t ⟨hdl, a, ha, hda⟩ hkind hk' ht
      have harm : s.ArmOf k' (s.item d).core :=
        ⟨q, List.mem_append_left _ (List.mem_filterMap.mpr ⟨d, hdl, by simp [hkind]⟩), hk'⟩
      have htcore : (s.item t).core = k' := ((s.wellFormed_parts h).items k' (s.core_lt ht) t ht).2
      refine ⟨by rw [htcore]; exact ht, a, ha, Or.inr ?_⟩
      rw [htcore]
      rcases hda with hda | hda
      · rw [← hda]; exact .arm harm
      · exact (Statement.Below.arm harm).trans hda
    · obtain ⟨t', ht', hti⟩ := ((PushDown.termGrows_pushDown s _ _ st0 i _ _).trans hfin0) i u hu
      exact hti _ (hr t' ht')
  · -- The conditions, then the carry.
    have hl : ∀ x ∈ ((s.core k).conds.map fun (cd, p) => (readCondition s k cd, p)).filter
          (fun (n, _) => !n.mayHoldOnNulls) ++
        ((s.core k).conds.map fun (cd, p) => (readCondition s k cd, p)).filter
          (fun (n, _) => n.mayHoldOnNulls),
        ∃ cd, (cd, x.2) ∈ (s.core k).conds ∧ x.1 = readCondition s k cd := by
      intro x hx
      have hx' : x ∈ (s.core k).conds.map fun (cd, p) => (readCondition s k cd, p) := by
        rcases List.mem_append.mp hx with hx | hx <;> exact (List.mem_filter.mp hx).1
      obtain ⟨⟨cd, p⟩, hmem, rfl⟩ := List.mem_map.mp hx'
      exact ⟨cd, hmem, rfl⟩
    have hX := PState.Sem.readConds h (hP done) k _ hl hs
    refine (PState.Sem.carry hX ?_ k (hX.pull h hk)).weaken fun Z f k b hb =>
      ⟨hb.1, fun ⟨a, ha, hba⟩ => hb.2 ⟨a, List.mem_append_left _ ha, hba⟩⟩
    refine foldl_invariant (P := fun x : PState => x.pushed = []) _ ?_ st hpushed
    intro x y _ hx
    obtain ⟨n, p⟩ := y
    cases p
    · exact (PState.narrow_pushed _ _ _).trans hx
    · exact (PState.narrowOn_pushed _ _ _ _).trans hx

/-- Reading the cores `rest` after the cores `done` keeps the invariant. -/
theorem plan_readSem_go {s : Statement} (h : s.wellFormed = true) {rel : Nat → Row → Bool}
    {Fin : PState}
    (hrel : ∀ d q v, d ∈ (s.core (s.item d).core).items → (s.item d).kind = .derived q v →
      ∀ r, rel q r = true → ∀ u, (Fin.items d).term = some u → u.holds (some r) = true) :
    ∀ (rest done : List Nat) (st : PState), done ++ rest = s.readOrder →
      st.Sem s (readCls s rel done) → st.pushed = [] → (rest.foldl (readCore s) st).TermGrows Fin →
      (rest.foldl (readCore s) st).Sem s (readCls s rel s.readOrder)
  | [], done, st, heq, hs, _, _ => by
    simp only [List.append_nil] at heq
    subst heq
    exact hs
  | k :: rest, done, st, heq, hs, hp, hfin => by
    have hpw := readOrder_postorder h
    rw [← heq] at hpw
    have hafter := (List.pairwise_append.mp hpw).2.2
    have hk : k ∉ done ∧ ∀ a ∈ done, ¬s.Below k a :=
      ⟨fun hkd => (hafter k hkd k List.mem_cons_self).1 rfl,
        fun a ha => (hafter a ha k List.mem_cons_self).2⟩
    simp only [List.foldl_cons] at hfin ⊢
    exact plan_readSem_go h hrel rest (done ++ [k]) _ (by simpa using heq)
      (PState.Sem.readCore h hrel hs hp hk
        ((PState.TermGrows.foldl (fun st k => termGrows_readCore s st k) rest _).trans hfin))
      (by rw [readCore_pushed]; exact hp) hfin

/-- Once the planner has read every core, its state says only what holds of
each binding a core keeps, and of each that sits below a core and matters,
when its terms grow to `Fin` and a row a derived item's query's test keeps
passes the derived item's term in `Fin`. -/
theorem plan_readSem {s : Statement} (h : s.wellFormed = true) {rel : Nat → Row → Bool}
    {Fin : PState}
    (hrel : ∀ d q v, d ∈ (s.core (s.item d).core).items → (s.item d).kind = .derived q v →
      ∀ r, rel q r = true → ∀ u, (Fin.items d).term = some u → u.holds (some r) = true)
    (hfin : (s.readOrder.foldl (readCore s) (PState.initial s)).TermGrows Fin) :
    (s.readOrder.foldl (readCore s) (PState.initial s)).Sem s (readCls s rel s.readOrder) :=
  plan_readSem_go h hrel s.readOrder [] _ rfl (PState.Sem.initial h fun _ _ _ _ hb => hb.1) rfl hfin

end SqliveNarrowing
