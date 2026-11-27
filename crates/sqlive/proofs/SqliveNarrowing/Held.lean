import SqliveNarrowing.Kept

/-! What the planner's state says of a LEFT JOIN item it still holds an ON
narrowing back for. While the planner holds one back, it adds to the item
only what the item's own ON says of it (`narrowOn`): any other narrowing or
pass into the item makes it an inner join's first (`narrow`, `joinInner`), and
such a step leaves every item it still holds back as it was
(`PState.Untouched`). The reader's promise holds wherever the condition does,
so each term, edge and OR the state holds of such an item holds at each
binding where the item's ON holds of a row of it (`PState.Held`), and a term
carried to the item from a partner holds there when the partner's row passes
what the planned state says of it. -/
namespace SqliveNarrowing

attribute [local instance] Classical.propDecidable

/-- Whether the ON conditions of the LEFT JOIN that adds item `i` hold of
binding `b` with `i`'s row `r`, their subqueries read on `Z` at fuel `f`. -/
def OnAt (s : Statement) (Z : Db) (f i : Nat) (b : Bind) (r : Row) : Prop :=
  onHolds (s.core (s.item i).core) (evalQuery s Z f) i (b.set i (some r)) = true

/-- Whether the ON of the LEFT JOIN that adds `i` names item `j`. -/
def OnNames (s : Statement) (i j : Nat) : Prop :=
  ∃ cd ∈ (s.core (s.item i).core).onOf i, j ∈ cd.items

/-- What an edge from `i` that `i`'s ON gives says of its partner: a lookup
reads it alone; the edge reads through no derived item (`Narrowing.split`);
outside a subquery, the partner is another item the ON names; inside one, it
sits in an arm of a subquery of one of the ON's conditions. -/
def Edge.OnPartner (s : Statement) (i : Nat) (e : Edge) : Prop :=
  (s.item e.partner).readable = true ∧ e.via = none ∧
  (e.nested = false → e.partner ≠ i ∧ OnNames s i e.partner) ∧
  (e.nested = true → ∃ cd ∈ (s.core (s.item i).core).onOf i, ∃ q ∈ cd.queries,
    (s.item e.partner).core ∈ (s.query q).arms)

/-- What a row `r` of item `j` holds that the planned state `Fin` asks of it:
`j`'s term, and, for an item a lookup reads alone, a place among the rows a
lookup reads. -/
def HeldGood (s : Statement) (Fin : PState) (j : Nat) (r : Row) : Prop :=
  (∀ u, (Fin.items j).term = some u → u.holds (some r) = true) ∧
  ((s.item j).readable = true → ∃ Z, r ∈ lookupRows s Z j)

/-- A binding around a row of a held item `i`: each other item's row that
`i`'s ON names holds what `Fin` asks of it, and each other item `i`'s ON
names that no LEFT JOIN adds has a row. -/
def HeldAround (s : Statement) (Fin : PState) (i : Nat) (b : Bind) : Prop :=
  (∀ j r, j ≠ i → OnNames s i j → b j = some r → HeldGood s Fin j r) ∧
  ∀ j, j ≠ i → OnNames s i j → s.alwaysBound j = true → b j ≠ none

/-- What state `st` says of each item it holds an ON narrowing back for, at
each binding where the item's ON holds of a row of it: its term holds of the
row, so does its carried term when the binding's other rows hold what `Fin`
asks of them, and each edge from it and each OR of it holds, an edge with a
partner its ON gives. -/
structure PState.Held (s : Statement) (st : PState) (Fin : PState) : Prop where
  term : ∀ i t, (st.items i).leftOn.isSome = true → (st.items i).term = some t →
    ∀ Z f b r, OnAt s Z f i b r → t.holds (some r) = true
  carried : ∀ i t, (st.items i).leftOn.isSome = true → (st.items i).carried = some t →
    ∀ Z f b r, OnAt s Z f i b r → HeldAround s Fin i b → t.holds (some r) = true
  edges : ∀ i, (st.items i).leftOn.isSome = true → ∀ e ∈ st.edges, e.member = i →
    e.OnPartner s i ∧ ∀ Z f b r, OnAt s Z f i b r → e.HoldsIn s Z f (b.set i (some r))
  ors : ∀ i, (st.items i).leftOn.isSome = true → ∀ sides ∈ (st.items i).ors,
    (∀ side ∈ sides, ∀ e ∈ side.edges, e.member = i ∧ e.OnPartner s i) ∧
    ∀ Z f b r, OnAt s Z f i b r → Disjunction.HoldsIn s Z f (b.set i (some r)) ⟨i, sides⟩

theorem PState.Held.initial (s : Statement) (Fin : PState) : (PState.initial s).Held s Fin :=
  ⟨fun _ _ _ ht => by simp [PState.initial] at ht, fun _ _ _ ht => by simp [PState.initial] at ht,
    fun _ _ e he => by simp [PState.initial] at he, fun _ _ sides hs => by simp [PState.initial] at hs⟩

/-! Steps that leave each held item as it was. -/

/-- Each item `st'` holds an ON narrowing back for is as `st` holds it, with
no edge from it that `st` lacks. -/
def PState.Untouched (st st' : PState) : Prop :=
  ∀ i, (st'.items i).leftOn.isSome = true → st'.items i = st.items i ∧
    ∀ e ∈ st'.edges, e.member = i → e ∈ st.edges

theorem PState.Untouched.refl (st : PState) : st.Untouched st := fun _ _ => ⟨rfl, fun _ h _ => h⟩

theorem PState.Untouched.trans {a b c : PState} (hab : a.Untouched b) (hbc : b.Untouched c) :
    a.Untouched c := by
  intro i hi
  obtain ⟨h1, h2⟩ := hbc i hi
  have hb : (b.items i).leftOn.isSome = true := by rw [← h1]; exact hi
  obtain ⟨h3, h4⟩ := hab i hb
  exact ⟨h1.trans h3, fun e he hm => h4 e (h2 e he hm) hm⟩

theorem PState.Untouched.foldl {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), st.Untouched (g st x)) :
    ∀ (l : List α) (st : PState), st.Untouched (l.foldl g st)
  | [], st => PState.Untouched.refl st
  | x :: l, st => (hg st x).trans (PState.Untouched.foldl hg l (g st x))

theorem PState.Held.untouched {s : Statement} {Fin st st' : PState} (h : st.Held s Fin)
    (hu : st.Untouched st') : st'.Held s Fin := by
  refine ⟨fun i t hi ht => ?_, fun i t hi ht => ?_, fun i hi e he hm => ?_, fun i hi sides hs => ?_⟩
  all_goals obtain ⟨heq, hedges⟩ := hu i hi
  all_goals rw [heq] at hi
  · rw [heq] at ht; exact h.term i t hi ht
  · rw [heq] at ht; exact h.carried i t hi ht
  · exact h.edges i hi e (hedges e he hm) hm
  · rw [heq] at hs; exact h.ors i hi sides hs

/-- Releasing an item leaves every item still held as it was. -/
theorem PState.untouched_release (st : PState) (j : Nat) :
    st.Untouched (st.update j fun p => { p with leftOn := none }) := by
  intro i hi
  by_cases hij : i = j
  · subst hij
    simp [PState.update] at hi
  · exact ⟨by simp [PState.update, hij], fun e he _ => he⟩

/-- Adding a narrowing leaves each item it doesn't narrow as it was. -/
theorem PState.add_other (st : PState) (n : Narrowing) {i : Nat} (hi : i ∉ (st.add n).2) :
    (st.add n).1.items i = st.items i ∧ ∀ e ∈ (st.add n).1.edges, e.member = i → e ∈ st.edges := by
  simp only [PState.add, List.mem_append, List.mem_map, not_or, not_exists, not_and] at hi ⊢
  obtain ⟨⟨hterms, hedges⟩, hors⟩ := hi
  have hT : ∀ (l : List (Nat × STerm)) (st0 : PState), (∀ x ∈ l, x.1 ≠ i) →
      (l.foldl (fun st (x : Nat × STerm) => st.addTerm x.1 x.2) st0).items i = st0.items i ∧
        (l.foldl (fun st (x : Nat × STerm) => st.addTerm x.1 x.2) st0).edges = st0.edges := by
    intro l
    induction l with
    | nil => intro _ _; exact ⟨rfl, rfl⟩
    | cons x l ih =>
      intro st0 hl
      simp only [List.foldl_cons]
      obtain ⟨h1, h2⟩ := ih _ (fun y hy => hl y (List.mem_cons_of_mem _ hy))
      rw [h1, h2]
      have hx : i ≠ x.1 := fun e => hl x (by simp) e.symm
      simp [PState.addTerm, PState.update, hx]
  have hD : ∀ (l : List Disjunction) (st0 : PState), (∀ d ∈ l, d.item ≠ i) →
      (l.foldl (fun st (d : Disjunction) =>
        st.update d.item fun p => { p with ors := p.ors ++ [d.sides] }) st0).items i = st0.items i ∧
        (l.foldl (fun st (d : Disjunction) =>
          st.update d.item fun p => { p with ors := p.ors ++ [d.sides] }) st0).edges = st0.edges := by
    intro l
    induction l with
    | nil => intro _ _; exact ⟨rfl, rfl⟩
    | cons d l ih =>
      intro st0 hl
      simp only [List.foldl_cons]
      obtain ⟨h1, h2⟩ := ih _ (fun y hy => hl y (List.mem_cons_of_mem _ hy))
      rw [h1, h2]
      have hx : i ≠ d.item := fun e => hl d (by simp) e.symm
      simp [PState.update, hx]
  obtain ⟨t1, t2⟩ := hT n.terms st (fun x hx he => hterms x hx he)
  obtain ⟨d1, d2⟩ := hD n.ors _ (fun d hd he => hors d hd he)
  refine ⟨by rw [d1, t1], fun e he hm => ?_⟩
  rw [d2, t2] at he
  rcases List.mem_append.mp (mem_addOnce.mp he) with he | he
  · exact he
  · exact absurd hm (hedges e he)

/-- A fold of steps that each leave the held items as they were, and each
release their item, leaves every item it releases released. -/
theorem fold_released {α : Type} {g : PState → α → PState} {item : α → Nat}
    (hg : ∀ (st : PState) (x : α), st.Untouched (g st x))
    (hrel : ∀ (st : PState) (x : α), ((g st x).items (item x)).leftOn = none) :
    ∀ (l : List α) (st : PState), ∀ x ∈ l, ((l.foldl g st).items (item x)).leftOn = none
  | [], _, _, hx => by cases hx
  | y :: l, st, x, hx => by
    simp only [List.foldl_cons]
    rcases List.mem_cons.mp hx with rfl | hx
    · cases h : ((l.foldl g (g st x)).items (item x)).leftOn with
      | none => rfl
      | some _ =>
        have := (PState.Untouched.foldl hg l (g st x) (item x) (by rw [h]; rfl)).1
        rw [this, hrel] at h
        cases h
    · exact fold_released hg hrel l (g st y) x hx

/-- Narrowing leaves each item still held as it was: it makes every item it
adds to an inner join's. -/
theorem PState.untouched_narrow : ∀ (fuel : Nat) (st : PState) (n : Narrowing),
    st.Untouched (PState.narrow fuel st n)
  | 0, st, _ => PState.Untouched.refl st
  | fuel + 1, st, n => by
    have hj : ∀ (st : PState) (j : Nat), st.Untouched (st.joinInner fuel j) := by
      intro st j
      unfold PState.joinInner
      split
      · exact (PState.untouched_release st j).trans (PState.untouched_narrow fuel _ _)
      · exact PState.Untouched.refl st
    show st.Untouched (List.foldl (fun st i => st.joinInner fuel i)
      (st.add (st.kept n)).1 (st.add (st.kept n)).2)
    intro i hi
    have hfold := PState.Untouched.foldl hj (st.add (st.kept n)).2 (st.add (st.kept n)).1 i hi
    have hnot : i ∉ (st.add (st.kept n)).2 := by
      intro hm
      have := fold_released (item := id) hj (fun st j => PState.joinInner_leftOn fuel st j)
        (st.add (st.kept n)).2 (st.add (st.kept n)).1 i hm
      simp only [id] at this
      rw [this] at hi
      cases hi
    obtain ⟨h1, h2⟩ := PState.add_other st (st.kept n) hnot
    exact ⟨hfold.1.trans h1, fun e he hm => h2 e (hfold.2 e he hm) hm⟩

theorem PState.untouched_joinInner (fuel : Nat) (st : PState) (j : Nat) :
    st.Untouched (st.joinInner fuel j) := by
  unfold PState.joinInner
  split
  · exact (PState.untouched_release st j).trans (PState.untouched_narrow fuel _ _)
  · exact PState.Untouched.refl st

/-- Passing a term and edges into a derived item's arms leaves each item
still held as it was: a target it adds to becomes an inner join's. -/
theorem PushDown.untouched_pushDown (s : Statement) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm) (edges : List Edge),
      st.Untouched (PushDown.pushDown s narrowFuel fuel st item term edges)
  | 0, st, _, _, _ => PState.Untouched.refl st
  | fuel + 1, st, item, term, edges => by
    simp only [PushDown.pushDown]
    split
    · exact PState.Untouched.refl st
    · split
      · exact PState.Untouched.refl st
      · have hstep : ∀ (st1 : PState) (target : Nat) (renamed : Option STerm) (passed : List Edge),
            (∀ e ∈ passed, e.member = target) →
            st1.Untouched (PushDown.pushDown s narrowFuel fuel
              (if renamed.isSome || !passed.isEmpty then
                PState.joinInner narrowFuel
                  { (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1) with
                    edges := addOnce (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1).edges passed,
                    pushed := (match renamed with
                      | some r => st1.addTerm target r
                      | none => st1).pushed ++ passed } target
              else
                { (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1) with
                  edges := addOnce (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1).edges passed,
                  pushed := (match renamed with
                    | some r => st1.addTerm target r
                    | none => st1).pushed ++ passed }) target renamed passed) := by
          intro st1 target renamed passed hpm
          refine PState.Untouched.trans ?_ (PushDown.untouched_pushDown s narrowFuel fuel _ target renamed passed)
          -- Every item but the target is as it was, with no new edge from it.
          have hY : ∀ i, i ≠ target →
              ({ (match renamed with
                  | some r => st1.addTerm target r
                  | none => st1) with
                edges := addOnce (match renamed with
                  | some r => st1.addTerm target r
                  | none => st1).edges passed,
                pushed := (match renamed with
                  | some r => st1.addTerm target r
                  | none => st1).pushed ++ passed } : PState).items i = st1.items i ∧
              ∀ e ∈ ({ (match renamed with
                  | some r => st1.addTerm target r
                  | none => st1) with
                edges := addOnce (match renamed with
                  | some r => st1.addTerm target r
                  | none => st1).edges passed,
                pushed := (match renamed with
                  | some r => st1.addTerm target r
                  | none => st1).pushed ++ passed } : PState).edges, e.member = i → e ∈ st1.edges := by
            intro i hit
            cases renamed with
            | none =>
              refine ⟨rfl, fun e he hm => ?_⟩
              rcases List.mem_append.mp (mem_addOnce.mp he) with he | he
              · exact he
              · exact absurd (hm.symm.trans (hpm e he)) hit
            | some r =>
              refine ⟨by simp [PState.addTerm, PState.update, hit], fun e he hm => ?_⟩
              rcases List.mem_append.mp (mem_addOnce.mp he) with he | he
              · simpa [PState.addTerm, PState.update] using he
              · exact absurd (hm.symm.trans (hpm e he)) hit
          split
          · intro i hi
            have hit : i ≠ target := by
              intro e
              subst e
              rw [PState.joinInner_leftOn] at hi
              cases hi
            obtain ⟨h1, h2⟩ := PState.untouched_joinInner narrowFuel _ target i hi
            obtain ⟨h3, h4⟩ := hY i hit
            exact ⟨h1.trans h3, fun e he hm => h4 e (h2 e he hm) hm⟩
          · rename_i hcond
            have hr : renamed = none := by
              cases renamed with
              | none => rfl
              | some _ => simp at hcond
            have hp : passed = [] := by
              cases passed with
              | nil => rfl
              | cons _ _ => simp at hcond
            subst hr
            subst hp
            intro i _
            exact ⟨rfl, fun e he _ => by simpa [addOnce_nil] using he⟩
        have hpm : ∀ {left : Bool} {target : Nat} {rename : Nat → Option Nat} {e : Edge},
            e ∈ PushDown.passedInto s left target rename edges → e.member = target := by
          intro left target rename e he
          obtain ⟨e0, _, value, _, rfl⟩ := PushDown.mem_passed he
          rfl
        exact PState.Untouched.foldl (fun st0 arm => PState.Untouched.foldl
          (fun st1 target => hstep st1 target _ _ fun e he => hpm he) _ st0) _ st

/-! Steps that add to a held item. -/

theorem PState.Held.update_same {s : Statement} {st Fin : PState} (h : st.Held s Fin) {i : Nat}
    {f : PItem → PItem} (hterm : (f (st.items i)).term = (st.items i).term)
    (hcar : (f (st.items i)).carried = (st.items i).carried) (hors : (f (st.items i)).ors = (st.items i).ors)
    (hl : (f (st.items i)).leftOn.isSome = true → (st.items i).leftOn.isSome = true) :
    (st.update i f).Held s Fin := by
  have hitem : ∀ j, ((st.update i f).items j).leftOn.isSome = true →
      (st.items j).leftOn.isSome = true ∧ ((st.update i f).items j).term = (st.items j).term ∧
      ((st.update i f).items j).carried = (st.items j).carried ∧
      ((st.update i f).items j).ors = (st.items j).ors := by
    intro j hj
    by_cases hji : j = i
    · subst hji
      simp only [PState.update, ↓reduceIte] at hj ⊢
      exact ⟨hl hj, hterm, hcar, hors⟩
    · have hj' : (st.items j).leftOn.isSome = true := by simpa [PState.update, hji] using hj
      exact ⟨hj', by simp [PState.update, hji], by simp [PState.update, hji], by simp [PState.update, hji]⟩
  refine ⟨fun j t hj ht => ?_, fun j t hj ht => ?_, fun j hj e he hm => ?_, fun j hj sides hs => ?_⟩
  all_goals obtain ⟨hj', h1, h2, h3⟩ := hitem j hj
  · rw [h1] at ht; exact h.term j t hj' ht
  · rw [h2] at ht; exact h.carried j t hj' ht
  · exact h.edges j hj' e he hm
  · rw [h3] at hs; exact h.ors j hj' sides hs

/-- An update of one item whose leftOn is the same keeps the facts of every
other item. -/
theorem PState.Held.update_fields {s : Statement} {st Fin : PState} (h : st.Held s Fin) {i : Nat}
    {f : PItem → PItem} (hl : (f (st.items i)).leftOn = (st.items i).leftOn)
    (hterm : (st.items i).leftOn.isSome = true → ∀ t, (f (st.items i)).term = some t →
      ∀ Z f' b r, OnAt s Z f' i b r → t.holds (some r) = true)
    (hcar : (st.items i).leftOn.isSome = true → ∀ t, (f (st.items i)).carried = some t →
      ∀ Z f' b r, OnAt s Z f' i b r → HeldAround s Fin i b → t.holds (some r) = true)
    (hors : (st.items i).leftOn.isSome = true → ∀ sides ∈ (f (st.items i)).ors,
      (∀ side ∈ sides, ∀ e ∈ side.edges, e.member = i ∧ e.OnPartner s i) ∧
      ∀ Z f' b r, OnAt s Z f' i b r → Disjunction.HoldsIn s Z f' (b.set i (some r)) ⟨i, sides⟩) :
    (st.update i f).Held s Fin := by
  have hsame : ∀ j, j ≠ i → (st.update i f).items j = st.items j := fun j hji => by
    simp [PState.update, hji]
  have hat : (st.update i f).items i = f (st.items i) := by simp [PState.update]
  refine ⟨fun j t hj ht => ?_, fun j t hj ht => ?_, fun j hj e he hm => ?_, fun j hj sides hs => ?_⟩
  · by_cases hji : j = i
    · subst hji; rw [hat] at hj ht; rw [hl] at hj; exact hterm hj t ht
    · rw [hsame j hji] at hj ht; exact h.term j t hj ht
  · by_cases hji : j = i
    · subst hji; rw [hat] at hj ht; rw [hl] at hj; exact hcar hj t ht
    · rw [hsame j hji] at hj ht; exact h.carried j t hj ht
  · by_cases hji : j = i
    · subst hji; rw [hat, hl] at hj; exact h.edges j hj e he hm
    · rw [hsame j hji] at hj; exact h.edges j hj e he hm
  · by_cases hji : j = i
    · subst hji; rw [hat] at hj hs; rw [hl] at hj; exact hors hj sides hs
    · rw [hsame j hji] at hj hs; exact h.ors j hj sides hs

theorem PState.Held.addTerm {s : Statement} {st Fin : PState} (h : st.Held s Fin) {i : Nat} {t : STerm}
    (ht : (st.items i).leftOn.isSome = true → ∀ Z f b r, OnAt s Z f i b r → t.holds (some r) = true) :
    (st.addTerm i t).Held s Fin := by
  unfold PState.addTerm
  refine h.update_fields rfl (fun hl u hu Z f b r hon => ?_) (fun hl u hu => h.carried i u hl hu)
    (fun hl sides hs => h.ors i hl sides hs)
  simp only [Option.some.injEq] at hu
  subst hu
  split
  · rename_i e he
    simp [STerm.holds, h.term i e hl he Z f b r hon, ht hl Z f b r hon]
  · exact ht hl Z f b r hon

theorem PState.Held.addCarried {s : Statement} {st Fin : PState} (h : st.Held s Fin) {i : Nat} {t : STerm}
    (ht : (st.items i).leftOn.isSome = true → ∀ Z f b r, OnAt s Z f i b r → HeldAround s Fin i b →
      t.holds (some r) = true) :
    (Transfer.addCarried st i t).Held s Fin := by
  unfold Transfer.addCarried
  refine h.update_fields rfl (fun hl u hu => h.term i u hl hu) (fun hl u hu Z f b r hon har => ?_)
    (fun hl sides hs => h.ors i hl sides hs)
  simp only [Option.some.injEq] at hu
  subst hu
  split
  · rename_i e he
    simp [STerm.holds, h.carried i e hl he Z f b r hon har, ht hl Z f b r hon har]
  · exact ht hl Z f b r hon har

/-- Adding a narrowing keeps the facts when what it adds to a held item holds
wherever the item's ON does. -/
theorem PState.Held.add {s : Statement} {st Fin : PState} (h : st.Held s Fin) {n : Narrowing}
    (ht : ∀ p ∈ n.terms, (st.items p.1).leftOn.isSome = true →
      ∀ Z f b r, OnAt s Z f p.1 b r → p.2.holds (some r) = true)
    (he : ∀ e ∈ n.edges, (st.items e.member).leftOn.isSome = true → e.OnPartner s e.member ∧
      ∀ Z f b r, OnAt s Z f e.member b r → e.HoldsIn s Z f (b.set e.member (some r)))
    (hd : ∀ d ∈ n.ors, (st.items d.item).leftOn.isSome = true →
      (∀ side ∈ d.sides, ∀ e ∈ side.edges, e.member = d.item ∧ e.OnPartner s d.item) ∧
      ∀ Z f b r, OnAt s Z f d.item b r → d.HoldsIn s Z f (b.set d.item (some r))) :
    (st.add n).1.Held s Fin := by
  simp only [PState.add]
  let P : PState → Prop := fun x => x.Held s Fin ∧ (∀ i, (x.items i).leftOn = (st.items i).leftOn) ∧
    x.edges = st.edges
  have hterms := foldl_invariant (P := P) (g := fun st (x : Nat × STerm) => st.addTerm x.1 x.2)
    n.terms (fun x p hp hx => ⟨hx.1.addTerm (fun hl => by rw [hx.2.1] at hl; exact ht p hp hl),
      fun i => by
        by_cases hi : i = p.1
        · subst hi; simp [PState.addTerm, PState.update, hx.2.1]
        · simp [PState.addTerm, PState.update, hi, hx.2.1],
      hx.2.2⟩) st ⟨h, fun _ => rfl, rfl⟩
  have hors := foldl_invariant (P := P) (g := fun st (d : Disjunction) =>
    st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
    n.ors (fun x d hdm hx => by
      refine ⟨hx.1.update_fields rfl (fun hl u hu => hx.1.term d.item u hl hu)
        (fun hl u hu => hx.1.carried d.item u hl hu) (fun hl sides hs => ?_), fun i => ?_, hx.2.2⟩
      · rcases List.mem_append.mp hs with hs | hs
        · exact hx.1.ors d.item hl sides hs
        · simp only [List.mem_singleton] at hs
          subst hs
          have hl' : (st.items d.item).leftOn.isSome = true := by rw [← hx.2.1]; exact hl
          exact hd d hdm hl'
      · by_cases hi : i = d.item
        · subst hi; simp [PState.update, hx.2.1]
        · simp [PState.update, hi, hx.2.1]) _ hterms
  obtain ⟨hH, hleft, hedges⟩ := hors
  refine ⟨hH.term, hH.carried, fun i hi e he' hm => ?_, hH.ors⟩
  rcases List.mem_append.mp (mem_addOnce.mp he') with he' | he'
  · exact hH.edges i hi e he' hm
  · have hl : (st.items e.member).leftOn.isSome = true := by rw [← hleft, hm]; exact hi
    have := he e he' hl
    rw [hm] at this
    exact this

/-- An ON condition of `i` holds wherever `i`'s ON holds. -/
theorem OnAt.cond {s : Statement} {Z : Db} {f i : Nat} {b : Bind} {r : Row} (h : OnAt s Z f i b r)
    {cd : Cond} (hcd : cd ∈ (s.core (s.item i).core).onOf i) :
    isTrue (cd.eval (b.set i (some r)) (evalQuery s Z f (b.set i (some r)))) = true :=
  List.all_eq_true.mp h cd hcd

/-- Narrowing by a condition of the ON of the LEFT JOIN adding `i` keeps the
facts: what it says of `i` holds wherever `i`'s ON holds, and the rest waits
or makes every item it adds to an inner join's. -/
theorem PState.Held.narrowOn {s : Statement} (hwf : s.wellFormed = true) {st Fin : PState}
    (h : st.Held s Fin) {k : Nat} {cd : Cond} {i : Nat} (hcd : (cd, Place.leftOn i) ∈ (s.core k).conds)
    (fuel : Nat) : (st.narrowOn fuel i (readCondition s k cd)).Held s Fin := by
  have hklt := s.cond_core_lt hcd
  have hik : i ∈ (s.core k).items := by
    simp only [Statement.wellFormed, Bool.and_eq_true, List.all_eq_true, List.mem_range] at hwf
    obtain ⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨⟨_, _⟩, _⟩, _⟩, hons⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩, _⟩ := hwf
    have := List.all_eq_true.mp (hons k hklt) _ hcd
    simp only [Bool.and_eq_true, List.contains_iff_mem] at this
    exact this.1.2
  have hcore : (s.item i).core = k := ((s.wellFormed_parts hwf).items k hklt i hik).2
  have hon : cd ∈ (s.core (s.item i).core).onOf i := by
    rw [hcore]
    exact List.mem_filterMap.mpr ⟨(cd, .leftOn i), hcd, by simp⟩
  obtain ⟨hterms, hedges, hors⟩ := readCondition_sound s k cd
  have hpartner : ∀ e : Edge, e.member = i → e.via = none → (e.nested = false → e.partner ≠ e.member) →
      (e.nested = false → e.partner ∈ cd.items) → (s.item e.partner).readable = true →
      e.NestedIn s cd.queries → e.OnPartner s i := by
    intro e hm hvn hne hin hread hnest
    refine ⟨hread, hvn, fun hn => ⟨hm ▸ hne hn, cd, hon, hin hn⟩, fun hn => ?_⟩
    obtain ⟨q, hq, harm⟩ := hnest.1 hn hvn
    exact ⟨cd, hon, q, hq, harm⟩
  unfold PState.narrowOn
  simp only [Narrowing.split]
  have hown := h.add (n := ⟨(readCondition s k cd).terms.filter (·.1 == i),
      (readCondition s k cd).edges.filter (Edge.ownOn i),
      (readCondition s k cd).ors.filter (Disjunction.ownOn i)⟩)
    (fun p hp _ Z f b r honAt => by
      obtain ⟨hp', hpi⟩ := List.mem_filter.mp hp
      simp only [beq_iff_eq] at hpi
      rw [hpi] at honAt
      have := (hterms p.1 p.2 hp').2 Z f (b.set i (some r)) (honAt.cond hon)
      rwa [hpi, show (b.set i (some r)) i = some r by simp [Bind.set]] at this)
    (fun e he _ => by
      obtain ⟨he', hei⟩ := List.mem_filter.mp he
      simp only [Edge.ownOn, Bool.and_eq_true, beq_iff_eq, Option.isNone_iff_eq_none] at hei
      obtain ⟨_, hne, hin, hread, _, hnest, hh⟩ := hedges e he'
      rw [hei.1]
      exact ⟨hpartner e hei.1 hei.2 hne hin hread hnest, fun Z f b r honAt => hh Z f _ (honAt.cond hon)⟩)
    (fun d hd _ => by
      obtain ⟨hd', hdi⟩ := List.mem_filter.mp hd
      simp only [Disjunction.ownOn, Bool.and_eq_true, beq_iff_eq, List.all_eq_true,
        Option.isNone_iff_eq_none] at hdi
      obtain ⟨_, _, hsides, hh⟩ := hors d hd'
      rw [hdi.1]
      refine ⟨fun side hside e he => ?_, fun Z f b r honAt => hh Z f _ (honAt.cond hon)⟩
      obtain ⟨hm, hne, hin, hread, _, hnest⟩ := hsides side hside e he
      rw [hdi.1] at hm
      exact ⟨hm, hpartner e hm (hdi.2 side hside e he) hne hin hread hnest⟩)
  split
  · rename_i held hheld
    exact hown.update_same rfl rfl rfl (fun _ => by rw [hheld]; rfl)
  · exact hown.untouched (PState.untouched_narrow fuel _ _)

/-! Carrying terms to a held item. -/

/-- An item that the ON of `i` names is an item of a core that `i`'s core
sits in, or `i`'s core itself. -/
theorem OnNames.listed {s : Statement} (hwf : s.wellFormed = true) {i j : Nat} (h : OnNames s i j) :
    j ∈ (s.core (s.item j).core).items := by
  obtain ⟨cd, hcd, hjc⟩ := h
  have hmem := Core.mem_onOf hcd
  have hklt := s.cond_core_lt hmem
  obtain ⟨⟨j', c⟩, hp, rfl⟩ := List.mem_map.mp hjc
  have hr : (j', c) ∈ (s.core (s.item i).core).refs := by
    unfold Core.refs
    exact List.mem_append_left _ (List.mem_append_left _ (List.mem_flatMap.mpr ⟨_, hmem, hp⟩))
  obtain ⟨k', _, hjk'⟩ := (s.wellFormed_parts hwf).refs _ hklt _ hr
  have := ((s.wellFormed_parts hwf).items k' (s.core_lt hjk') _ hjk').2
  simp only
  rw [this]
  exact hjk'

/-- Carrying terms across core `k`'s edges keeps the facts: a term carried to
a held item comes from an edge its ON gives, which holds wherever the ON
does, so the partner has a row there, and the partner's term, or what its
arms say of the compared column, holds of a partner row that holds what
`Fin` asks of it. -/
theorem PState.Held.carry {s : Statement} (hwf : s.wellFormed = true) {st Fin : PState}
    (h : st.Held s Fin) (k : Nat) (hfin : st.TermGrows Fin)
    (hpull : ∀ p y t, (s.item p).core = k → p ∈ (s.core k).items → (s.item p).readable = true →
      Transfer.pulled s st p y = some t → ∀ Z rp, rp ∈ lookupRows s Z p → rp.cell y ≠ none →
        t.holds (some rp) = true) :
    (Transfer.carry s st k).Held s Fin := by
  have hfact : ∀ x ∈ Transfer.carried s st k, (st.items x.1).leftOn.isSome = true →
      ∀ Z f b r, OnAt s Z f x.1 b r → HeldAround s Fin x.1 b → x.2.holds (some r) = true := by
    intro x hx hl Z f b r hon har
    obtain ⟨e, he, hcm, hcp, hn, y, mc, hv, hm, hi, u, hr, hu⟩ := Transfer.mem_carried hx
    rw [hi] at hl hon har
    obtain ⟨⟨hread, _, hpart, _⟩, hholds⟩ := h.edges e.member hl e he rfl
    obtain ⟨hne, hnames⟩ := hpart hn
    have hh := hholds Z f b r hon
    have hset : (b.set e.member (some r)) e.partner = b e.partner := by simp [Bind.set, hne]
    obtain ⟨rp, hrp, hcell⟩ : ∃ rp, b e.partner = some rp ∧ (e.nulls = false → rp.cell y ≠ none) := by
      rcases hh with ⟨x0, _, hx0⟩ | ⟨hnul, _, hab, _, _⟩ | ⟨hst, _, _, _⟩ | ⟨_, _, _, _, hnt, _⟩
      · simp only [hn, Bool.false_eq_true, ↓reduceIte] at hx0
        obtain ⟨rp, hrp, hval⟩ := hx0
        rw [hset] at hrp
        refine ⟨rp, hrp, fun _ => ?_⟩
        rw [hv] at hval
        simp only [PValue.eval] at hval
        rw [hval]
        simp
      · obtain ⟨rp, hrp⟩ := Option.ne_none_iff_exists'.mp (har.2 e.partner hne hnames hab)
        exact ⟨rp, hrp, fun hnf => by rw [hnf] at hnul; cases hnul⟩
      · simp [hv, PValue.strict] at hst
      · rw [hn] at hnt
        cases hnt
    have hgood := har.1 e.partner rp hne hnames hrp
    have hut : u.holds ((b.set e.member (some r)) e.partner) = true := by
      rw [hset, hrp]
      rcases hu with hu | ⟨hnf, hu⟩
      · obtain ⟨u', hu', himp⟩ := hfin e.partner u hu
        exact himp _ (hgood.1 u' hu')
      · have hlisted := OnNames.listed hwf hnames
        rw [hcp] at hlisted
        obtain ⟨Z', hZ'⟩ := hgood.2 hread
        exact hpull e.partner y u hcp hlisted hread hu Z' rp hZ' (hcell hnf)
    have := Transfer.transfer_sound hv hm hn hh hut hr
    simpa [Bind.set] using this
  unfold Transfer.carry
  refine (foldl_invariant (P := fun x : PState => x.Held s Fin ∧ ∀ i, (x.items i).leftOn = (st.items i).leftOn)
    _ (fun x c hc hx => ⟨hx.1.addCarried (fun hl => hfact c hc (by rw [← hx.2]; exact hl)),
      fun i => ?_⟩) st ⟨h, fun _ => rfl⟩).1
  by_cases hi : i = c.1
  · subst hi
    simp [Transfer.addCarried, PState.update, hx.2]
  · simp [Transfer.addCarried, PState.update, hi, hx.2]

/-- What a derived item's arms say of its column holds of each row of its
query a lookup reads, when the state says only what holds of every binding
its arms keep. -/
theorem PState.Sem.pull_lookup {s : Statement} (hwf : s.wellFormed = true) {rel : Nat → Row → Bool}
    {done : List Nat} {st : PState} (hs : st.Sem s (readCls s rel done)) {k : Nat}
    (hk : k ∉ done ∧ ∀ a ∈ done, ¬s.Below k a) :
    ∀ p y t, (s.item p).core = k → p ∈ (s.core k).items → (s.item p).readable = true →
      Transfer.pulled s st p y = some t → ∀ Z rp, rp ∈ lookupRows s Z p → rp.cell y ≠ none →
        t.holds (some rp) = true := by
  intro p y t _ hp hread hpl Z rp hrp hy
  cases hkind : (s.item p).kind with
  | table _ => simp [Transfer.pulled, hkind] at hpl
  | derived q v =>
    have hrp' : rp ∈ evalQuery s Z (s.queries.length + 1) Bind.empty q := by
      simpa [lookupRows, hkind] using hrp
    refine Transfer.pulled_sound hkind hpl hrp' (fun k' hk' b' hb' j u r0 hj hu hr0 => ?_) hy
    refine hs.terms Z s.queries.length b' j r0 ?_ hr0 u hu
    rw [hj]
    refine ⟨⟨q, Bind.empty, hk', outer_readable hwf (s.wellFormed_items hwf hp) hkind hread, hb'⟩,
      fun ⟨a, ha, hba⟩ => ?_⟩
    have harm : s.ArmOf k' k :=
      ⟨q, List.mem_append_left _ (List.mem_filterMap.mpr ⟨p, hp, by simp [hkind]⟩), hk'⟩
    rcases hba.chain hwf harm with rfl | hka
    · exact absurd ha hk.1
    · exact absurd hka (hk.2 a ha)

/-! Every core, and every item placed. -/

theorem PState.Held.readConds {s : Statement} (hwf : s.wellFormed = true) (k : Nat)
    (l : List (Narrowing × Place))
    (hl : ∀ x ∈ l, ∃ cd, (cd, x.2) ∈ (s.core k).conds ∧ x.1 = readCondition s k cd)
    {st Fin : PState} (h : st.Held s Fin) :
    (l.foldl (fun st (n, p) =>
      match p with
      | .inner => st.narrow s.fuel n
      | .leftOn i => st.narrowOn s.fuel i n) st).Held s Fin := by
  refine foldl_invariant (P := fun x : PState => x.Held s Fin) l (fun st x hx hst => ?_) st h
  obtain ⟨cd, hcd, hn⟩ := hl x hx
  obtain ⟨n, p⟩ := x
  simp only at hcd hn
  subst hn
  cases p with
  | inner => exact hst.untouched (PState.untouched_narrow _ _ _)
  | leftOn i => exact hst.narrowOn hwf hcd s.fuel

/-- Reading a core keeps the facts. -/
theorem PState.Held.readCore {s : Statement} (hwf : s.wellFormed = true) {rel : Nat → Row → Bool}
    {done : List Nat} {st Fin : PState} (h : st.Held s Fin) (hs : st.Sem s (readCls s rel done))
    {k : Nat} (hk : k ∉ done ∧ ∀ a ∈ done, ¬s.Below k a)
    (hfin : (SqliveNarrowing.readCore s st k).TermGrows Fin) :
    (SqliveNarrowing.readCore s st k).Held s Fin := by
  have hP : ∀ Z f k b, readCls s rel done Z f k b → s.KeptIn Z f k b := fun _ _ _ _ hb => hb.1
  unfold SqliveNarrowing.readCore at hfin ⊢
  simp only at hfin ⊢
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
  have hX := PState.Held.readConds hwf k _ hl h
  have hSX := PState.Sem.readConds hwf hP k _ hl hs
  have hfinX := (Transfer.termGrows_carry s _ k).trans ((PState.TermGrows.foldl
    (fun st0 i => PushDown.termGrows_pushDown s _ _ st0 i _ _) _ _).trans hfin)
  exact (hX.carry hwf k hfinX (hSX.pull_lookup hwf hk)).untouched
    (PState.Untouched.foldl (fun st0 i => PushDown.untouched_pushDown s _ _ st0 i _ _) _ _)

/-- Passing ORs into arms touches no item the state still holds an ON
narrowing back for. -/
theorem PushDown.untouched_pushOrs (s : Statement) (st : PState) (item : Nat) (ors : List (List Side)) :
    st.Untouched (PushDown.pushOrs s st item ors) := by
  refine PushDown.pushOrs_induct (P := st.Untouched) (PState.Untouched.refl st)
    (fun _ _ arm _ sides _ target _ x hx => hx.trans ?_)
  rcases PushDown.passOr_cases s target (PushDown.renameInto arm target) sides x with h | ⟨_, hleft, h⟩
  · rw [h]; exact PState.Untouched.refl x
  · rw [h]
    intro i hi
    simp only [PState.update] at hi ⊢
    refine ⟨?_, fun e he _ => he⟩
    split
    · rename_i hit
      subst hit
      simp [hleft] at hi
    · rfl

theorem PState.Held.ordered {s : Statement} {Fin : PState} :
    ∀ (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat), st.Held s Fin →
      (ordered.go s fuel all st order waited).1.Held s Fin
  | 0, _, _, _, _, h => h
  | fuel + 1, all, st, order, waited, h => by
    rcases ordered_go_place s fuel all st order waited with ⟨_, heq⟩ | ⟨next, waited', _, heq⟩
    · rw [heq]; exact h
    · rw [heq]
      refine PState.Held.ordered fuel all _ _ _ ?_
      unfold ordered.placed
      simp only
      exact (h.update_same rfl rfl rfl (fun hl => hl)).untouched
        ((PushDown.untouched_pushDown s _ _ _ _ _ _).trans (PushDown.untouched_pushOrs s _ _ _))

theorem plan_held_go {s : Statement} (hwf : s.wellFormed = true) {Fin : PState} :
    ∀ (rest done : List Nat) (st : PState), done ++ rest = s.readOrder →
      st.Sem s (readCls s (fun _ _ => false) done) → st.pushed = [] → st.Held s Fin →
      (rest.foldl (readCore s) st).TermGrows Fin → (rest.foldl (readCore s) st).Held s Fin
  | [], _, _, _, _, _, h, _ => h
  | k :: rest, done, st, heq, hs, hp, h, hfin => by
    have hpw := readOrder_postorder hwf
    rw [← heq] at hpw
    have hafter := (List.pairwise_append.mp hpw).2.2
    have hk : k ∉ done ∧ ∀ a ∈ done, ¬s.Below k a :=
      ⟨fun hkd => (hafter k hkd k List.mem_cons_self).1 rfl,
        fun a ha => (hafter a ha k List.mem_cons_self).2⟩
    simp only [List.foldl_cons] at hfin ⊢
    have hfin' := (PState.TermGrows.foldl (fun st k => termGrows_readCore s st k) rest _).trans hfin
    exact plan_held_go hwf rest (done ++ [k]) _ (by simpa using heq)
      (PState.Sem.readCore hwf (fun _ _ _ _ _ _ _ _ hr => absurd hr (by simp)) hs hp hk hfin')
      (by rw [readCore_pushed]; exact hp) (h.readCore hwf hs hk hfin') hfin

/-- What the planned state says of each item it still holds an ON narrowing
back for holds wherever the item's ON holds of a row of it. -/
theorem plan_held {s : Statement} (hwf : s.wellFormed = true) : (plan s).state.Held s (plan s).state := by
  have hread := plan_held_go hwf (Fin := (plan s).state) s.readOrder [] (PState.initial s) rfl
    (PState.Sem.initial hwf fun _ _ _ _ hb => hb.1) rfl (PState.Held.initial s _)
    (ordered_termGrows s _ _ _ _ _)
  unfold plan ordered
  simp only
  exact PState.Held.ordered _ _ _ _ _ hread

/-! The promise for a LEFT JOIN item the planned state still holds back. -/

/-- A row that the ON of a LEFT JOIN holds for, of an item the planned state
still holds an ON narrowing back for, passes the item's test, in a binding of
the core, run on `X` or `Y` at fuel `f`, whose rows pass their tests and that
holds a row of each item before it that no LEFT JOIN adds: the item's facts
hold there (`plan_held`), each partner its ON gives has its row in the
binding or in a binding of the ON's subquery under it, which passes its test,
and no edge passes into the item. -/
theorem plan_left_passes {s : Statement} {X Y : Db} {last : Option (List (List Value))} {f : Nat}
    (base : KeptBase s X Y last f) {Z : Db} (hZ : Z = X ∨ Z = Y) {q k : Nat} {o : Bind}
    (hq : q ≤ f) (ho : s.Outer q o) (hk : k ∈ (s.query q).arms) {i : Nat}
    (hi : i ∈ (s.core k).items) (hl : planLeft s i) {b : Bind} (hb : Passes (planTest s Y last) b)
    (hw : s.Within k o b) (hbd : s.BoundIn ((s.core k).items.takeWhile (· ≠ i)) b)
    {r : Row} (hr : r ∈ s.itemRows Z (evalQuery s Z f) (s.outerOf k o) i)
    (hon : onHolds (s.core k) (evalQuery s Z f) i (b.set i (some r)) = true) :
    planTest s Y last i r = true := by
  have hwf := base.wf
  have hH := plan_held hwf
  have hklt := s.core_lt hi
  have hcorei : (s.item i).core = k := ((s.wellFormed_parts hwf).items k hklt i hi).2
  have honAt : OnAt s Z f i b r := by unfold OnAt; rw [hcorei]; exact hon
  have hnd := (plan_order s).1
  have hio : i ∈ (plan s).order := (plan_order s).2 k hklt i hi
  obtain ⟨pre, post, horder⟩ := List.append_of_mem hio
  obtain ⟨bound, hbound, hbefore⟩ := (plan s).scopes_eq last hnd hio
  have hinpre : ∀ j, (∃ pre' post', (plan s).order = pre' ++ i :: post' ∧ j ∈ pre') → j ∈ pre := by
    rintro j ⟨pre', post', horder', hj⟩
    have hnd' := hnd
    rw [horder] at hnd'
    rw [split_unique hnd' (horder.symm.trans horder')]
    exact hj
  have hbefore' : ∀ j ∈ pre, bound j = (plan s).scopes last j :=
    fun j hj => hbefore j ⟨pre, post, horder, hj⟩
  have hipre : i ∉ pre := by
    intro hm
    rw [horder] at hnd
    exact (List.nodup_append.mp hnd).2.2 i hm i List.mem_cons_self rfl
  have hset : ∀ j, j ≠ i → (b.set i (some r)) j = b j := fun j hji => by simp [Bind.set, hji]
  have hbi : (b.set i (some r)) i = some r := by simp [Bind.set]
  -- What `i`'s ON names, other than `i`, has its row where no LEFT JOIN adds it.
  have hnamed : ∀ j, j ≠ i → OnNames s i j → s.alwaysBound j = true → b j ≠ none := by
    intro j hji ⟨cd, hcd, hjc⟩ hab
    rw [hcorei] at hcd
    have hmem := Core.mem_onOf hcd
    rcases s.wellFormed_onNamesEarlier hwf hklt hmem hjc with h1 | h1 | h1
    · exact absurd h1 hji
    · exact hbd j h1 hab
    · rw [hw j h1]
      obtain ⟨⟨j', c⟩, hp, rfl⟩ := List.mem_map.mp hjc
      have href : (j', c) ∈ (s.core k).refs := by
        unfold Core.refs
        exact List.mem_append_left _ (List.mem_append_left _ (List.mem_flatMap.mpr ⟨_, hmem, hp⟩))
      rcases s.refs_outer hwf hk href with h2 | h2
      · exact absurd h2 h1
      · have hpar := s.refs_parent hwf hklt href h1
        unfold Statement.outerOf
        cases hp : (s.core k).parent with
        | none => exact absurd hp hpar
        | some _ => exact ho j' h2 hab
  have haround : HeldAround s (plan s).state i b := by
    refine ⟨fun j rj hji hnames hbj => ?_, hnamed⟩
    have hg := hb j rj hbj
    obtain ⟨h1, h2⟩ := planTest_parts hg
    have hlisted := OnNames.listed hwf hnames
    have hjo := (plan_order s).2 _ (s.core_lt hlisted) j hlisted
    exact ⟨(scopes_parts last _ hjo h1).1, fun hread => ⟨Y, h2 hread⟩⟩
  -- A partner `i`'s ON gives, placed before `i`, finds its row.
  have hpartner : ∀ e : Edge, e.member = i → e.partner ∈ pre → e.OnPartner s i →
      e.HoldsIn s Z f (b.set i (some r)) →
      ∃ r', r' ∈ lookupRows s Y e.partner ∧
        ((plan s).scopes last e.partner).passes (lookupRows s Y) r' = true ∧
        e.value.eval r' = e.memberValue.eval r ∧
        ((e.memberValue.eval r).isSome = true ∨ e.nulls = true) ∧
        (e.nested = false → b e.partner = some r') := by
    intro e hm hp ⟨hread, hvn, hplain, hnest⟩ hh
    have hpi : e.partner ≠ i := fun he => hipre (he ▸ hp)
    suffices h : ∃ r', r' ∈ lookupRows s Y e.partner ∧
        ((plan s).scopes last e.partner).passes (lookupRows s Y) r' = true ∧
        e.value.eval r' = e.memberValue.eval r ∧
        ((e.memberValue.eval r).isSome = true ∨ e.nulls = true) ∧
        (e.nested = false → (b.set i (some r)) e.partner = some r') by
      obtain ⟨r', h1, h2, h3, h4, h5⟩ := h
      exact ⟨r', h1, h2, h3, h4, fun hn => by rw [← hset _ hpi]; exact h5 hn⟩
    refine partner_found hm hbi hh (fun hn hab => by rw [hset _ hpi]; exact hnamed _ hpi (hplain hn).2 hab)
      (fun r' hr' => by rw [hset _ hpi] at hr'; exact hb _ r' hr') (fun hn _ g' hg' b' hb' hgr r' hr' => ?_)
      (fun d pc _ hv => by rw [hvn] at hv; cases hv) hread
    obtain ⟨cd, hcd, q', hq', harm⟩ := hnest hn
    rw [hcorei] at hcd
    have hsub := Core.onOf_queries hcd hq'
    have hqq := s.wellFormed_nested hwf (s.query_lt hk) hk (List.mem_append_right _ hsub)
    have hw' : s.Within k o (b.set i (some r)) := fun j hj => by
      have hji : j ≠ i := fun e => hj (e ▸ hi)
      rw [hset j hji]
      exact hw j hj
    have hbd' : s.BoundIn ((s.core k).items.takeWhile (· ≠ i) ++ [i]) (b.set i (some r)) := by
      intro j hj hab
      by_cases hji : j = i
      · subst hji; rw [hbi]; simp
      · rw [hset j hji]
        rcases List.mem_append.mp hj with hj | hj
        · exact hbd j hj hab
        · exact absurd (List.mem_singleton.mp hj) hji
    have hall : ∀ Z', (Z' = X ∨ Z' = Y) →
        ∀ b'' ∈ coreBindings s Z' (evalQuery s Z' g') (b.set i (some r)) (s.item e.partner).core,
        (plan s).windowPasses s Y (s.item e.partner).core b'' = true →
        ∀ j r', j ∈ pre → b'' j = some r' → planTest s Y last j r' = true := by
      intro Z' hZ' b'' hb'' hwin j r' hj hr'
      refine plan_kept_rows base pre.length Z' hZ' g' (by omega) q' (by omega) (b.set i (some r))
        (s.outer_subOn hwf hk ho hw' hbd' hcd hq')
        (fun j rj hj hrj => by
          rw [horder, List.take_append_length] at hj
          have hji : j ≠ i := fun e => hipre (e ▸ hj)
          rw [hset j hji] at hrj
          exact hb j rj hrj) _ harm b'' hb''
        (relevant_of_all (queryTest_sub hwf (plan s) ((plan s).scopes last) (lookupRows s Y) hsub) _ _)
        (fun he => absurd (he ▸ List.mem_append_right _ hsub) (s.wellFormed_root hwf k)) hwin j r'
        (by rw [horder, List.take_append_length]; exact hj) hr'
    exact hall Z hZ b' hb'
      (nested_windowPasses base hZ hp (by rw [horder]; exact List.mem_append_left _ hp) rfl hall hb' hgr)
      _ r' hp hr'
  have hscope : ((plan s).scopes last i).passes (lookupRows s Y) r = true := by
    rw [hbound]
    refine scopeAt_passes (fun t ht => hH.term i t hl ht Z f b r honAt)
      (fun t ht => hH.carried i t hl ht Z f b r honAt haround) (fun m hmm => ?_)
      (fun q e0 he0 hq hn0 hp0 _ => ?_) (fun sides hsides => ?_)
      (fun sorted values found hs _ hf => ?_)
    · have hme := plan_membersEdges s i m hmm
      have hmi := ((plan_nestedOK s).2.1 i m hmm).1
      have hp := hinpre _ ((plan_placedBefore s i).1 m hmm)
      rw [membershipScope_congr (hbefore' _ hp)]
      obtain ⟨hpart, hholds⟩ := hH.edges i hl m hme hmi
      obtain ⟨r', hr', hsc, hv, hn, _⟩ := hpartner m hmi hp hpart (hholds Z f b r honAt)
      exact membershipScope_passes hr' hsc hv hn
    · subst hq
      have hme := plan_membersEdges s i e0 he0
      have hmi := ((plan_nestedOK s).2.1 i e0 he0).1
      have hp := hinpre _ ((plan_placedBefore s i).1 e0 he0)
      obtain ⟨hpart, hholds⟩ := hH.edges i hl e0 hme hmi
      obtain ⟨r0, hr0, hsc0, _, _, hb0⟩ := hpartner e0 hmi hp hpart (hholds Z f b r honAt)
      have hbq := hb0 hn0
      refine ⟨r0, hr0, by rw [hbefore' _ hp]; exact hsc0, fun e he hqe hne hpe _ => ?_⟩
      have hme' := plan_membersEdges s i e he
      have hmi' := ((plan_nestedOK s).2.1 i e he).1
      have hp' := hinpre _ ((plan_placedBefore s i).1 e he)
      obtain ⟨hpart', hholds'⟩ := hH.edges i hl e hme' hmi'
      obtain ⟨r1, _, _, hv1, hn1, hb1⟩ := hpartner e hmi' hp' hpart' (hholds' Z f b r honAt)
      have := hb1 hne
      rw [hqe, hbq] at this
      obtain rfl := Option.some.inj this
      exact ⟨hv1, hn1⟩
    · have hors := (plan_lookups hwf).2.2.1 i sides hsides
      obtain ⟨hsd, hholds⟩ := hH.ors i hl sides hors
      obtain ⟨side, hside, hterms, hedges⟩ := hholds Z f b r honAt
      refine ⟨side, hside, sideScope_passes (fun t ht => by have := hterms t ht; rwa [hbi] at this)
        fun e he => ?_⟩
      obtain ⟨hmem, hpart⟩ := hsd side hside e he
      have hp := hinpre _ ((plan_placedBefore s i).2 sides hsides side hside e he)
      rw [hbefore' _ hp]
      obtain ⟨r', hr', hsc, hv, hn, _⟩ := hpartner e hmem hp hpart (hedges e he)
      exact joined_passes_of hr' hsc hv hn
    · obtain ⟨⟨run, _⟩, hpr, hri⟩ := Limit.runBound_item hf
      obtain ⟨_, _, _, hleft⟩ := Limit.run_facts hwf hs (List.of_mem_zip hpr).1
      simp only at hri
      rw [hri] at hleft
      unfold planLeft at hl
      rw [hleft] at hl
      cases hl
  have hleft : (s.item i).left = true := by
    have := plan_leftOn s hl
    simpa [Statement.alwaysBound] using this
  have hst : (plan s).scopeTest ((plan s).scopes last) (lookupRows s Y) i r = true := by
    unfold Planned.scopeTest
    rw [hscope, windowScope_left hleft]
    simp only [RScope.passes, Bool.true_and]
    refine List.all_eq_true.mpr fun e he => ?_
    by_cases hm : e.member = i
    · have := (plan_pushed s he).1
      rw [hm] at this
      unfold planLeft at hl
      rw [this] at hl
      cases hl
    · simp [hm]
  have hlook : (s.item i).readable = true → r ∈ lookupRows s Y i := by
    intro hread
    have hilt : i < s.items.length := s.wellFormed_items hwf hi
    cases hkind : (s.item i).kind with
    | table t =>
      have hr' : r ∈ Z.rows t := by simpa [Statement.itemRows, hkind] using hr
      have hL : lookupRows s Y i = Y.rows t := by simp [lookupRows, hkind]
      rw [hL]
      rcases hZ with hZ | hZ <;> subst hZ
      · exact base.mem (by simp [Statement.table?, hkind]) hilt hr' hst
      · exact hr'
    | derived qi v =>
      have hr' : r ∈ evalQuery s Z f (s.outerOf k o) qi := by simpa [Statement.itemRows, hkind] using hr
      rw [evalQuery_readable hwf hilt hkind hread] at hr'
      have hqi : qi ∈ s.nestedQueries k :=
        List.mem_append_left _ (List.mem_filterMap.mpr ⟨i, hi, by simp [hkind]⟩)
      have hqlt := s.wellFormed_nested hwf (s.query_lt hk) hk hqi
      have hqlen : qi < s.queries.length := Nat.lt_trans hqlt (s.query_lt hk)
      have hL : lookupRows s Y i = evalQuery s Y f Bind.empty qi := by
        simp only [lookupRows, hkind]
        rw [evalQuery_fuel hwf Y qi hqlen f (by omega),
          evalQuery_fuel hwf Y qi hqlen (s.queries.length + 1) (by omega)]
      rw [hL]
      rcases hZ with hZ | hZ <;> subst hZ
      · have hne : qi ≠ s.root := fun e => s.wellFormed_root hwf k (e ▸ hqi)
        have hagree := base.agree f (Nat.le_refl f) qi (by omega) hne Bind.empty
          (outer_readable hwf hilt hkind hread) (Passes.empty _)
        have hmem : r ∈ (evalQuery s _ f Bind.empty qi).filter (planRel s Y last qi) :=
          List.mem_filter.mpr ⟨hr', queryTest_of_scope hwf hi hkind hst⟩
        rw [hagree] at hmem
        exact (List.mem_filter.mp hmem).1
      · exact hr'
  unfold planTest Planned.itemTest
  rw [hst, Bool.true_and]
  cases hread : (s.item i).readable
  · rfl
  · simp [hlook hread]

end SqliveNarrowing
