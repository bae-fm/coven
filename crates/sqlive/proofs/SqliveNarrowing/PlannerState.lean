import SqliveNarrowing.Planner

/-! What the planner's state keeps however it reads the conditions: it holds
an ON narrowing back only for an item a LEFT JOIN adds, and once it releases
one, as it makes the item an inner join's, it never holds one back again. So
an item the planned state still holds an ON narrowing back for is one a LEFT
JOIN adds (`plan_leftOn`), which the restriction's `hLeft` takes. The planner
places each item once, and every item of every core (`plan_order`), which
`Planned.scopes_eq` takes. -/
namespace SqliveNarrowing

/-- `st'` holds an ON narrowing back only for items `st` holds one back for. -/
def PState.Releases (st st' : PState) : Prop :=
  ∀ i, (st'.items i).leftOn.isSome → (st.items i).leftOn.isSome

theorem PState.Releases.refl (st : PState) : st.Releases st := fun _ h => h

theorem PState.Releases.trans {a b c : PState} (hab : a.Releases b) (hbc : b.Releases c) :
    a.Releases c := fun i h => hab i (hbc i h)

/-- A fold whose every step releases releases. -/
theorem PState.Releases.foldl {α : Type} {g : PState → α → PState}
    (hg : ∀ (st : PState) (x : α), st.Releases (g st x)) : ∀ (l : List α) (st : PState), st.Releases (l.foldl g st)
  | [], st => PState.Releases.refl st
  | x :: l, st => (hg st x).trans (PState.Releases.foldl hg l (g st x))

/-- An update that keeps or drops an item's held narrowing releases. -/
theorem PState.update_releases {st : PState} {i : Nat} {f : PItem → PItem}
    (hf : (f (st.items i)).leftOn.isSome → (st.items i).leftOn.isSome) :
    st.Releases (st.update i f) := by
  intro j h
  simp only [PState.update] at h
  split at h
  · rename_i hji
    subst hji
    exact hf h
  · exact h

theorem PState.addTerm_releases (st : PState) (i : Nat) (t : STerm) :
    st.Releases (st.addTerm i t) := by
  unfold PState.addTerm
  exact PState.update_releases (fun h => h)

theorem PState.add_releases (st : PState) (n : Narrowing) : st.Releases (st.add n).1 := by
  simp only [PState.add]
  intro i h
  have hors := PState.Releases.foldl (g := fun st (d : Disjunction) =>
    st.update d.item fun p => { p with ors := p.ors ++ [d.sides] })
    (fun st d => PState.update_releases (fun h => h)) n.ors
    (n.terms.foldl (fun st (i, t) => st.addTerm i t) st)
  have hterms := PState.Releases.foldl (g := fun st (x : Nat × STerm) => st.addTerm x.1 x.2)
    (fun st x => PState.addTerm_releases st x.1 x.2) n.terms st
  exact hterms i (hors i h)

theorem PState.narrow_releases : ∀ (fuel : Nat) (st : PState) (n : Narrowing),
    st.Releases (PState.narrow fuel st n)
  | 0, st, _ => PState.Releases.refl st
  | fuel + 1, st, n => by
    simp only [PState.narrow]
    refine (PState.add_releases st _).trans (PState.Releases.foldl (fun st' i => ?_) _ _)
    split
    · exact (PState.update_releases (by simp)).trans (PState.narrow_releases fuel _ _)
    · exact PState.Releases.refl _

theorem PState.joinInner_releases (fuel : Nat) (st : PState) (i : Nat) :
    st.Releases (st.joinInner fuel i) := by
  unfold PState.joinInner
  split
  · exact (PState.update_releases (by simp)).trans (PState.narrow_releases fuel _ _)
  · exact PState.Releases.refl _

theorem PState.narrowOn_releases (fuel : Nat) (st : PState) (i : Nat) (n : Narrowing) :
    st.Releases (st.narrowOn fuel i n) := by
  unfold PState.narrowOn
  generalize n.split i = x
  obtain ⟨own, others⟩ := x
  simp only
  refine (PState.add_releases st own).trans ?_
  split
  · rename_i held hheld
    exact PState.update_releases (by simp [hheld])
  · exact PState.narrow_releases fuel _ _

theorem Transfer.carry_releases (s : Statement) (st : PState) (k : Nat) :
    st.Releases (Transfer.carry s st k) :=
  PState.Releases.foldl (fun st (x : Nat × STerm) => by
    unfold Transfer.addCarried
    exact PState.update_releases (fun h => h)) _ st

theorem PushDown.pushDown_releases (s : Statement) (narrowFuel : Nat) :
    ∀ (fuel : Nat) (st : PState) (item : Nat) (term : Option STerm) (edges : List Edge),
      st.Releases (PushDown.pushDown s narrowFuel fuel st item term edges)
  | 0, st, _, _, _ => PState.Releases.refl st
  | fuel + 1, st, item, term, edges => by
    simp only [PushDown.pushDown]
    split
    · exact PState.Releases.refl st
    · split
      · exact PState.Releases.refl st
      · refine PState.Releases.foldl (fun st arm => ?_) _ st
        refine PState.Releases.foldl (fun st target => ?_) _ st
        refine PState.Releases.trans ?_ (PushDown.pushDown_releases s narrowFuel fuel _ _ _ _)
        have hterm : ∀ (r : Option STerm), st.Releases (match r with
            | some r => st.addTerm target r
            | none => st) := by
          intro r
          cases r
          · exact PState.Releases.refl st
          · exact PState.addTerm_releases _ _ _
        split
        · exact (hterm _).trans (PState.joinInner_releases _ _ _)
        · exact hterm _

theorem readCore_releases (s : Statement) (st : PState) (k : Nat) : st.Releases (readCore s st k) := by
  unfold readCore
  simp only
  refine PState.Releases.trans ?_ (PState.Releases.foldl
    (fun st i => PushDown.pushDown_releases s _ _ st i _ _) _ _)
  refine PState.Releases.trans ?_ (Transfer.carry_releases s _ k)
  refine PState.Releases.foldl (fun st x => ?_) _ st
  obtain ⟨n, p⟩ := x
  cases p
  · exact PState.narrow_releases _ _ _
  · exact PState.narrowOn_releases _ _ _ _

theorem ordered_releases (s : Statement) :
    ∀ (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat),
      st.Releases (ordered.go s fuel all st order waited).1
  | 0, _, st, _, _ => PState.Releases.refl st
  | fuel + 1, all, st, order, waited => by
    simp only [ordered.go]
    split
    · exact PState.Releases.refl st
    · refine PState.Releases.trans ?_ (ordered_releases s fuel _ _ _ _)
      refine PState.Releases.trans ?_ (PushDown.pushDown_releases s _ _ _ _ _ _)
      exact PState.update_releases (fun h => h)

/-- The planned state holds an ON narrowing back only for an item a LEFT JOIN
adds. -/
theorem plan_leftOn (s : Statement) {i : Nat} (h : ((plan s).state.items i).leftOn.isSome) :
    s.alwaysBound i = false := by
  have hread := PState.Releases.foldl (fun st k => readCore_releases s st k) s.readOrder
    (PState.initial s)
  unfold plan ordered at h
  simp only at h
  have h0 := hread i (ordered_releases s _ _ _ [] [] i h)
  simp only [PState.initial] at h0
  simp only [Statement.alwaysBound]
  revert h0
  cases (s.item i).left <;> simp


/-- The first of three searches of `l` that finds an item, or `first`, is in
any list holding `l` and `first`. -/
theorem getD_find_mem {l : List Nat} {p1 p2 p3 : Nat → Bool} {first : Nat} {S : List Nat}
    (hl : ∀ x ∈ l, x ∈ S) (hfirst : first ∈ S) :
    ((((l.find? p1).orElse fun _ => l.find? p2).orElse fun _ => l.find? p3).getD first) ∈ S := by
  cases h1 : l.find? p1 with
  | some x => simpa [h1] using hl x (List.mem_of_find?_eq_some h1)
  | none =>
    cases h2 : l.find? p2 with
    | some x => simpa [h1, h2] using hl x (List.mem_of_find?_eq_some h2)
    | none =>
      cases h3 : l.find? p3 with
      | some x => simpa [h1, h2, h3] using hl x (List.mem_of_find?_eq_some h3)
      | none => simpa [h1, h2, h3] using hfirst

/-- A step of placing items finds none left, or places one of those left. -/
theorem ordered_go_step (s : Statement) (fuel : Nat) (all : List Nat) (st : PState) (order waited : List Nat) :
    (all.filter (fun i => !order.contains i) = [] ∧ ordered.go s (fuel + 1) all st order waited = (st, order)) ∨
    ∃ next st' waited', next ∈ all.filter (fun i => !order.contains i) ∧
      ordered.go s (fuel + 1) all st order waited = ordered.go s fuel all st' (order ++ [next]) waited' := by
  simp only [ordered.go]
  generalize hR : List.filter (fun i => !order.contains i) all = R
  cases R with
  | nil => exact Or.inl ⟨rfl, rfl⟩
  | cons r tail =>
    refine Or.inr ⟨_, _, _, ?_, rfl⟩
    split
    · rename_i i _ hfind
      exact List.mem_of_find?_eq_some hfind
    · rename_i first tail' _ hfree
      have hsub : ∀ x ∈ List.filter (fun i => !(waiting st order (r :: tail)).contains i) (r :: tail),
          x ∈ r :: tail := fun x hx => (List.mem_filter.mp hx).1
      have hfirst : first ∈ r :: tail := hsub first (by rw [hfree]; simp)
      exact getD_find_mem (fun x hx => hsub x (List.mem_filter.mp hx).1) hfirst
    · exact List.mem_cons_self

/-- Placing an item leaves fewer to place. -/
theorem remaining_shrinks {all order : List Nat} {next : Nat}
    (h : next ∈ all.filter (fun i => !order.contains i)) :
    (all.filter (fun i => !(order ++ [next]).contains i)).length <
      (all.filter (fun i => !order.contains i)).length := by
  have heq : all.filter (fun i => !(order ++ [next]).contains i) =
      (all.filter (fun i => !order.contains i)).filter (fun i => i != next) := by
    rw [List.filter_filter]
    apply List.filter_congr
    intro i _
    simp only [List.contains_append, List.contains_cons, List.contains_nil, Bool.or_false,
      Bool.not_or]
    cases order.contains i <;> cases hi : (i == next) <;> simp_all [bne]
  rw [heq, List.length_filter_lt_length_iff_exists]
  exact ⟨next, h, by simp⟩

/-- Placing items from `all` after `order` adds each item once, and with fuel
for every item left, places them all. -/
theorem ordered_go_order (s : Statement) : ∀ (fuel : Nat) (all : List Nat) (st : PState)
    (order waited : List Nat), order.Nodup →
    ((ordered.go s fuel all st order waited).2).Nodup ∧
      ((all.filter (fun i => !order.contains i)).length ≤ fuel →
        ∀ i ∈ all, i ∈ (ordered.go s fuel all st order waited).2)
  | 0, all, st, order, waited, hnd => by
    refine ⟨hnd, fun hlen i hi => ?_⟩
    have hnil : all.filter (fun i => !order.contains i) = [] := List.eq_nil_of_length_eq_zero (by omega)
    have := List.filter_eq_nil_iff.mp hnil i hi
    simpa [ordered.go] using this
  | fuel + 1, all, st, order, waited, hnd => by
    rcases ordered_go_step s fuel all st order waited with ⟨hnil, heq⟩ | ⟨next, st', waited', hnext, heq⟩
    · rw [heq]
      refine ⟨hnd, fun _ i hi => ?_⟩
      have := List.filter_eq_nil_iff.mp hnil i hi
      simpa using this
    · rw [heq]
      have hnin : next ∉ order := by
        have := (List.mem_filter.mp hnext).2
        simpa using this
      have hnd' : (order ++ [next]).Nodup :=
        List.nodup_append.mpr ⟨hnd, (by simp), fun a ha b hb => by
          simp only [List.mem_singleton] at hb
          subst hb
          exact fun e => hnin (e ▸ ha)⟩
      obtain ⟨h1, h2⟩ := ordered_go_order s fuel all st' (order ++ [next]) waited' hnd'
      exact ⟨h1, fun hlen => h2 (by have := remaining_shrinks hnext; omega)⟩

/-- The planner places each item once, and every item of every core. -/
theorem plan_order (s : Statement) : (plan s).order.Nodup ∧
    ∀ k, k < s.cores.length → ∀ i ∈ (s.core k).items, i ∈ (plan s).order := by
  unfold plan ordered
  simp only
  obtain ⟨hnd, hall⟩ := ordered_go_order s _ ((List.range s.cores.length).flatMap fun k =>
    (s.core k).items) (s.readOrder.foldl (readCore s) (PState.initial s)) [] [] List.nodup_nil
  refine ⟨hnd, fun k hk i hi => hall (List.length_filter_le _ _) i ?_⟩
  exact List.mem_flatMap.mpr ⟨k, List.mem_range.mpr hk, hi⟩

end SqliveNarrowing
