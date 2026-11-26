import SqliveNarrowing.Planner

/-! What makes a row pass the scope the planner gives an item
(`Planned.scopes`): its terms and what its edges carry true of the row, each
membership's lookup finding a row of its partner in the partner's scope that
gives the row's value, a side of each OR it keeps passing, and the LIMIT
bound. `Planned.scopes_eq` states the scope of a placed item through the
scopes of the items placed before it, and `scopeAt_passes` what passes it. -/
namespace SqliveNarrowing

/-- A term's scope passes a row exactly when the term holds of it. -/
theorem STerm.bind_passes (rowsOf : Nat → List Row) (r : Row) :
    ∀ t : STerm, t.bind.passes rowsOf r = t.holds (some r)
  | .compare c op v => rfl
  | .inList c vs => by
    simp only [STerm.bind, RScope.passes, STerm.holds, cellOf]
    cases r.cell c with
    | none => rfl
    | some x =>
      simp only
      rw [Bool.eq_iff_iff, List.contains_iff_mem, List.contains_iff_mem, List.mem_eraseDups]
  | .null c n => rfl
  | .row i cd => rfl
  | .and l t => by
    simp only [STerm.bind, STerm.holds, Limit.passes_both, STerm.bind_passes rowsOf r l,
      STerm.bind_passes rowsOf r t]
  | .or l t => by
    simp only [STerm.bind, STerm.holds, Limit.passes_either, STerm.bind_passes rowsOf r l,
      STerm.bind_passes rowsOf r t]

/-- The values a scope says every row it passes holds in a column are the
values the rows it passes hold there. -/
theorem RScope.equalValues_passes {rowsOf : Nat → List Row} {r : Row} {c : Nat} :
    ∀ {sc : RScope} {vs : List Int}, sc.equalValues c = some vs → sc.passes rowsOf r = true →
      ∃ x, r.cell c = some x ∧ x ∈ vs
  | .inVals c' vs', vs, h, hp => by
    simp only [RScope.equalValues] at h
    split at h
    · rename_i hc
      subst hc
      cases h
      simp only [RScope.passes] at hp
      cases hx : r.cell c' with
      | none => rw [hx] at hp; cases hp
      | some x =>
        rw [hx] at hp
        exact ⟨x, rfl, List.contains_iff_mem.mp hp⟩
    · cases h
  | .compare c' op v, vs, h, hp => by
    cases op <;> simp only [RScope.equalValues, reduceCtorEq] at h
    split at h
    · rename_i hc
      subst hc
      cases h
      simp only [RScope.passes] at hp
      cases hx : r.cell c' with
      | none => rw [hx] at hp; cases hp
      | some x =>
        rw [hx] at hp
        simp only [Op.compare, beq_iff_eq] at hp
        exact ⟨x, rfl, by simp [hp]⟩
    · cases h
  | .and l t, vs, h, hp => by
    simp only [RScope.equalValues] at h
    simp only [RScope.passes, Bool.and_eq_true] at hp
    cases hl : l.equalValues c with
    | some a =>
      rw [hl] at h
      cases h
      exact RScope.equalValues_passes hl hp.1
    | none =>
      rw [hl] at h
      exact RScope.equalValues_passes h hp.2
  | .or l t, vs, h, hp => by
    simp only [RScope.equalValues] at h
    cases hl : l.equalValues c with
    | none => simp [hl] at h
    | some a =>
      cases ht : t.equalValues c with
      | none => simp [hl, ht] at h
      | some b =>
        simp only [hl, ht, Option.bind_eq_bind, Option.bind_some, Option.pure_def,
          Option.some.injEq] at h
        subst h
        simp only [RScope.passes, Bool.or_eq_true] at hp
        rcases hp with hp | hp
        · obtain ⟨x, hx, hm⟩ := RScope.equalValues_passes hl hp
          exact ⟨x, hx, List.mem_append_left _ hm⟩
        · obtain ⟨x, hx, hm⟩ := RScope.equalValues_passes ht hp
          exact ⟨x, hx, List.mem_append_right _ hm⟩
  | .all, _, h, _ => by simp [RScope.equalValues] at h
  | .null .., _, h, _ => by simp [RScope.equalValues] at h
  | .row .., _, h, _ => by simp [RScope.equalValues] at h
  | .joined .., _, h, _ => by simp [RScope.equalValues] at h
  | .window .., _, h, _ => by simp [RScope.equalValues] at h

/-- A lookup passes a row whose value, NULL only for an `IS` edge, a row of
the partner among `rowsOf` in the partner's scope gives. -/
theorem joined_passes_of {rowsOf : Nat → List Row} {r : Row} {mv v : PValue} {p : Nat}
    {sc : RScope} {nulls : Bool} {r' : Row} (hr' : r' ∈ rowsOf p) (hsc : sc.passes rowsOf r' = true)
    (hv : v.eval r' = mv.eval r) (hn : (mv.eval r).isSome = true ∨ nulls = true) :
    (RScope.joined mv p v sc nulls).passes rowsOf r = true := by
  simp only [RScope.passes, Bool.and_eq_true, Bool.or_eq_true, List.any_eq_true, beq_iff_eq]
  exact ⟨hn, r', hr', hv, hsc⟩

/-- One row of the partner among `rowsOf`, in the partner's scope, that gives
the row each of the edges' values, passes the edges' joined scope. -/
theorem joinedAll_passes_of {rowsOf : Nat → List Row} {r : Row} {es : List Edge} {p : Nat}
    {sc : RScope} {r' : Row} (hr' : r' ∈ rowsOf p) (hsc : sc.passes rowsOf r' = true)
    (hall : ∀ e ∈ es, e.value.eval r' = e.memberValue.eval r ∧
      ((e.memberValue.eval r).isSome = true ∨ e.nulls = true)) :
    (RScope.joinedAll es p sc).passes rowsOf r = true := by
  simp only [RScope.passes, Bool.and_eq_true, List.any_eq_true, List.all_eq_true, Bool.or_eq_true,
    beq_iff_eq]
  exact ⟨r', hr', fun e he => ⟨(hall e he).2, (hall e he).1⟩, hsc⟩

/-- A scope that joins several edges to one partner row passes a row only if
each edge's own join passes it: the one row gives each edge's value. -/
theorem joinedAll_passes_joined {rowsOf : Nat → List Row} {r : Row} {es : List Edge} {p : Nat}
    {sc : RScope} {e : Edge} (h : (RScope.joinedAll es p sc).passes rowsOf r = true) (he : e ∈ es) :
    (RScope.joined e.memberValue p e.value sc e.nulls).passes rowsOf r = true := by
  simp only [RScope.passes, Bool.and_eq_true, List.any_eq_true, List.all_eq_true, Bool.or_eq_true,
    beq_iff_eq] at h ⊢
  obtain ⟨r', hr', hes, hsc⟩ := h
  obtain ⟨hn, hv⟩ := hes e he
  exact ⟨hn, r', hr', hv, hsc⟩

/-- Each scope that joins an item's memberships to one partner passes a row
when one row of the partner gives it every value of the memberships that join
that partner. -/
theorem groupScopes_passes {bound : Nat → RScope} {rowsOf : Nat → List Row} {r : Row}
    {ms : List Edge}
    (hgroups : ∀ p, ∀ e0 ∈ ms, e0.partner = p → e0.nested = false → e0.pushed = false → e0.nulls = false →
      ∃ r' ∈ rowsOf p, (bound p).passes rowsOf r' = true ∧
        ∀ e ∈ ms, e.partner = p → e.nested = false → e.pushed = false → e.nulls = false →
          e.value.eval r' = e.memberValue.eval r ∧
            ((e.memberValue.eval r).isSome = true ∨ e.nulls = true)) :
    ∀ g ∈ Planned.groupScopes bound ms, g.passes rowsOf r = true := by
  intro g hg
  unfold Planned.groupScopes at hg
  obtain ⟨p, hp, hg⟩ := List.mem_filterMap.mp hg
  obtain ⟨e0, he0, rfl⟩ := List.mem_map.mp (List.mem_eraseDups.mp hp)
  simp only [List.mem_filter] at he0
  have hlisted := he0.2
  unfold Planned.listsPartner at hlisted
  simp only [Bool.and_eq_true, Bool.not_eq_true'] at hlisted
  obtain ⟨r', hr', hsc, hall⟩ := hgroups _ e0 he0.1 rfl hlisted.1.1.1 hlisted.1.1.2 hlisted.1.2
  generalize hes : List.filter (fun x => x.partner == e0.partner)
    (List.filter (Planned.listsPartner bound) ms) = l at hg
  rcases l with _ | ⟨a, _ | ⟨b, c⟩⟩
  · simp at hg
  · simp at hg
  · simp only [Option.some.injEq] at hg
    subst hg
    refine joinedAll_passes_of hr' hsc fun e he => ?_
    have hmem : e ∈ List.filter (fun x => x.partner == e0.partner)
        (List.filter (Planned.listsPartner bound) ms) := by
      rw [hes]; exact he
    simp only [List.mem_filter, beq_iff_eq] at hmem
    have hl := hmem.1.2
    unfold Planned.listsPartner at hl
    simp only [Bool.and_eq_true, Bool.not_eq_true'] at hl
    exact hall e hmem.1.1 hmem.2 hl.1.1.1 hl.1.1.2 hl.1.2

/-- A membership's scope passes the member's row when a row of the partner
among `rowsOf`, in the partner's scope, gives the row's value, NULL only for
an `IS` edge. -/
theorem membershipScope_passes {bound : Nat → RScope} {rowsOf : Nat → List Row} {m : Edge}
    {r r' : Row} (hr' : r' ∈ rowsOf m.partner) (hsc : (bound m.partner).passes rowsOf r' = true)
    (hv : m.value.eval r' = m.memberValue.eval r)
    (hn : (m.memberValue.eval r).isSome = true ∨ m.nulls = true) :
    (Planned.membershipScope bound m).passes rowsOf r = true := by
  unfold Planned.membershipScope
  simp only
  split
  · rename_i mc c hmv hval
    split
    · rename_i vs hvs
      obtain ⟨x, hx, hm⟩ := RScope.equalValues_passes hvs hsc
      rw [hmv, hval] at hv
      simp only [PValue.eval] at hv
      simp only [RScope.passes, ← hv, hx]
      exact List.contains_iff_mem.mpr (List.mem_eraseDups.mpr hm)
    · exact joined_passes_of hr' hsc hv hn
  · exact joined_passes_of hr' hsc hv hn

theorem foldl_both_passes {α : Type} {rowsOf : Nat → List Row} {r : Row} (g : α → RScope) :
    ∀ (l : List α) (acc : RScope), acc.passes rowsOf r = true → (∀ a ∈ l, (g a).passes rowsOf r = true) →
      (l.foldl (fun acc a => acc.both (g a)) acc).passes rowsOf r = true
  | [], _, h, _ => h
  | a :: l, acc, h, hl => by
    simp only [List.foldl_cons]
    apply foldl_both_passes g l
    · rw [Limit.passes_both, h, hl a (by simp)]; rfl
    · exact fun a' ha' => hl a' (List.mem_cons_of_mem a ha')

theorem foldl_either_passes {rowsOf : Nat → List Row} {r : Row} :
    ∀ (l : List RScope) (acc : RScope), (acc.passes rowsOf r = true ∨ ∃ x ∈ l, x.passes rowsOf r = true) →
      (l.foldl RScope.either acc).passes rowsOf r = true
  | [], _, h => by simpa using h
  | a :: l, acc, h => by
    simp only [List.foldl_cons]
    apply foldl_either_passes l
    rcases h with h | ⟨x, hx, hp⟩
    · left; rw [Limit.passes_either, h, Bool.true_or]
    · rcases List.mem_cons.mp hx with rfl | hx
      · left; rw [Limit.passes_either, hp, Bool.or_true]
      · exact Or.inr ⟨x, hx, hp⟩

/-- One side of an OR passes a row whose terms hold of it and each of whose
edges' lookups pass it. -/
theorem sideScope_passes {bound : Nat → RScope} {rowsOf : Nat → List Row} {side : Side} {r : Row}
    (hterms : ∀ t ∈ side.terms, t.holds (some r) = true)
    (hedges : ∀ m ∈ side.edges,
      (RScope.joined m.memberValue m.partner m.value (bound m.partner) m.nulls).passes rowsOf r = true) :
    (Planned.sideScope bound side).passes rowsOf r = true := by
  unfold Planned.sideScope
  apply foldl_both_passes (fun m : Edge => RScope.joined m.memberValue m.partner m.value (bound m.partner) m.nulls)
    _ _ _ hedges
  apply foldl_both_passes STerm.bind _ _ rfl
  intro t ht
  rw [STerm.bind_passes]
  exact hterms t ht

/-- An item's scope, with `bound` giving the items placed before it their
scopes, passes a row of which its terms and what its edges carry hold, that
each of its memberships' scopes passes, that a side of each OR it keeps
passes, and that the LIMIT bound passes. -/
theorem scopeAt_passes {p : Planned} {last : Option (List (List Value))} {bound : Nat → RScope}
    {rowsOf : Nat → List Row} {i : Nat} {r : Row}
    (hterm : ∀ t, (p.state.items i).term = some t → t.holds (some r) = true)
    (hcarried : ∀ t, (p.state.items i).carried = some t → t.holds (some r) = true)
    (hmembers : ∀ m ∈ (p.state.items i).memberships,
      (Planned.membershipScope bound m).passes rowsOf r = true)
    (hgroups : ∀ q, ∀ e0 ∈ (p.state.items i).memberships, e0.partner = q → e0.nested = false →
      e0.pushed = false → e0.nulls = false →
      ∃ r' ∈ rowsOf q, (bound q).passes rowsOf r' = true ∧
        ∀ e ∈ (p.state.items i).memberships, e.partner = q → e.nested = false → e.pushed = false →
          e.nulls = false →
          e.value.eval r' = e.memberValue.eval r ∧
            ((e.memberValue.eval r).isSome = true ∨ e.nulls = true))
    (heither : ∀ sides ∈ (p.state.items i).either, ∃ side ∈ sides,
      (Planned.sideScope bound side).passes rowsOf r = true)
    (hbound : ∀ sorted values found, p.sorted = some sorted → last = some values →
      Limit.runBound i (p.lookupOf bound i) (sorted.runs.zip values) = some found →
      found.passes rowsOf r = true) :
    (p.scopeAt last bound i).passes rowsOf r = true := by
  have hbase : ((((p.state.items i).term.map STerm.bind).getD .all).both
      (((p.state.items i).carried.map STerm.bind).getD .all)).passes rowsOf r = true := by
    rw [Limit.passes_both]
    cases ht : (p.state.items i).term <;> cases hc : (p.state.items i).carried <;>
      simp [RScope.passes, STerm.bind_passes, hterm, hcarried, ht, hc]
  have hm := foldl_both_passes (Planned.membershipScope bound) _ _ hbase hmembers
  have hm := foldl_both_passes id _ _ hm (groupScopes_passes hgroups)
  have he : ∀ (l : List (List Side)) (acc : RScope), acc.passes rowsOf r = true →
      (∀ sides ∈ l, ∃ side ∈ sides, (Planned.sideScope bound side).passes rowsOf r = true) →
      (l.foldl (fun scope sides =>
        match sides.map (Planned.sideScope bound) with
        | [] => scope
        | first :: rest => scope.both (rest.foldl RScope.either first)) acc).passes rowsOf r = true := by
    intro l
    induction l with
    | nil => intro acc h _; exact h
    | cons sides l ih =>
      intro acc h hl
      simp only [List.foldl_cons]
      apply ih _ _ (fun s hs => hl s (List.mem_cons_of_mem _ hs))
      split
      · exact h
      · rename_i first rest hmap
        rw [Limit.passes_both, h, Bool.true_and]
        obtain ⟨side, hside, hp⟩ := hl sides (by simp)
        have hmem : Planned.sideScope bound side ∈ first :: rest := by
          rw [← hmap]; exact List.mem_map_of_mem hside
        exact foldl_either_passes rest first (by
          rcases List.mem_cons.mp hmem with h | h
          · left; rw [← h]; exact hp
          · exact Or.inr ⟨_, h, hp⟩)
  have hall := he _ _ hm heither
  unfold Planned.scopeAt
  simp only
  cases hs : p.sorted with
  | none => exact hall
  | some sorted =>
    cases hl : last with
    | none => exact hall
    | some values =>
      simp only
      split
      · rename_i found hf
        rw [Limit.passes_both, Bool.and_eq_true]
        exact ⟨hall, hbound sorted values found hs hl hf⟩
      · exact hall

/-- The scopes' fold changes no item it doesn't place. -/
theorem scopes_fold_untouched (p : Planned) (last : Option (List (List Value))) :
    ∀ (l : List Nat) (bound : Nat → RScope) (j : Nat), j ∉ l →
      (l.foldl (fun bound i =>
        let scope := p.scopeAt last bound i
        fun j => if j = i then scope else bound j) bound) j = bound j
  | [], _, _, _ => rfl
  | i :: l, bound, j, hj => by
    simp only [List.foldl_cons]
    rw [scopes_fold_untouched p last l _ j (fun h => hj (List.mem_cons_of_mem i h))]
    have hji : j ≠ i := fun e => hj (e ▸ List.mem_cons_self)
    simp [hji]

/-- A list without repeats splits at one of its elements one way only. -/
theorem split_unique {α : Type} {i : α} :
    ∀ {pre pre' post post' : List α}, (pre ++ i :: post).Nodup →
      pre ++ i :: post = pre' ++ i :: post' → pre = pre'
  | [], [], _, _, _, _ => rfl
  | [], a :: pre', post, post', hnd, h => by
    simp only [List.nil_append, List.cons_append, List.cons.injEq] at h
    obtain ⟨rfl, rfl⟩ := h
    simp at hnd
  | a :: pre, [], post, post', hnd, h => by
    simp only [List.nil_append, List.cons_append, List.cons.injEq] at h
    obtain ⟨rfl, _⟩ := h
    simp at hnd
  | a :: pre, b :: pre', post, post', hnd, h => by
    simp only [List.cons_append, List.cons.injEq] at h
    obtain ⟨rfl, h⟩ := h
    rw [split_unique (List.nodup_cons.mp (by simpa using hnd)).2 h]

/-- A placed item's scope is its scope with the scopes of the items placed
before it, which keep those scopes to the end. -/
theorem Planned.scopes_eq (p : Planned) (last : Option (List (List Value))) {i : Nat}
    (hnd : p.order.Nodup) (hi : i ∈ p.order) :
    ∃ bound : Nat → RScope, p.scopes last i = p.scopeAt last bound i ∧
      ∀ j, (∃ pre post, p.order = pre ++ i :: post ∧ j ∈ pre) → bound j = p.scopes last j := by
  obtain ⟨pre, post, hsplit⟩ := List.append_of_mem hi
  let step := fun (bound : Nat → RScope) (i : Nat) =>
    let scope := p.scopeAt last bound i
    fun j => if j = i then scope else bound j
  let B := pre.foldl step fun _ => .all
  have hfold : p.scopes last = post.foldl step (step B i) := by
    unfold Planned.scopes
    rw [hsplit, List.foldl_append, List.foldl_cons]
  rw [hsplit] at hnd
  have hipost : i ∉ post := by
    have := (List.nodup_append.mp hnd).2.1
    exact (List.nodup_cons.mp this).1
  refine ⟨B, ?_, ?_⟩
  · rw [hfold, scopes_fold_untouched p last post _ i hipost]
    simp [step]
  · rintro j ⟨pre', post', hsplit', hj⟩
    -- The order splits at `i` one way only.
    have hpre : pre' = pre := by
      rw [hsplit] at hsplit'
      exact (split_unique hnd hsplit').symm
    subst hpre
    have hjpost : j ∉ post := by
      intro hm
      exact (List.nodup_append.mp hnd).2.2 j hj j (List.mem_cons_of_mem i hm) rfl
    have hji : j ≠ i := by
      intro e
      subst e
      exact (List.nodup_append.mp hnd).2.2 j hj j List.mem_cons_self rfl
    rw [hfold, scopes_fold_untouched p last post _ j hjpost]
    simp [step, hji, B]

end SqliveNarrowing
