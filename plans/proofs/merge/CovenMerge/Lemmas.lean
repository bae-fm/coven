import CovenMerge.Model

/-!
# Lemmas used by the step proof

* generic facts about the largest/smallest element of a set by timestamp;
* how the declarative predicates change when one write joins the set;
* facts about a state that satisfies `IsSpec` for a causally closed set.
-/

namespace CovenMerge

section Generic
variable {W : Type} [DecidableEq W]

/-- `x` is the element of `P` with the largest `ts`. -/
def IsMaxBy (ts : W → Nat) (P : W → Prop) (x : W) : Prop := P x ∧ ∀ y, P y → ts y ≤ ts x

/-- `x` is the element of `P` with the smallest `ts`. -/
def IsMinBy (ts : W → Nat) (P : W → Prop) (x : W) : Prop := P x ∧ ∀ y, P y → ts x ≤ ts y

theorem isMinBy_unique {ts : W → Nat} (inj : ∀ a b, ts a = ts b → a = b) {P : W → Prop}
    {x y : W} (hx : IsMinBy ts P x) (hy : IsMinBy ts P y) : x = y :=
  inj _ _ (Nat.le_antisymm (hx.2 y hy.1) (hy.2 x hx.1))

theorem isMaxBy_unique {ts : W → Nat} (inj : ∀ a b, ts a = ts b → a = b) {P : W → Prop}
    {x y : W} (hx : IsMaxBy ts P x) (hy : IsMaxBy ts P y) : x = y :=
  inj _ _ (Nat.le_antisymm (hy.2 x hx.1) (hx.2 y hy.1))

/-- Adding `w` to a set whose largest element is tracked by `old`: the new
largest is `maxBy ts old w`. -/
theorem isMaxBy_insert {ts : W → Nat} (inj : ∀ a b, ts a = ts b → a = b)
    {P P' : W → Prop} {w : W} {old : Option W}
    (hP' : ∀ y, P' y ↔ y = w ∨ P y) (hw : ¬ P w)
    (hsome : ∀ x, old = some x ↔ IsMaxBy ts P x) (hnone : old = none → ∀ x, ¬ P x) :
    ∀ x, IsMaxBy ts P' x ↔ x = maxBy ts old w := by
  intro x
  cases hold : old with
  | none =>
    have hP := hnone hold
    simp only [maxBy]
    constructor
    · intro h
      rcases (hP' x).1 h.1 with hx | hx
      · exact hx
      · exact absurd hx (hP x)
    · intro hx
      subst hx
      refine ⟨(hP' x).2 (Or.inl rfl), fun y hy => ?_⟩
      rcases (hP' y).1 hy with hy | hy
      · subst hy; exact Nat.le_refl _
      · exact absurd hy (hP y)
  | some s =>
    have hs : IsMaxBy ts P s := (hsome s).1 hold
    have hsw : s ≠ w := fun h => hw (h ▸ hs.1)
    simp only [maxBy]
    by_cases hlt : ts s < ts w
    · simp only [hlt, ite_true]
      constructor
      · intro h
        rcases (hP' x).1 h.1 with hx | hx
        · exact hx
        · have h1 := h.2 w ((hP' w).2 (Or.inl rfl))
          have h2 := hs.2 x hx
          omega
      · intro hx
        subst hx
        refine ⟨(hP' x).2 (Or.inl rfl), fun y hy => ?_⟩
        rcases (hP' y).1 hy with hy | hy
        · subst hy; exact Nat.le_refl _
        · have := hs.2 y hy; omega
    · simp only [hlt, ite_false]
      have hne : ts s ≠ ts w := fun h => hsw (inj _ _ h)
      have hws : ts w < ts s := by omega
      constructor
      · intro h
        rcases (hP' x).1 h.1 with hx | hx
        · subst hx
          have := h.2 s ((hP' s).2 (Or.inr hs.1)); omega
        · have hm : IsMaxBy ts P x := ⟨hx, fun y hy => h.2 y ((hP' y).2 (Or.inr hy))⟩
          have := (hsome x).2 hm
          rw [hold] at this
          exact (Option.some.inj this).symm
      · intro hx
        subst hx
        refine ⟨(hP' x).2 (Or.inr hs.1), fun y hy => ?_⟩
        rcases (hP' y).1 hy with hy | hy
        · subst hy; omega
        · exact hs.2 y hy

/-- Adding `w` to a set whose smallest element is tracked by `old`: the new
smallest is `minBy ts old w`. -/
theorem isMinBy_insert {ts : W → Nat} (inj : ∀ a b, ts a = ts b → a = b)
    {P P' : W → Prop} {w : W} {old : Option W}
    (hP' : ∀ y, P' y ↔ y = w ∨ P y) (hw : ¬ P w)
    (hsome : ∀ x, old = some x ↔ IsMinBy ts P x) (hnone : old = none → ∀ x, ¬ P x) :
    ∀ x, IsMinBy ts P' x ↔ x = minBy ts old w := by
  intro x
  cases hold : old with
  | none =>
    have hP := hnone hold
    simp only [minBy]
    constructor
    · intro h
      rcases (hP' x).1 h.1 with hx | hx
      · exact hx
      · exact absurd hx (hP x)
    · intro hx
      subst hx
      refine ⟨(hP' x).2 (Or.inl rfl), fun y hy => ?_⟩
      rcases (hP' y).1 hy with hy | hy
      · subst hy; exact Nat.le_refl _
      · exact absurd hy (hP y)
  | some s =>
    have hs : IsMinBy ts P s := (hsome s).1 hold
    have hsw : s ≠ w := fun h => hw (h ▸ hs.1)
    simp only [minBy]
    by_cases hlt : ts w < ts s
    · simp only [hlt, ite_true]
      constructor
      · intro h
        rcases (hP' x).1 h.1 with hx | hx
        · exact hx
        · have h1 := h.2 w ((hP' w).2 (Or.inl rfl))
          have h2 := hs.2 x hx
          omega
      · intro hx
        subst hx
        refine ⟨(hP' x).2 (Or.inl rfl), fun y hy => ?_⟩
        rcases (hP' y).1 hy with hy | hy
        · subst hy; exact Nat.le_refl _
        · have := hs.2 y hy; omega
    · simp only [hlt, ite_false]
      have hne : ts s ≠ ts w := fun h => hsw (inj _ _ h)
      have hws : ts s < ts w := by omega
      constructor
      · intro h
        rcases (hP' x).1 h.1 with hx | hx
        · subst hx
          have := h.2 s ((hP' s).2 (Or.inr hs.1)); omega
        · have hm : IsMinBy ts P x := ⟨hx, fun y hy => h.2 y ((hP' y).2 (Or.inr hy))⟩
          have := (hsome x).2 hm
          rw [hold] at this
          exact (Option.some.inj this).symm
      · intro hx
        subst hx
        refine ⟨(hP' x).2 (Or.inr hs.1), fun y hy => ?_⟩
        rcases (hP' y).1 hy with hy | hy
        · subst hy; omega
        · exact hs.2 y hy

end Generic

section Spec
variable {W Row Col : Type} [DecidableEq W] {M : Writes W Row Col}

/-- The set `S` with `w` added. -/
def ins (S : W → Prop) (w : W) : W → Prop := fun x => x = w ∨ S x

/-! ### Predicates on `ins S w`, row by row -/

theorem genChange_none {S : W → Prop} {w : W} {r : Row} (h : M.chg w r = none) (x : W) (n : Nat) :
    GenChange M (ins S w) r x n ↔ GenChange M S r x n := by
  constructor
  · rintro ⟨hx | hx, ch, hch, h1, h2⟩
    · subst hx; rw [h] at hch; cases hch
    · exact ⟨hx, ch, hch, h1, h2⟩
  · rintro ⟨hx, ch, hch, h1, h2⟩
    exact ⟨Or.inr hx, ch, hch, h1, h2⟩

theorem setter_none {S : W → Prop} {w : W} {r : Row} (h : M.chg w r = none) (c : Col) (x : W) (k : Nat) :
    Setter M (ins S w) r c x k ↔ Setter M S r c x k := by
  constructor
  · rintro ⟨hx | hx, ch, hch, h1, h2, h3⟩
    · subst hx; rw [h] at hch; cases hch
    · exact ⟨hx, ch, hch, h1, h2, h3⟩
  · rintro ⟨hx, ch, hch, h1, h2, h3⟩
    exact ⟨Or.inr hx, ch, hch, h1, h2, h3⟩

theorem del_none {S : W → Prop} {w : W} {r : Row} (h : M.chg w r = none) (x : W) (k : Nat) :
    Del M (ins S w) r x k ↔ Del M S r x k := by
  constructor
  · rintro ⟨hx | hx, ch, hch, h1, h2⟩
    · subst hx; rw [h] at hch; cases hch
    · exact ⟨hx, ch, hch, h1, h2⟩
  · rintro ⟨hx, ch, hch, h1, h2⟩
    exact ⟨Or.inr hx, ch, hch, h1, h2⟩

theorem genChange_some {S : W → Prop} {w : W} {r : Row} {ch : Change Col}
    (h : M.chg w r = some ch) (x : W) (n : Nat) :
    GenChange M (ins S w) r x n ↔ (x = w ∧ ch.kind ≠ .upd ∧ ch.gen + 1 = n) ∨ GenChange M S r x n := by
  constructor
  · rintro ⟨hx | hx, ch', hch, h1, h2⟩
    · subst hx; rw [h] at hch; cases hch; exact Or.inl ⟨rfl, h1, h2⟩
    · exact Or.inr ⟨hx, ch', hch, h1, h2⟩
  · rintro (⟨hx, h1, h2⟩ | ⟨hx, ch', hch, h1, h2⟩)
    · subst hx; exact ⟨Or.inl rfl, ch, h, h1, h2⟩
    · exact ⟨Or.inr hx, ch', hch, h1, h2⟩

theorem setter_some {S : W → Prop} {w : W} {r : Row} {ch : Change Col}
    (h : M.chg w r = some ch) (c : Col) (x : W) (k : Nat) :
    Setter M (ins S w) r c x k ↔
      (x = w ∧ ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = k) ∨ Setter M S r c x k := by
  constructor
  · rintro ⟨hx | hx, ch', hch, h1, h2, h3⟩
    · subst hx; rw [h] at hch; cases hch; exact Or.inl ⟨rfl, h1, h2, h3⟩
    · exact Or.inr ⟨hx, ch', hch, h1, h2, h3⟩
  · rintro (⟨hx, h1, h2, h3⟩ | ⟨hx, ch', hch, h1, h2, h3⟩)
    · subst hx; exact ⟨Or.inl rfl, ch, h, h1, h2, h3⟩
    · exact ⟨Or.inr hx, ch', hch, h1, h2, h3⟩

theorem del_some {S : W → Prop} {w : W} {r : Row} {ch : Change Col}
    (h : M.chg w r = some ch) (x : W) (k : Nat) :
    Del M (ins S w) r x k ↔ (x = w ∧ ch.kind = .del ∧ ch.gen = k) ∨ Del M S r x k := by
  constructor
  · rintro ⟨hx | hx, ch', hch, h1, h2⟩
    · subst hx; rw [h] at hch; cases hch; exact Or.inl ⟨rfl, h1, h2⟩
    · exact Or.inr ⟨hx, ch', hch, h1, h2⟩
  · rintro (⟨hx, h1, h2⟩ | ⟨hx, ch', hch, h1, h2⟩)
    · subst hx; exact ⟨Or.inl rfl, ch, h, h1, h2⟩
    · exact ⟨Or.inr hx, ch', hch, h1, h2⟩

/-- A write sets a cell in one incarnation only. -/
theorem setter_inc_eq {S₁ S₂ : W → Prop} {r : Row} {c : Col} {a : W} {k k' : Nat}
    (h₁ : Setter M S₁ r c a k) (h₂ : Setter M S₂ r c a k') : k = k' := by
  obtain ⟨_, ch, hch, _, _, h3⟩ := h₁
  obtain ⟨_, ch', hch', _, _, h3'⟩ := h₂
  rw [hch] at hch'
  cases hch'
  omega

/-! ### Parity -/

theorem kind_ne_ins_odd (hV : Valid M) {x : W} {r : Row} {ch : Change Col}
    (h : M.chg x r = some ch) (hk : ch.kind ≠ .ins) : ch.gen % 2 = 1 := by
  have := hV.parity x r ch h
  have : ¬ ch.gen % 2 = 0 := fun h0 => hk (this.2 h0)
  omega

theorem setter_inc_odd (hV : Valid M) {S : W → Prop} {r : Row} {c : Col} {x : W} {k : Nat}
    (h : Setter M S r c x k) : k % 2 = 1 := by
  obtain ⟨_, ch, hch, hk, _, hinc⟩ := h
  unfold Change.inc at hinc
  by_cases hi : ch.kind = .ins
  · have := (hV.parity x r ch hch).1 hi
    simp only [hi, ↓reduceIte] at hinc; omega
  · have := kind_ne_ins_odd hV hch hi
    simp only [hi, ↓reduceIte] at hinc; omega

theorem del_odd (hV : Valid M) {S : W → Prop} {r : Row} {x : W} {k : Nat}
    (h : Del M S r x k) : k % 2 = 1 := by
  obtain ⟨_, ch, hch, hk, hg⟩ := h
  have := kind_ne_ins_odd hV hch (by rw [hk]; decide)
  omega

/-- For an odd `k`, moving to `k + 1` is exactly deleting incarnation `k`. -/
theorem genChange_succ_iff_del (hV : Valid M) {S : W → Prop} {r : Row} {k : Nat}
    (hk : k % 2 = 1) (y : W) : GenChange M S r y (k + 1) ↔ Del M S r y k := by
  constructor
  · rintro ⟨hy, ch, hch, h1, h2⟩
    have hgen : ch.gen = k := by omega
    have hni : ch.kind ≠ .ins := by
      intro hi
      have := (hV.parity y r ch hch).1 hi
      omega
    refine ⟨hy, ch, hch, ?_, hgen⟩
    cases hc : ch.kind <;> simp_all
  · rintro ⟨hy, ch, hch, h1, h2⟩
    exact ⟨hy, ch, hch, by rw [h1]; decide, by omega⟩

theorem minGen_iff_minDel (hV : Valid M) {S : W → Prop} {r : Row} {k : Nat}
    (hk : k % 2 = 1) (x : W) : MinGen M S r (k + 1) x ↔ MinDel M S r k x := by
  unfold MinGen MinDel
  constructor
  · rintro ⟨h1, h2⟩
    exact ⟨(genChange_succ_iff_del hV hk x).1 h1,
      fun y hy => h2 y ((genChange_succ_iff_del hV hk y).2 hy)⟩
  · rintro ⟨h1, h2⟩
    exact ⟨(genChange_succ_iff_del hV hk x).2 h1,
      fun y hy => h2 y ((genChange_succ_iff_del hV hk y).1 hy)⟩

/-! ### Facts about a state that is the result for a closed set -/

variable {S : W → Prop} {st : St W Row Col}

theorem gen_ge (hS : IsSpec M S st) {r : Row} {x : W} {n : Nat}
    (h : GenChange M S r x n) : n ≤ st.gen r :=
  (hS.gen r).1 x n h

/-- A change's generation is at most the row's generation on any device that
has applied everything the change's write had read. -/
theorem chg_gen_le (hV : Valid M) (hS : IsSpec M S st) {w : W}
    (hpw : ∀ a, M.past w a = true → S a) {r : Row} {ch : Change Col}
    (h : M.chg w r = some ch) : ch.gen ≤ st.gen r := by
  rcases hV.gen_seen w r ch h with h0 | ⟨x, ch', hpx, hx, h1, h2⟩
  · omega
  · exact gen_ge hS ⟨hpw x hpx, ch', hx, h1, h2⟩

theorem setter_le (hV : Valid M) (hcl : Closed M S) (hS : IsSpec M S st)
    {r : Row} {c : Col} {x : W} {k : Nat}
    (h : Setter M S r c x k) : k ≤ st.gen r := by
  obtain ⟨hx, ch, hch, hk, _, hinc⟩ := h
  unfold Change.inc at hinc
  by_cases hi : ch.kind = .ins
  · simp only [hi, ↓reduceIte] at hinc
    exact gen_ge hS ⟨hx, ch, hch, by rw [hi]; decide, hinc⟩
  · simp only [hi, ↓reduceIte] at hinc
    have := chg_gen_le hV hS (fun a ha => hcl x hx a ha) hch
    omega

theorem del_lt (hS : IsSpec M S st) {r : Row} {x : W} {k : Nat}
    (h : Del M S r x k) : k < st.gen r := by
  obtain ⟨hx, ch, hch, hk, hg⟩ := h
  have := gen_ge hS ⟨hx, ch, hch, by rw [hk]; decide, rfl⟩
  omega

theorem del_of_lt (hV : Valid M) (hS : IsSpec M S st) {r : Row} {k : Nat}
    (hk : k % 2 = 1) (hlt : k < st.gen r) : ∃ d, Del M S r d k := by
  obtain ⟨x, hx⟩ := (hS.gen r).2 (k + 1) (by omega) (by omega)
  exact ⟨x, (genChange_succ_iff_del hV hk x).1 hx⟩

theorem del_exists_iff (hV : Valid M) (hS : IsSpec M S st) {r : Row} {k : Nat}
    (hk : k % 2 = 1) : (∃ d, Del M S r d k) ↔ k < st.gen r :=
  ⟨fun ⟨_, hd⟩ => del_lt hS hd, del_of_lt hV hS hk⟩

/-- The state's generation record for `k + 1` names the earliest delete of
incarnation `k`. -/
theorem gw_minDel (hV : Valid M) (hS : IsSpec M S st) {r : Row} {k : Nat}
    (hk : k % 2 = 1) (x : W) : st.genWrite r (k + 1) = some x ↔ MinDel M S r k x :=
  (hS.gw_some r (k + 1) x).trans (minGen_iff_minDel hV hk x)

/-- A value nobody replaced is the cell's current value. -/
theorem unreplaced (hV : Valid M) (hcl : Closed M S) (hS : IsSpec M S st)
    {r : Row} {c : Col} {a : W} {k : Nat} (ha : Setter M S r c a k)
    (hn : ¬ ∃ y, Replacer M S r c a k y) : k = st.gen r ∧ st.cell r c = some a := by
  have hle := setter_le hV hcl hS ha
  have hodd := setter_inc_odd hV ha
  have hk : k = st.gen r := by
    by_cases hlt : k < st.gen r
    · obtain ⟨d, hd⟩ := del_of_lt hV hS hodd hlt
      exact absurd ⟨d, Or.inr hd⟩ hn
    · omega
  refine ⟨hk, ?_⟩
  subst hk
  cases hcell : st.cell r c with
  | none => exact absurd ha (hS.cell_none r c hcell a)
  | some s =>
    have hs := (hS.cell_some r c s).1 hcell
    by_cases hsa : s = a
    · rw [hsa]
    · have hle := hs.2 a ha
      have hne : M.ts a ≠ M.ts s := fun h => hsa (hV.ts_inj _ _ h).symm
      exact absurd ⟨s, Or.inl ⟨hs.1, by omega⟩⟩ hn

/-- Every set value has a write to name as "replaced by". -/
theorem canon_exists (hV : Valid M) (hcl : Closed M S) (hS : IsSpec M S st)
    {r : Row} {c : Col} {a : W} {k : Nat} (ha : Setter M S r c a k) :
    ∃ x, Canon M S r c k x := by
  have hodd := setter_inc_odd hV ha
  by_cases hd : ∃ d, Del M S r d k
  · have hlt := del_lt hS hd.choose_spec
    cases hgw : st.genWrite r (k + 1) with
    | none =>
      obtain ⟨d, hd'⟩ := hd
      exact absurd ((genChange_succ_iff_del hV hodd d).2 hd') (hS.gw_none r (k + 1) hgw d)
    | some x => exact ⟨x, Or.inl ⟨hd, (gw_minDel hV hS hodd x).1 hgw⟩⟩
  · have hk : k = st.gen r := by
      have hle := setter_le hV hcl hS ha
      have : ¬ k < st.gen r := fun h => hd (del_of_lt hV hS hodd h)
      omega
    subst hk
    cases hcell : st.cell r c with
    | none => exact absurd ha (hS.cell_none r c hcell a)
    | some s => exact ⟨s, Or.inr ⟨hd, (hS.cell_some r c s).1 hcell⟩⟩

end Spec

end CovenMerge
