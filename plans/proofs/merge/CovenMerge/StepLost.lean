import CovenMerge.Step

/-!
# One step, `coven_lost`

The arriving write `w` changes row `r` with change `ch`. For every value `a`
of every cell of `r`, `lostStep` gives the `coven_lost` row the declarative
`Lost` predicate gives for `S ∪ {w}`.
-/

namespace CovenMerge

section
variable {W Row Col : Type} [DecidableEq W] {M : Writes W Row Col}

theorem minDel_unique (hV : Valid M) {S : W → Prop} {r : Row} {k : Nat} {x y : W}
    (hx : MinDel M S r k x) (hy : MinDel M S r k y) : x = y :=
  isMinBy_unique (P := fun z => Del M S r z k) hV.ts_inj hx hy

theorem maxSetter_unique (hV : Valid M) {S : W → Prop} {r : Row} {c : Col} {k : Nat} {x y : W}
    (hx : MaxSetter M S r c k x) (hy : MaxSetter M S r c k y) : x = y :=
  isMaxBy_unique (P := fun z => Setter M S r c z k) hV.ts_inj hx hy

theorem some_pair_iff {α β : Type} {a k : α} {b x : β} :
    some (a, b) = some (k, x) ↔ a = k ∧ b = x := by simp

theorem canon_unique (hV : Valid M) {S : W → Prop} {r : Row} {c : Col} {k : Nat} {x y : W}
    (hx : Canon M S r c k x) (hy : Canon M S r c k y) : x = y := by
  by_cases hd : ∃ d, Del M S r d k
  · exact minDel_unique hV ((canon_of_del hd).1 hx) ((canon_of_del hd).1 hy)
  · exact maxSetter_unique hV ((canon_of_nodel hd).1 hx) ((canon_of_nodel hd).1 hy)

variable {S : W → Prop} {st : St W Row Col} {w : W}

theorem not_past_of_mem (hcl : Closed M S) (hw : ¬ S w) {x : W} (hx : S x) :
    M.past x w = false := by
  cases h : M.past x w
  · rfl
  · exact absurd (hcl x hx w h) hw

/-! ### The arriving write's own values -/

theorem lostStep_self (hV : Valid M) (hcl : Closed M S) (hS : IsSpec M S st) (hw : ¬ S w)
    {r : Row} {ch : Change Col} (hch : M.chg w r = some ch) (c : Col) (k : Nat) (x : W) :
    lostStep M (st.gen r) (st.genWrite r) (st.cell r c) (st.lost r c w) c w w ch = some (k, x) ↔
      Lost M (ins S w) r c w k x := by
  unfold lostStep
  rw [ite_pos' rfl]
  by_cases hs : ch.kind ≠ .del ∧ ch.sets c = true
  · rw [ite_pos' hs]
    have hrep : ∀ k y, Replacer M (ins S w) r c w k y ↔ Replacer M S r c w k y := by
      intro k y
      rw [replacer_some hch]
      constructor
      · rintro (⟨_, ⟨_, _, _, hlt⟩ | ⟨hd, _⟩⟩ | h)
        · exact absurd hlt (Nat.lt_irrefl _)
        · exact absurd hd hs.1
        · exact h
      · exact Or.inr
    have hdel : ∀ k y, Del M (ins S w) r y k ↔ Del M S r y k := by
      intro k y
      rw [del_some hch]
      constructor
      · rintro (⟨_, hd, _⟩ | h)
        · exact absurd hd hs.1
        · exact h
      · exact Or.inr
    have hsw : Setter M (ins S w) r c w ch.inc :=
      (setter_some hch c w _).2 (Or.inl ⟨rfl, hs.1, hs.2, rfl⟩)
    have hodd := setter_inc_odd hV hsw
    have hL : Lost M (ins S w) r c w k x ↔
        k = ch.inc ∧ (∃ y, Replacer M S r c w ch.inc y) ∧ Canon M (ins S w) r c ch.inc x := by
      constructor
      · rintro ⟨h1, ⟨y, hy⟩, _, h4⟩
        have hk : k = ch.inc := setter_inc_eq h1 hsw
        subst hk
        exact ⟨rfl, ⟨y, (hrep _ y).1 hy⟩, h4⟩
      · rintro ⟨hk, ⟨y, hy⟩, h4⟩
        subst hk
        refine ⟨hsw, ⟨y, (hrep _ y).2 hy⟩, fun z hz => ?_, h4⟩
        exact not_past_of_mem hcl hw (replacer_mem ((hrep _ z).1 hz))
    rw [hL]
    by_cases hlt : ch.inc < st.gen r
    · -- made at an incarnation already deleted
      rw [ite_pos' hlt]
      have hd : ∃ d, Del M S r d ch.inc := del_of_lt hV hS hodd hlt
      have hd' : ∃ d, Del M (ins S w) r d ch.inc :=
        let ⟨d, h⟩ := hd; ⟨d, (hdel _ d).2 h⟩
      rw [canon_of_del hd']
      have hmd : MinDel M (ins S w) r ch.inc x ↔ MinDel M S r ch.inc x := by
        unfold MinDel; simp only [hdel]
      rw [hmd, ← gw_minDel hV hS hodd]
      obtain ⟨d, hd0⟩ := hd
      cases hg : st.genWrite r (ch.inc + 1) with
      | none => exact absurd ((genChange_succ_iff_del hV hodd d).2 hd0) (hS.gw_none r _ hg d)
      | some g =>
        simp only [Option.map]
        constructor
        · intro h
          cases h
          exact ⟨rfl, ⟨d, Or.inr hd0⟩, rfl⟩
        · rintro ⟨hk, _, hx⟩
          cases hx
          rw [hk]
    · rw [ite_neg' hlt]
      by_cases heq : ch.inc = st.gen r
      · -- made at the current incarnation
        rw [ite_pos' heq]
        have hnd : ¬ ∃ d, Del M S r d ch.inc := fun ⟨d, h⟩ => by
          have := del_lt hS h; omega
        have hnd' : ¬ ∃ d, Del M (ins S w) r d ch.inc := fun ⟨d, h⟩ => hnd ⟨d, (hdel _ d).1 h⟩
        rw [canon_of_nodel hnd']
        cases hcur : st.cell r c with
        | none =>
          simp only
          constructor
          · intro h; cases h
          · rintro ⟨_, ⟨y, hy⟩, _⟩
            rcases hy with ⟨hy, _⟩ | hy
            · rw [heq] at hy; exact (hS.cell_none r c hcur y hy).elim
            · exact (hnd ⟨y, hy⟩).elim
        | some s =>
          have hsm : MaxSetter M S r c (st.gen r) s := (hS.cell_some r c s).1 hcur
          simp only
          by_cases hts : M.ts w < M.ts s
          · rw [ite_pos' hts]
            have hmax : MaxSetter M (ins S w) r c ch.inc s := by
              rw [heq]
              refine ⟨(setter_some hch c s _).2 (Or.inr hsm.1), fun y hy => ?_⟩
              rcases (setter_some hch c y _).1 hy with ⟨hy, _⟩ | hy
              · rw [hy]; omega
              · exact hsm.2 y hy
            constructor
            · intro h
              obtain ⟨hk, hx⟩ := some_pair_iff.1 h
              rw [← hx]
              exact ⟨hk.symm, ⟨s, Or.inl ⟨by rw [heq]; exact hsm.1, hts⟩⟩, hmax⟩
            · rintro ⟨hk, _, hx⟩
              rw [hk, maxSetter_unique hV hx hmax]
          · rw [ite_neg' hts]
            constructor
            · intro h; cases h
            · rintro ⟨_, ⟨y, hy⟩, _⟩
              rcases hy with ⟨hy, hlt'⟩ | hy
              · rw [heq] at hy; have := hsm.2 y hy; omega
              · exact (hnd ⟨y, hy⟩).elim
      · -- an insert starting a new incarnation: nothing replaces it yet
        rw [ite_neg' heq]
        constructor
        · intro h; cases h
        · rintro ⟨_, ⟨y, hy⟩, _⟩
          rcases hy with ⟨hy, _⟩ | hy
          · have := setter_le hV hcl hS hy; omega
          · have := del_lt hS hy; omega
  · rw [ite_neg' hs]
    constructor
    · intro h; cases h
    · rintro ⟨h1, _⟩
      rcases (setter_some hch c w k).1 h1 with ⟨_, h2, h3, _⟩ | h
      · exact absurd ⟨h2, h3⟩ hs
      · exact absurd h.1 hw

/-! ### A value that becomes lost when `w` arrives -/

/-- `a` is the current value of cell `(r, c)`, `w` replaces it without having
read it: in `S ∪ {w}` the value is lost, replaced by `w`. -/
theorem lost_new (hS : IsSpec M S st) {r : Row} {ch : Change Col} (hch : M.chg w r = some ch)
    {c : Col} {a : W} (hms : MaxSetter M S r c (st.gen r) a)
    (hr : (ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = st.gen r ∧ M.ts a < M.ts w) ∨
      (ch.kind = .del ∧ ch.gen = st.gen r))
    (hp : M.past w a = false) : Lost M (ins S w) r c a (st.gen r) w := by
  have hnS : ∀ y, ¬ Replacer M S r c a (st.gen r) y := by
    rintro y (⟨hy, hlt⟩ | hy)
    · have := hms.2 y hy; omega
    · have := del_lt hS hy; omega
  refine ⟨(setter_some hch c a _).2 (Or.inr hms.1),
    ⟨w, (replacer_some hch c a _ w).2 (Or.inl ⟨rfl, hr⟩)⟩, fun y hy => ?_, ?_⟩
  · rcases (replacer_some hch c a _ y).1 hy with ⟨hy, _⟩ | hy
    · rw [hy]; exact hp
    · exact absurd hy (hnS y)
  · by_cases hd : ch.kind = .del
    · have hg : ch.gen = st.gen r := by
        rcases hr with ⟨h, _⟩ | ⟨_, h⟩
        · exact absurd hd h
        · exact h
      have hdw : Del M (ins S w) r w (st.gen r) := (del_some hch w _).2 (Or.inl ⟨rfl, hd, hg⟩)
      rw [canon_of_del ⟨w, hdw⟩]
      refine ⟨hdw, fun y hy => ?_⟩
      rcases (del_some hch y _).1 hy with ⟨hy, _⟩ | hy
      · rw [hy]; exact Nat.le_refl _
      · exact absurd (Or.inr hy) (hnS y)
    · obtain ⟨hk1, hk2, hk3, hk4⟩ : ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = st.gen r ∧
          M.ts a < M.ts w := by
        rcases hr with h | ⟨h, _⟩
        · exact h
        · exact absurd h hd
      have hnd : ¬ ∃ d, Del M (ins S w) r d (st.gen r) := by
        rintro ⟨d, hd'⟩
        rcases (del_some hch d _).1 hd' with ⟨_, h, _⟩ | h
        · exact hd h
        · exact hnS d (Or.inr h)
      rw [canon_of_nodel hnd]
      refine ⟨(setter_some hch c w _).2 (Or.inl ⟨rfl, hk1, hk2, hk3⟩), fun y hy => ?_⟩
      rcases (setter_some hch c y _).1 hy with ⟨hy, _⟩ | hy
      · rw [hy]; exact Nat.le_refl _
      · have := hms.2 y hy; omega

/-! ### Values of other writes -/

theorem lostStep_other (hV : Valid M) (hcl : Closed M S) (hS : IsSpec M S st) (hw : ¬ S w)
    {r : Row} {ch : Change Col} (hch : M.chg w r = some ch) (c : Col) {a : W} (haw : a ≠ w)
    (k : Nat) (x : W) :
    lostStep M (st.gen r) (st.genWrite r) (st.cell r c) (st.lost r c a) c a w ch = some (k, x) ↔
      Lost M (ins S w) r c a k x := by
  unfold lostStep
  rw [ite_neg' haw]
  have hsa' : ∀ k, Setter M (ins S w) r c a k ↔ Setter M S r c a k := by
    intro k
    rw [setter_some hch]
    constructor
    · rintro (⟨h, _⟩ | h)
      · exact absurd h haw
      · exact h
    · exact Or.inr
  cases hold : st.lost r c a with
  | some p =>
    obtain ⟨ka, xa⟩ := p
    simp only
    have hL : Lost M S r c a ka xa := (hS.lost r c a ka xa).1 hold
    obtain ⟨hsa, ⟨y0, hy0⟩, hunread, hcanon⟩ := hL
    have hle := setter_le hV hcl hS hsa
    have hodd := setter_inc_odd hV hsa
    have hshape : Lost M (ins S w) r c a k x ↔ k = ka ∧
        (∀ y, Replacer M (ins S w) r c a ka y → M.past y a = false) ∧
        Canon M (ins S w) r c ka x := by
      constructor
      · rintro ⟨h1, _, h3, h4⟩
        have hk : k = ka := setter_inc_eq ((hsa' k).1 h1) hsa
        subst hk
        exact ⟨rfl, h3, h4⟩
      · rintro ⟨hk, h3, h4⟩
        subst hk
        refine ⟨(hsa' _).2 hsa, ⟨y0, ?_⟩, h3, h4⟩
        rw [replacer_some hch]
        exact Or.inr hy0
    rw [hshape]
    by_cases hr : (ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = ka ∧ M.ts a < M.ts w) ∨
        (ch.kind = .del ∧ ch.gen = ka)
    · -- `w` replaces the lost value `a`
      rw [ite_pos' hr]
      have hwrep : Replacer M (ins S w) r c a ka w :=
        (replacer_some hch c a ka w).2 (Or.inl ⟨rfl, hr⟩)
      by_cases hp : M.past w a = true
      · -- having read it: no longer lost
        rw [ite_pos' hp]
        constructor
        · intro h; cases h
        · rintro ⟨_, h3, _⟩
          have := h3 w hwrep
          rw [hp] at this
          cases this
      · -- without having read it: still lost, possibly with a new "replaced by"
        rw [ite_neg' hp]
        have hp' : M.past w a = false := by
          cases h : M.past w a
          · rfl
          · exact absurd h hp
        have hall : ∀ y, Replacer M (ins S w) r c a ka y → M.past y a = false := by
          intro y hy
          rcases (replacer_some hch c a ka y).1 hy with ⟨hy, _⟩ | hy
          · rw [hy]; exact hp'
          · exact hunread y hy
        have hcanon' : Canon M (ins S w) r c ka x ↔ x =
            (if ch.kind = .del then (if ka < st.gen r then minBy M.ts (some xa) w else w)
             else (if ka < st.gen r then xa else maxBy M.ts (some xa) w)) := by
          by_cases hd : ch.kind = .del
          · rw [ite_pos' hd]
            have hg : ch.gen = ka := by
              rcases hr with ⟨h, _⟩ | ⟨_, h⟩
              · exact absurd hd h
              · exact h
            have hdel' : ∀ y, Del M (ins S w) r y ka ↔ y = w ∨ Del M S r y ka := by
              intro y
              rw [del_some hch]
              constructor
              · rintro (⟨h, _⟩ | h)
                · exact Or.inl h
                · exact Or.inr h
              · rintro (h | h)
                · exact Or.inl ⟨h, hd, hg⟩
                · exact Or.inr h
            have hex' : ∃ d, Del M (ins S w) r d ka := ⟨w, (hdel' w).2 (Or.inl rfl)⟩
            rw [canon_of_del hex']
            by_cases hlt : ka < st.gen r
            · rw [ite_pos' hlt]
              have hex := del_of_lt hV hS hodd hlt
              have hmd : MinDel M S r ka xa := (canon_of_del hex).1 hcanon
              exact isMinBy_insert hV.ts_inj (P := fun y => Del M S r y ka)
                (P' := fun y => Del M (ins S w) r y ka) (old := some xa)
                hdel' (fun h => hw h.1)
                (fun z => ⟨fun h => by cases h; exact hmd,
                  fun h => by rw [minDel_unique hV h hmd]⟩)
                (fun h => by cases h) x
            · rw [ite_neg' hlt]
              have hnd : ¬ ∃ d, Del M S r d ka := fun ⟨_, h⟩ => hlt (del_lt hS h)
              constructor
              · intro h
                rcases (hdel' x).1 h.1 with h | h
                · exact h
                · exact absurd ⟨x, h⟩ hnd
              · intro h
                rw [h]
                refine ⟨(hdel' w).2 (Or.inl rfl), fun y hy => ?_⟩
                rcases (hdel' y).1 hy with h | h
                · rw [h]; exact Nat.le_refl _
                · exact absurd ⟨y, h⟩ hnd
          · rw [ite_neg' hd]
            obtain ⟨hk1, hk2, hk3, _⟩ : ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = ka ∧
                M.ts a < M.ts w := by
              rcases hr with h | ⟨h, _⟩
              · exact h
              · exact absurd h hd
            have hdel' : ∀ y, Del M (ins S w) r y ka ↔ Del M S r y ka := by
              intro y
              rw [del_some hch]
              constructor
              · rintro (⟨_, h, _⟩ | h)
                · exact absurd h hd
                · exact h
              · exact Or.inr
            by_cases hlt : ka < st.gen r
            · rw [ite_pos' hlt]
              have hex := del_of_lt hV hS hodd hlt
              have hex' : ∃ d, Del M (ins S w) r d ka :=
                let ⟨d, h⟩ := hex; ⟨d, (hdel' d).2 h⟩
              rw [canon_of_del hex']
              have hmd : MinDel M S r ka xa := (canon_of_del hex).1 hcanon
              have e : MinDel M (ins S w) r ka x ↔ MinDel M S r ka x := by
                unfold MinDel; simp only [hdel']
              rw [e]
              exact ⟨fun h => minDel_unique hV h hmd, fun h => by rw [h]; exact hmd⟩
            · rw [ite_neg' hlt]
              have hnd : ¬ ∃ d, Del M S r d ka := fun ⟨_, h⟩ => hlt (del_lt hS h)
              have hnd' : ¬ ∃ d, Del M (ins S w) r d ka :=
                fun ⟨d, h⟩ => hnd ⟨d, (hdel' d).1 h⟩
              rw [canon_of_nodel hnd']
              have hms : MaxSetter M S r c ka xa := (canon_of_nodel hnd).1 hcanon
              exact isMaxBy_insert hV.ts_inj (P := fun y => Setter M S r c y ka)
                (P' := fun y => Setter M (ins S w) r c y ka) (old := some xa)
                (fun y => by
                  rw [setter_some hch]
                  constructor
                  · rintro (⟨h, _⟩ | h)
                    · exact Or.inl h
                    · exact Or.inr h
                  · rintro (h | h)
                    · exact Or.inl ⟨h, hk1, hk2, hk3⟩
                    · exact Or.inr h)
                (fun h => hw h.1)
                (fun z => ⟨fun h => by cases h; exact hms,
                  fun h => by rw [maxSetter_unique hV h hms]⟩)
                (fun h => by cases h) x
        rw [hcanon']
        constructor
        · intro h
          cases h
          exact ⟨rfl, hall, rfl⟩
        · rintro ⟨hk, _, hx⟩
          rw [hk, hx]
    · -- `w` does not replace `a`: its row stays as it is
      rw [ite_neg' hr]
      have hrep : ∀ y, Replacer M (ins S w) r c a ka y ↔ Replacer M S r c a ka y := by
        intro y
        rw [replacer_some hch]
        constructor
        · rintro (⟨_, h⟩ | h)
          · exact absurd h hr
          · exact h
        · exact Or.inr
      have hdel' : ∀ y, Del M (ins S w) r y ka ↔ Del M S r y ka := by
        intro y
        rw [del_some hch]
        constructor
        · rintro (⟨_, h1, h2⟩ | h)
          · exact absurd (Or.inr ⟨h1, h2⟩) hr
          · exact h
        · exact Or.inr
      have hcanonS : ∀ z, Canon M S r c ka z ↔ z = xa := fun z =>
        ⟨fun h => canon_unique hV h hcanon, fun h => by rw [h]; exact hcanon⟩
      have hcanon' : Canon M (ins S w) r c ka x ↔ x = xa := by
        by_cases hd : ∃ d, Del M S r d ka
        · have hd' : ∃ d, Del M (ins S w) r d ka := let ⟨d, h⟩ := hd; ⟨d, (hdel' d).2 h⟩
          rw [canon_of_del hd']
          have e : MinDel M (ins S w) r ka x ↔ MinDel M S r ka x := by
            unfold MinDel; simp only [hdel']
          rw [e, ← canon_of_del hd]
          exact hcanonS x
        · have hd' : ¬ ∃ d, Del M (ins S w) r d ka := fun ⟨d, h⟩ => hd ⟨d, (hdel' d).1 h⟩
          rw [canon_of_nodel hd']
          have hms : MaxSetter M S r c ka xa := (canon_of_nodel hd).1 hcanon
          by_cases hws : ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = ka
          · -- `w` sets the cell in this incarnation, with a smaller timestamp than `a`
            have hwa : M.ts w < M.ts a := by
              have h1 : ¬ M.ts a < M.ts w := fun h => hr (Or.inl ⟨hws.1, hws.2.1, hws.2.2, h⟩)
              have h2 : M.ts a ≠ M.ts w := fun h => haw (hV.ts_inj _ _ h)
              omega
            have hax := hms.2 a hsa
            have key := isMaxBy_insert hV.ts_inj (P := fun y => Setter M S r c y ka)
              (P' := fun y => Setter M (ins S w) r c y ka) (old := some xa)
              (fun y => by
                rw [setter_some hch]
                constructor
                · rintro (⟨h, _⟩ | h)
                  · exact Or.inl h
                  · exact Or.inr h
                · rintro (h | h)
                  · exact Or.inl ⟨h, hws⟩
                  · exact Or.inr h)
              (fun h => hw h.1)
              (fun z => ⟨fun h => by cases h; exact hms,
                fun h => by rw [maxSetter_unique hV h hms]⟩)
              (fun h => by cases h) x
            refine key.trans ?_
            simp only [maxBy]
            have : ¬ M.ts xa < M.ts w := by omega
            rw [ite_neg' this]
          · have e : ∀ y, Setter M (ins S w) r c y ka ↔ Setter M S r c y ka := by
              intro y
              rw [setter_some hch]
              constructor
              · rintro (⟨_, h⟩ | h)
                · exact absurd h hws
                · exact h
              · exact Or.inr
            have e2 : MaxSetter M (ins S w) r c ka x ↔ MaxSetter M S r c ka x :=
              isMaxBy_congr (P := fun y => Setter M (ins S w) r c y ka)
                (Q := fun y => Setter M S r c y ka) e x
            rw [e2, ← canon_of_nodel hd]
            exact hcanonS x
      have hall : ∀ y, Replacer M (ins S w) r c a ka y → M.past y a = false :=
        fun y hy => hunread y ((hrep y).1 hy)
      rw [hcanon']
      constructor
      · intro h
        cases h
        exact ⟨rfl, hall, rfl⟩
      · rintro ⟨hk, _, hx⟩
        rw [hk, hx]
  | none =>
    simp only
    have hnl : ∀ k x, ¬ Lost M S r c a k x := fun k x h => by
      have := (hS.lost r c a k x).2 h
      rw [hold] at this
      cases this
    constructor
    · intro h
      cases hcur : st.cell r c with
      | none =>
        rw [hcur] at h
        cases h
      | some s =>
        rw [hcur] at h
        simp only at h
        by_cases hcond : s = a ∧ ((ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = st.gen r ∧
            M.ts a < M.ts w) ∨ (ch.kind = .del ∧ ch.gen = st.gen r)) ∧ M.past w a = false
        · rw [ite_pos' hcond] at h
          cases h
          obtain ⟨hsa_eq, hr, hp⟩ := hcond
          rw [hsa_eq] at hcur
          exact lost_new hS hch ((hS.cell_some r c a).1 hcur) hr hp
        · rw [ite_neg' hcond] at h
          cases h
    · intro hL
      obtain ⟨h1, ⟨y, hy⟩, h3, h4⟩ := hL
      have hsa : Setter M S r c a k := (hsa' k).1 h1
      have hnoS : ¬ ∃ y, Replacer M S r c a k y := by
        rintro ⟨y', hy'⟩
        obtain ⟨z, hz⟩ := canon_exists hV hcl hS hsa
        exact hnl k z ⟨hsa, ⟨y', hy'⟩,
          fun y'' hy'' => h3 y'' ((replacer_some hch c a k y'').2 (Or.inr hy'')), hz⟩
      obtain ⟨hkG, hcur⟩ := unreplaced hV hcl hS hsa hnoS
      rcases (replacer_some hch c a k y).1 hy with ⟨hyw, hr⟩ | hyS
      · have hp : M.past w a = false := by
          have := h3 y hy
          rw [hyw] at this
          exact this
        rw [hkG] at hr h4
        have hLw := lost_new hS hch ((hS.cell_some r c a).1 hcur) hr hp
        have hx : x = w := canon_unique hV h4 hLw.2.2.2
        rw [hcur]
        show (if a = a ∧ _ ∧ M.past w a = false then some (st.gen r, w) else none) = some (k, x)
        rw [ite_pos' ⟨rfl, hr, hp⟩, hkG, hx]
      · exact absurd ⟨y, hyS⟩ hnoS

/-! ### The `coven_lost` step -/

theorem step_lost (hV : Valid M) (hcl : Closed M S) (hS : IsSpec M S st) (hw : ¬ S w)
    (r : Row) (c : Col) (a : W) (k : Nat) (x : W) :
    (step M st w).lost r c a = some (k, x) ↔ Lost M (ins S w) r c a k x := by
  unfold step
  simp only
  cases hch : M.chg w r with
  | none =>
    simp only
    rw [hS.lost]
    exact (lost_congr (fun y k => setter_none hch c y k) (fun y k => del_none hch y k) a k x).symm
  | some ch =>
    simp only
    by_cases haw : a = w
    · rw [haw]
      exact lostStep_self hV hcl hS hw hch c k x
    · exact lostStep_other hV hcl hS hw hch c haw k x

end

end CovenMerge
