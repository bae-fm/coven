import CovenMerge.Lemmas

/-!
# One step

`step_spec`: if `st` is the declarative result for a causally closed set `S`,
and `w` is a new write whose had-read writes are all in `S`, then
`step M st w` is the declarative result for `S ∪ {w}`.
-/

namespace CovenMerge

theorem ite_pos' {α : Type} {p : Prop} [Decidable p] {a b : α} (h : p) :
    (if p then a else b) = a := by simp [h]

theorem ite_neg' {α : Type} {p : Prop} [Decidable p] {a b : α} (h : ¬ p) :
    (if p then a else b) = b := by simp [h]

section
variable {W Row Col : Type} [DecidableEq W] {M : Writes W Row Col}

theorem isMinBy_congr {P Q : W → Prop} (h : ∀ y, P y ↔ Q y) (x : W) :
    IsMinBy M.ts P x ↔ IsMinBy M.ts Q x := by
  unfold IsMinBy; simp only [h]

theorem isMaxBy_congr {P Q : W → Prop} (h : ∀ y, P y ↔ Q y) (x : W) :
    IsMaxBy M.ts P x ↔ IsMaxBy M.ts Q x := by
  unfold IsMaxBy; simp only [h]

theorem canon_of_del {S : W → Prop} {r : Row} {c : Col} {k : Nat} {x : W}
    (hd : ∃ d, Del M S r d k) : Canon M S r c k x ↔ MinDel M S r k x := by
  unfold Canon
  constructor
  · rintro (⟨_, h⟩ | ⟨h, _⟩)
    · exact h
    · exact absurd hd h
  · intro h; exact Or.inl ⟨hd, h⟩

theorem canon_of_nodel {S : W → Prop} {r : Row} {c : Col} {k : Nat} {x : W}
    (hd : ¬ ∃ d, Del M S r d k) : Canon M S r c k x ↔ MaxSetter M S r c k x := by
  unfold Canon
  constructor
  · rintro (⟨h, _⟩ | ⟨_, h⟩)
    · exact absurd h hd
    · exact h
  · intro h; exact Or.inr ⟨hd, h⟩

/-- `Lost` depends only on the setters and deletes of its row. -/
theorem lost_congr {S₁ S₂ : W → Prop} {r : Row} {c : Col}
    (hset : ∀ y k, Setter M S₁ r c y k ↔ Setter M S₂ r c y k)
    (hdel : ∀ y k, Del M S₁ r y k ↔ Del M S₂ r y k) (a : W) (k : Nat) (x : W) :
    Lost M S₁ r c a k x ↔ Lost M S₂ r c a k x := by
  unfold Lost Replacer Canon MinDel MaxSetter
  simp only [hset, hdel]

theorem replacer_some {S : W → Prop} {w : W} {r : Row} {ch : Change Col}
    (hch : M.chg w r = some ch) (c : Col) (a : W) (k : Nat) (y : W) :
    Replacer M (ins S w) r c a k y ↔
      (y = w ∧ ((ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = k ∧ M.ts a < M.ts w) ∨
        (ch.kind = .del ∧ ch.gen = k))) ∨ Replacer M S r c a k y := by
  unfold Replacer
  rw [setter_some hch, del_some hch]
  constructor
  · intro h
    rcases h with ⟨hs, hts⟩ | hd
    · rcases hs with ⟨hy, h1, h2, h3⟩ | h
      · subst hy; exact Or.inl ⟨rfl, Or.inl ⟨h1, h2, h3, hts⟩⟩
      · exact Or.inr (Or.inl ⟨h, hts⟩)
    · rcases hd with ⟨hy, h1, h2⟩ | h
      · exact Or.inl ⟨hy, Or.inr ⟨h1, h2⟩⟩
      · exact Or.inr (Or.inr h)
  · intro h
    rcases h with ⟨hy, hw⟩ | h
    · rcases hw with ⟨h1, h2, h3, hts⟩ | ⟨h1, h2⟩
      · subst hy; exact Or.inl ⟨Or.inl ⟨rfl, h1, h2, h3⟩, hts⟩
      · exact Or.inr (Or.inl ⟨hy, h1, h2⟩)
    · rcases h with ⟨h, hts⟩ | h
      · exact Or.inl ⟨Or.inr h, hts⟩
      · exact Or.inr (Or.inr h)

theorem replacer_mem {S : W → Prop} {r : Row} {c : Col} {a : W} {k : Nat} {y : W}
    (h : Replacer M S r c a k y) : S y := by
  rcases h with ⟨⟨hy, _⟩, _⟩ | ⟨hy, _⟩ <;> exact hy

variable {S : W → Prop} {st : St W Row Col} {w : W}

/-! ### Generation -/

theorem step_gen (hV : Valid M) (hS : IsSpec M S st)
    (hpw : ∀ a, M.past w a = true → S a) (r : Row) :
    IsGen M (ins S w) r ((step M st w).gen r) := by
  show IsGen M (ins S w) r (genStep (st.gen r) (M.chg w r))
  cases hch : M.chg w r with
  | none =>
    simp only [genStep]
    exact ⟨fun x m h => (hS.gen r).1 x m ((genChange_none hch x m).1 h),
      fun n h1 h2 => let ⟨x, hx⟩ := (hS.gen r).2 n h1 h2; ⟨x, (genChange_none hch x n).2 hx⟩⟩
  | some ch =>
    have hle := chg_gen_le hV hS hpw hch
    simp only [genStep]
    by_cases hc : ch.kind ≠ .upd ∧ st.gen r < ch.gen + 1
    · rw [ite_pos' hc]
      refine ⟨fun x m h => ?_, fun n h1 h2 => ?_⟩
      · rcases (genChange_some hch x m).1 h with ⟨_, _, h3⟩ | h
        · omega
        · have := gen_ge hS h; omega
      · by_cases hn : n = ch.gen + 1
        · exact ⟨w, (genChange_some hch w n).2 (Or.inl ⟨rfl, hc.1, hn.symm⟩)⟩
        · obtain ⟨x, hx⟩ := (hS.gen r).2 n h1 (by omega)
          exact ⟨x, (genChange_some hch x n).2 (Or.inr hx)⟩
    · rw [ite_neg' hc]
      refine ⟨fun x m h => ?_, fun n h1 h2 => ?_⟩
      · rcases (genChange_some hch x m).1 h with ⟨_, h2', h3⟩ | h
        · have : ¬ st.gen r < ch.gen + 1 := fun hlt => hc ⟨h2', hlt⟩
          omega
        · exact gen_ge hS h
      · obtain ⟨x, hx⟩ := (hS.gen r).2 n h1 h2
        exact ⟨x, (genChange_some hch x n).2 (Or.inr hx)⟩

/-! ### Generation records -/

theorem step_gw (hV : Valid M) (hS : IsSpec M S st) (hw : ¬ S w) (r : Row) (n : Nat) :
    (∀ x, (step M st w).genWrite r n = some x ↔ MinGen M (ins S w) r n x) ∧
    ((step M st w).genWrite r n = none → ∀ x, ¬ GenChange M (ins S w) r x n) := by
  show (∀ x, genWriteStep M (st.genWrite r n) n w (M.chg w r) = some x ↔ _) ∧
    (genWriteStep M (st.genWrite r n) n w (M.chg w r) = none → _)
  cases hch : M.chg w r with
  | none =>
    simp only [genWriteStep]
    refine ⟨fun x => ?_, fun h x hx => hS.gw_none r n h x ((genChange_none hch x n).1 hx)⟩
    rw [hS.gw_some]
    exact isMinBy_congr (fun y => (genChange_none hch y n).symm) x
  | some ch =>
    simp only [genWriteStep]
    by_cases hc : ch.kind ≠ .upd ∧ n = ch.gen + 1
    · rw [ite_pos' hc]
      refine ⟨fun x => ?_, fun h => by cases h⟩
      have key := isMinBy_insert hV.ts_inj
        (P := fun y => GenChange M S r y n) (P' := fun y => GenChange M (ins S w) r y n)
        (w := w) (old := st.genWrite r n)
        (fun y => by
          rw [genChange_some hch]
          constructor
          · rintro (⟨h1, _, _⟩ | h)
            · exact Or.inl h1
            · exact Or.inr h
          · rintro (h | h)
            · exact Or.inl ⟨h, hc.1, hc.2.symm⟩
            · exact Or.inr h)
        (fun h => hw h.1) (fun x => hS.gw_some r n x) (hS.gw_none r n) x
      constructor
      · intro h
        exact key.2 (Option.some.inj h).symm
      · intro h
        rw [key.1 h]
    · rw [ite_neg' hc]
      have e : ∀ y, GenChange M (ins S w) r y n ↔ GenChange M S r y n := by
        intro y
        rw [genChange_some hch]
        constructor
        · rintro (⟨_, h1, h2⟩ | h)
          · exact absurd ⟨h1, h2.symm⟩ hc
          · exact h
        · exact Or.inr
      refine ⟨fun x => ?_, fun h x hx => hS.gw_none r n h x ((e x).1 hx)⟩
      rw [hS.gw_some]
      exact isMinBy_congr (fun y => (e y).symm) x

/-! ### Cells -/

theorem step_cell (hV : Valid M) (hS : IsSpec M S st) (hw : ¬ S w)
    (hpw : ∀ a, M.past w a = true → S a) (hcl : Closed M S) (r : Row) (c : Col) :
    (∀ x, (step M st w).cell r c = some x ↔
      MaxSetter M (ins S w) r c ((step M st w).gen r) x) ∧
    ((step M st w).cell r c = none → ∀ x, ¬ Setter M (ins S w) r c x ((step M st w).gen r)) := by
  show (∀ x, cellStep M (st.gen r) (st.cell r c) c w (M.chg w r) = some x ↔
      MaxSetter M (ins S w) r c (genStep (st.gen r) (M.chg w r)) x) ∧
    (cellStep M (st.gen r) (st.cell r c) c w (M.chg w r) = none →
      ∀ x, ¬ Setter M (ins S w) r c x (genStep (st.gen r) (M.chg w r)))
  cases hch : M.chg w r with
  | none =>
    simp only [cellStep, genStep]
    refine ⟨fun x => ?_, fun h x hx => hS.cell_none r c h x ((setter_none hch c x _).1 hx)⟩
    rw [hS.cell_some]
    exact isMaxBy_congr (fun y => (setter_none hch c y _).symm) x
  | some ch =>
    have hle := chg_gen_le hV hS hpw hch
    simp only [cellStep, genStep]
    by_cases hA : ch.kind ≠ .upd ∧ st.gen r < ch.gen + 1
    · rw [ite_pos' hA, ite_pos' hA]
      -- the setters of the new incarnation: only `w`, if it is an insert setting `c`
      have hset : ∀ y, Setter M (ins S w) r c y (ch.gen + 1) ↔
          y = w ∧ ch.kind = .ins ∧ ch.sets c = true := by
        intro y
        rw [setter_some hch]
        constructor
        · rintro (⟨hy, h1, h2, h3⟩ | h)
          · refine ⟨hy, ?_, h2⟩
            unfold Change.inc at h3
            cases hk : ch.kind
            · rfl
            · exact absurd hk hA.1
            · exact absurd hk h1
          · have := setter_le hV hcl hS h; omega
        · rintro ⟨hy, h1, h2⟩
          refine Or.inl ⟨hy, by rw [h1]; decide, h2, ?_⟩
          unfold Change.inc; rw [ite_pos' h1]
      by_cases hB : ch.kind = .ins ∧ ch.sets c = true
      · rw [ite_pos' hB]
        refine ⟨fun x => ⟨fun h => ?_, fun h => ?_⟩, fun h => by cases h⟩
        · cases h
          exact ⟨(hset w).2 ⟨rfl, hB⟩, fun y hy => by rw [((hset y).1 hy).1]; exact Nat.le_refl _⟩
        · rw [((hset x).1 h.1).1]
      · rw [ite_neg' hB]
        refine ⟨fun x => ⟨fun h => (by cases h), fun h => ?_⟩, fun _ x hx => ?_⟩
        · exact absurd ((hset x).1 h.1).2 hB
        · exact hB ((hset x).1 hx).2
    · rw [ite_neg' hA, ite_neg' hA]
      by_cases hC : ch.kind ≠ .del ∧ ch.sets c = true ∧ ch.inc = st.gen r
      · rw [ite_pos' hC]
        have key := isMaxBy_insert hV.ts_inj
          (P := fun y => Setter M S r c y (st.gen r))
          (P' := fun y => Setter M (ins S w) r c y (st.gen r))
          (w := w) (old := st.cell r c)
          (fun y => by
            rw [setter_some hch]
            constructor
            · rintro (⟨h1, _⟩ | h)
              · exact Or.inl h1
              · exact Or.inr h
            · rintro (h | h)
              · exact Or.inl ⟨h, hC⟩
              · exact Or.inr h)
          (fun h => hw h.1) (fun x => hS.cell_some r c x) (hS.cell_none r c)
        refine ⟨fun x => ⟨fun h => ?_, fun h => ?_⟩, fun h => by cases h⟩
        · exact (key x).2 (Option.some.inj h).symm
        · rw [(key x).1 h]
      · rw [ite_neg' hC]
        have e : ∀ y, Setter M (ins S w) r c y (st.gen r) ↔ Setter M S r c y (st.gen r) := by
          intro y
          rw [setter_some hch]
          constructor
          · rintro (⟨_, h⟩ | h)
            · exact absurd h hC
            · exact h
          · exact Or.inr
        refine ⟨fun x => ?_, fun h x hx => hS.cell_none r c h x ((e x).1 hx)⟩
        rw [hS.cell_some]
        exact isMaxBy_congr (fun y => (e y).symm) x

end

end CovenMerge
