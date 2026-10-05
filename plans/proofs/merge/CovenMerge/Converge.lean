import CovenMerge.StepLost

/-!
# Convergence of the core merge

* `step_spec`: one arriving write takes the result for `S` to the result for
  `S ∪ {w}`.
* `isSpec_unique`: the result for a set is unique; a state is determined by
  the set of writes alone.
* `foldl_isSpec`: applying writes in a causal order, from the empty database
  or from a snapshot, lands in the result for the set applied.
* `merge_converges`: two causal orders of the same set of writes give the
  same state.
* `snapshot_converges`: loading a snapshot and applying the writes after it
  gives the same state as applying everything from scratch (§15).
-/

namespace CovenMerge

section
variable {W Row Col : Type} [DecidableEq W] {M : Writes W Row Col}

theorem step_spec (hV : Valid M) {S : W → Prop} (hcl : Closed M S) {st : St W Row Col}
    (hS : IsSpec M S st) {w : W} (hw : ¬ S w) (hpw : ∀ a, M.past w a = true → S a) :
    IsSpec M (ins S w) (step M st w) where
  gen := step_gen hV hS hpw
  gw_some r n x := (step_gw hV hS hw r n).1 x
  gw_none r n := (step_gw hV hS hw r n).2
  cell_some r c x := (step_cell hV hS hw hpw hcl r c).1 x
  cell_none r c := (step_cell hV hS hw hpw hcl r c).2
  lost r c a k x := step_lost hV hcl hS hw r c a k x

theorem closed_ins {S : W → Prop} (hcl : Closed M S) {w : W}
    (hpw : ∀ a, M.past w a = true → S a) : Closed M (ins S w) := by
  intro x hx a ha
  rcases hx with hx | hx
  · rw [hx] at ha; exact Or.inr (hpw a ha)
  · exact Or.inr (hcl x hx a ha)

theorem isSpec_congr {S₁ S₂ : W → Prop} (h : ∀ x, S₁ x ↔ S₂ x) {st : St W Row Col}
    (hS : IsSpec M S₁ st) : IsSpec M S₂ st := by
  have : S₁ = S₂ := funext fun x => propext (h x)
  subst this
  exact hS

theorem closed_congr {S₁ S₂ : W → Prop} (h : ∀ x, S₁ x ↔ S₂ x) (hS : Closed M S₁) :
    Closed M S₂ := by
  have : S₁ = S₂ := funext fun x => propext (h x)
  subst this
  exact hS

theorem isSpec_init : IsSpec M (fun _ => False) (St.init : St W Row Col) where
  gen r := ⟨fun _ _ h => h.1.elim, fun n h1 h2 => by simp [St.init] at h2; omega⟩
  gw_some r n x := ⟨fun h => (by cases h), fun h => h.1.1.elim⟩
  gw_none r n _ x h := h.1
  cell_some r c x := ⟨fun h => (by cases h), fun h => h.1.1.elim⟩
  cell_none r c _ x h := h.1
  lost r c a k x := ⟨fun h => (by cases h), fun h => h.1.1.elim⟩

theorem closed_empty : Closed M (fun _ => False) := fun _ h => h.elim

/-- Applying, in a causal order, the writes after a snapshot of the closed set
`C` lands in the result for `C` plus those writes. -/
theorem foldl_isSpec (hV : Valid M) {C : W → Prop} (hC : Closed M C) {st : St W Row Col}
    (hst : IsSpec M C st) {L : List W} (hL : CausalFrom M C L) :
    IsSpec M (fun x => C x ∨ x ∈ L) (L.foldl (step M) st) ∧
      Closed M (fun x => C x ∨ x ∈ L) := by
  induction hL with
  | nil =>
    simp only [List.foldl_nil]
    have e : ∀ x, C x ↔ C x ∨ x ∈ ([] : List W) := fun x => by simp
    exact ⟨isSpec_congr e hst, closed_congr e hC⟩
  | @snoc L w _ hCw hwL hpw ih =>
    rw [List.foldl_append]
    simp only [List.foldl_cons, List.foldl_nil]
    obtain ⟨ih1, ih2⟩ := ih
    have hw : ¬ (C w ∨ w ∈ L) := fun h => h.elim hCw hwL
    have e : ∀ x, ins (fun x => C x ∨ x ∈ L) w x ↔ C x ∨ x ∈ L ++ [w] := by
      intro x
      simp only [ins, List.mem_append, List.mem_singleton]
      constructor
      · rintro (h | h | h)
        · exact Or.inr (Or.inr h)
        · exact Or.inl h
        · exact Or.inr (Or.inl h)
      · rintro (h | h | h)
        · exact Or.inr (Or.inl h)
        · exact Or.inr (Or.inr h)
        · exact Or.inl h
    exact ⟨isSpec_congr e (step_spec hV ih2 ih1 hw hpw),
      closed_congr e (closed_ins ih2 hpw)⟩

theorem causalFrom_of_causalOrder {L : List W} (h : CausalOrder M L) :
    CausalFrom M (fun _ => False) L := by
  induction h with
  | nil => exact CausalFrom.nil
  | snoc _ hwL hpw ih =>
    exact CausalFrom.snoc ih (fun h => h) hwL (fun a ha => Or.inr (hpw a ha))

/-- The result of applying a causal order from the empty database is the
declarative result for the set of writes applied. -/
theorem run_isSpec (hV : Valid M) {L : List W} (h : CausalOrder M L) :
    IsSpec M (fun x => x ∈ L) (L.foldl (step M) St.init) := by
  have := (foldl_isSpec hV closed_empty isSpec_init (causalFrom_of_causalOrder h)).1
  exact isSpec_congr (fun x => by simp) this

/-! ### The result for a set is unique -/

theorem isGen_unique {S : W → Prop} {r : Row} {G₁ G₂ : Nat}
    (h₁ : IsGen M S r G₁) (h₂ : IsGen M S r G₂) : G₁ = G₂ := by
  have a : G₁ ≤ G₂ := by
    by_cases h0 : G₁ = 0
    · omega
    · obtain ⟨x, hx⟩ := h₁.2 G₁ (by omega) (Nat.le_refl _)
      exact h₂.1 x G₁ hx
  have b : G₂ ≤ G₁ := by
    by_cases h0 : G₂ = 0
    · omega
    · obtain ⟨x, hx⟩ := h₂.2 G₂ (by omega) (Nat.le_refl _)
      exact h₁.1 x G₂ hx
  omega

theorem option_eq_of_iff {α : Type} {o₁ o₂ : Option α} (h : ∀ x, o₁ = some x ↔ o₂ = some x) :
    o₁ = o₂ := by
  cases o₁ with
  | none =>
    cases o₂ with
    | none => rfl
    | some y => exact absurd ((h y).2 rfl) (by simp)
  | some y => exact ((h y).1 rfl).symm

/-- Two states that are both the result for the same set are equal. -/
theorem isSpec_unique {S : W → Prop} {st₁ st₂ : St W Row Col}
    (h₁ : IsSpec M S st₁) (h₂ : IsSpec M S st₂) : st₁ = st₂ := by
  have hg : st₁.gen = st₂.gen := funext fun r => isGen_unique (h₁.gen r) (h₂.gen r)
  have hgw : st₁.genWrite = st₂.genWrite := funext fun r => funext fun n =>
    option_eq_of_iff fun x => (h₁.gw_some r n x).trans (h₂.gw_some r n x).symm
  have hc : st₁.cell = st₂.cell := funext fun r => funext fun c =>
    option_eq_of_iff fun x => by
      rw [h₁.cell_some, h₂.cell_some, hg]
  have hl : st₁.lost = st₂.lost := funext fun r => funext fun c => funext fun a =>
    option_eq_of_iff fun p => by
      obtain ⟨k, x⟩ := p
      exact (h₁.lost r c a k x).trans (h₂.lost r c a k x).symm
  cases st₁
  cases st₂
  simp only at hg hgw hc hl
  rw [hg, hgw, hc, hl]

/-! ### Convergence -/

/-- **Convergence.** Two devices that applied the same set of writes, each in
an order that respects causality, hold the same state: the same row
generations, generation records, cell values, and `coven_lost` rows. -/
theorem merge_converges (hV : Valid M) {L₁ L₂ : List W}
    (h₁ : CausalOrder M L₁) (h₂ : CausalOrder M L₂) (hset : ∀ x, x ∈ L₁ ↔ x ∈ L₂) :
    L₁.foldl (step M) St.init = L₂.foldl (step M) (St.init : St W Row Col) :=
  isSpec_unique (run_isSpec hV h₁) (isSpec_congr (fun x => (hset x).symm) (run_isSpec hV h₂))

/-- **Snapshots (§15).** Device A applied `L₀` and wrote a snapshot of its
state. Device B loads that snapshot and applies `L₁`, in an order where the
snapshot's writes count as applied. Device C applied `L₂` from scratch. If B
and C applied the same set of writes, they hold the same state. -/
theorem snapshot_converges (hV : Valid M) {L₀ L₁ L₂ : List W}
    (h₀ : CausalOrder M L₀) (h₁ : CausalFrom M (fun x => x ∈ L₀) L₁) (h₂ : CausalOrder M L₂)
    (hset : ∀ x, (x ∈ L₀ ∨ x ∈ L₁) ↔ x ∈ L₂) :
    L₁.foldl (step M) (L₀.foldl (step M) St.init) = L₂.foldl (step M) (St.init : St W Row Col) := by
  have hC : Closed M (fun x => x ∈ L₀) := by
    have := (foldl_isSpec hV closed_empty isSpec_init (causalFrom_of_causalOrder h₀)).2
    exact closed_congr (fun x => by simp) this
  have hB := (foldl_isSpec hV hC (run_isSpec hV h₀) h₁).1
  exact isSpec_unique hB (isSpec_congr (fun x => (hset x).symm) (run_isSpec hV h₂))

/-- Applying writes in timestamp order is one causal order (a write is
stamped after everything it had read), so every causal order gives the
result of applying them in timestamp order. Stated here for any list whose
timestamps increase. -/
theorem causalOrder_of_ts_sorted (hV : Valid M) {L : List W}
    (hclosed : ∀ x, x ∈ L → ∀ a, M.past x a = true → a ∈ L)
    (hnd : L.Nodup) (hsorted : L.Pairwise (fun a b => M.ts a < M.ts b)) :
    CausalOrder M L := by
  obtain ⟨R, rfl⟩ : ∃ R : List W, L = R.reverse := ⟨L.reverse, by simp⟩
  induction R with
  | nil => exact CausalOrder.nil
  | cons w R ih =>
    rw [List.reverse_cons] at hclosed hnd hsorted ⊢
    have hnd' := hnd
    rw [List.nodup_append] at hnd'
    have hs' := hsorted
    rw [List.pairwise_append] at hs'
    have hwL : w ∉ R.reverse := fun h => by
      have := hnd'.2.2 w h w (List.mem_singleton_self w)
      exact this rfl
    refine CausalOrder.snoc (ih ?_ hnd'.1 hs'.1) hwL ?_
    · intro x hx a ha
      have hx' : x ∈ R.reverse ++ [w] := List.mem_append_left _ hx
      have := hclosed x hx' a ha
      rcases List.mem_append.1 this with h | h
      · exact h
      · rw [List.mem_singleton] at h
        subst h
        have h1 := hs'.2.2 x hx a (List.mem_singleton_self a)
        have h2 := hV.past_ts x a ha
        omega
    · intro a ha
      have := hclosed w (List.mem_append_right _ (List.mem_singleton_self w)) a ha
      rcases List.mem_append.1 this with h | h
      · exact h
      · rw [List.mem_singleton] at h
        subst h
        have := hV.past_ts a a ha
        omega

end

end CovenMerge
