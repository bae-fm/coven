import CovenMerge.Rewriting
import CovenMerge.ListLemmas

/-!
# Removal rules as a fixpoint

Generic results about rules that take rows out:

* a removal step takes one present row for which a rule fires;
* `kill_unique_normal`: if every rule keeps firing when more rows are
  removed (*monotone*), any order of steps that runs until no rule fires ends
  with the same set, by Newman's lemma;
* `normal_least`: that set is the smallest set containing the start that is
  closed under the rules, so it can be stated with no order at all;
* `Stratified`, `stratified_unique`: §8's three steps, with the unique rule
  judged once between two runs of the others, have exactly one result;
* `closeN`: an executable run of the rules, which `closeN_star` and
  `closeN_normal` show is one of those orders;
* `star_fires`: every row a run removes satisfies a rule in the end state;
* `lfp_local`, `stratified_local`: two devices whose rules agree on a set of
  rows closed under what those rules read end with the same removals there.
-/

namespace CovenMerge

theorem ite_pos'' {α : Type} {p : Prop} [Decidable p] {a b : α} (h : p) :
    (if p then a else b) = a := by simp [h]

theorem ite_neg'' {α : Type} {p : Prop} [Decidable p] {a b : α} (h : ¬ p) :
    (if p then a else b) = b := by simp [h]

section
variable {Row : Type} [DecidableEq Row]

/-- `D` with `x` added. -/
def kill (D : Row → Bool) (x : Row) : Row → Bool := fun y => if y = x then true else D y

/-- One removal: a rule fires for a live row in `rows`, and that row is lost. -/
def KillStep (rows : List Row) (fires : (Row → Bool) → Row → Prop) (D D' : Row → Bool) : Prop :=
  ∃ x, x ∈ rows ∧ D x = false ∧ fires D x ∧ D' = kill D x

/-- Losing more rows never stops a rule from firing for a row. -/
def MonotoneRules (fires : (Row → Bool) → Row → Prop) : Prop :=
  ∀ D D' x, (∀ y, D y = true → D' y = true) → fires D x → fires D' x

theorem killStep_terminating (rows : List Row) (fires : (Row → Bool) → Row → Prop) :
    Terminating (KillStep rows fires) := by
  apply Subrelation.wf (r := fun b a => rows.countP (fun y => !b y) < rows.countP (fun y => !a y))
  · intro a b h
    obtain ⟨x, hx, hDx, _, rfl⟩ := h
    apply countP_lt_of (x := x) _ hx
    · simp [hDx]
    · simp [kill]
    · intro y hy
      simp only [kill, Bool.not_eq_true'] at hy ⊢
      by_cases hyx : y = x
      · rw [ite_pos'' hyx] at hy; cases hy
      · rw [ite_neg'' hyx] at hy; simpa using hy
  · exact (measure (fun D : Row → Bool => rows.countP (fun y => !D y))).wf

theorem killStep_locallyConfluent {rows : List Row} {fires : (Row → Bool) → Row → Prop}
    (hm : MonotoneRules fires) : LocallyConfluent (KillStep rows fires) := by
  rintro D D₁ D₂ ⟨x, hx, hDx, hfx, rfl⟩ ⟨y, hy, hDy, hfy, rfl⟩
  by_cases hxy : x = y
  · subst hxy
    exact ⟨kill D x, Star.refl _, Star.refl _⟩
  · have sub : ∀ z, ∀ E : Row → Bool, D z = true → kill E z = kill E z := fun _ _ _ => rfl
    have hDx' : ∀ z, D z = true → kill D x z = true := by
      intro z hz; unfold kill; by_cases h : z = x
      · rw [ite_pos'' h]
      · rw [ite_neg'' h]; exact hz
    have hDy' : ∀ z, D z = true → kill D y z = true := by
      intro z hz; unfold kill; by_cases h : z = y
      · rw [ite_pos'' h]
      · rw [ite_neg'' h]; exact hz
    have e : kill (kill D x) y = kill (kill D y) x := by
      funext z
      unfold kill
      by_cases h1 : z = x <;> by_cases h2 : z = y <;> simp [h1, h2]
    refine ⟨kill (kill D x) y, Star.single ⟨y, hy, ?_, hm D _ y hDx' hfy, rfl⟩, ?_⟩
    · unfold kill; rw [ite_neg'' (Ne.symm hxy)]; exact hDy
    · rw [e]
      refine Star.single ⟨x, hx, ?_, hm D _ x hDy' hfx, rfl⟩
      unfold kill; rw [ite_neg'' hxy]; exact hDx

/-- **Monotone post-rules have one result.** From the same starting state,
any two orders of removal that run until no rule fires end in the same set
of lost rows. -/
theorem kill_unique_normal {rows : List Row} {fires : (Row → Bool) → Row → Prop}
    (hm : MonotoneRules fires) {D D₁ D₂ : Row → Bool}
    (h₁ : Star (KillStep rows fires) D D₁) (h₂ : Star (KillStep rows fires) D D₂)
    (n₁ : Normal (KillStep rows fires) D₁) (n₂ : Normal (KillStep rows fires) D₂) : D₁ = D₂ :=
  unique_normal (newman (killStep_terminating rows fires) (killStep_locallyConfluent hm)) h₁ h₂ n₁ n₂

theorem kill_exists_normal (rows : List Row) (fires : (Row → Bool) → Row → Prop) (D : Row → Bool) :
    ∃ D', Star (KillStep rows fires) D D' ∧ Normal (KillStep rows fires) D' :=
  exists_normal (killStep_terminating rows fires) D

/-- A set of lost rows is closed when every rule that fires for a row in
`rows` names a row already in it. -/
def ClosedUnder (rows : List Row) (fires : (Row → Bool) → Row → Prop) (D : Row → Bool) : Prop :=
  ∀ x, x ∈ rows → fires D x → D x = true

theorem normal_closed {rows : List Row} {fires : (Row → Bool) → Row → Prop} {D : Row → Bool}
    (hn : Normal (KillStep rows fires) D) : ClosedUnder rows fires D := by
  intro x hx hf
  cases h : D x
  · exact absurd ⟨kill D x, x, hx, h, hf, rfl⟩ hn
  · rfl

theorem star_sub_closed {rows : List Row} {fires : (Row → Bool) → Row → Prop}
    (hm : MonotoneRules fires) {D₀ D D' : Row → Bool} (h : Star (KillStep rows fires) D₀ D)
    (hc : ClosedUnder rows fires D') : (∀ y, D₀ y = true → D' y = true) →
    ∀ y, D y = true → D' y = true := by
  induction h with
  | refl => exact id
  | head hab _ ih =>
    intro h0
    apply ih
    obtain ⟨x, hx, _, hf, rfl⟩ := hab
    intro y hy
    unfold kill at hy
    by_cases hyx : y = x
    · rw [hyx]; exact hc x hx (hm _ _ x h0 hf)
    · rw [ite_neg'' hyx] at hy; exact h0 y hy

/-- **The end result is the least fixpoint.** For monotone rules, the set
reached by running the rules to the end is the smallest set containing the
start that is closed under the rules. So it can be stated without any order
of rule application. -/
theorem normal_least {rows : List Row} {fires : (Row → Bool) → Row → Prop}
    (hm : MonotoneRules fires) {D₀ D : Row → Bool} (h : Star (KillStep rows fires) D₀ D)
    (hn : Normal (KillStep rows fires) D) :
    ClosedUnder rows fires D ∧ (∀ y, D₀ y = true → D y = true) ∧
    ∀ D', ClosedUnder rows fires D' → (∀ y, D₀ y = true → D' y = true) →
      ∀ y, D y = true → D' y = true := by
  refine ⟨normal_closed hn, ?_, fun D' hc h0 => star_sub_closed hm h hc h0⟩
  clear hn
  induction h with
  | refl => exact fun _ => id
  | head hab _ ih =>
    intro y hy
    apply ih
    obtain ⟨x, _, _, _, rfl⟩ := hab
    unfold kill
    by_cases hyx : y = x
    · rw [ite_pos'' hyx]
    · rw [ite_neg'' hyx]; exact hy

/-- Rows that lose a unique value, judged once against `D₁`: row `x` loses
when some row still present in `D₁` claims the same value before it. -/
def uniqueLosers (rows : List Row) (claims : Row → Row → Bool) (D₁ : Row → Bool) : Row → Bool :=
  fun x => D₁ x || rows.any (fun y => claims y x && !D₁ y)

/-- The result `D` of §8's three steps from the starting set `D₀`: apply the
other rules until none fires (`D₁`), add the unique losers judged among
`D₁`'s survivors, apply the other rules again until none fires (`D`). -/
def Stratified (rows : List Row) (fires : (Row → Bool) → Row → Prop)
    (claims : Row → Row → Bool) (D₀ D : Row → Bool) : Prop :=
  ∃ D₁, Star (KillStep rows fires) D₀ D₁ ∧ Normal (KillStep rows fires) D₁ ∧
    Star (KillStep rows fires) (uniqueLosers rows claims D₁) D ∧ Normal (KillStep rows fires) D

theorem stratified_unique {rows : List Row} {fires : (Row → Bool) → Row → Prop}
    (hm : MonotoneRules fires) {claims : Row → Row → Bool} {D₀ D D' : Row → Bool}
    (h : Stratified rows fires claims D₀ D) (h' : Stratified rows fires claims D₀ D') : D = D' := by
  obtain ⟨D₁, a₁, b₁, c₁, d₁⟩ := h
  obtain ⟨D₁', a₁', b₁', c₁', d₁'⟩ := h'
  have e : D₁ = D₁' := kill_unique_normal hm a₁ a₁' b₁ b₁'
  subst e
  exact kill_unique_normal hm c₁ c₁' d₁ d₁'

theorem stratified_exists (rows : List Row) (fires : (Row → Bool) → Row → Prop)
    (claims : Row → Row → Bool) (D₀ : Row → Bool) : ∃ D, Stratified rows fires claims D₀ D := by
  obtain ⟨D₁, a, b⟩ := kill_exists_normal rows fires D₀
  obtain ⟨D, c, d⟩ := kill_exists_normal rows fires (uniqueLosers rows claims D₁)
  exact ⟨D, D₁, a, b, c, d⟩


/-! ## An executable run of the rules -/

/-- Rules given as a Boolean test, read as a proposition. -/
abbrev FiresP (fires : (Row → Bool) → Row → Bool) : (Row → Bool) → Row → Prop :=
  fun D x => fires D x = true

/-- The first row of `rows` still present for which a rule fires. -/
def pick (rows : List Row) (fires : (Row → Bool) → Row → Bool) (D : Row → Bool) : Option Row :=
  rows.find? (fun x => !D x && fires D x)

/-- Remove rows one at a time, for at most `n` steps. -/
def closeN (rows : List Row) (fires : (Row → Bool) → Row → Bool) : Nat → (Row → Bool) → (Row → Bool)
  | 0, D => D
  | n + 1, D =>
    match pick rows fires D with
    | some x => closeN rows fires n (kill D x)
    | none => D

/-- Run the rules until none fires. -/
def close (rows : List Row) (fires : (Row → Bool) → Row → Bool) (D : Row → Bool) : Row → Bool :=
  closeN rows fires rows.length D

theorem pick_step {rows : List Row} {fires : (Row → Bool) → Row → Bool} {D : Row → Bool} {x : Row}
    (h : pick rows fires D = some x) : KillStep rows (FiresP fires) D (kill D x) := by
  have hp := List.find?_some h
  have hm := List.mem_of_find?_eq_some h
  simp only [Bool.and_eq_true, Bool.not_eq_true'] at hp
  exact ⟨x, hm, hp.1, hp.2, rfl⟩

theorem pick_none_normal {rows : List Row} {fires : (Row → Bool) → Row → Bool} {D : Row → Bool}
    (h : pick rows fires D = none) : Normal (KillStep rows (FiresP fires)) D := by
  rintro ⟨D', x, hx, hDx, hf, _⟩
  have := List.find?_eq_none.1 h x hx
  simp only [hDx, Bool.not_false, Bool.true_and] at this
  exact this hf

theorem closeN_star (rows : List Row) (fires : (Row → Bool) → Row → Bool) :
    ∀ n D, Star (KillStep rows (FiresP fires)) D (closeN rows fires n D) := by
  intro n
  induction n with
  | zero => intro D; exact Star.refl D
  | succ n ih =>
    intro D
    simp only [closeN]
    cases h : pick rows fires D with
    | none => exact Star.refl D
    | some x => exact Star.head (pick_step h) (ih (kill D x))

theorem count_kill_lt {rows : List Row} {D : Row → Bool} {x : Row} (hx : x ∈ rows)
    (hDx : D x = false) :
    rows.countP (fun y => !kill D x y) < rows.countP (fun y => !D y) := by
  apply countP_lt_of _ hx (by simp [hDx]) (by simp [kill])
  intro y hy
  simp only [kill, Bool.not_eq_true'] at hy ⊢
  by_cases hyx : y = x
  · rw [ite_pos'' hyx] at hy; cases hy
  · rw [ite_neg'' hyx] at hy; simpa using hy

theorem closeN_normal (rows : List Row) (fires : (Row → Bool) → Row → Bool) :
    ∀ n D, rows.countP (fun y => !D y) ≤ n →
      Normal (KillStep rows (FiresP fires)) (closeN rows fires n D) := by
  intro n
  induction n with
  | zero =>
    intro D hn
    rintro ⟨D', x, hx, hDx, _, _⟩
    have hDx' : D x = false := hDx
    have := countP_pos_of (p := fun y => !D y) hx (by simp [hDx'])
    omega
  | succ n ih =>
    intro D hn
    simp only [closeN]
    cases h : pick rows fires D with
    | none => exact pick_none_normal h
    | some x =>
      have hp := List.find?_some h
      have hm := List.mem_of_find?_eq_some h
      simp only [Bool.and_eq_true, Bool.not_eq_true'] at hp
      have := count_kill_lt hm hp.1
      exact ih (kill D x) (by omega)

theorem close_star (rows : List Row) (fires : (Row → Bool) → Row → Bool) (D : Row → Bool) :
    Star (KillStep rows (FiresP fires)) D (close rows fires D) :=
  closeN_star rows fires _ D

theorem close_normal (rows : List Row) (fires : (Row → Bool) → Row → Bool) (D : Row → Bool) :
    Normal (KillStep rows (FiresP fires)) (close rows fires D) :=
  closeN_normal rows fires _ D List.countP_le_length

/-! ## Every removed row has a reason in the end state -/

theorem star_grow {rows : List Row} {fires : (Row → Bool) → Row → Prop} {D D' : Row → Bool}
    (h : Star (KillStep rows fires) D D') : ∀ y, D y = true → D' y = true := by
  induction h with
  | refl => exact fun _ => id
  | head hab _ ih =>
    intro y hy
    apply ih
    obtain ⟨x, _, _, _, rfl⟩ := hab
    unfold kill
    by_cases hyx : y = x
    · rw [ite_pos'' hyx]
    · rw [ite_neg'' hyx]; exact hy

/-- A row a run removed satisfies some rule in the state the run ends in. -/
theorem star_fires {rows : List Row} {fires : (Row → Bool) → Row → Prop}
    (hm : MonotoneRules fires) {D₀ D : Row → Bool} (h : Star (KillStep rows fires) D₀ D) :
    ∀ x, D x = true → D₀ x = false → fires D x := by
  induction h with
  | refl => intro x h1 h2; rw [h1] at h2; cases h2
  | @head a b c hab hbc ih =>
    intro x hx h0
    obtain ⟨y, _, _, hf, rfl⟩ := hab
    by_cases hxy : x = y
    · subst hxy
      exact hm a c x (star_grow (Star.head ⟨x, by assumption, by assumption, hf, rfl⟩ hbc)) hf
    · apply ih x hx
      unfold kill; rw [ite_neg'' hxy]; exact h0

/-! ## Locality: rules that agree on a closed set of rows -/

/-- Two rule sets agree on the rows `R`, and for those rows read only which
rows of `R` are removed. -/
def AgreeOn (R : Row → Bool) (F G : (Row → Bool) → Row → Prop) : Prop :=
  ∀ D D' x, R x = true → (∀ y, R y = true → D y = D' y) → (F D x ↔ G D' x)

theorem lfp_sub_local {R : Row → Bool} {F G : (Row → Bool) → Row → Prop}
    (hmF : MonotoneRules F) (hA : AgreeOn R F G) {rowsF rowsG : List Row}
    (hrows : ∀ x, R x = true → x ∈ rowsF → x ∈ rowsG)
    {D₀ E₀ D E : Row → Bool} (h0 : ∀ x, R x = true → D₀ x = true → E₀ x = true)
    (hD : Star (KillStep rowsF F) D₀ D) (nD : Normal (KillStep rowsF F) D)
    (hE : Star (KillStep rowsG G) E₀ E) (nE : Normal (KillStep rowsG G) E) :
    ∀ x, R x = true → D x = true → E x = true := by
  let E' : Row → Bool := fun y => if R y = true then E y else true
  have hcl : ClosedUnder rowsF F E' := by
    intro x hx hf
    by_cases hR : R x = true
    · have hG : G E x := (hA E' E x hR (fun y hy => by simp [E', hy])).1 hf
      have := normal_closed nE x (hrows x hR hx) hG
      simp [E', hR, this]
    · simp [E', hR]
  have h0' : ∀ y, D₀ y = true → E' y = true := by
    intro y hy
    by_cases hR : R y = true
    · simp only [E', hR, ite_true]; exact star_grow hE y (h0 y hR hy)
    · simp [E', hR]
  have := (normal_least hmF hD nD).2.2 E' hcl h0'
  intro x hR hx
  have := this x hx
  simpa [E', hR] using this

/-- **Locality.** Two devices whose rules agree on a set of rows `R`, closed
under what those rules read, and whose starting removals agree on `R`, end
with the same removals on `R`. -/
theorem lfp_local {R : Row → Bool} {F G : (Row → Bool) → Row → Prop}
    (hmF : MonotoneRules F) (hmG : MonotoneRules G) (hA : AgreeOn R F G)
    {rowsF rowsG : List Row} (hrows : ∀ x, R x = true → (x ∈ rowsF ↔ x ∈ rowsG))
    {D₀ E₀ D E : Row → Bool} (h0 : ∀ x, R x = true → D₀ x = E₀ x)
    (hD : Star (KillStep rowsF F) D₀ D) (nD : Normal (KillStep rowsF F) D)
    (hE : Star (KillStep rowsG G) E₀ E) (nE : Normal (KillStep rowsG G) E) :
    ∀ x, R x = true → D x = E x := by
  have hA' : AgreeOn R G F := fun D D' x hx hag => (hA D' D x hx (fun y hy => (hag y hy).symm)).symm
  have a := lfp_sub_local hmF hA (fun x hx => (hrows x hx).1)
    (fun x hx h => by rw [← h0 x hx]; exact h) hD nD hE nE
  have b := lfp_sub_local hmG hA' (fun x hx => (hrows x hx).2)
    (fun x hx h => by rw [h0 x hx]; exact h) hE nE hD nD
  intro x hx
  cases h : D x
  · cases h' : E x
    · rfl
    · have := b x hx h'; rw [h] at this; cases this
  · exact (a x hx h).symm

/-- **Locality of §8's three steps.** As `lfp_local`, when the unique
judgment of each row in `R` also reads only rows of `R`. -/
theorem stratified_local {R : Row → Bool} {F G : (Row → Bool) → Row → Prop}
    (hmF : MonotoneRules F) (hmG : MonotoneRules G) (hA : AgreeOn R F G)
    {rowsF rowsG : List Row} (hrows : ∀ x, R x = true → (x ∈ rowsF ↔ x ∈ rowsG))
    {claimsF claimsG : Row → Row → Bool}
    (hU : ∀ (D D' : Row → Bool) x, R x = true → (∀ y, R y = true → D y = D' y) →
      rowsF.any (fun y => claimsF y x && !D y) = rowsG.any (fun y => claimsG y x && !D' y))
    {D₀ E₀ D E : Row → Bool} (h0 : ∀ x, R x = true → D₀ x = E₀ x)
    (hD : Stratified rowsF F claimsF D₀ D) (hE : Stratified rowsG G claimsG E₀ E) :
    ∀ x, R x = true → D x = E x := by
  obtain ⟨D₁, a, b, c, d⟩ := hD
  obtain ⟨E₁, a', b', c', d'⟩ := hE
  have h1 := lfp_local hmF hmG hA hrows h0 a b a' b'
  have h2 : ∀ x, R x = true → uniqueLosers rowsF claimsF D₁ x = uniqueLosers rowsG claimsG E₁ x := by
    intro x hx
    simp only [uniqueLosers]
    rw [h1 x hx, hU D₁ E₁ x hx h1]
  exact lfp_local hmF hmG hA hrows h2 c d c' d'

end

end CovenMerge
