import CovenMerge.Rewriting
import CovenMerge.Converge

/-!
# Post-rules: rows lost after the merge

After the core merge (cells, generations), some rows must be removed: a child
whose parent is gone (cascade, restrict), a row failing a CHECK, a row
losing a unique value, an ancestor no shared child references. Each removal
can enable others. This file:

* models one removal as a step `KillStep rows fires D D'`: some live row `x`
  for which `fires D x` holds is added to the set `D` of lost rows;
* proves that if `fires` is *monotone* (losing more rows never stops a rule
  from firing), every order of removals ends in the same set
  (`kill_unique_normal`), via Newman's lemma;
* shows the spec's unique rule, judged among rows still present, is not
  monotone, and gives a concrete state with two different end results
  (`spec_unique_not_confluent`);
* defines a stratified rule set (monotone rules, then unique among the
  survivors, then monotone rules again) and proves its result is unique
  (`stratified_unique`), and that the whole merge, core plus post-rules, is
  a function of the set of writes (`end_to_end`).
-/

namespace CovenMerge

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

theorem countP_le_of {l : List Row} {p q : Row → Bool} (hpq : ∀ y, p y = true → q y = true) :
    l.countP p ≤ l.countP q := by
  induction l with
  | nil => simp
  | cons y l ih =>
    simp only [List.countP_cons]
    cases hp : p y <;> cases hq : q y <;> simp
    · omega
    · omega
    · exact absurd (hpq y hp) (by simp [hq])
    · omega

theorem countP_lt_of {l : List Row} {p q : Row → Bool} (hpq : ∀ y, p y = true → q y = true)
    {x : Row} (hx : x ∈ l) (hq : q x = true) (hp : p x = false) :
    l.countP p < l.countP q := by
  induction l with
  | nil => simp at hx
  | cons y l ih =>
    simp only [List.countP_cons]
    rcases List.mem_cons.1 hx with h | h
    · subst h
      rw [hp, hq]
      have := countP_le_of (l := l) hpq
      simp
      omega
    · have := ih h
      cases hp' : p y <;> cases hq' : q y <;> simp
      · omega
      · omega
      · exact absurd (hpq y hp') (by simp [hq'])
      · omega

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
      · rw [ite_pos' hyx] at hy; cases hy
      · rw [ite_neg' hyx] at hy; simpa using hy
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
      · rw [ite_pos' h]
      · rw [ite_neg' h]; exact hz
    have hDy' : ∀ z, D z = true → kill D y z = true := by
      intro z hz; unfold kill; by_cases h : z = y
      · rw [ite_pos' h]
      · rw [ite_neg' h]; exact hz
    have e : kill (kill D x) y = kill (kill D y) x := by
      funext z
      unfold kill
      by_cases h1 : z = x <;> by_cases h2 : z = y <;> simp [h1, h2]
    refine ⟨kill (kill D x) y, Star.single ⟨y, hy, ?_, hm D _ y hDx' hfy, rfl⟩, ?_⟩
    · unfold kill; rw [ite_neg' (Ne.symm hxy)]; exact hDy
    · rw [e]
      refine Star.single ⟨x, hx, ?_, hm D _ x hDy' hfx, rfl⟩
      unfold kill; rw [ite_neg' hxy]; exact hDx

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
    · rw [ite_neg' hyx] at hy; exact h0 y hy

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
    · rw [ite_pos' hyx]
    · rw [ite_neg' hyx]; exact hy

/-! ## The monotone rules -/

/-- What the monotone rules read from the merged state, all fixed before
they run:
* `checkFails x`: row `x`'s merged values fail a CHECK;
* `parent x`: the parent row `x`'s winning reference names, through a key
  with `ON DELETE CASCADE` or `RESTRICT` / `NO ACTION`;
* `stale x`: that reference carries a parent generation the parent has left
  (deleted, perhaps re-added) (§8.4);
* `isAncestor x`: `x` is in an ancestor table, with no place of its own;
* `refs y x`: row `y`'s winning reference through its place key names `x`. -/
structure Base (Row : Type) where
  rows : List Row
  checkFails : Row → Bool
  parent : Row → Option Row
  stale : Row → Bool
  isAncestor : Row → Bool
  refs : Row → Row → Bool

/-- A row is lost when its CHECK fails, its reference is stale, its parent is
lost, or it is an ancestor every referencing child of which is lost. -/
def monoFires (B : Base Row) (D : Row → Bool) (x : Row) : Prop :=
  B.checkFails x = true ∨ B.stale x = true ∨ (∃ p, B.parent x = some p ∧ D p = true) ∨
    (B.isAncestor x = true ∧ ∀ y, y ∈ B.rows → B.refs y x = true → D y = true)

theorem monoFires_monotone (B : Base Row) : MonotoneRules (monoFires B) := by
  intro D D' x hDD' h
  rcases h with h | h | ⟨p, hp, hD⟩ | ⟨ha, hall⟩
  · exact Or.inl h
  · exact Or.inr (Or.inl h)
  · exact Or.inr (Or.inr (Or.inl ⟨p, hp, hDD' p hD⟩))
  · exact Or.inr (Or.inr (Or.inr ⟨ha, fun y hy hr => hDD' y (hall y hy hr)⟩))

/-! ## The spec's unique rule is not monotone -/

/-- The spec's unique rule: `claims y x` says rows `y` and `x` claim one
unique value and `y`'s claim has the smaller timestamp. Row `x` is lost if
such a `y` is still present. Presence appears negated, so losing a row can
stop this rule from firing. -/
def specUniqueFires (claims : Row → Row → Bool) (D : Row → Bool) (x : Row) : Prop :=
  ∃ y, claims y x = true ∧ D y = false

end

/-! ### A concrete state with two end results

Rows: `0` is a folder, already deleted; `1` is note A in that folder,
through a cascade key; `2` is note B. A and B claim one unique title, and
A's claim has the smaller timestamp. -/

def exBase : Base Nat where
  rows := [0, 1, 2]
  checkFails _ := false
  parent x := if x = 1 then some 0 else none
  stale _ := false
  isAncestor _ := false
  refs _ _ := false

def exClaims (y x : Nat) : Bool := y == 1 && x == 2

def exFires (D : Nat → Bool) (x : Nat) : Prop :=
  monoFires exBase D x ∨ specUniqueFires exClaims D x

/-- The folder is deleted; A and B are present. -/
def exStart : Nat → Bool := fun y => y == 0

theorem ex_end1_normal : Normal (KillStep exBase.rows exFires) (kill exStart 1) := by
  rintro ⟨D', x, hx, hDx, hf, _⟩
  simp only [exBase, List.mem_cons, List.mem_nil_iff, or_false] at hx
  rcases hx with rfl | rfl | rfl
  · simp [kill, exStart] at hDx
  · simp [kill] at hDx
  · rcases hf with h | ⟨y, hy, hDy⟩
    · rcases h with h | h | ⟨p, hp, _⟩ | ⟨h, _⟩
      · simp [exBase] at h
      · simp [exBase] at h
      · simp [exBase] at hp
      · simp [exBase] at h
    · simp only [exClaims, Bool.and_eq_true, beq_iff_eq] at hy
      rw [hy.1] at hDy
      simp [kill] at hDy

theorem ex_end2_normal : Normal (KillStep exBase.rows exFires) (kill (kill exStart 2) 1) := by
  rintro ⟨D', x, hx, hDx, _, _⟩
  simp only [exBase, List.mem_cons, List.mem_nil_iff, or_false] at hx
  rcases hx with rfl | rfl | rfl <;> simp [kill, exStart] at hDx

/-- **The spec's post-rules are not confluent.** From one state, the cascade
first leaves B present; the unique rule first loses B, then the cascade
loses A. Both end states are final and they differ. -/
theorem spec_unique_not_confluent :
    Star (KillStep exBase.rows exFires) exStart (kill exStart 1) ∧
    Star (KillStep exBase.rows exFires) exStart (kill (kill exStart 2) 1) ∧
    Normal (KillStep exBase.rows exFires) (kill exStart 1) ∧
    Normal (KillStep exBase.rows exFires) (kill (kill exStart 2) 1) ∧
    kill exStart 1 ≠ kill (kill exStart 2) 1 := by
  refine ⟨Star.single ⟨1, by simp [exBase], by simp [exStart],
      Or.inl (Or.inr (Or.inr (Or.inl ⟨0, by simp [exBase], by simp [exStart]⟩))), rfl⟩,
    Star.head ⟨2, by simp [exBase], by simp [exStart],
      Or.inr ⟨1, by simp [exClaims], by simp [exStart]⟩, rfl⟩
      (Star.single ⟨1, by simp [exBase], by simp [kill, exStart],
        Or.inl (Or.inr (Or.inr (Or.inl ⟨0, by simp [exBase], by simp [kill, exStart]⟩))), rfl⟩),
    ex_end1_normal, ex_end2_normal, ?_⟩
  intro h
  have := congrFun h 2
  simp [kill, exStart] at this

/-! ## A stratified rule set with one result -/

section
variable {Row : Type} [DecidableEq Row]

/-- Rows that lose a unique value, judged once, among the rows present
after the monotone rules: some present row with a smaller timestamp claims
the same value. -/
def uniqueLosers (rows : List Row) (claims : Row → Row → Bool) (D₁ : Row → Bool) : Row → Bool :=
  fun x => D₁ x || rows.any (fun y => claims y x && !D₁ y)

/-- The stratified result `D` from the starting state `D₀` (rows absent by
their own generation): run the monotone rules to the end (`D₁`), add the
unique losers judged among `D₁`'s survivors, run the monotone rules to the
end again (`D`). -/
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

end

/-! ## End to end -/

/-- What the post-rules read from a merged state: the rows, the monotone
rules' inputs, the unique claims, and which rows are absent by their own
generation. Any function of the core state. -/
structure PostOf (Row : Type) where
  base : Base Row
  claims : Row → Row → Bool
  start : Row → Bool

/-- **The whole merge is a function of the set of writes.** Two devices that
applied the same writes in causal orders, then ran the stratified post-rules
in any order to the end, hold the same set of lost rows. -/
theorem end_to_end {W Row Col : Type} [DecidableEq W] [DecidableEq Row]
    {M : Writes W Row Col} (hV : Valid M) (post : St W Row Col → PostOf Row)
    {L₁ L₂ : List W} (h₁ : CausalOrder M L₁) (h₂ : CausalOrder M L₂)
    (hset : ∀ x, x ∈ L₁ ↔ x ∈ L₂) {D D' : Row → Bool}
    (hD : let P := post (L₁.foldl (step M) St.init)
      Stratified P.base.rows (monoFires P.base) P.claims P.start D)
    (hD' : let P := post (L₂.foldl (step M) St.init)
      Stratified P.base.rows (monoFires P.base) P.claims P.start D') : D = D' := by
  have e := merge_converges hV h₁ h₂ hset
  simp only at hD hD'
  rw [e] at hD
  exact stratified_unique (monoFires_monotone _) hD hD'

end CovenMerge
