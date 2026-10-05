import CovenMerge.Fixpoint
import CovenMerge.Converge

/-!
# §8's removal rules

What the rules read from the merged state is collected in `Inputs`: which
rows are present, each row's references, whether its merged values fail a
CHECK, ancestors and the rows that keep them, and unique claims. `Inputs` is
computed from the merged state; nothing in it depends on which rows the rules
remove.

* `fires`: the rules other than the unique one: foreign keys, CHECK, and
  ancestors (§8, §8.4, §8.6, §14).
* `fires_monotone`: each keeps firing when more rows are removed.
* `removal`: §8's three steps, run executably; `removal_stratified` shows
  it is one of the orders `Stratified` allows, so by `any_order_removal`
  every order of applying the rules gives it.
* `view`: what the app sees: rows shown, rows removed, and the rules
  recorded for each removed row.
* `removed_has_rule`: every removed row has a rule recorded.
* `device_converges`: two devices that applied the same writes in causal
  orders hold the same merged state, the same view, and the same `coven_lost`
  rows for removed rows.
* `unique_with_others`: run as one more rule among the others, the unique
  rule can end two ways from one state; the reason it is judged once.
-/

namespace CovenMerge

/-- One reference, as the merged state has it.
* `parent`: the row it points at;
* `stale`: the parent's generation it carries was deleted since (§8.4).

A reference under set null or set default whose parent's generation was
deleted holds null or the default in the merged state (§8.4), so it is no
reference here; where SQLite would refuse that value, it stays here, stale,
and is taken out as under restrict. -/
structure Ref (Row : Type) where
  parent : Row
  stale : Bool

/-- One unique claim: the constraint, the value claimed, and the claim's
stamp. A key present in two audiences is a claim too, with the store's
claim first (§14.2). -/
structure Claim (K : Type) where
  con : Nat
  key : K
  ts : Nat

/-- What the removal rules read, computed from the merged state.
* `rows`: the synced rows the device has;
* `present`: the row's generation is odd;
* `refs`: its references;
* `checkFails`: its merged values fail a CHECK (§8.6);
* `isAncestor`: it is in an ancestor table (§14.1);
* `keepRef`: the reference through which it keeps an ancestor, if its table
  keeps one;
* `sharedBase`: its audience is the store or a circle, when that doesn't
  come from an ancestor;
* `audienceFrom`: the ancestor its audience comes from, if it does;
* `claims`: its unique claims (§8.5, §14.2);
* `rank`: its primary key's order, which breaks ties between claims. -/
structure Inputs (Row K : Type) where
  rows : List Row
  present : Row → Bool
  refs : Row → List (Ref Row)
  checkFails : Row → Bool
  isAncestor : Row → Bool
  keepRef : Row → Option (Ref Row)
  sharedBase : Row → Bool
  audienceFrom : Row → Option Row
  claims : Row → List (Claim K)
  rank : Row → Nat

section
variable {Row K : Type} [DecidableEq Row] [DecidableEq K] (I : Inputs Row K)

/-- Foreign keys: a reference whose parent's generation was deleted since,
under cascade, restrict or no action, or whose parent is absent or taken
out, under every action (§8, §8.4). -/
def fkFires (D : Row → Bool) (x : Row) : Bool :=
  (I.refs x).any (fun r => r.stale || D r.parent)

/-- The row is shared: its audience is the store or a circle. A row whose
audience comes from an ancestor is shared while that ancestor is present. -/
def sharedGiven (D : Row → Bool) (y : Row) : Bool :=
  match I.audienceFrom y with
  | none => I.sharedBase y
  | some a => I.present a && !D a

/-- Row `y` keeps ancestor `x`: it is present, not removed, shared, and its
keeping reference points at `x`. -/
def keeps (D : Row → Bool) (y x : Row) : Bool :=
  match I.keepRef y with
  | some r => decide (r.parent = x) && !r.stale && I.present y && !D y && sharedGiven I D y
  | none => false

/-- Ancestors: no shared row the device has keeps it (§14). -/
def ancestorFires (D : Row → Bool) (x : Row) : Bool :=
  I.isAncestor x && !(I.rows.any (fun y => keeps I D y x))

/-- Every rule but the unique one. -/
def fires (D : Row → Bool) (x : Row) : Bool :=
  I.checkFails x || fkFires I D x || ancestorFires I D x

/-- Row `y`'s claim to a value comes before row `x`'s: same constraint, same
value, smaller stamp, or equal stamps and smaller key. -/
def rivalBefore (y x : Row) : Bool :=
  decide (y ≠ x) && (I.claims x).any (fun c => (I.claims y).any (fun c' =>
    decide (c'.con = c.con) && decide (c'.key = c.key) &&
      (decide (c'.ts < c.ts) || (decide (c'.ts = c.ts) && decide (I.rank y < I.rank x)))))

/-- Rows absent by their own generation. In `D`, absent and removed rows
alike are "out". -/
def start : Row → Bool := fun x => !I.present x

/-- §8's step 1: the other rules until none fires. -/
def pass1 : Row → Bool := close I.rows (fires I) (start I)

/-- §8's steps 2 and 3: unique values judged among `pass1`'s survivors, then
the other rules again. The result: every absent or removed row. -/
def removal : Row → Bool := close I.rows (fires I) (uniqueLosers I.rows (rivalBefore I) (pass1 I))

/-! ### Monotonicity -/

theorem sharedGiven_anti {D D' : Row → Bool} (h : ∀ y, D y = true → D' y = true) {y : Row}
    (hs : sharedGiven I D' y = true) : sharedGiven I D y = true := by
  unfold sharedGiven at *
  cases ha : I.audienceFrom y with
  | none => rw [ha] at hs; exact hs
  | some a =>
    rw [ha] at hs
    simp only [Bool.and_eq_true, Bool.not_eq_true'] at hs ⊢
    refine ⟨hs.1, ?_⟩
    cases hD : D a
    · rfl
    · have := h a hD; rw [hs.2] at this; cases this

theorem keeps_anti {D D' : Row → Bool} (h : ∀ y, D y = true → D' y = true) {y x : Row}
    (hk : keeps I D' y x = true) : keeps I D y x = true := by
  unfold keeps at *
  cases hr : I.keepRef y with
  | none => rw [hr] at hk; exact hk
  | some r =>
    rw [hr] at hk
    simp only [Bool.and_eq_true, Bool.not_eq_true'] at hk ⊢
    obtain ⟨⟨⟨⟨h1, h2⟩, h3⟩, h4⟩, h5⟩ := hk
    refine ⟨⟨⟨⟨h1, h2⟩, h3⟩, ?_⟩, sharedGiven_anti I h h5⟩
    cases hD : D y
    · rfl
    · have := h y hD; rw [h4] at this; cases this

/-- **Every rule but the unique one keeps firing when more rows are
removed.** -/
theorem fires_monotone : MonotoneRules (FiresP (fires I)) := by
  intro D D' x h hf
  unfold FiresP fires at *
  simp only [Bool.or_eq_true] at hf ⊢
  rcases hf with (h1 | h1) | h1
  · exact Or.inl (Or.inl h1)
  · refine Or.inl (Or.inr ?_)
    unfold fkFires at *
    simp only [List.any_eq_true, Bool.or_eq_true] at h1 ⊢
    obtain ⟨r, hr, hg⟩ := h1
    exact ⟨r, hr, hg.elim Or.inl (fun hd => Or.inr (h _ hd))⟩
  · refine Or.inr ?_
    unfold ancestorFires at *
    simp only [Bool.and_eq_true, Bool.not_eq_true', List.any_eq_false] at h1 ⊢
    refine ⟨h1.1, fun y hy hk => ?_⟩
    exact h1.2 y hy (by simpa using keeps_anti I h (by simpa using hk))

/-! ### The result -/

theorem removal_stratified :
    Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) (removal I) :=
  ⟨pass1 I, close_star _ _ _, close_normal _ _ _, close_star _ _ _, close_normal _ _ _⟩

/-- **Any order of applying the rules gives `removal`.** -/
theorem any_order_removal {D : Row → Bool}
    (h : Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) D) : D = removal I :=
  stratified_unique (fires_monotone I) h (removal_stratified I)

/-- A rule a removed row's `coven_lost` row names (§8, §20.4). -/
inductive Rule where
  | foreignKey
  | check
  | unique
  | ancestor
  deriving DecidableEq, Repr

/-- The rules for a removed row: every rule that holds for it once the rules
have run, and `unique` if it lost a unique value in step 2. -/
def rulesOf (x : Row) : List Rule :=
  let D := removal I
  (if fkFires I D x then [Rule.foreignKey] else []) ++
  (if I.checkFails x then [Rule.check] else []) ++
  (if !pass1 I x && I.rows.any (fun y => rivalBefore I y x && !pass1 I y) then [Rule.unique] else []) ++
  (if ancestorFires I D x then [Rule.ancestor] else [])

/-- What the app sees.
* `shown`: in the app's table;
* `removed`: present in the merged state, taken out by a rule;
* `rules`: the rules recorded for a removed row. -/
structure View (Row : Type) where
  shown : Row → Bool
  removed : Row → Bool
  rules : Row → List Rule

def view : View Row where
  shown x := I.present x && !removal I x
  removed x := I.present x && removal I x
  rules x := if I.present x && removal I x then rulesOf I x else []

/-- **Every removed row has a rule recorded.** -/
theorem removed_has_rule {x : Row} (h : (view I).removed x = true) : (view I).rules x ≠ [] := by
  simp only [view] at h ⊢
  rw [ite_pos'' h]
  simp only [Bool.and_eq_true] at h
  obtain ⟨hp, hx⟩ := h
  have h0 : start I x = false := by simp [start, hp]
  by_cases hu : uniqueLosers I.rows (rivalBefore I) (pass1 I) x = true
  · by_cases h1 : pass1 I x = true
    · have hf := star_fires (fires_monotone I) (close_star I.rows (fires I) (start I)) x h1 h0
      have hf' : fires I (removal I) x = true :=
        fires_monotone I _ _ x (fun y hy => star_grow (close_star _ _ _) y
          (by
            show uniqueLosers I.rows (rivalBefore I) (pass1 I) y = true
            simp only [uniqueLosers, Bool.or_eq_true]
            exact Or.inl hy)) hf
      unfold rulesOf
      unfold fires at hf'
      simp only [Bool.or_eq_true] at hf'
      rcases hf' with (a | a) | a <;> simp [a]
    · have hr : (I.rows.any fun y => rivalBefore I y x && !pass1 I y) = true := by
        simpa [uniqueLosers, h1] using hu
      unfold rulesOf
      simp [h1, hr]
  · have hf0 := star_fires (fires_monotone I) (close_star I.rows (fires I) _) x hx
      (by simpa using hu)
    have hf : fires I (removal I) x = true := hf0
    unfold rulesOf
    simp only [fires, Bool.or_eq_true] at hf
    rcases hf with (a | a) | a <;> simp [a]

end

/-! ## End to end -/

/-- A device's whole state: the merged state, what the app sees, and the
`coven_lost` rows for removed rows: each cell's value, the write that set it,
and the rules that removed the row. -/
structure Device (W Row Col : Type) where
  merged : St W Row Col
  view : View Row
  removedLost : Row → Col → Option (W × List Rule)

section
variable {W Row Col K : Type} [DecidableEq W] [DecidableEq Row] [DecidableEq K]

/-- The device a list of writes, applied in order, gives, with the removal
rules reading `inputs` of the merged state. -/
def device (M : Writes W Row Col) (inputs : St W Row Col → Inputs Row K) (L : List W) :
    Device W Row Col :=
  let st := L.foldl (step M) St.init
  let v := view (inputs st)
  { merged := st
    view := v
    removedLost := fun r c => if v.removed r then (st.cell r c).map (fun w => (w, v.rules r)) else none }

/-- **Convergence, end to end.** Two devices that applied the same writes,
each in an order that respects causality, hold the same merged state, show
the same rows, remove the same rows, and hold the same `coven_lost` rows,
lost values and removed rows alike. -/
theorem device_converges {M : Writes W Row Col} (hV : Valid M)
    (inputs : St W Row Col → Inputs Row K) {L₁ L₂ : List W}
    (h₁ : CausalOrder M L₁) (h₂ : CausalOrder M L₂) (hset : ∀ x, x ∈ L₁ ↔ x ∈ L₂) :
    device M inputs L₁ = device M inputs L₂ := by
  unfold device
  rw [merge_converges hV h₁ h₂ hset]

/-- And whatever order each device applies the rules in, it removes the same
rows. -/
theorem rule_order_converges {M : Writes W Row Col} (hV : Valid M)
    (inputs : St W Row Col → Inputs Row K) {L₁ L₂ : List W}
    (h₁ : CausalOrder M L₁) (h₂ : CausalOrder M L₂) (hset : ∀ x, x ∈ L₁ ↔ x ∈ L₂)
    {D₁ D₂ : Row → Bool}
    (hD₁ : let I := inputs (L₁.foldl (step M) St.init)
      Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) D₁)
    (hD₂ : let I := inputs (L₂.foldl (step M) St.init)
      Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) D₂) : D₁ = D₂ := by
  simp only at hD₁ hD₂
  rw [any_order_removal _ hD₁, any_order_removal _ hD₂, merge_converges hV h₁ h₂ hset]

end

/-! ## Why the unique rule is judged once

Folder 0 is deleted. Note 1 is in it, through a cascade key; note 2 is not.
Both claim one title, note 1's claim first. Run as one more rule among the
others, the unique rule can end two ways. -/

namespace UniqueOnce

def I : Inputs Nat Nat where
  rows := [0, 1, 2]
  present x := x != 0
  refs x := if x = 1 then [⟨0, false⟩] else []
  checkFails _ := false
  isAncestor _ := false
  keepRef _ := none
  sharedBase _ := true
  audienceFrom _ := none
  claims x := if x = 1 then [⟨0, 7, 10⟩] else if x = 2 then [⟨0, 7, 11⟩] else []
  rank x := x

/-- The unique rule as one more rule: a row is removed while a present row
claims its value first. -/
def withUnique (D : Nat → Bool) (x : Nat) : Bool :=
  fires I D x || I.rows.any (fun y => rivalBefore I y x && !D y)

theorem end1_normal : Normal (KillStep I.rows (FiresP withUnique)) (kill (start I) 1) := by
  rintro ⟨D', x, hx, hDx, hf, _⟩
  simp only [I, List.mem_cons, List.mem_nil_iff, or_false] at hx
  rcases hx with rfl | rfl | rfl <;> revert hDx hf <;> decide

theorem end2_normal :
    Normal (KillStep I.rows (FiresP withUnique)) (kill (kill (start I) 2) 1) := by
  rintro ⟨D', x, hx, hDx, hf, _⟩
  simp only [I, List.mem_cons, List.mem_nil_iff, or_false] at hx
  rcases hx with rfl | rfl | rfl <;> revert hDx hf <;> decide

/-- Cascade first keeps note 2; unique first removes note 2, then cascade
removes note 1. Both end states are final, and they differ. -/
theorem unique_with_others :
    Star (KillStep I.rows (FiresP withUnique)) (start I) (kill (start I) 1) ∧
    Star (KillStep I.rows (FiresP withUnique)) (start I) (kill (kill (start I) 2) 1) ∧
    Normal (KillStep I.rows (FiresP withUnique)) (kill (start I) 1) ∧
    Normal (KillStep I.rows (FiresP withUnique)) (kill (kill (start I) 2) 1) ∧
    kill (start I) 1 ≠ kill (kill (start I) 2) 1 := by
  refine ⟨Star.single ⟨1, by decide, by decide, by decide, rfl⟩,
    Star.head ⟨2, by decide, by decide, by decide, rfl⟩
      (Star.single ⟨1, by decide, by decide, by decide, rfl⟩),
    end1_normal, end2_normal, fun h => ?_⟩
  have := congrFun h 2
  revert this
  decide

/-- §8's three steps: step 1 removes note 1 in the deleted folder, so note 2
keeps the title. -/
theorem judged_once : (view I).shown 2 = true ∧ (view I).removed 1 = true := by decide

end UniqueOnce

end CovenMerge
