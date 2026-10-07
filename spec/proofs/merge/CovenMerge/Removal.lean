import CovenMerge.Fixpoint
import CovenMerge.Converge

/-!
# §8's removal rules

What the rules read from the merged state is collected in `Inputs`: which
rows are present, each row's references, whether its merged values fail a
CHECK, whether it is in a deleted circle, and its claims. `Inputs` is computed
from the merged state and the store log; nothing in it depends on which rows
the rules remove.

* `fires`: the rules other than the unique ones: foreign keys, CHECK, and
  deleted circles (§8, §8.4, §8.6, §14.7).
* `fires_monotone`: each keeps firing when more rows are removed.
* `removal`: §8's three steps, run executably; `removal_stratified` shows
  it is one of the orders `Stratified` allows, so by `any_order_removal`
  every order of applying the rules gives it.
* `view`: what the app sees: rows shown, rows removed, and the rules
  recorded for each removed row.
* `removed_has_rule`: every removed row has a rule recorded.
* `device_converges`: two devices that applied the same writes in causal
  orders hold the same merged state, the same view, and the same `_coven_lost`
  rows for removed rows.
* `unique_with_others`: run as one more rule among the others, the unique
  rule can end two ways from one state; the reason it is judged once.
-/

namespace CovenMerge

/-- One reference, as the merged state has it.
* `parent`: the row it points at;
* `stale`: the parent's generation it carries was deleted since (§8.4).

A reference under set null whose parent's generation was deleted holds null
in the merged state, so it is no reference here. One under set default points
at the default parent's current generation, never stale. Where SQLite would
refuse the null or the default, the reference stays here, stale, and the row
is taken out as under restrict (§8.4). -/
structure Ref (Row : Type) where
  parent : Row
  stale : Bool

/-- A unique constraint's ordered column/expression terms and partial predicate. -/
structure UniqueConstraint where
  terms : List String
  predicate : Option String
  deriving DecidableEq, Repr

/-- One claim: the constraint, the value claimed, the claim's stamp, and
whether it is a key present in two audiences (§8.5, §14.2). -/
structure Claim (K : Type) where
  con : UniqueConstraint
  key : K
  ts : Nat
  other : Bool

/-- What the removal rules read, computed from the merged state and the store
log.
* `rows`: the synced rows the device has;
* `present`: the row's generation is odd;
* `refs`: its references;
* `checkFails`: its merged values fail a CHECK (§8.6);
* `inDeletedCircle`: its audience is a circle the store log has deleted
  (§14.7);
* `claims`: its claims (§8.5, §14.2);
* `rank`: its primary key's order, which breaks ties between claims. -/
structure Inputs (Row K : Type) where
  rows : List Row
  present : Row → Bool
  refs : Row → List (Ref Row)
  checkFails : Row → Bool
  inDeletedCircle : Row → Bool
  claims : Row → List (Claim K)
  rank : Row → Nat

section
variable {Row K : Type} [DecidableEq Row] [DecidableEq K] (I : Inputs Row K)

/-- Foreign keys: a reference whose parent's generation was deleted since,
or whose parent is absent or taken out (§8, §8.4). -/
def fkFires (D : Row → Bool) (x : Row) : Bool :=
  (I.refs x).any (fun r => r.stale || D r.parent)

/-- Every rule but the unique ones. -/
def fires (D : Row → Bool) (x : Row) : Bool :=
  I.checkFails x || fkFires I D x || I.inDeletedCircle x

/-- Row `y`'s claim of kind `other` comes before row `x`'s: same constraint,
same value, smaller stamp, or equal stamps and smaller key. -/
def rivalOf (other : Bool) (y x : Row) : Bool :=
  decide (y ≠ x) && (I.claims x).any (fun c => decide (c.other = other) && (I.claims y).any (fun c' =>
    decide (c'.other = other) && decide (c'.con = c.con) && decide (c'.key = c.key) &&
      (decide (c'.ts < c.ts) || (decide (c'.ts = c.ts) && decide (I.rank y < I.rank x)))))

/-- Row `y`'s claim comes before row `x`'s, of either kind. -/
def rivalBefore (y x : Row) : Bool := rivalOf I false y x || rivalOf I true y x

/-- Rows absent by their own generation. In `D`, absent and removed rows
alike are "out". -/
def start : Row → Bool := fun x => !I.present x

/-- §8's step 1: the other rules until none fires. -/
def pass1 : Row → Bool := close I.rows (fires I) (start I)

/-- §8's steps 2 and 3: unique values and keys in two audiences judged among
`pass1`'s survivors, then the other rules again. The result: every absent or
removed row. -/
def removal : Row → Bool := close I.rows (fires I) (uniqueLosers I.rows (rivalBefore I) (pass1 I))

/-- **Every rule but the unique ones keeps firing when more rows are
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
  · exact Or.inr h1

theorem removal_stratified :
    Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) (removal I) :=
  ⟨pass1 I, close_star _ _ _, close_normal _ _ _, close_star _ _ _, close_normal _ _ _⟩

/-- **Any order of applying the rules gives `removal`.** -/
theorem any_order_removal {D : Row → Bool}
    (h : Stratified I.rows (FiresP (fires I)) (rivalBefore I) (start I) D) : D = removal I :=
  stratified_unique (fires_monotone I) h (removal_stratified I)

/-- A rule a removed row's `_coven_lost` row names (§8, §20.4). -/
inductive Rule where
  | foreignKey
  | check
  | deletedCircle
  | otherAudience
  | unique
  deriving DecidableEq, Repr

/-- Lost in step 2 to a claim of kind `other`. -/
def lostIn2 (other : Bool) (x : Row) : Bool :=
  !pass1 I x && I.rows.any (fun y => rivalOf I other y x && !pass1 I y)

/-- The rules for a removed row: every rule that holds for it once the rules
have run, and a unique rule from the step that judged it. -/
def rulesOf (x : Row) : List Rule :=
  let D := removal I
  (if fkFires I D x then [Rule.foreignKey] else []) ++
  (if I.checkFails x then [Rule.check] else []) ++
  (if I.inDeletedCircle x then [Rule.deletedCircle] else []) ++
  (if lostIn2 I true x then [Rule.otherAudience] else []) ++
  (if lostIn2 I false x then [Rule.unique] else [])

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
      have h1' : pass1 I x = false := by simpa using h1
      simp only [List.any_eq_true, Bool.and_eq_true, Bool.not_eq_true', rivalBefore,
        Bool.or_eq_true] at hr
      obtain ⟨y, hy, hk | hk, hpy⟩ := hr
      · have : lostIn2 I false x = true := by
          simp only [lostIn2, h1', Bool.not_false, Bool.true_and, List.any_eq_true,
            Bool.and_eq_true, Bool.not_eq_true']
          exact ⟨y, hy, hk, hpy⟩
        unfold rulesOf
        simp [this]
      · have : lostIn2 I true x = true := by
          simp only [lostIn2, h1', Bool.not_false, Bool.true_and, List.any_eq_true,
            Bool.and_eq_true, Bool.not_eq_true']
          exact ⟨y, hy, hk, hpy⟩
        unfold rulesOf
        simp [this]
  · have hf0 := star_fires (fires_monotone I) (close_star I.rows (fires I) _) x hx
      (by simpa using hu)
    have hf : fires I (removal I) x = true := hf0
    unfold rulesOf
    simp only [fires, Bool.or_eq_true] at hf
    rcases hf with (a | a) | a <;> simp [a]

end

/-! ## End to end -/

/-- A device's whole state: the merged state, what the app sees, and the
`_coven_lost` rows for removed rows: each cell's value, the write that set it,
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
the same rows, remove the same rows, and hold the same `_coven_lost` rows,
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
  inDeletedCircle _ := false
  claims x := if x = 1 then [⟨⟨["title"], none⟩, 7, 10, false⟩] else if x = 2 then [⟨⟨["title"], none⟩, 7, 11, false⟩] else []
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
