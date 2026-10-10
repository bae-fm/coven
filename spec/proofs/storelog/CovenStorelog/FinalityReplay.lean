import CovenStorelog.ReplayPrefix
import CovenStorelog.Accounting
import CovenStorelog.Converge

/-! The decided contradiction rules and storage-time admission, separately
from the replay policy compared with Rust. Both use `scan` and `settle`. -/
namespace CovenStorelog.Finality

/-- Store removals, circle deletions, circle removals, ordinary changes,
admin grants. Device removal cuts store access and belongs to the first tier. -/
def tier : Action → Nat
  | .removeMember _ _ | .removeDevice _ _ => 0
  | .deleteCircle _ => 1
  | .removeFromCircle _ _ => 2
  | .addMember _ .admin _ | .changeRole _ .admin => 4
  | _ => 3

def prefer (M : Log) (a b : Nat) : Bool :=
  tier (M a).action < tier (M b).action ||
    (tier (M a).action == tier (M b).action && a < b)

/-- Removing two memberships or devices makes compatible statements. The
last-admin check remains in `checkedEffect`, independently of pair conflicts. -/
def isRemoval : Action → Bool
  | .removeMember _ _ | .removeDevice _ _ | .deleteCircle _ | .removeFromCircle _ _ => true
  | _ => false

/-- No key-rotation conflicts. Deleting a circle does not oppose its other
changes; those encounter the circle's absence in `checkedEffect`.
Different statements about a member/device and competing snapshots remain. -/
def contradiction (a b : Entry) : Bool :=
  !sameMeaning a b && !(isRemoval a.action && isRemoval b.action) &&
    (sameTarget (memberTarget a.action) (memberTarget b.action) ||
     sameTarget (deviceTarget a.action) (deviceTarget b.action) ||
     match a.action, b.action with
     | .raiseSchema v s, .raiseSchema w t => s.audience == t.audience && v == w && s != t
     | .reset s, .reset t => s.audience == t.audience && s != t
     | .reset s, .raiseSchema _ t | .raiseSchema _ t, .reset s => s.audience == t.audience
     | _, _ => false)

def conflict (M : Log) (a b : Nat) : Bool :=
  concurrent M a b && contradiction (M a) (M b)

theorem read_no_conflict (M : Log) (a b : Nat) (h : hadRead M a b = true) :
    conflict M a b = false := by simp [conflict, concurrent, h]

theorem preference_strict (M : Log) :
    (∀ a, prefer M a a = false) ∧
    (∀ a b c, prefer M a b = true → prefer M b c = true → prefer M a c = true) ∧
    (∀ a b, a ≠ b → prefer M a b = true ∨ prefer M b a = true) := by
  constructor
  · intro a; simp [prefer]
  · constructor
    · intro a b c hab hbc
      simp only [prefer, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
        beq_iff_eq] at *
      omega
    · intro a b h
      simp only [prefer, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
        beq_iff_eq]
      omega

/-- Immutable landing and first-attempt duration values. Equal stored times
are representable; constraints belong to each model's validity predicate.
Author ids retain only timestamp order; storage durations are never renumbered. -/
structure History where
  log : Log
  stored : Nat → Nat
  attempted : Nat → Nat

/-- Online authors read precisely the visible prefix at first attempt. A retry
retains that past. Attempt events precede landings at the same clock tick. -/
structure Valid (H : History) (n : Nat) : Prop where
  causal : CovenStorelog.Valid H.log n
  unique : ∀ a b, a < n → b < n → H.stored a = H.stored b → a = b
  attempt_le : ∀ w, w < n → H.attempted w ≤ H.stored w
  online : ∀ w a, w < n → a < n →
    (hadRead H.log w a = true ↔ H.stored a < H.attempted w)

/-- Rule 1 uses storage duration, never author timestamps or receipt time. -/
def tooLate (H : History) (W n : Nat) (S : EntrySet) (w : Nat) : Bool :=
  (List.range n).any fun a => S a &&
    decide (H.stored a + W < H.stored w) && !hadRead H.log w a

theorem tooLate_iff (H : History) (W n : Nat) (S : EntrySet) (w : Nat) :
    tooLate H W n S w = true ↔ ∃ a, a < n ∧ S a = true ∧
      H.stored a + W < H.stored w ∧ hadRead H.log w a = false := by
  simp only [tooLate, List.any_eq_true, List.mem_range, Bool.and_eq_true,
    decide_eq_true_eq, Bool.not_eq_true']
  constructor
  · rintro ⟨a, ha, ⟨hs, ht⟩, hp⟩; exact ⟨a, ha, hs, ht, hp⟩
  · rintro ⟨a, ha, hs, ht, hp⟩; exact ⟨a, ha, ⟨hs, ht⟩, hp⟩

def admitted (H : History) (W n : Nat) (S : EntrySet) : EntrySet :=
  fun w => S w && !tooLate H W n S w

/-- Time-dropped entries remain accounted for, but are never replay candidates. -/
def materialize (H : History) (W n : Nat) (S : EntrySet) (views : Nat → State) : Result :=
  let r := settle H.log views ((List.range n).filter (admitted H W n S))
    (conflict H.log) (prefer H.log)
  { r with dropped := r.dropped ++ (List.range n).filter (fun w => S w && tooLate H W n S w) }

/-- Historical authority is itself obtained by this same time-aware replay of
the entry's recorded past, recursively at smaller author timestamps. -/
def authorViews (H : History) (W : Nat) : Nat → Nat → State
  | 0 => fun _ => State.empty
  | n + 1 =>
      let prior := authorViews H W n
      fun w => if w = n then (materialize H W n (hadRead H.log n) prior).state else prior w

def resolve (H : History) (W n : Nat) (S : EntrySet) : Result :=
  materialize H W n S (authorViews H W n)

def authorView (H : History) (W w : Nat) : State :=
  (resolve H W w (hadRead H.log w)).state

theorem authorViews_at (H : History) (W : Nat) {n w : Nat} (h : w < n) :
    authorViews H W n w = authorView H W w := by
  induction n with
  | zero => omega
  | succ n ih =>
      by_cases he : w = n
      · subst w; simp [authorViews, authorView, resolve]
      · simp only [authorViews, he, ite_false]
        exact ih (by omega)

/-- One status for each received entry; time rejection does not lose an id. -/
theorem partition (H : History) (W n : Nat) (S : EntrySet) (w : Nat) :
    (w < n ∧ S w = true ↔ w ∈ (resolve H W n S).kept ∨ w ∈ (resolve H W n S).dropped) ∧
    (w ∈ (resolve H W n S).kept → w ∉ (resolve H W n S).dropped) := by
  have h := settleN_accounting H.log (authorViews H W n)
    ((List.range n).filter (admitted H W n S))
    ((List.nodup_range (n := n)).filter _) (by simp)
    (settle_eq_some H.log (authorViews H W n) _ (conflict H.log) (prefer H.log))
  have hc := h.covered w
  have hs := h.separate w
  simp only [List.not_mem_nil, false_or, List.mem_filter, List.mem_range,
    admitted, Bool.and_eq_true, Bool.not_eq_true'] at hc
  simp only [resolve, materialize, List.mem_append, List.mem_filter, List.mem_range,
    Bool.and_eq_true]
  cases ht : tooLate H W n S w <;> simp only [ht] at hc ⊢ <;>
    constructor
  all_goals simp only [Bool.false_eq_true, Bool.true_eq_false, and_false, and_true,
    or_false] at *
  · exact hc
  · intro hk; exact (hs hk).2
  · constructor
    · exact fun hw => Or.inr (Or.inr hw)
    · intro hw
      rcases hw with hk | hd | hw
      · exact False.elim (hc.mpr (Or.inl hk))
      · exact False.elim (hc.mpr (Or.inr hd))
      · exact hw
  · intro hk; exact False.elim (hc.mpr (Or.inl hk))

end CovenStorelog.Finality
