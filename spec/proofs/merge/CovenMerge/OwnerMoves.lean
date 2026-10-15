import CovenMerge.OwnerAdmission
import CovenMerge.OwnerProtection

/-! Moving a row to another audience copies it, and every row its declared
audience parent carries with it, into the destination (§8.8, §14.2). Each copy
is a direct change of the mover's write, made by coven without SQL, so it has
no step a receiver could classify as a foreign-key action. A copy keeps its
original creation, so its owner is the original row's owner.

* `refused_iff`: a move is permitted exactly when every row it carries is
  one its writer may change. Carrying, not the root, decides: Ben's own album
  cannot carry Ana's track (`Album.foreign_track_refuses`).
* `copy_refused`: each such copy fails `Admission.check` and its loss names
  the original owner, whatever origin a sender proposes for it.
* `admitted_permitted`: if every copy passes `Admission.check`, the move was
  permitted.

Destination readability stays the separate `move_safe` requirement. -/
namespace CovenMerge.Ownership.Moves

open Admission

variable {W Row Table Member Audience Payload : Type}
  [DecidableEq W] [DecidableEq Row] [DecidableEq Member] [DecidableEq Payload]

/-- The rows a move of `root` copies: the root and every present row its
declared audience parent carries with it, in the merged descendant graph. -/
def moved (s : MoveState Row Member Audience) (root r : Row) : Bool :=
  s.present r && carried s.rows s.parent root r

/-- Every copied row is a direct target and needs its own permission. -/
def permitted (P : Policy W Row Table Member) (w : W) (s : MoveState Row Member Audience)
    (root : Row) : Bool :=
  s.rows.all fun r => !moved s root r || allowed P w r

theorem refused_iff (P : Policy W Row Table Member) (w : W) (s : MoveState Row Member Audience)
    (root : Row) :
    permitted P w s root = false ↔
      ∃ r ∈ s.rows, moved s root r = true ∧ P.owned (P.table r) = true ∧ P.author w ≠ owner P r := by
  simp only [permitted, List.all_eq_false, allowed]
  constructor
  · rintro ⟨r, mem, h⟩
    refine ⟨r, mem, ?_⟩
    cases hm : moved s root r <;> cases ho : P.owned (P.table r) <;>
      simp_all
  · rintro ⟨r, mem, hm, ho, other⟩
    exact ⟨r, mem, by simp [hm, ho, other]⟩

/-- A carried row of another member refuses the move, even when the root
is the mover's own row. -/
theorem carried_foreign_refuses (P : Policy W Row Table Member) (w : W)
    (s : MoveState Row Member Audience) (root r : Row) (mem : r ∈ s.rows)
    (present : s.present r = true) (carriedRow : carried s.rows s.parent root r = true)
    (owned : P.owned (P.table r) = true) (other : P.author w ≠ owner P r) :
    permitted P w s root = false :=
  (refused_iff P w s root).mpr ⟨r, mem, by simp [moved, present, carriedRow], owned, other⟩

/-- **A copy of another member's row is refused.** The copy `c` of row `r`
keeps `r`'s creation and table, and is a direct target of the move. Whatever
origin is proposed for it, it fails the check and its loss names `r`'s owner. -/
theorem copy_refused (P : Policy W Row Table Member) (E : Evidence W Row Payload) (w : W)
    (r c : Row) (creation : P.creator c = P.creator r) (table : P.table c = P.table r)
    (owned : P.owned (P.table r) = true) (other : P.author w ≠ owner P r)
    (target : ((E.execution w).directPayload c).isSome = true) :
    check P E w c = false ∧
      ∀ D prior, Admission.guard P E D prior w c = some (.notOwner (owner P r)) := by
  have same : owner P c = owner P r := owner_fixed P c r creation
  have denied : allowed P w c = false :=
    non_owner_denied P w c (by rw [table]; exact owned) (by rw [same]; exact other)
  have refused : check P E w c = false := by
    cases h : check P E w c
    · rfl
    · exact absurd (statement_authorized P E w c target (by rw [table]; exact owned) h)
        (by rw [same]; exact other)
  refine ⟨refused, fun D prior => ?_⟩
  simp [Admission.guard, refused, denied, same]

/-- **Admitted copies mean a permitted move.** If the copy of every moved
row passes the check, no moved row belongs to another member. -/
theorem admitted_permitted (P : Policy W Row Table Member) (E : Evidence W Row Payload) (w : W)
    (s : MoveState Row Member Audience) (root : Row) (copy : Row → Row)
    (creation : ∀ r, P.creator (copy r) = P.creator r) (table : ∀ r, P.table (copy r) = P.table r)
    (target : ∀ r, moved s root r = true → ((E.execution w).directPayload (copy r)).isSome = true)
    (admitted : ∀ r ∈ s.rows, moved s root r = true → check P E w (copy r) = true) :
    permitted P w s root = true := by
  cases h : permitted P w s root
  · obtain ⟨r, mem, isMoved, owned, other⟩ := (refused_iff P w s root).mp h
    have := (copy_refused P E w r (copy r) (creation r) (table r) owned other (target r isMoved)).1
    rw [admitted r mem isMoved] at this
    cases this
  · rfl

namespace Album

/-! Ben's album 0 in the store, with Ana's track 1 carried through its
audience parent. Rows are their own tables and creations; writes 0 and 1 are
Ben's and Ana's. -/

def policy : Policy Nat Nat Nat Nat where
  table := id
  owned _ := true
  creator := id
  author r := if r = 1 then 1 else 0

def family (anaTrack : Bool) : MoveState Nat Nat Nat where
  rows := if anaTrack then [0, 1] else [0]
  present r := r == 0 || (anaTrack && r == 1)
  owner r := some (policy.author r)
  parent r := if r = 1 then some 0 else none
  canRead _ _ := true

/-- Ben's write 0 may move his album alone, but not with Ana's track in it. -/
theorem foreign_track_refuses :
    permitted policy 0 (family false) 0 = true ∧
    permitted policy 0 (family true) 0 = false ∧
    permitted policy 1 (family true) 1 = true := by decide

end Album

end CovenMerge.Ownership.Moves
