import CovenSequence.LiveClaims
import CovenSequence.Causality

/-! Owner identity is selected from counted generation-zero creations before
permission filtering. It is retained independently of the row's audience,
presence, value winners and generation representative. -/
namespace CovenSequence.Owners

structure Creation (Dev Stamp Bytes Member : Type) where
  original : Causality.Original Dev Stamp Bytes
  slot : Nat
  /-- The original write's authenticated member, not the mover's member. -/
  member : Member

variable {Dev Stamp Bytes Member Audience : Type}
abbrev Creator := Creation Dev Stamp Bytes Member

def rank (c : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member)) : LiveClaims.Rank :=
  (c.slot, c.original.number, 0)

def establish (prior : Option (Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member)))
    (creations : List (Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member))) :=
  match prior with
  | some c => some c
  | none => LiveClaims.select rank creations

theorem selected_creation
    (creations : List (Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member)))
    (c : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member))
    (selected : establish none creations = some c) :
    c ∈ creations ∧ ∀ other ∈ creations, LiveClaims.le (rank c) (rank other) :=
  LiveClaims.select_min rank creations c selected

theorem lower_write_number_wins
    (a b : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member))
    (same : a.slot = b.slot) (earlier : a.original.number < b.original.number) :
    establish none [b, a] = some a := by
  have notLater := (LiveClaims.within_slot b.slot a.original.number b.original.number 0 0 earlier).2
  simp [establish, LiveClaims.select, rank, same, notLater]

theorem later_claim_preserves_creator
    (c : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member))
    (creations : List (Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member))) :
    establish (some c) creations = some c := rfl

structure RowCopy (Dev Stamp Bytes Member Audience : Type) where
  creator : Creation Dev Stamp Bytes Member
  audience : Audience
  present : Bool

inductive Change (Audience : Type) where
  | move (to : Audience)
  | delete
  | reinsert
  | update

def apply (r : RowCopy Dev Stamp Bytes Member Audience) : Change Audience → RowCopy Dev Stamp Bytes Member Audience
  | .move to => { r with audience := to }
  | .delete => { r with present := false }
  | .reinsert => { r with present := true }
  | .update => r

def owner (r : RowCopy Dev Stamp Bytes Member Audience) : Member := r.creator.member

theorem change_preserves_creator (r : RowCopy Dev Stamp Bytes Member Audience) (ch : Change Audience) :
    (apply r ch).creator = r.creator := by cases ch <;> rfl

/-- Both halves of an audience move retain the same authenticated creator:
the source's deleted copy and the destination's full inserted copy. -/
theorem move_preserves_owner (r : RowCopy Dev Stamp Bytes Member Audience) (to : Audience) :
    owner (apply r .delete) = owner r ∧
    owner (apply (apply r (.move to)) .reinsert) = owner r ∧
    (apply (apply r (.move to)) .reinsert).creator.original = r.creator.original :=
  ⟨rfl, rfl, rfl⟩

theorem retained_creator (r : RowCopy Dev Stamp Bytes Member Audience) (changes : List (Change Audience)) :
    (changes.foldl apply r).creator = r.creator := by
  induction changes generalizing r with
  | nil => rfl
  | cons ch cs ih => exact (ih (apply r ch)).trans (change_preserves_creator r ch)

end CovenSequence.Owners
