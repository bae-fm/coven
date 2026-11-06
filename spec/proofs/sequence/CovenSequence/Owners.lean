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

/-! ## A creation still in the queue (§9.6)

A creation still in this device's queue is provisional. The device ranks it as
§9.2 ranks a queued claim: after every published creation, in write-number
order. -/

/-- The owner a device at `pos` picks among the creations it holds; `standing c`
is where `c`'s write stands on the device now. -/
def provisional (pos : Nat)
    (standing : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member) →
      LiveClaims.Standing)
    (creations : List (Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member))) :=
  LiveClaims.select (fun c => LiveClaims.withKey ((standing c).held pos) 0) creations

/-- **The device picks, among the creations it holds, the owner every receiver
picks once its queued creations land.** A creation's `slot` is the slot it is
in once landed. -/
theorem provisional_owner_lands {pos : Nat} {slotOf : Nat → Nat}
    (lands : LiveClaims.Lands pos slotOf)
    (standing : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member) →
      LiveClaims.Standing)
    (creations : List (Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member)))
    (applied : ∀ c ∈ creations, (standing c).applied pos)
    (landed : ∀ c ∈ creations, (standing c).landed slotOf = (c.slot, c.original.number)) :
    provisional pos standing creations = establish none creations := by
  unfold provisional establish
  rw [LiveClaims.queued_loss_stands lands creations standing (fun _ => 0) applied]
  apply LiveClaims.select_congr
  intro a ha b hb
  simp only [LiveClaims.withKey, landed a ha, landed b hb, rank]

/-- **A published creation the device holds owns the key before every queued
one,** so the device checks its queued changes against that owner, and later
claims never change it (`later_claim_preserves_creator`). -/
theorem published_owner_first {pos slot number : Nat}
    (standing : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member) →
      LiveClaims.Standing)
    (creations : List (Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member)))
    {p c : Creator (Dev := Dev) (Stamp := Stamp) (Bytes := Bytes) (Member := Member)}
    (held : p ∈ creations) (published : standing p = .published slot number)
    (applied : slot ≤ pos) (chosen : provisional pos standing creations = some c) :
    ∃ s n, standing c = .published s n := by
  have first := (LiveClaims.select_min _ creations c chosen).2 p held
  cases hc : standing c with
  | published s n => exact ⟨s, n, rfl⟩
  | queued m =>
    rw [hc, published] at first
    exact (LiveClaims.published_before_queued applied 0 0 first).elim

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
