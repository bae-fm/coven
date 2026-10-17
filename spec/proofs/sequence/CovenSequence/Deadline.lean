import CovenSequence.Retention

/-! Successful service bounds, in abstract duration ticks. Snapshot service
includes preparation, upload, announcement, contention and validation. The
maintenance bound includes a successful conditional floor update. Neither
service asks an absent device to acknowledge anything. -/
namespace CovenSequence.Deadline

variable {Aud : Type}

structure Snapshot where
  covers : Nat
  announcedAt : Nat

/-- `usable` includes announcement validation and activation of a preparation;
an uploaded body or an unactivated preparation is not a witness. -/
def Mature (relevant : List Aud) (usable : Aud → Snapshot → Prop)
    (cut grace time : Nat) : Prop :=
  ∀ a ∈ relevant, ∃ s, usable a s ∧ cut ≤ s.covers ∧ s.announcedAt + grace ≤ time

structure Service (relevant : List Aud) (usable : Aud → Snapshot → Prop)
    (floor : Nat → Nat) (removed : Nat → Nat → Prop) (cut ready U P D : Nat) : Prop where
  snapshots : ∀ a ∈ relevant, ∃ s, usable a s ∧ cut ≤ s.covers ∧ s.announcedAt ≤ ready + U
  maintenance : ∀ grace time, Mature relevant usable cut grace time →
    ∃ pass, time ≤ pass ∧ pass ≤ time + P ∧ cut < floor pass
  deletion : ∀ pass, cut < floor pass →
    ∃ finish, pass ≤ finish ∧ finish ≤ pass + D ∧ ∀ i, 1 ≤ i → i ≤ cut → removed finish i

/-- A ready cut is deleted within U+G+P+D. The services quantify only
over relevant audiences, floor updates and deletion, never device presence. -/
theorem bounded_retention (relevant : List Aud) (usable : Aud → Snapshot → Prop)
    (floor : Nat → Nat) (removed : Nat → Nat → Prop) (cut ready U G P D : Nat)
    (service : Service relevant usable floor removed cut ready U P D) :
    ∃ finish, finish ≤ ready + U + G + P + D ∧
      ∀ i, 1 ≤ i → i ≤ cut → removed finish i := by
  have mature : Mature relevant usable cut G (ready + U + G) := by
    intro a ha
    obtain ⟨s, hs, hc, ht⟩ := service.snapshots a ha
    exact ⟨s, hs, hc, by omega⟩
  obtain ⟨pass, _, upper, raised⟩ := service.maintenance G _ mature
  obtain ⟨finish, _, doneBy, gone⟩ := service.deletion pass raised
  exact ⟨finish, by omega, gone⟩

/-- If one relevant audience has no covering snapshot, no time satisfies the
retention gate. Thus no unconditional deletion deadline follows. -/
theorem uncovered_has_no_deadline (relevant : List Aud) (usable : Aud → Snapshot → Prop)
    (cut : Nat) (a : Aud) (ha : a ∈ relevant)
    (missing : ∀ s, usable a s → s.covers < cut) :
    ∀ grace time, ¬ Mature relevant usable cut grace time := by
  intro grace time mature
  obtain ⟨s, hs, covered, _⟩ := mature a ha
  have := missing s hs
  omega

/-- The abstract floor bound is exactly the concrete storage deletion gate. -/
theorem cut_delete_allowed {S : Sig} (R : Rules S) (seq : Nat → Slot S)
    (st : Store S) (cut : Nat) (raised : cut < st.floor.start)
    (i : Nat) (hi : i ≤ cut) :
    StoreStep R seq st { st with slot := fun j => if j = i then none else st.slot j } :=
  StoreStep.delete st i (by omega)

end CovenSequence.Deadline
