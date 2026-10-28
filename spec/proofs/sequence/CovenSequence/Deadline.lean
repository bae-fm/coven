import CovenSequence.Retention

/-! Successful service bounds in duration ticks. Snapshot service includes
preparation, upload, announcement and validation; maintenance includes a
successful conditional floor write. Neither waits for absent devices. -/
namespace CovenSequence.Deadline
open CovenSequence.Retention
variable {Aud : Type}

/-- `usable` includes announcement validation and activation of a preparation;
an uploaded body or an unactivated preparation is not a witness. -/
def Mature (relevant : List Aud) (usable : Aud → Snapshot → Prop)
    (cut grace time : Nat) : Prop :=
  ∀ a ∈ relevant, ∃ s, usable a s ∧ cut ≤ s.covers ∧ s.announcedAt + grace ≤ time

theorem selection_advances (relevant : List Aud) (nonempty : relevant ≠ [])
    (catalog : Aud → List Snapshot) (usable : Aud → Snapshot → Bool)
    (cut grace time : Nat)
    (ordered : ∀ a ∈ relevant, (catalog a).Pairwise (fun x y => y.covers ≤ x.covers))
    (witness : ∀ a ∈ relevant, ∃ s ∈ catalog a,
      eligible (usable a) grace time s = true ∧ cut ≤ s.covers) :
    ∃ first, floorStart (selectedCoverage relevant catalog usable grace time) = some first ∧
      cut < first := by
  apply complete_coverage_advances _ cut
  · simpa [selectedCoverage] using nonempty
  · intro c hc
    obtain ⟨a, ha, rfl⟩ := List.mem_map.mp hc
    obtain ⟨w, hw, he, hcut⟩ := witness a ha
    obtain ⟨s, hs, _, _, hcover⟩ := choose_dominates Snapshot.covers (catalog a)
      (ordered a ha) (usable a) grace time hw he
    exact ⟨s.covers, by simp [hs], by omega⟩

/-- Snapshot service includes announced coverage that remains applicable at
maintenance. Catalogs retain announcement history, even after body replacement.
Maintenance promises a successful write of the selected floor, not progress. -/
structure Service (relevant : List Aud) (catalog : Nat → Aud → List Snapshot)
    (usable : Nat → Aud → Snapshot → Bool) (floor : Nat → Nat)
    (removed : Nat → Nat → Prop) (cut ready U G P D : Nat) : Prop where
  nonempty : relevant ≠ []
  ordered : ∀ time a, a ∈ relevant →
    (catalog time a).Pairwise (fun x y => y.covers ≤ x.covers)
  snapshots : ∀ time, ready + U ≤ time → time ≤ ready + U + G + P → ∀ a ∈ relevant,
    ∃ s ∈ catalog time a, usable time a s = true ∧ cut ≤ s.covers ∧
      s.announcedAt ≤ ready + U
  maintenance : ∀ time, ∃ pass, time ≤ pass ∧ pass ≤ time + P ∧
    ∀ first, floorStart (selectedCoverage relevant (catalog pass) (usable pass) G pass) = some first →
      first ≤ floor pass
  deletion : ∀ pass, cut < floor pass →
    ∃ finish, pass ≤ finish ∧ finish ≤ pass + D ∧ ∀ i, 1 ≤ i → i ≤ cut → removed finish i

/-- No bound or spacing requirement is placed on newer announcements. -/
theorem bounded_retention (relevant : List Aud) (catalog : Nat → Aud → List Snapshot)
    (usable : Nat → Aud → Snapshot → Bool) (floor : Nat → Nat)
    (removed : Nat → Nat → Prop) (cut ready U G P D : Nat)
    (service : Service relevant catalog usable floor removed cut ready U G P D) :
    ∃ finish, finish ≤ ready + U + G + P + D ∧
      ∀ i, 1 ≤ i → i ≤ cut → removed finish i := by
  obtain ⟨pass, lower, upper, written⟩ := service.maintenance (ready + U + G)
  have witness : ∀ a ∈ relevant, ∃ s ∈ catalog pass a,
      eligible (usable pass a) G pass s = true ∧ cut ≤ s.covers := by
    intro a ha
    obtain ⟨s, hs, hu, hc, ht⟩ := service.snapshots pass (by omega) (by omega) a ha
    exact ⟨s, hs, by simp [eligible, hu, show s.announcedAt + G ≤ pass by omega], hc⟩
  obtain ⟨first, selected, advanced⟩ := selection_advances relevant service.nonempty
    (catalog pass) (usable pass) cut G pass (service.ordered pass) witness
  have raised := written first selected
  obtain ⟨finish, _, upper', deleted⟩ := service.deletion pass (by omega)
  exact ⟨finish, by omega, deleted⟩

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
