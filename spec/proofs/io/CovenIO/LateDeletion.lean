import CovenIO.Retention

/-! §15 (T221): a snapshot that landed more than a day after its `T_c` never
counts, so no reader selects, loads, or takes coverage, deletion evidence or
a historical witness from it. Deleting it, which its author does in the
operation that abandons it, changes nothing any reader decides. -/

namespace CovenIO

/-- Selection, a fresh catalog's deletion certificate and mature coverage are
the same with or without a never-counting snapshot listed. -/
theorem late_deletion_unseen (order : SnapshotOrder) (snap : Snapshot) (rest : List Snapshot)
    (never : snap.counts = false) :
    catalogCurrent order (snap :: rest) = catalogCurrent order rest ∧
      (∀ writer position, coveredMiss (snap :: rest) writer position =
        coveredMiss rest writer position) ∧
      ∀ time w, MatureCoverage (snap :: rest) time w ↔ MatureCoverage rest time w := by
  refine ⟨?_, ?_, ?_⟩
  · funext audience
    simp [catalogCurrent, never]
  · intro writer position
    simp [coveredMiss, never]
  · intro time w
    constructor
    · intro mature a ha
      obtain ⟨witness, member, counted, covers, age⟩ := mature a ha
      rcases List.mem_cons.mp member with rfl | old
      · rw [never] at counted; cases counted
      · exact ⟨witness, old, counted, covers, age⟩
    · intro mature a ha
      obtain ⟨witness, member, counted, covers, age⟩ := mature a ha
      exact ⟨witness, List.mem_cons_of_mem _ member, counted, covers, age⟩

/-- In particular a landing more than a day after its sample. -/
theorem late_landing_never_counts (snap : Snapshot) (late : snap.caughtAt + day < snap.storedAt) :
    snap.counts = false := by
  simp [Snapshot.counts]; omega

end CovenIO
