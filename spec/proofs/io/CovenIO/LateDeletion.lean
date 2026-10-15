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

/-! T224: an attempt abandoned before its landing was confirmed. A request
already sent may still land, so its author records the attempt with the
abandonment and later checks its path: a landing that does not count is
deleted, which no reader notices (`late_deletion_unseen`); one that counts
stays an ordinary snapshot. -/

/-- A request of an attempt abandoned once an observed storage time was past
its sample plus a day lands no earlier than that time, so it never counts:
no reader selects, loads or takes coverage from it, and deleting it changes
nothing a reader decides. -/
theorem abandoned_landing_unseen (order : SnapshotOrder) (l : Snapshot) (rest : List Snapshot)
    (abandonedAt : Nat) (passed : l.caughtAt + day < abandonedAt)
    (after : abandonedAt ≤ l.storedAt) :
    l.counts = false ∧ catalogCurrent order (l :: rest) = catalogCurrent order rest ∧
      (∀ writer position, coveredMiss (l :: rest) writer position =
        coveredMiss rest writer position) ∧
      ∀ time w, MatureCoverage (l :: rest) time w ↔ MatureCoverage rest time w := by
  have never := late_landing_never_counts l (by omega)
  exact ⟨never, late_deletion_unseen order l rest never⟩

/-- So a landing of such an attempt that counts was stored before the
abandonment: a request whose reply was lost. Readers may select it, and the
cleanup keeps it. -/
theorem counting_abandoned_landed_before (l : Snapshot) (abandonedAt : Nat)
    (passed : l.caughtAt + day < abandonedAt) (counts : l.counts = true) :
    l.storedAt < abandonedAt := by
  simp [Snapshot.counts] at counts; omega

end CovenIO
