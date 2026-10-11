import CovenIO.CommonReload
import CovenIO.RetentionDeadline

namespace CovenIO.AudienceRetention
open SnapshotPublication SnapshotRefresh RetentionDeadline

/-- The bound starts when a reader can supply successful passes, finality
and the reader/age gate hold, and the write precedes the catch-up sample. -/
def ready (w : Write) (gated online : Nat) : Nat := max (max gated online) (w.storedAt + 1)

/-- A due pass either observes existing coverage or requests a first/renewed
snapshot. U bounds the entire successful counting publication, not one upload.
This service has no historical-witness precondition. -/
structure WitnessService (order : SnapshotOrder) (catalog : List Snapshot) (w : Write)
    (a start passGap upload : Nat) where
  base : List Snapshot
  added : List Snapshot
  inventory : catalog = added ++ base
  observedAt : Nat
  observed : start ≤ observedAt ∧ observedAt ≤ start + passGap
  stored : ∀ s ∈ base, s.storedAt ≤ observedAt
  outcome : (∃ previous, catalogCurrent order base a = some previous ∧ previous.covers w a) ∨
    ∃ fresh ∈ added, fresh.audience = a ∧ fresh.counts = true ∧ start ≤ fresh.caughtAt ∧
      fresh.storedAt ≤ start + 2 * passGap + upload

/-- Supplies a first witness from the reader service itself, including an
empty initial catalog. Later selection races do not erase this witness. -/
theorem first_witness (order : SnapshotOrder) (catalog : List Snapshot)
    (published : Write → Prop) (w : Write) (pub : published w) (a gated online passGap upload : Nat)
    (ha : a ∈ w.audiences) (sound : ∀ s ∈ catalog, SnapshotHistory.Sound published s)
    (service : WitnessService order catalog w a (ready w gated online) passGap upload) :
    ∃ snap ∈ catalog, snap.counts = true ∧ snap.covers w a ∧
      snap.storedAt ≤ ready w gated online + 2 * passGap + upload := by
  rcases service.outcome with ⟨previous, selected, cov⟩ | ⟨fresh, member, audience, counted, caught, landed⟩
  · have facts := current_member order service.base a previous selected
    refine ⟨previous, ?_, facts.2.2, cov, ?_⟩
    · rw [service.inventory]; exact List.mem_append_right _ facts.1
    · have := service.stored previous facts.1
      have := service.observed.2
      omega
  · have inCatalog : fresh ∈ catalog := by
      rw [service.inventory]; exact List.mem_append_left _ member
    have old : w.storedAt < fresh.caughtAt := by
      have : w.storedAt + 1 ≤ ready w gated online := Nat.le_max_right _ _
      omega
    have cov := (sound fresh inCatalog).caught w pub (audience ▸ ha) old
    exact ⟨fresh, inCatalog, counted, by simpa [audience] using cov, landed⟩

/-- An audience's obligations are mature historical coverage and current
coverage. A multipart object's DELETE also needs every other part audience. -/
def Prepared (order : SnapshotOrder) (catalog : List Snapshot) (time : Nat) (w : Write) (a : Nat) : Prop :=
  (∃ snap, catalogCurrent order catalog a = some snap ∧ snap.covers w a) ∧
    ∃ snap ∈ catalog, snap.counts = true ∧ snap.covers w a ∧ snap.storedAt + day < time

/-- Per-audience deadline, relative to that audience's available reader.
No loader state or retention pin occurs in its premises. -/
theorem audience_deadline (order : SnapshotOrder) (writers : List Nat) (catalog : List Snapshot)
    (published : Write → Prop) (w : Write) (pub : published w) (writer : w.writer ∈ writers)
    (a gated online passGap upload time : Nat) (ha : a ∈ w.audiences)
    (sound : ∀ s ∈ catalog, SnapshotHistory.Sound published s)
    (normalized : ∀ s ∈ catalog, Normalized writers s)
    (first : WitnessService order catalog w a (ready w gated online) passGap upload)
    (repair : Service order writers catalog w a
      (ready w gated online + 2 * passGap + upload + day + 1) passGap upload)
    (scheduled : ready w gated online + 4 * passGap + 2 * upload + day + 1 ≤ time) :
    Prepared order catalog time w a := by
  let bound := ready w gated online + 2 * passGap + upload
  obtain ⟨witness, member, counted, cov, age⟩ := first_witness order catalog published w pub a gated online
    passGap upload ha sound first
  have stored : w.storedAt ≤ bound := by
    have : w.storedAt + 1 ≤ ready w gated online := Nat.le_max_right _ _
    dsimp [bound]; omega
  refine ⟨service_covers order writers catalog published w pub writer a bound (bound + day + 1)
    passGap upload ha sound normalized stored (by omega) repair, witness, member, counted, cov, ?_⟩
  omega

/-- Each part has its own reader-availability time. Their maximum bounds the
indivisible write object, whose eligibility still checks every part. -/
def witnessBound (w : Write) (gated : Nat) (online : Nat → Nat) (passGap upload : Nat) : Nat :=
  CommonReload.maximum w.audiences (fun a => ready w gated (online a) + 2 * passGap + upload)

/-- First witnesses feed the existing witnessed-write theorem. Its deadline
R+3P+U+D is unchanged; R includes first publication and strict witness age.
An audience without a successful reader service supplies no such deadline. -/
theorem bounded_storage (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Write → Bool) (w : Write) (published : Write → Prop)
    (pub : published w) (isFinal : final w = true) (present : w ∈ s.writes) (writer : w.writer ∈ s.writers)
    (gated passGap upload latency : Nat) (online : Nat → Nat)
    (sound : ∀ snap ∈ s.landed, SnapshotHistory.Sound published snap)
    (normalized : ∀ snap ∈ s.landed, Normalized s.writers snap)
    (nonempty : ∃ a, a ∈ w.audiences)
    (first : ∀ a ∈ w.audiences, WitnessService order s.landed w a (ready w gated (online a)) passGap upload)
    (repair : ∀ a ∈ w.audiences, Service order s.writers s.landed w a
      (max gated (witnessBound w gated online passGap upload + day + 1)) passGap upload)
    (gate : (∀ reader ∈ s.writers, w.number ≤ postedPosition posted reader w.writer) ∨
      w.storedAt + month ≤ s.time)
    (scheduled : max gated (witnessBound w gated online passGap upload + day + 1) + 2 * passGap + upload ≤ s.time ∧
      s.time ≤ max gated (witnessBound w gated online passGap upload + day + 1) + 3 * passGap + upload)
    (removed : Nat → Write → Prop)
    (completion : ∀ v ∈ (retainWrites order s posted final).deleted, removed (s.time + latency) v) :
    ∃ time, time ≤ max gated (witnessBound w gated online passGap upload + day + 1) +
      3 * passGap + upload + latency ∧ removed time w := by
  apply bounded_witnessed_storage order s posted final w published pub isFinal present writer
    (witnessBound w gated online passGap upload) gated passGap upload latency sound normalized
    ?_ nonempty repair gate scheduled removed completion
  intro a ha
  obtain ⟨snap, member, counted, cov, bound⟩ := first_witness order s.landed published w pub a gated
    (online a) passGap upload ha sound (first a ha)
  exact ⟨snap, member, counted, cov, Nat.le_trans bound
    (CommonReload.maximum_member w.audiences
      (fun a => ready w gated (online a) + 2 * passGap + upload) a ha)⟩

end CovenIO.AudienceRetention
