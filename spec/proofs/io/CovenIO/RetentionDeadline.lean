import CovenIO.SnapshotRefresh

namespace CovenIO.RetentionDeadline
open SnapshotPublication SnapshotRefresh

def historical (ss : List Snapshot) (w : Write) (a : Nat) : Bool :=
  ss.any (fun s => s.counts && decide (s.covers w a))

/-- The coverage deficit itself also detects an audience's first snapshot. -/
def refreshDue (order : SnapshotOrder) (s : State) (posted : List (Nat × Nat × Nat))
    (final readable : Bool) (w : Write) (a : Nat) : Bool :=
  final && readable && decide (a ∈ w.holders) &&
    decide ((∀ reader ∈ s.writers, w.number ≤ postedPosition posted reader w.writer) ∨
      w.storedAt + month ≤ s.time) &&
    !(match catalogCurrent order s.landed a with | none => false | some snap => decide (snap.covers w a))

/-- Historical-witness-only trigger, retained for its checked counterexample. -/
def witnessedRefreshDue (order : SnapshotOrder) (s : State) (posted : List (Nat × Nat × Nat))
    (final readable : Bool) (w : Write) (a : Nat) : Bool :=
  refreshDue order s posted final readable w a && historical s.landed w a

/-- At the due pass, keep existing coverage or perform the required refresh
in that pass or the next. The final catalog retains that observation and all
subsequent publications. U bounds the entire successful counting publication,
including catch-up, preparation, failed/abandoned attempts and confirmation. -/
structure Service (order : SnapshotOrder) (writers : List Nat) (catalog : List Snapshot)
    (w : Write) (a ready passGap upload : Nat) where
  base : List Snapshot
  added : List Snapshot
  inventory : catalog = added ++ base
  previous : Snapshot
  selected : catalogCurrent order base a = some previous
  afterReady : ∀ s ∈ added, ready ≤ s.storedAt
  outcome : previous.covers w a ∨ ∃ fresh ∈ added,
    fresh.audience = a ∧ fresh.counts = true ∧ Normalized writers fresh ∧
    Dominates fresh previous ∧ ready ≤ fresh.caughtAt ∧ fresh.caughtAt ≤ fresh.storedAt ∧
    fresh.storedAt ≤ ready + 2 * passGap + upload

/-- Once witness age has elapsed, the bounded refresh makes this audience's
current selection cover the write; coverage is a conclusion of the service. -/
theorem service_covers (order : SnapshotOrder) (writers : List Nat) (catalog : List Snapshot)
    (published : Write → Prop) (w : Write) (pub : published w) (writer : w.writer ∈ writers)
    (a bound ready passGap upload : Nat) (ha : a ∈ w.holders)
    (sound : ∀ snap ∈ catalog, SnapshotHistory.Sound published snap)
    (normalized : ∀ snap ∈ catalog, Normalized writers snap)
    (stored : w.storedAt ≤ bound) (mature : bound + day < ready)
    (action : Service order writers catalog w a ready passGap upload) :
    ∃ chosen, catalogCurrent order catalog a = some chosen ∧ chosen.covers w a := by
  have newCoverage : ∀ snap ∈ action.added, snap.audience = a → snap.counts = true → snap.covers w a := by
    intro snap member audience counted
    have inCatalog : snap ∈ catalog := by rw [action.inventory]; exact List.mem_append_left _ member
    have cov := fresh_covers published w pub snap (sound snap inCatalog) (audience ▸ ha)
      counted bound ready stored mature (action.afterReady snap member)
    simpa [audience] using cov
  have oldMember : action.previous ∈ catalog := by
    have member := List.mem_append_right action.added (current_member order action.base a action.previous action.selected).1
    simpa only [← action.inventory] using member
  have oldFacts := current_member order action.base a action.previous action.selected
  have covered : ∃ chosen, catalogCurrent order catalog a = some chosen ∧ chosen.covers w a := by
    rcases action.outcome with already | ⟨fresh, member, audience, counted, form, dominates, caught, clock, _⟩
    · rw [action.inventory]
      exact covered_selection_preserved order action.base action.added a w
        ⟨action.previous, action.selected, already⟩ newCoverage
    · by_cases already : action.previous.covers w a
      · rw [action.inventory]
        exact covered_selection_preserved order action.base action.added a w
          ⟨action.previous, action.selected, already⟩ newCoverage
      · have cov := newCoverage fresh member audience counted
        have wins := refresh_outranks order writers published w pub writer action.previous fresh
          (by simpa only [oldFacts.2.1] using ha) (sound action.previous oldMember) oldFacts.2.2
          (normalized action.previous oldMember) form dominates
          (by simpa [oldFacts.2.1] using already) (by simpa [audience] using cov)
          bound ready stored mature caught clock
        rw [action.inventory]
        exact refresh_selected order action.base action.added a action.previous fresh w action.selected
          member audience counted newCoverage wins
  exact covered

/-- This joins the publication repair, all part audiences, historical age,
retention scheduling and DELETE completion. It assumes historical witnesses,
not current coverage; the latter is derived from the executed service. -/
theorem bounded_witnessed_storage (order : SnapshotOrder) (s : State)
    (posted : List (Nat × Nat × Nat)) (final : Write → Bool) (w : Write) (published : Write → Prop)
    (pub : published w) (isFinal : final w = true) (present : w ∈ s.writes) (writer : w.writer ∈ s.writers)
    (bound gated passGap upload latency : Nat)
    (sound : ∀ snap ∈ s.landed, SnapshotHistory.Sound published snap)
    (normalized : ∀ snap ∈ s.landed, Normalized s.writers snap)
    (witnesses : ∀ a ∈ w.holders, ∃ snap ∈ s.landed,
      snap.counts = true ∧ snap.covers w a ∧ snap.storedAt ≤ bound)
    (service : ∀ a ∈ w.holders, Service order s.writers s.landed w a
      (max gated (bound + day + 1)) passGap upload)
    (gate : (∀ reader ∈ s.writers, w.number ≤ postedPosition posted reader w.writer) ∨
      w.storedAt + month ≤ s.time)
    (scheduled : max gated (bound + day + 1) + 2 * passGap + upload ≤ s.time ∧
      s.time ≤ max gated (bound + day + 1) + 3 * passGap + upload)
    (removed : Nat → Write → Prop)
    (completion : ∀ v ∈ (retainWrites order s posted final).deleted, removed (s.time + latency) v) :
    ∃ time, time ≤ max gated (bound + day + 1) + 3 * passGap + upload + latency ∧ removed time w := by
  let ready := max gated (bound + day + 1)
  have mature : bound + day < ready := by dsimp [ready]; omega
  have ha := store_holds w
  obtain ⟨witness, member, _, covers, age⟩ := witnesses store ha
  have stored : w.storedAt ≤ bound := Nat.le_trans
    ((sound witness member).causal w pub (covers.1 ▸ ha) (by simpa [covers.1] using covers)) age
  have current : Covered (catalogCurrent order s.landed) w := by
    intro a ha
    have covered := service_covers order s.writers s.landed published w pub writer a bound ready
      passGap upload ha sound normalized stored mature (service a ha)
    obtain ⟨chosen, selected, cov⟩ := covered
    simpa [selected] using cov
  have due : eligible order s w posted (final w) = true := by
    rw [isFinal]
    simp only [eligible, decide_eq_true_eq, Eligible, Deletable]
    refine ⟨⟨current, trivial, gate⟩, ?_⟩
    intro a ha
    obtain ⟨snap, member, counts, cov, age⟩ := witnesses a ha
    exact ⟨snap, member, counts, cov, by dsimp [ready] at mature; omega⟩
  exact ⟨s.time + latency, by omega, completion w (due_write_deleted order s posted final w present due)⟩

end CovenIO.RetentionDeadline
