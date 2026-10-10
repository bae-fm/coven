import CovenIO.Storage

namespace CovenIO

def day : Nat := 24 * 60 * 60
def month : Nat := 30 * day
def freshFor : Nat := 5 * 60

structure Write where
  writer : Nat
  number : Nat
  storedAt : Nat
  audiences : List Nat
  deriving DecidableEq, Repr

structure Snapshot where
  audience : Nat
  writer : Nat
  number : Nat
  storedAt : Nat
  positions : List (Nat × Nat)
  deriving DecidableEq, Repr

def Snapshot.position (s : Snapshot) (writer : Nat) : Nat :=
  ((s.positions.filter (fun p => p.1 == writer)).map Prod.snd).foldl max 0

/-- §15: latest storage time, then smaller path within one audience. -/
def newer (a b : Snapshot) : Bool :=
  decide (b.storedAt < a.storedAt ∨ (a.storedAt = b.storedAt ∧
    (a.writer < b.writer ∨ (a.writer = b.writer ∧ a.number < b.number))))

def newest : List Snapshot → Option Snapshot
  | [] => none
  | s :: rest => match newest rest with
    | none => some s
    | some other => some (if newer s other then s else other)

def advances (s : Snapshot) (localPosition : Nat → Nat) : Bool :=
  s.positions.any fun p => decide (localPosition p.1 < p.2)

def Snapshot.covers (s : Snapshot) (w : Write) (a : Nat) : Prop :=
  s.audience = a ∧ w.number ≤ s.position w.writer

instance (s : Snapshot) (w : Write) (a : Nat) : Decidable (s.covers w a) :=
  inferInstanceAs (Decidable (s.audience = a ∧ w.number ≤ s.position w.writer))

def Dominates (new old : Snapshot) : Prop :=
  new.audience = old.audience ∧ ∀ w, old.position w ≤ new.position w

abbrev Current := Nat → Option Snapshot

def Covered (current : Current) (w : Write) : Prop :=
  ∀ a ∈ w.audiences, ∃ s, current a = some s ∧ s.covers w a

/-- The capture covers the current snapshot. Checking this at capture/send
and checking it at publication are different contracts. -/
def CanSnapshot (current : Current) (snap : Snapshot) : Prop :=
  ∀ old, current snap.audience = some old → Dominates snap old

def publishCurrent (current : Current) (snap : Snapshot) : Current :=
  fun a => if a = snap.audience then
    match current a with
    | none => some snap
    | some old => some (if newer snap old then snap else old)
  else current a

/-- The incremental current map is the listing's storage-time selection. -/
def catalogCurrent (snapshots : List Snapshot) : Current :=
  fun audience => newest (snapshots.filter (fun s => s.audience == audience))

theorem current_matches_listing (snapshots : List Snapshot) (snap : Snapshot) :
    catalogCurrent (snap :: snapshots) = publishCurrent (catalogCurrent snapshots) snap := by
  funext audience
  by_cases same : audience = snap.audience
  · subst audience
    simp only [catalogCurrent, List.filter_cons, beq_self_eq_true, ↓reduceIte,
      newest, publishCurrent]
  · simp [catalogCurrent, publishCurrent, same, Ne.symm same]

/-- Older retained snapshots do not affect this projection. Removing an old
snapshot leaves the current one; removing the current one is not permitted. -/
structure Retention where
  current : Current
  deleted : List Write

def Retention.Valid (s : Retention) : Prop := ∀ w ∈ s.deleted, Covered s.current w

def Deletable (current : Current) (active : List Nat)
    (posted : Nat → Nat → Nat) (time : Nat) (final : Prop) (w : Write) : Prop :=
  Covered current w ∧ final ∧
    ((∀ reader ∈ active, w.number ≤ posted reader w.writer) ∨ w.storedAt + month ≤ time)

/-- This relation enforces the coverage rule at publication. The separate
SnapshotPublication relation checks it at send and allows concurrent requests. -/
inductive RetainStep : Retention → Retention → Prop
  | publish (s : Retention) (snap : Snapshot) (covered : CanSnapshot s.current snap) :
      RetainStep s { s with current := publishCurrent s.current snap }
  | deleteWrite (s : Retention) (w : Write) (active : List Nat)
      (posted : Nat → Nat → Nat) (time : Nat) (final : Prop)
      (eligible : Deletable s.current active posted time final w) :
      RetainStep s { s with deleted := w :: s.deleted }
  | discardOld (s : Retention) : RetainStep s s

def CoverageGrows (before after : Current) : Prop :=
  ∀ a old, before a = some old → ∃ next, after a = some next ∧ Dominates next old

theorem publication_coverage_grows (current : Current) (snap : Snapshot)
    (covered : CanSnapshot current snap) : CoverageGrows current (publishCurrent current snap) := by
  intro a old found
  by_cases same : a = snap.audience
  · subst a
    cases chosen : newer snap old
    · exact ⟨old, by simp [publishCurrent, found, chosen], rfl, fun _ => Nat.le_refl _⟩
    · exact ⟨snap, by simp [publishCurrent, found, chosen], covered old found⟩
  · exact ⟨old, by simp [publishCurrent, same, found], rfl, fun _ => Nat.le_refl _⟩

theorem coverage_trans {a b c : Current} (ab : CoverageGrows a b) (bc : CoverageGrows b c) :
    CoverageGrows a c := by
  intro audience old found
  obtain ⟨middle, hm, dm⟩ := ab audience old found
  obtain ⟨last, hl, dl⟩ := bc audience middle hm
  exact ⟨last, hl, dl.1.trans dm.1, fun w => Nat.le_trans (dm.2 w) (dl.2 w)⟩

theorem covered_of_grows {a b : Current} (grows : CoverageGrows a b) (w : Write)
    (covered : Covered a w) : Covered b w := by
  intro audience ha
  obtain ⟨old, found, hc⟩ := covered audience ha
  obtain ⟨next, hn, dominates⟩ := grows audience old found
  exact ⟨next, hn, dominates.1.trans hc.1, Nat.le_trans hc.2 (dominates.2 _)⟩

theorem current_never_shrinks {a b : Retention} (step : RetainStep a b) :
    CoverageGrows a.current b.current := by
  cases step with
  | publish snap covered => exact publication_coverage_grows _ snap covered
  | deleteWrite | discardOld =>
      exact fun _ old found => ⟨old, found, rfl, fun _ => Nat.le_refl _⟩

inductive RetainSteps : Retention → Retention → Prop
  | refl (s : Retention) : RetainSteps s s
  | step {a b c : Retention} : RetainSteps a b → RetainStep b c → RetainSteps a c

theorem coverage_never_shrinks {a b : Retention} (steps : RetainSteps a b) :
    CoverageGrows a.current b.current := by
  induction steps with
  | refl => exact fun _ old found => ⟨old, found, rfl, fun _ => Nat.le_refl _⟩
  | step _ next ih => exact coverage_trans ih (current_never_shrinks next)

theorem retention_preserves {a b : Retention} (step : RetainStep a b)
    (valid : a.Valid) : b.Valid := by
  cases step with
  | publish snap covered =>
      exact fun w hw => covered_of_grows (publication_coverage_grows _ snap covered) w (valid w hw)
  | deleteWrite w active posted time final eligible =>
      intro v hv
      rcases List.mem_cons.mp hv with rfl | hv
      · exact eligible.1
      · exact valid v hv
  | discardOld => exact valid

inductive RetainRun : Retention → Prop
  | initial : RetainRun ⟨fun _ => none, []⟩
  | step {a b : Retention} : RetainRun a → RetainStep a b → RetainRun b

theorem deleted_write_covered {s : Retention} (h : RetainRun s) : s.Valid := by
  induction h with
  | initial => intro w hw; cases hw
  | step _ step ih => exact retention_preserves step ih

/-- Selecting the current snapshot needs no separate dominance premise. -/
theorem selection_covers_deleted (s : Retention) (run : RetainRun s)
    (w : Write) (deleted : w ∈ s.deleted) : Covered s.current w :=
  deleted_write_covered run w deleted

/-- The reader is active and its posted position never claims unconsumed work. -/
theorem recent_write_protected (current : Current) (active : List Nat)
    (posted : Nat → Nat → Nat) (time checkpoint reader consumed : Nat) (final : Prop) (w : Write)
    (member : reader ∈ active) (honest : posted reader w.writer ≤ consumed)
    (unconsumed : consumed < w.number) (afterCheckpoint : checkpoint ≤ w.storedAt)
    (recent : time < checkpoint + month) :
    ¬ Deletable current active posted time final w := by
  rintro ⟨_, _, readers | aged⟩
  · have := readers reader member; omega
  · omega

def checkpoint (old : Option Nat) (sample : Nat) (history post : Bool) : Option Nat :=
  if history && post then some sample else old

theorem unresolved_preserves_checkpoint (old : Option Nat) (sample : Nat) (post : Bool) :
    checkpoint old sample false post = old := by simp [checkpoint]

/-- §15: evaluate at every miss, with monotonic elapsed time including sleep. -/
def needsSnapshots (saved : Option Nat) (observed elapsed : Nat) : Bool :=
  match saved with | none => true | some s => decide (s + 29 * day ≤ observed + elapsed)

theorem exact_boundary_discovers (s : Nat) : needsSnapshots (some s) (s + 29 * day) 0 = true := by
  simp [needsSnapshots]

/-- The one-day margin accommodates at most a day of sample age. Relating
provider clock advance to elapsed time is explicit, not implied by monotonicity. -/
theorem miss_before_retention (checkpoint observed elapsed storageTime : Nat)
    (recent : needsSnapshots (some checkpoint) observed elapsed = false)
    (clockBound : storageTime ≤ observed + elapsed + day) : storageTime < checkpoint + month := by
  simp only [needsSnapshots, decide_eq_false_iff_not, Nat.not_le] at recent
  unfold month day at *
  omega

namespace SnapshotPublication

structure State where
  retention : Retention
  pending : List Snapshot

inductive Step : State → State → Prop
  | prepare (s : State) (snap : Snapshot) (covered : CanSnapshot s.retention.current snap) :
      Step s { s with pending := snap :: s.pending }
  | land (s : State) (snap : Snapshot) (time : Nat) (pending : snap ∈ s.pending) :
      Step s ⟨{ s.retention with
        current := publishCurrent s.retention.current { snap with storedAt := time } },
        s.pending.filter (· != snap)⟩
  | deleteWrite (s : State) (w : Write) (active : List Nat) (posted : Nat → Nat → Nat)
      (time : Nat) (final : Prop)
      (eligible : Deletable s.retention.current active posted time final w) :
      Step s { s with retention := { s.retention with deleted := w :: s.retention.deleted } }

inductive Run : State → Prop
  | initial : Run ⟨⟨fun _ => none, []⟩, []⟩
  | step {a b : State} : Run a → Step a b → Run b

end SnapshotPublication
end CovenIO
