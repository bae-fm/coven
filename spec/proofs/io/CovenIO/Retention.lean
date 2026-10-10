import CovenIO.Storage

namespace CovenIO

def month : Nat := 30 * 24 * 60 * 60

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
  positions : List (Nat × Nat)
  deriving DecidableEq, Repr

def Snapshot.position (s : Snapshot) (writer : Nat) : Nat :=
  ((s.positions.filter (fun p => p.1 == writer)).map Prod.snd).foldl max 0

def Snapshot.score (s : Snapshot) : Nat := (s.positions.map Prod.snd).sum

/-- Lists are scoped to one audience. The path tie-break compares writer
and snapshot number; the audience component is common. -/
def newer (a b : Snapshot) : Bool :=
  decide (b.score < a.score ∨ (a.score = b.score ∧
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

def Covered (snapshots : List Snapshot) (w : Write) : Prop :=
  ∀ a ∈ w.audiences, ∃ s ∈ snapshots, s.covers w a

def Dominates (new old : Snapshot) : Prop :=
  new.audience = old.audience ∧ ∀ w, old.position w ≤ new.position w

structure Retention where
  snapshots : List Snapshot
  deleted : List Write
  deriving DecidableEq, Repr

def Retention.Valid (s : Retention) : Prop := ∀ w ∈ s.deleted, Covered s.snapshots w

/-- Posted positions are conservative fully realized positions, not discovery
bounds. The caller supplies finality, authority and boundary eligibility. -/
def Deletable (snapshots : List Snapshot) (active : List Nat)
    (posted : Nat → Nat → Nat) (time : Nat) (final : Prop) (w : Write) : Prop :=
  Covered snapshots w ∧ final ∧
    ((∀ reader ∈ active, w.number ≤ posted reader w.writer) ∨ w.storedAt + month ≤ time)

inductive RetainStep : Retention → Retention → Prop
  | publish (s : Retention) (snap : Snapshot) :
      RetainStep s { s with snapshots := snap :: s.snapshots }
  | deleteWrite (s : Retention) (w : Write) (active : List Nat)
      (posted : Nat → Nat → Nat) (time : Nat) (final : Prop)
      (eligible : Deletable s.snapshots active posted time final w) :
      RetainStep s { s with deleted := w :: s.deleted }
  | deleteSnapshot (s : Retention) (old new : Snapshot)
      (retained : new ∈ s.snapshots) (different : new ≠ old)
      (covers : Dominates new old) (newer : old.score ≤ new.score)
      (final : Prop) (usable : final) :
      RetainStep s { s with snapshots := s.snapshots.filter (· != old) }

theorem retention_preserves {a b : Retention} (h : RetainStep a b)
    (valid : a.Valid) : b.Valid := by
  cases h with
  | publish snap =>
      intro w hw audience ha
      obtain ⟨old, hm, hc⟩ := valid w hw audience ha
      exact ⟨old, List.mem_cons_of_mem _ hm, hc⟩
  | deleteWrite w active posted time final eligible =>
      intro v hv
      rcases List.mem_cons.mp hv with rfl | hv
      · exact eligible.1
      · exact valid v hv
  | deleteSnapshot old new retained different dominates _ _ _ =>
      intro w hw audience ha
      obtain ⟨cover, hm, hc⟩ := valid w hw audience ha
      by_cases eq : cover = old
      · subst cover
        exact ⟨new, List.mem_filter.mpr ⟨retained, by simpa using different⟩,
          dominates.1.trans hc.1, Nat.le_trans hc.2 (dominates.2 _)⟩
      · exact ⟨cover, List.mem_filter.mpr ⟨hm, by simpa using eq⟩, hc⟩

inductive RetainRun : Retention → Prop
  | initial : RetainRun ⟨[], []⟩
  | step {a b : Retention} : RetainRun a → RetainStep a b → RetainRun b

theorem deleted_write_covered {s : Retention} (h : RetainRun s) : s.Valid := by
  induction h with
  | initial => intro w hw; cases hw
  | step _ step ih => exact retention_preserves step ih

/-- The qualifying checkpoint consumed everything stored through S. For an
unconsumed write this gives S ≤ storedAt. The deadline must hold at deletion,
not merely when the pass began. -/
theorem recent_write_protected (snapshots : List Snapshot) (active : List Nat)
    (posted : Nat → Nat → Nat) (time checkpoint reader consumed : Nat) (final : Prop) (w : Write)
    (member : reader ∈ active) (honest : posted reader w.writer ≤ consumed)
    (unconsumed : consumed < w.number) (afterCheckpoint : checkpoint ≤ w.storedAt)
    (recent : time < checkpoint + month) :
    ¬ Deletable snapshots active posted time final w := by
  rintro ⟨_, _, readers | aged⟩
  · have := readers reader member; omega
  · omega

/-- A usable selection must dominate ALL retained coverage, not merely have
the greatest sum of positions. This premise is not implied by §15's ordering. -/
def CompleteSelection (snapshots selected : List Snapshot) : Prop :=
  ∀ s ∈ snapshots, ∃ chosen ∈ selected, Dominates chosen s

theorem selection_covers_deleted (s : Retention) (valid : s.Valid)
    (selected : List Snapshot) (complete : CompleteSelection s.snapshots selected)
    (w : Write) (deleted : w ∈ s.deleted) : Covered selected w := by
  intro a ha
  obtain ⟨snap, hs, hc⟩ := valid w deleted a ha
  obtain ⟨chosen, hm, hd⟩ := complete snap hs
  exact ⟨chosen, hm, hd.1.trans hc.1, Nat.le_trans hc.2 (hd.2 _)⟩

/-- Checkpoints are published only with fully realized history AND a
confirmed positions post. File waits do not occur in these prerequisites. -/
def checkpoint (old : Option Nat) (sample : Nat) (history post : Bool) : Option Nat :=
  if history && post then some sample else old

theorem unresolved_preserves_checkpoint (old : Option Nat) (sample : Nat) (post : Bool) :
    checkpoint old sample false post = old := by simp [checkpoint]

def needsSnapshots (saved : Option Nat) (observed : Nat) : Bool :=
  match saved with | none => true | some s => decide (s + month ≤ observed)

theorem exact_boundary_discovers (s : Nat) : needsSnapshots (some s) (s + month) = true := by
  simp [needsSnapshots]

end CovenIO
