import CovenStorage.Objects

/-! Conditional publication of one floor. Native staging is invisible; a provider
may commit with its last part or with a separate completion request. Both are
represented by `publish`, after all bytes have arrived. Atomic comparison with
the observed revision is a provider premise, not a theorem about its service. -/
namespace CovenStorage.FloorUpload

structure Store where
  revision : Nat
  bytes : Option Bytes
  deriving DecidableEq, Repr

inductive Condition where
  | absent
  | observed (revision : Nat)
  deriving DecidableEq, Repr

def Condition.holds (condition : Condition) (store : Store) : Bool :=
  match condition with
  | .absent => store.bytes.isNone
  | .observed revision => store.bytes.isSome && store.revision == revision

structure Upload where
  condition : Condition
  total : Nat
  received : Bytes
  deriving DecidableEq, Repr

/-- Receiving more bytes preserves the original publication condition. -/
def receive (upload : Upload) (part : Bytes) : Upload :=
  { upload with received := upload.received ++ part }

inductive Outcome where
  | stored
  | incomplete
  | conflict
  deriving DecidableEq, Repr

/-- The complete bytes become the one floor only while the original condition
holds. A fresh revision prevents a later retry from replacing another write. -/
def publish (store : Store) (upload : Upload) : Store × Outcome :=
  if upload.total = 0 ∨ upload.received.length ≠ upload.total then (store, .incomplete)
  else if upload.condition.holds store then
    (⟨store.revision + 1, some upload.received⟩, .stored)
  else (store, .conflict)

theorem receive_preserves_condition (upload : Upload) (part : Bytes) :
    (receive upload part).condition = upload.condition := rfl

theorem incomplete_keeps_floor (store : Store) (upload : Upload)
    (incomplete : upload.received.length ≠ upload.total) :
    publish store upload = (store, .incomplete) := by
  simp [publish, incomplete]

theorem stale_keeps_floor (store : Store) (upload : Upload)
    (complete : upload.received.length = upload.total) (nonempty : upload.total ≠ 0)
    (stale : upload.condition.holds store = false) :
    publish store upload = (store, .conflict) := by
  simp [publish, complete, nonempty, stale]

theorem stored_whole (store : Store) (upload : Upload)
    (stored : (publish store upload).2 = .stored) :
    upload.received.length = upload.total ∧ upload.total ≠ 0 ∧
      upload.condition.holds store = true ∧
      (publish store upload).1.bytes = some upload.received := by
  unfold publish at stored ⊢
  split
  · simp_all
  · split
    · simp_all
    · simp_all

/-- Once one writer succeeds, the same absent/revision condition cannot succeed
again, regardless of the second writer's bytes or transfer progress. -/
theorem one_winner (store : Store) (first second : Upload)
    (same : second.condition = first.condition)
    (stored : (publish store first).2 = .stored) :
    (publish (publish store first).1 second).2 ≠ .stored := by
  have allowed := (stored_whole store first stored).2.2.1
  have after : (publish store first).1 = ⟨store.revision + 1, some first.received⟩ := by
    unfold publish at stored ⊢
    split <;> simp_all
  rw [after]
  unfold publish
  split
  · simp
  · have refused : second.condition.holds ⟨store.revision + 1, some first.received⟩ = false := by
      rw [same]
      cases hc : first.condition with
      | absent => simp [Condition.holds]
      | observed revision =>
          simp [Condition.holds, hc] at allowed
          simp [Condition.holds, allowed.2]
    simp [refused]

end CovenStorage.FloorUpload
