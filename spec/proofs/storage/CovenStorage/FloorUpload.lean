import CovenStorage.Objects

/-! Conditional replacement of the floor pointer, one request. The pointer is a
small record: it fits one provider request, so there are no parts and no staging
to model. Atomic comparison with the observed revision is a provider premise,
not a theorem about its service. -/
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

/-- A pointer write: the condition its writer read the floor under, and its
complete bytes. -/
structure Write where
  condition : Condition
  bytes : Bytes
  deriving DecidableEq, Repr

inductive Outcome where
  | stored
  | conflict
  deriving DecidableEq, Repr

/-- The bytes become the one pointer only while the condition holds. A fresh
revision prevents a later retry from replacing another write. -/
def publish (store : Store) (write : Write) : Store × Outcome :=
  if write.bytes = [] then (store, .conflict)
  else if write.condition.holds store then
    (⟨store.revision + 1, some write.bytes⟩, .stored)
  else (store, .conflict)

theorem stale_keeps_floor (store : Store) (write : Write) (nonempty : write.bytes ≠ [])
    (stale : write.condition.holds store = false) :
    publish store write = (store, .conflict) := by
  simp [publish, nonempty, stale]

theorem stored_whole (store : Store) (write : Write)
    (stored : (publish store write).2 = .stored) :
    write.bytes ≠ [] ∧ write.condition.holds store = true ∧
      (publish store write).1.bytes = some write.bytes := by
  unfold publish at stored ⊢
  split
  · simp_all
  · split
    · simp_all
    · simp_all

/-- Once one writer succeeds, the same absent/revision condition cannot succeed
again, regardless of the second writer's bytes. -/
theorem one_winner (store : Store) (first second : Write)
    (same : second.condition = first.condition)
    (stored : (publish store first).2 = .stored) :
    (publish (publish store first).1 second).2 ≠ .stored := by
  have allowed := (stored_whole store first stored).2.1
  have after : (publish store first).1 = ⟨store.revision + 1, some first.bytes⟩ := by
    unfold publish at stored ⊢
    split <;> simp_all
  rw [after]
  unfold publish
  split
  · simp
  · have refused : second.condition.holds ⟨store.revision + 1, some first.bytes⟩ = false := by
      rw [same]
      cases hc : first.condition with
      | absent => simp [Condition.holds]
      | observed revision =>
          simp [Condition.holds, hc] at allowed
          simp [Condition.holds, allowed.2]
    simp [refused]

end CovenStorage.FloorUpload
