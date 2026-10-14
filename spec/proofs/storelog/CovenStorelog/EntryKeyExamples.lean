import CovenStorelog.EntryKeys
import CovenStorelog.CurrentKeyExamples

namespace CovenStorelog.EntryKeyExamples
open CurrentKeys
open EntryKeys (Metadata Sealed sealMetadata canOpen canRead)
open CurrentExamples (entry key)
open CurrentKeyExamples (payload copy)

set_option maxRecDepth 32768
set_option maxHeartbeats 1600000

/-- Creation, addition, removal: the first prefix with a former member and a
remaining owner. The next entry rotates; Ana's other device then registers. -/
def log : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .member "ben")
  | 2 => entry 0 0 [0, 1] (.removeMember 1)
  | 3 => entry 0 0 (List.range 3) (.rotateKey .store (key 1))
  | _ => entry 0 1 (List.range 4) (.addDevice 0 1)

def history : CurrentReplay.History := ⟨log, id, id⟩
def received (n : Nat) := entrySet (List.range n)
def replay (n : Nat) := CurrentReplay.resolve history 30 5 (received n)
def intros (n : Nat) := receivedIntroductions history 30 5 (received n)
def original : Introduction := ⟨0, .store, key 0⟩
def replacement : Introduction := ⟨3, .store, key 1⟩
def oldHeld := [payload original]
def bothHeld := [payload original, payload replacement]
def copies := [copy original 0, copy original 1, copy replacement 0]
def benKnows := [(payload original).bytes]

theorem history_valid : CurrentReplay.Valid log 5 :=
  validCheck_sound _ _ (by decide)

theorem online_history : Finality.Valid history.membership 5 := by
  refine ⟨history_valid, ?_, ?_, ?_⟩
  · intro a b _ _ same; exact same
  · intro w _; exact Nat.le_refl _
  · intro w a hw ha
    have ws : w = 0 ∨ w = 1 ∨ w = 2 ∨ w = 3 ∨ w = 4 := by omega
    have as : a = 0 ∨ a = 1 ∨ a = 2 ∨ a = 3 ∨ a = 4 := by omega
    rcases ws with rfl | rfl | rfl | rfl | rfl <;>
      rcases as with rfl | rfl | rfl | rfl | rfl <;> decide

theorem removal_kept :
    2 ∈ (replay 3).kept ∧ member (replay 3).state 1 = false ∧
    admin (replay 3).state 0 = true ∧
    acceptedCopy id (intros 3) 1 (copy original 1) = some (payload original) := by decide

/-- Ana can seal the removal before applying it, and its following rotation
after applying it. The same old key is exposed and unusable for app data. -/
theorem removal_and_rotation_sealable :
    EntryKeys.firstAttempt history 30 5 (received 2) id oldHeld (.entry (log 2)) =
      some ⟨.entry (log 2), payload original⟩ ∧
    EntryKeys.firstAttempt history 30 5 (received 3) id bothHeld (.entry (log 3)) =
      some ⟨.entry (log 3), payload original⟩ ∧
    CurrentReplay.authorized (replay 3).state (log 3) = true ∧
    exposed id (intros 3) (replay 3).state 0 copies original.identity = true ∧
    CurrentKeys.firstAttempt history 30 5 (received 3) id 0 (.complete copies) oldHeld .store =
      .pending .key := by decide

/-- The prefix has three entries. Neither positions nor clock publication
needs another store-log entry, so both can disclose metadata immediately. -/
theorem removed_reads_positions_and_clock :
    member (replay 3).state 1 = false ∧
    EntryKeys.firstAttempt history 30 5 (received 3) id oldHeld .positions =
      some ⟨.positions, payload original⟩ ∧
    EntryKeys.firstAttempt history 30 5 (received 3) id oldHeld .clock =
      some ⟨.clock, payload original⟩ ∧
    canRead benKnows true ⟨.positions, payload original⟩ = true ∧
    canRead benKnows true ⟨.clock, payload original⟩ = true := by decide

/-- A fourth entry is the first store-log entry authored after removal here.
Ben opens it while provider revocation is pending, despite Ana knowing removal. -/
theorem removed_reads_rotation :
    hadRead history.membership.log 3 2 = true ∧
    history.stored 2 < history.attempted 3 ∧
    3 ∈ (replay 4).kept ∧ member (replay 4).state 1 = false ∧
    EntryKeys.firstAttempt history 30 5 (received 3) id bothHeld (.entry (log 3)) =
      some ⟨.entry (log 3), payload original⟩ ∧
    canRead benKnows true ⟨.entry (log 3), payload original⟩ = true := by decide

example :
    member (replay 3).state 1 = false ∧
    EntryKeys.firstAttempt history 30 5 (received 3) id oldHeld .positions =
      some ⟨.positions, payload original⟩ ∧
    canRead benKnows true ⟨.positions, payload original⟩ = true := by decide

example :
    hadRead history.membership.log 3 2 = true ∧
    EntryKeys.firstAttempt history 30 5 (received 3) id bothHeld (.entry (log 3)) =
      some ⟨.entry (log 3), payload original⟩ ∧
    canRead benKnows true ⟨.entry (log 3), payload original⟩ = true := by decide

/-- Receipt of a rotation without its key does not stop old-key metadata.
Once the writer holds the replacement, every metadata kind uses it. -/
theorem rotation_changes_only_holders (object : Metadata) :
    sealMetadata id (intros 4) oldHeld object = some ⟨object, payload original⟩ ∧
    sealMetadata id (intros 4) bothHeld object = some ⟨object, payload replacement⟩ ∧
    canRead benKnows true ⟨object, payload original⟩ = true ∧
    canRead benKnows true ⟨object, payload replacement⟩ = false := by
  exact ⟨rfl, rfl, rfl, rfl⟩

/- Ana's registering device has received the rotation entry but has not
acquired its key; the rotating device's custody has not been erased. -/
theorem later_entry_still_readable_without_replacement :
    (log 4).device ≠ (log 3).device ∧
    4 ∈ (replay 5).kept ∧
    sealMetadata id (intros 4) oldHeld (.entry (log 4)) =
      some ⟨.entry (log 4), payload original⟩ ∧
    canRead benKnows true ⟨.entry (log 4), payload original⟩ = true := by decide

/-- Even an exposed newer key wins for metadata. App data still waits for
a non-exposed key. This adds knowledge, not a claim that sharing is authorized. -/
theorem later_disclosure (object : Metadata) :
    sealMetadata id (intros 4) bothHeld object = some ⟨object, payload replacement⟩ ∧
    canRead (benKnows ++ [(payload replacement).bytes]) true ⟨object, payload replacement⟩ = true ∧
    CurrentKeys.select id (intros 4) (replay 4).state 0
      (.complete (copy replacement 1 :: copies)) bothHeld .store = .pending .key := by
  exact ⟨rfl, rfl, by decide⟩

theorem cutoff_does_not_erase_keys (object : Metadata) :
    canRead benKnows false ⟨object, payload original⟩ = false ∧
    canOpen benKnows ⟨object, payload original⟩ = true := by
  exact ⟨rfl, rfl⟩

theorem custody_checks (object : Metadata) :
    sealMetadata id (intros 3) [payload replacement] object = none ∧
    sealMetadata id (intros 3) [⟨original.identity, (key 99).keyHash⟩] object = none ∧
    sealMetadata id (intros 3) [] object = none := by
  exact ⟨rfl, rfl, rfl⟩

end CovenStorelog.EntryKeyExamples
