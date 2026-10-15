import CovenStorage.KeyDelivery
import CovenStorage.Identity

/-! Ana's admin desktop (device 7) replaces Ben's S3 key. Ids come from one
counter starting at 100: attempt 100, the made key 101, then sealing
randomness and the reset's new device id. Slot 0 is the live database; a
`clone` is a backup and `drop 0` is restoring that backup over it. -/

namespace CovenStorage.KeyDelivery.Examples

def desktop : World := start [7] 100

/-- Backup after the key is made, before it is sealed; the live database
delivers; the backup is restored and re-seals with fresh randomness. -/
def beforeSealSteps : List Step :=
  [.begin 0, .mint 0, .clone 0, .sealKey 0, .deliver 0, .drop 0, .sealKey 1]

/-- §10's counted evidence cannot see the restore: nothing was written to a
numbered log since the backup, so its counters equal storage. The delivery
comparison alone resets the restored copy. The reset keeps no made key
(T267): no database journals key 101 and no kept entry records it, but it is
live at the provider until §13's reclaim takes it once device 7 is retired
(KeyListing `reset_key_reclaimed_once_retired`). -/
theorem restored_before_seal_orphans_key :
    let restored := run .random desktop beforeSealSteps
    let after := deliver restored 1
    checkIdentity 7 ⟨⟨0, 1⟩, 0, 0⟩ (some 7) (.ok ⟨⟨⟨0, 1⟩, 0, 0⟩, ⟨0, 0⟩, ⟨0, 0⟩, false, false⟩) = .send ∧
    restored.copies 1 = some ⟨7, some ⟨100, some 101, some [101, 103], false⟩⟩ ∧
    restored.storage 100 = some ⟨[101, 102], 7⟩ ∧
    Accounted restored ∧
    after.resets = [7] ∧
    after.copies 1 = some ⟨104, none⟩ ∧
    after.provider = [⟨101, 7⟩] ∧ after.recorded = [] ∧
    ¬ Accounted after := by decide

/-- The same restore with sealing bound to its inputs resends the stored bytes,
counts the delivery and records the key. -/
theorem bound_sealing_continues :
    let after := run .bound desktop (beforeSealSteps ++ [.deliver 1, .record 1])
    after.resets = [] ∧ after.recorded = [101] ∧ Accounted after := by decide

/-- A backup taken after sealing holds the fixed bytes: its retry counts the
stored delivery, as §10 expects of a restored copy. -/
theorem restored_after_seal_continues :
    let after := run .random desktop
      [.begin 0, .mint 0, .sealKey 0, .clone 0, .deliver 0, .drop 0, .deliver 1, .record 1]
    after.resets = [] ∧ after.recorded = [101] ∧ Accounted after := by decide

/-- A backup taken before the key is made: the restored copy's mint makes 103
beside 101 (§4). The stored delivery holds 101, so counting it would record
103 while Ben can install only 101; the comparison must refuse it. The reset
then leaves 103 unrecorded, whatever the sealer. -/
theorem restored_before_mint_orphans_key :
    let restored := run .bound desktop
      [.begin 0, .clone 0, .mint 0, .sealKey 0, .deliver 0, .drop 0, .mint 1, .sealKey 1]
    let after := deliver restored 1
    restored.storage 100 = some ⟨[101], 7⟩ ∧
    restored.provider = [⟨101, 7⟩, ⟨103, 7⟩] ∧
    restored.copies 1 = some ⟨7, some ⟨100, some 103, some [103], false⟩⟩ ∧
    after.resets = [7] ∧ after.provider = [⟨101, 7⟩, ⟨103, 7⟩] ∧ after.recorded = [] ∧
    ¬ Accounted after := by decide

end CovenStorage.KeyDelivery.Examples
