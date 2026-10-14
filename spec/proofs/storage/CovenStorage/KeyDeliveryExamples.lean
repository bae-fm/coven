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
comparison alone resets the restored copy. After the reset no database
journals key 101 and no kept entry records it, but it is live at the
provider: §13's "leaves no key unrecorded" fails, and nothing will delete it. -/
theorem restored_before_seal_orphans_key :
    let restored := run .random desktop beforeSealSteps
    let after := deliver restored 1
    checkIdentity 7 ⟨⟨0, 1⟩, 0⟩ (some 7) (.ok ⟨⟨⟨0, 1⟩, 0⟩, ⟨0, 0⟩, ⟨0, 0⟩, false, false⟩) = .send ∧
    restored.copies 1 = some ⟨7, some ⟨100, some 101, some [101, 103], false⟩⟩ ∧
    restored.storage 100 = some ⟨[101, 102], 7⟩ ∧
    Accounted restored ∧
    after.resets = [7] ∧
    after.copies 1 = some ⟨104, none⟩ ∧
    after.provider = [⟨101, 100⟩] ∧ after.recorded = [] ∧
    ¬ Accounted after := by decide

example : ¬ Accounted (run .random desktop (beforeSealSteps ++ [.deliver 1])) := by decide

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

/-- A backup taken before the key is made: the restored copy's mint deletes
key 101 under the attempt's name (§4) and makes 103. The stored delivery holds
the deleted key, so counting it would leave Ben unable to install the
recorded one; the comparison must refuse it. The reset then orphans 103,
whatever the sealer: the loss is the reset discarding a made key. -/
theorem restored_before_mint_orphans_key :
    let restored := run .bound desktop
      [.begin 0, .clone 0, .mint 0, .sealKey 0, .deliver 0, .drop 0, .mint 1, .sealKey 1]
    let after := deliver restored 1
    restored.storage 100 = some ⟨[101], 7⟩ ∧
    restored.provider = [⟨103, 100⟩] ∧
    restored.copies 1 = some ⟨7, some ⟨100, some 103, some [103], false⟩⟩ ∧
    Accounted restored ∧
    after.resets = [7] ∧ after.provider = [⟨103, 100⟩] ∧ after.recorded = [] ∧
    ¬ Accounted after := by decide

end CovenStorage.KeyDelivery.Examples
