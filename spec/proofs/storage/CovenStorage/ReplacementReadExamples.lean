import CovenStorage.ReplacementRead
import CovenStorage.Examples

namespace CovenStorage.ReplacementRead.Examples
open CovenStorage.Examples

def replaced : Registration := ⟨7, 8, 1⟩
def replacedAgain : Registration := ⟨7, 9, 1⟩
def firstWrite : LogObject := ⟨7, 1, .writes, [.entry 7 1]⟩
def racingWrite : LogObject := ⟨7, 2, .writes, [.entry 7 1]⟩

/-- The first replacement lands between the two old-id writes. The old
copy checked before that replacement and has not read it when it commits 2. -/
def racingStorage : Storage :=
  create (create (create emptyStorage firstWrite.path [42] 10)
    replaced.entry [7] 11) racingWrite.path [43] 12

theorem racing_upload_kept :
    gate (.active pending [.entry 7 1]) .write (some 7)
      (.ok { registeredEvidence with listed := pending.reserved }) = .send ∧
    (commit (.active pending [.entry 7 1]) [43]).2 = some racingWrite ∧
    racingStorage firstWrite.path = some ⟨[42], 10⟩ ∧
    racingStorage replaced.entry = some ⟨[7], 11⟩ ∧
    racingStorage racingWrite.path = some ⟨[43], 12⟩ ∧
    closedLog ⟨1, 1⟩ .writes 2 = .awaitsReplacement ∧
    load [replaced] [firstWrite, racingWrite] = [racingWrite.path, firstWrite.path] ∧
    load [replacedAgain, replaced, replacedAgain] [racingWrite, firstWrite, racingWrite] =
      [firstWrite.path, racingWrite.path] := by decide

theorem restored_and_live_copies_stop :
    gate (.active installed []) .write (some 7)
      (.ok { registeredEvidence with listed := pending.reserved }) = .reset .storageAhead ∧
    gate (.active installed []) .entry (some 7)
      (.ok { registeredEvidence with replaced := true }) = .reset .replaced ∧
    observe (.active pending [.entry 7 1]) replaced 9 = .resetting replacedAgain .replaced ∧
    (commit (observe (.active pending [.entry 7 1]) replaced 9) [44]).2 = none ∧
    gate (observe (.active pending [.entry 7 1]) replaced 9) .file (some 7)
      (.ok registeredEvidence) = .reset .replaced ∧
    (commit (finish replacedAgain (load [replaced] [firstWrite, racingWrite])
      [replaced.entry, replacedAgain.entry]) [44]).2 =
      some ⟨9, 1, .writes, [replaced.entry, replacedAgain.entry]⟩ := by decide

theorem post_read_objects_rejected :
    accepts [replaced] ⟨7, 2, .writes, [replaced.entry]⟩ = false ∧
    accepts [replaced] ⟨7, 2, .entries, [replaced.entry]⟩ = false ∧
    accepts [replaced, replacedAgain] ⟨7, 2, .writes, [replacedAgain.entry]⟩ = false ∧
    accepts [replaced, replacedAgain] ⟨8, 1, .writes, [replaced.entry]⟩ = true := by decide

/-- A restore that retains custody need leave no evidence for §10's check.
There is one backed-up queued write, no earlier attempted or stored write,
and one post-restore send under the old id. -/
def restoredPending : BackupRun := restoreDatabase ⟨pending, pending, some 7, []⟩

theorem restored_copy_can_send :
    checkIdentity restoredPending.live.device restoredPending.live.reserved
      restoredPending.custody (.ok registeredEvidence) = .send ∧
    (backupAttempt restoredPending 9 2 (.ok registeredEvidence)).encrypted =
      [⟨⟨.write 7 1, [42]⟩, 9, 2⟩] := by decide

example : (backupAttempt restoredPending 9 2 (.ok registeredEvidence)).encrypted =
    [⟨⟨.write 7 1, [42]⟩, 9, 2⟩] := by decide

/-- One committed but unstored write is absent after reset. The variant has
no log ends, and deliberately follows the same discard rule. -/
theorem unsent_loss :
    firstWrite.path ∈ pending.applied ∧
    firstWrite.path ∉ (loadDevice replaced.fresh replaced.number (load [replaced] [])).applied := by
  decide

example : firstWrite.path ∈ pending.applied ∧
    firstWrite.path ∉ (loadDevice replaced.fresh replaced.number (load [replaced] [])).applied := by
  decide

theorem colliding_copies_lose_one_value :
    checkIdentity 7 pending.reserved (some 7) (.ok registeredEvidence) = .send ∧
    checkIdentity 7 secondCopy.reserved (some 7) (.ok registeredEvidence) = .send ∧
    create (create emptyStorage firstWrite.path [42] 10) firstWrite.path [43] 11
      firstWrite.path = some ⟨[42], 10⟩ ∧
    compareOccupied [43] (.ok (some ⟨[42], 10⟩)) = .reset ∧
    load [replaced] [firstWrite, firstWrite] = [firstWrite.path] := by decide

example : create (create emptyStorage firstWrite.path [42] 10) firstWrite.path [43] 11
    firstWrite.path = some ⟨[42], 10⟩ ∧
    compareOccupied [43] (.ok (some ⟨[42], 10⟩)) = .reset := by decide

end CovenStorage.ReplacementRead.Examples
