import CovenStorage.ReplacementRead
import CovenStorage.ReplacementViews
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

/-! Per-object views. Each replay below is a stand-in for store-log replay on
the entries that matter, set-determined like the real one. -/

def anaAdds5 : Path := .entry 0 9
def carolRemovesAna : Path := .entry 2 9
/-- Ben's restored copy registers id 5, replacing his phone 1. -/
def benReplaces1 : Registration := ⟨1, 5, 1⟩

/-- coven-sync's `a_dropped_replacement_restores_the_old_id_without_rejudging_recorded_pasts`:
Ana registers device 5 while Ben's restored copy, unaware, registers 5 too.
Replay keeps Ana's registration and drops Ben's replacement. -/
def pinnedReplay (es : List Path) : List Registration :=
  if anaAdds5 ∈ es then [] else if benReplaces1.entry ∈ es then [benReplaces1] else []

theorem pinned_set_determined : SetDetermined pinnedReplay := by
  intro a b same r
  simp only [pinnedReplay, same]

/-- Phone 1's entry after reading Ben's replacement, before Ana's registration arrived. -/
def afterReading : LogObject := ⟨1, 2, .entries, [benReplaces1.entry]⟩

/-- Its own view kept the replacement, so it is excluded, and stays excluded
once Ana's registration arrives and the full replay drops the replacement.
One kept set for all objects would accept it then. -/
theorem dropped_replacement_keeps_verdict :
    acceptsView id pinnedReplay [benReplaces1.entry] afterReading = false ∧
    acceptsView id pinnedReplay [benReplaces1.entry, anaAdds5] afterReading = false ∧
    pinnedReplay [benReplaces1.entry, anaAdds5] = [] ∧
    accepts (pinnedReplay [benReplaces1.entry, anaAdds5]) afterReading = true := by decide

/-- The other direction: Carol removes Ana concurrently with Ana registering
device 5, so the full replay drops Ana's registration and keeps Ben's
replacement. A view without Carol's removal keeps Ana's and drops Ben's. -/
def removalReplay (es : List Path) : List Registration :=
  if benReplaces1.entry ∈ es ∧ (anaAdds5 ∉ es ∨ carolRemovesAna ∈ es) then [benReplaces1]
  else []

theorem removal_set_determined : SetDetermined removalReplay := by
  intro a b same r
  simp only [removalReplay, same]

/-- Phone 1's write after reading both registrations, before Carol's removal. -/
def beforeEffective : LogObject := ⟨1, 3, .writes, [anaAdds5, benReplaces1.entry]⟩

/-- Phone 1's own replay kept no replacement, so it had not stopped, and the
write counts. One kept set for all objects, the full replay's, would
exclude this write although its copy could not yet know it was replaced. -/
theorem full_set_excludes_unstopped_write :
    removalReplay [anaAdds5, benReplaces1.entry] = [] ∧
    acceptsView id removalReplay [carolRemovesAna, anaAdds5, benReplaces1.entry]
      beforeEffective = true ∧
    removalReplay [carolRemovesAna, anaAdds5, benReplaces1.entry] = [benReplaces1] ∧
    accepts (removalReplay [carolRemovesAna, anaAdds5, benReplaces1.entry])
      beforeEffective = false := by decide

end CovenStorage.ReplacementRead.Examples
