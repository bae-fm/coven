import CovenStorage.Files
import CovenStorage.Clocks

namespace CovenStorage.Examples

deriving instance DecidableEq for Except

def registeredEvidence : Evidence := ⟨⟨⟨0, 1⟩, 0⟩, ⟨0, 0⟩, ⟨0, 0⟩, false, false⟩
def installed : Local := ⟨7, ⟨⟨0, 1⟩, 0⟩, [], []⟩
def pending : Local := commitWrite installed [42]
def replacement : Replacement := ⟨7, 8, ⟨0, 1⟩, 1, .identityMismatch⟩

/-- One committed write, never uploaded, suffices. §10 explicitly orders
this loss and its reset notice; it contradicts unqualified no-loss. -/
theorem unsent_restore_loss :
    Path.write 7 1 ∈ pending.applied ∧ pending.queue ≠ [] ∧
    checkIdentity 7 pending.reserved none (.ok registeredEvidence) = .reset .identityMismatch ∧
    writable (beginReset pending replacement) = false ∧
    (finishReset replacement []).queue = [] ∧
    Path.write 7 1 ∉ (finishReset replacement []).applied ∧
    replacement.reason = .identityMismatch := by decide

example : Path.write 7 1 ∈ pending.applied ∧
    Path.write 7 1 ∉ (finishReset replacement []).applied := by decide

theorem restore_from_deleted_log_detected :
    checkIdentity 7 ⟨⟨5, 1⟩, 0⟩ (some 7)
      (.ok { registeredEvidence with covered := ⟨7, 1⟩ }) = .reset .storageAhead ∧
    checkIdentity 7 ⟨⟨5, 1⟩, 0⟩ (some 7)
      (.ok { registeredEvidence with posted := ⟨7, 1⟩ }) = .reset .storageAhead ∧
    checkIdentity 7 ⟨⟨5, 1⟩, 2⟩ (some 7)
      (.ok { registeredEvidence with listed := ⟨⟨5, 1⟩, 3⟩ }) = .reset .storageAhead := by decide

/-- A successful upload whose acknowledgement was lost is still reserved. -/
theorem outstanding_reservation_is_not_stale :
    checkIdentity 7 pending.reserved (some 7)
      (.ok { registeredEvidence with listed := pending.reserved }) = .send ∧
    compareOccupied [42] (.ok (some ⟨[42], 10⟩)) = .stored ∧
    compareOccupied [42] (.error .network) = .pending .network := by decide

/-- One live installation; a backup before reservation, with no format or
schema change: attempt A, restore, then commit B into the forgotten slot.
The first attempt never lands, while non-backed-up custody survives. -/
def beforeReservation : BackupRun := ⟨pending, installed, some 7, []⟩
def failedWrite : BackupRun := backupAttempt beforeReservation 9 2 (.ok registeredEvidence)
def rolledBackWrite : BackupRun := restoreDatabase failedWrite
def newWrite : BackupRun := { rolledBackWrite with live := commitWrite rolledBackWrite.live [43] }
def retriedSlot : BackupRun := backupAttempt newWrite 9 2 (.ok registeredEvidence)

theorem single_live_restore_nonce_separation :
    rolledBackWrite.live.reserved.logs.writes = 0 ∧
    rolledBackWrite.custody = some 7 ∧
    newWrite.live.reserved.logs.writes = 1 ∧
    checkIdentity newWrite.live.device newWrite.live.reserved newWrite.custody
      (.ok registeredEvidence) = .send ∧
    retriedSlot.encrypted = [⟨⟨.write 7 1, [42]⟩, 9, 2⟩, ⟨⟨.write 7 1, [43]⟩, 9, 2⟩] ∧
    (prepare 9 2 (.untried ⟨.write 7 1, [42]⟩)).nonce 0 0 ≠
      (prepare 9 2 (.untried ⟨.write 7 1, [43]⟩)).nonce 0 0 ∧
    (prepare 9 2 (.untried ⟨.write 7 1, [42]⟩)).draft.plaintext ≠
      (prepare 9 2 (.untried ⟨.write 7 1, [43]⟩)).draft.plaintext := by decide

example : retriedSlot.encrypted =
    [⟨⟨.write 7 1, [42]⟩, 9, 2⟩, ⟨⟨.write 7 1, [43]⟩, 9, 2⟩] := by decide

/-- Conversion of an apparently untried backup changes the nonce with its
plaintext. A retained first-attempt flag still fixes the original attempt. -/
theorem rollback_conversion_changes_plaintext :
    let original := prepare 9 2 (.untried ⟨.write 7 1, [42]⟩)
    let converted := prepare 9 2 (convert (fun _ => [43]) (.untried ⟨.write 7 1, [42]⟩))
    original.nonce 0 0 ≠ converted.nonce 0 0 ∧
    original.draft.plaintext ≠ converted.draft.plaintext ∧
    prepare 9 2 (convert (fun _ => [43]) (.tried original)) = original := by decide

/-- Dropping content from the nonce inputs makes the same rollback history
reuse a nonce for different plaintexts. This is why D11 binds content. -/
theorem content_free_rollback_nonce_reuse :
    let first := prepare 9 2 (.untried ⟨.write 7 1, [42]⟩)
    let second := prepare 9 2 (.untried ⟨.write 7 1, [43]⟩)
    retriedSlot.encrypted = [first, second] ∧
    (first.nonce 0 0).position = (second.nonce 0 0).position ∧
    first.draft.plaintext ≠ second.draft.plaintext := by decide

example :
    let first := prepare 9 2 (.untried ⟨.write 7 1, [42]⟩)
    let second := prepare 9 2 (.untried ⟨.write 7 1, [43]⟩)
    (first.nonce 0 0).position = (second.nonce 0 0).position ∧
    first.draft.plaintext ≠ second.draft.plaintext := by decide

/-- Two unequal sealing attempts are necessary for the content-free failure. -/
theorem fewer_than_two_attempts_no_reuse (history : List Attempt) (h : history.length < 2) :
    ∀ a ∈ history, ∀ b ∈ history, a = b := by
  cases history with
  | nil => simp
  | cons x xs =>
    cases xs with
    | nil => simp
    | cons y ys => simp only [List.length_cons] at h; omega

def secondCopy : Local := commitWrite installed [43]

/-- §10's stated race: both copies check before publication. Create-once
preserves the winner, and byte comparison resets the loser. Its edit is lost. -/
theorem two_live_copies_race :
    checkIdentity 7 pending.reserved (some 7) (.ok registeredEvidence) = .send ∧
    checkIdentity 7 secondCopy.reserved (some 7) (.ok registeredEvidence) = .send ∧
    (prepare 9 2 (.untried ⟨.write 7 1, [42]⟩)).nonce 0 0 ≠
      (prepare 9 2 (.untried ⟨.write 7 1, [43]⟩)).nonce 0 0 ∧
    create (create emptyStorage (.write 7 1) [42] 10) (.write 7 1) [43] 11 (.write 7 1) =
      some ⟨[42], 10⟩ ∧
    compareOccupied [43] (.ok (some ⟨[42], 10⟩)) = .reset ∧
    (finishReset ⟨7, 8, ⟨1, 1⟩, 1, .slotMismatch⟩ [.write 7 1]).queue = [] := by decide

example : compareOccupied [43] (.ok (some ⟨[42], 10⟩)) = .reset ∧
    create (create emptyStorage (.write 7 1) [42] 10) (.write 7 1) [43] 11 (.write 7 1) =
      some ⟨[42], 10⟩ := by decide

/-- D11 uses the same content-bound recipe for store-log chunks and write
chunks. A changed cleartext prefix also changes the nonce context. -/
theorem entry_chunks_and_prefixes_separated :
    (prepare 9 2 (.untried ⟨.entry 7 1, [42]⟩)).nonce 0 0 ≠
      (prepare 9 2 (.untried ⟨.entry 7 1, [43]⟩)).nonce 0 0 ∧
    (NonceContext.mk ⟨9, .write 7 1, 1, 2⟩ [32, 2, 1] [42]) ≠
      (NonceContext.mk ⟨9, .write 7 1, 1, 2⟩ [32, 2, 2] [42]) := by decide

/-- The two ends must combine independently; neither replacement alone
contains all the observed uploads. Objects above an end stay blocked. -/
theorem concurrent_replacements :
    (Ends.mk 7 2).combine ⟨6, 3⟩ = ⟨7, 3⟩ ∧
    closedLog ⟨7, 2⟩ .entries 3 = .awaitsReplacement ∧
    closedLog ((Ends.mk 7 2).combine ⟨6, 3⟩) .entries 3 = .consume ∧
    checkIdentity 7 pending.reserved (some 7)
      (.ok { registeredEvidence with replaced := true }) = .reset .replaced ∧
    replacementAllowed 1 2 replacement = false ∧
    replacementAllowed 1 1 { replacement with fresh := 7 } = false := by decide

theorem duplicate_delivery_after_reset :
    receiveAll (finishReset replacement [.write 7 1, .write 7 2]).applied
      [.write 7 2, .write 7 1, .write 8 1, .write 8 1] =
      [.write 8 1, .write 7 2, .write 7 1] := by decide

open Files

def photoA : Reference := ⟨7, 1, 100⟩
def photoB : Reference := ⟨8, 2, 101⟩
def originalPhoto : QueuedFile := ⟨⟨photoA, 2, 12, 1⟩, [[10, 20]]⟩
def otherPhoto : QueuedFile := ⟨⟨photoB, 1, 13, 2⟩, [[30]]⟩
def noFiles : Files.State := ⟨fun _ => none, [], [], 0⟩
def activeReports : Reports := ⟨fun _ => .active, fun _ _ => none⟩
def missingReports : Reports := ⟨fun _ => .active, fun _ _ => some .missing⟩

theorem status_cases :
    status photoA (.ok none) activeReports = .ok (.uploading 7) ∧
    status photoA (.ok none) missingReports = .ok (.missing 7 (.source .missing)) ∧
    status photoA (.ok (some ⟨[1], 0⟩)) missingReports = .ok .available ∧
    status photoA (.ok none) { activeReports with device := fun _ => .replaced } =
      .ok (.missing 7 .deviceReplaced) ∧
    status photoA (.error .permission) missingReports = .error .permission ∧
    status photoA (.error .noStorage) activeReports = .error .noStorage := by decide

theorem older_upload_cannot_restore_photo :
    let attached := attach (attach noFiles 7 originalPhoto) 7 otherPhoto
    let finished := (finish attached photoA [10, 20] (.ok (some ⟨[10, 20], 20⟩))).1
    finished.rows 7 = some otherPhoto.row ∧ finished.writes = 2 ∧
    finished.queue = [otherPhoto] := by decide

/-- A whole-file check could precede a source mutation. Checking the actual
chunk sent still rejects it, before nonce use, including a partial retry. -/
theorem changed_source_not_encrypted :
    checkChunk originalPhoto 0 [10, 21] = .error .changed ∧
    checkChunk originalPhoto 1 [10, 20] = .error .integrity ∧
    checkChunk originalPhoto 0 [10, 20] = .ok ⟨photoA, 2, 0, [10, 20]⟩ := by decide

open Clocks

def ts (tick device : Nat) (h : tick < tickLimit := by decide) : Timestamp :=
  ⟨⟨tick, h⟩, device⟩

theorem clock_boundaries :
    stamp (-1000000) 7 (ts 65535 8) = .ok (ts 65536 7) ∧
    stamp 2 7 (ts 65535 8) = .ok (ts 131072 7) ∧
    stamp 0 7 (ts 18446744073709551615 8) = .error .clockOutOfRange ∧
    stamp 281474976710656 7 (ts 0 8) = .error .clockOutOfRange ∧
    (receive ⟨-1000000, ts 0 7, 0⟩ (ts 18446744073709551615 8) true true).applied = 1 := by
  decide

theorem storage_age_boundaries :
    aged ⟨1⟩ ⟨31⟩ 30 = true ∧
    landedTooLate ⟨1⟩ ⟨31⟩ 30 = false ∧
    landedTooLate ⟨1⟩ ⟨32⟩ 30 = true ∧
    oldForFinality ⟨1⟩ ⟨31⟩ 30 = false ∧
    recentForFinality ⟨1⟩ ⟨31⟩ 30 = true ∧
    oldForFinality ⟨1⟩ ⟨32⟩ 30 = true ∧
    retentionAge ⟨1000000000, ⟨2⟩, 8⟩ ⟨1⟩ 30 = false ∧
    retryReady ⟨-1000000000, ⟨2⟩, 8⟩ 0 8 = true ∧
    retryReady ⟨1000000000, ⟨2⟩, 7⟩ 0 8 = false ∧
    retryDelay 0 = 1 ∧ retryDelay 3 = 8 ∧ retryDelay 9 = 300 := by decide

end CovenStorage.Examples
