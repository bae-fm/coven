import CovenStorage.Files
import CovenStorage.Clocks

namespace CovenStorage.Examples

deriving instance DecidableEq for Except

open Files

def photoA : Reference := ⟨7, 1, 100⟩
def photoB : Reference := ⟨8, 2, 101⟩
def originalPhoto : Files.Upload := ⟨photoA, 2, 12, [[10, 20]], []⟩
def otherPhoto : Files.Upload := ⟨photoB, 1, 13, [[30]], []⟩
/-- Device 7, with no rows, files or uploads. -/
def noFiles : Files.State := ⟨7, [], [], [], [], [], [], 0⟩
def stored : Except ReadFailure (Option Object) := .ok (some ⟨[10, 20], 20⟩)

theorem status_cases :
    judge (.cloud photoA) 1 (.ok none) none = .ok (.missing 7 .lost) ∧
    judge (.device 7) 1 (.ok none) (some none) = .ok .uploading ∧
    judge (.device 7) 1 (.error .noStorage) (some (some .missing)) =
      .ok (.missing 7 (.source .missing)) ∧
    judge (.device 8) 1 (.error .noStorage) none = .ok (.deviceOnly 8) ∧
    judge (.cloud photoA) 1 (.ok (some ⟨[1], 0⟩)) none = .ok .available ∧
    judge (.cloud photoA) 1 (.error .permission) none = .error (.failed .permission) ∧
    judge (.cloud photoA) 1 (.error .noStorage) none = .error (.failed .noStorage) := by decide

/-- A wrong-size object at the fixed path is damage. A ranged read past a
short object's end is damage, not absence, and a read meeting absence fails
with status's verdict. -/
theorem damage_cases :
    status photoA 3 (.ok (some ⟨[1, 2], 0⟩)) = .error .integrity ∧
    status photoA 3 (.ok (some ⟨[1, 2, 3, 4], 0⟩)) = .error .integrity ∧
    fetch photoA 3 1 3 (rangeReply (.ok (some ⟨[1, 2], 0⟩)) 1 3) =
      .error (.file .integrity) ∧
    fetch photoA 3 0 2 (rangeReply (.ok none) 0 2) =
      .error (.unavailable (.missing 7 .lost)) := by decide

/-- An object longer than the file: every range inside the file's size is
there, but the reply reports the object's complete size, so the read is
damage, as status says. -/
theorem longer_object_is_damage :
    status photoA 2 (.ok (some ⟨[1, 2, 9], 0⟩)) = .error .integrity ∧
    rangeReply (.ok (some ⟨[1, 2, 9], 0⟩)) 0 2 = .bytes [1, 2] 3 ∧
    fetch photoA 2 0 2 (rangeReply (.ok (some ⟨[1, 2, 9], 0⟩)) 0 2) =
      .error (.file .integrity) ∧
    fetch photoA 2 0 2 (rangeReply (.ok (some ⟨[1, 2], 0⟩)) 0 2) =
      .ok [1, 2] := by decide

/-- Ana stages photo A, file 1, for row 7: one write makes its record, on her
device only, and points the row at it. Completing the upload switches the
record to the stored bytes in one more write; the row is unchanged. -/
theorem completion_stores_the_file :
    let staged := stage noFiles 7 originalPhoto 1
    let finished := (finish staged photoA [10, 20] stored).1
    staged.file 7 = some 1 ∧ staged.records = [⟨1, 2, none, .device 7⟩] ∧ staged.writes = 1 ∧
    finished.file 7 = some 1 ∧ finished.records = [⟨1, 2, some 12, .cloud photoA⟩] ∧
    finished.writes = 2 ∧ finished.queue = [] ∧ finished.stored = [photoA] := by decide

/-- Ana deletes row 7 while A uploads. The delete removes A's last
reference, so the same write deletes A's record. The upload still settles
and retires its queue entry; no file waits for it, so its bytes become a
deletion candidate. -/
theorem deleted_row_upload_finishes :
    let deleted := change (stage noFiles 7 originalPhoto 1) 7 none
    let finished := finish deleted photoA [10, 20] stored
    deleted.records = [] ∧ deleted.writes = 2 ∧
    finished.2 = .stored ∧ finished.1.file 7 = none ∧ finished.1.queue = [] ∧
    finished.1.writes = 2 ∧ finished.1.candidates = [photoA] := by decide

/-- Ana stages B, file 2, for row 7 while A uploads. The write deletes A's
record, which nothing references now; A's completion writes nothing, and a
failed attempt retires nothing. -/
theorem restaged_row_keeps_new_upload :
    let moved := stage (stage noFiles 7 originalPhoto 1) 7 otherPhoto 2
    let finished := finish moved photoA [10, 20] stored
    let failed := finish moved photoA [10, 20] (.error .network)
    moved.records = [⟨2, 1, none, .device 7⟩] ∧
    finished.1.file 7 = some 2 ∧ finished.1.writes = moved.writes ∧
    finished.1.queue = [{ otherPhoto with files := [2] }] ∧
    failed.1.queue = moved.queue := by decide

/-- Rows 7 and 8 share A. Deleting row 7 keeps A's record; deleting row 8
too deletes it. -/
theorem shared_file_until_both_go :
    let shared := change (stage noFiles 7 originalPhoto 1) 8 (some 1)
    let one := change shared 7 none
    let both := change one 8 none
    one.known 1 = true ∧ both.known 1 = false := by decide

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
    retentionAge ⟨1000000000, ⟨2⟩, 8⟩ ⟨1⟩ 30 = false ∧
    retryReady ⟨-1000000000, ⟨2⟩, 8⟩ 0 8 = true ∧
    retryReady ⟨1000000000, ⟨2⟩, 7⟩ 0 8 = false ∧
    retryDelay 0 = 1 ∧ retryDelay 3 = 8 ∧ retryDelay 9 = 300 := by decide

end CovenStorage.Examples
