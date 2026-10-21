import CovenStorage.Files
import CovenStorage.Clocks

namespace CovenStorage.Examples

deriving instance DecidableEq for Except

open Files

def photoA : Reference := ⟨7, 1, 100⟩
def photoB : Reference := ⟨8, 2, 101⟩
def originalPhoto : QueuedFile := ⟨⟨photoA, 2, 12⟩, [[10, 20]]⟩
def otherPhoto : QueuedFile := ⟨⟨photoB, 1, 13⟩, [[30]]⟩
def noFiles : Files.State := ⟨fun _ => none, [], [], [], [], [], [], 0, 0⟩
def stored : Except ReadFailure (Option Object) := .ok (some ⟨[10, 20], 20⟩)

theorem status_cases :
    status photoA 1 (.ok none) none = .ok (.missing 7 .lost) ∧
    status photoA 1 (.ok none) (some none) = .ok .uploading ∧
    status photoA 1 (.error .noStorage) (some (some .missing)) =
      .ok (.missing 7 (.source .missing)) ∧
    status photoA 1 (.ok (some ⟨[1], 0⟩)) none = .ok .available ∧
    status photoA 1 (.error .permission) none = .error (.failed .permission) ∧
    status photoA 1 (.error .noStorage) none = .error (.failed .noStorage) := by decide

/-- A wrong-size object at the fixed path is damage. A ranged read past a
short object's end is damage, not absence, and a read meeting absence fails
with status's verdict. -/
theorem damage_cases :
    status photoA 3 (.ok (some ⟨[1, 2], 0⟩)) none = .error .integrity ∧
    status photoA 3 (.ok (some ⟨[1, 2, 3, 4], 0⟩)) none = .error .integrity ∧
    fetch photoA 3 1 3 (rangeReply (.ok (some ⟨[1, 2], 0⟩)) 1 3) =
      .error (.file .integrity) ∧
    fetch photoA 3 0 2 (rangeReply (.ok none) 0 2) =
      .error (.unavailable (.missing 7 .lost)) := by decide

/-- An object longer than the file: every range inside the file's size is
there, but the reply reports the object's complete size, so the read is
damage, as status says. -/
theorem longer_object_is_damage :
    status photoA 2 (.ok (some ⟨[1, 2, 9], 0⟩)) none = .error .integrity ∧
    rangeReply (.ok (some ⟨[1, 2, 9], 0⟩)) 0 2 = .bytes [1, 2] 3 ∧
    fetch photoA 2 0 2 (rangeReply (.ok (some ⟨[1, 2, 9], 0⟩)) 0 2) =
      .error (.file .integrity) ∧
    fetch photoA 2 0 2 (rangeReply (.ok (some ⟨[1, 2], 0⟩)) 0 2) =
      .ok [1, 2] := by decide

/-- Ana attaches photo A to row 7. Until A's upload completes, row 7 has no
file; completing it writes A into the row as one write. -/
theorem completion_names_the_file :
    let attached := attach (insert noFiles 7) 7 originalPhoto
    let finished := (finish attached photoA [10, 20] stored).1
    (attached.rows 7).bind (·.file) = none ∧
    (finished.rows 7).bind (·.file) = some originalPhoto.file ∧
    finished.writes = 2 ∧ finished.queue = [] ∧ finished.stored = [photoA] := by decide

/-- Ana deletes row 7 while A uploads. The upload still settles and retires
its queue entry; the row stays deleted, A is abandoned and its bytes become a
deletion candidate. -/
theorem deleted_row_upload_finishes :
    let deleted := change (attach (insert noFiles 7) 7 originalPhoto) 7 none true
    let finished := finish deleted photoA [10, 20] stored
    finished.2 = .stored ∧ finished.1.rows 7 = none ∧ finished.1.queue = [] ∧
    finished.1.writes = 2 ∧ finished.1.abandoned = [(photoA, .removed)] ∧
    finished.1.candidates = [photoA] := by decide

/-- Ana attaches B to row 7 while A uploads. A is abandoned at once; its
completion writes nothing, and a failed attempt retires nothing. -/
theorem reattached_row_keeps_new_upload :
    let moved := attach (attach (insert noFiles 7) 7 originalPhoto) 7 otherPhoto
    let finished := finish moved photoA [10, 20] stored
    let failed := finish moved photoA [10, 20] (.error .network)
    moved.abandoned = [(photoA, .replaced)] ∧
    (finished.1.rows 7).bind (·.file) = none ∧ finished.1.queue = [otherPhoto] ∧
    failed.1.queue = [otherPhoto, originalPhoto] := by decide

/-- Ben's write setting B arrives on Ana's phone before A's upload finishes.
B stays: A's late completion writes nothing. -/
theorem seen_change_wins :
    let attached := attach (insert noFiles 7) 7 originalPhoto
    let withB := { attached with stored := [photoB] }
    let seen := change withB 7 (some (some otherPhoto.file)) false
    let finished := (finish seen photoA [10, 20] stored).1
    (finished.rows 7).bind (·.file) = some otherPhoto.file ∧ finished.writes = 1 ∧
    seen.abandoned = [(photoA, .replaced)] := by decide

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
