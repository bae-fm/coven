import CovenStorage.Files
import CovenStorage.Clocks

namespace CovenStorage.Examples

deriving instance DecidableEq for Except

open Files

def photoA : Reference := ⟨7, 1, 100⟩
def photoB : Reference := ⟨8, 2, 101⟩
def originalPhoto : QueuedFile := ⟨⟨photoA, 2, 12, 1⟩, [[10, 20]]⟩
def otherPhoto : QueuedFile := ⟨⟨photoB, 1, 13, 2⟩, [[30]]⟩
def noFiles : Files.State := ⟨fun _ => none, [], [], 0⟩
def activeReports : Reports := ⟨fun _ => .active, fun _ _ => none⟩
def missingReports : Reports := ⟨fun _ => .active, fun _ _ => some .missing⟩

theorem status_cases :
    status photoA 1 (.ok none) activeReports = .ok (.uploading 7) ∧
    status photoA 1 (.ok none) missingReports = .ok (.missing 7 (.source .missing)) ∧
    status photoA 1 (.ok (some ⟨[1], 0⟩)) missingReports = .ok .available ∧
    status photoA 1 (.ok none) { activeReports with device := fun _ => .replaced } =
      .ok (.missing 7 .deviceReplaced) ∧
    status photoA 1 (.error .permission) missingReports = .error (.failed .permission) ∧
    status photoA 1 (.error .noStorage) activeReports = .error (.failed .noStorage) := by decide

/-- A wrong-size object at the fixed path is damage whatever the reports say.
A ranged read past a short object's end is damage, not absence, and a read
meeting absence fails with status's verdict. -/
theorem damage_cases :
    status photoA 3 (.ok (some ⟨[1, 2], 0⟩)) missingReports = .error .integrity ∧
    status photoA 3 (.ok (some ⟨[1, 2, 3, 4], 0⟩)) activeReports = .error .integrity ∧
    fetch photoA activeReports 3 1 3 (rangeReply (.ok (some ⟨[1, 2], 0⟩)) 1 3) =
      .error (.file .integrity) ∧
    fetch photoA missingReports 3 0 2 (rangeReply (.ok none) 0 2) =
      .error (.unavailable (.missing 7 (.source .missing))) := by decide

/-- An object longer than the file: every range inside the file's size is
there, but the reply reports the object's complete size, so the read is
damage, as status says. -/
theorem longer_object_is_damage :
    status photoA 2 (.ok (some ⟨[1, 2, 9], 0⟩)) activeReports = .error .integrity ∧
    rangeReply (.ok (some ⟨[1, 2, 9], 0⟩)) 0 2 = .bytes [1, 2] 3 ∧
    fetch photoA activeReports 2 0 2 (rangeReply (.ok (some ⟨[1, 2, 9], 0⟩)) 0 2) =
      .error (.file .integrity) ∧
    fetch photoA activeReports 2 0 2 (rangeReply (.ok (some ⟨[1, 2], 0⟩)) 0 2) =
      .ok [1, 2] := by decide

/-- Ana attaches photo A to row 7 and deletes the row while A uploads. The
upload still settles and retires its queue entry; the row stays deleted. -/
theorem deleted_row_upload_finishes :
    let deleted := deleteRow (attach noFiles 7 originalPhoto) 7
    let finished := finish deleted photoA [10, 20] (.ok (some ⟨[10, 20], 20⟩))
    finished.2 = .stored ∧ finished.1.rows 7 = none ∧ finished.1.queue = [] ∧
    finished.1.writes = 2 := by decide

/-- Row 7 is deleted and then attached to photo B while A uploads. A's
settlement retires only A; a failed attempt retires nothing. -/
theorem reattached_row_keeps_new_upload :
    let moved := attach (deleteRow (attach noFiles 7 originalPhoto) 7) 7 otherPhoto
    let stored := finish moved photoA [10, 20] (.ok (some ⟨[10, 20], 20⟩))
    let failed := finish moved photoA [10, 20] (.error .network)
    stored.1.rows 7 = some otherPhoto.row ∧ stored.1.queue = [otherPhoto] ∧
    failed.1.queue = [otherPhoto, originalPhoto] := by decide

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
    retentionAge ⟨1000000000, ⟨2⟩, 8⟩ ⟨1⟩ 30 = false ∧
    retryReady ⟨-1000000000, ⟨2⟩, 8⟩ 0 8 = true ∧
    retryReady ⟨1000000000, ⟨2⟩, 7⟩ 0 8 = false ∧
    retryDelay 0 = 1 ∧ retryDelay 3 = 8 ∧ retryDelay 9 = 300 := by decide

end CovenStorage.Examples
