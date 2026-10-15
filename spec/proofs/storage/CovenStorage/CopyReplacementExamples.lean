import CovenStorage.CopyReplacement

/-! Ana's phone is device 7 and Ben's laptop device 8. Sealing randomness and
new device ids come from one counter starting at 100; routing facts 5 stand
for "the store key, sealed to Ben's current sealing key". -/

namespace CovenStorage.CopyReplacement.Examples

def household : World := start [7, 8] 100

/-- Ana publishes key copy 1 to Ben; the provider corrupts it before either
device reads it; Ben's first phase waits. -/
def corrupted : List Step := [.reserve 0 5, .publish 0, .damage (7, 1), .scan 1]

/-- Ana reads her own copy as undecodable. Her record of copy 1 stands in
for it, so her own first phase completes and she can send: she reseals the
same key to the same recipient as copy 2, publishes it and then the entry
naming copies 1 and 2. Ben's next reading skips copy 1 and completes, and
copy 2 tells him what copy 1 did. -/
theorem replaced_copy_completes :
    let waiting := run household corrupted
    let read := run household (corrupted ++ [.scan 0])
    let after := run household
      (corrupted ++ [.scan 0, .replace 0 1, .publish 0, .announce 0, .scan 0, .scan 1])
    (waiting.copies 1).map (complete waiting.marks) = some false ∧
    (read.copies 0).map (complete read.marks) = some true ∧
    after.marks = [⟨7, 1, 2⟩] ∧
    (after.copies 1).map (complete after.marks) = some true ∧
    (after.copies 0).map (complete after.marks) = some true ∧
    after.storage (7, 2) = some ⟨.sealed 101 5, 7, 5, false⟩ ∧
    after.resets = [] := by decide

/-- The liveness gap: Ana read her copy before the provider damaged it, so
she keeps the bytes she read and never sees the damage. Nothing marks the
copy, and Ben's first phase stays incomplete however often both scan. -/
theorem owner_read_first_stalls :
    let after := run household
      [.reserve 0 5, .publish 0, .scan 0, .damage (7, 1), .scan 1,
       .replace 0 1, .publish 0, .announce 0, .scan 0, .scan 1]
    after.marks = [] ∧
    (after.copies 0).map (complete after.marks) = some true ∧
    (after.copies 1).map (complete after.marks) = some false := by decide

/-- Ben cannot mark Ana's copy: an entry names numbers in its own writer's
log, and Ben has no confirmed copy, so his replacement does nothing. -/
theorem non_owner_cannot_mark :
    let after := run household (corrupted ++ [.replace 1 1, .publish 1, .announce 1, .scan 1])
    after.marks = [] ∧ (after.copies 1).map (complete after.marks) = some false := by decide

/-- A replacement damaged in turn is replaced again; both marks hold and Ben
completes on copy 3. -/
theorem damaged_replacement_replaced_again :
    let after := run household
      (corrupted ++ [.scan 0, .replace 0 1, .publish 0, .announce 0, .damage (7, 2), .scan 0,
        .replace 0 2, .publish 0, .announce 0, .scan 1])
    after.marks = [⟨7, 1, 2⟩, ⟨7, 2, 3⟩] ∧
    (after.copies 1).map (complete after.marks) = some true := by decide

/-- The second liveness gap: Ana's copy 1 and Ben's copy 1 are both
damaged before either reads them. Each one's record stands in for its own
copy, but the other's undecodable copy leaves its reading incomplete, so
neither has the sending authority a replacement needs, and neither ever
completes. -/
theorem cross_damage_stalls :
    let after := run household
      [.reserve 0 5, .publish 0, .reserve 1 6, .publish 1, .damage (7, 1), .damage (8, 1),
       .scan 0, .scan 1, .replace 0 1, .replace 1 1, .publish 0, .publish 1,
       .announce 0, .announce 1, .scan 0, .scan 1]
    after.marks = [] ∧
    (after.copies 0).map (complete after.marks) = some false ∧
    (after.copies 1).map (complete after.marks) = some false := by decide

end CovenStorage.CopyReplacement.Examples
