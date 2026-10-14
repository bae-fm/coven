import CovenStorage.OwnCopyDamage

/-! Ana's phone is device 7 and Ben's laptop device 8, each with one
database that is never backed up, restored or cloned. Sealing randomness and
new device ids come from one counter starting at 100. -/

namespace CovenStorage.OwnCopyDamage.Examples

def household : World := start [7, 8] 100

/-- Ana edits a note, which waits unsent; she reserves and publishes key copy
1; the provider corrupts it before anyone reads it; then both devices scan. -/
def corrupted : List Step := [.edit 0, .reserve 0, .publish 0, .damage (7, 1), .scan 0, .scan 1]

/-- T182's rule resets Ana's phone although no other copy of its database
exists: her unsent edit is discarded. The new device 101 reads every writer's
log, device 7's included, finds the same bytes and still has an incomplete
first phase, as Ben does. The reset bought nothing and lost the edit. -/
theorem damage_resets_uncopied :
    let after := run .resetOnUnreadable household (corrupted ++ [Step.scan 0])
    (corrupted ++ [Step.scan 0]).all (fun s => !s.copiesDatabase) = true ∧
    after.slots = 2 ∧ after.resets = [7] ∧
    after.storage (7, 1) = some ⟨.garbage, 7, true⟩ ∧
    after.copies 0 = some ⟨101, 0, none, 0, [((7, 1), ⟨.garbage, 7, true⟩)]⟩ ∧
    (after.copies 0).map complete = some false ∧
    (after.copies 1).map complete = some false := by decide

/-- Waiting on the same history keeps Ana's device and her unsent edit. Her
first phase is incomplete, exactly as after the reset, and as Ben's is. -/
theorem damage_waits_with_edits :
    let after := run .waitOnUnreadable household corrupted
    after.resets = [] ∧
    after.copies 0 = some ⟨7, 1, none, 1, [((7, 1), ⟨.garbage, 7, true⟩)]⟩ ∧
    (after.copies 0).map complete = some false ∧
    (after.copies 1).map complete = some false := by decide

/-- A device that read the copy before the provider damaged it keeps the
bytes it read: only readings after the damage are incomplete. -/
theorem read_before_damage_keeps_bytes :
    let after := run .waitOnUnreadable household
      [.reserve 0, .publish 0, .scan 1, .damage (7, 1), .scan 0, .scan 1]
    (after.copies 1).map complete = some true ∧
    (after.copies 0).map complete = some false := by decide

/-- Waiting does not weaken §10's comparison: a clone of Ana's database
that seals copy 1 with other randomness finds her copy there and resets. -/
theorem live_copies_still_reset :
    let after := run .waitOnUnreadable (start [7] 100)
      [.clone 0, .reserve 0, .reserve 1, .publish 0, .publish 1]
    after.resets = [7] ∧ after.storage (7, 1) = some ⟨.sealed 100, 7, false⟩ ∧
    after.copies 1 = some ⟨102, 0, none, 0, []⟩ := by decide

end CovenStorage.OwnCopyDamage.Examples
