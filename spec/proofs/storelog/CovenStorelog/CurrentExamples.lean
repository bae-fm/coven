import CovenStorelog.CurrentReplay
import CovenStorelog.ExamplesSupport

namespace CovenStorelog.CurrentExamples
open CurrentReplay

def entry (author device : Nat) (past : List Nat) (action : CurrentReplay.Action) : CurrentReplay.Entry :=
  ⟨author, device, past, action⟩

def key (id : Nat) : KeyCommitment := ⟨id, Vector.replicate 32 (UInt8.ofNat id)⟩

def emptyCircleLog : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .member "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 0 0 [0, 1, 2] (.addMember 3 .admin "dan")
  | 4 => entry 1 1 (List.range 4) (.addDevice 1 1)
  | 5 => entry 0 0 (List.range 5) (.addDevice 0 2)
  | 6 => entry 0 0 (List.range 6) (.makeCircle 0 "Gifts" (key 1))
  | 7 => entry 0 0 (List.range 7) (.addToCircle 0 1)
  | 8 => entry 0 0 (List.range 8) (.removeFromCircle 0 1)
  | 9 => entry 0 2 (List.range 8) (.removeMember 0)
  | _ => entry 1 1 (List.range 8) (.addToCircle 0 2)

def keyFreeEmptyCircleHistory : CurrentReplay.History := ⟨emptyCircleLog, id, fun e => min e 8⟩
/-- Historical fixture consumed by the data package. Its removal names the
Gifts replacement; the current-policy examples below use the key-free history. -/
def emptyCircleHistory : Finality.History :=
  { keyFreeEmptyCircleHistory.membership with
    log := fun e =>
      if e = 9 then ⟨0, 2, List.range 8, .removeMember 0 [0]⟩
      else keyFreeEmptyCircleHistory.membership.log e }

def emptyCircleResult := CurrentReplay.resolve keyFreeEmptyCircleHistory 30 11 (entrySet (List.range 11))

theorem concurrent_addition_populates_empty_circle : CurrentReplay.Valid emptyCircleLog 11 ∧
    8 ∈ emptyCircleResult.kept ∧ 9 ∈ emptyCircleResult.kept ∧
    10 ∈ emptyCircleResult.kept ∧ inCircle emptyCircleResult.state 0 2 = true := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

example : 10 ∈ emptyCircleResult.kept ∧ inCircle emptyCircleResult.state 0 2 = true := by decide


/-- Ana's phone removes Ben while her tablet removes Ana from the store.
Only their combined replay is empty; a later-received concurrent Carol add
populates Gifts without undoing either removal. -/
theorem empty_circle_cause :
    lookup (CurrentReplay.resolve keyFreeEmptyCircleHistory 30 11
      (entrySet (List.range 10))).state.circles 0 = none ∧
    CurrentReplay.circleCause keyFreeEmptyCircleHistory 30 11 (entrySet (List.range 10)) 0 = some 9 ∧
    CurrentReplay.circleCause keyFreeEmptyCircleHistory 30 11 (entrySet (List.range 11)) 0 = none := by decide

def rotations : CurrentReplay.Log
  | 8 => entry 0 0 (List.range 8) (.rotateKey (.circle 0) (key 10))
  | 9 => entry 1 1 (List.range 8) (.rotateKey (.circle 0) (key 11))
  | 10 => entry 3 3 (List.range 8) (.rotateKey (.circle 0) (key 12))
  | 11 => entry 1 1 (List.range 8 ++ [9]) (.rotateKey .store (key 13))
  | n => emptyCircleLog n

def rotationHistory : CurrentReplay.History := ⟨rotations, id, fun e => min e 8⟩
def rotationResult := CurrentReplay.resolve rotationHistory 30 12 (fun _ => true)

theorem rotation_entries :
    CurrentReplay.Valid rotations 12 ∧
    8 ∈ rotationResult.kept ∧ 9 ∈ rotationResult.kept ∧
    10 ∈ rotationResult.dropped ∧ 11 ∈ rotationResult.kept ∧
    inCircle rotationResult.state 0 0 = true ∧ inCircle rotationResult.state 0 1 = true := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

def removedRotator : CurrentReplay.Log
  | 8 => entry 0 0 (List.range 8) (.removeMember 1)
  | 9 => entry 1 1 (List.range 8) (.rotateKey (.circle 0) (key 11))
  | n => emptyCircleLog n

theorem removed_rotator_keeps_recorded_authority :
    let H : CurrentReplay.History := ⟨removedRotator, id, fun e => min e 8⟩
    let r := CurrentReplay.resolve H 30 10 (fun _ => true)
    CurrentReplay.Valid removedRotator 10 ∧ 8 ∈ r.kept ∧ 9 ∈ r.kept ∧
      inCircle r.state 0 1 = false := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

def explicitDeletion : CurrentReplay.Log
  | 8 => entry 0 0 (List.range 8) (.deleteCircle 0)
  | 9 => entry 1 1 (List.range 8) (.addToCircle 0 2)
  | n => emptyCircleLog n

theorem explicit_deletion_still_wins :
    let H : CurrentReplay.History := ⟨explicitDeletion, id, fun e => min e 8⟩
    let r := CurrentReplay.resolve H 30 10 (fun _ => true)
    8 ∈ r.kept ∧ 9 ∈ r.dropped ∧ lookup r.state.circles 0 = none ∧
      CurrentReplay.circleCause H 30 10 (fun _ => true) 0 = some 8 := by decide

def deletedRotator : CurrentReplay.Log
  | 8 => entry 0 0 (List.range 8) (.deleteCircle 0)
  | 9 => entry 1 1 (List.range 8) (.rotateKey (.circle 0) (key 11))
  | n => emptyCircleLog n
theorem deleted_circle_keeps_rotation :
    let H : CurrentReplay.History := ⟨deletedRotator, id, fun e => min e 8⟩
    let r := CurrentReplay.resolve H 30 10 (fun _ => true)
    CurrentReplay.Valid deletedRotator 10 ∧ 8 ∈ r.kept ∧ 9 ∈ r.kept ∧
      lookup r.state.circles 0 = none := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩
end CovenStorelog.CurrentExamples
