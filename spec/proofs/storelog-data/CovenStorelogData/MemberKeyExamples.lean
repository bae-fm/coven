import CovenStorelogData.MemberKeyPhases
import CovenStorelog.MemberKeyExamples
import CovenStorelog.OldKeyObjects

namespace CovenStorelogData.MemberKeyExamples
open CovenStorelog CurrentReplay

set_option maxRecDepth 8192
set_option maxHeartbeats 4000000

def commitment (id : Nat) : KeyCommitment := ⟨id, Vector.replicate 32 0⟩
def oldKeys : Keyed.MemberKeys := ⟨100, 200⟩
def newKeys : Keyed.MemberKeys := ⟨102, 202⟩

def log : Keyed.Log
  | 0 => ⟨0, 100, 0, [], .entry (.create "Ana" (commitment 10)) oldKeys⟩
  | 1 => ⟨0, 100, 0, [0], .entry (.makeCircle 7 "Gifts" (commitment 20)) ()⟩
  | 2 => ⟨0, 100, 0, [0, 1], .replaceMemberKeys 0 newKeys⟩
  | 3 => ⟨0, 102, 0, [0, 1, 2], .entry (.rotateKey .store (commitment 11)) ()⟩
  | _ => ⟨0, 102, 0, [0, 1, 2, 3], .entry (.rotateKey (.circle 7) (commitment 21)) ()⟩

def H : Keyed.History := ⟨log, id, id⟩

theorem checked_metadata : Keyed.metadataValid H 5 = true := by decide

def storeOld : KeySelection.Key := ⟨0, .store, commitment 10⟩
def circleOld : KeySelection.Key := ⟨1, .circle 7, commitment 20⟩
def storeNew : KeySelection.Key := ⟨3, .store, commitment 11⟩
def circleNew : KeySelection.Key := ⟨4, .circle 7, commitment 21⟩

def caughtUp : KeyPhases.MemberKeys.CaughtUp :=
  ⟨[0, 1, 2], ⟨[storeOld, circleOld], [(storeOld, ⟨0, 200⟩), (circleOld, ⟨0, 200⟩)]⟩⟩

theorem replacement_retires_both_audiences :
    KeyPhases.MemberKeys.firstSend H 30 5 0 .store caughtUp [9] = none ∧
    KeyPhases.MemberKeys.firstSend H 30 5 0 (.circle 7) caughtUp [9] = none ∧
    member (Keyed.resolve H 30 5 (entrySet caughtUp.received)).state.membership 0 = true := by decide

theorem historical_sharing_uses_new_binding :
    KeySelection.MemberKeys.shareInReplay H 30 5 (entrySet caughtUp.received)
      caughtUp.observation 0 storeOld =
      some (caughtUp.observation.copies ++ [(storeOld, ⟨0, 202⟩)]) := by decide

/-- Sharing an old key to the new credential does not erase its old copy. -/
theorem resharing_does_not_unexpose :
    KeySelection.MemberKeys.selectInReplay H 30 5 (entrySet caughtUp.received)
      { caughtUp.observation with copies := caughtUp.observation.copies ++ [(storeOld, ⟨0, 202⟩)] }
      0 .store = none := by decide

def afterStore : KeyPhases.MemberKeys.CaughtUp :=
  ⟨[0, 1, 2, 3], ⟨storeNew :: caughtUp.observation.custody,
    caughtUp.observation.copies ++ [(storeNew, ⟨0, 202⟩)]⟩⟩

def afterCircle : KeyPhases.MemberKeys.CaughtUp :=
  ⟨[0, 1, 2, 3, 4], ⟨circleNew :: afterStore.observation.custody,
    afterStore.observation.copies ++ [(circleNew, ⟨0, 202⟩)]⟩⟩

theorem both_rotations_publish_current_copies :
    KeyPhases.MemberKeys.rotate H 30 5 0 0 caughtUp 3 (.ok ()) (.ok ()) = .ok afterStore ∧
    KeyPhases.MemberKeys.rotate H 30 5 0 0 afterStore 4 (.ok ()) (.ok ()) = .ok afterCircle := by exact ⟨rfl, rfl⟩

theorem rotation_selection_avoids_old_keys :
    KeyPhases.MemberKeys.firstSend H 30 5 0 .store afterCircle [9] = some ⟨storeNew, [9]⟩ ∧
    KeyPhases.MemberKeys.firstSend H 30 5 0 (.circle 7) afterCircle [9] = some ⟨circleNew, [9]⟩ := by decide

theorem failed_rotation_cannot_send :
    KeyPhases.MemberKeys.rotate H 30 5 0 0 caughtUp 3 (.error .refused) (.ok ()) = .error .refused ∧
    KeyPhases.MemberKeys.rotate H 30 5 0 0 caughtUp 3 (.ok ()) (.error .refused) = .error .refused ∧
    KeyPhases.MemberKeys.firstSend H 30 5 0 .store caughtUp [9] = none := by exact ⟨rfl, rfl, rfl⟩

/-- The statement is about new reservations. A copy sealed before replacement
can still land later; the next observation excludes even this newer key. -/
theorem later_old_copy_exposes_rotation :
    KeyPhases.MemberKeys.firstSend H 30 5 0 .store
      { afterCircle with observation := { afterCircle.observation with
          copies := afterCircle.observation.copies ++ [(storeNew, ⟨0, 200⟩)] } } [9] = none := by decide

/-- A prerequisite copy for a losing replacement names an active member but
does not name that member's current credential. It still exposes the key. -/
theorem unbound_copy_exposes_rotation :
    KeyPhases.MemberKeys.firstSend H 30 5 0 .store
      { afterCircle with observation := { afterCircle.observation with
          copies := afterCircle.observation.copies ++ [(storeNew, ⟨0, 999⟩)] } } [9] = none := by decide

namespace OldWriteWithRotation

/-- The admission counterexample can use the required fresh store key: one
rotation on day 2, before the finality window, supplies the later reader. -/
def H : Keyed.History := { Keyed.Examples.onlyAdmin with log := (fun e =>
  if e = 2 then ⟨0, 102, 0, [0, 1], .entry (.rotateKey .store (commitment 11)) ()⟩
  else Keyed.Examples.onlyAdmin.log e) }

def fresh : KeySelection.Key := ⟨2, .store, commitment 11⟩
def before : KeyPhases.MemberKeys.CaughtUp :=
  ⟨[0, 1], ⟨[storeOld], [(storeOld, ⟨0, 200⟩)]⟩⟩
def after : KeyPhases.MemberKeys.CaughtUp :=
  ⟨[0, 1, 2], ⟨[fresh, storeOld], [(storeOld, ⟨0, 200⟩), (fresh, ⟨0, 202⟩)]⟩⟩

def writes : Keyed.OldKeyObjects.Writes
  | 0 => ⟨0, 100, 0, [0], [], 32⟩
  | _ => ⟨0, 102, 0, [0, 1, 2], [0], 34⟩

def counted (received : List Nat) (w : Nat) : Bool :=
  Keyed.OldKeyObjects.countInReplay H 30 3 (entrySet [0, 1, 2]) (fun _ => true)
    writes 2 (entrySet received) w

theorem reader_uses_fresh_rotation :
    Keyed.metadataValid H 3 = true ∧
    KeyPhases.MemberKeys.rotate H 30 3 0 0 before 2 (.ok ()) (.ok ()) = .ok after ∧
    KeyPhases.MemberKeys.firstSend H 30 3 0 .store after [9] = some ⟨fresh, [9]⟩ := by
  exact ⟨by decide, rfl, by decide⟩

theorem final_replacement_still_allows_rescue :
    H.stored 1 + 30 < 33 ∧
    (List.range 3).all (fun e => !(3 ≤ H.stored e && H.stored e ≤ 33)) = true ∧
    1 ∈ (Keyed.resolve H 30 3 (entrySet [0, 1, 2])).kept ∧
    counted [0] 0 = false ∧ counted [0, 1] 1 = true ∧ counted [0, 1] 0 = true := by decide

end OldWriteWithRotation

end CovenStorelogData.MemberKeyExamples
