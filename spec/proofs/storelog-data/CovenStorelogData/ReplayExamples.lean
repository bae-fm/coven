import CovenStorelogData.CircleExamples
import CovenStorelogData.Keys

namespace CovenStorelogData.ReplayExamples

open CovenStorelog
open CovenStorelog.Examples (entry)
open CircleExamples (note writes schema)

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Ben renames his circle. Ana removes Ben, deleting his sole-member circle.
Carol's earlier concurrent removal of Ana makes Ana's removal lose, so Ben's
rename is kept again. None of these operations authors a row deletion. -/
def log : Log
  | 0 => entry 0 0 [] (.create "S3")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "S3")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .admin "S3")
  | 3 => entry 1 1 [0, 1, 2] (.addDevice 1 1)
  | 4 => entry 2 2 [0, 1, 2, 3] (.addDevice 2 2)
  | 5 => entry 1 1 (List.range 5) (.makeCircle 0 "Gifts")
  | 6 => entry 2 2 (List.range 6) (.removeMember 0 [])
  | 7 => entry 0 0 (List.range 6) (.removeMember 1 [])
  | _ => entry 1 1 (List.range 6) (.renameCircle 0 "Notes")

theorem valid_log : Valid log 9 := validCheck_sound _ _ (by decide)

def renamed : CovenStorelogData.State (Fin 3) Unit :=
  ⟨⟨entrySet [0, 1, 2, 3, 4, 5, 8], resolve log 9 (entrySet [0, 1, 2, 3, 4, 5, 8])⟩,
    [0, 1].foldl (CovenMerge.step writes) CovenMerge.St.init⟩
def deleted := CovenStorelogData.step writes log 9 renamed (.entry 7)
def restored := CovenStorelogData.step writes log 9 deleted (.entry 6)

theorem kept_dropped_rekept :
    8 ∈ renamed.log.result.kept ∧ 8 ∈ deleted.log.result.dropped ∧
    8 ∈ restored.log.result.kept ∧ 7 ∈ restored.log.result.dropped ∧
    (observe schema writes log renamed).view.shown (note 9) = true ∧
    (observe schema writes log deleted).view.rules (note 9) = [.deletedCircle] ∧
    ((observe schema writes log deleted).losses (note 9) none).isSome = true ∧
    (observe schema writes log restored).view.shown (note 9) = true ∧
    (observe schema writes log restored).losses (note 9) none = none := by decide

theorem causal_arrivals : CausalOrder log [0, 1, 2, 3, 4, 5, 8, 7, 6] ∧
    CausalOrder log (List.range 9) := by
  constructor
  · apply causalCheck_sound _ .nil; decide
  · exact full_history_causal log 9 valid_log

/-- Device revocation stops future syncing, without undoing writes made
against the preceding registration. The member remains in the store. -/
def deviceLog : Log
  | 0 => entry 0 0 [] (.create "S3")
  | 1 => entry 0 0 [0] (.addMember 1 .member "S3")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | _ => entry 0 0 [0, 1, 2] (.removeDevice 1 1)

theorem device_removal_authority :
    writeAuthority deviceLog 4 ⟨1, 1, [0, 1, 2]⟩ = true ∧
    writeAuthority deviceLog 4 ⟨1, 1, [0, 1, 2, 3]⟩ = false ∧
    member (resolve deviceLog 4 (entrySet (List.range 4))).state 1 = true ∧
    running (resolve deviceLog 4 (entrySet (List.range 4))).state 1 1 = false := by decide

end CovenStorelogData.ReplayExamples
