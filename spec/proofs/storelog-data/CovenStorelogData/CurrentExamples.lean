import CovenStorelogData.CurrentData
import CovenStorelogData.DeliveryExamples
import CovenStorelogData.StorageFinality
import CovenStorelogData.SecurityExamples

namespace CovenStorelogData.CurrentExamples
open CovenStorelog
open CovenStorelog.Examples (entry)

def circleLog : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 [0, 1, 2] (.makeCircle 0 "Gifts")
  | 4 => entry 0 0 (List.range 4) (.addToCircle 0 1)
  | 5 => entry 0 0 (List.range 5) (.removeMember 1 [0])
  | _ => entry 1 1 (List.range 5) (.deleteCircle 0)

def circleHistory : Finality.History := ⟨circleLog, id, fun e => min e 5⟩
def circleResult (arrivals : List Nat) :=
  CurrentReplay.resolve circleHistory 30 7 (entrySet (List.range 5 ++ arrivals))
def original := [0].foldl (CovenMerge.step DeliveryExamples.writes) CovenMerge.St.init
def circleView (arrivals : List Nat) := CurrentData.observe
  DeliveryExamples.schema DeliveryExamples.writes circleHistory 30 7
  (List.range 5 ++ arrivals) original

theorem entry_only_deletion_restores :
    Valid circleLog 7 ∧
    (circleView [6]).view.shown DeliveryExamples.row = false ∧
    ((circleView [6]).losses DeliveryExamples.row none).isSome = true ∧
    (circleView [6, 5]).view.shown DeliveryExamples.row = true ∧
    (circleView [6, 5]).losses DeliveryExamples.row none = none ∧
    CurrentData.needsReload (circleResult [6]).state (circleResult [6, 5]).state 0 0 = true := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

def snapshot : Snapshot (Fin 1) Unit := ⟨CovenMerge.St.init, [], [], []⟩
def headers (_ : Fin 1) : Header (Fin 1) := ⟨0, [], .apply⟩
def reloadData := CurrentData.prepareReload DeliveryExamples.writes headers
  circleHistory 30 7 (List.range 7) 0 (fun _ => snapshot) snapshot [0]
def skipped : CurrentData.CircleReader (Fin 1) Unit :=
  ⟨⟨CovenMerge.St.init, [], []⟩, [0], .outside⟩

/-- The overall position already passed the write. The returning circle still
loads it; its data and position become available in the same replacement. -/
theorem passed_position_does_not_lose_part :
    skipped.positions = [0] ∧
    (CurrentData.finishReload skipped (.ok (reloadData, [0]))).data.data.cell
      DeliveryExamples.row () = some 0 ∧
    CurrentData.writable (CurrentData.finishReload skipped (.ok (reloadData, [0]))) = true := by
  decide

/-- The shared engine erases a circle as soon as concurrent removals empty it.
§14.6 instead needs that decision after replay so the concurrent addition can
populate it. This checks the dependency gap, not a counterexample to §14.6. -/
def emptyCircleLog : Log
  | 0 => entry 0 0 [] (.create "ana")
  | 1 => entry 0 0 [0] (.addMember 1 .member "ben")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "carol")
  | 3 => entry 0 0 [0, 1, 2] (.addMember 3 .admin "dan")
  | 4 => entry 1 1 (List.range 4) (.addDevice 1 1)
  | 5 => entry 0 0 (List.range 5) (.addDevice 0 2)
  | 6 => entry 0 0 (List.range 6) (.makeCircle 0 "Gifts")
  | 7 => entry 0 0 (List.range 7) (.addToCircle 0 1)
  | 8 => entry 0 0 (List.range 8) (.removeFromCircle 0 1)
  | 9 => entry 0 2 (List.range 8) (.removeMember 0 [0])
  | _ => entry 1 1 (List.range 8) (.addToCircle 0 2)

def emptyCircleHistory : Finality.History := ⟨emptyCircleLog, id, fun e => min e 8⟩
def emptyCircleResult := CurrentReplay.resolve emptyCircleHistory 30 11 (entrySet (List.range 11))

theorem empty_circle_dependency_gap : Valid emptyCircleLog 11 ∧
    8 ∈ emptyCircleResult.kept ∧ 9 ∈ emptyCircleResult.kept ∧
    10 ∈ emptyCircleResult.dropped ∧ lookup emptyCircleResult.state.circles 0 = none := by
  exact ⟨validCheck_sound _ _ (by decide), by decide⟩

example : 10 ∈ emptyCircleResult.dropped ∧ lookup emptyCircleResult.state.circles 0 = none := by decide

theorem inclusive_window_boundary :
    StorageFinality.quiet SecurityExamples.storageHistory 30 9 38 = false ∧
    StorageFinality.certified SecurityExamples.storageHistory 30 9 38 (fun _ => true) 0 = false ∧
    StorageFinality.quiet SecurityExamples.storageHistory 30 9 39 = true ∧
    StorageFinality.certified SecurityExamples.storageHistory 30 9 39 (fun _ => true) 8 = true := by decide

end CovenStorelogData.CurrentExamples
