import CovenStorelogData.CurrentExamples

namespace CovenStorelogData.CircleReturnExamples
open CovenStorelog
open CovenStorelogData.CurrentData
open CovenStorelog.CurrentExamples (key)
open CovenStorelogData.CurrentExamples (entry)

/-- Ben creates Gifts and adds Carol, then removes Ana. Ana's concurrent
removal of Ben initially drops the creation; Ben's removal later defeats hers. -/
def log : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addMember 1 .admin "ben")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 (List.range 3) (.addMember 2 .member "carol")
  | 4 => entry 2 2 (List.range 4) (.addDevice 2 2)
  | 5 => entry 1 1 (List.range 5) (.makeCircle 0 "Gifts" (key 1))
  | 6 => entry 1 1 (List.range 6) (.addToCircle 0 2)
  | 7 => entry 1 1 (List.range 7) (.removeMember 0)
  | _ => entry 0 0 (List.range 5) (.removeMember 1)

def history : CurrentReplay.History := ⟨log, id, fun e => if e = 8 then 5 else e⟩
def before := List.range 5 ++ [8, 5, 6]
def after := before ++ [7]
def result (received : List Nat) := CurrentReplay.resolve history 30 9 (entrySet received)

theorem checked_history : Valid history.membership.log 9 ∧
    CausalOrder history.membership.log after := by
  exact ⟨validCheck_sound _ _ (by decide), causalCheck_sound _ (seen := []) (L := after) .nil (by decide)⟩

theorem dropped_creation_returns :
    5 ∈ (result before).dropped ∧ lookup (result before).state.circles 0 = none ∧
    5 ∈ (result after).kept ∧
    needsReload history 30 9 before (result before).state (result after).state 1 0 = true ∧
    needsReload history 30 9 before (result before).state (result after).state 2 0 = true := by
  decide

theorem same_batch_creation_no_reload :
    inCircle (result after).state 0 2 = true ∧
    needsReload history 30 9 (List.range 5 ++ [8]) (result (List.range 5 ++ [8])).state
      (result after).state 2 0 = false ∧
    needsReload history 30 9 [] State.empty (result after).state 1 0 = false := by
  decide

theorem passed_part_forces_return_reload :
    let author : Author := ⟨1, 1, List.range 6⟩
    (∀ e ∈ author.entries, e ∈ before) ∧
    inCircle (result author.entries).state 0 author.member = true ∧
    needsReload history 30 9 before (result before).state (result after).state 2 0 = true := by
  dsimp only
  refine ⟨?_, by decide, ?_⟩
  · intro e he
    simp [before, List.mem_range] at he ⊢
    omega
  · apply passed_part_return_reloads history 30 9 before _ _ 2 0 ⟨1, 1, List.range 6⟩
    · intro e he
      simp [before, List.mem_range] at he ⊢
      omega
    · decide
    · decide
    · decide

/-- A second installation misses the creation of a different circle and
lands its own creation at the deadline, or one tick past it. -/
def lateLog : CurrentReplay.Log
  | 0 => entry 0 0 [] (.create "ana" (key 0))
  | 1 => entry 0 0 [0] (.addDevice 0 1)
  | 2 => entry 0 0 [0, 1] (.makeCircle 1 "Notes" (key 1))
  | 3 => entry 0 1 [0, 1] (.makeCircle 0 "Gifts" (key 2))
  | _ => entry 0 0 (List.range 4) (.makeCircle 0 "Gifts" (key 3))

def lateHistory (landing : Nat) : CurrentReplay.History :=
  ⟨lateLog, fun e => if e = 3 then landing else if e = 4 then landing + 1 else e,
    fun e => if e = 3 then 2 else if e = 4 then landing + 1 else e⟩

theorem late_history_checked : Valid (lateHistory 33).membership.log 5 ∧
    CausalOrder (lateHistory 33).membership.log [0, 1, 3, 2, 4] := by
  exact ⟨validCheck_sound _ _ (by decide), causalCheck_sound _ .nil (by decide)⟩

theorem late_creation_never_available :
    let H := lateHistory 33
    Finality.tooLate H.membership 30 5 (fun _ => true) 3 = true ∧
    createdBefore H 30 5 [0, 1, 3] 0 = false ∧
    inCircle (CurrentReplay.resolve H 30 5 (entrySet [0, 1, 3])).state 0 0 = false ∧
    inCircle (CurrentReplay.resolve H 30 5 (entrySet (List.range 4))).state 0 0 = false := by
  decide

theorem inclusive_deadline_creation_counts :
    let H := lateHistory 32
    Finality.tooLate H.membership 30 5 (fun _ => true) 3 = false ∧
    createdBefore H 30 5 [0, 1, 3] 0 = true ∧
    inCircle (CurrentReplay.resolve H 30 5 (entrySet [0, 1, 3])).state 0 0 = true := by
  decide

/-- The predicate must disregard a time-rejected creation even if a later
batch supplies another creation of that identity. No uniqueness is assumed. -/
theorem only_late_earlier_creation_no_reload :
    let H := lateHistory 33
    let before := (CurrentReplay.resolve H 30 5 (entrySet (List.range 4))).state
    let after := (CurrentReplay.resolve H 30 5 (entrySet (List.range 5))).state
    inCircle before 0 0 = false ∧ inCircle after 0 0 = true ∧
    needsReload H 30 5 (List.range 4) before after 0 0 = false := by
  decide

theorem fresh_circle_available_and_wait_preserved :
    let reader : CircleReader (Fin 1) Unit := ⟨⟨CovenMerge.St.init, [], []⟩, [], .outside⟩
    writable (circleReplay history 30 9 [] State.empty (result after).state 2 0 reader) = true ∧
    (circleReplay history 30 9 [] State.empty (result after).state 2 0 reader).positions =
      reader.positions ∧
    writable (circleReplay history 30 9 [] State.empty (result after).state 2 0
      { reader with status := .reloading (.waits (.reload (.circle 0))) }) = false ∧
    writable (circleReplay history 30 9 before (result before).state (result after).state
      2 0 reader) = false := by
  decide

theorem boundary_changes_still_reload :
    let state := (result after).state
    needsReload history 30 9 [] state
      { state with versions := [(.circle 0, ⟨1, 0, 9⟩)] } 2 0 = true ∧
    needsReload history 30 9 [] state { state with resets := [(.circle 0, 0)] } 2 0 = true := by
  decide

end CovenStorelogData.CircleReturnExamples
