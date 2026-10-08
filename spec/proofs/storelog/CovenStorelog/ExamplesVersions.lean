import CovenStorelog.ExamplesCircles

namespace CovenStorelog.Examples

set_option maxRecDepth 32768
set_option maxHeartbeats 32000000

def updates (secondSnapshot : Nat) : Log
  | 3 => entry 0 0 [0, 1, 2] (.raiseSchema 2 ⟨.store, 30⟩)
  | 4 => entry 1 1 [0, 1, 2] (.raiseSchema 2 ⟨.store, secondSnapshot⟩)
  | 5 => entry 1 1 [0, 1, 2, 4] (.raiseSchema 3 ⟨.store, 50⟩)
  | w => household .member .member w

def resets (audience : Audience) (same : Bool := false) : Log
  | 5 => entry 0 0 (List.range 5) (.reset ⟨audience, 50⟩)
  | 6 => entry 1 1 (List.range 5) (.reset ⟨audience, (if same then 50 else 60)⟩)
  | 7 => entry 0 0 (List.range 7) (.reset ⟨audience, 70⟩)
  | w => if audience == .store then household .admin .member w else gifts w

theorem version_examples_valid :
    validCheck (updates 40) 6 = true ∧
    validCheck (resets .store) 8 = true ∧ validCheck (resets (.circle 0)) 8 = true ∧
    validCheck (resets .store true) 7 = true ∧
    validCheck (updates 30) 5 = true := by decide

/-- §17: ordinary members can raise the schema version. Different snapshots for
the same concurrent raise conflict, and the later entry is dropped. -/
theorem same_version_snapshot :
    EveryOrder (updates 40) 5 (List.range 5) (fun r =>
      lookup r.state.versions .store = some ⟨2, 30, 3⟩ ∧ r.dropped = [4]) := by
  apply every_order
  decide

theorem version_raised :
    EveryOrder (updates 40) 6 (List.range 6) (fun r =>
      lookup r.state.versions .store = some ⟨3, 50, 5⟩ ∧ r.dropped = [4]) := by
  apply every_order
  decide

/-- The in-place rule keeps both identities when the version and snapshot agree. -/
theorem identical_version_raises :
    EveryOrder (updates 30) 5 (List.range 5) (fun r =>
      lookup r.state.versions .store = some ⟨2, 30, 3⟩ ∧
      3 ∈ r.kept ∧ 4 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order
  decide

/-- §19.3: store resets require an admin; circle resets require membership. -/
theorem store_reset_tie : EveryOrder (resets .store) 7 (List.range 7) (fun r =>
    lookup r.state.resets .store = some 50 ∧ r.dropped = [6]) := by
  apply every_order; decide

theorem circle_reset_tie : EveryOrder (resets (.circle 0)) 7 (List.range 7) (fun r =>
    lookup r.state.resets (.circle 0) = some 50 ∧ r.dropped = [6]) := by
  apply every_order; decide

theorem equal_resets_combine : EveryOrder (resets .store true) 7 (List.range 7) (fun r =>
    lookup r.state.resets .store = some 50 ∧ 5 ∈ r.kept ∧ 6 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order; decide

theorem later_reset : EveryOrder (resets .store) 8 (List.range 8) (fun r =>
    lookup r.state.resets .store = some 70 ∧ r.dropped = [6]) := by
  apply every_order; decide

def resetAndRaise (resetFirst : Bool) (audience : Audience)
    (causal : Bool) (same : Bool := false) : Log
  | 5 => entry 0 0 (List.range 5)
      (if resetFirst then .reset ⟨audience, (if same then 30 else 50)⟩
        else .raiseSchema 2 ⟨.store, 30⟩)
  | 6 => entry 1 1 (List.range (if causal then 6 else 5))
      (if resetFirst then .raiseSchema 2 ⟨.store, 30⟩
        else .reset ⟨audience, (if same then 30 else 50)⟩)
  | w => if audience == .store then household .admin .member w else gifts w

theorem reset_raise_examples_valid (resetFirst causal same : Bool) :
    validCheck (resetAndRaise resetFirst .store causal same) 7 = true ∧
    validCheck (resetAndRaise resetFirst (.circle 0) causal same) 7 = true := by
  cases resetFirst <;> cases causal <;> cases same <;> decide

/-- A reset and version raise compete even when they name the same snapshot. -/
theorem concurrent_reset_and_raise (resetFirst same : Bool) :
    EveryOrder (resetAndRaise resetFirst .store false same) 7 (List.range 7) (fun r =>
      lookup r.state.versions .store = (if resetFirst then none else some ⟨2, 30, 5⟩) ∧
      lookup r.state.resets .store =
        (if resetFirst then some (if same then 30 else 50) else none) ∧
      r.dropped = [6]) := by
  apply every_order
  cases resetFirst <;> cases same <;> decide

theorem causal_reset_and_raise (resetFirst : Bool) :
    EveryOrder (resetAndRaise resetFirst .store true) 7 (List.range 7) (fun r =>
      lookup r.state.versions .store = some ⟨2, 30, if resetFirst then 6 else 5⟩ ∧
      lookup r.state.resets .store = some 50 ∧ r.dropped = []) := by
  apply every_order
  cases resetFirst <;> decide

theorem circle_reset_and_store_raise (resetFirst : Bool) :
    EveryOrder (resetAndRaise resetFirst (.circle 0) false) 7 (List.range 7) (fun r =>
      lookup r.state.versions .store = some ⟨2, 30, if resetFirst then 6 else 5⟩ ∧
      lookup r.state.resets (.circle 0) = some 50 ∧ r.dropped = []) := by
  apply every_order
  cases resetFirst <;> decide

end CovenStorelog.Examples
