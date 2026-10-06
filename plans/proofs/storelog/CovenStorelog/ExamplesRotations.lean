import CovenStorelog.ExamplesCircles

namespace CovenStorelog.Examples

set_option maxRecDepth 32768
set_option maxHeartbeats 32000000

/-- Ana, Ben and Carol share two circles; two devices then act concurrently. -/
def rotations (first second : Action) : Log
  | 5 => entry 0 0 (List.range 5) (.makeCircle 0 "Gifts")
  | 6 => entry 0 0 (List.range 6) (.addToCircle 0 1)
  | 7 => entry 0 0 (List.range 7) (.addToCircle 0 2)
  | 8 => entry 0 0 (List.range 8) (.makeCircle 1 "Notes")
  | 9 => entry 0 0 (List.range 9) (.addToCircle 1 1)
  | 10 => entry 0 0 (List.range 10) (.addToCircle 1 2)
  | 11 => entry 0 0 (List.range 11) first
  | 12 => entry 2 2 (List.range 11) second
  | w => household .admin .admin w

theorem rotation_examples_valid :
    validCheck (rotations (.removeFromCircle 0 1) (.removeFromCircle 0 2)) 13 = true ∧
    validCheck (rotations (.removeFromCircle 0 1) (.removeFromCircle 1 2)) 13 = true ∧
    validCheck (rotations (.removeMember 2 [0, 1]) (.removeFromCircle 0 1)) 13 = true ∧
    validCheck (rotations (.removeMember 1 [0, 1]) (.removeMember 1 [0, 1])) 13 = true ∧
    validCheck (rotations (.removeFromCircle 0 1) (.removeFromCircle 0 1)) 13 = true := by
  decide

theorem same_circle_rotations_conflict :
    EveryOrder (rotations (.removeFromCircle 0 1) (.removeFromCircle 0 2)) 13 (List.range 13)
      (fun r => lookup r.state.circles 0 = some ⟨"Gifts", [2, 0]⟩ ∧
        r.dropped = [12]) := by
  apply every_order; decide

theorem different_circle_rotations_both_apply :
    EveryOrder (rotations (.removeFromCircle 0 1) (.removeFromCircle 1 2)) 13 (List.range 13)
      (fun r => lookup r.state.circles 0 = some ⟨"Gifts", [2, 0]⟩ ∧
        lookup r.state.circles 1 = some ⟨"Notes", [1, 0]⟩ ∧ r.dropped = []) := by
  apply every_order; decide

theorem store_rotation_beats_later_circle_rotation :
    EveryOrder (rotations (.removeMember 2 [0, 1]) (.removeFromCircle 0 1)) 13 (List.range 13)
      (fun r => member r.state 2 = false ∧
        lookup r.state.circles 0 = some ⟨"Gifts", [1, 0]⟩ ∧ r.dropped = [12]) := by
  apply every_order; decide

theorem repeated_store_removals_keep_both :
    EveryOrder (rotations (.removeMember 1 [0, 1]) (.removeMember 1 [0, 1])) 13 (List.range 13)
      (fun r => member r.state 1 = false ∧ 11 ∈ r.kept ∧ 12 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order; decide

theorem repeated_circle_removals_keep_both :
    EveryOrder (rotations (.removeFromCircle 0 1) (.removeFromCircle 0 1)) 13 (List.range 13)
      (fun r => lookup r.state.circles 0 = some ⟨"Gifts", [2, 0]⟩ ∧
        11 ∈ r.kept ∧ 12 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order; decide

end CovenStorelog.Examples
