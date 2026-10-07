import CovenStorelog.ExamplesMembers

namespace CovenStorelog.Examples

set_option maxRecDepth 32768
set_option maxHeartbeats 32000000

def gifts : Log
  | 3 => entry 0 0 [0, 1, 2] (.makeCircle 0 "Gifts")
  | 4 => entry 0 0 [0, 1, 2, 3] (.addToCircle 0 1)
  | w => household .member .member w

def renames : Log
  | 5 => entry 0 0 (List.range 5) (.renameCircle 0 "Birthdays")
  | 6 => entry 1 1 (List.range 5) (.renameCircle 0 "Presents")
  | w => gifts w

def deletion (change : Action) : Log
  | 5 => entry 0 0 (List.range 5) change
  | 6 => entry 1 1 (List.range 5) (.deleteCircle 0)
  | w => gifts w

def circleRotation : Log
  | 5 => entry 0 0 (List.range 5) (.addMember 2 .member "initial")
  | 6 => entry 0 0 (List.range 6) (.addToCircle 0 2)
  | 7 => entry 1 1 (List.range 6) (.removeFromCircle 0 0)
  | w => gifts w

def leaveAndRename : Log
  | 5 => entry 0 0 (List.range 5) (.removeFromCircle 0 1)
  | 6 => entry 1 1 (List.range 5) (.renameCircle 0 "Birthdays")
  | w => gifts w

def outsider (change : Action) : Log
  | 3 => entry 1 1 [0, 1, 2] (.makeCircle 0 "Gifts")
  | 4 => entry 0 0 [0, 1, 2, 3] change
  | w => household .member .member w

def lastLeaves : Log
  | 3 => entry 1 1 [0, 1, 2] (.makeCircle 0 "Gifts")
  | 4 => entry 1 1 [0, 1, 2, 3] (.removeFromCircle 0 1)
  | w => household .member .member w

/-- Ana is outside Carol and Ben's Gifts; Carol also has a private circle. -/
def carolsCircles : Log
  | 5 => entry 2 2 (List.range 5) (.makeCircle 0 "Gifts")
  | 6 => entry 2 2 (List.range 6) (.addToCircle 0 1)
  | 7 => entry 2 2 (List.range 7) (.makeCircle 1 "Carol's notes")
  | 8 => entry 0 0 (List.range 8) (.removeMember 2 [0])
  | w => household .member .member w

/-- Removing Carol rotates Gifts' key and defeats Ben's concurrent add
of Dan, even though Ana has never been a member of Gifts. -/
def removeCarolAndAddDan : Log
  | 7 => entry 0 0 (List.range 7) (.addMember 3 .member "initial")
  | 8 => entry 1 1 (List.range 8) (.addToCircle 0 3)
  | 9 => entry 0 0 (List.range 8) (.removeMember 2 [0])
  | w => carolsCircles w

theorem circle_examples_valid :
    validCheck gifts 5 = true ∧ validCheck renames 7 = true ∧
    validCheck (deletion (.renameCircle 0 "Birthdays")) 7 = true ∧
    validCheck (deletion (.reset ⟨.circle 0, 50⟩)) 7 = true ∧
    validCheck circleRotation 8 = true ∧ validCheck leaveAndRename 7 = true ∧
    validCheck (outsider (.deleteCircle 0)) 5 = true ∧ validCheck lastLeaves 5 = true ∧
    validCheck carolsCircles 9 = true ∧ validCheck removeCarolAndAddDan 10 = true := by decide

theorem circle_created_by_member : EveryOrder (outsider (.deleteCircle 0)) 4 (List.range 4)
    (fun r => lookup r.state.circles 0 = some ⟨"Gifts", [1]⟩ ∧
      admin r.state 1 = false ∧ r.dropped = []) := by
  apply every_order; decide

theorem circle_renamed : EveryOrder renames 7 (List.range 7) (fun r =>
    lookup r.state.circles 0 = some ⟨"Presents", [1, 0]⟩ ∧
    5 ∈ r.kept ∧ 6 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order; decide

/-- The §14.7 store-log deletion; Appendix B checks the associated row loss. -/
theorem circle_deleted :
    EveryOrder (deletion (.renameCircle 0 "Birthdays")) 7 (List.range 7) (fun r =>
      lookup r.state.circles 0 = none ∧ r.dropped = [5]) := by
  apply every_order; decide

theorem circle_delete_beats_reset :
    EveryOrder (deletion (.reset ⟨.circle 0, 50⟩)) 7 (List.range 7) (fun r =>
      lookup r.state.circles 0 = none ∧ lookup r.state.resets (.circle 0) = none ∧
      r.dropped = [5]) := by
  apply every_order; decide

theorem circle_key_rotation : EveryOrder circleRotation 8 (List.range 8) (fun r =>
    lookup r.state.circles 0 = some ⟨"Gifts", [1]⟩ ∧ r.dropped = [6]) := by
  apply every_order; decide

/-- §14.6: Ben loses circle membership but keeps store membership. His
concurrent rename still has authority in his recorded circle view. -/
theorem circle_leaving : EveryOrder leaveAndRename 7 (List.range 7) (fun r =>
    lookup r.state.circles 0 = some ⟨"Birthdays", [0]⟩ ∧ member r.state 1 = true ∧
    inCircle (authorView leaveAndRename 6) 0 1 = true ∧ r.dropped = []) := by
  apply every_order; decide

/-- Store administration does not confer circle management rights. -/
theorem outsider_cannot_manage :
    ∀ action ∈ ([.renameCircle 0 "Gifts", .renameCircle 0 "Birthdays", .addToCircle 0 0,
      .removeFromCircle 0 1, .deleteCircle 0, .reset ⟨.circle 0, 50⟩] : List Action),
    EveryOrder (outsider action) 5 (List.range 5) (fun r =>
      lookup r.state.circles 0 = some ⟨"Gifts", [1]⟩ ∧ admin r.state 0 = true ∧
      r.dropped = [4]) := by
  intro action ha
  simp only [List.mem_cons, List.not_mem_nil, or_false] at ha
  rcases ha with rfl | rfl | rfl | rfl | rfl | rfl <;> apply every_order <;> decide

theorem empty_circle_deleted : EveryOrder lastLeaves 5 (List.range 5) (fun r =>
    lookup r.state.circles 0 = none ∧ member r.state 1 = true ∧ r.dropped = []) := by
  apply every_order; decide

/-- §13: remove Carol from Gifts, remove her devices, and delete the circle
where she was alone. Ana remains outside Gifts. -/
theorem member_removal_updates_circles : EveryOrder carolsCircles 9 (List.range 9) (fun r =>
    member r.state 2 = false ∧ lookup r.state.devices 2 = none ∧
    lookup r.state.circles 0 = some ⟨"Gifts", [1]⟩ ∧ lookup r.state.circles 1 = none ∧
    inCircle r.state 0 0 = false ∧ r.dropped = []) := by
  apply every_order; decide

theorem store_removal_beats_circle_add :
    EveryOrder removeCarolAndAddDan 10 (List.range 10) (fun r =>
      member r.state 2 = false ∧ member r.state 3 = true ∧
      lookup r.state.circles 0 = some ⟨"Gifts", [1]⟩ ∧ r.dropped = [8] ∧
      reports removeCarolAndAddDan r 1 = [8]) := by
  apply every_order; decide

end CovenStorelog.Examples
