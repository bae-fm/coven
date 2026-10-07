import CovenStorelog.ExamplesRotations
import CovenStorelog.ExamplesVersions

namespace CovenStorelog.Examples

set_option maxRecDepth 32768
set_option maxHeartbeats 32000000

/-- Each snapshot raises only its audience, even when versions coincide. -/
def audienceUpdates (kind : VersionKind) : Log
  | 13 => entry 1 1 (List.range 11) (.raiseVersion kind 3 ⟨.circle 1, 40⟩)
  | w => rotations (.raiseVersion kind 9 ⟨.store, 90⟩)
      (.raiseVersion kind 3 ⟨.circle 0, 30⟩) w

theorem audience_updates_valid (kind : VersionKind) :
    validCheck (audienceUpdates kind) 14 = true := by
  cases kind <;> decide

theorem each_audience_has_its_version (kind : VersionKind) :
    EveryOrder (audienceUpdates kind) 14 (List.range 14) (fun r =>
      lookup r.state.versions (kind, .store) = some ⟨9, 90, 11⟩ ∧
      lookup r.state.versions (kind, .circle 0) = some ⟨3, 30, 12⟩ ∧
      lookup r.state.versions (kind, .circle 1) = some ⟨3, 40, 13⟩ ∧ r.dropped = []) := by
  apply every_order
  cases kind <;> decide

def circleUpdates (kind : VersionKind) (higherFirst sameVersion sameSnapshot : Bool) : Log :=
  rotations (.raiseVersion kind (if higherFirst then 3 else 2) ⟨.circle 0, 30⟩)
    (.raiseVersion kind (if sameVersion then (if higherFirst then 3 else 2)
      else (if higherFirst then 2 else 3)) ⟨.circle 0, if sameSnapshot then 30 else 40⟩)

theorem circle_updates_valid (kind : VersionKind) (higherFirst sameVersion sameSnapshot : Bool) :
    validCheck (circleUpdates kind higherFirst sameVersion sameSnapshot) 13 = true := by
  cases kind <;> cases higherFirst <;> cases sameVersion <;> cases sameSnapshot <;> decide

theorem circle_higher_version_wins (kind : VersionKind) (higherFirst : Bool) :
    EveryOrder (circleUpdates kind higherFirst false false) 13 (List.range 13) (fun r =>
      lookup r.state.versions (kind, .circle 0) =
        some ⟨3, if higherFirst then 30 else 40, if higherFirst then 11 else 12⟩ ∧
      11 ∈ r.kept ∧ 12 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order
  cases kind <;> cases higherFirst <;> decide

theorem circle_equal_version_snapshots (kind : VersionKind) (same : Bool) :
    EveryOrder (circleUpdates kind false true same) 13 (List.range 13) (fun r =>
      lookup r.state.versions (kind, .circle 0) = some ⟨2, 30, 11⟩ ∧
      r.dropped = (if same then [] else [12]) ∧
      reports (circleUpdates kind false true same) r 2 = (if same then [] else [12])) := by
  apply every_order
  cases kind <;> cases same <;> decide

def circleResetAndRaise (kind : VersionKind) (resetFirst causal same : Bool)
    (resetAudience : Audience := .circle 0) : Log
  | 11 => entry 0 0 (List.range 11)
      (if resetFirst then .reset ⟨resetAudience, if same then 30 else 50⟩
        else .raiseVersion kind 2 ⟨.circle 0, 30⟩)
  | 12 => entry 2 2 (List.range (if causal then 12 else 11))
      (if resetFirst then .raiseVersion kind 2 ⟨.circle 0, 30⟩
        else .reset ⟨resetAudience, if same then 30 else 50⟩)
  | w => rotations (.deleteCircle 0) (.deleteCircle 1) w

theorem circle_reset_raise_valid (kind : VersionKind) (resetFirst causal same : Bool) :
    validCheck (circleResetAndRaise kind resetFirst causal same) 13 = true ∧
    validCheck (circleResetAndRaise kind resetFirst causal same .store) 13 = true ∧
    validCheck (circleResetAndRaise kind resetFirst causal same (.circle 1)) 13 = true := by
  cases kind <;> cases resetFirst <;> cases causal <;> cases same <;> decide

theorem circle_concurrent_reset_and_raise (kind : VersionKind) (resetFirst same : Bool) :
    EveryOrder (circleResetAndRaise kind resetFirst false same) 13 (List.range 13) (fun r =>
      lookup r.state.versions (kind, .circle 0) =
        (if resetFirst then none else some ⟨2, 30, 11⟩) ∧
      lookup r.state.resets (.circle 0) =
        (if resetFirst then some (if same then 30 else 50) else none) ∧ r.dropped = [12]) := by
  apply every_order
  cases kind <;> cases resetFirst <;> cases same <;> decide

theorem circle_causal_reset_and_raise (kind : VersionKind) (resetFirst : Bool) :
    EveryOrder (circleResetAndRaise kind resetFirst true false) 13 (List.range 13) (fun r =>
      lookup r.state.versions (kind, .circle 0) = some ⟨2, 30, if resetFirst then 12 else 11⟩ ∧
      lookup r.state.resets (.circle 0) = some 50 ∧ r.dropped = []) := by
  apply every_order
  cases kind <;> cases resetFirst <;> decide

theorem circle_raise_and_other_reset (kind : VersionKind) (resetFirst store : Bool) :
    let audience := if store then Audience.store else .circle 1
    EveryOrder (circleResetAndRaise kind resetFirst false false audience) 13 (List.range 13)
      (fun r => lookup r.state.versions (kind, .circle 0) =
        some ⟨2, 30, if resetFirst then 12 else 11⟩ ∧
        lookup r.state.resets audience = some 50 ∧ r.dropped = []) := by
  dsimp only
  apply every_order
  cases kind <;> cases resetFirst <;> cases store <;> decide

def outsideRaise (kind : VersionKind) : Log
  | 4 => entry 1 1 (List.range 4) (.raiseVersion kind 2 ⟨.circle 0, 30⟩)
  | 5 => entry 0 0 (List.range 5) (.raiseVersion kind 2 ⟨.circle 0, 30⟩)
  | w => outsider (.deleteCircle 0) w

def leavingRaise (kind : VersionKind) : Log
  | 6 => entry 1 1 (List.range 5) (.raiseVersion kind 2 ⟨.circle 0, 30⟩)
  | w => leaveAndRename w

theorem circle_raise_authority_examples_valid (kind : VersionKind) :
    validCheck (outsideRaise kind) 6 = true ∧ validCheck (leavingRaise kind) 7 = true := by
  cases kind <;> decide

theorem outside_admin_cannot_repeat_raise (kind : VersionKind) :
    EveryOrder (outsideRaise kind) 6 (List.range 6) (fun r =>
      lookup r.state.versions (kind, .circle 0) = some ⟨2, 30, 4⟩ ∧
      admin r.state 0 = true ∧ r.dropped = [5]) := by
  apply every_order
  cases kind <;> decide

theorem removed_circle_member_can_raise_concurrently (kind : VersionKind) :
    EveryOrder (leavingRaise kind) 7 (List.range 7) (fun r =>
      lookup r.state.versions (kind, .circle 0) = some ⟨2, 30, 6⟩ ∧
      inCircle r.state 0 1 = false ∧
      inCircle (authorView (leavingRaise kind) 6) 0 1 = true ∧ r.dropped = []) := by
  apply every_order
  cases kind <;> decide

def deletingRaise (kind : VersionKind) (raiseFirst : Bool) : Log
  | 4 => entry (if raiseFirst then 1 else 0) (if raiseFirst then 1 else 0) (List.range 4)
      (if raiseFirst then .raiseVersion kind 2 ⟨.circle 0, 30⟩ else .removeMember 1 [])
  | 5 => entry (if raiseFirst then 0 else 1) (if raiseFirst then 0 else 1) (List.range 4)
      (if raiseFirst then .removeMember 1 [] else .raiseVersion kind 2 ⟨.circle 0, 30⟩)
  | w => outsider (.deleteCircle 0) w

/-- Ana removes Ben from the store; Ben's private circle disappears. -/
theorem derived_circle_deletion_beats_raise (kind : VersionKind) (raiseFirst : Bool) :
    validCheck (deletingRaise kind raiseFirst) 6 = true ∧
    EveryOrder (deletingRaise kind raiseFirst) 6 (List.range 6) (fun r =>
      lookup r.state.circles 0 = none ∧ lookup r.state.versions (kind, .circle 0) = none ∧
      r.dropped = [if raiseFirst then 4 else 5]) := by
  constructor
  · cases kind <;> cases raiseFirst <;> decide
  · apply every_order
    cases kind <;> cases raiseFirst <;> decide

theorem explicit_circle_deletion_beats_raise (kind : VersionKind) :
    validCheck (deletion (.raiseVersion kind 2 ⟨.circle 0, 30⟩)) 7 = true ∧
    EveryOrder (deletion (.raiseVersion kind 2 ⟨.circle 0, 30⟩)) 7 (List.range 7) (fun r =>
      lookup r.state.circles 0 = none ∧ lookup r.state.versions (kind, .circle 0) = none ∧
      r.dropped = [5]) := by
  constructor
  · cases kind <;> decide
  · apply every_order
    cases kind <;> decide

end CovenStorelog.Examples
