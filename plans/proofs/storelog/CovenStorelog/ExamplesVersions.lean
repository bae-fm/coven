import CovenStorelog.ExamplesCircles

namespace CovenStorelog.Examples

set_option maxRecDepth 32768
set_option maxHeartbeats 32000000

def updates (kind : VersionKind) (secondSnapshot : Nat) : Log
  | 3 => entry 0 0 [0, 1, 2] (.raiseVersion kind 2 30)
  | 4 => entry 1 1 [0, 1, 2] (.raiseVersion kind 2 secondSnapshot)
  | 5 => entry 1 1 [0, 1, 2, 4] (.raiseVersion kind 3 50)
  | w => household .member .member w

def resets (audience : Audience) (same : Bool := false) : Log
  | 5 => entry 0 0 (List.range 5) (.reset audience 50)
  | 6 => entry 1 1 (List.range 5) (.reset audience (if same then 50 else 60))
  | 7 => entry 0 0 (List.range 7) (.reset audience 70)
  | w => if audience == .store then household .admin .member w else gifts w

theorem version_examples_valid :
    validCheck (updates .schema 40) 6 = true ∧ validCheck (updates .format 40) 6 = true ∧
    validCheck (resets .store) 8 = true ∧ validCheck (resets (.circle 0)) 8 = true ∧
    validCheck (resets .store true) 7 = true ∧
    validCheck (updates .schema 30) 5 = true ∧ validCheck (updates .format 30) 5 = true := by decide

/-- §17: ordinary members can raise both versions. Different snapshots for
the same concurrent raise conflict, and the later entry is reported. -/
theorem same_version_snapshot (kind : VersionKind) :
    EveryOrder (updates kind 40) 5 (List.range 5) (fun r =>
      lookup r.state.versions kind = some ⟨2, 30, 3⟩ ∧ r.dropped = [4] ∧
      reports (updates kind 40) r 1 = [4]) := by
  apply every_order
  cases kind <;> decide

theorem version_raised (kind : VersionKind) :
    EveryOrder (updates kind 40) 6 (List.range 6) (fun r =>
      lookup r.state.versions kind = some ⟨3, 50, 5⟩ ∧ r.dropped = [4]) := by
  apply every_order
  cases kind <;> decide

/-- The in-place rule keeps both identities when the version and snapshot agree. -/
theorem identical_version_raises (kind : VersionKind) :
    EveryOrder (updates kind 30) 5 (List.range 5) (fun r =>
      lookup r.state.versions kind = some ⟨2, 30, 3⟩ ∧
      3 ∈ r.kept ∧ 4 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order
  cases kind <;> decide

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

end CovenStorelog.Examples
