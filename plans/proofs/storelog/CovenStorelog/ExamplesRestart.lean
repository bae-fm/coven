import CovenStorelog.ExamplesSupport

namespace CovenStorelog.Examples

set_option maxRecDepth 16384
set_option maxHeartbeats 8000000

/-- Ana and Ben start as admins. Ben has two registered devices. Three
concurrent entries add Carol as admin, demote Ben, and remove Ana. -/
def losingRemoval : Log
  | 0 => entry 0 0 [] .create
  | 1 => entry 0 0 [0] (.addMember 1 .admin)
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 1 4 [0, 1, 2] (.addDevice 1 4)
  | 4 => entry 0 0 [0, 1, 2, 3] (.addMember 2 .admin)
  | 5 => entry 1 1 [0, 1, 2, 3] (.changeRole 1 .member)
  | _ => entry 1 4 [0, 1, 2, 3] (.removeMember 0 [])

theorem history_valid : validCheck losingRemoval 7 = true ∧
    causalCheck losingRemoval [] [0, 1, 2, 3, 4, 5, 6] = true ∧
    causalCheck losingRemoval [] [0, 1, 2, 3, 6, 5, 4] = true := by decide

theorem valid_history : Valid losingRemoval 7 :=
  validCheck_sound losingRemoval 7 history_valid.1

theorem causal_arrivals : CausalOrder losingRemoval (List.range 7) ∧
    CausalOrder losingRemoval [0, 1, 2, 3, 6, 5, 4] := by
  exact ⟨full_history_causal losingRemoval 7 valid_history,
    causalCheck_sound losingRemoval .nil history_valid.2.2⟩

theorem entries_concurrent : concurrent losingRemoval 4 5 = true ∧
    concurrent losingRemoval 4 6 = true ∧ concurrent losingRemoval 5 6 = true := by decide

theorem each_entry_allowed_in_its_view :
    ([4, 5, 6] : List Nat).all (fun w =>
      authorized (authorView losingRemoval w) (losingRemoval w) &&
      (checkedEffect (authorView losingRemoval w) w (losingRemoval w)).isSome) = true := by decide

theorem first_pass_drops_add :
    scan losingRemoval (authorViews losingRemoval 7) (List.range 7)
      ⟨State.empty, [], []⟩ = .restart [4] := by decide

theorem second_pass_drops_removal :
    (scan losingRemoval (authorViews losingRemoval 7) (List.range 7)
      ⟨State.empty, [], [4]⟩) =
      .complete ⟨{
        created := true
        members := [(1, .member), (0, .admin)]
        devices := [(4, 1), (1, 1), (0, 0)]
        circles := [], versions := [], resets := [] }, [5, 3, 2, 1, 0], [6, 4]⟩ := by decide

/-- Carol is declined even though the removal that beat her add is itself
dropped. This is the result in every causal arrival order. -/
theorem losing_removal_discards_add :
    EveryOrder losingRemoval 7 (List.range 7) (fun r =>
      admin r.state 0 = true ∧ lookup r.state.members 1 = some .member ∧
      member r.state 2 = false ∧ r.dropped = [6, 4] ∧
      reports losingRemoval r 0 = [4] ∧ reports losingRemoval r 1 = [6]) := by
  apply every_order
  decide

/-- With only the applied entries and Carol's add, the add succeeds; no
applied entry opposes it. Its only opponent is the dropped removal. -/
theorem add_has_no_surviving_opponent :
    (resolve losingRemoval 7 (entrySet [0, 1, 2, 3, 4, 5])).dropped = [] ∧
    admin (resolve losingRemoval 7 (entrySet [0, 1, 2, 3, 4, 5])).state 2 = true ∧
    ((resolve losingRemoval 7 (entrySet (List.range 7))).kept.all
      (fun w => !pairConflict losingRemoval (authorViews losingRemoval 7) 4 w)) = true := by decide

/-- At the final state Carol's add has authority, would change the state,
preserves an admin, and has no surviving concurrent opponent. It is still
reported as dropped because drops persist through the restart. -/
theorem dropped_add_meets_conditions :
    let r := resolve losingRemoval 7 (entrySet (List.range 7))
    4 ∈ r.dropped ∧
    authorized (authorView losingRemoval 4) (losingRemoval 4) = true ∧
    alreadyInPlace r.state (losingRemoval 4) = false ∧
    (checkedEffect r.state 4 (losingRemoval 4)).isSome = true ∧
    r.kept.all (fun w => !pairConflict losingRemoval (authorViews losingRemoval 7) 4 w) = true := by decide

end CovenStorelog.Examples
