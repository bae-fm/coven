import CovenStorelog.Admission

namespace CovenStorelog.Examples.Gifts

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Ana's tablet adds Ben, then Carol; her phone concurrently removes Ben
from the store. The removal's circle-key list is empty: its view of Gifts
contains Ana alone. -/
def history : Log
  | 0 => entry 0 0 [] (.create "initial")
  | 1 => entry 0 0 [0] (.addMember 1 .member "initial")
  | 2 => entry 0 0 [0, 1] (.addMember 2 .member "initial")
  | 3 => entry 0 0 [0, 1, 2] (.makeCircle 0 "Gifts")
  | 4 => entry 0 4 [0, 1, 2, 3] (.addToCircle 0 1)
  | 5 => entry 0 4 [0, 1, 2, 3, 4] (.addToCircle 0 2)
  | _ => entry 0 0 [0, 1, 2, 3] (.removeMember 1 [])

theorem valid_history : Valid history 7 := validCheck_sound _ _ (by decide)

theorem causal_arrivals : CausalOrder history [0, 1, 2, 3, 4, 5, 6] ∧
    CausalOrder history [0, 1, 2, 3, 6, 4, 5] := by
  constructor <;> apply causalCheck_sound _ .nil <;> decide

theorem removal_names_no_circle :
    lookup (authorView history 6).circles 0 = some ⟨"Gifts", [0]⟩ ∧
    (history 6).action = .removeMember 1 [] ∧
    removesCircleKey (history 6).action 0 = false ∧
    concurrent history 6 4 = true ∧ concurrent history 6 5 = true := by decide

/-- Ben's addition is about the removed member. Carol's is about neither
Ben nor a circle named by the removal. -/
theorem conflicts :
    pairConflict history (authorViews history 7) 6 4 = true ∧
    pairConflict history (authorViews history 7) 6 5 = false := by decide

theorem first_pass :
    scan history (authorViews history 7) (List.range 7) ⟨State.empty, [], []⟩ =
      .restart [4] := by decide

/-- §9's Gifts example, including kept identities and the report to Ana. -/
theorem carol_stays : EveryOrder history 7 (List.range 7) (fun r =>
    lookup r.state.circles 0 = some ⟨"Gifts", [2, 0]⟩ ∧
    member r.state 1 = false ∧ member r.state 2 = true ∧
    5 ∈ r.kept ∧ 6 ∈ r.kept ∧ r.dropped = [4] ∧ reports history r 0 = [4]) := by
  apply every_order; decide

end CovenStorelog.Examples.Gifts
