import CovenStorelog.Admission

namespace CovenStorelog.Examples.CircleDeletion

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- §9: Ana and Ben are admins and share Gifts. Ana's phone removes Ben
from Gifts while her tablet removes Ana from the store. Both had read Gifts
with two members. -/
def history : Log
  | 0 => entry 0 0 [] .create
  | 1 => entry 0 0 [0] (.addMember 1 .admin)
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 [0, 1, 2] (.makeCircle 0 "Gifts")
  | 4 => entry 0 0 [0, 1, 2, 3] (.addToCircle 0 1)
  | 5 => entry 0 0 (List.range 5) (.removeFromCircle 0 1)
  | _ => entry 0 4 (List.range 5) (.removeMember 0 [0])

theorem valid_history : Valid history 7 := validCheck_sound _ _ (by decide)

theorem causal_arrivals : CausalOrder history [0, 1, 2, 3, 4, 5, 6] ∧
    CausalOrder history [0, 1, 2, 3, 4, 6, 5] := by
  constructor <;> apply causalCheck_sound _ .nil <;> decide

/-- Neither entry deletes Gifts in its author's view, and they are about
different members, so they don't conflict. -/
theorem no_conflict :
    lookup (authorView history 5).circles 0 = some ⟨"Gifts", [1, 0]⟩ ∧
    lookup (authorView history 6).circles 0 = some ⟨"Gifts", [1, 0]⟩ ∧
    deletesCircle (authorView history 5) (history 5).action 0 = false ∧
    deletesCircle (authorView history 6) (history 6).action 0 = false ∧
    concurrent history 5 6 = true ∧
    pairConflict history (authorViews history 7) 5 6 = false := by decide

/-- Both apply: Ana leaves the store, Ben stays its admin, and Gifts, left
with no members, is deleted. -/
theorem both_apply : EveryOrder history 7 (List.range 7) (fun r =>
    member r.state 0 = false ∧ admin r.state 1 = true ∧
    lookup r.state.circles 0 = none ∧ r.dropped = []) := by
  apply every_order; decide

/-- Ben is alone in his circle. He renames it while Ana, concurrently,
removes him from the store. In Ana's view the removal takes the circle's
only member, so it deletes the circle, and it beats the rename. -/
def alone : Log
  | 0 => entry 0 0 [] .create
  | 1 => entry 0 0 [0] (.addMember 1 .member)
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 1 1 [0, 1, 2] (.makeCircle 0 "Ben's notes")
  | 4 => entry 1 1 [0, 1, 2, 3] (.renameCircle 0 "Journal")
  | _ => entry 0 0 [0, 1, 2, 3] (.removeMember 1 [])

theorem alone_valid : Valid alone 6 := validCheck_sound _ _ (by decide)

theorem alone_conflict :
    deletesCircle (authorView alone 5) (alone 5).action 0 = true ∧
    pairConflict alone (authorViews alone 6) 5 4 = true ∧ before alone 5 4 = true := by
  decide

theorem removal_beats_rename : EveryOrder alone 6 (List.range 6) (fun r =>
    member r.state 1 = false ∧ lookup r.state.circles 0 = none ∧
    r.dropped = [4] ∧ reports alone r 1 = [4]) := by
  apply every_order; decide

end CovenStorelog.Examples.CircleDeletion
