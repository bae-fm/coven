import CovenMerge.OwnerProtection

namespace CovenMerge.Ownership.Examples


/-- Ben's parent, Ana's concurrent child. The rows are merged facts, before
removals. Destination membership is the replayed result, not the mover's read. -/
def moving (child readable : Bool) : MoveState Nat Nat Nat where
  rows := if child then [0, 1] else [0]
  present r := r == 0 || (child && r == 1)
  owner r := if r = 0 then some 0 else if r = 1 then some 1 else none
  parent r := if r = 1 then some 0 else none
  canRead m _ := m == 0 || readable

theorem concurrent_child_blocks :
    moveDenied (moving false false) 0 1 = false ∧
    moveDenied (moving true false) 0 1 = true := by decide

theorem concurrent_membership_blocks :
    moveDenied (moving true true) 0 1 = false ∧
    moveDenied (moving true false) 0 1 = true := by decide

/-- Depth does not stop protection at the moved parent's direct children. -/
example : carried [0, 1, 2] (fun r => if r = 1 then some 0 else if r = 2 then some 1 else none) 0 2 = true := by decide

def family (deleted : Bool) : Inputs Nat Nat where
  rows := [0, 1]
  present r := (r == 0 && !deleted) || r == 1
  refs r := if r = 1 then [⟨0, deleted⟩] else []
  checkFails _ := false
  inDeletedCircle _ := false
  claims _ := []
  rank := id

/-- Ordinary CHECK removal does not erase the row used for move protection. -/
theorem hidden_child_still_blocks :
    (view { family false with checkFails := fun r => r == 1 }).shown 1 = false ∧
    moveDenied (moving true false) 0 1 = true := by decide

end CovenMerge.Ownership.Examples
