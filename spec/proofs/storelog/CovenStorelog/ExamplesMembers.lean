import CovenStorelog.ExamplesRestart
import CovenStorelog.Bootstrap

namespace CovenStorelog.Examples

set_option maxRecDepth 16384
set_option maxHeartbeats 16000000

/-- Ana creates, adds Ben, Ben registers, Ana adds Carol, Carol registers. -/
def household (ben carol : Role) : Log
  | 0 => entry 0 0 [] (.create "initial")
  | 1 => entry 0 0 [0] (.addMember 1 ben "initial")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 1)
  | 3 => entry 0 0 [0, 1, 2] (.addMember 2 carol "initial")
  | _ => entry 2 2 [0, 1, 2, 3] (.addDevice 2 2)

def both : Log
  | 4 => entry 0 0 [0, 1, 2, 3] (.addMember 3 .member "initial")
  | 5 => entry 1 1 [0, 1, 2, 3] (.changeRole 2 .admin)
  | w => household .admin .member w

def phoneAndRemoval : Log
  | 3 => entry 1 4 [0, 1, 2] (.addDevice 1 4)
  | 4 => entry 0 0 [0, 1, 2] (.removeMember 1 [])
  | w => household .admin .member w

def roles : Log
  | 5 => entry 0 0 [0, 1, 2, 3, 4] (.changeRole 1 .admin)
  | 6 => entry 2 2 [0, 1, 2, 3, 4] (.changeRole 1 .member)
  | w => household .member .admin w

def mutualRemovals : Log
  | 3 => entry 0 0 [0, 1, 2] (.removeMember 1 [])
  | 4 => entry 1 1 [0, 1, 2] (.removeMember 0 [])
  | w => household .admin .member w

def carol : Log
  | 3 => entry 0 0 [0, 1, 2] (.addMember 3 .member "initial")
  | 4 => entry 0 0 [0, 1, 2, 3] (.addMember 2 .member "initial")
  | 5 => entry 1 1 [0, 1, 2, 3] (.removeMember 3 [])
  | 6 => entry 2 2 [0, 1, 2, 3, 4] (.addDevice 2 2)
  | w => household .admin .member w

def equalAdds : Log
  | 3 => entry 0 0 [0, 1, 2] (.addMember 3 .member "initial")
  | 4 => entry 1 1 [0, 1, 2] (.addMember 3 .member "initial")
  | w => household .admin .member w

def threeRemovals : Log
  | 5 => entry 0 0 [0, 1, 2, 3, 4] (.removeMember 1 [])
  | 6 => entry 1 1 [0, 1, 2, 3, 4] (.removeMember 2 [])
  | 7 => entry 2 2 [0, 1, 2, 3, 4] (.removeMember 0 [])
  | w => household .admin .admin w

def removedAuthor : Log
  | 6 => entry 1 1 [0, 1, 2, 3, 4] (.changeRole 2 .member)
  | w => threeRemovals w

def removalAndPhone : Log
  | 4 => entry 1 1 [0, 1, 2, 3] (.removeMember 0 [])
  | 5 => entry 0 0 [0, 1, 2, 3] (.removeMember 1 [])
  | 6 => entry 1 4 [0, 1, 2, 3] (.addDevice 1 5)
  | w => losingRemoval w

/-- §9's opening log: Ben registers his phone while Ana's iPad promotes him. -/
def openingLog : Log
  | 0 => entry 0 0 [] (.create "initial")
  | 1 => entry 0 0 [0] (.addMember 1 .member "initial")
  | 2 => entry 1 1 [0, 1] (.addDevice 1 2)
  | _ => entry 0 3 [0, 1] (.changeRole 1 .admin)

/-- The entry examples are valid, so their universal causal-order claims
have actual arrival orders (in particular, timestamp order). -/
theorem member_examples_valid :
    validCheck both 6 = true ∧ validCheck phoneAndRemoval 5 = true ∧ validCheck roles 7 = true ∧
    validCheck mutualRemovals 5 = true ∧ validCheck carol 7 = true ∧
    validCheck equalAdds 5 = true ∧ validCheck threeRemovals 8 = true ∧
    validCheck removalAndPhone 7 = true ∧ validCheck openingLog 4 = true ∧
    validCheck removedAuthor 7 = true := by decide

/-- Both changes concern Ben; the admin grant loses to the device addition. -/
theorem opening_log : EveryOrder openingLog 4 (List.range 4) (fun r =>
    lookup r.state.members 1 = some .member ∧ lookup r.state.devices 2 = some 1 ∧
    r.dropped = [3] ∧ reports openingLog r 0 = [3]) := by
  apply every_order; decide

/-- §9 table, row 1. -/
theorem example_add_and_promote : EveryOrder both 6 (List.range 6) (fun r =>
    member r.state 3 = true ∧ admin r.state 2 = true ∧ r.dropped = []) := by
  apply every_order; decide

/-- §9 table, row 2: removing Ben defeats his concurrent phone addition. -/
theorem example_member_removal_beats_phone : EveryOrder phoneAndRemoval 5 (List.range 5) (fun r =>
    member r.state 1 = false ∧ lookup r.state.devices 4 = none ∧
    4 ∈ r.kept ∧ r.dropped = [3] ∧ reports phoneAndRemoval r 1 = [3]) := by
  apply every_order; decide

/-- §12.1: the new phone registers itself before the concurrent removal arrives. -/
theorem new_device_registers_itself : EveryOrder phoneAndRemoval 4 (List.range 4) (fun r =>
    lookup r.state.devices 4 = some 1 ∧ r.dropped = []) := by
  apply every_order; decide

/-- Even an admin must have read the device's addition, with the matching owner. -/
theorem device_removal_checks_owner :
    let view := authorView phoneAndRemoval 3
    authorized view (entry 0 0 [0, 1, 2] (.removeDevice 1 4)) = false ∧
    authorized view (entry 0 0 [0, 1, 2] (.removeDevice 0 1)) = false ∧
    authorized view (entry 0 0 [0, 1, 2] (.removeDevice 1 1)) = true ∧
    authorized view (entry 1 1 [0, 1, 2] (.removeDevice 1 1)) = true := by decide

/-- §9 table, row 3. -/
theorem example_lower_role : EveryOrder roles 7 (List.range 7) (fun r =>
    lookup r.state.members 1 = some .member ∧ r.dropped = [5]) := by
  apply every_order; decide

/-- §9 table, row 4. -/
theorem example_mutual_removal : EveryOrder mutualRemovals 5 (List.range 5) (fun r =>
    admin r.state 0 = true ∧ member r.state 1 = false ∧ r.dropped = [4]) := by
  apply every_order; decide

/-- §9 table, row 5, and §13. -/
theorem example_carol : EveryOrder carol 6 (List.range 6) (fun r =>
    member r.state 2 = false ∧ member r.state 3 = false ∧ r.dropped = [4] ∧
    reports carol r 0 = [4]) := by
  apply every_order; decide

theorem carol_device : EveryOrder carol 7 (List.range 7) (fun r =>
    member r.state 2 = false ∧ lookup r.state.devices 2 = none ∧
    r.dropped = [6, 4] ∧ reports carol r 0 = [4] ∧ reports carol r 2 = [6]) := by
  apply every_order; decide

/-- Equal effects keep both identities and have no drop reports. -/
theorem equal_adds_combine : EveryOrder equalAdds 5 (List.range 5) (fun r =>
    lookup r.state.members 3 = some .member ∧ 3 ∈ r.kept ∧ 4 ∈ r.kept ∧ r.dropped = []) := by
  apply every_order; decide

theorem three_admins : EveryOrder threeRemovals 8 (List.range 8) (fun r =>
    r.state.members = [(2, .admin), (0, .admin)] ∧
    r.state.devices = [(2, 2), (0, 0)] ∧ 5 ∈ r.kept ∧ r.dropped = [7, 6] ∧
    reports threeRemovals r 1 = [6] ∧ reports threeRemovals r 2 = [7]) := by
  apply every_order; decide

/-- Ben's concurrent role change still has authority after Ana removes him. -/
theorem removed_author_view :
    admin (authorView removedAuthor 6) 1 = true ∧
    EveryOrder removedAuthor 7 (List.range 7) (fun r => member r.state 1 = false ∧
      lookup r.state.members 2 = some .member ∧ 6 ∈ r.kept ∧ r.dropped = []) := by
  constructor
  · decide
  · apply every_order; decide

theorem dropped_removal_allows_phone : EveryOrder removalAndPhone 7 (List.range 7) (fun r =>
    lookup r.state.members 1 = some .admin ∧ member r.state 0 = false ∧
    lookup r.state.devices 5 = some 1 ∧ r.dropped = [5]) := by
  apply every_order; decide

/-- A member's own access update supplies a later removal's access record. -/
def replacedAccess : Log
  | 3 => entry 1 1 [0, 1, 2] (.setAccess 1 "replacement")
  | 4 => entry 0 0 [0, 1, 2, 3] (.removeMember 1 [])
  | w => household .member .member w

def concurrentAccessRemoval : Log
  | 4 => entry 0 0 [0, 1, 2] (.removeMember 1 [])
  | w => replacedAccess w

theorem access_examples_valid : validCheck replacedAccess 5 = true ∧
    validCheck concurrentAccessRemoval 5 = true := by decide

theorem replacement_then_removal : EveryOrder replacedAccess 5 (List.range 5) (fun r =>
    member r.state 1 = false ∧ lookup r.state.access 1 = some "replacement" ∧
    r.dropped = []) := by
  apply every_order; decide

theorem removal_defeats_access : EveryOrder concurrentAccessRemoval 5 (List.range 5) (fun r =>
    member r.state 1 = false ∧ lookup r.state.access 1 = some "initial" ∧
    r.dropped = [3]) := by
  apply every_order; decide

end CovenStorelog.Examples
