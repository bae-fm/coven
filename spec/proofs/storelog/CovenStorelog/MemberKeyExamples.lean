import CovenStorelog.MemberKeys

namespace CovenStorelog.CurrentReplay.Keyed.Examples

set_option maxRecDepth 8192
set_option maxHeartbeats 4000000

def storeKey : KeyCommitment := ⟨10, Vector.replicate 32 0⟩
def ana : MemberKeys := ⟨100, 200⟩
def ben : MemberKeys := ⟨101, 201⟩
def selfKeys : MemberKeys := ⟨102, 202⟩
def adminKeys : MemberKeys := ⟨103, 203⟩

def history : Log
  | 0 => ⟨0, 100, 0, [], .entry (.create "Ana" storeKey) ana⟩
  | 1 => ⟨0, 100, 0, [0], .entry (.addMember 1 .member "Ben") ben⟩
  | 2 => ⟨1, 101, 1, [0, 1], .entry (.addDevice 1) ()⟩
  | 3 => ⟨1, 101, 1, [0, 1, 2], .replaceMemberKeys 1 selfKeys⟩
  | 4 => ⟨0, 100, 0, [0, 1, 2], .replaceMemberKeys 1 adminKeys⟩
  | 5 => ⟨1, 101, 1, [0, 1, 2, 3, 4], .entry (.setAccess 1 "old signer") ()⟩
  | 6 => ⟨1, 103, 1, [0, 1, 2, 3, 4, 5], .entry (.setAccess 1 "new signer") ()⟩
  | _ => ⟨0, 100, 0, [0, 1, 2, 3, 4], .entry (.removeMember 1) ()⟩

def H : History := ⟨history, id, fun n => if n = 4 then 3 else n⟩

theorem self_replacement :
    lookup (resolve H 30 4 (entrySet [0, 1, 2, 3])).state.keys 1 = some selfKeys ∧
    3 ∈ (resolve H 30 4 (entrySet [0, 1, 2, 3])).kept := by decide

theorem admin_beats_earlier_self :
    lookup (resolve H 30 5 (entrySet [0, 1, 2, 3, 4])).state.keys 1 = some adminKeys ∧
    4 ∈ (resolve H 30 5 (entrySet [0, 1, 2, 3, 4])).kept ∧
    3 ∈ (resolve H 30 5 (entrySet [0, 1, 2, 3, 4])).dropped := by decide

theorem any_order (arrivals : List Nat)
    (same : ∀ e, e ∈ arrivals ↔ e ∈ [0, 1, 2, 3, 4]) :
    lookup (resolve H 30 5 (entrySet arrivals)).state.keys 1 = some adminKeys := by
  rw [equal_received H 30 5 arrivals _ same]
  exact admin_beats_earlier_self.1

theorem signer_uses_recorded_view :
    5 ∈ (resolve H 30 7 (entrySet [0, 1, 2, 3, 4, 5, 6])).dropped ∧
    6 ∈ (resolve H 30 7 (entrySet [0, 1, 2, 3, 4, 5, 6])).kept := by decide

def twoSelf : History := { H with
  attempted := (fun e => if e = 5 then 4 else e),
  log := fun e =>
  if e = 3 then ⟨1, 101, 2, [0, 1, 2], .entry (.addDevice 1) ()⟩ else
  if e = 4 then ⟨1, 101, 1, [0, 1, 2, 3], .replaceMemberKeys 1 selfKeys⟩ else
  if e = 5 then ⟨1, 101, 2, [0, 1, 2, 3], .replaceMemberKeys 1 adminKeys⟩ else history e }

def twoAdmin : History := { H with
  attempted := (fun e => if e = 5 then 4 else e),
  log := fun e =>
  if e = 3 then ⟨0, 100, 2, [0, 1, 2], .entry (.addDevice 0) ()⟩ else
  if e = 4 then ⟨0, 100, 0, [0, 1, 2, 3], .replaceMemberKeys 1 selfKeys⟩ else
  if e = 5 then ⟨0, 100, 2, [0, 1, 2, 3], .replaceMemberKeys 1 adminKeys⟩ else history e }

theorem same_kind_earlier_wins :
    lookup (resolve twoSelf 30 6 (entrySet [0, 1, 2, 3, 4, 5])).state.keys 1 = some selfKeys ∧
    lookup (resolve twoAdmin 30 6 (entrySet [0, 1, 2, 3, 4, 5])).state.keys 1 = some selfKeys := by decide

def removal : History := { H with log := fun e =>
  if e = 4 then ⟨0, 100, 0, [0, 1, 2], .entry (.removeMember 1) ()⟩ else history e }

theorem removal_beats_replacement :
    member (resolve removal 30 5 (entrySet [0, 1, 2, 3, 4])).state.membership 1 = false ∧
    3 ∈ (resolve removal 30 5 (entrySet [0, 1, 2, 3, 4])).dropped := by decide

def removedThenReplace : History := { removal with log := fun e =>
  if e = 5 then ⟨0, 100, 0, [0, 1, 2, 3, 4], .replaceMemberKeys 1 adminKeys⟩
  else (removal.log e) }

theorem removed_stays_removed :
    member (resolve removedThenReplace 30 6 (entrySet [0, 1, 2, 3, 4, 5])).state.membership 1 = false ∧
    5 ∈ (resolve removedThenReplace 30 6 (entrySet [0, 1, 2, 3, 4, 5])).dropped := by decide

def onlyAdmin : History := ⟨(fun
  | 0 => history 0
  | 1 => ⟨0, 100, 0, [0], .replaceMemberKeys 0 selfKeys⟩
  | _ => ⟨0, 102, 0, [0, 1], .entry (.changeRole 0 .member) ()⟩), id, id⟩

theorem last_admin_unaffected :
    lookup (resolve onlyAdmin 30 3 (entrySet [0, 1, 2])).state.membership.members 0 = some .admin ∧
    lookup (resolve onlyAdmin 30 3 (entrySet [0, 1, 2])).state.keys 0 = some selfKeys ∧
    2 ∈ (resolve onlyAdmin 30 3 (entrySet [0, 1, 2])).dropped := by decide

/-- An entry made under the previous keys can still apply without having
read replacement, even though the receiver already has the replacement. -/
def unread : History := { H with
  attempted := (fun e => if e = 5 then 4 else H.attempted e),
  log := fun e =>
  if e = 3 then ⟨1, 101, 2, [0, 1, 2], .entry (.addDevice 1) ()⟩ else
  if e = 5 then ⟨1, 101, 2, [0, 1, 2, 3], .entry (.setAccess 1 "concurrent") ()⟩ else history e }

theorem old_unread_entry_counts :
    5 ∈ (resolve unread 30 6 (entrySet [0, 1, 2, 3, 4, 5])).kept := by decide

/-- An existing id cannot change its credentials through another addition. -/
def reAdd : History := { H with log := fun e =>
  if e = 3 then ⟨0, 100, 0, [0, 1, 2], .entry (.addMember 1 .member "Ben") selfKeys⟩ else history e }

theorem addition_cannot_replace :
    lookup (resolve reAdd 30 4 (entrySet [0, 1, 2, 3])).state.keys 1 = some ben ∧
    3 ∈ (resolve reAdd 30 4 (entrySet [0, 1, 2, 3])).dropped := by decide


theorem checked_metadata :
    metadataValid H 7 = true ∧ metadataValid twoSelf 6 = true ∧
    metadataValid twoAdmin 6 = true ∧ metadataValid removal 5 = true ∧
    metadataValid removedThenReplace 6 = true ∧ metadataValid onlyAdmin 3 = true ∧
    metadataValid unread 6 = true ∧ metadataValid reAdd 4 = true := by decide

/-- Equal replacement payloads still use the admin/self winner rule. -/
def sameKeys : History := { H with log := fun e =>
  if e = 4 then ⟨0, 100, 0, [0, 1, 2], .replaceMemberKeys 1 selfKeys⟩ else history e }

theorem equal_payload_admin_wins :
    4 ∈ (resolve sameKeys 30 5 (entrySet [0, 1, 2, 3, 4])).kept ∧
    3 ∈ (resolve sameKeys 30 5 (entrySet [0, 1, 2, 3, 4])).dropped := by decide


/-- Holding the admin role does not turn self-replacement into recovery by
another person. The other admin still has priority. -/
def adminTarget : History := { H with log := fun e =>
  if e = 1 then ⟨0, 100, 0, [0], .entry (.addMember 1 .admin "Ben") ben⟩ else history e }

theorem other_admin_beats_admin_self :
    lookup (resolve adminTarget 30 5 (entrySet [0, 1, 2, 3, 4])).state.keys 1 = some adminKeys ∧
    3 ∈ (resolve adminTarget 30 5 (entrySet [0, 1, 2, 3, 4])).dropped ∧
    4 ∈ (resolve adminTarget 30 5 (entrySet [0, 1, 2, 3, 4])).kept := by decide

end CovenStorelog.CurrentReplay.Keyed.Examples
