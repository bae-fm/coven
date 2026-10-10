import CovenStorelogData.Pending
import CovenStorelog.CurrentReplay

/-! §4, §13, §18: one owner's device serializes provider requests. The
provider's actual grants are distinct from replay's intended access. -/
namespace CovenStorelogData.Access
open CovenStorelog CovenStorelogData.Pending

inductive Request where
  | ready (target : Bool)
  | sending (target : Bool)
  | failed (target : Bool) (reason : Reason)
  deriving DecidableEq, Repr

structure Account where
  name : String
  intended : Bool
  actual : Bool
  request : Option Request
  deriving DecidableEq, Repr

def pending (actual intended : Bool) : Option Request :=
  if actual = intended then none else some (.ready intended)

/-- Replay never overwrites a request already in flight. Its completion
compares with the latest intention before declaring this account finished. -/
def adopt (intended : Bool) (s : Account) : Account :=
  { s with intended, request := match s.request with
      | some (.sending target) => some (.sending target)
      | some (.failed target why) =>
          if intended == s.intended then some (.failed target why) else pending s.actual intended
      | _ => pending s.actual intended }

/-- There is no start transition while another request is in flight. -/
def start (s : Account) : Option Account :=
  match s.request with
  | some (.ready _) => some { s with request := some (.sending s.intended) }
  | _ => none

/-- An explicit due retry retains an observable request and reads the latest
intention. Replaying unchanged entries is not permission to hide a failure. -/
def retryFailed (s : Account) : Option Account :=
  match s.request with
  | some (.failed _ _) => some { s with request := some (.ready s.intended) }
  | _ => none

def complete (s : Account) (reply : Except Reason Unit) : Option Account :=
  match s.request with
  | some (.sending target) => some (match reply with
      | .error reason => { s with request := some (.failed target reason) }
      | .ok () => { s with actual := target, request := pending target s.intended })
  | _ => none

def reason (s : Account) : Option Reason :=
  match s.request with
  | none => none
  | some (.failed _ why) => some why
  | some (.ready target) | some (.sending target) =>
      some (.providerPending (if target then .grant else .revoke) s.name)

/-- The operation subject remains visible until completion matches intention. -/
def work (device operation : Nat) (s : Account) : Work :=
  ⟨.operation operation, device,
    (reason s).toList.map (fun why => ⟨false, why⟩)⟩

theorem no_overlapping_request (s : Account) (target : Bool)
    (inFlight : s.request = some (.sending target)) : start s = none := by
  simp [start, inFlight]

theorem completion_matches_or_queues (s next : Account)
    (success : complete s (.ok ()) = some next) :
    (next.request = none ∧ next.actual = next.intended) ∨
    next.request = some (.ready next.intended) := by
  unfold complete at success
  split at success
  · rename_i target _
    simp only [Option.some.injEq] at success
    subst next
    dsimp
    by_cases he : target = s.intended
    · exact Or.inl ⟨by simp [pending, he], he⟩
    · exact Or.inr (by simp [pending, he])
  · cases success

theorem obsolete_completion_queues_opposite (s : Account) (target : Bool)
    (sending : s.request = some (.sending target)) (obsolete : target ≠ s.intended) :
    complete s (.ok ()) = some { s with actual := target, request := some (.ready s.intended) } := by
  simp [complete, sending, pending, obsolete]

theorem pending_visible (s : Account) (request : Request)
    (active : s.request = some request) : ∃ why, reason s = some why := by
  cases request <;> simp [reason, active]

theorem pending_until_finished (s : Account) (request : Request)
    (active : s.request = some request) (device operation : Nat) :
    ∃ why, ⟨.operation operation, device, why⟩ ∈ records [work device operation s] := by
  obtain ⟨why, h⟩ := pending_visible s request active
  exact ⟨why, by simp [records, work, h, first]⟩

/-- Every recorded access, including dropped entries, remains a revocation
input. Shared accounts and this device's open invites preserve their grant. -/
def recorded (entry : CurrentReplay.Entry) : Option (Nat × String) :=
  match entry.action with
  | .create access _ => some (entry.author, access)
  | .addMember m _ access | .setAccess m access => some (m, access)
  | _ => none

def intended (state : State) (invites : List String) (account : String) : Bool :=
  account ∈ invites || state.access.any (fun (m, a) => member state m && a == account)

def revokedCredentials (log : CurrentReplay.Log) (received : List Nat) (state : State)
    (confirmed : List String) : List String :=
  (received.filterMap fun e => (recorded (log e)).bind fun (m, key) =>
    if !member state m && key ∉ confirmed then some key else none).eraseDups

theorem confirmed_never_reported (log : CurrentReplay.Log) (received : List Nat) (state : State)
    (confirmed : List String) (key : String) (h : key ∈ confirmed) :
    key ∉ revokedCredentials log received state confirmed := by
  simp only [revokedCredentials, List.mem_eraseDups, List.mem_filterMap]
  rintro ⟨e, _, he⟩
  cases hr : recorded (log e) with
  | none => simp [hr] at he
  | some pair =>
      simp only [hr, Option.bind_some] at he
      split at he
      · simp only [Option.some.injEq] at he
        subst key
        simp [h] at *
      · cases he

/-- One recorded account's journal and the replay commit together. The same
boundary is used for local publications and downloaded entries. -/
structure Applied where
  received : List Nat
  result : Result
  account : Account
  deriving DecidableEq, Repr

def applyEntries (H : CurrentReplay.History) (W n : Nat) (invites : List String)
    (before : Applied) (received : List Nat) (saved : Except Failure Unit) :
    Applied × Except Failure Unit :=
  match saved with
  | .error why => (before, .error why)
  | .ok () =>
      let result := CurrentReplay.resolve H W n (entrySet received)
      (⟨received, result, adopt (intended result.state invites before.account.name) before.account⟩,
        .ok ())

/-- The removal call observes entry fate. It neither runs a provider request
nor schedules a second job; its successful result contains only unit (§13). -/
def removalCall (s : Applied) (entry : Nat) : Option Unit :=
  if entry ∈ s.result.kept then some () else none

def ownerWork (owner : Bool) (device operation : Nat) (s : Account) : Work :=
  if owner then work device operation s
  else ⟨.operation operation, device,
    if s.request.isSome then [⟨false, .pendingOwner s.name⟩] else []⟩

theorem apply_records_intention (H : CurrentReplay.History) (W n : Nat) (invites : List String)
    (before : Applied) (received : List Nat) :
    let after := (applyEntries H W n invites before received (.ok ())).1
    after.result = CurrentReplay.resolve H W n (entrySet received) ∧
    after.account.intended = intended after.result.state invites before.account.name ∧
    after.account.actual = before.account.actual := ⟨rfl, rfl, rfl⟩

theorem failed_apply_preserves_state (H : CurrentReplay.History) (W n : Nat)
    (invites : List String) (before : Applied) (received : List Nat) (why : Failure) :
    applyEntries H W n invites before received (.error why) = (before, .error why) := rfl

theorem removal_returns_without_provider (s : Applied) (entry : Nat)
    (kept : entry ∈ s.result.kept) : removalCall s entry = some () := by
  simp [removalCall, kept]

theorem apply_removal_visible (H : CurrentReplay.History) (W n : Nat)
    (invites : List String) (before : Applied) (received : List Nat) (device operation : Nat)
    (revoked : intended (CurrentReplay.resolve H W n (entrySet received)).state
      invites before.account.name = false)
    (granted : before.account.actual = true) (idle : before.account.request = none) :
    let after := (applyEntries H W n invites before received (.ok ())).1
    ⟨.operation operation, device, .providerPending .revoke before.account.name⟩ ∈
      records [ownerWork true device operation after.account] ∧
    ⟨.operation operation, device, .pendingOwner before.account.name⟩ ∈
      records [ownerWork false device operation after.account] := by
  simp [applyEntries, revoked, adopt, idle, pending, granted, ownerWork, work, reason,
    records, first]

theorem owner_wait_visible (s : Account) (request : Request)
    (active : s.request = some request) (device operation : Nat) :
    ⟨.operation operation, device, .pendingOwner s.name⟩ ∈
      records [ownerWork false device operation s] := by
  simp [ownerWork, active, records, first]

theorem finished_work_absent (s : Account) (device operation : Nat)
    (finished : s.request = none) : records [work device operation s] = [] := by
  simp [work, reason, finished, records, first]

theorem replay_preserves_failure (s : Account) (target : Bool) (why : Reason)
    (failed : s.request = some (.failed target why)) :
    (adopt s.intended s).request = some (.failed target why) := by
  simp [adopt, failed]

theorem recorded_removed_key_visible (log : CurrentReplay.Log) (received : List Nat) (state : State)
    (confirmed : List String) (e m : Nat) (key : String)
    (present : e ∈ received) (access : recorded (log e) = some (m, key))
    (removed : member state m = false) (unconfirmed : key ∉ confirmed) :
    key ∈ revokedCredentials log received state confirmed := by
  simp only [revokedCredentials, List.mem_eraseDups, List.mem_filterMap]
  exact ⟨e, present, by simp [access, removed, unconfirmed]⟩

def credentialWork (device operation : Nat) (key : String) : Work :=
  ⟨.operation operation, device, [⟨false, .deleteAccessKey key⟩]⟩

/-- The operation-id allocator supplies the existing journal id per key. -/
def credentialRecords (device : Nat) (operation : String → Nat) (log : CurrentReplay.Log)
    (received : List Nat) (state : State) (confirmed : List String) : List Record :=
  records ((revokedCredentials log received state confirmed).map
    (fun key => credentialWork device (operation key) key))

theorem credential_pending_visible (log : CurrentReplay.Log) (received : List Nat) (state : State)
    (confirmed : List String) (e m device : Nat) (operation : String → Nat) (key : String)
    (present : e ∈ received) (access : recorded (log e) = some (m, key))
    (removed : member state m = false) (unconfirmed : key ∉ confirmed) :
    ⟨.operation (operation key), device, .deleteAccessKey key⟩ ∈
      credentialRecords device operation log received state confirmed := by
  apply every_pending_subject _ (credentialWork device (operation key) key) _
  · exact List.mem_map.mpr ⟨key, recorded_removed_key_visible log received state confirmed
      e m key present access removed unconfirmed, rfl⟩
  · rfl

theorem retained_grant_visible (s : Account) (device operation : Nat)
    (failed : s.request = some (.failed false (.accessRemains s.name))) :
    ⟨.operation operation, device, .accessRemains s.name⟩ ∈ records [work device operation s] := by
  simp [records, work, reason, failed, first]

theorem retry_keeps_work_visible (s : Account) (target : Bool) (why : Reason)
    (failed : s.request = some (.failed target why)) (device operation : Nat) :
    retryFailed s = some { s with request := some (.ready s.intended) } ∧
    ⟨.operation operation, device,
      .providerPending (if s.intended then .grant else .revoke) s.name⟩ ∈
      records [work device operation { s with request := some (.ready s.intended) }] := by
  simp [retryFailed, failed, records, work, reason, first]

end CovenStorelogData.Access
