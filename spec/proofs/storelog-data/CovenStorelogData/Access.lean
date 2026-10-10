import CovenStorelogData.Blocked

/-! §4, §13, §18: one owner's device serializes provider requests. The
provider's actual grants are distinct from replay's intended access. -/
namespace CovenStorelogData.Access
open CovenStorelog CovenStorelogData.Blocked

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
      | _ => pending s.actual intended }

/-- There is no start transition while another request is in flight. -/
def start (s : Account) : Option Account :=
  match s.request with
  | some (.ready _) => some { s with request := some (.sending s.intended) }
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

theorem blocked_until_finished (s : Account) (request : Request)
    (active : s.request = some request) (device operation : Nat) :
    ∃ why, ⟨.operation operation, device, why⟩ ∈ records [work device operation s] := by
  obtain ⟨why, h⟩ := pending_visible s request active
  exact ⟨why, by simp [records, work, h, first]⟩

/-- Every recorded access, including dropped entries, remains a revocation
input. Shared accounts and this device's open invites preserve their grant. -/
def recorded (entry : Entry) : Option (Nat × String) :=
  match entry.action with
  | .create access => some (entry.author, access)
  | .addMember m _ access | .setAccess m access => some (m, access)
  | _ => none

def intended (state : State) (invites : List String) (account : String) : Bool :=
  account ∈ invites || state.access.any (fun (m, a) => member state m && a == account)

def revokedCredentials (log : Log) (received : List Nat) (state : State)
    (confirmed : List String) : List String :=
  (received.filterMap fun e => (recorded (log e)).bind fun (m, key) =>
    if !member state m && key ∉ confirmed then some key else none).eraseDups

theorem confirmed_never_reported (log : Log) (received : List Nat) (state : State)
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

end CovenStorelogData.Access
