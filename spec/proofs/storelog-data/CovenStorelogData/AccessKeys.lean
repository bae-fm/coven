import CovenStorelogData.Access
import CovenStorelog.ReplayMachineTermination

/-! §9 and §13: S3 access replacement uses the existing entry payload,
conflicts and effects, with authority for an admin recording another member's
key. Provider-admin access is separate from the store-log role. -/
namespace CovenStorelogData.Access.S3
open CovenStorelog CovenStorelogData.Pending

def authorized (view : State) (entry : CurrentReplay.Entry) : Bool :=
  match entry.action with
  | .setAccess m _ => member view m && member view entry.author &&
      (entry.author == m || admin view entry.author)
  | _ => CurrentReplay.authorized view entry

def materialize (H : CurrentReplay.History) (W n bound : Nat) (S : EntrySet)
    (views : Nat → State) : Result :=
  let live := ReplayPolicy.admitted H.membership W n S
  let r := ReplayMachine.settle State.empty (fun e => authorized (views e) (H.log e))
    (fun s e => alreadyInPlace s (H.log e).membership)
    (fun s e => ReplayPolicy.realize views s e (H.log e).membership)
    (ReplayPolicy.conflict H.membership.log views)
    (ReplayPolicy.prefer H.membership.log views) ((List.range bound).filter live)
  ReplayPolicy.finish ⟨r.state, r.kept,
    r.dropped ++ (List.range bound).filter (fun e => S e && !live e)⟩

def authorViews (H : CurrentReplay.History) (W n : Nat) : Nat → Nat → State
  | 0 => fun _ => State.empty
  | k + 1 =>
      let prior := authorViews H W n k
      fun e => if e = k then
        (materialize H W n k (hadRead H.membership.log k) prior).state else prior e

def resolve (H : CurrentReplay.History) (W n : Nat) (S : EntrySet) : Result :=
  materialize H W n n S (authorViews H W n n)

theorem kept_authorized (H : CurrentReplay.History) (W n : Nat) (S : EntrySet) (e : Nat)
    (kept : e ∈ (resolve H W n S).kept) :
    authorized (authorViews H W n n e) (H.log e) = true :=
  (ReplayMachine.settle_kept _ _ _ _ _ _ _ e kept).2

theorem replacement_authority (view : State) (entry : CurrentReplay.Entry) (m : Nat)
    (key : String) (action : entry.action = .setAccess m key) :
    authorized view entry = true ↔ member view m = true ∧
      member view entry.author = true ∧ (entry.author = m ∨ admin view entry.author = true) := by
  simp [authorized, action, and_assoc]

theorem other_member_requires_admin (H : CurrentReplay.History) (W n e m : Nat)
    (S : EntrySet) (key : String) (action : (H.log e).action = .setAccess m key)
    (other : (H.log e).author ≠ m) (kept : e ∈ (resolve H W n S).kept) :
    admin (authorViews H W n n e) (H.log e).author = true := by
  have h := (replacement_authority _ _ m key action).mp (kept_authorized H W n S e kept)
  exact h.2.2.resolve_left other

/-- Replacement also retires the key it replaces in its recorded view.
Neither a lost conflict nor a later restored binding cancels that work.
An identical key id records no replacement of that same credential. -/
def replacedKey (view : State) (entry : CurrentReplay.Entry) : Option String :=
  match entry.action with
  | .setAccess m key => if authorized view entry then
      (lookup view.access m).filter (· != key) else none
  | _ => none

def required (H : CurrentReplay.History) (W n : Nat) (received : List Nat)
    (state : State) : List String :=
  revokedCredentials H.log received state [] ++
    received.filterMap (fun e => replacedKey (authorViews H W n n e) (H.log e))

/-- The journal stores ids, never provider-admin credentials. Received entries,
replay and deletion obligations commit together. Confirmed ids are permanent. -/
structure Journal where
  received : List Nat
  result : Result
  requiredKeys : List String
  confirmed : List String
  deriving DecidableEq, Repr

def Journal.empty : Journal := ⟨[], ⟨State.empty, [], []⟩, [], []⟩

def keys (s : Journal) : List String :=
  s.requiredKeys.eraseDups.filter (· ∉ s.confirmed)

def pendingRecords (s : Journal) (device : Nat) (operation : String → Nat) : List Record :=
  records ((keys s).map (fun key => credentialWork device (operation key) key))

theorem key_pending_iff (s : Journal) (key : String) :
    key ∈ keys s ↔ key ∈ s.requiredKeys ∧ key ∉ s.confirmed := by
  simp [keys]

theorem pending_visible (s : Journal) (key : String) (device : Nat)
    (operation : String → Nat) (waiting : key ∈ keys s) :
    ⟨.operation (operation key), device, .deleteAccessKey key⟩ ∈
      pendingRecords s device operation := by
  apply every_pending_subject _ (credentialWork device (operation key) key) _
  · exact List.mem_map.mpr ⟨key, waiting, rfl⟩
  · rfl

theorem pending_record_iff (s : Journal) (key : String) (device : Nat)
    (operation : String → Nat) :
    ⟨.operation (operation key), device, .deleteAccessKey key⟩ ∈
      pendingRecords s device operation ↔ key ∈ keys s := by
  simp [pendingRecords, records, credentialWork, first]

inductive Presence where
  | present | absent
  deriving DecidableEq, Repr

/-- A read's key id is independent of the deletion target, so a reply for
another key cannot accidentally serve as evidence for this one. -/
structure Observation where
  key : String
  reply : Except Reason Presence
  deriving Repr

inductive DeleteFailure where
  | adminRequired | deleteFailed (reason : Reason) | observationMissing
  | wrongKey | stillPresent | observationFailed (reason : Reason)
  deriving DecidableEq, Repr

/-- Receipts mean deletion with operation-scoped admin access followed by the
provider read. They abstract IO, not the provider's truthfulness. -/
def verifyDeletion (key : String) (adminAccess : Bool) (deleted : Except Reason Unit)
    (observation : Option Observation) : Except DeleteFailure Unit :=
  if !adminAccess then .error .adminRequired else
    match deleted with
    | .error why => .error (.deleteFailed why)
    | .ok () => match observation with
      | none => .error .observationMissing
      | some read =>
          if read.key != key then .error .wrongKey else
            match read.reply with
            | .error why => .error (.observationFailed why)
            | .ok .present => .error .stillPresent
            | .ok .absent => .ok ()

theorem deletion_requires_exact_absence (key : String) (adminAccess : Bool)
    (deleted : Except Reason Unit) (observation : Option Observation) :
    verifyDeletion key adminAccess deleted observation = .ok () ↔
      adminAccess = true ∧ deleted = .ok () ∧
      observation = some ⟨key, .ok .absent⟩ := by
  cases adminAccess <;> cases deleted <;> cases observation <;>
    simp [verifyDeletion]
  rename_i value read
  cases value
  rcases read with ⟨other, reply⟩
  cases reply with
  | error why => by_cases h : other = key <;> simp [h]
  | ok presence => cases presence <;> by_cases h : other = key <;> simp [h]

inductive Event where
  | entries (received : List Nat)
  | providerDeletion (key : String) (adminAccess : Bool) (deleted : Except Reason Unit)
      (observation : Option Observation)
  | confirmAccessKeyDeleted (key : String)
  deriving Repr

def clearance : Event → Option String
  | .entries _ => none
  | .confirmAccessKeyDeleted key => some key
  | .providerDeletion key adminAccess deleted observation =>
      match verifyDeletion key adminAccess deleted observation with
      | .ok () => some key
      | .error _ => none

def applyEvent (H : CurrentReplay.History) (W n : Nat) (s : Journal) :
    Event → Journal × Except DeleteFailure Unit
  | .entries incoming =>
      let received := (s.received ++ incoming).eraseDups
      let result := resolve H W n (entrySet received)
      (⟨received, result, (s.requiredKeys ++ required H W n received result.state).eraseDups,
        s.confirmed⟩, .ok ())
  | .confirmAccessKeyDeleted key =>
      ({ s with confirmed := (key :: s.confirmed).eraseDups }, .ok ())
  | .providerDeletion key adminAccess deleted observation =>
      match verifyDeletion key adminAccess deleted observation with
      | .ok () => ({ s with confirmed := (key :: s.confirmed).eraseDups }, .ok ())
      | .error why => (s, .error why)

/-- A failed local transaction cannot clear work or advance received entries. -/
def commit (H : CurrentReplay.History) (W n : Nat) (s : Journal) (event : Event)
    (saved : Except Failure Unit) : Journal × Except Failure (Except DeleteFailure Unit) :=
  match saved with
  | .error why => (s, .error why)
  | .ok () => let (next, reply) := applyEvent H W n s event; (next, .ok reply)

theorem failed_commit_retains (H : CurrentReplay.History) (W n : Nat) (s : Journal)
    (event : Event) (why : Failure) :
    commit H W n s event (.error why) = (s, .error why) := rfl

theorem event_retains_required (H : CurrentReplay.History) (W n : Nat) (s : Journal)
    (event : Event) (key : String) (h : key ∈ s.requiredKeys) :
    key ∈ (applyEvent H W n s event).1.requiredKeys := by
  cases event with
  | entries incoming => simp [applyEvent, h]
  | confirmAccessKeyDeleted key => exact h
  | providerDeletion key adminAccess deleted observation =>
      simp only [applyEvent]; split <;> exact h

theorem event_confirmation (H : CurrentReplay.History) (W n : Nat) (s : Journal)
    (event : Event) (key : String) :
    key ∈ (applyEvent H W n s event).1.confirmed ↔
      key ∈ s.confirmed ∨ clearance event = some key := by
  cases event with
  | entries incoming => simp [applyEvent, clearance]
  | confirmAccessKeyDeleted target => simp [applyEvent, clearance, eq_comm, or_comm]
  | providerDeletion target adminAccess deleted observation =>
      simp only [applyEvent, clearance]
      split <;> simp [eq_comm, or_comm]

theorem no_other_transition_clears (H : CurrentReplay.History) (W n : Nat) (s : Journal)
    (event : Event) (key : String) (waiting : key ∈ keys s)
    (gone : key ∉ keys (applyEvent H W n s event).1) : clearance event = some key := by
  obtain ⟨required, unconfirmed⟩ := (key_pending_iff s key).mp waiting
  have retained := event_retains_required H W n s event key required
  have confirmed : key ∈ (applyEvent H W n s event).1.confirmed := by
    by_cases h : key ∈ (applyEvent H W n s event).1.confirmed
    · exact h
    · exact False.elim (gone ((key_pending_iff _ key).mpr ⟨retained, h⟩))
  exact ((event_confirmation H W n s event key).mp confirmed).resolve_left unconfirmed

theorem committed_clear_requires_evidence (H : CurrentReplay.History) (W n : Nat)
    (s : Journal) (event : Event) (saved : Except Failure Unit) (key : String)
    (waiting : key ∈ keys s) (gone : key ∉ keys (commit H W n s event saved).1) :
    saved = .ok () ∧ clearance event = some key := by
  cases saved with
  | error why => exact False.elim (gone waiting)
  | ok value =>
      cases value
      exact ⟨rfl, no_other_transition_clears H W n s event key waiting gone⟩

theorem records_clear_only_with_evidence (H : CurrentReplay.History) (W n : Nat)
    (s : Journal) (event : Event) (saved : Except Failure Unit) (key : String)
    (device : Nat) (operation : String → Nat)
    (waiting : ⟨.operation (operation key), device, .deleteAccessKey key⟩ ∈
      pendingRecords s device operation)
    (gone : ⟨.operation (operation key), device, .deleteAccessKey key⟩ ∉
      pendingRecords (commit H W n s event saved).1 device operation) :
    saved = .ok () ∧ clearance event = some key := by
  apply committed_clear_requires_evidence H W n s event saved key
    ((pending_record_iff s key device operation).mp waiting)
  exact fun h => gone ((pending_record_iff _ key device operation).mpr h)

theorem provider_clear_requires_observation (H : CurrentReplay.History) (W n : Nat)
    (s : Journal) (key target : String) (adminAccess : Bool) (deleted : Except Reason Unit)
    (observation : Option Observation) (waiting : key ∈ keys s)
    (gone : key ∉ keys (applyEvent H W n s
      (.providerDeletion target adminAccess deleted observation)).1) :
    target = key ∧ adminAccess = true ∧ deleted = .ok () ∧
      observation = some ⟨key, .ok .absent⟩ := by
  have h := no_other_transition_clears H W n s _ key waiting gone
  simp only [clearance] at h
  cases result : verifyDeletion target adminAccess deleted observation with
  | error why => simp [result] at h
  | ok value =>
      cases value
      simp only [result, Option.some.injEq] at h
      subst target
      exact ⟨rfl, (deletion_requires_exact_absence _ _ _ _).mp result⟩

theorem history_pending_until_cleared (H : CurrentReplay.History) (W n : Nat)
    (events : List Event) (s : Journal) (key : String) (waiting : key ∈ keys s)
    (uncleared : ∀ event ∈ events, clearance event ≠ some key) :
    key ∈ keys (events.foldl (fun state event => (applyEvent H W n state event).1) s) := by
  induction events generalizing s with
  | nil => exact waiting
  | cons event events ih =>
      apply ih _ ?_ (fun e h => uncleared e (List.mem_cons_of_mem event h))
      by_cases next : key ∈ keys (applyEvent H W n s event).1
      · exact next
      · exact False.elim (uncleared event List.mem_cons_self
          (no_other_transition_clears H W n s event key waiting next))

theorem history_loss_requires_clearance (H : CurrentReplay.History) (W n : Nat)
    (events : List Event) (s : Journal) (key : String) (waiting : key ∈ keys s)
    (gone : key ∉ keys (events.foldl (fun state event => (applyEvent H W n state event).1) s)) :
    ∃ event ∈ events, clearance event = some key := by
  by_cases witness : ∃ event ∈ events, clearance event = some key
  · exact witness
  · exact False.elim (gone (history_pending_until_cleared H W n events s key waiting
      (fun event present cleared => witness ⟨event, present, cleared⟩)))

theorem confirmed_never_returns (H : CurrentReplay.History) (W n : Nat)
    (events : List Event) (s : Journal) (key : String) (confirmed : key ∈ s.confirmed) :
    key ∉ keys (events.foldl (fun state event => (applyEvent H W n state event).1) s) := by
  have retained : key ∈ (events.foldl (fun state event =>
      (applyEvent H W n state event).1) s).confirmed := by
    induction events generalizing s with
    | nil => exact confirmed
    | cons event events ih =>
        exact ih _ ((event_confirmation H W n s event key).mpr (Or.inl confirmed))
  simp [keys, retained]

theorem recorded_retired_pending (H : CurrentReplay.History) (W n e m : Nat)
    (s : Journal) (incoming : List Nat) (key : String)
    (present : e ∈ s.received ++ incoming) (access : recorded (H.log e) = some (m, key))
    (retired : credentialRetired
      (resolve H W n (entrySet (s.received ++ incoming).eraseDups)).state m key = true)
    (unconfirmed : key ∉ s.confirmed) :
    key ∈ keys (applyEvent H W n s (.entries incoming)).1 := by
  apply (key_pending_iff _ key).mpr
  refine ⟨?_, unconfirmed⟩
  simp only [applyEvent, List.mem_eraseDups, List.mem_append]
  apply Or.inr
  unfold required
  apply List.mem_append_left
  simp only [revokedCredentials, List.mem_eraseDups, List.mem_filterMap]
  exact ⟨e, by simpa using present, by simp [access, retired]⟩

/-- Every received key is accounted for: pending, confirmed deleted, or still
the member's current access. Entry fate and author identity cannot erase it. -/
theorem every_recorded_key_accounted (H : CurrentReplay.History) (W n e m : Nat)
    (s : Journal) (incoming : List Nat) (key : String)
    (present : e ∈ s.received ++ incoming) (access : recorded (H.log e) = some (m, key)) :
    let next := (applyEvent H W n s (.entries incoming)).1
    key ∈ keys next ∨ key ∈ next.confirmed ∨
      (member next.result.state m = true ∧ lookup next.result.state.access m = some key) := by
  dsimp only
  by_cases confirmed : key ∈ s.confirmed
  · exact Or.inr (Or.inl confirmed)
  · by_cases retired : credentialRetired
        (resolve H W n (entrySet (s.received ++ incoming).eraseDups)).state m key = true
    · exact Or.inl (recorded_retired_pending H W n e m s incoming key present access retired confirmed)
    · apply Or.inr ∘ Or.inr
      simpa [credentialRetired, applyEvent] using retired

theorem received_never_forgotten (H : CurrentReplay.History) (W n : Nat)
    (events : List Event) (s : Journal) (e : Nat) (received : e ∈ s.received) :
    e ∈ (events.foldl (fun state event => (applyEvent H W n state event).1) s).received := by
  induction events generalizing s with
  | nil => exact received
  | cons event events ih =>
      apply ih
      cases event with
      | entries incoming => simp [applyEvent, received]
      | confirmAccessKeyDeleted key => exact received
      | providerDeletion key adminAccess deleted observation =>
          simp only [applyEvent]; split <;> exact received

/-- No premise asks whether the access entry is kept, who signed it, or
whether it arrived before the removal. All recorded ids contribute. -/
theorem removed_recorded_pending (H : CurrentReplay.History) (W n e m : Nat)
    (s : Journal) (incoming : List Nat) (key : String)
    (present : e ∈ s.received ++ incoming) (access : recorded (H.log e) = some (m, key))
    (removed : member (resolve H W n (entrySet (s.received ++ incoming).eraseDups)).state m = false)
    (unconfirmed : key ∉ s.confirmed) :
    key ∈ keys (applyEvent H W n s (.entries incoming)).1 :=
  recorded_retired_pending H W n e m s incoming key present access
    (by simp [credentialRetired, removed]) unconfirmed

theorem replacement_old_pending (H : CurrentReplay.History) (W n e : Nat)
    (s : Journal) (incoming : List Nat) (key : String)
    (present : e ∈ s.received ++ incoming)
    (replaces : replacedKey (authorViews H W n n e) (H.log e) = some key)
    (unconfirmed : key ∉ s.confirmed) :
    key ∈ keys (applyEvent H W n s (.entries incoming)).1 := by
  apply (key_pending_iff _ key).mpr
  refine ⟨?_, unconfirmed⟩
  simp only [applyEvent, List.mem_eraseDups, List.mem_append]
  apply Or.inr
  unfold required
  exact List.mem_append_right _ (List.mem_filterMap.mpr
    ⟨e, by simpa using present, replaces⟩)

end CovenStorelogData.Access.S3
