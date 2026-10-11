import CovenStorelog.MemberKeys
import CovenStorelogData.KeySelection

/-! §11 exposure after member-key replacement. The member routes the copy;
the explicit sealing public key distinguishes credentials across replacements.
The ordinary KeySelection selector still supplies custody, audience and order. -/
namespace CovenStorelogData.KeySelection.MemberKeys
open CovenStorelog
open CurrentReplay

structure Recipient where
  member : Nat
  sealing : Nat
  deriving DecidableEq, Repr

abbrev Copies := List (Key × Recipient)

structure Pass where
  custody : List Key
  copies : Copies
  deriving DecidableEq, Repr

def Pass.members (p : Pass) : KeySelection.Pass :=
  ⟨p.custody, p.copies.map (fun (key, recipient) => (key, recipient.member))⟩

/-- Every kept replacement exposes the credential in its recorded past.
Copies to any of those sealing keys count even if the member remains active. -/
def retired (H : Keyed.History) (W n : Nat) (r : Keyed.Result) : List Nat :=
  r.kept.filterMap fun e => match (H.log e).action with
    | .replaceMemberKeys m _ => ((lookup (Keyed.authorViews H W n n e).keys m).map (·.sealing))
    | _ => none

def currentCredential (state : Keyed.State) (recipient : Recipient) : Bool :=
  (lookup state.keys recipient.member).any (fun keys => keys.sealing == recipient.sealing)

/-- A candidate that never became current cannot borrow the stable member's
authority. Its prerequisite copies expose their keys too. -/
def exposed (state : Keyed.State) (retired : List Nat) (p : Pass) (key : Key) : Bool :=
  p.copies.any (fun (k, recipient) => k == key &&
    (recipient.sealing ∈ retired || !currentCredential state recipient))

def select (state : Keyed.State) (authorized : Key → Bool) (retired : List Nat)
    (p : Pass) (who : Nat) (audience : Audience) : Option Key :=
  KeySelection.select (audienceMembers state.membership)
    (fun key => authorized key && !exposed state retired p key)
    p.members who audience

/-- Routing resolves the current public key and refuses any exposed one.
This also blocks a malformed/reused binding instead of sending to an old key. -/
def route (state : Keyed.State) (retired : List Nat) (member : Nat) : Option Recipient := do
  let keys ← lookup state.keys member
  if keys.sealing ∈ retired then none else some ⟨member, keys.sealing⟩

def share (state : Keyed.State) (retired : List Nat) (p : Pass)
    (member : Nat) (key : Key) : Option Copies := do
  let _ ← KeySelection.share (audienceMembers state.membership) p.custody
    p.members.copies member key
  let recipient ← route state retired member
  return p.copies ++ [(key, recipient)]

def shareInReplay (H : Keyed.History) (W n : Nat) (received : EntrySet)
    (p : Pass) (member : Nat) (key : Key) : Option Copies :=
  let r := Keyed.resolve H W n received
  share r.state (retired H W n r) p member key

theorem kept_replacement_retires (H : Keyed.History) (W n e m : Nat) (r : Keyed.Result)
    (old keys : Keyed.MemberKeys) (kept : e ∈ r.kept)
    (action : (H.log e).action = .replaceMemberKeys m keys)
    (past : lookup (Keyed.authorViews H W n n e).keys m = some old) :
    old.sealing ∈ retired H W n r := by
  apply List.mem_filterMap.mpr
  exact ⟨e, kept, by simp [action, past]⟩

theorem route_current (state : Keyed.State) (retired : List Nat) (m : Nat) (recipient : Recipient)
    (routed : route state retired m = some recipient) :
    recipient.member = m ∧ recipient.sealing ∉ retired ∧
      ∃ keys, lookup state.keys m = some keys ∧ keys.sealing = recipient.sealing := by
  unfold route at routed
  cases h : lookup state.keys m with
  | none => simp [h] at routed
  | some keys =>
      simp only [h, bind, Option.bind] at routed
      split at routed
      · cases routed
      · cases routed; exact ⟨rfl, by assumption, keys, rfl, rfl⟩

theorem no_new_old_copy (state : Keyed.State) (retired : List Nat) (p : Pass)
    (member : Nat) (key : Key) (after : Copies)
    (shared : share state retired p member key = some after) :
    ∃ recipient, after = p.copies ++ [(key, recipient)] ∧
      recipient.member = member ∧ recipient.sealing ∉ retired := by
  unfold share at shared
  cases h : KeySelection.share (audienceMembers state.membership) p.custody
      p.members.copies member key with
  | none => simp [h] at shared
  | some copies =>
      simp only [h, bind, Option.bind] at shared
      cases hr : route state retired member with
      | none => simp [hr] at shared
      | some recipient =>
          simp only [hr, pure, Option.some.injEq] at shared
          have safe := route_current state retired member recipient hr
          exact ⟨recipient, shared.symm, safe.1, safe.2.1⟩

theorem replacement_sharing (H : Keyed.History) (W n e m : Nat) (received : EntrySet)
    (old keys : Keyed.MemberKeys) (p : Pass) (recipient : Nat) (key : Key) (after : Copies)
    (kept : e ∈ (Keyed.resolve H W n received).kept)
    (action : (H.log e).action = .replaceMemberKeys m keys)
    (past : lookup (Keyed.authorViews H W n n e).keys m = some old)
    (shared : shareInReplay H W n received p recipient key = some after) :
    ∃ destination, after = p.copies ++ [(key, destination)] ∧
      destination.member = recipient ∧ destination.sealing ≠ old.sealing := by
  obtain ⟨destination, shape, member, safe⟩ := no_new_old_copy _ _ p recipient key after shared
  refine ⟨destination, shape, member, ?_⟩
  intro reused
  apply safe
  rw [reused]
  exact kept_replacement_retires H W n e m _ old keys kept action past

theorem selection_avoids_old_key (state : Keyed.State) (authorized : Key → Bool)
    (retired : List Nat) (p : Pass) (who : Nat) (audience : Audience) (key : Key)
    (selected : select state authorized retired p who audience = some key)
    (recipient : Recipient) (old : recipient.sealing ∈ retired) :
    (key, recipient) ∉ p.copies := by
  have allowed := (KeySelection.first_attempt_safe _ _ _ _ _ _ selected).2.2.1
  have safe : exposed state retired p key = false := by
    simpa using (Bool.and_eq_true_iff.mp allowed).2
  intro copied
  have exposed : exposed state retired p key = true :=
    List.any_eq_true.mpr ⟨(key, recipient), copied, by simp [old]⟩
  simp [safe] at exposed

theorem selection_uses_current_bindings (state : Keyed.State) (authorized : Key → Bool)
    (retired : List Nat) (p : Pass) (who : Nat) (audience : Audience) (key : Key)
    (selected : select state authorized retired p who audience = some key)
    (recipient : Recipient) (copied : (key, recipient) ∈ p.copies) :
    currentCredential state recipient = true := by
  have allowed := (KeySelection.first_attempt_safe _ _ _ _ _ _ selected).2.2.1
  have safe : exposed state retired p key = false := by
    simpa using (Bool.and_eq_true_iff.mp allowed).2
  have h := List.any_eq_false.mp safe (key, recipient) copied
  have binding : recipient.sealing ∉ retired ∧ currentCredential state recipient = true := by
    simpa using h
  exact binding.2

def introduction (e : Keyed.Entry) : Option (Audience × CurrentReplay.KeyCommitment) :=
  match e.action with
  | .entry a _ => a.introduction
  | .replaceMemberKeys .. => none

def receivedAuthorized (H : Keyed.History) (W n : Nat) (received : EntrySet) (key : Key) : Bool :=
  key.entry < n && received key.entry &&
    Keyed.authorized (Keyed.authorViews H W n n key.entry) (H.log key.entry) &&
    introduction (H.log key.entry) == some (key.audience, key.key)

def selectInReplay (H : Keyed.History) (W n : Nat) (received : EntrySet)
    (p : Pass) (who : Nat) (audience : Audience) : Option Key :=
  let r := Keyed.resolve H W n received
  select r.state (receivedAuthorized H W n received)
    (retired H W n r) p who audience

theorem replacement_selection (H : Keyed.History) (W n e m : Nat) (received : EntrySet)
    (old keys : Keyed.MemberKeys) (p : Pass) (who : Nat) (audience : Audience) (key : Key)
    (kept : e ∈ (Keyed.resolve H W n received).kept)
    (action : (H.log e).action = .replaceMemberKeys m keys)
    (past : lookup (Keyed.authorViews H W n n e).keys m = some old)
    (selected : selectInReplay H W n received p who audience = some key)
    (recipient : Recipient) (sameSeal : recipient.sealing = old.sealing) :
    (key, recipient) ∉ p.copies := by
  apply selection_avoids_old_key _ _ _ _ _ _ _ selected recipient
  rw [sameSeal]
  exact kept_replacement_retires H W n e m _ old keys kept action past

theorem historical_copy_retained (state : Keyed.State) (retired : List Nat) (p : Pass)
    (member : Nat) (key : Key) (after : Copies)
    (shared : share state retired p member key = some after) :
    ∀ copy ∈ p.copies, copy ∈ after := by
  obtain ⟨recipient, rfl, _⟩ := no_new_old_copy state retired p member key after shared
  exact fun _ h => List.mem_append_left _ h

end CovenStorelogData.KeySelection.MemberKeys
