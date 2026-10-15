import CovenStorelog.CurrentReplay
import CovenStorelog.ReplayMachineTermination

/-! Stable member identities and replaceable credentials. This proposal uses
§9's replay and §11's exposure policy; the unkeyed CurrentReplay API remains
available to the existing Rust differential runner. -/
namespace CovenStorelog.CurrentReplay.Keyed

/-- Identities and public keys are independent abstract values, not a hash or
conversion of one another. Random generation and signature verification are
at the model boundary. -/
structure MemberKeys where
  signing : Nat
  sealing : Nat
  deriving DecidableEq, Repr

/-- Creation and addition carry keys; every other existing action carries no
member-key payload. The index prevents missing or irrelevant key fields. -/
def InitialKeys : CurrentReplay.Action → Type
  | .create .. | .addMember .. => MemberKeys
  | _ => Unit

instance (a : CurrentReplay.Action) : DecidableEq (InitialKeys a) := by
  cases a <;> simp only [InitialKeys] <;> infer_instance

inductive Action where
  | entry (action : CurrentReplay.Action) (keys : InitialKeys action)
  | replaceMemberKeys (member : Nat) (keys : MemberKeys)
  deriving DecidableEq

structure Entry where
  author : Nat
  signer : Nat
  device : Nat
  past : List Nat
  action : Action
  deriving DecidableEq

abbrev Log := Nat → Entry

structure State where
  membership : CovenStorelog.State
  keys : List (Nat × MemberKeys)
  usedKeys : List MemberKeys
  deriving DecidableEq, Repr

def State.empty : State := ⟨CovenStorelog.State.empty, [], []⟩
abbrev Result := ReplayMachine.Result State

/-- Common membership effects retain the existing action, with the stable
member id as author. Replacements never pass through this projection. -/
def Entry.membership (e : Entry) (a : CurrentReplay.Action) : CovenStorelog.Entry :=
  ⟨e.author, e.device, e.past, a.membership⟩

def hadRead (M : Log) (e a : Nat) : Bool := a ∈ (M e).past

structure History where
  log : Log
  stored : Nat → Nat
  attempted : Nat → Nat

/-- Finite checked histories include causal closure, own-device predecessors,
the unique root, and the online first-attempt/storage relationship. -/
def metadataValid (H : History) (n : Nat) : Bool :=
  (List.range n).all fun e =>
    (match (H.log e).action with
     | .entry (.create ..) _ => e == 0
     | _ => e != 0) &&
    (H.log e).past.all (fun a => a < e && (H.log a).past.all (· ∈ (H.log e).past)) &&
    (e == 0 || hadRead H.log e 0) && H.attempted e ≤ H.stored e &&
    (List.range n).all (fun a =>
      (!(a < e && (H.log a).device == (H.log e).device) || hadRead H.log e a) &&
      (hadRead H.log e a == decide (H.stored a < H.attempted e)))

def admitted (H : History) (W n : Nat) (S : EntrySet) : EntrySet := fun e =>
  S e && !Finality.landingTooLate H.stored (hadRead H.log) W n (fun _ => true) e

/-- A signer is checked in exactly the recorded past, never against the
receiving device's latest credentials. Creation authenticates its own keys. -/
def signatureCurrent (view : State) (e : Entry) : Bool :=
  match e.action with
  | .entry (.create ..) keys => e.signer == keys.signing
  | _ => (lookup view.keys e.author).any (fun keys => e.signer == keys.signing)

/-- An author with no keys in the recorded view has no current signer,
whatever key signed: the entry is judged like any other unbound signature
(T245). Rust refuses it before replay, so it never counts in lateness or
finality. -/
theorem unknown_author_not_current (view : State) (e : Entry)
    (notCreation : ∀ access key keys, e.action ≠ .entry (.create access key) keys)
    (absent : lookup view.keys e.author = none) :
    signatureCurrent view e = false := by
  unfold signatureCurrent
  split
  · rename_i access key keys h; exact absurd h (notCreation access key keys)
  · simp [absent]

/-- Reusing either public key cannot revoke a leaked credential. All keys
installed by kept entries remain in this replay-derived history. -/
def freshKeys (s : State) (keys : MemberKeys) : Bool :=
  !s.usedKeys.any (fun old => old.signing == keys.signing || old.sealing == keys.sealing)

def authorized (view : State) (e : Entry) : Bool :=
  signatureCurrent view e &&
    match e.action with
    | .replaceMemberKeys m keys =>
        freshKeys view keys && member view.membership m && member view.membership e.author &&
          (admin view.membership e.author || e.author == m)
    | .entry (.addMember m r access) keys =>
        admin view.membership e.author &&
          (if member view.membership m then
            lookup view.keys m == some keys && lookup view.membership.members m == some r &&
              lookup view.membership.access m == some access
           else freshKeys view keys)
    | .entry a _ => CovenStorelog.authorized view.membership (e.membership a)

/-- Replacement is an event even when its payload repeats: it still exposes
the old keys and participates in the replacement winner rule. -/
def unchanged (s : State) (e : Entry) : Bool :=
  match e.action with
  | .replaceMemberKeys .. => false
  | .entry (.addMember m r access) keys =>
      lookup s.membership.members m == some r &&
      lookup s.membership.access m == some access && lookup s.keys m == some keys
  | .entry a _ => alreadyInPlace s.membership (e.membership a)

def updateKeys (s : State) (e : Entry) : List (Nat × MemberKeys) :=
  match e.action with
  | .entry (.create ..) keys => [(e.author, keys)]
  | .entry (.addMember m ..) keys => put s.keys m keys
  | _ => s.keys

/-- Replacing credentials never adds a member, changes their role, restores
a device or restores a circle membership. Absence fails at replay time too. -/
def effect (view s : State) (id : Nat) (e : Entry) : Option State :=
  match e.action with
  | .replaceMemberKeys m keys =>
      if member s.membership m && (freshKeys s keys || lookup s.keys m == some keys) then
        some { s with keys := put s.keys m keys, usedKeys := keys :: s.usedKeys } else none
  | .entry a _ =>
      (ReplayPolicy.realize (fun _ => view.membership) s.membership id (e.membership a)).map
        (fun membership => ⟨membership, updateKeys s e,
          match e.action with
          | .entry (.create ..) keys | .entry (.addMember ..) keys => keys :: s.usedKeys
          | _ => s.usedKeys⟩)

def contradiction (va vb : State) (a b : Entry) : Bool :=
  match a.action, b.action with
  | .replaceMemberKeys m _, .replaceMemberKeys k _ => m == k
  | .replaceMemberKeys m _, .entry (.removeMember k) _ |
    .entry (.removeMember k) _, .replaceMemberKeys m _ => m == k
  | .replaceMemberKeys .., _ | _, .replaceMemberKeys .. => false
  | .entry (.addMember m ..) _, .entry (.addMember k ..) _ =>
      m == k && a.action != b.action
  | .entry x _, .entry y _ =>
      ReplayPolicy.contradiction va.membership vb.membership (a.membership x) (b.membership y)

def conflict (M : Log) (views : Nat → State) (a b : Nat) : Bool :=
  (a != b && !hadRead M a b && !hadRead M b a) && contradiction (views a) (views b) (M a) (M b)

/-- Self-replacement is determined by the stable target, including when the
member is an admin. An authorized other-member replacement requires an admin;
it takes priority without a caller-selected operation flag. -/
def tier (view : State) (e : Entry) : Nat :=
  match e.action with
  | .replaceMemberKeys m _ => if e.author == m then 4 else 3
  | .entry a _ => 2 * ReplayPolicy.tier view.membership a.membership

def prefer (M : Log) (views : Nat → State) (a b : Nat) : Bool :=
  tier (views a) (M a) < tier (views b) (M b) ||
    (tier (views a) (M a) == tier (views b) (M b) && a < b)

def materialize (H : History) (W n bound : Nat) (S : EntrySet) (views : Nat → State) : Result :=
  let live := admitted H W n S
  let r := ReplayMachine.settle State.empty (fun e => authorized (views e) (H.log e))
    (fun s e => unchanged s (H.log e)) (fun s e => effect (views e) s e (H.log e))
    (conflict H.log views) (prefer H.log views) ((List.range bound).filter live)
  let membership := (ReplayPolicy.finish ⟨r.state.membership, r.kept, r.dropped⟩).state
  { state := { r.state with membership }
    kept := r.kept
    dropped := r.dropped ++ (List.range bound).filter (fun e => S e && !live e) }

def authorViews (H : History) (W n : Nat) : Nat → Nat → State
  | 0 => fun _ => State.empty
  | k + 1 =>
      let prior := authorViews H W n k
      fun e => if e = k then
        (materialize H W n k (hadRead H.log k) prior).state else prior e

def resolve (H : History) (W n : Nat) (S : EntrySet) : Result :=
  materialize H W n n S (authorViews H W n n)

theorem equal_received (H : History) (W n : Nat) (A B : List Nat)
    (same : ∀ e, e ∈ A ↔ e ∈ B) :
    resolve H W n (entrySet A) = resolve H W n (entrySet B) := by
  have eq : entrySet A = entrySet B := by funext e; simp [entrySet, same e]
  rw [eq]

theorem member_keys_converge (H : History) (W n : Nat) (A B : List Nat)
    (same : ∀ e, e ∈ A ↔ e ∈ B) :
    (resolve H W n (entrySet A)).state.keys = (resolve H W n (entrySet B)).state.keys := by
  rw [equal_received H W n A B same]

theorem kept_authorized (H : History) (W n : Nat) (S : EntrySet) (e : Nat)
    (kept : e ∈ (resolve H W n S).kept) :
    authorized (authorViews H W n n e) (H.log e) = true :=
  (ReplayMachine.settle_kept _ _ _ _ _ _ _ e kept).2

theorem kept_current_signature (H : History) (W n : Nat) (S : EntrySet) (e : Nat)
    (kept : e ∈ (resolve H W n S).kept) :
    signatureCurrent (authorViews H W n n e) (H.log e) = true :=
  (Bool.and_eq_true_iff.mp (kept_authorized H W n S e kept)).1

theorem replacement_authority (view : State) (e : Entry) (m : Nat) (keys : MemberKeys)
    (action : e.action = .replaceMemberKeys m keys) :
    authorized view e = true ↔
      signatureCurrent view e = true ∧ freshKeys view keys = true ∧
      member view.membership m = true ∧
      member view.membership e.author = true ∧
      (admin view.membership e.author = true ∨ e.author = m) := by
  simp [authorized, action, and_assoc]

theorem removed_cannot_be_replaced (view s : State) (id m : Nat) (e : Entry) (keys : MemberKeys)
    (action : e.action = .replaceMemberKeys m keys) (absent : member s.membership m = false) :
    effect view s id e = none := by simp [effect, action, absent]

theorem replacement_preserves_membership (view s t : State) (id m : Nat)
    (e : Entry) (keys : MemberKeys) (action : e.action = .replaceMemberKeys m keys)
    (applied : effect view s id e = some t) : t.membership = s.membership := by
  simp only [effect, action] at applied
  split at applied <;> cases applied <;> rfl

theorem effect_safe (view s t : State) (id : Nat) (e : Entry)
    (prior : safe s.membership = true) (applied : effect view s id e = some t) :
    safe t.membership = true := by
  cases action : e.action with
  | replaceMemberKeys m keys =>
      rw [replacement_preserves_membership view s t id m e keys action applied]; exact prior
  | entry a keys =>
      simp only [effect, action, Option.map_eq_some_iff] at applied
      obtain ⟨next, hn, rfl⟩ := applied
      exact (checkedEffect_sound hn).2

theorem admin_remains (H : History) (W n : Nat) (S : EntrySet)
    (created : (resolve H W n S).state.membership.created = true) :
    hasAdmin (resolve H W n S).state.membership = true := by
  have h := ReplayMachine.settle_state State.empty
    (fun e => authorized (authorViews H W n n e) (H.log e))
    (fun s e => unchanged s (H.log e)) (fun s e => effect (authorViews H W n n e) s e (H.log e))
    (conflict H.log (authorViews H W n n)) (prefer H.log (authorViews H W n n))
    (fun s => safe s.membership = true) (by decide)
    (fun s e t hs he => effect_safe _ s t e _ hs he)
    ((List.range n).filter (admitted H W n S))
  change safe (resolve H W n S).state.membership = true at h
  simpa [safe, created] using h

theorem replacements_conflict (va vb : State) (a b : Entry) (m : Nat) (ka kb : MemberKeys)
    (ha : a.action = .replaceMemberKeys m ka) (hb : b.action = .replaceMemberKeys m kb) :
    contradiction va vb a b = true := by simp [contradiction, ha, hb]

theorem admin_preferred (M : Log) (views : Nat → State) (a b m : Nat) (ka kb : MemberKeys)
    (ha : (M a).action = .replaceMemberKeys m ka)
    (hb : (M b).action = .replaceMemberKeys m kb)
    (other : (M a).author ≠ m) (self : (M b).author = m) : prefer M views a b = true := by
  simp [prefer, tier, ha, hb, other, self]

theorem same_kind_timestamp (M : Log) (views : Nat → State) (a b m : Nat) (ka kb : MemberKeys)
    (ha : (M a).action = .replaceMemberKeys m ka)
    (hb : (M b).action = .replaceMemberKeys m kb)
    (same : ((M a).author == m) = ((M b).author == m)) :
    prefer M views a b = decide (a < b) := by
  simp only [prefer, tier, ha, hb, same]
  split <;> simp

theorem entry_deadline (H : History) (W n : Nat) (S : EntrySet) (e replacement : Nat)
    (kept : e ∈ (resolve H W n S).kept) (present : replacement < n)
    (unread : hadRead H.log e replacement = false) : H.stored e ≤ H.stored replacement + W := by
  have h := (ReplayMachine.settle_kept _ _ _ _ _ _ _ e kept).1
  have admitted : admitted H W n S e = true := (List.mem_filter.mp h).2
  by_cases late : H.stored e ≤ H.stored replacement + W
  · exact late
  exfalso
  have tooLate : Finality.landingTooLate H.stored (hadRead H.log) W n (fun _ => true) e = true := by
    apply List.any_eq_true.mpr
    exact ⟨replacement, List.mem_range.mpr present, by simp [unread, show H.stored replacement + W < H.stored e by omega]⟩
  simp [Keyed.admitted, tooLate] at admitted

end CovenStorelog.CurrentReplay.Keyed
