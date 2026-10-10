import CovenStorelog.Finality

/-! §9's contradiction rules use the terminating replay from Appendix C.
The historical replay and its Rust differential interface remain separate. -/
namespace CovenStorelogData.CurrentReplay
open CovenStorelog

def membershipState : Action → Option Nat
  | .addMember m _ _ | .changeRole m _ | .removeMember m _ => some m
  | _ => none

def requiredMembers (e : Entry) : List Nat :=
  match e.action with
  | .setAccess m _ | .changeRole m _ | .addDevice m _ | .removeDevice m _ => [m]
  | .addToCircle _ m | .removeFromCircle _ m => [m]
  | .makeCircle _ _ | .deleteCircle _ => [e.author]
  | _ => []

def removesRequired (a b : Entry) : Bool :=
  match a.action with
  | .removeMember m _ => m ∈ requiredMembers b
  | _ => false

def sameResult (a b : Entry) : Bool :=
  match a.action, b.action with
  | .removeMember m _, .removeMember n _ => m == n
  | _, _ => sameMeaning a b

def contradiction (va vb : State) (a b : Entry) : Bool :=
  !sameResult a b &&
    (sameTarget (membershipState a.action) (membershipState b.action) ||
     sameTarget (deviceTarget a.action) (deviceTarget b.action) ||
     removesRequired a b || removesRequired b a ||
     (match a.action, b.action with
      | .setAccess m _, .setAccess n _ => m == n
      | .addToCircle c m, .removeFromCircle d n |
        .removeFromCircle c m, .addToCircle d n => c == d && m == n
      | .raiseSchema v s, .raiseSchema w t => s.audience == t.audience && v == w && s != t
      | .reset s, .reset t => s.audience == t.audience && s != t
      | .reset s, .raiseSchema _ t | .raiseSchema _ t, .reset s => s.audience == t.audience
      | _, _ => false) ||
     (circleTarget b.action).any (deletesCircle va a.action) ||
     (circleTarget a.action).any (deletesCircle vb b.action))

def conflict (M : Log) (views : Nat → State) (a b : Nat) : Bool :=
  concurrent M a b && contradiction (views a) (views b) (M a) (M b)

def tier (view : State) (a : Action) : Nat :=
  match a with
  | .removeMember _ _ | .removeDevice _ _ => 0
  | .deleteCircle _ => 1
  | .removeFromCircle c _ => if deletesCircle view a c then 1 else 2
  | .addMember _ .admin _ | .changeRole _ .admin => 4
  | _ => 3

def prefer (M : Log) (views : Nat → State) (a b : Nat) : Bool :=
  tier (views a) (M a).action < tier (views b) (M b).action ||
    (tier (views a) (M a).action == tier (views b) (M b).action && a < b)

theorem read_no_conflict (M : Log) (views : Nat → State) (a b : Nat)
    (read : hadRead M a b = true) : conflict M views a b = false := by
  simp [conflict, concurrent, read]

/-- §13 checks the complete set of nonempty circles in the immutable author view. -/
def keysMatch (view : State) (entry : Entry) : Bool :=
  match entry.action with
  | .removeMember m keys =>
      let expected := view.circles.filterMap fun (c, circle) =>
        if m ∈ circle.members && circle.members.any (· != m) then some c else none
      keys.all (· ∈ expected) && expected.all (· ∈ keys)
  | _ => true

/-- Permanent time rejection is computed against the complete storage history,
including entries outside this author's past (§9). -/
def admitted (H : Finality.History) (W n : Nat) (views : Nat → State)
    (S : EntrySet) : EntrySet := fun e =>
  S e && !Finality.tooLate H W n (fun _ => true) e && keysMatch (views e) (H.log e)

def materialize (H : Finality.History) (W n bound : Nat) (S : EntrySet)
    (views : Nat → State) : Result :=
  let live := admitted H W n views S
  let r := settle H.log views ((List.range bound).filter live)
    (conflict H.log views) (prefer H.log views)
  { r with dropped := r.dropped ++ (List.range bound).filter (fun e => S e && !live e) }

def authorViews (H : Finality.History) (W n : Nat) : Nat → Nat → State
  | 0 => fun _ => State.empty
  | k + 1 =>
      let prior := authorViews H W n k
      fun e => if e = k then
        (materialize H W n k (hadRead H.log k) prior).state else prior e

def resolve (H : Finality.History) (W n : Nat) (S : EntrySet) : Result :=
  materialize H W n n S (authorViews H W n n)

theorem equal_received (H : Finality.History) (W n : Nat) (A B : List Nat)
    (same : ∀ e, e ∈ A ↔ e ∈ B) :
    resolve H W n (entrySet A) = resolve H W n (entrySet B) := by
  have he : entrySet A = entrySet B := by funext e; simp [entrySet, same e]
  rw [he]

/-- The examples distinguish compatible changes from merely shared ids. -/
example (m d e : Nat) (different : d ≠ e) (view : State) :
    contradiction view view ⟨m, d, [], .addDevice m d⟩ ⟨m, e, [], .addDevice m e⟩ = false := by
  simp [contradiction, sameResult, sameMeaning, sameTarget, membershipState,
    deviceTarget, removesRequired, circleTarget, different]

example (m : Nat) (view : State) :
    contradiction view view ⟨m, 0, [], .addDevice m 1⟩ ⟨m, 2, [], .setAccess m "replacement"⟩ = false := by
  simp [contradiction, sameResult, sameMeaning, sameTarget, membershipState,
    deviceTarget, removesRequired, circleTarget]

example (view : State) :
    contradiction view view ⟨0, 0, [], .deleteCircle 1⟩
      ⟨0, 1, [], .reset ⟨.circle 1, 4⟩⟩ = true := by
  simp [contradiction, sameResult, sameMeaning, sameTarget, membershipState,
    deviceTarget, removesRequired, circleTarget, deletesCircle]

end CovenStorelogData.CurrentReplay
