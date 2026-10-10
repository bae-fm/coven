import CovenStorelog.Finality
import CovenStorelog.Invariants

/-! §9's contradiction rules use the terminating replay from Appendix C.
The historical replay and its Rust differential interface remain separate. -/
namespace CovenStorelog.ReplayPolicy
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

/-- Permanent time rejection is computed against the complete storage history,
including entries outside this author's past (§9). -/
def admitted (H : Finality.History) (W n : Nat) (S : EntrySet)
    (check : Option (Nat → Bool) := none) : EntrySet := fun e =>
  let timely := S e && !Finality.tooLate H W n (fun _ => true) e
  match check with
  | none => timely
  | some check => timely && check e

/-- Only a removal that saw the circle's sole member deletes it during replay.
Other removals retain the empty membership until the whole replay finishes. -/
def removeInView (view : State) (c : Nat) (circle : Circle) (m : Nat) : Option Circle :=
  if (lookup view.circles c).any (fun old => old.members == [m]) then none
  else some { circle with members := circle.members.filter (· != m) }

def realize (views : Nat → State) (s : State) (e : Nat) (entry : Entry) : Option State :=
  checkedEffect s e entry (removeInView (views e))

/-- §14.6: emptiness is a result of the complete replay, never a new conflict. -/
def finish (r : Result) : Result :=
  { r with state := { r.state with circles := r.state.circles.filter (fun p => !p.2.members.isEmpty) } }

/-- Membership removals affecting this circle, from their immutable recorded past. -/
def removesFrom (view : State) (entry : Entry) (c : Nat) : Bool :=
  match entry.action with
  | .removeMember m _ => inCircle view c m
  | .removeFromCircle d m => c == d && inCircle view c m
  | _ => false

/-- A result's lost-row cause is separate from conflict classification. Explicit
or author-view deletion uses its deleting entry; a replay-empty circle uses
the latest kept removal. Entry ids have timestamp order. -/
def deletionCause (M : Log) (views : Nat → State) (r : Result) (c : Nat) : Option Nat :=
  match lookup r.state.circles c with
  | some circle => if circle.members.isEmpty then
      (r.kept.filter (fun e => removesFrom (views e) (M e) c)).max?
    else none
  | none => (r.kept.filter (fun e => deletesCircle (views e) (M e).action c)).max?

theorem empty_cause_latest (M : Log) (views : Nat → State) (r : Result) (c e : Nat)
    (circle : Circle) (present : lookup r.state.circles c = some circle)
    (empty : circle.members = []) (cause : deletionCause M views r c = some e) :
    e ∈ r.kept ∧ removesFrom (views e) (M e) c = true ∧
      ∀ other ∈ r.kept, removesFrom (views other) (M other) c = true → other ≤ e := by
  have maximum : (r.kept.filter (fun e => removesFrom (views e) (M e) c)).max? = some e := by
    simpa [deletionCause, present, empty] using cause
  have h := List.max?_eq_some_iff.mp maximum
  have mem := List.mem_filter.mp h.1
  exact ⟨mem.1, mem.2, fun other kept affects => h.2 other (List.mem_filter.mpr ⟨kept, affects⟩)⟩

theorem finished_circles_nonempty (r : Result) (c : Nat) (circle : Circle)
    (present : (c, circle) ∈ (finish r).state.circles) : circle.members ≠ [] := by
  have h := (List.mem_filter.mp present).2
  simpa using h

def replay (H : Finality.History) (W n bound : Nat) (S : EntrySet)
    (views : Nat → State) (check : Option (State → Entry → Bool) := none) : Result :=
  let live := admitted H W n S (check.map fun f e => f (views e) (H.log e))
  let r := settle H.log views ((List.range bound).filter live)
    (conflict H.log views) (prefer H.log views) (realize views)
  { r with dropped := r.dropped ++ (List.range bound).filter (fun e => S e && !live e) }

def materialize (H : Finality.History) (W n bound : Nat) (S : EntrySet)
    (views : Nat → State) (check : Option (State → Entry → Bool) := none) : Result :=
  finish (replay H W n bound S views check)

/-- An optional admission predicate supports the historical data package.
The current D6 policy supplies none, so removals have no key admission gate. -/
def authorViews (H : Finality.History) (W n k : Nat)
    (check : Option (State → Entry → Bool) := none) : Nat → State :=
  match k with
  | 0 => fun _ => State.empty
  | k + 1 =>
      let prior := authorViews H W n k check
      fun e => if e = k then
        (materialize H W n k (hadRead H.log k) prior check).state else prior e

def resolve (H : Finality.History) (W n : Nat) (S : EntrySet)
    (check : Option (State → Entry → Bool) := none) : Result :=
  materialize H W n n S (authorViews H W n n check) check

def circleCause (H : Finality.History) (W n : Nat) (S : EntrySet) (circle : Nat)
    (check : Option (State → Entry → Bool) := none) : Option Nat :=
  let views := authorViews H W n n check
  deletionCause H.log views (replay H W n n S views check) circle

theorem equal_received (H : Finality.History) (W n : Nat) (A B : List Nat)
    (same : ∀ e, e ∈ A ↔ e ∈ B) :
    resolve H W n (entrySet A) = resolve H W n (entrySet B) := by
  have he : entrySet A = entrySet B := by funext e; simp [entrySet, same e]
  rw [he]

theorem kept_authorized (H : Finality.History) (W n : Nat) (S : EntrySet) (e : Nat)
    (kept : e ∈ (resolve H W n S).kept) :
    authorized (authorViews H W n n none e) (H.log e) = true := by
  exact (settleN_kept H.log _ _
    (settle_eq_some H.log _ _ (conflict H.log _) (prefer H.log _) (realize _)) e kept).2

theorem rotation_authority (H : Finality.History) (W n : Nat) (S : EntrySet) (e key : Nat)
    (audience : Audience) (action : (H.log e).action = .rotateKey audience key)
    (kept : e ∈ (resolve H W n S).kept) :
    match audience with
    | .store => member (authorViews H W n n none e) (H.log e).author = true
    | .circle c => inCircle (authorViews H W n n none e) c (H.log e).author = true := by
  have h := kept_authorized H W n S e kept
  cases audience <;> simpa [authorized, action] using h

theorem rotations_conflict_with_nothing (a b : Entry) (va vb : State)
    (audience : Audience) (key : Nat) (rotation : a.action = .rotateKey audience key) :
    contradiction va vb a b = false ∧ contradiction vb va b a = false := by
  cases hb : b.action <;>
    simp [contradiction, sameResult, sameMeaning, rotation, hb, membershipState,
      sameTarget, deviceTarget, removesRequired, requiredMembers, circleTarget, deletesCircle]
  all_goals simp only [Option.any]; split <;> rfl

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

end CovenStorelog.ReplayPolicy
