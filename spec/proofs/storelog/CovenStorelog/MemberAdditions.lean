import CovenStorelog.MemberKeys
import CovenStorelog.Ownership

/-! # Member additions under stable ids (§9)

`Keyed` treats an addition's whole payload (keys, role and storage access) as
its meaning. An addition of an id already in the author's view is allowed only
when it repeats that payload exactly, and then changes nothing; two concurrent
additions of one id conflict unless their payloads are equal.

T104's first Rust rule differed in both places: a re-addition with the same
keys overwrote role and access, and concurrent additions conflicted only on
keys or role. `Overwrite` states that rule, and two checked histories show what
it breaks:

* `overwrite_sets_anothers_access`: an admin's re-addition changes another
  member's storage access, which no entry by that admin could do (§9: each
  member records their own access).
* `overwrite_keeps_unapplied_addition`: of two concurrent additions differing
  only in access, both stay kept, though the later one's access never holds.

`Keyed` refuses the first and drops the later addition of the second
(`keyed_refuses_readd`, `keyed_drops_concurrent_access`).
`authorized_readd_identical`, `addition_in_place` and
`additions_conflict_iff_different` state the keyed rule for every view.
Convergence and the admin invariant are `Keyed.equal_received` and
`Keyed.admin_remains`.
-/

namespace CovenStorelog.CurrentReplay.Keyed.Additions

/-! ## The keyed rule -/

/-- An addition its author's view authorizes for an id already there repeats
that id's keys, role and access exactly. -/
theorem authorized_readd_identical (view : State) (e : Entry) (m : Nat) (r : Role)
    (access : String) (keys : MemberKeys)
    (action : e.action = .entry (.addMember m r access) keys)
    (present : member view.membership m = true) (ok : authorized view e = true) :
    lookup view.keys m = some keys ∧ lookup view.membership.members m = some r ∧
      lookup view.membership.access m = some access := by
  simp only [authorized, action, present, ite_true, Bool.and_eq_true] at ok
  exact ⟨eq_of_beq ok.2.2.1.1, eq_of_beq ok.2.2.1.2, eq_of_beq ok.2.2.2⟩

/-- **A kept addition is in place where it applies.** Unchanged means the id
already has its keys, role and access; applied, it sets exactly those. -/
theorem addition_in_place (view s : State) (id : Nat) (e : Entry) (m : Nat) (r : Role)
    (access : String) (keys : MemberKeys)
    (action : e.action = .entry (.addMember m r access) keys) :
    (unchanged s e = true →
      lookup s.membership.members m = some r ∧ lookup s.membership.access m = some access ∧
        lookup s.keys m = some keys) ∧
    (∀ t, effect view s id e = some t →
      lookup t.membership.members m = some r ∧ lookup t.membership.access m = some access ∧
        lookup t.keys m = some keys) := by
  constructor
  · intro h
    simp only [unchanged, action, Bool.and_eq_true] at h
    exact ⟨eq_of_beq h.1.1, eq_of_beq h.1.2, eq_of_beq h.2⟩
  · intro t h
    simp only [effect, action, Option.map_eq_some_iff] at h
    obtain ⟨next, hn, rfl⟩ := h
    simp only [ReplayPolicy.realize, checkedEffect, Entry.membership, Action.membership,
      CovenStorelog.effect, Option.bind_eq_bind] at hn
    split at hn
    · simp only [Option.bind_some] at hn
      split at hn
      · cases hn
        simp [lookup_put, updateKeys, action]
      · cases hn
    · simp at hn

/-- **Concurrent additions of one id conflict exactly when their payloads
differ**: keys, role or access. Equal payloads are the same result. -/
theorem additions_conflict_iff_different (va vb : State) (a b : Entry) (m k : Nat)
    (r q : Role) (x y : String) (ka kb : MemberKeys)
    (ha : a.action = .entry (.addMember m r x) ka) (hb : b.action = .entry (.addMember k q y) kb) :
    contradiction va vb a b = true ↔ m = k ∧ a.action ≠ b.action := by
  rw [show contradiction va vb a b = (m == k && a.action != b.action) by
    simp only [contradiction, ha, hb]]
  simp

/-! ## T104's first Rust rule -/

namespace Overwrite

/-- A re-addition needs only the same keys; a new id needs no fresh keys. -/
def authorized (view : State) (e : Entry) : Bool :=
  match e.action with
  | .entry (.addMember m _ _) keys =>
      signatureCurrent view e && admin view.membership e.author &&
        (!member view.membership m || lookup view.keys m == some keys)
  | _ => Keyed.authorized view e

/-- Access does not count toward an addition already being in place. -/
def unchanged (s : State) (e : Entry) : Bool :=
  match e.action with
  | .entry (.addMember m r _) keys =>
      lookup s.membership.members m == some r && lookup s.keys m == some keys
  | _ => Keyed.unchanged s e

/-- Concurrent additions of one id conflict on different keys or roles only. -/
def contradiction (va vb : State) (a b : Entry) : Bool :=
  match a.action, b.action with
  | .entry (.addMember m r _) ka, .entry (.addMember k q _) kb => m == k && (ka != kb || r != q)
  | _, _ => Keyed.contradiction va vb a b

def conflict (M : Log) (views : Nat → State) (a b : Nat) : Bool :=
  (a != b && !hadRead M a b && !hadRead M b a) && contradiction (views a) (views b) (M a) (M b)

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

end Overwrite

/-! ## Checked histories -/

set_option maxRecDepth 8192
set_option maxHeartbeats 4000000

def storeKey : KeyCommitment := ⟨10, Vector.replicate 32 0⟩
def ana : MemberKeys := ⟨100, 200⟩
def ben : MemberKeys := ⟨101, 201⟩
def dan : MemberKeys := ⟨103, 203⟩

/-- Ana creates the store and adds Ben, who records access "ben". Ana then
re-adds Ben with his own keys, as an admin, with access "ana". -/
def readd : History := ⟨fun
  | 0 => ⟨0, 100, 0, [], .entry (.create "ana" storeKey) ana⟩
  | 1 => ⟨0, 100, 0, [0], .entry (.addMember 1 .member "ben") ben⟩
  | _ => ⟨0, 100, 0, [0, 1], .entry (.addMember 1 .admin "ana") ben⟩, id, id⟩

/-- **The overwrite rule changes another member's access.** Ana's re-addition
stays kept and sets Ben's access to "ana", though in that view Ana could not
record Ben's access herself. -/
theorem overwrite_sets_anothers_access :
    2 ∈ (Overwrite.resolve readd 30 3 (entrySet [0, 1, 2])).kept ∧
    lookup (Overwrite.resolve readd 30 3 (entrySet [0, 1, 2])).state.membership.access 1 =
      some "ana" ∧
    CovenStorelog.authorized (Overwrite.authorViews readd 30 3 3 2).membership
      ⟨0, 0, [0, 1], .setAccess 1 "ana"⟩ = false := by decide

/-- The keyed rule drops that re-addition; Ben keeps his own access and role. -/
theorem keyed_refuses_readd :
    2 ∈ (resolve readd 30 3 (entrySet [0, 1, 2])).dropped ∧
    lookup (resolve readd 30 3 (entrySet [0, 1, 2])).state.membership.access 1 = some "ben" ∧
    lookup (resolve readd 30 3 (entrySet [0, 1, 2])).state.membership.members 1 =
      some .member := by decide

/-- Ana's phone and tablet each add Dan, concurrently, with the same keys and
role but different access. -/
def concurrent : History := ⟨fun
  | 0 => ⟨0, 100, 0, [], .entry (.create "ana" storeKey) ana⟩
  | 1 => ⟨0, 100, 0, [0], .entry (.addMember 3 .member "x") dan⟩
  | _ => ⟨0, 100, 1, [0], .entry (.addMember 3 .member "y") dan⟩,
  id, fun e => if e = 2 then 1 else e⟩

/-- **The overwrite rule keeps an addition that never applied.** Both stay
kept, in every arrival order, but Dan's access is the earlier "x": the later
addition's "y" holds nowhere, and its author is never told. -/
theorem overwrite_keeps_unapplied_addition (arrivals : List Nat)
    (same : ∀ e, e ∈ arrivals ↔ e ∈ [0, 1, 2]) :
    1 ∈ (Overwrite.resolve concurrent 30 3 (entrySet arrivals)).kept ∧
    2 ∈ (Overwrite.resolve concurrent 30 3 (entrySet arrivals)).kept ∧
    lookup (Overwrite.resolve concurrent 30 3 (entrySet arrivals)).state.membership.access 3 =
      some "x" := by
  have eq : entrySet arrivals = entrySet [0, 1, 2] := by
    funext e; simp [entrySet, same e]
  rw [eq]
  decide

/-- The keyed rule drops the later addition, with its conflict as the reason. -/
theorem keyed_drops_concurrent_access :
    1 ∈ (resolve concurrent 30 3 (entrySet [0, 1, 2])).kept ∧
    2 ∈ (resolve concurrent 30 3 (entrySet [0, 1, 2])).dropped ∧
    lookup (resolve concurrent 30 3 (entrySet [0, 1, 2])).state.membership.access 3 =
      some "x" := by decide

theorem checked_metadata : metadataValid readd 3 = true ∧ metadataValid concurrent 3 = true := by
  decide

end CovenStorelog.CurrentReplay.Keyed.Additions
