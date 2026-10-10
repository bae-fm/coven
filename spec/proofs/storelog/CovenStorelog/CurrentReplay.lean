import CovenStorelog.ReplayCompatibility

/-! §9 replay on D6's key-free removals and committed key introductions.
The membership projection shares the terminating engine with C1–C9. -/
namespace CovenStorelog.CurrentReplay

def contradiction (va vb : State) (a b : Entry) : Bool :=
  ReplayPolicy.contradiction va vb a.membership b.membership

def tier (view : State) (a : Action) : Nat := ReplayPolicy.tier view a.membership

def authorViews {α : Type} [ReplayInput α] (H : α) (W n k : Nat) : Nat → State :=
  ReplayPolicy.authorViews (ReplayInput.membership H) W n k (ReplayInput.admission (α := α))

def resolve {α : Type} [ReplayInput α] (H : α) (W n : Nat) (S : EntrySet) : Result :=
  ReplayPolicy.resolve (ReplayInput.membership H) W n S (ReplayInput.admission (α := α))

def circleCause {α : Type} [ReplayInput α] (H : α) (W n : Nat) (S : EntrySet) (c : Nat) : Option Nat :=
  ReplayPolicy.circleCause (ReplayInput.membership H) W n S c (ReplayInput.admission (α := α))

def removesFrom (view : State) (entry : Entry) (c : Nat) : Bool :=
  ReplayPolicy.removesFrom view entry.membership c

def deletionCause (M : Log) (views : Nat → State) (r : Result) (c : Nat) : Option Nat :=
  ReplayPolicy.deletionCause (membershipLog M) views r c

abbrev finish := ReplayPolicy.finish

theorem empty_cause_latest (M : Log) (views : Nat → State) (r : Result) (c e : Nat)
    (circle : Circle) (present : lookup r.state.circles c = some circle)
    (empty : circle.members = []) (cause : deletionCause M views r c = some e) :
    e ∈ r.kept ∧ removesFrom (views e) (M e) c = true ∧
      ∀ other ∈ r.kept, removesFrom (views other) (M other) c = true → other ≤ e :=
  ReplayPolicy.empty_cause_latest _ _ _ _ _ _ present empty cause

theorem finished_circles_nonempty (r : Result) (c : Nat) (circle : Circle)
    (present : (c, circle) ∈ (finish r).state.circles) : circle.members ≠ [] :=
  ReplayPolicy.finished_circles_nonempty r c circle present

theorem equal_received {α : Type} [ReplayInput α] (H : α) (W n : Nat) (A B : List Nat)
    (same : ∀ e, e ∈ A ↔ e ∈ B) :
    resolve H W n (entrySet A) = resolve H W n (entrySet B) := by
  have eq : entrySet A = entrySet B := by funext e; simp [entrySet, same e]
  rw [eq]

theorem kept_authorized (H : History) (W n : Nat) (S : EntrySet) (e : Nat)
    (kept : e ∈ (resolve H W n S).kept) :
    authorized (authorViews H W n n e) (H.log e) = true :=
  ReplayPolicy.kept_authorized H.membership W n S e kept

theorem kept_received (H : History) (W n : Nat) (S : EntrySet) (e : Nat)
    (kept : e ∈ (resolve H W n S).kept) : e < n ∧ S e = true := by
  have h := (settleN_kept H.membership.log _ _
    (settle_eq_some H.membership.log _ _ (ReplayPolicy.conflict H.membership.log _)
      (ReplayPolicy.prefer H.membership.log _) (ReplayPolicy.realize _)) e kept).1
  change e ∈ (List.range n).filter (ReplayPolicy.admitted H.membership W n S) at h
  simp only [List.mem_filter, List.mem_range, ReplayPolicy.admitted,
    Bool.and_eq_true, Bool.not_eq_true'] at h
  exact ⟨h.1, h.2.1⟩

theorem rotation_authority (H : History) (W n : Nat) (S : EntrySet) (e : Nat)
    (key : KeyCommitment) (audience : Audience)
    (action : (H.log e).action = .rotateKey audience key)
    (kept : e ∈ (resolve H W n S).kept) :
    match audience with
    | .store => member (authorViews H W n n e) (H.log e).author = true
    | .circle c => inCircle (authorViews H W n n e) c (H.log e).author = true := by
  have h := kept_authorized H W n S e kept
  cases audience <;> simpa [authorized, Entry.membership, Action.membership,
    CovenStorelog.authorized, action] using h

theorem rotations_conflict_with_nothing (a b : Entry) (va vb : State)
    (audience : Audience) (key : KeyCommitment)
    (rotation : a.action = .rotateKey audience key) :
    contradiction va vb a b = false ∧ contradiction vb va b a = false :=
  ReplayPolicy.rotations_conflict_with_nothing a.membership b.membership va vb audience key.id
    (by simp [Entry.membership, Action.membership, rotation])

theorem tier_results (view : State) (m d c : Nat) :
    tier view (.removeMember m) = 0 ∧ tier view (.removeDevice m d) = 0 ∧
    tier view (.deleteCircle c) = 1 ∧
    tier view (.removeFromCircle c m) =
      (if (lookup view.circles c).any (fun circle => circle.members == [m]) then 1 else 2) ∧
    tier view (.setAccess m "replacement") = 3 ∧ tier view (.changeRole m .admin) = 4 := by
  simp [tier, ReplayPolicy.tier, Action.membership, deletesCircle]

theorem store_removals_compatible (va vb : State) (a b : Entry) (m k : Nat)
    (ha : a.action = .removeMember m) (hb : b.action = .removeMember k) :
    contradiction va vb a b = false := by
  simp [contradiction, Entry.membership, Action.membership, ha, hb,
    ReplayPolicy.contradiction, ReplayPolicy.sameResult, ReplayPolicy.membershipState,
    ReplayPolicy.removesRequired, ReplayPolicy.requiredMembers, sameTarget,
    deviceTarget, circleTarget]

theorem resolve_safe (H : History) (W n : Nat) (S : EntrySet) :
    safe (resolve H W n S).state = true := by
  have h := settleN_state H.membership.log (authorViews H W n n)
    ((List.range n).filter (ReplayPolicy.admitted H.membership W n S))
    (fun s => safe s = true) (by decide) (realize := ReplayPolicy.realize (authorViews H W n n))
    (fun _ _ _ _ effect => (checkedEffect_sound effect).2)
    (settle_eq_some H.membership.log _ _ (ReplayPolicy.conflict H.membership.log (authorViews H W n n))
      (ReplayPolicy.prefer H.membership.log (authorViews H W n n)) (ReplayPolicy.realize _))
  exact h

theorem admin_remains (H : History) (W n : Nat) (S : EntrySet)
    (created : (resolve H W n S).state.created = true) :
    hasAdmin (resolve H W n S).state = true := by
  simpa [safe, created] using resolve_safe H W n S

end CovenStorelog.CurrentReplay
