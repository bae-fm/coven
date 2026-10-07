import CovenStorelog.Model

namespace CovenStorelog

def priority : Action → Nat
  | .removeMember _ _ | .removeDevice _ _ | .deleteCircle _ | .removeFromCircle _ _ => 0
  | .addMember _ .admin _ | .changeRole _ .admin => 2
  | _ => 1

def before (M : Log) (a b : Nat) : Bool :=
  priority (M a).action < priority (M b).action ||
  (priority (M a).action == priority (M b).action && a < b)

/-- Priority followed by the unique timestamp is a strict total order. -/
theorem before_irrefl (M : Log) (a : Nat) : before M a a = false := by
  simp [before]

theorem before_trans (M : Log) {a b c : Nat}
    (hab : before M a b = true) (hbc : before M b c = true) : before M a c = true := by
  simp only [before, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
    beq_iff_eq] at *
  omega

theorem before_total (M : Log) {a b : Nat} (h : a ≠ b) :
    before M a b = true ∨ before M b a = true := by
  simp only [before, Bool.or_eq_true, Bool.and_eq_true, decide_eq_true_eq,
    beq_iff_eq]
  omega

theorem before_asymm (M : Log) {a b : Nat} (h : before M a b = true) :
    before M b a = false := by
  cases hr : before M b a
  · rfl
  · have := before_trans M h hr
    simp [before_irrefl] at this

def memberTarget : Action → Option Nat
  | .addMember m _ _ | .setAccess m _ | .removeMember m _ | .changeRole m _ |
    .addDevice m _ | .removeDevice m _ | .addToCircle _ m | .removeFromCircle _ m => some m
  | _ => none

def deviceTarget : Action → Option Nat
  | .addDevice _ d | .removeDevice _ d => some d
  | _ => none

def circleTarget : Action → Option Nat
  | .makeCircle c _ | .renameCircle c _ | .deleteCircle c |
    .addToCircle c _ | .removeFromCircle c _ => some c
  | .reset ⟨.circle c, _⟩ | .raiseSchema _ ⟨.circle c, _⟩ => some c
  | _ => none

def sameTarget [DecidableEq α] (a b : Option α) : Bool :=
  match a, b with
  | some x, some y => x == y
  | _, _ => false

def sameMeaning (a b : Entry) : Bool :=
  match a.action, b.action with
  | .addMember m r _, .changeRole n s | .changeRole m r, .addMember n s _ => m == n && r == s
  | .create _, .create _ => true
  | .addMember m r _, .addMember n s _ => m == n && r == s
  | .makeCircle c x, .makeCircle d y => c == d && x == y && a.author == b.author
  | _, _ => a.action == b.action

/-- A store removal names the circles whose keys it replaced. These names
are carried by the entry; replay membership cannot add names to that list. -/
def removesCircleKey (a : Action) (c : Nat) : Bool :=
  match a with
  | .removeFromCircle d _ => c == d
  | .removeMember _ circles => c ∈ circles
  | _ => false

/-- Whether an entry deletes a circle is judged in its author's view (§9):
removing the circle's only member there deletes it. A store removal's key
list names only circles left with members, so it plays no part here. -/
def deletesCircle (view : State) (a : Action) (c : Nat) : Bool :=
  match a with
  | .deleteCircle d => c == d
  | .removeMember m _ =>
      (lookup view.circles c).any (fun circle => circle.members == [m])
  | .removeFromCircle d m => c == d &&
      (lookup view.circles c).any (fun circle => circle.members == [m])
  | _ => false

def specialConflict (va vb : State) (a b : Action) : Bool :=
  (match a, b with
    | .addMember _ _ _, .removeMember _ _ | .removeMember _ _, .addMember _ _ _ => true
    | .removeMember _ _, .removeMember _ _ => true
    | .addToCircle c _, _ => removesCircleKey b c
    | _, .addToCircle c _ => removesCircleKey a c
    | .removeFromCircle c _, _ => removesCircleKey b c
    | _, .removeFromCircle c _ => removesCircleKey a c
    | .raiseSchema v s, .raiseSchema w t => s.audience == t.audience && v == w && s != t
    | .reset s, .reset t => s.audience == t.audience && s != t
    | .reset s, .raiseSchema _ t | .raiseSchema _ t, .reset s => s.audience == t.audience
    | _, _ => false) ||
  (match circleTarget b with | some c => deletesCircle va a c | none => false) ||
  (match circleTarget a with | some c => deletesCircle vb b c | none => false)

def pairConflict (M : Log) (views : Nat → State) (a b : Nat) : Bool :=
  concurrent M a b && !sameMeaning (M a) (M b) &&
    (sameTarget (memberTarget (M a).action) (memberTarget (M b).action) ||
     sameTarget (deviceTarget (M a).action) (deviceTarget (M b).action) ||
     specialConflict (views a) (views b) (M a).action (M b).action)

/-- Apart from being about the removed member, a circle addition conflicts
with a store removal exactly when the removal replaces that circle's key or,
in the remover's view, takes the circle's only member and so deletes it. -/
theorem store_removal_circle_add (M : Log) (views : Nat → State) (a b m n c : Nat)
    (keys : List Nat) (ha : (M a).action = .removeMember m keys)
    (hb : (M b).action = .addToCircle c n) :
    pairConflict M views a b = (concurrent M a b && (m == n || c ∈ keys ||
      (lookup (views a).circles c).any (fun circle => circle.members == [m]))) := by
  simp only [pairConflict, sameMeaning, ha, hb,
    sameTarget, memberTarget, deviceTarget, specialConflict, removesCircleKey,
    circleTarget, deletesCircle]
  cases concurrent M a b <;> cases m == n <;> cases decide (c ∈ keys) <;>
    cases (lookup (views a).circles c).any (fun circle => circle.members == [m]) <;> simp

end CovenStorelog
