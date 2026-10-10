import CovenStorelog.FinalityReplay

/-! D6's current action boundary. The historical action type remains the Rust
runner's input; membership replay forgets cryptographic commitments. -/
namespace CovenStorelog.CurrentReplay

abbrev KeyBytes := Vector UInt8 32
abbrev KeyHash := Vector UInt8 32

structure KeyCommitment where
  id : Nat
  keyHash : KeyHash
  deriving DecidableEq, Repr

inductive Action where
  | create (access : String) (key : KeyCommitment)
  | addMember (member : Nat) (role : Role) (access : String)
  | setAccess (member : Nat) (access : String)
  | removeMember (member : Nat)
  | changeRole (member : Nat) (role : Role)
  | addDevice (member device : Nat)
  | removeDevice (member device : Nat)
  | makeCircle (circle : Nat) (name : String) (key : KeyCommitment)
  | renameCircle (circle : Nat) (name : String)
  | deleteCircle (circle : Nat)
  | addToCircle (circle member : Nat)
  | removeFromCircle (circle member : Nat)
  | raiseSchema (version : Nat) (snapshot : SnapshotId)
  | reset (snapshot : SnapshotId)
  | rotateKey (audience : Audience) (key : KeyCommitment)
  deriving DecidableEq, Repr

/-- Reuse the membership engine. The empty historical list states that a
removal replaces no keys; ReplayPolicy never inspects that field. -/
def Action.membership : Action → CovenStorelog.Action
  | .create access _ => .create access
  | .addMember m r access => .addMember m r access
  | .setAccess m access => .setAccess m access
  | .removeMember m => .removeMember m []
  | .changeRole m r => .changeRole m r
  | .addDevice m d => .addDevice m d
  | .removeDevice m d => .removeDevice m d
  | .makeCircle c name _ => .makeCircle c name
  | .renameCircle c name => .renameCircle c name
  | .deleteCircle c => .deleteCircle c
  | .addToCircle c m => .addToCircle c m
  | .removeFromCircle c m => .removeFromCircle c m
  | .raiseSchema v s => .raiseSchema v s
  | .reset s => .reset s
  | .rotateKey a key => .rotateKey a key.id

structure Entry where
  author : Nat
  device : Nat
  past : List Nat
  action : Action
  deriving DecidableEq, Repr

def Entry.membership (e : Entry) : CovenStorelog.Entry :=
  ⟨e.author, e.device, e.past, e.action.membership⟩

abbrev Log := Nat → Entry

def membershipLog (M : Log) : CovenStorelog.Log := fun e => (M e).membership

structure History where
  log : Log
  stored : Nat → Nat
  attempted : Nat → Nat

def History.membership (H : History) : Finality.History :=
  ⟨membershipLog H.log, H.stored, H.attempted⟩

abbrev Valid (M : Log) (n : Nat) := CovenStorelog.Valid (membershipLog M) n

def authorized (s : State) (e : Entry) : Bool := CovenStorelog.authorized s e.membership

/-- Only D6 tags 0, 6 and 15 introduce keys. No removal has key fields. -/
def Action.introduction : Action → Option (Audience × KeyCommitment)
  | .create _ key => some (.store, key)
  | .makeCircle c _ key => some (.circle c, key)
  | .rotateKey audience key => some (audience, key)
  | _ => none

theorem introduction_cases (a : Action) (audience : Audience) (key : KeyCommitment)
    (h : a.introduction = some (audience, key)) :
    (audience = .store ∧ ∃ access, a = .create access key) ∨
    (∃ c name, audience = .circle c ∧ a = .makeCircle c name key) ∨
    a = .rotateKey audience key := by
  cases a <;> simp_all [Action.introduction]

theorem removals_introduce_nothing (m c : Nat) :
    (Action.removeMember m).introduction = none ∧
    (Action.removeFromCircle c m).introduction = none := ⟨rfl, rfl⟩

end CovenStorelog.CurrentReplay
