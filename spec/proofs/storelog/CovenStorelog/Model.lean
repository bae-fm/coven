import Std

/-! Store-log data and entry effects. An entry's identifier is its unique
timestamp; a finite log occupies
an initial interval after order-preserving renumbering. No arithmetic duration
is used. Receipt is causal; timestamp order is used only to materialize a
chosen history. The author is supplied by signature verification (§9). -/

namespace CovenStorelog

inductive Role where
  | member | admin
  deriving DecidableEq, Repr

inductive Audience where
  | store | circle (id : Nat)
  deriving DecidableEq, Repr

/-- The audience and an abstract identity for the snapshot's device/number pair. -/
structure SnapshotId where
  audience : Audience
  number : Nat
  deriving DecidableEq, Repr

inductive Action where
  | create (access : String)
  | addMember (member : Nat) (role : Role) (access : String)
  | setAccess (member : Nat) (access : String)
  | removeMember (member : Nat) (circleKeys : List Nat)
  | changeRole (member : Nat) (role : Role)
  | addDevice (member device : Nat)
  | removeDevice (member device : Nat)
  | makeCircle (circle : Nat) (name : String)
  | renameCircle (circle : Nat) (name : String)
  | deleteCircle (circle : Nat)
  | addToCircle (circle member : Nat)
  | removeFromCircle (circle member : Nat)
  | raiseSchema (version : Nat) (snapshot : SnapshotId)
  | reset (snapshot : SnapshotId)
  | rotateKey (audience : Audience) (key : Nat)
  deriving DecidableEq, Repr

/-- The creation tag, independently of its initial access. -/
def Action.isCreation : Action → Bool
  | .create _ => true
  | _ => false

theorem Action.isCreation_true (a : Action) :
    a.isCreation = true ↔ ∃ access, a = .create access := by
  cases a <;> simp [Action.isCreation]

structure Entry where
  author : Nat
  device : Nat
  past : List Nat
  action : Action
  deriving DecidableEq, Repr

abbrev Log := Nat → Entry
abbrev EntrySet := Nat → Bool

def hadRead (M : Log) (w a : Nat) : Bool := a ∈ (M w).past

def concurrent (M : Log) (a b : Nat) : Bool :=
  a != b && !hadRead M a b && !hadRead M b a

def Closed (M : Log) (S : EntrySet) : Prop :=
  ∀ w, S w = true → ∀ a, hadRead M w a = true → S a = true

/-- The same closed-set and causal-order notions as Appendix B. The own-device
condition records §7.1's requirement to read one's own earlier entries. -/
structure Valid (M : Log) (n : Nat) : Prop where
  past_lt : ∀ w, w < n → ∀ a, hadRead M w a = true → a < w
  past_closed : ∀ w, w < n → Closed M (hadRead M w)
  own_past : ∀ a b, a < b → b < n → (M a).device = (M b).device →
    hadRead M b a = true
  root : (M 0).action.isCreation = true
  only_root : ∀ w, w < n → (M w).action.isCreation = true → w = 0
  read_root : ∀ w, 0 < w → w < n → hadRead M w 0 = true

inductive CausalOrder (M : Log) : List Nat → Prop where
  | nil : CausalOrder M []
  | snoc {L : List Nat} {w : Nat} : CausalOrder M L → w ∉ L →
      (∀ a, hadRead M w a = true → a ∈ L) → CausalOrder M (L ++ [w])

structure Circle where
  name : String
  members : List Nat
  deriving DecidableEq, Repr

structure Version where
  number : Nat
  snapshot : Nat
  entry : Nat
  deriving DecidableEq, Repr

structure State where
  created : Bool
  members : List (Nat × Role)
  access : List (Nat × String)
  devices : List (Nat × Nat)
  circles : List (Nat × Circle)
  versions : List (Audience × Version)
  resets : List (Audience × Nat)
  deriving DecidableEq, Repr

def State.empty : State := ⟨false, [], [], [], [], [], []⟩

def lookup [DecidableEq α] : List (α × β) → α → Option β
  | [], _ => none
  | (key, value) :: xs, k => if key = k then some value else lookup xs k

def put [DecidableEq α] (xs : List (α × β)) (k : α) (v : β) : List (α × β) :=
  (k, v) :: xs.filter (fun p => p.1 != k)

def erase [DecidableEq α] (xs : List (α × β)) (k : α) : List (α × β) :=
  xs.filter (fun p => p.1 != k)

def member (s : State) (m : Nat) : Bool := (lookup s.members m).isSome
def admin (s : State) (m : Nat) : Bool := lookup s.members m == some .admin
def inCircle (s : State) (c m : Nat) : Bool :=
  match lookup s.circles c with
  | none => false
  | some circle => member s m && m ∈ circle.members

def hasAdmin (s : State) : Bool := s.members.any (fun p => admin s p.1)

/-- The empty state precedes creation. A created store must keep an admin. -/
def safe (s : State) : Bool := !s.created || hasAdmin s

/-- Authority comes from the signature and is checked before the in-place
rule. The writing device is not a second authority check. -/
def authorized (s : State) (e : Entry) : Bool :=
  match e.action with
  | .create _ => !s.created
  | .addMember _ _ _ | .removeMember _ _ | .changeRole _ _ => admin s e.author
  | .setAccess m _ | .addDevice m _ => member s e.author && e.author == m
  | .removeDevice m d => lookup s.devices d == some m &&
      member s e.author && (e.author == m || admin s e.author)
  | .makeCircle _ _ | .raiseSchema _ ⟨.store, _⟩ => member s e.author
  | .raiseSchema _ ⟨.circle c, _⟩ => inCircle s c e.author
  | .renameCircle c _ | .deleteCircle c | .addToCircle c _ | .removeFromCircle c _ =>
      inCircle s c e.author
  | .reset ⟨.store, _⟩ => admin s e.author
  | .reset ⟨.circle c, _⟩ => inCircle s c e.author
  | .rotateKey .store _ => member s e.author
  | .rotateKey (.circle c) _ => inCircle s c e.author

def alreadyInPlace (s : State) (e : Entry) : Bool :=
  match e.action with
  | .create _ => s.created
  | .addMember m r _ | .changeRole m r => lookup s.members m == some r
  | .setAccess m access => member s m && lookup s.access m == some access
  | .removeMember m _ => !member s m
  | .addDevice m d => lookup s.devices d == some m
  | .removeDevice _ d => (lookup s.devices d).isNone
  | .makeCircle c name => lookup s.circles c == some ⟨name, [e.author]⟩
  | .renameCircle c name => (lookup s.circles c).any (fun x => x.name == name)
  | .deleteCircle c => (lookup s.circles c).isNone
  | .addToCircle c m => inCircle s c m
  | .removeFromCircle c m => !inCircle s c m
  | .raiseSchema v snapshot => (lookup s.versions snapshot.audience).any
      (fun x => v < x.number || (v == x.number && snapshot.number == x.snapshot))
  | .reset snapshot => lookup s.resets snapshot.audience == some snapshot.number
  | .rotateKey _ _ => false

/-- Removing a circle's last member deletes the circle. -/
def withoutMember (circle : Circle) (m : Nat) : Option Circle :=
  let members := circle.members.filter (· != m)
  if members.isEmpty then none else some { circle with members }

def removeFromCircles (circles : List (Nat × Circle)) (m : Nat)
    (remove : Nat → Circle → Nat → Option Circle := fun _ => withoutMember) : List (Nat × Circle) :=
  circles.filterMap fun (c, circle) => (remove c circle m).map (c, ·)

def audienceExists (s : State) : Audience → Bool
  | .store => s.created
  | .circle c => (lookup s.circles c).isSome

/-- Realize a change after the authority and already-in-place checks. -/
def effect (s : State) (w : Nat) (e : Entry)
    (remove : Nat → Circle → Nat → Option Circle := fun _ => withoutMember) : Option State := do
  match e.action with
  | .create access =>
      if s.created then none else some { State.empty with
        created := true, members := [(e.author, .admin)], access := [(e.author, access)],
        devices := [(e.device, e.author)] }
  | .addMember m r access =>
      if s.created then some { s with
        members := put s.members m r
        access := put s.access m access } else none
  | .setAccess m access =>
      if member s m then some { s with access := put s.access m access } else none
  | .removeMember m _ =>
      if member s m then some { s with
        members := erase s.members m
        devices := s.devices.filter (fun p => p.2 != m)
        circles := removeFromCircles s.circles m remove } else none
  | .changeRole m r =>
      if member s m then some { s with members := put s.members m r } else none
  | .addDevice m d =>
      if member s m && (lookup s.devices d).isNone
      then some { s with devices := put s.devices d m } else none

  | .removeDevice m d =>
      if lookup s.devices d == some m
      then some { s with devices := erase s.devices d } else none
  | .makeCircle c name =>
      if member s e.author && (lookup s.circles c).isNone
      then some { s with circles := put s.circles c ⟨name, [e.author]⟩ } else none
  | .renameCircle c name =>
      let circle ← lookup s.circles c
      some { s with circles := put s.circles c { circle with name } }
  | .deleteCircle c =>
      if (lookup s.circles c).isSome
      then some { s with circles := erase s.circles c } else none
  | .addToCircle c m =>
      let circle ← lookup s.circles c
      if member s m then some { s with
        circles := put s.circles c { circle with members := m :: circle.members } } else none
  | .removeFromCircle c m =>
      let circle ← lookup s.circles c
      if m ∈ circle.members then
        match remove c circle m with
        | none => some { s with circles := erase s.circles c }
        | some next => some { s with circles := put s.circles c next }
      else none
  | .raiseSchema v snapshot =>
      if audienceExists s snapshot.audience then
        some { s with versions := put s.versions snapshot.audience ⟨v, snapshot.number, w⟩ }
      else none
  | .reset snapshot =>
      if audienceExists s snapshot.audience then
        some { s with resets := put s.resets snapshot.audience snapshot.number } else none

  | .rotateKey _ _ => some s

def checkedEffect (s : State) (w : Nat) (e : Entry)
    (remove : Nat → Circle → Nat → Option Circle := fun _ => withoutMember) : Option State := do
  let next ← effect s w e remove
  if safe next then some next else none

end CovenStorelog
