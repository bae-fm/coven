import CovenSequence.Keys

/-!
# Copies before need (§12.1, §12.3)

A device reads slots in order, and a slot it can't open stops it there (§5.4):
reading in order, it never reaches a key in a later slot (`keyless_stops`). So
a member following an audience reads a slot of that audience without waiting
only if a copy of the slot's key, sealed to the member's current sealing key,
sits below it. This module states that claim for every history whose devices
follow §12.1's order, as `CopyPrecedesNeed`, shows where it holds and where it
doesn't, and shows that where it fails, a stuck reader that scans ahead for its
copy finds it once a member of the audience has sent what it owes.

The model is `Keys`' projection (`keysAt`) beside a roster of who reads each
audience with which sealing key (`Keys.Current`), as `effects.rs` and
`circle_effects.rs` keep it. An entry's `required` copies are what Rust's
`required_copies` gives it. An admission, which admits or recovers a member
(§13.2), carries those copies in its own slot (`carried`); any other entry's
sit below it. The admission also makes the member's circle: it introduces the
circle's key, with a copy to the member alone, in the same slot, so a circle's
first reader has its copy from the slot that makes the circle. `legal` says a slot followed the rules at its position: an
entry's required copies sit below it, or in its own slot for an admission,
and every key that seals the slot is introduced. The claim loop makes that the
state of the slot just before the entry, whatever raced it: an operation reads
every slot before each claim, works out its copies again, and lands only at
the slot after what it read (`operation_outcome`, with validation asking for
those copies). A slot here is what landed; `Follows` ranges over every
interleaving of its devices, so two admins racing, a member joining
mid-rotation and a replacement landing among a rotation's copies are all
histories of `Follows`, not cases of their own
(`CopyOrderExamples.races_follow`). Store creation holds the rule in its one
slot, as an admission does: slot 1 sets up the first member and key and carries
the creator's copy of that key (§10.3, §12.1).

`CopyPrecedesNeed` fails. An admin outside a circle who recovers a member can
send only the keys it holds (`required`, §12.1), so a circle's key has
no copy to the member's new sealing key until a circle member's next pass sends
it, and a write already sealed with that key can land first
(`CopyOrderExamples.copy_precedes_need_fails`). That is the only way it fails: a
store key always has its copy first (`store_copy_precedes_need`), and so does a
circle's key when every replacement or recovery of one of the circle's
members comes from another of them (`copy_precedes_need_inside`).

Removing a member from a circle is an entry that introduces the circle's
replacement key, with its copies below it to the readers who remain
(`leave_selects_new_key`): there is no stretch after the removal in which the
newest usable key is one the removed member holds.

The floor and deletion are outside this model: `copies` keeps every copy ever
claimed, as a store snapshot does for slots a floor has passed (§16.1).
A slot's author and signature, and a write's validity, are checked elsewhere; a
slot here is whatever bytes a reader must open. Announcements and raises are
left out: any accepted store key seals them, as it does a copy, so a reader
needs nothing for them that a copy doesn't ask. Sealing keys are fresh by §12.3
but needn't be for these results.
-/
namespace CovenSequence.CopyOrder
open Keys

/-- An admin entry, as far as keys and readers go. Removing a device changes
neither: a member's device shares the member's keys (§12.2). -/
inductive Change (Aud Mem : Type) where
  /-- Slot 1: the first member, with its sealing key, and the first store key. -/
  | create (key : Key Aud) (hash sealing : Nat)
  /-- An admission adding member `m` with its sealing key, and the circle of
  `key`'s audience, which `m` alone reads (§13.2). -/
  | admit (m : Mem) (admin : Bool) (sealing : Nat) (key : Key Aud) (hash : Nat)
  | removeMember (m : Mem)
  /-- A member's own replacement of its keys, or an admin's (§12.3). -/
  | replaceKeys (m : Mem) (sealing : Nat)
  /-- An admission recovering member `m` with a new sealing key, and a new
  circle of `key`'s audience, which `m` alone reads (§13.2). -/
  | recover (m : Mem) (sealing : Nat) (key : Key Aud) (hash : Nat)
  | createCircle (key : Key Aud) (hash : Nat)
  | addToCircle (circle : Aud) (m : Mem)
  /-- Removing `m` from `circle` introduces the circle's replacement key in the
  same entry (§15.8); its copies go to the readers who remain. -/
  | leaveCircle (circle : Aud) (m : Mem) (key : Key Aud) (hash : Nat)
  | rotate (key : Key Aud) (hash : Nat)
  deriving DecidableEq

/-- What a reader must open at a slot. `outer` is the store key around its body,
named so because `seal` is taken by Lean. -/
inductive Slot (Aud Mem : Type) where
  | entry (author : Mem) (outer : Key Aud) (change : Change Aud Mem)
  | copy (outer : Key Aud) (route : Route Aud Mem)
  /-- `parts` are the keys of the write's parts, each with its own audience. -/
  | writes (outer : Key Aud) (parts : List (Key Aud))
  deriving DecidableEq

/-- Who reads each audience, with which sealing key. -/
structure Roster (Aud Mem : Type) where
  current : Current Aud Mem
  /-- Every id ever added: ids are never reused (§10.4). -/
  people : List Mem
  admins : List Mem

variable {Aud Mem : Type} [DecidableEq Aud] [DecidableEq Mem]

/-- The slot's event in `Keys`' projection. -/
def Slot.event : Slot Aud Mem → Event Aud Mem
  | .entry _ _ (.create key hash _) | .entry _ _ (.createCircle key hash)
  | .entry _ _ (.rotate key hash) | .entry _ _ (.leaveCircle _ _ key hash)
  | .entry _ _ (.admit _ _ _ key hash) | .entry _ _ (.recover _ _ key hash) =>
    .introduce key hash
  | .copy _ route => .copy route
  | _ => .other

/-- The keys a reader of each key's audience needs to open the slot. -/
def Slot.needs : Slot Aud Mem → List (Key Aud)
  | .entry _ outer _ | .copy outer _ => [outer]
  | .writes outer parts => outer :: parts

def Roster.seat (r : Roster Aud Mem) (a : Aud) (m : Mem) (value : Option Nat) : Roster Aud Mem :=
  { r with current := fun a' m' => if a' = a ∧ m' = m then value else r.current a' m' }

/-- An entry's effect on the roster. -/
def Roster.apply (store : Aud) (r : Roster Aud Mem) (author : Mem) : Change Aud Mem → Roster Aud Mem
  | .create _ _ c =>
    { r.seat store author (some c) with people := author :: r.people, admins := author :: r.admins }
  | .admit m admin c key _ =>
    { (r.seat store m (some c)).seat key.audience m (some c) with
      people := m :: r.people, admins := if admin then m :: r.admins else r.admins }
  | .removeMember m => { r with current := fun a x => if x = m then none else r.current a x }
  | .replaceKeys m c =>
    { r with
      current := fun a x => if x = m then (r.current a x).map (fun _ => c) else r.current a x }
  | .recover m c key _ =>
    ({ r with
      current := fun a x => if x = m then (r.current a x).map (fun _ => c) else r.current a x }
        : Roster Aud Mem).seat key.audience m (some c)
  | .createCircle key _ => r.seat key.audience author (r.current store author)
  | .addToCircle a m => r.seat a m (r.current store m)
  | .leaveCircle a m _ _ => r.seat a m none
  | .rotate _ _ => r

/-- A slot's effect on the roster: only entries change it. -/
def Roster.after (store : Aud) (r : Roster Aud Mem) : Slot Aud Mem → Roster Aud Mem
  | .entry author _ change => r.apply store author change
  | _ => r

/-- The roster through slot `n`. -/
def rosterAt (store : Aud) (seq : Nat → Slot Aud Mem) : Nat → Roster Aud Mem
  | 0 => ⟨fun _ _ => none, [], []⟩
  | n + 1 => (rosterAt store seq n).after store (seq (n + 1))

/-- `author` holds `key`: a copy of it reached their member. A device may also
hold a key its member has left the audience of. -/
def held (h : History Aud Mem) (author : Mem) (key : Key Aud) : Bool :=
  h.copies.any fun r => decide (r.key = key ∧ r.member = author)

/-- Rust's `required_copies`: the copies `author`'s device sends with the
entry, against the state just before its slot. Adding or recovering a member
sends only the keys the acting device holds (§12.1). -/
def required (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) (author : Mem) :
    Change Aud Mem → List (Route Aud Mem)
  | .admit m _ c key _ =>
    (h.introductions.filter fun i => decide (i.key.audience = store)).map (fun i => ⟨i.key, m, c⟩)
      ++ [⟨key, m, c⟩]
  | .replaceKeys m c =>
    (h.introductions.filter fun i => decide (i.key.audience = store) ||
      ((r.current i.key.audience m).isSome && held h author i.key)).map fun i => ⟨i.key, m, c⟩
  | .recover m c key _ =>
    (h.introductions.filter fun i => decide (i.key.audience = store) ||
      ((r.current i.key.audience m).isSome && held h author i.key)).map (fun i => ⟨i.key, m, c⟩)
      ++ [⟨key, m, c⟩]
  | .createCircle key _ =>
    match r.current store author with
    | some c => [⟨key, author, c⟩]
    | none => []
  | .addToCircle a m =>
    match r.current store m with
    | some c => (h.introductions.filter fun i => decide (i.key.audience = a)).map
        fun i => ⟨i.key, m, c⟩
    | none => []
  | .rotate key _ => routes r.current r.people key
  | .leaveCircle a m key _ => routes (r.seat a m none).current r.people key
  | _ => []

/-- A circle audience no one has used: no key was introduced for it and no
member reads it (§15.5). -/
def circleFree (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) (key : Key Aud) : Bool :=
  decide (key.audience ≠ store) &&
    h.introductions.all (fun i => decide (i.key.audience ≠ key.audience)) &&
    r.people.all (fun m => (r.current key.audience m).isNone)

/-- Who may make an entry (§10.1), as `checked_change` decides it. Creation is
slot 1's alone. -/
def allowed (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) (author : Mem) :
    Change Aud Mem → Bool
  | .create .. => false
  | .admit m _ _ key _ =>
    (r.current store author).isSome && decide (author ∈ r.admins) && decide (m ∉ r.people) &&
      circleFree store h r key
  | .removeMember m =>
    (r.current store author).isSome && decide (author ∈ r.admins) && (r.current store m).isSome
  | .replaceKeys m _ =>
    (r.current store author).isSome && (decide (author = m) || decide (author ∈ r.admins)) &&
      (r.current store m).isSome
  | .recover m _ key _ =>
    (r.current store author).isSome && decide (author ∈ r.admins) && (r.current store m).isSome &&
      circleFree store h r key
  | .createCircle key _ =>
    (r.current store author).isSome && decide (key.audience ≠ store) &&
      h.introductions.all (fun i => decide (i.key.audience ≠ key.audience)) &&
      r.people.all (fun m => (r.current key.audience m).isNone)
  | .addToCircle a m =>
    decide (a ≠ store) && (r.current a author).isSome && (r.current store m).isSome
  | .leaveCircle a m key _ =>
    decide (a ≠ store) && (r.current a author).isSome && decide (key.audience = a) &&
      (decide (m ≠ author) || r.people.all fun x => decide (x = author) || (r.current a x).isNone)
  | .rotate key _ => (r.current key.audience author).isSome

def introduced (h : History Aud Mem) (key : Key Aud) : Bool :=
  h.introductions.any fun i => decide (i.key = key)

/-- The copies an entry needs below its slot: an admission carries its own
(§13.2), so it needs none below. -/
def before (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) (author : Mem) :
    Change Aud Mem → List (Route Aud Mem)
  | .admit .. | .recover .. => []
  | change => required store h r author change

/-- The copies a slot carries: a key-copy slot's one, an admission's required
copies, one per key, all to the member it admits or recovers, and the store's
creation's, the creator's copy of the first store key (§10.3). -/
def carried (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) :
    Slot Aud Mem → List (Route Aud Mem)
  | .copy _ route => [route]
  | .entry author _ (.create key _ c) => [⟨key, author, c⟩]
  | .entry author _ (.admit m admin c key hash) =>
    required store h r author (.admit m admin c key hash)
  | .entry author _ (.recover m c key hash) => required store h r author (.recover m c key hash)
  | _ => []

/-- One slot's step of the key projection: introductions from `Keys`, and the
copies the slot carries. -/
def advance (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) (k : Nat)
    (x : Slot Aud Mem) : History Aud Mem :=
  ⟨(step h k x.event).introductions, carried store h r x ++ h.copies⟩

/-- The key projection through slot `n`. -/
def keysAt (store : Aud) (seq : Nat → Slot Aud Mem) : Nat → History Aud Mem
  | 0 => ⟨[], []⟩
  | n + 1 => advance store (keysAt store seq n) (rosterAt store seq n) (n + 1) (seq (n + 1))

/-- The copies slot `t` carries, in its place. -/
def carriedAt (store : Aud) (seq : Nat → Slot Aud Mem) (t : Nat) : List (Route Aud Mem) :=
  carried store (keysAt store seq (t - 1)) (rosterAt store seq (t - 1)) (seq t)

/-- A copy is in the projection through `n` exactly when a slot up to `n`
carries it. -/
theorem copy_carried (store : Aud) (seq : Nat → Slot Aud Mem) (route : Route Aud Mem) :
    ∀ n, route ∈ (keysAt store seq n).copies ↔ ∃ t, 0 < t ∧ t ≤ n ∧ route ∈ carriedAt store seq t
  | 0 => by simp [keysAt]; omega
  | n + 1 => by
    rw [keysAt]
    simp only [advance, List.mem_append, copy_carried store seq route n]
    constructor
    · rintro (now | ⟨t, h0, h1, ht⟩)
      · exact ⟨n + 1, by omega, Nat.le_refl _, by simpa [carriedAt] using now⟩
      · exact ⟨t, h0, by omega, ht⟩
    · rintro ⟨t, h0, h1, ht⟩
      by_cases last : t = n + 1
      · subst last; left; simpa [carriedAt] using ht
      · right; exact ⟨t, h0, by omega, ht⟩

/-- The projection's introductions are `Keys.historyAt`'s: the copies a slot
carries never change them. -/
theorem keysAt_introductions (store : Aud) (seq : Nat → Slot Aud Mem) :
    ∀ n, (keysAt store seq n).introductions =
      (historyAt (fun k => (seq k).event) n).introductions
  | 0 => rfl
  | n + 1 => by
    rw [keysAt, historyAt]
    simp only [advance]
    have ih := keysAt_introductions store seq n
    cases (seq (n + 1)).event <;> simp [step, ih]

/-- An introduction is in the projection through `n` exactly when its slot,
up to `n`, introduces it. -/
theorem introduction_at (store : Aud) (seq : Nat → Slot Aud Mem) (i : Introduction Aud) (n : Nat) :
    i ∈ (keysAt store seq n).introductions ↔
      0 < i.slot ∧ i.slot ≤ n ∧ (seq i.slot).event = .introduce i.key i.hash := by
  rw [keysAt_introductions]
  exact introduction_slot _ i n

/-- **An admission's slot carries its member's circle key copy.** The
introduction of the circle's key and the copy to the member's sealing key sit
in the one slot that admits or recovers the member, so the circle's first
reader has its copy from the slot that makes the circle (§13.2). -/
theorem admission_carries_own_copy (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem)
    (author m : Mem) (outer key : Key Aud) (admin : Bool) (c hash : Nat) :
    (⟨key, m, c⟩ : Route Aud Mem) ∈
        carried store h r (.entry author outer (.admit m admin c key hash)) ∧
      (⟨key, m, c⟩ : Route Aud Mem) ∈
        carried store h r (.entry author outer (.recover m c key hash)) ∧
      (Slot.entry author outer (.admit m admin c key hash)).event = .introduce key hash ∧
      (Slot.entry author outer (.recover m c key hash)).event = .introduce key hash := by
  simp [carried, required, Slot.event]

/-- A slot follows the rules at its position. Metadata may use any accepted
store key, exposed or not (§12.2), and the new key may encrypt its own
introduction (§12.1). A write's slot needs a usable store key on each attempt,
but its parts keep the keys of their first attempt (§6.3), so those are
introduced and may be exposed by now. -/
def legal (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) : Slot Aud Mem → Bool
  | .entry author outer change =>
    allowed store h r author change &&
      (before store h r author change).all (fun route => decide (route ∈ h.copies)) &&
      decide (outer.audience = store) &&
      (introduced h outer || match change with
        | .rotate key _ => decide (key = outer)
        | _ => false)
  | .copy outer _ => decide (outer.audience = store) && introduced h outer
  | .writes outer parts =>
    decide (outer.audience = store) && introduced h outer && !exposed h r.current outer &&
      parts.all (introduced h)

/-- Every slot through `n` followed the rules: slot 1 is the store's creation,
which carries its creator's copy of the first store key, and each later slot
was legal at its position. -/
def Follows (store : Aud) (seq : Nat → Slot Aud Mem) (n : Nat) : Prop :=
  (∃ x key hash c, key.audience = store ∧ seq 1 = .entry x key (.create key hash c)) ∧
  ∀ k, 2 ≤ k → k ≤ n →
    legal store (keysAt store seq (k - 1)) (rosterAt store seq (k - 1)) (seq k) = true

/-- **The claim.** For every slot `s` after creation, every key `s` needs, and
every member who reads that key's audience just before `s`: a copy of the key
sealed to the member's current sealing key sits in a slot below `s`. -/
def CopyPrecedesNeed (store : Aud) (seq : Nat → Slot Aud Mem) (n : Nat) : Prop :=
  ∀ s, 2 ≤ s → s ≤ n → ∀ key ∈ (seq s).needs, ∀ m c,
    (rosterAt store seq (s - 1)).current key.audience m = some c →
    ∃ t, 0 < t ∧ t < s ∧ ⟨key, m, c⟩ ∈ carriedAt store seq t

/-! ## What the roster keeps -/

/-- Only added ids read anything, and a member reads every audience with the
sealing key it reads the store with. -/
structure RosterOk (store : Aud) (r : Roster Aud Mem) : Prop where
  people : ∀ a m c, r.current a m = some c → m ∈ r.people
  sealing : ∀ a m c, r.current a m = some c → r.current store m = some c

theorem seat_current (r : Roster Aud Mem) (a a' : Aud) (m m' : Mem) (v : Option Nat) :
    (r.seat a m v).current a' m' = if a' = a ∧ m' = m then v else r.current a' m' := rfl

theorem apply_ok (store : Aud) {r : Roster Aud Mem} (ok : RosterOk store r) (h : History Aud Mem)
    (author : Mem) (change : Change Aud Mem) (yes : allowed store h r author change = true) :
    RosterOk store (r.apply store author change) := by
  cases change with
  | create key hash c => simp [allowed] at yes
  | admit m admin c key hash =>
    simp only [allowed, circleFree, Bool.and_eq_true, decide_eq_true_eq, ne_eq] at yes
    obtain ⟨⟨_, fresh⟩, ⟨⟨notStore, _⟩, _⟩⟩ := yes
    constructor
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · rename_i same; exact List.mem_cons.mpr (Or.inl same.2)
      · split at hv
        · rename_i same; exact List.mem_cons.mpr (Or.inl same.2)
        · exact List.mem_cons_of_mem _ (ok.people a x v hv)
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      have no : ¬ (store = key.audience ∧ x = m) := fun e => notStore e.1.symm
      simp only [no, ite_false]
      split at hv
      · rename_i same
        cases hv
        simp [same.2]
      · split at hv
        · rename_i same
          cases hv
          simp [same.2]
        · rename_i n1 n2
          have different : x ≠ m := fun e => fresh (e ▸ ok.people a x v hv)
          simp [different, ok.sealing a x v hv]
  | removeMember m =>
    constructor
    · intro a x v hv
      simp only [Roster.apply] at hv ⊢
      split at hv
      · cases hv
      · exact ok.people a x v hv
    · intro a x v hv
      simp only [Roster.apply] at hv ⊢
      split at hv
      · cases hv
      · rename_i different; simp [different, ok.sealing a x v hv]
  | recover m c key hash =>
    simp only [allowed, circleFree, Bool.and_eq_true, decide_eq_true_eq, ne_eq] at yes
    obtain ⟨⟨_, known⟩, ⟨⟨notStore, _⟩, _⟩⟩ := yes
    obtain ⟨w₀, hw₀⟩ := Option.isSome_iff_exists.mp known
    constructor
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · rename_i same; rw [same.2]; exact ok.people _ _ _ hw₀
      · split at hv
        · rename_i same
          obtain ⟨w, hw, _⟩ := Option.map_eq_some_iff.mp hv
          exact ok.people a x w hw
        · exact ok.people a x v hv
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      have no : ¬ (store = key.audience ∧ x = m) := fun e => notStore e.1.symm
      simp only [no, ite_false]
      split at hv
      · rename_i same
        cases hv
        obtain ⟨_, hx⟩ := same
        subst hx
        simp [hw₀]
      · split at hv
        · rename_i same
          subst same
          obtain ⟨w, hw, eq⟩ := Option.map_eq_some_iff.mp hv
          simp [ok.sealing a x w hw, eq]
        · rename_i different; simp [different, ok.sealing a x v hv]
  | replaceKeys m c =>
    constructor
    · intro a x v hv
      simp only [Roster.apply] at hv ⊢
      split at hv
      · rename_i same
        obtain ⟨w, hw, _⟩ := Option.map_eq_some_iff.mp hv
        exact ok.people a x w hw
      · exact ok.people a x v hv
    · intro a x v hv
      simp only [Roster.apply] at hv ⊢
      split at hv
      · rename_i same
        subst same
        obtain ⟨w, hw, eq⟩ := Option.map_eq_some_iff.mp hv
        simp [ok.sealing a x w hw, eq]
      · rename_i different; simp [different, ok.sealing a x v hv]
  | createCircle key hash =>
    simp only [allowed, Bool.and_eq_true, decide_eq_true_eq, Option.isSome_iff_exists,
      ne_eq] at yes
    constructor
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · rename_i same
        rw [same.2]
        exact ok.people _ _ _ hv
      · exact ok.people a x v hv
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      have different := yes.1.1.2
      have notStore : ¬ (store = key.audience ∧ x = author) := fun e => different e.1.symm
      simp only [notStore, ite_false]
      split at hv
      · rename_i same
        rw [same.2]
        exact ok.sealing _ _ _ hv
      · exact ok.sealing a x v hv
  | addToCircle c m =>
    simp only [allowed, Bool.and_eq_true, decide_eq_true_eq, ne_eq] at yes
    constructor
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · rename_i same
        rw [same.2]
        exact ok.people _ _ _ hv
      · exact ok.people a x v hv
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      have notStore : ¬ (store = c ∧ x = m) := fun e => yes.1.1 e.1.symm
      simp only [notStore, ite_false]
      split at hv
      · rename_i same
        rw [same.2]
        exact hv
      · exact ok.sealing a x v hv
  | leaveCircle c m key hash =>
    simp only [allowed, Bool.and_eq_true, decide_eq_true_eq, ne_eq] at yes
    constructor
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · cases hv
      · exact ok.people a x v hv
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      have notStore : ¬ (store = c ∧ x = m) := fun e => yes.1.1.1 e.1.symm
      simp only [notStore, ite_false]
      split at hv
      · cases hv
      · exact ok.sealing a x v hv
  | rotate key hash => exact ok

theorem legal_entry {store : Aud} {h : History Aud Mem} {r : Roster Aud Mem} {author : Mem}
    {outer : Key Aud} {change : Change Aud Mem}
    (yes : legal store h r (.entry author outer change) = true) :
    allowed store h r author change = true ∧
      (∀ route ∈ before store h r author change, route ∈ h.copies) ∧
      outer.audience = store ∧
      (introduced h outer = true ∨ ∃ hash, change = .rotate outer hash) := by
  simp only [legal, Bool.and_eq_true, Bool.or_eq_true, List.all_eq_true, decide_eq_true_eq] at yes
  obtain ⟨⟨⟨allow, copies⟩, audience⟩, seals⟩ := yes
  refine ⟨allow, copies, audience, ?_⟩
  rcases seals with old | own
  · exact Or.inl old
  · right
    cases change with
    | rotate key hash =>
      simp only [decide_eq_true_eq] at own
      exact ⟨hash, by rw [own]⟩
    | _ => simp at own

/-- The roster every slot through `n` of a followed history leaves. -/
theorem roster_ok {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat} (hf : Follows store seq n) :
    RosterOk store (rosterAt store seq n) := by
  obtain ⟨⟨x, key, hash, c, _, s1⟩, later⟩ := hf
  have first : RosterOk store (rosterAt store seq 1) := by
    constructor <;> intro a m v hv <;>
      simp only [rosterAt, s1, Roster.after, Roster.apply, seat_current] at hv ⊢
    · split at hv
      · rename_i same; simp [same.2]
      · cases hv
    · split at hv
      · rename_i same; simp [same.2, hv]
      · cases hv
  have up : ∀ j, j + 1 ≤ n → RosterOk store (rosterAt store seq (j + 1)) := by
    intro j
    induction j with
    | zero => exact fun _ => first
    | succ j ih =>
      intro below
      have ok := ih (by omega)
      have yes := later (j + 2) (by omega) below
      simp only [show j + 2 - 1 = j + 1 by omega] at yes
      cases hs : seq (j + 2) with
      | entry author outer change =>
        rw [hs] at yes
        have := apply_ok store ok _ author change (legal_entry yes).1
        simpa [rosterAt, hs, Roster.after] using this
      | copy outer route => simpa [rosterAt, hs, Roster.after] using ok
      | writes outer parts => simpa [rosterAt, hs, Roster.after] using ok
  by_cases one : 1 ≤ n
  · have := up (n - 1) (by omega)
    rwa [show n - 1 + 1 = n by omega] at this
  · have : n = 0 := by omega
    subst this
    exact ⟨fun _ _ _ h => by simp [rosterAt] at h, fun _ _ _ h => by simp [rosterAt] at h⟩

/-! ## Every reader of an audience has a copy of each of its keys -/

/-- Each key introduced for `a` has a copy to each member reading `a`, sealed to
the key the member reads it with. -/
def Complete (h : History Aud Mem) (r : Roster Aud Mem) (a : Aud) : Prop :=
  ∀ i ∈ h.introductions, i.key.audience = a → ∀ m c, r.current a m = some c →
    (⟨i.key, m, c⟩ : Route Aud Mem) ∈ h.copies

/-- A replacement's or recovery's author reads `a` whenever its target does. -/
def Inside (r : Roster Aud Mem) (a : Aud) : Slot Aud Mem → Prop
  | .entry author _ (.replaceKeys m _) | .entry author _ (.recover m _ _ _) =>
    (r.current a m).isSome = true → (r.current a author).isSome = true
  | _ => True

/-- What a replacement or a recovery of `m` with sealing key `c` leaves
complete: every key `a`'s readers hold, `m` holds sealed to `c` when the
author held it and the copies are in `copies`. -/
theorem keys_follow_replacement {a : Aud} {h : History Aud Mem} {r : Roster Aud Mem}
    (done : Complete h r a) {author m : Mem} {c : Nat}
    (inside : (r.current a m).isSome = true → (r.current a author).isSome = true)
    (copies : List (Route Aud Mem))
    (sent : ∀ i ∈ h.introductions, i.key.audience = a → (r.current a m).isSome = true →
      held h author i.key = true → (⟨i.key, m, c⟩ : Route Aud Mem) ∈ copies) :
    ∀ i ∈ h.introductions, i.key.audience = a → ∀ x v,
      (if x = m then (r.current a x).map (fun _ => c) else r.current a x) = some v →
      (⟨i.key, x, v⟩ : Route Aud Mem) ∈ copies ++ h.copies := by
  intro i hi ha x v hv
  split at hv
  · rename_i same
    subst same
    obtain ⟨w, hw, eq⟩ := Option.map_eq_some_iff.mp hv
    subst eq
    have reads : (r.current a x).isSome = true := by simp [hw]
    obtain ⟨y, hy⟩ := Option.isSome_iff_exists.mp (inside reads)
    have reached := done i hi ha author y hy
    have holds : held h author i.key = true := by
      simp only [held, List.any_eq_true, decide_eq_true_eq]
      exact ⟨_, reached, rfl, rfl⟩
    exact List.mem_append_left _ (sent i hi ha reads holds)
  · exact List.mem_append_right _ (done i hi ha x v hv)

theorem step_complete {store a : Aud} {h : History Aud Mem} {r : Roster Aud Mem}
    (ok : RosterOk store r) (done : Complete h r a) (k : Nat) (x : Slot Aud Mem)
    (yes : legal store h r x = true) (inside : Inside r a x) :
    Complete (advance store h r k x) (r.after store x) a := by
  cases x with
  | copy outer route =>
    intro i hi ha m c hc
    simp only [advance, Slot.event, step] at hi ⊢
    exact List.mem_append_right _ (done i hi ha m c hc)
  | writes outer parts =>
    intro i hi ha m c hc
    simp only [advance, Slot.event, step, carried, List.nil_append] at hi ⊢
    exact done i hi ha m c hc
  | entry author outer change =>
    obtain ⟨allow, copies, -, -⟩ := legal_entry yes
    cases change with
    | create key hash c => simp [allowed] at allow
    | admit m admin c key hash =>
      simp only [allowed, circleFree, Bool.and_eq_true, decide_eq_true_eq, ne_eq,
        List.all_eq_true, Option.isNone_iff_eq_none] at allow
      obtain ⟨⟨_, _⟩, ⟨⟨notStore, unused⟩, empty⟩⟩ := allow
      intro i hi ha x v hv
      simp only [advance, Slot.event, step, carried] at hi ⊢
      simp only [Roster.after, Roster.apply, seat_current] at hv
      rcases List.mem_cons.mp hi with rfl | old
      · simp only at ha
        split at hv
        · rename_i same
          cases hv
          obtain ⟨_, hx⟩ := same
          subst hx
          exact List.mem_append_left _ (by simp [required])
        · split at hv
          · rename_i _ same
            exact absurd (ha.trans same.1) notStore
          · have hx := ok.people _ x v hv
            rw [← ha, empty x hx] at hv
            cases hv
      · split at hv
        · rename_i same
          exact absurd (ha.trans same.1) (unused i old)
        · split at hv
          · rename_i _ same
            cases hv
            obtain ⟨hs, hx⟩ := same
            subst hx
            apply List.mem_append_left
            simp only [required]
            exact List.mem_append_left _
              (List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨old, by simp [ha, hs]⟩, rfl⟩)
          · exact List.mem_append_right _ (done i old ha x v hv)
    | removeMember m =>
      intro i hi ha x v hv
      simp only [advance, Slot.event, step, carried, List.nil_append] at hi ⊢
      simp only [Roster.after, Roster.apply] at hv
      split at hv
      · cases hv
      · exact done i hi ha x v hv
    | replaceKeys m c =>
      intro i hi ha x v hv
      simp only [advance, Slot.event, step] at hi ⊢
      simp only [Roster.after, Roster.apply] at hv
      have := keys_follow_replacement done inside h.copies
        (fun i hi ha reads holds => by
          apply copies
          simp only [before, required]
          refine List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨hi, ?_⟩, rfl⟩
          simp [ha, reads, holds]) i hi ha x v hv
      simp only [carried, List.nil_append]
      simpa only [List.mem_append, or_self] using this
    | recover m c key hash =>
      simp only [allowed, circleFree, Bool.and_eq_true, decide_eq_true_eq, ne_eq,
        List.all_eq_true, Option.isNone_iff_eq_none] at allow
      obtain ⟨⟨_, _⟩, ⟨⟨notStore, unused⟩, empty⟩⟩ := allow
      intro i hi ha x v hv
      simp only [advance, Slot.event, step, carried] at hi ⊢
      simp only [Roster.after, Roster.apply, seat_current] at hv
      rcases List.mem_cons.mp hi with rfl | old
      · simp only at ha
        split at hv
        · rename_i same
          cases hv
          obtain ⟨_, hx⟩ := same
          subst hx
          exact List.mem_append_left _ (by simp [required])
        · split at hv
          · obtain ⟨w, hw, -⟩ := Option.map_eq_some_iff.mp hv
            have hx := ok.people _ x w hw
            rw [← ha, empty x hx] at hw
            cases hw
          · have hx := ok.people _ x v hv
            rw [← ha, empty x hx] at hv
            cases hv
      · split at hv
        · rename_i same
          exact absurd (ha.trans same.1) (unused i old)
        · exact keys_follow_replacement done inside _ (fun i hi ha reads holds => by
            simp only [required]
            exact List.mem_append_left _
              (List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨hi, by simp [ha, reads, holds]⟩, rfl⟩))
            i old ha x v hv
    | createCircle key hash =>
      simp only [allowed, Bool.and_eq_true, decide_eq_true_eq, ne_eq, List.all_eq_true,
        Option.isSome_iff_exists, Option.isNone_iff_eq_none] at allow
      obtain ⟨⟨⟨⟨y, hy⟩, _⟩, fresh⟩, empty⟩ := allow
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply, seat_current] at hv
      simp only [advance, Slot.event, step, carried, List.nil_append, List.mem_cons] at hi ⊢
      rcases hi with rfl | old
      · simp only at ha
        subst ha
        split at hv
        · rename_i same
          obtain ⟨_, hx⟩ := same
          subst hx
          rw [hy] at hv
          cases hv
          have : (⟨key, x, y⟩ : Route Aud Mem) ∈ before store h r x (.createCircle key hash) := by
            simp [before, required, hy]
          exact copies _ this
        · rename_i different
          have hm := ok.people _ x v hv
          rw [empty x hm] at hv
          cases hv
      · split at hv
        · rename_i same
          exact absurd (ha ▸ same.1) (fresh i old)
        · exact done i old ha x v hv
    | addToCircle circle m =>
      intro i hi ha x v hv
      simp only [advance, Slot.event, step, carried, List.nil_append] at hi ⊢
      simp only [Roster.after, Roster.apply, seat_current] at hv
      split at hv
      · rename_i same
        obtain ⟨hs, hx⟩ := same
        subst hx
        apply copies
        simp only [before, required, hv]
        exact List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨hi, by simp [ha, hs]⟩, rfl⟩
      · exact done i hi ha x v hv
    | leaveCircle circle m key hash =>
      intro i hi ha x v hv
      simp only [advance, Slot.event, step, carried, List.nil_append, List.mem_cons] at hi ⊢
      simp only [Roster.after, Roster.apply, seat_current] at hv
      rcases hi with rfl | old
      · simp only at ha
        subst ha
        split at hv
        · cases hv
        · rename_i different
          exact copies _ (by
            simp only [before, required]
            exact routes_complete _ _ _ x v (ok.people _ x v hv) (by
              simp only [Roster.seat, different, ite_false]; exact hv))
      · split at hv
        · cases hv
        · exact done i old ha x v hv
    | rotate key hash =>
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply] at hv
      simp only [advance, Slot.event, step, carried, List.nil_append, List.mem_cons] at hi ⊢
      rcases hi with rfl | old
      · simp only at ha
        subst ha
        exact copies _ (by
          simpa [before, required] using
            routes_complete r.current r.people key x v (ok.people _ x v hv) hv)
      · exact done i old ha x v hv

/-- **Every reader has its copies.** In a followed history where every
replacement or recovery of a reader of `a` comes from another of them, at every
prefix from creation on, each key introduced for `a` has a copy to each
reader's current sealing key. -/
theorem audience_complete {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n) (a : Aud)
    (inside : ∀ k, 2 ≤ k → k ≤ n → Inside (rosterAt store seq (k - 1)) a (seq k)) :
    ∀ k, 1 ≤ k → k ≤ n → Complete (keysAt store seq k) (rosterAt store seq k) a := by
  have up : ∀ j, j + 1 ≤ n →
      Complete (keysAt store seq (j + 1)) (rosterAt store seq (j + 1)) a := by
    intro j
    induction j with
    | zero =>
      intro _
      obtain ⟨⟨x, key, hash, c, _, s1⟩, _⟩ := hf
      intro i hi ha m v hv
      simp only [keysAt, advance, s1, Slot.event, step, carried, List.mem_cons,
        List.not_mem_nil, or_false, List.append_nil] at hi ⊢
      subst hi
      simp only [rosterAt, s1, Roster.after, Roster.apply, seat_current] at hv
      split at hv
      · rename_i same
        cases hv
        simp [same.2]
      · cases hv
    | succ j ih =>
      intro below
      have done := ih (by omega)
      have yes := hf.2 (j + 2) (by omega) below
      have side := inside (j + 2) (by omega) below
      simp only [show j + 2 - 1 = j + 1 by omega] at yes side
      have prior : Follows store seq (j + 1) := ⟨hf.1, fun i h1 h2 => hf.2 i h1 (by omega)⟩
      exact step_complete (roster_ok prior) done (j + 2) (seq (j + 2)) yes side
  intro k one below
  have := up (k - 1) (by omega)
  rwa [show k - 1 + 1 = k by omega] at this

theorem need_met {store a : Aud} {seq : Nat → Slot Aud Mem} {n : Nat} (hf : Follows store seq n)
    (inside : ∀ k, 2 ≤ k → k ≤ n → Inside (rosterAt store seq (k - 1)) a (seq k))
    {s : Nat} (two : 2 ≤ s) (below : s ≤ n) {key : Key Aud} (need : key ∈ (seq s).needs)
    (audience : key.audience = a) {m : Mem} {c : Nat}
    (reads : (rosterAt store seq (s - 1)).current key.audience m = some c) :
    ∃ t, 0 < t ∧ t < s ∧ ⟨key, m, c⟩ ∈ carriedAt store seq t := by
  have done := audience_complete hf a inside (s - 1) (by omega) (by omega)
  have prior : Follows store seq (s - 1) := ⟨hf.1, fun j h1 h2 => hf.2 j h1 (by omega)⟩
  have ok := roster_ok prior
  have yes := hf.2 s two below
  have sent : (⟨key, m, c⟩ : Route Aud Mem) ∈ (keysAt store seq (s - 1)).copies := by
    have fromIntroduced : introduced (keysAt store seq (s - 1)) key = true →
        (⟨key, m, c⟩ : Route Aud Mem) ∈ (keysAt store seq (s - 1)).copies := by
      intro known
      simp only [introduced, List.any_eq_true, decide_eq_true_eq] at known
      obtain ⟨i, hi, hk⟩ := known
      have := done i hi (by rw [hk]; exact audience) m c (audience ▸ reads)
      rwa [hk] at this
    cases hs : seq s with
    | entry author outer change =>
      rw [hs] at yes need
      simp only [Slot.needs, List.mem_singleton] at need
      subst need
      obtain ⟨_, copies, _, seals⟩ := legal_entry yes
      rcases seals with old | ⟨hash, rfl⟩
      · exact fromIntroduced old
      · exact copies _ (by
          simpa [before, required] using
            routes_complete _ _ _ m c (ok.people _ m c reads) reads)
    | copy outer route =>
      rw [hs] at yes need
      simp only [Slot.needs, List.mem_singleton] at need
      subst need
      simp only [legal, Bool.and_eq_true] at yes
      exact fromIntroduced yes.2
    | writes outer parts =>
      rw [hs] at yes need
      simp only [legal, Bool.and_eq_true, List.all_eq_true] at yes
      simp only [Slot.needs, List.mem_cons] at need
      rcases need with rfl | part
      · exact fromIntroduced yes.1.1.2
      · exact fromIntroduced (yes.2 key part)
  obtain ⟨t, h0, h1, ht⟩ := (copy_carried store seq _ (s - 1)).mp sent
  exact ⟨t, h0, by omega, ht⟩

/-- **A store key always has its copy first.** In every followed history, for
every slot after creation and every store key it needs, each member has a copy
sealed to its current sealing key below the slot: whatever raced, a member
admitted mid-rotation or a replacement among a rotation's copies. An admitted
member's copies sit in its admission's own slot, below every slot it reads. -/
theorem store_copy_precedes_need {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n) {s : Nat} (two : 2 ≤ s) (below : s ≤ n) {key : Key Aud}
    (need : key ∈ (seq s).needs) (audience : key.audience = store) {m : Mem} {c : Nat}
    (reads : (rosterAt store seq (s - 1)).current store m = some c) :
    ∃ t, 0 < t ∧ t < s ∧ ⟨key, m, c⟩ ∈ carriedAt store seq t := by
  refine need_met hf (fun k h1 h2 => ?_) two below need audience (audience ▸ reads)
  have yes := hf.2 k h1 h2
  cases hs : seq k with
  | entry author outer change =>
    cases change with
    | replaceKeys m c =>
      rw [hs] at yes
      have allow := (legal_entry yes).1
      simp only [allowed, Bool.and_eq_true] at allow
      exact fun _ => allow.1.1
    | recover m c key hash =>
      rw [hs] at yes
      have allow := (legal_entry yes).1
      simp only [allowed, Bool.and_eq_true] at allow
      exact fun _ => allow.1.1.1
    | _ => trivial
  | _ => trivial

/-- **A circle's key has its copy first unless an outsider replaced or
recovered a member.** When every replacement or recovery of a reader of an
audience comes from another reader of it, the claim holds in full. -/
theorem copy_precedes_need_inside {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n)
    (inside : ∀ a k, 2 ≤ k → k ≤ n → Inside (rosterAt store seq (k - 1)) a (seq k)) :
    CopyPrecedesNeed store seq n := fun _ two below _ need _ _ reads =>
  need_met hf (inside _) two below need rfl reads

/-! ## A stuck reader scans ahead

Where `CopyPrecedesNeed` fails, a reader stops at a slot whose key has no copy
below it. Reading in order, it would never reach the copy. So the stuck reader
scans the slots after the stop for a copy of the missing key sealed to its pair,
and a copy found there ends the wait. Two facts make the scan enough. Every key
a slot needs is introduced by that slot (`need_introduced`), so the copy's
material is checked against an applied commitment and is accepted at once, not
held as tentative (`Keys.openCopy`). And a circle member's pass sends every copy
its audience still needs (`owed_complete`); once those copies are in the
sequence, a scan through them finds a copy of every key the stuck slot needs
(`stuck_scan_finds_copy`). -/

/-- An introduction applied by slot `j` is applied by every later slot. -/
theorem introduced_mono {store : Aud} {seq : Nat → Slot Aud Mem} {j k : Nat} (le : j ≤ k)
    {key : Key Aud} (known : introduced (keysAt store seq j) key = true) :
    introduced (keysAt store seq k) key = true := by
  simp only [introduced, List.any_eq_true, decide_eq_true_eq] at known ⊢
  obtain ⟨i, hi, hk⟩ := known
  have at_j := (introduction_at store seq i j).mp hi
  exact ⟨i, (introduction_at store seq i k).mpr ⟨at_j.1, by omega, at_j.2.2⟩, hk⟩

/-- **A slot's keys are introduced by that slot.** In a followed history,
every key a slot after creation needs has an applied introduction at or below
the slot: its store key and its parts' keys below it, or the slot's own
rotation. -/
theorem need_introduced {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n) {s : Nat} (two : 2 ≤ s) (below : s ≤ n) {key : Key Aud}
    (need : key ∈ (seq s).needs) : introduced (keysAt store seq s) key = true := by
  have yes := hf.2 s two below
  have up := fun known =>
    introduced_mono (store := store) (seq := seq) (show s - 1 ≤ s by omega) (key := key) known
  cases hs : seq s with
  | entry author outer change =>
    rw [hs] at yes need
    simp only [Slot.needs, List.mem_singleton] at need
    subst need
    obtain ⟨_, _, _, seals⟩ := legal_entry yes
    rcases seals with old | ⟨hash, rfl⟩
    · exact up old
    · simp only [introduced, List.any_eq_true, decide_eq_true_eq]
      exact ⟨⟨key, hash, s⟩, (introduction_at store seq _ s).mpr ⟨show 0 < s by omega,
        Nat.le_refl s, by simp [hs, Slot.event]⟩, rfl⟩
  | copy outer route =>
    rw [hs] at yes need
    simp only [Slot.needs, List.mem_singleton] at need
    subst need
    simp only [legal, Bool.and_eq_true] at yes
    exact up yes.2
  | writes outer parts =>
    rw [hs] at yes need
    simp only [legal, Bool.and_eq_true, List.all_eq_true] at yes
    simp only [Slot.needs, List.mem_cons] at need
    rcases need with rfl | part
    · exact up yes.1.1.2
    · exact up (yes.2 key part)

/-- **A stuck reader's scan finds its copy.** In a followed history through
`n`, take any slot `s` after creation, any key it needs, and a member reading
that key's audience with pair `c` at `n`. Once the audience is complete at `n`,
some slot through `n` carries a copy of the key sealed to `c`, a key-copy slot
or an admission. The copy may sit above `s`
(`CopyOrderExamples.copy_precedes_need_fails`), so only a reader that scans
past its stop finds it. -/
theorem stuck_scan_finds_copy {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n) {s : Nat} (two : 2 ≤ s) (below : s ≤ n) {key : Key Aud}
    (need : key ∈ (seq s).needs) {m : Mem} {c : Nat}
    (reads : (rosterAt store seq n).current key.audience m = some c)
    (owed : Complete (keysAt store seq n) (rosterAt store seq n) key.audience) :
    ∃ t, 0 < t ∧ t ≤ n ∧ ⟨key, m, c⟩ ∈ carriedAt store seq t := by
  have known := introduced_mono below (need_introduced hf two below need)
  simp only [introduced, List.any_eq_true, decide_eq_true_eq] at known
  obtain ⟨i, hi, hk⟩ := known
  have sent := owed i hi (by rw [hk]) m c reads
  rw [hk] at sent
  exact (copy_carried store seq _ n).mp sent

/-- **A circle member's pass completes its audience.** Publishing the copies
`Keys.needed` lists for an audience, as a pass of one of its members does
(§12.1), gives every key introduced for it a copy to each current reader's
current sealing key. -/
theorem owed_complete {store a : Aud} {r : Roster Aud Mem} (ok : RosterOk store r)
    (h : History Aud Mem) (through : Nat) :
    Complete (publish h through ((needed h r.current r.people a).map Event.copy)) r a := by
  intro i hi ha m c reads
  rw [publish_copies] at hi
  exact needed_complete h through r.current r.people a i hi ha m c (ok.people a m c reads) reads

/-! ## Leaving a circle replaces its key (§15.8)

The entry that removes `m` from a circle introduces the circle's replacement
key, and the copies it requires sit below it, to the readers who remain. So at
the slot that removes `m`, the newest usable key of the circle is the new one,
and none of its copies reached `m`: nothing written under the circle's key from
then on is readable to `m`. There is no stretch after the removal in which the
newest usable key is one `m` holds. -/

/-- **A removed circle member reads nothing written after the removal.** Take a
followed history through `n` and a slot `k` whose entry removes `m` from circle
`a` and introduces `key`. Suppose `key` is new, every copy of it below `k` went
to a reader who remains, and a member `writer` who remains holds its material.
Then `m` no longer reads `a`; the key is the first attempt's choice for `writer`
(the newest usable key, not one exposed to anyone who left); and no copy of it
is routed to `m`, whatever sealing keys `m` retains. -/
theorem leave_selects_new_key {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n) {k : Nat} (two : 2 ≤ k) (below : k ≤ n)
    {author : Mem} {outer : Key Aud} {a : Aud} {m : Mem} {key : Key Aud} {hash : Nat}
    (hs : seq k = .entry author outer (.leaveCircle a m key hash))
    (freshIntro : ∀ i ∈ (keysAt store seq (k - 1)).introductions, i.key ≠ key)
    (remaining : ∀ r ∈ (keysAt store seq (k - 1)).copies, r.key = key →
      (rosterAt store seq k).current a r.member = some r.credential)
    (held : Custody Aud) (generated : held key = some hash) (writer : Mem)
    (reads : (rosterAt store seq k).current a writer ≠ none) :
    (rosterAt store seq k).current a m = none ∧
    firstKey (keysAt store seq k) held (rosterAt store seq k).current writer a =
      some ⟨key, hash, k⟩ ∧
    ∀ cs, ¬ RoutedTo (keysAt store seq k) m cs key := by
  obtain ⟨j, rfl⟩ : ∃ j, k = j + 1 := ⟨k - 1, by omega⟩
  have yes := hf.2 (j + 1) two below
  rw [hs] at yes
  obtain ⟨allow, -, -, -⟩ := legal_entry yes
  simp only [allowed, Bool.and_eq_true, decide_eq_true_eq, ne_eq] at allow
  have inA : key.audience = a := allow.1.2
  have gone : (rosterAt store seq (j + 1)).current a m = none := by
    simp [rosterAt, hs, Roster.after, Roster.apply, seat_current]
  have intros : (keysAt store seq (j + 1)).introductions =
      ⟨key, hash, j + 1⟩ :: (keysAt store seq j).introductions := by
    simp [keysAt, advance, hs, Slot.event, step]
  have copies : (keysAt store seq (j + 1)).copies = (keysAt store seq j).copies := by
    simp [keysAt, advance, hs, carried]
  have unexposed : exposed (keysAt store seq (j + 1)) (rosterAt store seq (j + 1)).current key =
      false := by
    apply Bool.eq_false_iff.mpr
    intro bad
    obtain ⟨r, hr, hk, off⟩ := (exposed_iff _ _ _).mp bad
    rw [copies] at hr
    exact off (inA ▸ remaining r (by simpa using hr) hk)
  have noCopy : ∀ cs, ¬ RoutedTo (keysAt store seq (j + 1)) m cs key := by
    rintro cs ⟨r, hr, hk, hm, -⟩
    rw [copies] at hr
    have := remaining r hr hk
    rw [hm, gone] at this
    cases this
  refine ⟨gone, ?_, noCopy⟩
  have usableNew : usable (keysAt store seq (j + 1)) held (rosterAt store seq (j + 1)).current
      writer key = true := by
    have accept : accepted (keysAt store seq (j + 1)) held key = true := by
      have := introduction_accepts ⟨(keysAt store seq j).introductions, (keysAt store seq j).copies⟩
        held key hash (j + 1) freshIntro generated
      simpa [accepted, intros, step] using this
    simp only [usable, inA, accept, unexposed, Bool.not_false, Bool.and_true,
      Option.isSome_iff_ne_none]
    exact reads
  unfold firstKey
  rw [intros]
  simp only [List.filter_cons, inA, decide_true, Bool.true_and]
  simp only [usableNew, ↓reduceIte]
  apply newest_head
  intro i hi
  have := ((introduction_at store seq i j).mp (List.mem_filter.mp hi).1).2.1
  dsimp
  omega

end CovenSequence.CopyOrder
