import CovenSequence.Keys

/-!
# Copies before need (§12.1, §12.3)

A device reads slots in order, and a slot it can't open stops it there (§5.4):
a key arriving in a later slot can't get it past the stop (`keyless_stops`). So
a member following an audience reads a slot of that audience only if a copy of
the slot's key, sealed to the member's current sealing key, sits below it. This
module states that claim for every history whose devices follow §12.1's order,
as `CopyPrecedesNeed`, and shows where it holds and where it doesn't.

The model is `Keys`' projection (`keysAt`) beside a roster of who reads each
audience with which sealing key (`Keys.Current`), as `effects.rs` and
`circle_effects.rs` keep it. An entry's `required` copies are what Rust's
`required_copies` gives it. `legal` says a slot followed the rules at its
position: an entry's required copies sit below it and every key that seals the
slot is introduced. The claim loop makes that the state of the slot just before
the entry, whatever raced it: an operation reads every slot before each claim,
works out its copies again, and lands only at the slot after what it read
(`operation_outcome`, with validation asking for those copies). A slot here is
what landed; `Follows` ranges over every interleaving of its devices, so two
admins racing, a member joining mid-rotation and a replacement landing among a
rotation's copies are all histories of `Follows`, not cases of their own
(`CopyOrderExamples.races_follow`). Store creation is the exception §12.1 names: slot 1 sets up
the first member and key, whose copy follows at slot 2.

`CopyPrecedesNeed` fails. An admin outside a circle who replaces a member's
keys can send only the keys it holds (`required`, §12.1), so a circle's key has
no copy to the member's new sealing key until a circle member's next pass sends
it, and a write already sealed with that key can land first
(`CopyOrderExamples.copy_precedes_need_fails`). That is the only way it fails: a
store key always has its copy first (`store_copy_precedes_need`), and so does a
circle's key when every replacement of one of the circle's members comes from
another of them (`copy_precedes_need_inside`).

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
  | addMember (m : Mem) (admin : Bool) (sealing : Nat)
  | removeMember (m : Mem)
  | replaceKeys (m : Mem) (sealing : Nat)
  | createCircle (key : Key Aud) (hash : Nat)
  | addToCircle (circle : Aud) (m : Mem)
  | leaveCircle (circle : Aud) (m : Mem)
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
  | .entry _ _ (.rotate key hash) => .introduce key hash
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
  | .addMember m admin c =>
    { r.seat store m (some c) with
      people := m :: r.people, admins := if admin then m :: r.admins else r.admins }
  | .removeMember m => { r with current := fun a x => if x = m then none else r.current a x }
  | .replaceKeys m c =>
    { r with current := fun a x => if x = m then (r.current a x).map (fun _ => c) else r.current a x }
  | .createCircle key _ => r.seat key.audience author (r.current store author)
  | .addToCircle a m => r.seat a m (r.current store m)
  | .leaveCircle a m => r.seat a m none
  | .rotate _ _ => r

/-- The key projection through slot `n`. -/
def keysAt (seq : Nat → Slot Aud Mem) (n : Nat) : History Aud Mem :=
  historyAt (fun k => (seq k).event) n

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

/-- Rust's `required_copies`: the copies `author`'s device claims before the
entry, against the state just before its slot. Adding or recovering a member
sends only the keys the acting device holds (§12.1). -/
def required (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) (author : Mem) :
    Change Aud Mem → List (Route Aud Mem)
  | .addMember m _ c =>
    (h.introductions.filter fun i => decide (i.key.audience = store)).map fun i => ⟨i.key, m, c⟩
  | .replaceKeys m c =>
    (h.introductions.filter fun i => decide (i.key.audience = store) ||
      ((r.current i.key.audience m).isSome && held h author i.key)).map fun i => ⟨i.key, m, c⟩
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
  | _ => []

/-- Who may make an entry (§10.1), as `checked_change` decides it. Creation is
slot 1's alone. -/
def allowed (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) (author : Mem) :
    Change Aud Mem → Bool
  | .create .. => false
  | .addMember m _ _ =>
    (r.current store author).isSome && decide (author ∈ r.admins) && decide (m ∉ r.people)
  | .removeMember m =>
    (r.current store author).isSome && decide (author ∈ r.admins) && (r.current store m).isSome
  | .replaceKeys m _ =>
    (r.current store author).isSome && (decide (author = m) || decide (author ∈ r.admins)) &&
      (r.current store m).isSome
  | .createCircle key _ =>
    (r.current store author).isSome && decide (key.audience ≠ store) &&
      h.introductions.all (fun i => decide (i.key.audience ≠ key.audience)) &&
      r.people.all (fun m => (r.current key.audience m).isNone)
  | .addToCircle a m =>
    decide (a ≠ store) && (r.current a author).isSome && (r.current store m).isSome
  | .leaveCircle a _ => decide (a ≠ store) && (r.current a author).isSome
  | .rotate key _ => (r.current key.audience author).isSome

def introduced (h : History Aud Mem) (key : Key Aud) : Bool :=
  h.introductions.any fun i => decide (i.key = key)

/-- A slot follows the rules at its position. Metadata may use any accepted
store key, exposed or not (§12.2), and the new key may encrypt its own
introduction (§12.1). A write's slot needs a usable store key on each attempt,
but its parts keep the keys of their first attempt (§6.3), so those are
introduced and may be exposed by now. -/
def legal (store : Aud) (h : History Aud Mem) (r : Roster Aud Mem) : Slot Aud Mem → Bool
  | .entry author outer change =>
    allowed store h r author change &&
      (required store h r author change).all (fun route => decide (route ∈ h.copies)) &&
      decide (outer.audience = store) &&
      (introduced h outer || match change with
        | .rotate key _ => decide (key = outer)
        | _ => false)
  | .copy outer _ => decide (outer.audience = store) && introduced h outer
  | .writes outer parts =>
    decide (outer.audience = store) && introduced h outer && !exposed h r.current outer &&
      parts.all (introduced h)

/-- Every slot through `n` followed the rules: slots 1 and 2 are the store's
creation, and each later slot was legal at its position. -/
def Follows (store : Aud) (seq : Nat → Slot Aud Mem) (n : Nat) : Prop :=
  (∃ x key hash c, key.audience = store ∧ seq 1 = .entry x key (.create key hash c) ∧
    seq 2 = .copy key ⟨key, x, c⟩) ∧
  ∀ k, 3 ≤ k → k ≤ n → legal store (keysAt seq (k - 1)) (rosterAt store seq (k - 1)) (seq k) = true

/-- **The claim.** For every slot `s` after creation, every key `s` needs, and
every member who reads that key's audience just before `s`: a copy of the key
sealed to the member's current sealing key sits in a slot below `s`. -/
def CopyPrecedesNeed (store : Aud) (seq : Nat → Slot Aud Mem) (n : Nat) : Prop :=
  ∀ s, 3 ≤ s → s ≤ n → ∀ key ∈ (seq s).needs, ∀ m c,
    (rosterAt store seq (s - 1)).current key.audience m = some c →
    ∃ t, 0 < t ∧ t < s ∧ (seq t).event = .copy ⟨key, m, c⟩

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
  | addMember m admin c =>
    simp only [allowed, Bool.and_eq_true, decide_eq_true_eq] at yes
    constructor
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · rename_i same; exact List.mem_cons.mpr (Or.inl same.2)
      · exact List.mem_cons_of_mem _ (ok.people a x v hv)
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · rename_i same; simp [same.2, hv]
      · by_cases fresh : x = m
        · subst fresh
          exact absurd (ok.people a x v hv) yes.2
        · simp [fresh, ok.sealing a x v hv]
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
  | leaveCircle c m =>
    simp only [allowed, Bool.and_eq_true, decide_eq_true_eq, ne_eq] at yes
    constructor
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      split at hv
      · cases hv
      · exact ok.people a x v hv
    · intro a x v hv
      simp only [Roster.apply, seat_current] at hv ⊢
      have notStore : ¬ (store = c ∧ x = m) := fun e => yes.1 e.1.symm
      simp only [notStore, ite_false]
      split at hv
      · cases hv
      · exact ok.sealing a x v hv
  | rotate key hash => exact ok

theorem legal_entry {store : Aud} {h : History Aud Mem} {r : Roster Aud Mem} {author : Mem}
    {outer : Key Aud} {change : Change Aud Mem}
    (yes : legal store h r (.entry author outer change) = true) :
    allowed store h r author change = true ∧
      (∀ route ∈ required store h r author change, route ∈ h.copies) ∧
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
  obtain ⟨⟨x, key, hash, c, _, s1, s2⟩, later⟩ := hf
  have first : RosterOk store (rosterAt store seq 2) := by
    constructor <;> intro a m v hv <;>
      simp only [rosterAt, s1, s2, Roster.after, Roster.apply, seat_current] at hv ⊢
    · split at hv
      · rename_i same; simp [same.2]
      · cases hv
    · split at hv
      · rename_i same; simp [same.2, hv]
      · cases hv
  have up : ∀ j, j + 2 ≤ n → RosterOk store (rosterAt store seq (j + 2)) := by
    intro j
    induction j with
    | zero => exact fun _ => first
    | succ j ih =>
      intro below
      have ok := ih (by omega)
      have yes := later (j + 3) (by omega) below
      simp only [show j + 3 - 1 = j + 2 by omega] at yes
      cases hs : seq (j + 3) with
      | entry author outer change =>
        rw [hs] at yes
        have := apply_ok store ok _ author change (legal_entry yes).1
        simpa [rosterAt, hs, Roster.after] using this
      | copy outer route => simpa [rosterAt, hs, Roster.after] using ok
      | writes outer parts => simpa [rosterAt, hs, Roster.after] using ok
  by_cases two : 2 ≤ n
  · have := up (n - 2) (by omega)
    rwa [show n - 2 + 2 = n by omega] at this
  · have : n = 0 ∨ n = 1 := by omega
    rcases this with rfl | rfl
    · exact ⟨fun _ _ _ h => by simp [rosterAt] at h, fun _ _ _ h => by simp [rosterAt] at h⟩
    · constructor <;> intro a m v hv <;>
        simp only [rosterAt, s1, Roster.after, Roster.apply, seat_current] at hv ⊢
      · split at hv
        · rename_i same; simp [same.2]
        · cases hv
      · split at hv
        · rename_i same; simp [same.2, hv]
        · cases hv

/-! ## Every reader of an audience has a copy of each of its keys -/

/-- Each key introduced for `a` has a copy to each member reading `a`, sealed to
the key the member reads it with. -/
def Complete (h : History Aud Mem) (r : Roster Aud Mem) (a : Aud) : Prop :=
  ∀ i ∈ h.introductions, i.key.audience = a → ∀ m c, r.current a m = some c →
    (⟨i.key, m, c⟩ : Route Aud Mem) ∈ h.copies

/-- A replacement's author reads `a` whenever its target does. -/
def Inside (r : Roster Aud Mem) (a : Aud) : Slot Aud Mem → Prop
  | .entry author _ (.replaceKeys m _) =>
    (r.current a m).isSome = true → (r.current a author).isSome = true
  | _ => True

theorem step_complete {store a : Aud} {h : History Aud Mem} {r : Roster Aud Mem}
    (ok : RosterOk store r) (done : Complete h r a) (k : Nat) (x : Slot Aud Mem)
    (yes : legal store h r x = true) (inside : Inside r a x) :
    Complete (step h k x.event) (r.after store x) a := by
  cases x with
  | copy outer route =>
    intro i hi ha m c hc
    exact List.mem_cons_of_mem _ (done i hi ha m c hc)
  | writes outer parts => exact done
  | entry author outer change =>
    obtain ⟨allow, copies, -, -⟩ := legal_entry yes
    cases change with
    | create key hash c => simp [allowed] at allow
    | addMember m admin c =>
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply, seat_current] at hv
      split at hv
      · rename_i same
        obtain ⟨hs, hx⟩ := same
        cases hv
        subst hx
        apply copies
        exact List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨hi, by simp [ha, hs]⟩, rfl⟩
      · exact done i hi ha x v hv
    | removeMember m =>
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply] at hv
      split at hv
      · cases hv
      · exact done i hi ha x v hv
    | replaceKeys m c =>
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply] at hv
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
        apply copies
        refine List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨hi, ?_⟩, rfl⟩
        simp [ha, reads, holds]
      · exact done i hi ha x v hv
    | createCircle key hash =>
      simp only [allowed, Bool.and_eq_true, decide_eq_true_eq, ne_eq, List.all_eq_true,
        Option.isSome_iff_exists, Option.isNone_iff_eq_none] at allow
      obtain ⟨⟨⟨⟨y, hy⟩, _⟩, fresh⟩, empty⟩ := allow
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply, seat_current] at hv
      simp only [Slot.event, step, List.mem_cons] at hi
      rcases hi with rfl | old
      · simp only at ha
        subst ha
        split at hv
        · rename_i same
          obtain ⟨_, hx⟩ := same
          subst hx
          rw [hy] at hv
          cases hv
          have : (⟨key, x, y⟩ : Route Aud Mem) ∈ required store h r x (.createCircle key hash) := by
            simp [required, hy]
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
      simp only [Roster.after, Roster.apply, seat_current] at hv
      split at hv
      · rename_i same
        obtain ⟨hs, hx⟩ := same
        subst hx
        apply copies
        simp only [required, hv]
        exact List.mem_map.mpr ⟨i, List.mem_filter.mpr ⟨hi, by simp [ha, hs]⟩, rfl⟩
      · exact done i hi ha x v hv
    | leaveCircle circle m =>
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply, seat_current] at hv
      split at hv
      · cases hv
      · exact done i hi ha x v hv
    | rotate key hash =>
      intro i hi ha x v hv
      simp only [Roster.after, Roster.apply] at hv
      simp only [Slot.event, step, List.mem_cons] at hi
      rcases hi with rfl | old
      · simp only at ha
        subst ha
        exact copies _ (routes_complete r.current r.people key x v (ok.people _ x v hv) hv)
      · exact done i old ha x v hv

/-- **Every reader has its copies.** In a followed history where every
replacement of a reader of `a` comes from another of them, at every prefix from
creation's copy on, each key introduced for `a` has a copy to each reader's
current sealing key. -/
theorem audience_complete {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n) (a : Aud)
    (inside : ∀ k, 3 ≤ k → k ≤ n → Inside (rosterAt store seq (k - 1)) a (seq k)) :
    ∀ k, 2 ≤ k → k ≤ n → Complete (keysAt seq k) (rosterAt store seq k) a := by
  have up : ∀ j, j + 2 ≤ n → Complete (keysAt seq (j + 2)) (rosterAt store seq (j + 2)) a := by
    intro j
    induction j with
    | zero =>
      intro _
      obtain ⟨⟨x, key, hash, c, _, s1, s2⟩, _⟩ := hf
      intro i hi ha m v hv
      simp only [keysAt, historyAt, s1, s2, Slot.event, step, List.mem_cons, List.not_mem_nil,
        or_false] at hi ⊢
      subst hi
      simp only [rosterAt, s1, s2, Roster.after, Roster.apply, seat_current] at hv
      split at hv
      · rename_i same
        cases hv
        simp [same.2]
      · cases hv
    | succ j ih =>
      intro below
      have done := ih (by omega)
      have yes := hf.2 (j + 3) (by omega) below
      have side := inside (j + 3) (by omega) below
      simp only [show j + 3 - 1 = j + 2 by omega] at yes side
      have prior : Follows store seq (j + 2) := ⟨hf.1, fun i h1 h2 => hf.2 i h1 (by omega)⟩
      exact step_complete (roster_ok prior) done (j + 3) (seq (j + 3)) yes side
  intro k two below
  have := up (k - 2) (by omega)
  rwa [show k - 2 + 2 = k by omega] at this

theorem need_met {store a : Aud} {seq : Nat → Slot Aud Mem} {n : Nat} (hf : Follows store seq n)
    (inside : ∀ k, 3 ≤ k → k ≤ n → Inside (rosterAt store seq (k - 1)) a (seq k))
    {s : Nat} (three : 3 ≤ s) (below : s ≤ n) {key : Key Aud} (need : key ∈ (seq s).needs)
    (audience : key.audience = a) {m : Mem} {c : Nat}
    (reads : (rosterAt store seq (s - 1)).current key.audience m = some c) :
    ∃ t, 0 < t ∧ t < s ∧ (seq t).event = .copy ⟨key, m, c⟩ := by
  have done := audience_complete hf a inside (s - 1) (by omega) (by omega)
  have prior : Follows store seq (s - 1) := ⟨hf.1, fun j h1 h2 => hf.2 j h1 (by omega)⟩
  have ok := roster_ok prior
  have yes := hf.2 s three below
  have sent : (⟨key, m, c⟩ : Route Aud Mem) ∈ (keysAt seq (s - 1)).copies := by
    have fromIntroduced : introduced (keysAt seq (s - 1)) key = true →
        (⟨key, m, c⟩ : Route Aud Mem) ∈ (keysAt seq (s - 1)).copies := by
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
      · exact copies _ (routes_complete _ _ _ m c (ok.people _ m c reads) reads)
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
  obtain ⟨t, h0, h1, ht⟩ := (copy_slot _ _ (s - 1)).mp sent
  exact ⟨t, h0, by omega, ht⟩

/-- **A store key always has its copy first.** In every followed history, for
every slot after creation and every store key it needs, each member has a copy
sealed to its current sealing key below the slot: whatever raced, a member
joining mid-rotation or a replacement among a rotation's copies. -/
theorem store_copy_precedes_need {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n) {s : Nat} (three : 3 ≤ s) (below : s ≤ n) {key : Key Aud}
    (need : key ∈ (seq s).needs) (audience : key.audience = store) {m : Mem} {c : Nat}
    (reads : (rosterAt store seq (s - 1)).current store m = some c) :
    ∃ t, 0 < t ∧ t < s ∧ (seq t).event = .copy ⟨key, m, c⟩ := by
  refine need_met hf (fun k h1 h2 => ?_) three below need audience (audience ▸ reads)
  have yes := hf.2 k h1 h2
  cases hs : seq k with
  | entry author outer change =>
    cases change with
    | replaceKeys m c =>
      rw [hs] at yes
      have allow := (legal_entry yes).1
      simp only [allowed, Bool.and_eq_true] at allow
      exact fun _ => allow.1.1
    | _ => trivial
  | _ => trivial

/-- **A circle's key has its copy first unless an outsider replaced a member's
keys.** When every replacement of a reader of an audience comes from another
reader of it, the claim holds in full. -/
theorem copy_precedes_need_inside {store : Aud} {seq : Nat → Slot Aud Mem} {n : Nat}
    (hf : Follows store seq n)
    (inside : ∀ a k, 3 ≤ k → k ≤ n → Inside (rosterAt store seq (k - 1)) a (seq k)) :
    CopyPrecedesNeed store seq n := fun _ three below _ need _ _ reads =>
  need_met hf (inside _) three below need rfl reads

end CovenSequence.CopyOrder
