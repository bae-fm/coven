/-!
# A circle's name, members and deletion

The admin entries that run a circle (§15.5, §15.8, §15.9), and the store's
removal of a member, decided as Rust's `checked_change` decides them
(`crates/coven-sync/src/effects.rs`, with `circle_effects.rs`). The state is what those checks read and
change: each member id's standing in the store, each circle's name and members,
and the slot that deleted it. `Model`'s ledger keeps circles abstract (`aux`);
this model is concrete and stands alone, and the Rust test
`circle_lifecycle_matches_lean` runs the same histories through both.

- A circle's name is its creation's, then each valid rename's in slot order
  (`name_is_latest_rename`).
- A rename of a deleted circle, or an addition to it or a removal from it,
  fails its check because its target is gone; so does making a circle under
  its id (`deleted_target_refused`).
- Every live circle has a member (`live_has_member`): a circle left empty is
  deleted at the slot that left it so, whether its last member left it
  (`leaving_deletes`) or was removed from the store (`member_removal_deletes`).
  The state before that slot decides it, and no other slot records a deletion
  (`deleted_at_slot`).
- A deleted circle never changes again: no later entry adds a member to it,
  renames it or takes its deletion back (`deleted_stays`).

- An admission makes its member's circle in the slot that admits them
  (§13.2): `admit` for a new person, `recover` for a recovered member, whose
  old circle no device can read. A circle id already used, live or deleted,
  refuses the entry (`used_circle_refuses_admission`); the entry's repeat
  changes nothing once the circle holds the member (`admission_repeat_unchanged`).

The store's admin authority over a removal is `Authority`'s. That the author's
device is active and acts for the author is §10's check, made with the
author's standing before these rules; the model takes the standing alone.
-/
namespace CovenSequence.CircleLifecycle

/-- A member id's place in the store: never added, a member, or removed for
good (§14.1). -/
inductive Standing where
  | absent
  | active
  | removed
  deriving DecidableEq, Repr

structure Circle (M : Type) where
  name : String
  members : List M

structure State (M C : Type) where
  standing : M → Standing
  circles : C → Option (Circle M)
  /-- The slot that deleted the circle; `none` while it is live. -/
  deleted : C → Option Nat

/-- An admin entry that runs a circle. -/
inductive Change (M C : Type) where
  | create (circle : C) (name : String)
  | rename (circle : C) (name : String)
  | delete (circle : C)
  | add (circle : C) (member : M)
  | remove (circle : C) (member : M)

inductive Entry (M C : Type) where
  | circle (author : M) (change : Change M C)
  /-- The store's removal of a member, which an admin makes (`Authority`). -/
  | removeMember (member : M)
  /-- An admin's admission of a new person, who comes with a circle named
  `name` that they alone are in. -/
  | admit (author : M) (member : M) (circle : C) (name : String)
  /-- An admin's recovery of a member who lost their keys, who comes with a
  new circle named `name` that they alone are in. -/
  | recover (author : M) (member : M) (circle : C) (name : String)

/-- Why a check refuses an entry, as `OperationRejection` names it:
`NotAllowed`, or `TargetGone` naming the circle or the member. -/
inductive Refusal where
  | notAllowed
  | goneCircle
  | goneMember
  deriving DecidableEq, Repr

/-- What a check decides: a refusal, a change already in place, or the state
the slot commits. -/
inductive Outcome (M C : Type) where
  | refused (why : Refusal)
  | unchanged
  | applied (next : State M C)

variable {M C : Type} [DecidableEq M] [DecidableEq C]

def State.set (s : State M C) (c : C) (x : Circle M) : State M C :=
  { s with circles := fun c' => if c' = c then some x else s.circles c' }

def emptied : Option (Circle M) → Bool
  | some x => x.members.isEmpty
  | none => false

/-- `m` leaves each live circle `which` names at slot `k`; a circle it leaves
empty is deleted at `k`. -/
def State.leave (s : State M C) (k : Nat) (m : M) (which : C → Bool) : State M C :=
  let left := fun c =>
    if which c && (s.deleted c).isNone then
      (s.circles c).map fun x => { x with members := x.members.erase m }
    else s.circles c
  { s with
    circles := left
    deleted := fun c =>
      if which c && (s.deleted c).isNone && emptied (left c) then some k else s.deleted c }

/-- A change to an existing circle: a gone circle refuses it whoever asks, and
only the circle's members may make it. -/
def managed (s : State M C) (c : C) (author : M) (act : Circle M → Outcome M C) :
    Outcome M C :=
  match s.circles c with
  | none => .refused .goneCircle
  | some x =>
    if (s.deleted c).isSome then .refused .goneCircle
    else if author ∈ x.members then act x else .refused .notAllowed

/-- Whether `author` may take `m` out of a circle with members `x`. The entry
introduces the circle's new key, which its author makes: a member removing
themselves while others stay would know the key meant to shut them out
(§15.8). -/
def mayLeave (author m : M) (x : Circle M) : Bool :=
  !(decide (m = author) && decide (1 < x.members.length))

def changeEffect (s : State M C) (k : Nat) (author : M) (change : Change M C) : Outcome M C :=
  if s.standing author ≠ .active then .refused .notAllowed else
  match change with
  | .create c name =>
    if (s.circles c).isSome then .refused .goneCircle else .applied (s.set c ⟨name, [author]⟩)
  | .rename c name => managed s c author fun x =>
    if x.name = name then .unchanged else .applied (s.set c { x with name })
  | .delete c =>
    if (s.deleted c).isSome then .unchanged else managed s c author fun x =>
      .applied { s.set c { x with members := [] } with
        deleted := fun c' => if c' = c then some k else s.deleted c' }
  | .add c m => managed s c author fun x =>
    if s.standing m ≠ .active then .refused .goneMember
    else if m ∈ x.members then .unchanged
    else .applied (s.set c { x with members := x.members ++ [m] })
  | .remove c m => managed s c author fun x =>
    if m ∈ x.members then
      if mayLeave author m x then .applied (s.leave k m (· == c)) else .refused .notAllowed
    else .unchanged

def removalEffect (s : State M C) (k : Nat) (m : M) : Outcome M C :=
  match s.standing m with
  | .absent => .refused .goneMember
  | .removed => .unchanged
  | .active => .applied { s.leave k m (fun _ => true) with
      standing := fun m' => if m' = m then .removed else s.standing m' }

/-- A live circle `c` with exactly this name and `m` among its members: the
circle an earlier application of the same admission made. -/
def madeFor (s : State M C) (c : C) (m : M) (name : String) : Bool :=
  match s.circles c with
  | some x => (s.deleted c).isNone && decide (m ∈ x.members) && decide (x.name = name)
  | none => false

/-- An admission or a recovery. A new person must be unknown and the circle's
id unused; a recovered member must be active and the circle's id unused. The
same entry again, once the circle holds the member under that name, changes
nothing. -/
def admissionEffect (s : State M C) (author m : M) (c : C) (name : String)
    (recovery : Bool) : Outcome M C :=
  if s.standing author ≠ .active then .refused .notAllowed
  else if madeFor s c m name && decide (s.standing m = .active) then .unchanged
  else if s.standing m ≠ (if recovery then .active else .absent) then .refused .goneMember
  else if (s.circles c).isSome then .refused .goneCircle
  else .applied { s.set c ⟨name, [m]⟩ with
    standing := fun m' => if m' = m then .active else s.standing m' }

def effect (s : State M C) (k : Nat) : Entry M C → Outcome M C
  | .circle author change => changeEffect s k author change
  | .removeMember m => removalEffect s k m
  | .admit author m c name => admissionEffect s author m c name false
  | .recover author m c name => admissionEffect s author m c name true

/-- The state a slot commits: a refused entry, or one already in place, leaves
it as it was. -/
def Outcome.after (s : State M C) : Outcome M C → State M C
  | .applied next => next
  | _ => s

/-- Whether the check accepts the entry: an entry already in place passes it. -/
def Outcome.accepted : Outcome M C → Bool
  | .refused _ => false
  | _ => true

def valid (s : State M C) (k : Nat) (e : Entry M C) : Bool :=
  (effect s k e).accepted

/-- Slots holding neither a circle entry nor a member's removal are `none`. -/
def step (s : State M C) (k : Nat) : Option (Entry M C) → State M C
  | none => s
  | some e => (effect s k e).after s

def stateAt (init : State M C) (seq : Nat → Option (Entry M C)) : Nat → State M C
  | 0 => init
  | k + 1 => step (stateAt init seq k) (k + 1) (seq (k + 1))

/-! ## Names -/

/-- The name an entry gives circle `c` if its check accepts it. -/
def naming (c : C) : Entry M C → Option String
  | .circle _ (.create c' n) | .circle _ (.rename c' n)
  | .admit _ _ c' n | .recover _ _ c' n => if c' = c then some n else none
  | _ => none

/-- The name after an entry that gives circle `c` the name `named`, if any:
that name if the check accepts the entry, else `old`. -/
def renamed (named : Option String) (accepted : Bool) (old : Option String) : Option String :=
  match named with
  | some n => if accepted then some n else old
  | none => old

/-- The creation name followed by the valid renames, in slot order. -/
def nameAt (init : State M C) (seq : Nat → Option (Entry M C)) (c : C) : Nat → Option String
  | 0 => (init.circles c).map (·.name)
  | k + 1 =>
    match seq (k + 1) with
    | some e => renamed (naming c e) (valid (stateAt init seq k) (k + 1) e) (nameAt init seq c k)
    | none => nameAt init seq c k

omit [DecidableEq M] in
theorem set_name (s : State M C) (c c' : C) (x : Circle M) :
    ((s.set c' x).circles c).map (·.name) =
      if c = c' then some x.name else (s.circles c).map (·.name) := by
  unfold State.set; split <;> simp_all

omit [DecidableEq C] in
theorem leave_name (s : State M C) (k : Nat) (m : M) (which : C → Bool) (c : C) :
    ((s.leave k m which).circles c).map (·.name) = (s.circles c).map (·.name) := by
  unfold State.leave
  simp only
  split
  · cases s.circles c <;> simp
  · rfl

/-- What an outcome does to circle `c`'s name, given the name the entry gives
it if accepted: a refusal leaves it, an entry already in place already gave it,
and an applied entry gives it or leaves it. -/
def Names (s : State M C) (c : C) (named : Option String) : Outcome M C → Prop
  | .refused _ => True
  | .unchanged => ∀ n, named = some n → (s.circles c).map (·.name) = some n
  | .applied next => (next.circles c).map (·.name) = named.or ((s.circles c).map (·.name))

theorem admission_names (s : State M C) (a m : M) (c c' : C) (n : String) (rec : Bool) :
    Names s c (if c' = c then some n else none) (admissionEffect s a m c' n rec) := by
  unfold admissionEffect
  by_cases active : s.standing a ≠ .active
  · rw [if_pos active]; trivial
  · rw [if_neg active]
    by_cases made : (madeFor s c' m n && decide (s.standing m = .active)) = true
    · rw [if_pos made]
      intro n' named
      by_cases same : c' = c
      · subst c'
        simp only [ite_true, Option.some.injEq] at named
        subst n'
        cases h : s.circles c with
        | none => simp [madeFor, h] at made
        | some x =>
          simp [madeFor, h] at made
          simp [made.1.2]
      · simp [same] at named
    · rw [if_neg made]
      by_cases standing : s.standing m ≠ (if rec = true then .active else .absent)
      · rw [if_pos standing]; trivial
      · rw [if_neg standing]
        by_cases used : (s.circles c').isSome = true
        · rw [if_pos used]; trivial
        · rw [if_neg used]
          by_cases same : c' = c
          · subst c'; simp [Names, State.set]
          · simp [Names, State.set, same, Ne.symm same]

theorem effect_names (s : State M C) (k : Nat) (e : Entry M C) (c : C) :
    Names s c (naming c e) (effect s k e) := by
  cases e with
  | removeMember m =>
    simp only [naming, effect, removalEffect]
    split <;> simp [Names, leave_name]
  | admit a m c' n => exact admission_names s a m c c' n false
  | recover a m c' n => exact admission_names s a m c c' n true
  | circle a change =>
    by_cases active : s.standing a = .active
    · cases change with
      | create c' n =>
        by_cases made : (s.circles c').isSome
        · simp [effect, changeEffect, active, made, Names]
        · by_cases same : c' = c
          · subst c'; simp [naming, effect, changeEffect, active, made, Names, set_name]
          · simp [naming, effect, changeEffect, active, made, Names, set_name, same, Ne.symm same]
      | rename c' n =>
        cases h : s.circles c' with
        | none => simp [effect, changeEffect, managed, active, h, Names]
        | some x =>
          by_cases gone : (s.deleted c').isSome
          · simp [effect, changeEffect, managed, active, h, gone, Names]
          · by_cases member : a ∈ x.members
            · by_cases same : c' = c
              · subst c'
                by_cases named : x.name = n
                · simp [naming, effect, changeEffect, managed, active, h, gone, member, named,
                    Names]
                · simp [naming, effect, changeEffect, managed, active, h, gone, member, named,
                    Names, set_name]
              · by_cases named : x.name = n
                · simp [naming, effect, changeEffect, managed, active, h, gone, member, same,
                    named, Names]
                · simp [naming, effect, changeEffect, managed, active, h, gone, member, same,
                    named, Ne.symm same, Names, set_name]
            · simp [effect, changeEffect, managed, active, h, gone, member, Names]
      | delete c' =>
        by_cases gone : (s.deleted c').isSome
        · simp [naming, effect, changeEffect, active, gone, Names]
        · cases h : s.circles c' with
          | none => simp [effect, changeEffect, managed, active, h, gone, Names]
          | some x =>
            by_cases member : a ∈ x.members
            · by_cases same : c' = c
              · subst c'; simp [naming, effect, changeEffect, managed, active, h, gone, member,
                  Names, set_name]
              · simp [naming, effect, changeEffect, managed, active, h, gone, member, Names,
                  set_name, Ne.symm same]
            · simp [effect, changeEffect, managed, active, h, gone, member, Names]
      | add c' m =>
        cases h : s.circles c' with
        | none => simp [effect, changeEffect, managed, active, h, Names]
        | some x =>
          by_cases gone : (s.deleted c').isSome
          · simp [effect, changeEffect, managed, active, h, gone, Names]
          · by_cases member : a ∈ x.members
            · by_cases joins : s.standing m = .active ∧ m ∉ x.members
              · by_cases same : c' = c
                · subst c'; simp [naming, effect, changeEffect, managed, active, h, gone, member,
                    joins, Names, set_name]
                · simp [naming, effect, changeEffect, managed, active, h, gone, member, joins,
                    Names, set_name, Ne.symm same]
              · by_cases target : s.standing m = .active
                · have held : m ∈ x.members := by simpa [target] using joins
                  simp [naming, effect, changeEffect, managed, active, h, gone, member, target,
                    held, Names]
                · simp [effect, changeEffect, managed, active, h, gone, member, target,
                    Names]
            · simp [effect, changeEffect, managed, active, h, gone, member, Names]
      | remove c' m =>
        cases h : s.circles c' with
        | none => simp [effect, changeEffect, managed, active, h, Names]
        | some x =>
          by_cases gone : (s.deleted c').isSome
          · simp [effect, changeEffect, managed, active, h, gone, Names]
          · by_cases member : a ∈ x.members
            · by_cases leaves : m ∈ x.members
              · by_cases may : mayLeave a m x = true
                · simp [naming, effect, changeEffect, managed, active, h, gone, member, leaves,
                    may, Names, leave_name]
                · simp [naming, effect, changeEffect, managed, active, h, gone, member, leaves,
                    may, Names]
              · simp [naming, effect, changeEffect, managed, active, h, gone, member, leaves,
                  Names]
            · simp [effect, changeEffect, managed, active, h, gone, member, Names]
    · cases change <;> simp [effect, changeEffect, active, Names]

omit [DecidableEq M] [DecidableEq C] in
theorem names_after (s : State M C) (c : C) (named : Option String) (o : Outcome M C)
    (names : Names s c named o) :
    ((o.after s).circles c).map (·.name) =
      renamed named o.accepted ((s.circles c).map (·.name)) := by
  cases named <;> cases o <;> simp only [Names] at names <;>
    simp_all [renamed, Outcome.after, Outcome.accepted]

theorem step_name (s : State M C) (k : Nat) (e : Entry M C) (c : C) :
    ((step s k (some e)).circles c).map (·.name) =
      renamed (naming c e) (valid s k e) ((s.circles c).map (·.name)) :=
  names_after s c (naming c e) (effect s k e) (effect_names s k e c)

/-- **A circle's name is its creation's, then each valid rename's, in slot
order** (§15.5). A refused rename, by someone outside the circle or after its
deletion, leaves the name as it was. -/
theorem name_is_latest_rename (init : State M C) (seq : Nat → Option (Entry M C)) (c : C) :
    ∀ k, ((stateAt init seq k).circles c).map (·.name) = nameAt init seq c k
  | 0 => rfl
  | k + 1 => by
    have prior := name_is_latest_rename init seq c k
    simp only [stateAt, nameAt]
    cases seq (k + 1) with
    | none => exact prior
    | some e => rw [step_name, prior]

/-! ## Deletion -/

/-- What one slot can do to one circle's record and deletion slot: nothing,
make it, rename a live circle, add a member to it, delete it at the slot, or
take a member out of it, deleting it at the slot if that leaves it empty. -/
inductive Move (k : Nat) :
    Option (Circle M) × Option Nat → Option (Circle M) × Option Nat → Prop
  | stay (p : Option (Circle M) × Option Nat) : Move k p p
  | make (x : Circle M) (d : Option Nat) (member : x.members ≠ []) : Move k (none, d) (some x, d)
  | rename (x : Circle M) (n : String) : Move k (some x, none) (some { x with name := n }, none)
  | add (x : Circle M) (m : M) :
      Move k (some x, none) (some { x with members := x.members ++ [m] }, none)
  | delete (x : Circle M) : Move k (some x, none) (some { x with members := [] }, some k)
  | leave (x : Circle M) (m : M) :
      Move k (some x, none) (some { x with members := x.members.erase m },
        if (x.members.erase m).isEmpty then some k else none)

/-- One circle's record and deletion slot agree: a deleted circle was made and
has no members, and a live circle has one. -/
def Kept : Option (Circle M) × Option Nat → Prop
  | (some x, some _) => x.members = []
  | (some x, none) => x.members ≠ []
  | (none, some _) => False
  | (none, none) => True

/-- What every state the entries reach keeps, circle by circle. -/
def Wf (s : State M C) : Prop := ∀ c, Kept (s.circles c, s.deleted c)

omit [DecidableEq C] in
theorem leave_deleted (s : State M C) (k : Nat) (m : M) (which : C → Bool) (c : C) :
    (s.leave k m which).deleted c =
      if which c && (s.deleted c).isNone && emptied ((s.circles c).map fun x =>
        { x with members := x.members.erase m }) then some k else s.deleted c := by
  unfold State.leave
  simp only
  by_cases picked : which c = true
  · by_cases live : (s.deleted c).isNone = true <;> simp [picked, live]
  · simp [picked]

omit [DecidableEq C] in
theorem leave_moves (s : State M C) (k : Nat) (m : M) (which : C → Bool) (c : C) :
    Move k (s.circles c, s.deleted c)
      ((s.leave k m which).circles c, (s.leave k m which).deleted c) := by
  by_cases picked : which c = true
  · cases d : s.deleted c with
    | some j =>
      have stay := Move.stay (k := k) (s.circles c, some j)
      simpa [State.leave, picked, d] using stay
    | none =>
      cases h : s.circles c with
      | none =>
        have stay := Move.stay (k := k) ((none : Option (Circle M)), (none : Option Nat))
        simpa [State.leave, picked, d, h, emptied] using stay
      | some x =>
        have leave := Move.leave (k := k) x m
        simpa [State.leave, picked, d, h, emptied] using leave
  · have stay := Move.stay (k := k) (s.circles c, s.deleted c)
    simpa [State.leave, picked] using stay

theorem set_moves (s : State M C) (k : Nat) (c c' : C) (x : Circle M)
    (here : s.circles c' = s.circles c' →
      Move k (s.circles c', s.deleted c') (some x, s.deleted c')) :
    Move k (s.circles c, s.deleted c) ((s.set c' x).circles c, (s.set c' x).deleted c) := by
  by_cases same : c = c'
  · subst c; simpa [State.set] using here rfl
  · have stay := Move.stay (k := k) (s.circles c, s.deleted c)
    simpa [State.set, same] using stay

theorem admission_moves (s : State M C) (k : Nat) (a m : M) (c' : C) (n : String) (rec : Bool)
    (next : State M C) (applied : admissionEffect s a m c' n rec = .applied next) (c : C) :
    Move k (s.circles c, s.deleted c) (next.circles c, next.deleted c) := by
  unfold admissionEffect at applied
  by_cases active : s.standing a ≠ .active
  · rw [if_pos active] at applied; cases applied
  · rw [if_neg active] at applied
    by_cases made : (madeFor s c' m n && decide (s.standing m = .active)) = true
    · rw [if_pos made] at applied; cases applied
    · rw [if_neg made] at applied
      by_cases standing : s.standing m ≠ (if rec = true then .active else .absent)
      · rw [if_pos standing] at applied; cases applied
      · rw [if_neg standing] at applied
        by_cases used : (s.circles c').isSome = true
        · rw [if_pos used] at applied; cases applied
        · rw [if_neg used] at applied
          cases applied
          have none : s.circles c' = none := by simpa using used
          exact set_moves s k c c' ⟨n, [m]⟩ (fun _ => by rw [none]; exact .make _ _ (by simp))

/-- Each circle moves as `Move` allows at every applied entry. -/
theorem effect_moves (s : State M C) (k : Nat) (e : Entry M C) (next : State M C)
    (applied : effect s k e = .applied next) (c : C) :
    Move k (s.circles c, s.deleted c) (next.circles c, next.deleted c) := by
  cases e with
  | removeMember m =>
    simp only [effect, removalEffect] at applied
    split at applied
    · cases applied
    · cases applied
    · cases applied; exact leave_moves s k m (fun _ => true) c
  | admit a m c' n => exact admission_moves s k a m c' n false next applied c
  | recover a m c' n => exact admission_moves s k a m c' n true next applied c
  | circle a change =>
    simp only [effect, changeEffect] at applied
    split at applied
    · cases applied
    · cases change with
      | create c' n =>
        simp only at applied
        split at applied
        · cases applied
        · rename_i fresh
          cases applied
          apply set_moves
          intro _
          have none : s.circles c' = none := by simpa using fresh
          rw [none]
          exact .make _ _ (by simp)
      | rename c' n =>
        simp only [managed] at applied
        split at applied
        · cases applied
        · rename_i x h
          split at applied
          · cases applied
          · rename_i live
            split at applied
            · split at applied
              · cases applied
              · cases applied
                apply set_moves
                intro _
                have live : s.deleted c' = none := by simpa using live
                rw [h, live]
                exact .rename x n
            · cases applied
      | delete c' =>
        simp only at applied
        split at applied
        · cases applied
        · rename_i live
          simp only [managed] at applied
          split at applied
          · cases applied
          · rename_i x h
            split at applied
            · cases applied
            · split at applied
              · cases applied
                have live : s.deleted c' = none := by simpa using live
                by_cases same : c = c'
                · subst c
                  have delete := Move.delete (k := k) x
                  simpa [State.set, h, live] using delete
                · have stay := Move.stay (k := k) (s.circles c, s.deleted c)
                  simpa [State.set, same] using stay
              · cases applied
      | add c' m =>
        simp only [managed] at applied
        split at applied
        · cases applied
        · rename_i x h
          split at applied
          · cases applied
          · rename_i live
            split at applied
            · split at applied
              · cases applied
              · split at applied
                · cases applied
                · cases applied
                  apply set_moves
                  intro _
                  have live : s.deleted c' = none := by simpa using live
                  rw [h, live]
                  exact .add x m
            · cases applied
      | remove c' m =>
        simp only [managed] at applied
        split at applied
        · cases applied
        · split at applied
          · cases applied
          · split at applied
            · split at applied
              · split at applied
                · cases applied; exact leave_moves s k m _ c
                · cases applied
              · cases applied
            · cases applied

theorem move_kept {k : Nat} {p q : Option (Circle M) × Option Nat} (move : Move k p q)
    (kept : Kept p) : Kept q := by
  cases move with
  | stay => exact kept
  | make x d member => cases d <;> simp_all [Kept]
  | rename x n => simpa [Kept] using kept
  | add x m => simp [Kept]
  | delete x => simp [Kept]
  | leave x m =>
    split
    · rename_i empty; simpa [Kept, List.isEmpty_iff] using empty
    · rename_i full; simpa [Kept, List.isEmpty_iff] using full

/-- Every slot keeps `Wf`. -/
theorem step_wf (s : State M C) (k : Nat) (e : Option (Entry M C)) (wf : Wf s) :
    Wf (step s k e) := by
  cases e with
  | none => exact wf
  | some e =>
    simp only [step]
    cases h : effect s k e with
    | applied next => exact fun c => move_kept (effect_moves s k e next h c) (wf c)
    | _ => exact wf

theorem wf_at (init : State M C) (seq : Nat → Option (Entry M C)) (wf : Wf init) :
    ∀ k, Wf (stateAt init seq k)
  | 0 => wf
  | k + 1 => step_wf _ _ _ (wf_at init seq wf k)

/-- **Every live circle has a member** (§15.8): a circle with none is deleted. -/
theorem live_has_member (init : State M C) (seq : Nat → Option (Entry M C)) (wf : Wf init)
    (k : Nat) (c : C) (x : Circle M) (made : (stateAt init seq k).circles c = some x)
    (live : (stateAt init seq k).deleted c = none) : x.members ≠ [] := by
  have here := wf_at init seq wf k c
  rw [made, live] at here
  exact here

theorem move_deletes {k j : Nat} {p q : Option (Circle M) × Option Nat} (move : Move k p q)
    (live : p.2 = none) (now : q.2 = some j) : j = k := by
  cases move with
  | leave x m => by_cases empty : (x.members.erase m).isEmpty <;> simp_all
  | _ => simp_all

/-- **A slot records only its own deletions** (§15.8, §15.9): a circle live
before slot `k` and deleted after it was deleted at `k`. -/
theorem deleted_at_slot (s : State M C) (k : Nat) (e : Option (Entry M C)) (c : C) (j : Nat)
    (live : s.deleted c = none) (deleted : (step s k e).deleted c = some j) : j = k := by
  cases e with
  | none => simp_all [step]
  | some e =>
    simp only [step] at deleted
    cases h : effect s k e with
    | applied next =>
      rw [h] at deleted
      exact move_deletes (effect_moves s k e next h c) live deleted
    | _ => rw [h] at deleted; simp_all [Outcome.after]

theorem erase_nil_iff {l : List M} {m : M} (nonempty : l ≠ []) : l.erase m = [] ↔ l = [m] := by
  cases l with
  | nil => contradiction
  | cons a t =>
    by_cases same : a = m
    · subst a; simp
    · simp [same]

/-- **A circle its last member leaves is deleted at that slot** (§15.8): a
removal from live circle `c` deletes it exactly when the member removed was
its only member before the slot. -/
theorem leaving_deletes (s : State M C) (k : Nat) (a m : M) (c : C) (x : Circle M)
    (made : s.circles c = some x) (next : State M C)
    (applied : effect s k (.circle a (.remove c m)) = .applied next) :
    next.deleted c = some k ↔ x.members = [m] := by
  simp only [effect, changeEffect, managed, made] at applied
  split at applied
  · cases applied
  · split at applied
    · cases applied
    · rename_i live
      split at applied
      · split at applied
        · split at applied
          · rename_i member _
            cases applied
            have live : s.deleted c = none := by simpa using live
            rw [leave_deleted]
            simp [live, made, emptied, List.isEmpty_iff, erase_nil_iff (List.ne_nil_of_mem member)]
          · cases applied
        · cases applied
      · cases applied

/-- **A circle whose last member is removed from the store is deleted at that
slot** (§15.8): the store's removal of `m` deletes live circle `c` exactly when
`m` was its only member before the slot. -/
theorem member_removal_deletes (s : State M C) (k : Nat) (m : M) (c : C) (x : Circle M)
    (wf : Wf s) (made : s.circles c = some x) (live : s.deleted c = none) (next : State M C)
    (applied : effect s k (.removeMember m) = .applied next) :
    next.deleted c = some k ↔ x.members = [m] := by
  have nonempty : x.members ≠ [] := by
    have here := wf c
    rw [made, live] at here
    exact here
  simp only [effect, removalEffect] at applied
  split at applied
  · cases applied
  · cases applied
  · cases applied
    rw [leave_deleted]
    simp [live, made, emptied, List.isEmpty_iff, erase_nil_iff nonempty]

omit [DecidableEq M] [DecidableEq C] in
theorem deleted_made (s : State M C) (wf : Wf s) (c : C) (gone : (s.deleted c).isSome) :
    ∃ x, s.circles c = some x := by
  have here := wf c
  cases h : s.circles c <;> cases d : s.deleted c <;> simp_all [Kept]

/-- **Nothing changes a deleted circle** (§15.5, §15.8, §15.9): a rename of it,
an addition to it or a removal from it fails because its target is gone, a
second deletion changes nothing, and making a circle under its id fails too. -/
theorem deleted_target_refused (s : State M C) (k : Nat) (a m : M) (c : C) (n : String)
    (wf : Wf s) (gone : (s.deleted c).isSome) (active : s.standing a = .active) :
    effect s k (.circle a (.rename c n)) = .refused .goneCircle ∧
      effect s k (.circle a (.add c m)) = .refused .goneCircle ∧
      effect s k (.circle a (.remove c m)) = .refused .goneCircle ∧
      effect s k (.circle a (.delete c)) = .unchanged ∧
      effect s k (.circle a (.create c n)) = .refused .goneCircle := by
  obtain ⟨x, made⟩ := deleted_made s wf c gone
  simp [effect, changeEffect, managed, active, gone, made]

theorem move_frozen {k : Nat} {p q : Option (Circle M) × Option Nat} (move : Move k p q)
    (kept : Kept p) (gone : p.2.isSome) : q = p := by
  cases move with
  | stay => rfl
  | make x d _ => cases d <;> simp_all [Kept]
  | _ => simp at gone

theorem step_keeps_deleted (s : State M C) (k : Nat) (e : Option (Entry M C)) (c : C)
    (wf : Wf s) (gone : (s.deleted c).isSome) :
    (step s k e).circles c = s.circles c ∧ (step s k e).deleted c = s.deleted c := by
  cases e with
  | none => exact ⟨rfl, rfl⟩
  | some e =>
    simp only [step]
    cases h : effect s k e with
    | applied next =>
      have frozen := move_frozen (effect_moves s k e next h c) (wf c) gone
      simp only [Prod.mk.injEq] at frozen
      exact frozen
    | _ => exact ⟨rfl, rfl⟩

/-- **A deletion is permanent** (§15.8, §15.9): once a circle is deleted, every
later prefix holds the same circle, with no members, and the same deletion
slot. -/
theorem deleted_stays (init : State M C) (seq : Nat → Option (Entry M C)) (wf : Wf init)
    (j : Nat) (c : C) (gone : ((stateAt init seq j).deleted c).isSome) :
    ∀ i, j ≤ i → (stateAt init seq i).circles c = (stateAt init seq j).circles c ∧
      (stateAt init seq i).deleted c = (stateAt init seq j).deleted c
  | i, later => by
    induction i with
    | zero =>
      have same : j = 0 := by omega
      subst j; exact ⟨rfl, rfl⟩
    | succ i ih =>
      by_cases same : j = i + 1
      · subst j; exact ⟨rfl, rfl⟩
      · have prior := ih (by omega)
        have still : ((stateAt init seq i).deleted c).isSome := by rw [prior.2]; exact gone
        have kept := step_keeps_deleted (stateAt init seq i) (i + 1) (seq (i + 1)) c
          (wf_at init seq wf i) still
        exact ⟨kept.1.trans prior.1, kept.2.trans prior.2⟩

/-! ## Admissions make their member's circle -/

/-- **An admission makes its member's circle.** An admission or a recovery
that applies leaves the member active and alone in the circle it names, whose
id no circle, live or deleted, held before; a new person was unknown, and a
recovered member active. -/
theorem admission_makes_circle (s : State M C) (a m : M) (c : C) (n : String) (rec : Bool)
    (next : State M C) (applied : admissionEffect s a m c n rec = .applied next) :
    s.standing a = .active ∧ s.circles c = none ∧
      next.circles c = some ⟨n, [m]⟩ ∧ next.standing m = .active ∧
      s.standing m = (if rec = true then .active else .absent) := by
  unfold admissionEffect at applied
  by_cases active : s.standing a ≠ .active
  · rw [if_pos active] at applied; cases applied
  · rw [if_neg active] at applied
    by_cases made : (madeFor s c m n && decide (s.standing m = .active)) = true
    · rw [if_pos made] at applied; cases applied
    · rw [if_neg made] at applied
      by_cases standing : s.standing m ≠ (if rec = true then .active else .absent)
      · rw [if_pos standing] at applied; cases applied
      · rw [if_neg standing] at applied
        by_cases used : (s.circles c).isSome = true
        · rw [if_pos used] at applied; cases applied
        · rw [if_neg used] at applied
          cases applied
          refine ⟨by simpa using active, by simpa using used, ?_, ?_, by simpa using standing⟩
          · simp [State.set]
          · simp

/-- **A circle id already used refuses an admission** (§13.2): live or deleted,
a circle under the id leaves a new person unknown and the store unchanged. -/
theorem used_circle_refuses_admission (s : State M C) (k : Nat) (a m : M) (c : C) (n : String)
    (active : s.standing a = .active) (unknown : s.standing m = .absent)
    (used : (s.circles c).isSome) :
    effect s k (.admit a m c n) = .refused .goneCircle := by
  have notMade : (madeFor s c m n && decide (s.standing m = .active)) = false := by simp [unknown]
  simp [effect, admissionEffect, active, notMade, unknown, used]

/-- The same admission again, once its circle holds the member under the name
it gave, changes nothing (§13.2): the check that finds an admission already
done requires the circle too. -/
theorem admission_repeat_unchanged (s : State M C) (k : Nat) (a m : M) (c : C) (n : String)
    (x : Circle M) (active : s.standing a = .active) (member : s.standing m = .active)
    (made : s.circles c = some x) (live : s.deleted c = none) (held : m ∈ x.members)
    (named : x.name = n) :
    effect s k (.admit a m c n) = .unchanged ∧ effect s k (.recover a m c n) = .unchanged := by
  simp [effect, admissionEffect, active, madeFor, made, live, held, named, member]

/-- A recovered member's new circle is made beside their old one, which
stays as it was. -/
theorem recovery_keeps_old_circles (s : State M C) (a m : M) (c d : C) (n : String)
    (next : State M C) (applied : admissionEffect s a m c n true = .applied next)
    (other : d ≠ c) : next.circles d = s.circles d := by
  have := admission_makes_circle s a m c n true next applied
  unfold admissionEffect at applied
  by_cases active : s.standing a ≠ .active
  · rw [if_pos active] at applied; cases applied
  · rw [if_neg active] at applied
    by_cases made : (madeFor s c m n && decide (s.standing m = .active)) = true
    · rw [if_pos made] at applied; cases applied
    · rw [if_neg made] at applied
      by_cases standing : s.standing m ≠ (if true = true then .active else .absent)
      · rw [if_pos standing] at applied; cases applied
      · rw [if_neg standing] at applied
        by_cases used : (s.circles c).isSome = true
        · rw [if_pos used] at applied; cases applied
        · rw [if_neg used] at applied
          cases applied
          simp [State.set, other]

end CovenSequence.CircleLifecycle
