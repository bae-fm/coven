import CovenMerge.OwnerAdmission

/-! How a receiver tells a write's foreign-key actions from its direct targets
without any label (§9.6, D5). A write lists every SQL operation as a step and
says nothing about what made it. The receiver calls a step a foreign-key
action exactly when it equals the schema's consequence of an earlier step of
the same write, given a reference the step's row holds at that moment: the
authoring view replayed through the write's earlier steps. Every other step is
a direct target and needs its row's permission.

* `action_iff` is that rule; `cause` names the nearest such earlier step.
* `action_classified`: every action SQLite actually ran is classified as one,
  so an honest author's cascades are never refused as direct edits.
* `certificate`, `every_step_rooted`: every step has a finite B18 certificate
  whose roots are direct steps. With the direct permission, every step of an
  admitted write, also on another member's row, is an exact chain of schema
  consequences from steps its writer may change.
* `follows_cause`: when steps carry the parent each written reference names
  (D5 `parents`), an action can only name the row whose step caused it.
  Like B18's `consequence`, the model updates a referenced key in place; coven
  makes that update a delete and an insert, and the action then names the row
  the insert made.

Four checked histories bound the rule. `TriggerFirst`: a shared trigger can
make the exact step SQLite's own action would have made, before the action
reaches the row; it is classified as an action, so "action" here means "is
the schema's consequence", not "SQLite's action code made it". `FollowBack`:
references must be replayed through the write's earlier steps; the view at the
write's start refuses an honest cascade. `Retarget`: with a key updated in
place, comparing values without the parents they name accepts a direct edit
that points another member's row at a different parent; comparing the named
parents refuses it. In coven a referenced key is a primary key, and changing
it ends the incarnation an older reference names, which refuses that edit
too. `Affinity`: a cascade stores the parent's new key in the child column's
affinity; comparing the key as the parent holds it refuses an honest cascade
into a TEXT column from an INTEGER key (`storedRule` compares stored values). -/
namespace CovenMerge.Ownership.Classification

open ForeignKey

/-- One write's steps in execution order (D5 `sequence`, across the write's
parts), and the references each row holds just before each step: what its
reference columns hold then and the parent incarnation each names. -/
structure Trace (Row Ref Payload : Type) where
  steps : List Payload
  refs : Nat → Row → List Ref

/-- What the receiver reads from a step and the original schema: the row the
step changes, and the action a resolved reference takes on a parent step. -/
structure Rule (Row Ref Payload : Type) where
  row : Payload → Row
  act : Ref → Payload → Option Payload

variable {Row Ref Payload : Type} [DecidableEq Payload]

/-- Step `i` equals the consequence of earlier step `j`, through a reference
step `i`'s row holds at that moment. -/
def Candidate (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (i j : Nat) : Prop :=
  j < i ∧ ∃ s p, t.steps[i]? = some s ∧ t.steps[j]? = some p ∧
    ∃ f ∈ t.refs i (R.row s), R.act f p = some s

def candidate (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (i j : Nat) : Bool :=
  decide (j < i) && match t.steps[i]?, t.steps[j]? with
    | some s, some p => (t.refs i (R.row s)).any fun f => decide (R.act f p = some s)
    | _, _ => false

theorem candidate_iff (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (i j : Nat) :
    candidate R t i j = true ↔ Candidate R t i j := by
  unfold candidate Candidate
  cases hs : t.steps[i]? <;> cases hp : t.steps[j]? <;> simp [List.any_eq_true]

/-- The greatest `j < n` satisfying `p`. -/
def nearest (p : Nat → Bool) : Nat → Option Nat
  | 0 => none
  | n + 1 => if p n then some n else nearest p n

theorem nearest_some {p : Nat → Bool} {n k : Nat} (h : nearest p n = some k) :
    k < n ∧ p k = true := by
  induction n with
  | zero => simp [nearest] at h
  | succ n ih =>
    simp only [nearest] at h
    split at h
    · cases h
      exact ⟨Nat.lt_succ_self _, by assumption⟩
    · obtain ⟨lt, hp⟩ := ih h
      exact ⟨by omega, hp⟩

theorem nearest_greatest {p : Nat → Bool} {n j : Nat} (lt : j < n) (hj : p j = true) :
    ∃ k, nearest p n = some k ∧ j ≤ k := by
  induction n with
  | zero => omega
  | succ n ih =>
    simp only [nearest]
    by_cases hn : p n = true
    · exact ⟨n, by simp [hn], by omega⟩
    · have below : j < n := by
        rcases Nat.lt_succ_iff_lt_or_eq.mp lt with h | h
        · exact h
        · subst h
          exact absurd hj hn
      obtain ⟨k, hk, le⟩ := ih below
      exact ⟨k, by simp [hn, hk], le⟩

/-- The nearest earlier step whose consequence step `i` is, if any. -/
def cause (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (i : Nat) : Option Nat :=
  nearest (candidate R t i) i

theorem cause_candidate {R : Rule Row Ref Payload} {t : Trace Row Ref Payload} {i j : Nat}
    (h : cause R t i = some j) : Candidate R t i j :=
  (candidate_iff R t i j).mp (nearest_some h).2

theorem cause_lt {R : Rule Row Ref Payload} {t : Trace Row Ref Payload} {i j : Nat}
    (h : cause R t i = some j) : j < i :=
  (cause_candidate h).1

/-- **The receiver's rule.** A step is classified as a foreign-key action
exactly when it equals the schema's consequence of an earlier step, given a
reference its row holds at that moment. The classification reads only the
steps, the replayed references and the schema; a `Trace` has no field for a
sender's label or cause list. -/
theorem action_iff (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (i : Nat) :
    (cause R t i).isSome = true ↔ ∃ j, Candidate R t i j := by
  constructor
  · intro h
    obtain ⟨j, hj⟩ := Option.isSome_iff_exists.mp h
    exact ⟨j, cause_candidate hj⟩
  · rintro ⟨j, hj⟩
    obtain ⟨k, hk, _⟩ := nearest_greatest hj.1 ((candidate_iff R t i j).mpr hj)
    simp [cause, hk]

/-- What actually made a step on the author's SQLite. It is never sent. -/
inductive Made where
  | statement
  | trigger
  | action (cause : Nat)
  deriving DecidableEq, Repr

/-- SQLite at the extraction boundary: each foreign-key action it runs is the
schema's consequence of its cause, through the reference the child holds when
the action reaches it. -/
def Faithful (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (made : Nat → Made) : Prop :=
  ∀ i j, made i = .action j → Candidate R t i j

/-- Every action SQLite ran is classified as an action. The receiver names
its actual cause or a later step whose consequence is the same payload. -/
theorem action_classified {R : Rule Row Ref Payload} {t : Trace Row Ref Payload}
    {made : Nat → Made} (faithful : Faithful R t made) {i j : Nat} (h : made i = .action j) :
    ∃ k, cause R t i = some k ∧ j ≤ k := by
  have hj := faithful i j h
  exact nearest_greatest hj.1 ((candidate_iff R t i j).mpr hj)

/-- A step the receiver calls direct was made by a statement or a trigger. -/
theorem direct_was_direct {R : Rule Row Ref Payload} {t : Trace Row Ref Payload}
    {made : Nat → Made} (faithful : Faithful R t made) {i : Nat} (h : cause R t i = none) :
    made i = .statement ∨ made i = .trigger := by
  cases hm : made i with
  | statement => exact Or.inl rfl
  | trigger => exact Or.inr rfl
  | action j =>
    obtain ⟨k, hk, _⟩ := action_classified faithful hm
    rw [h] at hk
    cases hk

/-! ## Certificates

Each step is a node of B18's derivation model, indexed by its position: the
direct steps are roots, and each action is a consequence of its cause. -/

def directSteps (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (j : Nat) : Option Payload :=
  if cause R t j = none then t.steps[j]? else none

def stepSchema (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) : Schema Nat Payload where
  consequences j p := (List.range t.steps.length).filterMap fun i =>
    if cause R t i = some j ∧ t.steps[j]? = some p then (t.steps[i]?).map (i, ·) else none
  combine _ _ := none
  -- Steps set no generated cells: there are none to leave out.
  withoutGenerated := id

/-- Every edge of the certificate schema is a real schema consequence. -/
theorem edge_exact {R : Rule Row Ref Payload} {t : Trace Row Ref Payload} {j i : Nat}
    {p s : Payload} (h : (i, s) ∈ (stepSchema R t).consequences j p) :
    cause R t i = some j ∧ t.steps[i]? = some s ∧ t.steps[j]? = some p ∧
      ∃ f ∈ t.refs i (R.row s), R.act f p = some s := by
  simp only [stepSchema, List.mem_filterMap, List.mem_range] at h
  obtain ⟨a, _, ha⟩ := h
  split at ha
  · rename_i cond
    obtain ⟨s', hs', eq⟩ := Option.map_eq_some_iff.mp ha
    cases eq
    obtain ⟨_, s'', p', hi, hj, f, mem, act⟩ := cause_candidate cond.1
    rw [hi] at hs'
    cases hs'
    rw [cond.2] at hj
    cases hj
    exact ⟨cond.1, hi, cond.2, f, mem, act⟩
  · cases ha

theorem step_implied (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (i : Nat) (s : Payload)
    (h : t.steps[i]? = some s) : Implied (stepSchema R t) (directSteps R t) i s := by
  induction i using Nat.strongRecOn generalizing s with
  | ind i ih =>
    cases hc : cause R t i with
    | none => exact .direct (by simp [directSteps, hc, h])
    | some j =>
      obtain ⟨lt, s', p, hs, hp, _⟩ := cause_candidate hc
      rw [h] at hs
      cases hs
      refine .foreign (ih j lt p hp) ?_
      have bound : i < t.steps.length := (List.getElem?_eq_some_iff.mp h).1
      simp only [stepSchema, List.mem_filterMap, List.mem_range]
      exact ⟨i, bound, by simp [hc, hp, h]⟩

/-- **Every step has a certificate.** It evaluates exactly to the step under
B18's `evaluate`, and every root is a step the receiver calls direct. -/
theorem certificate (R : Rule Row Ref Payload) (t : Trace Row Ref Payload) (i : Nat) (s : Payload)
    (h : t.steps[i]? = some s) :
    ∃ d, evaluate (stepSchema R t) (directSteps R t) d = some (i, s) ∧
      ∀ j ∈ roots d, cause R t j = none ∧ ∃ p, t.steps[j]? = some p := by
  obtain ⟨d, hd⟩ := evaluate_complete _ _ (step_implied R t i s h)
  refine ⟨d, hd, fun j hj => ?_⟩
  obtain ⟨p, hp⟩ := evaluate_roots _ _ d hd j hj
  unfold directSteps at hp
  split at hp
  · exact ⟨by assumption, p, hp⟩
  · cases hp

/-- **Composition with the direct permission** (`Admission.statement_authorized`).
If every step the receiver calls direct is on a row its writer may change,
every step of the write, also one on a row the writer may not change, is an
exact chain of schema consequences rooted only in permitted direct steps. -/
theorem every_step_rooted (R : Rule Row Ref Payload) (t : Trace Row Ref Payload)
    (permitted : Row → Bool)
    (direct_ok : ∀ j p, cause R t j = none → t.steps[j]? = some p → permitted (R.row p) = true)
    (i : Nat) (s : Payload) (h : t.steps[i]? = some s) :
    ∃ d, evaluate (stepSchema R t) (directSteps R t) d = some (i, s) ∧
      ∀ j ∈ roots d, ∃ p, t.steps[j]? = some p ∧ permitted (R.row p) = true := by
  obtain ⟨d, hd, rooted⟩ := certificate R t i s h
  refine ⟨d, hd, fun j hj => ?_⟩
  obtain ⟨direct, p, hp⟩ := rooted j hj
  exact ⟨p, hp, direct_ok j p direct hp⟩

/-- A step on a row its writer may not change is admitted only as an action. -/
theorem forbidden_row_needs_action (R : Rule Row Ref Payload) (t : Trace Row Ref Payload)
    (permitted : Row → Bool)
    (direct_ok : ∀ j p, cause R t j = none → t.steps[j]? = some p → permitted (R.row p) = true)
    (i : Nat) (s : Payload) (h : t.steps[i]? = some s) (forbidden : permitted (R.row s) = false) :
    ∃ j, cause R t i = some j := by
  cases hc : cause R t i with
  | some j => exact ⟨j, rfl⟩
  | none =>
    have := direct_ok i s hc h
    rw [forbidden] at this
    cases this

/-! ## Steps on rows -/

section Steps

variable {Row Col Value : Type} [DecidableEq Row] [DecidableEq Col] [DecidableEq Value]

/-- Values only: a step is its operation, generation and complete scalar patch. -/
def effectRule : Rule Row (Reference Row Col Value) (Effect Row Col Value) :=
  ⟨Effect.row, consequence⟩

/-- A D5 step: the effect, and the parent incarnation each written reference
column names at that moment (`ColumnValue.parents`). NULL names no parent. -/
structure Step (Row Col Value : Type) where
  effect : Effect Row Col Value
  parents : List (Col × Row × Nat)
  deriving DecidableEq

/-- An action's step names the reference's own parent for every column it
writes a value into. A delete or a NULL names none. -/
def stepConsequence (f : Reference Row Col Value) (p : Step Row Col Value) :
    Option (Step Row Col Value) :=
  (consequence f p.effect).map fun e =>
    ⟨e, if e.kind = .upd ∧ e.cells.all (fun cell => cell.2.isSome) then
      f.columns.map (·, f.parent, f.parentGeneration) else []⟩

def stepRule : Rule Row (Reference Row Col Value) (Step Row Col Value) :=
  ⟨fun s => s.effect.row, stepConsequence⟩

theorem consequence_parent {f : Reference Row Col Value} {p e : Effect Row Col Value}
    (h : consequence f p = some e) : p.row = f.parent ∧ p.generation = f.parentGeneration := by
  unfold consequence at h
  split at h
  · assumption
  · cases h

/-- **An action follows its cause.** A classified step names, in every
reference it writes, the very row and incarnation whose step caused it; an
action cannot point a row at another parent. -/
theorem follows_cause {t : Trace Row (Reference Row Col Value) (Step Row Col Value)} {i j : Nat}
    (h : cause stepRule t i = some j) :
    ∃ s p, t.steps[i]? = some s ∧ t.steps[j]? = some p ∧
      ∀ named ∈ s.parents, named.2 = (p.effect.row, p.effect.generation) := by
  obtain ⟨_, s, p, hs, hp, f, _, act⟩ := cause_candidate h
  refine ⟨s, p, hs, hp, fun named mem => ?_⟩
  obtain ⟨e, he, eq⟩ := Option.map_eq_some_iff.mp act
  obtain ⟨row, gen⟩ := consequence_parent he
  subst eq
  simp only at mem
  split at mem
  · obtain ⟨c, _, rfl⟩ := List.mem_map.mp mem
    simp [row, gen]
  · cases mem

/-- Values as the child's columns store them. SQLite converts a value written
into a column to that column's affinity, so a cascade stores the parent's new
key in the child's affinity (`store`). A reference already holds its parent's
old key in the parent's affinity (`Reference.oldKey`), as SQLite compares it. -/
def storedRule (store : Col → Option Value → Option Value) :
    Rule Row (Reference Row Col Value) (Effect Row Col Value) :=
  ⟨Effect.row, fun f p => (consequence f p).map fun e =>
    { e with cells := e.cells.map fun cell => (cell.1, store cell.1 cell.2) }⟩

end Steps

/-! ## Checked histories

Rows and columns are numbers. Every reference below is CASCADE on delete and
on update, from column 1 of the child to column 0 of the parent. -/

def ref (parent child : Nat) (key : Nat) : Reference Nat Nat Nat :=
  ⟨parent, 1, child, 1, [1], [some key], .cascade, .cascade, [0], []⟩

namespace TriggerFirst

/-! Ben deletes his track 0. SQLite's cascade deletes Ana's review 1, and a
shared AFTER DELETE trigger on reviews, fired by that deletion, deletes Ana's
review 2 before the cascade reaches it; the cascade then finds 2 gone. -/

def trace : Trace Nat (Reference Nat Nat Nat) (Step Nat Nat Nat) where
  steps := [⟨⟨0, 1, .del, []⟩, []⟩, ⟨⟨1, 1, .del, []⟩, []⟩, ⟨⟨2, 1, .del, []⟩, []⟩]
  refs _ r := if r = 1 ∨ r = 2 then [ref 0 r 7] else []

def made : Nat → Made
  | 1 => .action 0
  | 2 => .trigger
  | _ => .statement

theorem faithful : Faithful stepRule trace made := by
  intro i j h
  rcases i with _ | _ | _ | i
  · cases h
  · cases h
    exact (candidate_iff stepRule trace 1 0).mp (by decide)
  · cases h
  · cases h

/-- The trigger's step is classified as the track deletion's action: its
payload is exactly that action's, so no label could tell them apart. -/
theorem trigger_classified_as_action :
    made 2 = .trigger ∧ cause stepRule trace 2 = some 0 ∧
    stepConsequence (ref 0 2 7) ⟨⟨0, 1, .del, []⟩, []⟩ = some ⟨⟨2, 1, .del, []⟩, []⟩ := by
  decide

end TriggerFirst

namespace FollowBack

/-! Ben changes his track 0's referenced code 1 to 2 and back. Ana's review 1
follows both times through ON UPDATE CASCADE. -/

def steps : List (Effect Nat Nat Nat) :=
  [⟨0, 1, .upd, [(0, some 2)]⟩, ⟨1, 1, .upd, [(1, some 2)]⟩,
   ⟨0, 1, .upd, [(0, some 1)]⟩, ⟨1, 1, .upd, [(1, some 1)]⟩]

/-- References replayed through the write's earlier steps. -/
def replayed : Trace Nat (Reference Nat Nat Nat) (Effect Nat Nat Nat) where
  steps := steps
  refs i r := if r = 1 then [ref 0 1 (if i = 2 ∨ i = 3 then 2 else 1)] else []

/-- References as the write's authoring view held them before it began. -/
def atStart : Trace Nat (Reference Nat Nat Nat) (Effect Nat Nat Nat) where
  steps := steps
  refs _ r := if r = 1 then [ref 0 1 1] else []

def made : Nat → Made
  | 1 => .action 0
  | 3 => .action 2
  | _ => .statement

theorem faithful : Faithful effectRule replayed made := by
  intro i j h
  rcases i with _ | _ | _ | _ | i
  · cases h
  · cases h
    exact (candidate_iff effectRule replayed 1 0).mp (by decide)
  · cases h
  · cases h
    exact (candidate_iff effectRule replayed 3 2).mp (by decide)
  · cases h

theorem replayed_classifies :
    cause effectRule replayed 1 = some 0 ∧ cause effectRule replayed 3 = some 2 := by decide

/-- The view at the write's start would make the second cascade a direct
edit of Ana's review, refusing Ben's honest write. -/
theorem start_view_refuses_cascade :
    made 3 = .action 2 ∧ cause effectRule atStart 3 = none := by decide

end FollowBack

namespace Retarget

/-! After the follow-back, Ben inserts his track 2 with code 2, then his own
statement sets Ana's review 1 to code 2, which now names track 2. -/

def made : Nat → Made
  | 1 => .action 0
  | 3 => .action 2
  | _ => .statement

def refs (i r : Nat) : List (Reference Nat Nat Nat) :=
  if r = 1 then [ref 0 1 (if i = 2 ∨ i = 3 then 2 else 1)] else []

def values : Trace Nat (Reference Nat Nat Nat) (Effect Nat Nat Nat) where
  steps := FollowBack.steps ++ [⟨2, 0, .ins, [(0, some 2)]⟩, ⟨1, 1, .upd, [(1, some 2)]⟩]
  refs := refs

/-- The same SQL with each written reference's named parent. -/
def named : Trace Nat (Reference Nat Nat Nat) (Step Nat Nat Nat) where
  steps := [⟨⟨0, 1, .upd, [(0, some 2)]⟩, []⟩, ⟨⟨1, 1, .upd, [(1, some 2)]⟩, [(1, 0, 1)]⟩,
    ⟨⟨0, 1, .upd, [(0, some 1)]⟩, []⟩, ⟨⟨1, 1, .upd, [(1, some 1)]⟩, [(1, 0, 1)]⟩,
    ⟨⟨2, 0, .ins, [(0, some 2)]⟩, []⟩, ⟨⟨1, 1, .upd, [(1, some 2)]⟩, [(1, 2, 1)]⟩]
  refs := refs

theorem faithful : Faithful effectRule values made ∧ Faithful stepRule named made := by
  constructor
  all_goals
    intro i j h
    rcases i with _ | _ | _ | _ | i
    · cases h
    · cases h
      exact (candidate_iff _ _ 1 0).mp (by decide)
    · cases h
    · cases h
      exact (candidate_iff _ _ 3 2).mp (by decide)
    · cases h

/-- Values alone make Ben's direct edit of Ana's review look like the first
cascade, which it had already taken. -/
theorem values_accept_retarget :
    made 5 = .statement ∧ cause effectRule values 5 = some 0 := by decide

/-- With named parents the edit names track 2, not track 0, and stays direct;
the honest cascades are still actions. -/
theorem parents_refuse_retarget :
    cause stepRule named 5 = none ∧
    cause stepRule named 1 = some 0 ∧ cause stepRule named 3 = some 2 := by decide

end Retarget

namespace Affinity

/-! Ben's code 0 has the INTEGER key 7, which Ana's tag 1 holds in a TEXT
column as the text '7'. Ben changes the code to 8, and ON UPDATE CASCADE
stores the text '8' in the tag. Values are numbers: 7 and 8 are the integers,
17 and 18 the texts. -/

def store (c : Nat) (v : Option Nat) : Option Nat :=
  if c = 1 then v.map (· + 10) else v

def trace : Trace Nat (Reference Nat Nat Nat) (Effect Nat Nat Nat) where
  steps := [⟨0, 1, .upd, [(0, some 8)]⟩, ⟨1, 1, .upd, [(1, some 18)]⟩]
  refs _ r := if r = 1 then [ref 0 1 7] else []

def made : Nat → Made
  | 1 => .action 0
  | _ => .statement

theorem faithful : Faithful (storedRule store) trace made := by
  intro i j h
  rcases i with _ | _ | i
  · cases h
  · cases h
    exact (candidate_iff _ _ 1 0).mp (by decide)
  · cases h

/-- Comparing the parent's new key with the tag's stored value calls SQLite's
cascade a direct edit of Ana's tag, refusing Ben's honest write. -/
theorem unstored_refuses_cascade : made 1 = .action 0 ∧ cause effectRule trace 1 = none := by
  decide

theorem stored_classifies : cause (storedRule store) trace 1 = some 0 := by decide

end Affinity

end CovenMerge.Ownership.Classification
