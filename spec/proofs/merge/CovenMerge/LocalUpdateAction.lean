import CovenMerge.Model

/-!
# Local children of values coven updates in place (§8.4, Appendix A.9)

A synced table's non-key columns change in place when coven applies another
device's write: SQLite runs an `UPDATE`, and every local foreign key into the
changed column runs its `ON UPDATE` action. Local rows exist only on this
device, so no write can account for them.

* `stuck_forever`: a local child with the default `ON UPDATE NO ACTION`
  holds the old value. The apply fails its foreign-key check at commit, rolls
  back to the same state, and so fails again on every retry. `RESTRICT` fails
  at once (`restrict_fails`).
* `follow_holds`: with `CASCADE` or `SET NULL`, every reference that held
  before the update still holds after it.
* `chain_holds`: the same along a chain of local keys, each into the column
  the previous key's action rewrites, as the schema check follows them.

A column is a list of its rows' values; `none` is NULL. The model covers one
parent value changing; keys act independently, as SQLite applies each key's
action to its own child columns.
-/

namespace CovenMerge
namespace LocalUpdate

/-- A foreign key's `ON UPDATE` action. -/
inductive Action where
  | cascade | setNull | setDefault | restrict | noAction
  deriving DecidableEq, Repr

variable {V : Type} [DecidableEq V]

/-- A key holds: every non-NULL child value names a parent value. -/
def Holds (parent child : List (Option V)) : Prop :=
  ∀ r ∈ child, r = none ∨ r ∈ parent

/-- Rows holding the non-NULL value `old` now hold `new`. -/
def rename (old : V) (new : Option V) (column : List (Option V)) : List (Option V) :=
  column.map fun r => if r = some old then new else r

/-- The value a child takes when its parent's value changes to `new`. -/
def Action.target (a : Action) (dflt new : Option V) : Option V :=
  match a with
  | .cascade => new
  | .setNull => none
  | .setDefault => dflt
  | .restrict | .noAction => new

/-- SQLite's `ON UPDATE` action when parent value `old` becomes `new`.
`none` is the statement failing at once (RESTRICT). NO ACTION changes nothing;
its check waits for commit. -/
def follow (a : Action) (dflt : Option V) (old : V) (new : Option V)
    (child : List (Option V)) : Option (List (Option V)) :=
  match a with
  | .cascade | .setNull | .setDefault => some (rename old (a.target dflt new) child)
  | .restrict => if some old ∈ child then none else some child
  | .noAction => some child

/-- **A rewritten reference still holds.** A child renamed alongside its
parent, to the parent's new value or to NULL, keeps every reference valid. -/
theorem rename_holds {parent child : List (Option V)} (h : Holds parent child)
    (old : V) (new new' : Option V) (hnew : new' = new ∨ new' = none) :
    Holds (rename old new parent) (rename old new' child) := by
  intro r hr
  simp only [rename, List.mem_map] at hr
  obtain ⟨r₀, hr₀, rfl⟩ := hr
  by_cases heq : r₀ = some old
  · subst heq
    simp only [ite_true]
    rcases hnew with rfl | rfl
    · cases hn : new' with
      | none => exact Or.inl rfl
      | some _ =>
        right
        rcases h _ hr₀ with hnone | hin
        · cases hnone
        · simp only [rename, List.mem_map]
          exact ⟨some old, hin, by simp⟩
    · exact Or.inl rfl
  · simp only [heq, ite_false]
    rcases h r₀ hr₀ with hnone | hin
    · exact Or.inl hnone
    · right
      simp only [rename, List.mem_map]
      exact ⟨r₀, hin, by simp [heq]⟩

/-- The actions the schema check accepts on a key into a column coven updates. -/
def Accepted (a : Action) : Prop := a = .cascade ∨ a = .setNull

/-- **CASCADE and SET NULL are safe.** When parent value `old` becomes `new`,
an accepted action rewrites the child so its key still holds. -/
theorem follow_holds {a : Action} (ha : Accepted a) (dflt : Option V) (old : V)
    (new : Option V) {parent child : List (Option V)} (h : Holds parent child) :
    ∃ child', follow a dflt old new child = some child' ∧
      Holds (rename old new parent) child' := by
  rcases ha with rfl | rfl
  · exact ⟨_, rfl, rename_holds h old new new (Or.inl rfl)⟩
  · exact ⟨_, rfl, rename_holds h old new none (Or.inr rfl)⟩

/-- A chain of local keys: column `i + 1` references column `i` with action
`actions[i]`; column 0 is the synced column coven updates. Each rewritten
column passes its change to the next key. -/
def propagate : List Action → Option V → V → Option V → List (List (Option V)) →
    Option (List (List (Option V)))
  | [], _, _, _, [] => some []
  | a :: as, dflt, old, new, child :: rest =>
    match follow a dflt old new child with
    | none => none
    | some child' =>
      (propagate as dflt old (a.target dflt new) rest).map (child' :: ·)
  | _, _, _, _, _ => none

/-- Every key of a chain holds: `parent` against the first column, and each
column against the next. -/
def ChainHolds : List (Option V) → List (List (Option V)) → Prop
  | _, [] => True
  | parent, child :: rest => Holds parent child ∧ ChainHolds child rest

/-- **Chains stay valid.** If every key along the chain uses CASCADE or SET
NULL, coven's in-place update of the synced value propagates without breaking
any key. -/
theorem chain_holds (dflt : Option V) (old : V) :
    ∀ (actions : List Action) (new : Option V) (parent : List (Option V))
      (columns : List (List (Option V))), actions.length = columns.length →
      (∀ a ∈ actions, Accepted a) → ChainHolds parent columns →
      ∃ columns', propagate actions dflt old new columns = some columns' ∧
        ChainHolds (rename old new parent) columns'
  | [], _, _, [], _, _, _ => ⟨[], rfl, trivial⟩
  | a :: as, new, parent, child :: rest, hlen, hacc, ⟨hfirst, hrest⟩ => by
    have ha : Accepted a := hacc a List.mem_cons_self
    have hnext : a.target dflt new = new ∨ a.target dflt new = none := by
      rcases ha with rfl | rfl
      · exact Or.inl rfl
      · exact Or.inr rfl
    have hfollow : follow a dflt old new child =
        some (rename old (a.target dflt new) child) := by
      rcases ha with rfl | rfl <;> rfl
    obtain ⟨rest', hprop, hrest'⟩ := chain_holds dflt old as (a.target dflt new) child rest
      (by simpa using hlen) (fun b hb => hacc b (List.mem_cons_of_mem _ hb)) hrest
    refine ⟨rename old (a.target dflt new) child :: rest', ?_, ?_, hrest'⟩
    · simp [propagate, hfollow, hprop]
    · exact rename_holds hfirst old new _ hnext
  | [], _, _, _ :: _, hlen, _, _ => by simp at hlen
  | _ :: _, _, _, [], hlen, _, _ => by simp at hlen

/-! ## The default `ON UPDATE NO ACTION` -/

namespace Stuck

/-- Synced `parents.code` holds `'a'` (1); this device's local `pins.code`
references it with the default `ON UPDATE NO ACTION`. Ben's write renames the
code to `'b'` (2). -/
def parent : List (Option Nat) := [some 1]
def pins : List (Option Nat) := [some 1]

/-- Applying Ben's write: update the synced value, run the key's action, check
the key at commit. A failed apply rolls back and leaves the state as it was. -/
def apply (a : Action) (state : List (Option Nat) × List (Option Nat)) :
    Option (List (Option Nat) × List (Option Nat)) :=
  let parent' := rename 1 (some 2) state.1
  match follow a none 1 (some 2) state.2 with
  | none => none
  | some pins' => if pins'.all (fun r => r.isNone || parent'.contains r) then some (parent', pins')
      else none

/-- One retry: the committed state, or the same state after a rollback. -/
def retry (a : Action) (state : List (Option Nat) × List (Option Nat)) :
    List (Option Nat) × List (Option Nat) :=
  (apply a state).getD state

/-- The state after `n` retries. -/
def retries (a : Action) : Nat → List (Option Nat) × List (Option Nat)
  | 0 => (parent, pins)
  | n + 1 => retry a (retries a n)

/-- **The apply fails forever.** Under NO ACTION the commit check fails, the
state rolls back unchanged, and every later retry fails the same way. Ben's
write can never apply on this device. -/
theorem stuck_forever : ∀ n, retries .noAction n = (parent, pins) ∧
    apply .noAction (retries .noAction n) = none := by
  have h : apply .noAction (parent, pins) = none := by decide
  intro n
  induction n with
  | zero => exact ⟨rfl, h⟩
  | succ n ih =>
    have : retries .noAction (n + 1) = (parent, pins) := by
      simp [retries, retry, ih.1, h]
    exact ⟨this, this ▸ h⟩

/-- RESTRICT refuses the update itself. -/
theorem restrict_fails : apply .restrict (parent, pins) = none := by decide

/-- CASCADE and SET NULL apply, rewriting the local pin. -/
theorem accepted_apply :
    apply .cascade (parent, pins) = some ([some 2], [some 2]) ∧
    apply .setNull (parent, pins) = some ([some 2], [none]) := by decide

end Stuck

end LocalUpdate
end CovenMerge
