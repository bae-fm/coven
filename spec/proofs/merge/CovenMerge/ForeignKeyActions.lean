import CovenMerge.Ownership

/-! Foreign-key consequences are recomputed on the receiver. `Effect` contains
all changed scalars, so matching an operation tag alone cannot authorize a
foreign-owned target. Schema extraction and the authenticated authoring view
supply resolved foreign keys, as they supply `Ref` to the removal model. -/
namespace CovenMerge.Ownership.ForeignKey

inductive Action where
  | cascade | setNull | restrict | noAction
  deriving DecidableEq, Repr

/-- SQL NULL is distinct from an absent column. Insert/update cells are new
values; delete cells are D5.6's `old` map, as the replaced setters wrote them. -/
structure Effect (Row Col Value : Type) where
  row : Row
  generation : Nat
  kind : Kind
  cells : List (Col × Option Value)
  deriving DecidableEq

structure Reference (Row Col Value : Type) where
  parent : Row
  parentGeneration : Nat
  child : Row
  childGeneration : Nat
  columns : List Col
  oldKey : List (Option Value)
  onDelete : Action
  onUpdate : Action
  parentColumns : List Col
  /-- Authenticated values of the child's replaced setters, before the write.
  Step-only classification supplies no net delete map; receipt supplies all cells. -/
  childValues : List (Col × Option Value)
  deriving DecidableEq

variable {Row Col Value : Type} [DecidableEq Row] [DecidableEq Col] [DecidableEq Value]

/-- Compound key actions write every child column in declaration order. -/
def actionEffect (f : Reference Row Col Value) (action : Action)
    (replacement : Option (List (Option Value))) : Option (Effect Row Col Value) :=
  match action, replacement with
  | .cascade, none => some ⟨f.child, f.childGeneration, .del, f.childValues⟩
  | .cascade, some values =>
    if values.length = f.columns.length then
      some ⟨f.child, f.childGeneration, .upd, f.columns.zip values⟩
    else none
  | .setNull, _ => some ⟨f.child, f.childGeneration, .upd, f.columns.map (·, none)⟩
  | .restrict, _ | .noAction, _ => none

/-- The parent key after its patch, retaining the old value of an unchanged
column. The resolved reference retains the original complete key tuple. -/
def keyAfter (f : Reference Row Col Value) (parent : Effect Row Col Value) :
    List (Option Value) :=
  (f.parentColumns.zip f.oldKey).map fun (column, old) =>
    match parent.cells.lookup column with
    | some value => value
    | none => old

/-- Only the schema chooses the action and columns; the sender supplies no
replacement tuple, target generation, or action kind to this computation. -/
def consequence (f : Reference Row Col Value) (parent : Effect Row Col Value) :
    Option (Effect Row Col Value) :=
  if parent.row = f.parent ∧ parent.generation = f.parentGeneration then
    match parent.kind with
    | .del => actionEffect f f.onDelete none
    | .upd => if keyAfter f parent = f.oldKey then none
      else actionEffect f f.onUpdate (some (keyAfter f parent))
    | .ins => none
  else none

/-- Coalesce successive actions on one row. Later setters replace earlier
setters of the same column; other columns remain. A delete retains its old map. -/
def combineEffects (a b : Effect Row Col Value) : Option (Effect Row Col Value) :=
  if a.row = b.row ∧ a.generation = b.generation then
    if b.kind = .del then some b
    else if a.kind = .del then some a
    else if a.kind = .upd ∧ b.kind = .upd then
      some ⟨a.row, a.generation, .upd,
        (a.cells.filter fun cell => !(b.cells.map Prod.fst).contains cell.1) ++ b.cells⟩
    else none
  else none

/-- The receiver's deterministic SQL interpretation. Its inputs are the
schema and reconstructed authoring execution, never a wire origin tag.
As with `EntryInputs`, SQL extraction is a boundary: complete row patches,
key normalization, and sequential/coalesced actions are supplied there. -/
structure Schema (Row Payload : Type) where
  consequences : Row → Payload → List (Row × Payload)
  combine : Row × Payload → Row × Payload → Option (Row × Payload)

/-- Concrete DELETE and referenced-key UPDATE actions on resolved references. -/
def resolvedSchema (references : List (Reference Row Col Value)) :
    Schema Row (Effect Row Col Value) where
  consequences _ parent := references.filterMap fun ref =>
    (consequence ref parent).map fun effect => (effect.row, effect)

  combine a b := (combineEffects a.2 b.2).map fun effect => (effect.row, effect)

/-- A finite derivation rooted in a direct change in this same write. An edge
names a schema consequence by position; its payload is recomputed. Shared
trigger writes are direct roots, including triggers fired by FK actions. -/
inductive Derivation (Row : Type) where
  | direct (row : Row)
  | foreign (parent : Derivation Row) (index : Nat)
  | together (first second : Derivation Row)
  deriving DecidableEq, Repr

variable {Payload : Type}

def evaluate (schema : Schema Row Payload) (direct : Row → Option Payload) :
    Derivation Row → Option (Row × Payload)
  | .direct row => (direct row).map (row, ·)
  | .foreign parent index =>
    (evaluate schema direct parent).bind fun (row, value) =>
      (schema.consequences row value)[index]?

  | .together first second =>
    (evaluate schema direct first).bind fun a =>
      (evaluate schema direct second).bind fun b => schema.combine a b

def root : Derivation Row → Row
  | .direct row => row
  | .foreign parent _ => root parent
  | .together first _ => root first

def roots : Derivation Row → List Row
  | .direct row => [row]
  | .foreign parent _ => roots parent
  | .together first second => roots first ++ roots second

/-- Every intermediate row used by a certificate is retained as a cause.
The root is included even when a malformed certificate cannot be evaluated. -/
def sources (schema : Schema Row Payload) (direct : Row → Option Payload) :
    Derivation Row → List Row
  | .direct row => [row]
  | .foreign parent _ => sources schema direct parent ++
      ((evaluate schema direct parent).map Prod.fst).toList

  | .together first second => sources schema direct first ++ sources schema direct second

theorem root_mem_sources (schema : Schema Row Payload) (direct : Row → Option Payload)
    (d : Derivation Row) : root d ∈ sources schema direct d := by
  induction d with
  | direct row => simp [sources, root]
  | foreign parent index ih => exact List.mem_append_left _ ih
  | together first second ih _ => exact List.mem_append_left _ ih

theorem roots_mem_sources (schema : Schema Row Payload) (direct : Row → Option Payload)
    (d : Derivation Row) (r : Row) (h : r ∈ roots d) : r ∈ sources schema direct d := by
  induction d with
  | direct row => exact h
  | foreign parent index ih => exact List.mem_append_left _ (ih h)
  | together first second ia ib =>
    exact (List.mem_append.mp h).elim
      (fun ha => List.mem_append_left _ (ia ha)) (fun hb => List.mem_append_right _ (ib hb))

/-- Declarative meaning of the schema, independent of certificates. -/
inductive Implied (schema : Schema Row Payload) (direct : Row → Option Payload) :
    Row → Payload → Prop where
  | direct {row value} : direct row = some value → Implied schema direct row value
  | foreign {row value child result} : Implied schema direct row value →
      (child, result) ∈ schema.consequences row value → Implied schema direct child result
  | together {a av b bv row value} : Implied schema direct a av →
      Implied schema direct b bv → schema.combine (a, av) (b, bv) = some (row, value) →
      Implied schema direct row value

theorem evaluate_sound (schema : Schema Row Payload) (direct : Row → Option Payload)
    (d : Derivation Row) {row value} (h : evaluate schema direct d = some (row, value)) :
    Implied schema direct row value := by
  induction d generalizing row value with
  | direct r =>
    obtain ⟨p, hp, eq⟩ := Option.map_eq_some_iff.mp h
    cases eq
    exact .direct hp
  | foreign parent index ih =>
    obtain ⟨⟨r, p⟩, hp, effect⟩ := Option.bind_eq_some_iff.mp h
    exact .foreign (ih hp) (List.mem_of_getElem? effect)
  | together first second ia ib =>
    obtain ⟨⟨a, av⟩, ha, rest⟩ := Option.bind_eq_some_iff.mp h
    obtain ⟨⟨b, bv⟩, hb, combined⟩ := Option.bind_eq_some_iff.mp rest
    exact .together (ia ha) (ib hb) combined

theorem evaluate_root (schema : Schema Row Payload) (direct : Row → Option Payload)
    (d : Derivation Row) {row value} (h : evaluate schema direct d = some (row, value)) :
    ∃ p, direct (root d) = some p := by
  induction d generalizing row value with
  | direct r =>
    obtain ⟨p, hp, _⟩ := Option.map_eq_some_iff.mp h
    exact ⟨p, hp⟩
  | foreign parent index ih =>
    obtain ⟨_, hp, _⟩ := Option.bind_eq_some_iff.mp h
    exact ih hp
  | together first second ih _ =>
    obtain ⟨_, hp, _⟩ := Option.bind_eq_some_iff.mp h
    exact ih hp

theorem evaluate_complete (schema : Schema Row Payload) (direct : Row → Option Payload)
    {row value} (h : Implied schema direct row value) :
    ∃ d, evaluate schema direct d = some (row, value) := by
  induction h with
  | @direct r v hp => exact ⟨.direct r, by simp [evaluate, hp]⟩
  | @foreign r p child result parent member ih =>
    obtain ⟨d, hd⟩ := ih
    obtain ⟨i, hi⟩ := List.mem_iff_getElem?.mp member
    exact ⟨.foreign d i, by simp [evaluate, hd, hi]⟩
  | together _ _ combined ia ib =>
    obtain ⟨a, ha⟩ := ia
    obtain ⟨b, hb⟩ := ib
    exact ⟨.together a b, by simp [evaluate, ha, hb, combined]⟩

theorem evaluate_roots (schema : Schema Row Payload) (direct : Row → Option Payload)
    (d : Derivation Row) {row value} (h : evaluate schema direct d = some (row, value))
    (source : Row) (used : source ∈ roots d) : ∃ p, direct source = some p := by
  induction d generalizing row value with
  | direct r =>
    have eq : source = r := by simpa [roots] using used
    subst source
    obtain ⟨p, hp, _⟩ := Option.map_eq_some_iff.mp h
    exact ⟨p, hp⟩
  | foreign parent index ih =>
    obtain ⟨⟨r, p⟩, hp, _⟩ := Option.bind_eq_some_iff.mp h
    exact ih hp used
  | together first second ia ib =>
    obtain ⟨⟨a, av⟩, ha, rest⟩ := Option.bind_eq_some_iff.mp h
    obtain ⟨⟨b, bv⟩, hb, _⟩ := Option.bind_eq_some_iff.mp rest
    exact (List.mem_append.mp used).elim (ia ha) (ib hb)

theorem dropped_root (schema : Schema Row Payload) (direct : Row → Option Payload)
    (d : Derivation Row) (dropped : direct (root d) = none) :
    evaluate schema direct d = none := by
  cases h : evaluate schema direct d with
  | none => rfl
  | some pair =>
    obtain ⟨p, hp⟩ := evaluate_root schema direct d h
    simp [dropped] at hp

/-- Matching includes every scalar, the target, generation, and operation. -/
def verifies [DecidableEq Payload] (schema : Schema Row Payload)
    (direct : Row → Option Payload) (d : Derivation Row) (row : Row) (value : Payload) : Bool :=
  decide (evaluate schema direct d = some (row, value))

/-- D5.6: changing, omitting or adding any deleted cell refuses the action.
The recomputed payload comes from authenticated setter values, not displayed rows. -/
theorem delete_old_mismatch_refused (schema : Schema Row (Effect Row Col Value))
    (direct : Row → Option (Effect Row Col Value)) (d : Derivation Row)
    (row : Row) (generation : Nat) (written proposed : List (Col × Option Value))
    (expected : evaluate schema direct d = some (row, ⟨row, generation, .del, written⟩))
    (different : proposed ≠ written) :
    verifies schema direct d row ⟨row, generation, .del, proposed⟩ = false := by
  simp [verifies, expected, different.symm]

theorem no_forged_effect [DecidableEq Payload] (schema : Schema Row Payload)
    (direct : Row → Option Payload) (d : Derivation Row) (row : Row) (value : Payload)
    (accepted : verifies schema direct d row value = true) :
    Implied schema direct row value :=
  evaluate_sound schema direct d (of_decide_eq_true accepted)

end CovenMerge.Ownership.ForeignKey
