import CovenMerge.OwnerDeleteContexts

namespace CovenMerge.Ownership.Examples

open ViewComparison

/-- First receipt is not creation identity. Two independent creations of a
shared key cannot both establish an immutable owner before their conflict is
resolved and still yield one owner on every device. -/
def firstCreator (old : Option Bool) (author : Bool) : Option Bool :=
  match old with
  | none => some author
  | some member => some member

theorem first_arrival_owner_diverges :
    [false, true].foldl firstCreator none = some false ∧
    [true, false].foldl firstCreator none = some true := by decide

example : [false, true].foldl firstCreator none ≠ [true, false].foldl firstCreator none := by decide

theorem immutable_first_arrivals_incompatible (select : List Bool → Option Bool)
    (ana : select [false] = some false) (ben : select [true] = some true)
    (keepAna : select [false, true] = select [false])
    (keepBen : select [true, false] = select [true])
    (converges : select [false, true] = select [true, false]) : False := by
  rw [keepAna, keepBen, ana, ben] at converges
  cases converges

theorem one_creation_agrees (member : Bool) :
    [member].foldl firstCreator none = some member := rfl

/-- Ben's parent, Ana's concurrent child. The rows are merged facts, before
removals. Destination membership is the replayed result, not the mover's read. -/
def moving (child readable : Bool) : MoveState Nat Nat Nat where
  rows := if child then [0, 1] else [0]
  present r := r == 0 || (child && r == 1)
  owner r := if r = 0 then some 0 else if r = 1 then some 1 else none
  parent r := if r = 1 then some 0 else none
  canRead m _ := m == 0 || readable

theorem concurrent_child_blocks :
    moveDenied (moving false false) 0 1 = false ∧
    moveDenied (moving true false) 0 1 = true := by decide

theorem concurrent_membership_blocks :
    moveDenied (moving true true) 0 1 = false ∧
    moveDenied (moving true false) 0 1 = true := by decide

/-- Depth does not stop protection at the moved parent's direct children. -/
example : carried [0, 1, 2] (fun r => if r = 1 then some 0 else if r = 2 then some 1 else none) 0 2 = true := by decide

def family (deleted : Bool) : Inputs Nat Nat where
  rows := [0, 1]
  present r := (r == 0 && !deleted) || r == 1
  refs r := if r = 1 then [⟨0, deleted⟩] else []
  checkFails _ := false
  inDeletedCircle _ := false
  claims _ := []
  rank := id

/-- Ordinary CHECK removal does not erase the row used for move protection. -/
theorem hidden_child_still_blocks :
    (view { family false with checkFails := fun r => r == 1 }).shown 1 = false ∧
    moveDenied (moving true false) 0 1 = true := by decide

def familyTrial (before after : Bool) : DeleteTrial Nat Nat Nat :=
  ⟨family before, family after, fun r => if r = 1 then some 1 else some 0, 0⟩

theorem cascade_delete_blocked : deleteDenied (familyTrial false true) = true := by decide

theorem rejected_delete_returns :
    (view (family true)).shown 0 = false ∧ (view (family true)).shown 1 = false ∧
    (view (family false)).shown 0 = true ∧ (view (family false)).shown 1 = true := by decide

def audienceTrial : DeleteTrial Nat Nat Nat :=
  let before : Inputs Nat Nat :=
    { family false with rows := [1], refs := fun _ => [], present := fun r => r == 1 }
  ⟨before, { before with inDeletedCircle := fun r => r == 1 }, fun _ => some 1, 0⟩

theorem audience_delete_blocked : deleteDenied audienceTrial = true := by decide

/-- Re-enabling a circle deletion after its active violation disappears
loops with one owned row. Direct row-owner filtering cannot reject this
store-log operation; its protection test is the data-dependent trial. -/
def rescan (rejected : Bool) : Bool := !rejected && deleteDenied audienceTrial

theorem restoration_cycle : rescan false = true ∧ rescan true = false := by decide

example : rescan (rescan false) = false ∧ rescan false ≠ false := by decide

theorem no_fixed_rejection : ¬ ∃ rejected, rescan rejected = rejected := by decide

theorem no_row_no_cycle :
    deleteDenied { audienceTrial with before := { audienceTrial.before with rows := [] } } = false := by decide

theorem no_delete_no_cycle :
    deleteDenied { audienceTrial with after := audienceTrial.before } = false := by decide

/-- One genuine trial is enough for the accumulating pass. -/
example : settle (fun _ (_ : Unit) => deleteDenied audienceTrial) [()] = [] := by
  have h : deleteDenied audienceTrial = true := by decide
  simp [settle, keepRound, h]

/-- Comparing 'all deletes' with 'all except this write' is insufficient:
two concurrent deletes mask each other's effect on Ana's child. -/
def masked (active : List Bool) (op : Bool) : Bool :=
  deleteDenied (familyTrial (!(active.filter (fun d => d != op)).isEmpty) (!active.isEmpty))

theorem concurrent_deletes_mask :
    masked [false, true] false = false ∧ masked [false, true] true = false ∧
    settle masked [false, true] = [false, true] ∧
    (view (family true)).shown 1 = false ∧ (view (family false)).shown 1 = true := by
  have h0 : masked [false, true] false = false := by decide
  have h1 : masked [false, true] true = false := by decide
  refine ⟨h0, h1, ?_, by decide, by decide⟩
  simp [settle, keepRound, h0, h1]

theorem one_delete_cannot_mask (op : Bool) : settle masked [op] = [] := by
  have h : masked [op] op = true := by cases op <;> decide
  simp [settle, keepRound, h]

/-- Every context is rebuilt; a duplicate cannot mask the other delete. -/
def familyContexts : DeleteContexts Nat Nat Nat Bool where
  rows := [0, 1]
  owner r := if r = 1 then some 1 else some 0
  actor _ := 0
  isDelete _ := true
  inputs active := family (!active.isEmpty)

def protectDuplicates (active : List Bool) (op : Bool) : Bool :=
  (contextDeleteReason (W := Bool) familyContexts active op).isSome

theorem concurrent_deletes_blocked :
    contextDeleteReason (W := Bool) familyContexts [false, true] false = some (.delete 1 1) ∧
    settle protectDuplicates [false, true] = [] ∧
    (view (family (!(settle protectDuplicates [false, true]).isEmpty))).shown 0 = true ∧
    (view (family (!(settle protectDuplicates [false, true]).isEmpty))).shown 1 = true := by
  have left : protectDuplicates [false, true] false = true := by decide
  have right : protectDuplicates [false, true] true = true := by decide
  have gone : settle protectDuplicates [false, true] = [] := by
    simp [settle, keepRound, left, right]
  exact ⟨by decide, gone, by rw [gone]; decide, by rw [gone]; decide⟩

example : settle protectDuplicates [false, true] = [] := concurrent_deletes_blocked.2.1

theorem hidden_reference_still_protected :
    contextDeleteReason (W := Bool) { familyContexts with
      inputs := fun active => { family (!active.isEmpty) with checkFails := fun r => r == 1 } }
      [false] false = some (.delete 1 1) := by decide

theorem nullable_reference_not_hidden :
    contextDeleteReason (W := Bool) { familyContexts with
      inputs := fun active => { family (!active.isEmpty) with refs := fun _ => [] } }
      [false] false = none ∧ protectDuplicates [false] false = true := by decide

theorem own_delete_allowed :
    contextDeleteReason (W := Bool) { familyContexts with
      actor := fun _ => 1
      inputs := fun active => { family false with present := fun r => r == 0 || active.isEmpty } }
      [false] false = none := by decide

theorem context_audience_delete_blocked :
    contextDeleteReason (W := Bool) { familyContexts with
      inputs := fun active => { family false with inDeletedCircle := fun _ => !active.isEmpty } }
      [false] false = some (.delete 1 1) := by decide

/-- A failed alternative follows logical keys without comparing generations.
It is retained only to state the checked counterexamples below. -/
def rowKeyReferenceDenied {Row K Member : Type} [DecidableEq Row] [BEq Member]
    (inputs : Inputs Row K) (targets : Row → Bool)
    (owners : Row → Option Member) (actor : Member) : Bool :=
  let affected := close inputs.rows (fun removed r => (inputs.refs r).any (fun ref => removed ref.parent)) targets
  inputs.rows.any fun r => inputs.present r && affected r && (owners r).any (fun m => m != actor)

/-- Ben's concurrent child reads its deleted parent as null. Its unique
expression then beats Ana's independently inserted row. Both children passed
the constraint before the deletion; the substitution retains Ben's stamp. -/
def nullUnique (deleted : Bool) : Inputs Nat Nat where
  rows := [0, 1, 2]
  present r := (r == 0 && !deleted) || r == 1 || r == 2
  refs r := if r = 1 ∧ deleted = false then [⟨0, false⟩] else []
  checkFails _ := false
  inDeletedCircle _ := false
  claims r :=
    if r = 1 then [⟨⟨["coalesce(parent_id, -1)"], none⟩, if deleted then 0 else 1, 1, false⟩]
    else if r = 2 then [⟨⟨["coalesce(parent_id, -1)"], none⟩, 0, 2, false⟩] else []
  rank := id

def nullUniqueOwner (r : Nat) : Option Nat := if r = 2 then some 1 else some 0

/-- A reference-only guard cannot establish protection through all three
passes: the set-null child stays visible while its new claim hides Ana. -/
theorem null_unique_escapes_reference_guard :
    rowKeyReferenceDenied (nullUnique true) (fun r => r == 0) nullUniqueOwner 0 = false ∧
    (view (nullUnique false)).shown 2 = true ∧ (view (nullUnique true)).shown 1 = true ∧
    (view (nullUnique true)).shown 2 = false := by decide

def nullUniqueContexts : DeleteContexts Nat Nat Nat Bool where
  rows := [0, 1, 2]
  owner := nullUniqueOwner
  actor _ := 0
  isDelete _ := true
  inputs active := nullUnique (!active.isEmpty)

def nullUniqueTrial (active : List Bool) (op : Bool) : Option (Cause Bool Nat Nat) :=
  contextDeleteReason nullUniqueContexts active op

theorem null_unique_delete_blocked :
    nullUniqueTrial [false] false = some (.delete 2 1) := by decide

/-- The empty context exposes both deletes, even though subtracting either
one from the complete view would leave Ana hidden by the other. -/
theorem duplicate_null_unique_deletes_blocked :
    nullUniqueTrial [false, true] false = some (.delete 2 1) ∧
    nullUniqueTrial [false, true] true = some (.delete 2 1) ∧
    settle (fun active op => (nullUniqueTrial active op).isSome) [false, true] = [] ∧
    (view (nullUniqueContexts.inputs
      (settle (fun active op => (nullUniqueTrial active op).isSome) [false, true]))).shown 2 = true := by
  have left : nullUniqueTrial [false, true] false = some (.delete 2 1) := by decide
  have right : nullUniqueTrial [false, true] true = some (.delete 2 1) := by decide
  have settled : settle (fun active op => (nullUniqueTrial active op).isSome) [false, true] = [] := by
    simp [settle, keepRound, left, right]
  exact ⟨left, right, settled, by rw [settled]; decide⟩

/-- False deletes Ben's parent; true is Ana's explicit deletion of her own
row. Holding Ana's operation fixed allows Ben's deletion. -/
def ownDeleteContexts : DeleteContexts Nat Nat Nat Bool where
  rows := [0, 1, 2]
  owner := nullUniqueOwner
  actor op := if op then 1 else 0
  isDelete _ := true
  inputs active :=
    let base := nullUnique (decide (false ∈ active))
    { base with present := fun r => base.present r && !(r == 2 && decide (true ∈ active)) }

theorem explicit_owner_delete_not_resurrected :
    contextDeleteReason (W := Bool) ownDeleteContexts [false, true] false = none ∧
    (view (ownDeleteContexts.inputs [false, true])).shown 2 = false := by decide

/-- Restoring a rejected deletion cannot override an independent CHECK. -/
example : (view { audienceTrial.before with checkFails := fun _ => true }).shown 1 = false := by decide

end CovenMerge.Ownership.Examples
