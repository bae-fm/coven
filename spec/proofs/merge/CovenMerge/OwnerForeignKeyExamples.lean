import CovenMerge.OwnerAdmission

namespace CovenMerge.Ownership.ForeignKeyExamples

open ForeignKey

/-- An owned child with a two-column reference and a third, unrelated cell. -/
def compound (action : Action) : Reference Nat Nat Nat :=
  ⟨0, 1, 1, 3, [10, 11], [some 7, some 8], action, action, [20, 21], []⟩

def parentDelete : Effect Nat Nat Nat := ⟨0, 1, .del, []⟩

def rootChange (r : Nat) : Option (Effect Nat Nat Nat) :=
  if r = 0 then some parentDelete else none

def nullSchema := resolvedSchema [compound .setNull]

def certificate : Derivation Nat := .foreign (.direct 0) 0

theorem compound_null_exact :
    evaluate nullSchema rootChange certificate =
      some (1, ⟨1, 3, .upd, [(10, none), (11, none)]⟩) := by decide

theorem null_forgery_refused :
    verifies nullSchema rootChange certificate 1 ⟨1, 3, .upd, [(10, none)]⟩ = false ∧
    verifies nullSchema rootChange certificate 1
      ⟨1, 3, .upd, [(10, none), (11, none), (12, some 9)]⟩ = false ∧
    verifies nullSchema rootChange certificate 1
      ⟨1, 3, .upd, [(10, some 9), (11, none)]⟩ = false ∧
    verifies nullSchema rootChange certificate 1 ⟨1, 3, .del, []⟩ = false := by decide

theorem compound_update_exact :
    consequence (compound .cascade) ⟨0, 1, .upd, [(20, some 9)]⟩ =
      some ⟨1, 3, .upd, [(10, some 9), (11, some 8)]⟩ ∧
    consequence (compound .cascade) ⟨0, 1, .upd, [(22, some 9)]⟩ = none := by decide

theorem wrong_incarnation_refused :
    consequence (compound .cascade) ⟨0, 3, .del, []⟩ = none := by decide

/-- A displayed NULL cannot replace a reference's original written value. -/
def deletedValues : List (Nat × Option Nat) := [(10, some 7), (11, some 8), (12, some 9)]

def deleteSchema := resolvedSchema [{ compound .cascade with childValues := deletedValues }]

theorem cascade_delete_keeps_written_values :
    evaluate deleteSchema rootChange certificate = some (1, ⟨1, 3, .del, deletedValues⟩) := by decide

theorem changed_delete_old_refused :
    verifies deleteSchema rootChange certificate 1 ⟨1, 3, .del, deletedValues⟩ = true ∧
    verifies deleteSchema rootChange certificate 1 ⟨1, 3, .del, []⟩ = false ∧
    verifies deleteSchema rootChange certificate 1
      ⟨1, 3, .del, [(10, none), (11, some 8), (12, some 9)]⟩ = false ∧
    verifies deleteSchema rootChange certificate 1
      ⟨1, 3, .del, deletedValues ++ [(13, some 10)]⟩ = false := by decide

def chain := resolvedSchema [compound .cascade,
  { compound .cascade with parent := 1, parentGeneration := 3, child := 2, childGeneration := 5 }]

theorem transitive_cascade :
    evaluate chain rootChange (.foreign certificate 0) = some (2, ⟨2, 5, .del, []⟩) ∧
    evaluate chain (fun _ => none) (.foreign certificate 0) = none := by decide

def twoParents := resolvedSchema [
  { compound .setNull with columns := [10], oldKey := [some 7], parentColumns := [20] },
  { compound .setNull with parent := 2, columns := [11], oldKey := [some 8], parentColumns := [21] }]

def directParents (r : Nat) : Option (Effect Nat Nat Nat) :=
  if r = 0 ∨ r = 2 then some ⟨r, 1, .del, []⟩ else none

def combined : Derivation Nat :=
  .together (.foreign (.direct 0) 0) (.foreign (.direct 2) 0)

theorem coalesced_actions_keep_both_causes :
    evaluate twoParents directParents combined =
      some (1, ⟨1, 3, .upd, [(10, none), (11, none)]⟩) ∧
    roots combined = [0, 2] ∧
    evaluate twoParents (fun r => if r = 2 then none else directParents r) combined = none ∧
    evaluate twoParents (fun r => if r = 0 then none else directParents r) combined = none := by decide

/-- The receiver reconstructs both actions. An otherwise genuine partial
consequence cannot stand in for the complete recorded row patch. -/
def coalescedEvidence (proposed : Effect Nat Nat Nat) :
    Admission.Evidence Nat Nat (Effect Nat Nat Nat) where
  payload _ r := if r = 1 then some proposed else directParents r
  execution _ := {
    direct := fun r => (directParents r).map fun value => ⟨value, []⟩
    origin := fun r => if r = 1 then .foreign combined else .direct
    schema := twoParents }

def coalescedPolicy : Policy Nat Nat Nat Nat where
  table := id
  owned r := decide (r = 1)
  creator := id
  author := id

theorem incomplete_coalescing_refused :
    Admission.check coalescedPolicy
      (coalescedEvidence ⟨1, 3, .upd, [(10, none), (11, none)]⟩) 0 1 = true ∧
    Admission.check coalescedPolicy
      (coalescedEvidence ⟨1, 3, .upd, [(10, none)]⟩) 0 1 = false ∧
    Admission.check coalescedPolicy
      (coalescedEvidence ⟨1, 3, .upd, [(11, none)]⟩) 0 1 = false := by decide

/-- A CHECK on the shared parent may hide an owned dependent. Neither is a
recorded direct edit to that dependent. All causes and values remain in loss. -/
def sharedConflict (fails : Bool) : EntryInputs Nat Nat where
  rows := [0, 1]
  present _ := true
  refs r := if r = 1 then [⟨0, false⟩] else []
  checkFails r := decide (r = 0) && fails
  inDeletedCircle _ := none
  claims _ := []
  rank := id

def state : St Nat Nat Nat where
  gen _ := 1
  genWrite _ _ := some 0
  cell r _ := some r
  lost _ _ _ := none

def policy : Policy Nat Nat Nat Nat where
  table := id
  owned r := decide (r ≠ 0)
  creator := id
  author := id

theorem direct_permissions :
    allowed policy 0 0 = true ∧ allowed policy 0 1 = false ∧ owner policy 1 = 1 ∧
    allowed policy 0 2 = false ∧ owner policy 2 = 2 := by decide

theorem shared_check_loss_allowed :
    (entryView (sharedConflict true)).shown 1 = false ∧
    (lossRecord state (entryView (sharedConflict true)) 1 none).map
      (fun record => (record.values 0, record.cause)) =
        some (some 1, .rules [.foreignKey]) ∧
    (entryView (sharedConflict false)).shown 1 = true ∧
    lossRecord state (entryView (sharedConflict false)) 1 none = none := by decide

/-- SET NULL may change a unique expression and hide an owned rival. -/
def nullUnique (deleted : Bool) : EntryInputs Nat Nat where
  rows := [0, 1, 2]
  present r := decide (r ≠ 0) || !deleted
  refs r := if r = 1 ∧ deleted = false then [⟨0, false⟩] else []
  checkFails _ := false
  inDeletedCircle _ := none
  claims r := if r = 1 then [⟨⟨["coalesce(parent_id, -1)"], none⟩,
    if deleted then 0 else 1, 1, false⟩]
    else if r = 2 then [⟨⟨["coalesce(parent_id, -1)"], none⟩, 0, 2, false⟩] else []
  rank := id

theorem unique_loss_allowed :
    (entryView (nullUnique true)).shown 2 = false ∧
    (lossRecord state (entryView (nullUnique true)) 2 none).map (fun record => record.cause) =
      some (.rules [.unique]) ∧
    (entryView (nullUnique false)).shown 2 = true := by decide

end CovenMerge.Ownership.ForeignKeyExamples
