import CovenStorelogData.RetentionSafety

/-! E4 and D7: pending_entries is local, derived metadata. It is excluded from
snapshot/fingerprint identity but included in the built-in live query. -/
namespace CovenStorelogData.LostValues
open CovenStorelogData.RetentionSafety

inductive Cause where
  | write (id : Nat)
  | deletedCircle (entry : Nat)
  | schemaChange (version : Nat)
  | otherRule
  deriving DecidableEq, Repr

structure Value where
  row : Nat
  setters : List Nat
  values : List Nat
  cause : Cause
  frozen : Bool
  deriving DecidableEq, Repr

structure DerivedLoss where
  value : Value
  present : Need
  deriving DecidableEq, Repr

structure LostValue where
  value : Value
  pendingEntries : List Nat
  deriving DecidableEq, Repr

/-- Sorting uses abstract entry ids; serialization and UUID order are outside
this model. Erasing duplicates cannot lose a distinct dependency. -/
def pending (final : Nat → Bool) (loss : DerivedLoss) : List Nat :=
  ((loss.present.entries.eraseDups).filter (fun e => !final e)).mergeSort (· ≤ ·)

def lostValues (kept final : Nat → Bool) (losses : List DerivedLoss) : List LostValue :=
  (losses.filter fun loss => loss.present.eval kept).map fun loss =>
    ⟨loss.value, pending final loss⟩

def subscribeLostValues := lostValues

theorem pending_exact (final : Nat → Bool) (loss : DerivedLoss) (entry : Nat) :
    entry ∈ pending final loss ↔ entry ∈ loss.present.entries ∧ final entry = false := by
  simp [pending]

theorem all_dependencies_final (final : Nat → Bool) (loss : DerivedLoss)
    (h : ∀ e ∈ loss.present.entries, final e = true) : pending final loss = [] := by
  apply List.eq_nil_iff_forall_not_mem.mpr
  intro e he
  obtain ⟨he, hf⟩ := (pending_exact final loss e).mp he
  simp [h e he] at hf

theorem paired_reads (kept final : Nat → Bool) (losses : List DerivedLoss) :
    lostValues kept final losses = subscribeLostValues kept final losses := rfl

/-- Neither storage-time finality nor local exposure changes the loss identity
used by snapshots or fingerprints. -/
def fingerprintInputs (kept : Nat → Bool) (losses : List DerivedLoss) : List Value :=
  (losses.filter fun loss => loss.present.eval kept).map (·.value)

theorem pending_not_in_fingerprint (kept final : Nat → Bool) (losses : List DerivedLoss) :
    (lostValues kept final losses).map (·.value) = fingerprintInputs kept losses := by
  simp [lostValues, fingerprintInputs, List.map_map]

theorem final_loss_stable (before after final : Nat → Bool)
    (stable : Stable before after final) (loss : DerivedLoss)
    (settled : pending final loss = []) : loss.present.eval before = loss.present.eval after := by
  apply dependency_agreement
  intro e he
  apply stable
  cases hf : final e
  · have hp : e ∈ pending final loss := (pending_exact final loss e).mpr ⟨he, hf⟩
    simp [settled] at hp
  · rfl

/-- Live queries notify on a changed current result; callbacks already seen
are not undone. A finality-only commit can therefore deliver a new result. -/
def notify (before after : List LostValue) : List (List LostValue) :=
  if before = after then [] else [after]

def circleLoss : DerivedLoss :=
  ⟨⟨7, [0], [42], .deletedCircle 3, false⟩, .kept 3⟩

example :
    lostValues (fun _ => true) (fun _ => false) [circleLoss] =
      [⟨circleLoss.value, [3]⟩] ∧
    notify (lostValues (fun _ => true) (fun _ => false) [circleLoss])
      (lostValues (fun _ => true) (fun _ => true) [circleLoss]) =
      [[⟨circleLoss.value, []⟩]] := by
  have hp : pending (fun _ => false) circleLoss = [3] := by
    change [3].mergeSort (· ≤ ·) = [3]
    simp
  have hf : pending (fun _ => true) circleLoss = [] := by
    change ([] : List Nat).mergeSort (fun a b => decide (a ≤ b)) = []
    simp
  change [LostValue.mk circleLoss.value (pending (fun _ => false) circleLoss)] =
      [⟨circleLoss.value, [3]⟩] ∧
    notify [⟨circleLoss.value, pending (fun _ => false) circleLoss⟩]
      [⟨circleLoss.value, pending (fun _ => true) circleLoss⟩] = [[⟨circleLoss.value, []⟩]]
  simp [hp, hf, notify]

example : lostValues (fun _ => false) (fun _ => false) [circleLoss] = [] := by decide

end CovenStorelogData.LostValues
