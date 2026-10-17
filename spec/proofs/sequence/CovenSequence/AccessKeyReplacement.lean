/-! The owner of an S3 replacement resumes its delivery, claim and undo (§11.4,
§14.3). `due` is supplied by the shared retry schedule. Provider outcomes and
claim validation are inputs; the claim protocol itself is modeled in Operation.
The journal survives a dropped waiter. Admin access is an attempt input, never
part of the durable record, so reopening can require it again. -/
namespace CovenSequence.AccessKeyReplacement

inductive Phase where
  | delivering | recording | undo (rejection : Nat) | finished
  deriving DecidableEq

inductive Failure where
  | network | adminRequired
  deriving DecidableEq

inductive Cause where
  | failed (failure : Failure)
  | rejected (rejection : Nat)
  | cleanup (rejection : Nat) (failure : Failure)
  deriving DecidableEq

structure Journal where
  phase : Phase
  cause : Option Cause
  deriving DecidableEq

def undo (reason : Nat) (admin : Bool) (failure : Option Failure) : Journal :=
  let failure := if admin then failure else some .adminRequired
  match failure with
  | some error => ⟨.undo reason, some (.cleanup reason error)⟩
  | none => ⟨.finished, some (.rejected reason)⟩

/-- One due attempt runs until completion or its next failure. A child claim's
rejection belongs to this owner, which must undo before ending its journal. -/
def resume (j : Journal) (due admin : Bool) (rejection : Option Nat)
    (failure : Option Failure) : Journal :=
  if !due then j else
  match j.phase with
  | .finished => j
  | .undo reason => undo reason admin failure
  | .delivering | .recording =>
    match rejection with
    | some reason => undo reason admin failure
    | none => match failure with
      | some error => ⟨j.phase, some (.failed error)⟩
      | none => ⟨.finished, none⟩

theorem not_due_unchanged (j : Journal) (admin : Bool) (rejection : Option Nat)
    (failure : Option Failure) : resume j false admin rejection failure = j := by
  simp [resume]

theorem due_delivery_finishes (cause : Option Cause) (admin : Bool) :
    resume ⟨.delivering, cause⟩ true admin none none = ⟨.finished, none⟩ := by
  rfl

theorem failed_undo_keeps_both_causes (reason : Nat) (failure : Failure) :
    undo reason true (some failure) = ⟨.undo reason, some (.cleanup reason failure)⟩ := by
  rfl

theorem reopened_undo_needs_admin (reason : Nat) (failure : Option Failure) :
    undo reason false failure = ⟨.undo reason, some (.cleanup reason .adminRequired)⟩ := by
  rfl

theorem undo_finishes_only_after_success (reason : Nat) (admin : Bool)
    (failure : Option Failure) (finished : (undo reason admin failure).phase = .finished) :
    admin = true ∧ failure = none := by
  cases admin <;> cases failure <;> simp_all [undo]

theorem claim_rejection_reaches_undo (cause : Option Cause) (reason : Nat)
    (admin : Bool) (failure : Option Failure) :
    resume ⟨.recording, cause⟩ true admin (some reason) failure = undo reason admin failure := by
  rfl

end CovenSequence.AccessKeyReplacement
