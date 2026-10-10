import CovenStorelog.Model

/-! §18, §19.1, E4–E5. Work and its first unmet condition are committed together.
A failed database commit is a failed read, never a successful empty list. -/
namespace CovenStorelogData.Pending
open CovenStorelog

inductive Subject where
  | write (device number : Nat) | entry (id : Nat)
  | keyCopy (audience : Audience) (key member : Nat)
  | snapshot (id : SnapshotId) | positions (device : Nat)
  | file (device id : Nat) | operation (id : Nat)
  | retention (path : String) | audience (id : Audience)
  | agreement (audience : Audience) (peer : Nat)
  | joinRequest (id : Nat) | connection
  deriving DecidableEq, Repr

inductive Prerequisite where
  | object (path : String) | deviceRegistration (id : Nat)
  | deviceReplacement (id : Nat) | schemaPublication (audience : Audience) (version : Nat)
  | reload (audience : Audience) | ownUploads | positions (device : Nat)
  | entryFinality (entry : Nat) | circleVisible (circle : Nat)
  deriving DecidableEq, Repr

inductive DropReason where
  | landedTooLate | beatenBy (entry : Nat) | targetGone | noAdminLeft
  | notAllowed
  deriving DecidableEq, Repr

inductive Failure where
  | database | disk | keyCustody | crypto | operation
  deriving DecidableEq, Repr

inductive AccessAction where
  | grant | revoke
  deriving DecidableEq, Repr

inductive Reason where
  | missing (path : String) | waits (prerequisite : Prerequisite)
  | keyUnavailable (audience : Audience) (key : Nat)
  | updateRequired (version : Nat) | noStorage
  | storage (retryable : Bool) | refused | invalidPositions
  | dropped (reason : DropReason)
  | providerPending (action : AccessAction) (account : String)
  | pendingOwner (account : String) | accountInUse (account : String)
  | accessRemains (account : String) | deleteAccessKey (key : String)
  | fileUnavailable | disagrees | failed (cause : Failure) | paused | removed
  deriving DecidableEq, Repr

inductive Retry where
  | automatic | afterUpdate | appAction | never
  deriving DecidableEq, Repr

def retry : Reason → Retry
  | .missing _ | .waits _ | .keyUnavailable _ _ | .invalidPositions |
    .providerPending _ _ | .pendingOwner _ | .accountInUse _ | .storage true => .automatic
  | .updateRequired _ | .refused => .afterUpdate
  | .dropped _ | .removed => .never
  | _ => .appAction

structure Condition where
  met : Bool
  reason : Reason
  deriving DecidableEq, Repr

/-- Conditions are in the subject's processing order, not error priority. -/
def first : List Condition → Option Reason
  | [] => none
  | c :: cs => if c.met then first cs else some c.reason

structure Work where
  subject : Subject
  reporter : Nat
  conditions : List Condition
  deriving DecidableEq, Repr

structure Record where
  subject : Subject
  reporter : Nat
  reason : Reason
  deriving DecidableEq, Repr

def records (work : List Work) : List Record :=
  work.filterMap fun w => (first w.conditions).map (Record.mk w.subject w.reporter)

theorem every_pending_subject (work : List Work) (w : Work) (reason : Reason)
    (present : w ∈ work) (waiting : first w.conditions = some reason) :
    ⟨w.subject, w.reporter, reason⟩ ∈ records work := by
  apply List.mem_filterMap.mpr
  exact ⟨w, present, by simp [waiting]⟩

theorem only_first_reason (work : List Work) (record : Record)
    (present : record ∈ records work) :
    ∃ w ∈ work, w.subject = record.subject ∧ w.reporter = record.reporter ∧
      first w.conditions = some record.reason := by
  obtain ⟨w, hw, h⟩ := List.mem_filterMap.mp present
  cases hf : first w.conditions with
  | none => simp [hf] at h
  | some reason =>
      simp only [hf, Option.map_some, Option.some.injEq] at h
      subst record
      exact ⟨w, hw, rfl, rfl, hf⟩

theorem first_unmet (before after : List Condition) (c : Condition)
    (earlier : ∀ x ∈ before, x.met = true) (unmet : c.met = false) :
    first (before ++ c :: after) = some c.reason := by
  induction before with
  | nil => simp [first, unmet]
  | cons x xs ih =>
      simp [first, earlier x List.mem_cons_self,
        ih (fun y hy => earlier y (List.mem_cons_of_mem _ hy))]

/-- An observation replaces this subject's previous observation in the same
transaction as its progress. A task with no unmet conditions emits no record. -/
def observe (work : List Work) (next : Work) : List Work :=
  next :: work.filter (fun w => !(w.subject == next.subject && w.reporter == next.reporter))

theorem observed_wait_visible (work : List Work) (next : Work) (reason : Reason)
    (h : first next.conditions = some reason) :
    ⟨next.subject, next.reporter, reason⟩ ∈ records (observe work next) :=
  every_pending_subject _ _ _ List.mem_cons_self h

theorem latest_reason_replaces_previous (work : List Work) (next : Work) (record : Record)
    (present : record ∈ records (observe work next))
    (subject : record.subject = next.subject) (reporter : record.reporter = next.reporter) :
    first next.conditions = some record.reason := by
  obtain ⟨w, hw, hs, hr, hf⟩ := only_first_reason _ _ present
  rcases List.mem_cons.mp hw with rfl | hw
  · exact hf
  · have hh := (List.mem_filter.mp hw).2
    simp [hs, hr, subject, reporter] at hh

theorem progress_removes_record (work : List Work) (next : Work)
    (advanced : first next.conditions = none) :
    ∀ record ∈ records (observe work next),
      ¬(record.subject = next.subject ∧ record.reporter = next.reporter) := by
  intro record present same
  have := latest_reason_replaces_previous work next record present same.1 same.2
  simp [advanced] at this

inductive Database where
  | available (work : List Work)
  | failed (cause : Failure)
  deriving DecidableEq, Repr

/-- Database failure prevents calls and live queries until reopening (E5). -/
def commit (db : Database) (next : Work) (saved : Except Failure Unit) : Database :=
  match db, saved with
  | .failed cause, _ => .failed cause
  | _, .error cause => .failed cause
  | .available work, .ok () => .available (observe work next)

def query : Database → Except Failure (List Record)
  | .available work => .ok (records work)
  | .failed cause => .error cause

/-- Both paired E4 reads execute this same query against a committed snapshot. -/
def pending := query
def subscribePending := query

theorem paired_reads (db : Database) : pending db = subscribePending db := rfl

theorem save_failure_visible (work : List Work) (next : Work) (cause : Failure) :
    pending (commit (.available work) next (.error cause)) = .error cause := rfl

/-- A reset suppresses prior observations while effective; the observations
remain inputs until finality. Later observations are separate work (§19.1). -/
structure Observation where
  work : Work
  suppressedBy : List Nat
  deriving DecidableEq, Repr

def visible (kept : Nat → Bool) (observations : List Observation) : List Work :=
  (observations.filter fun o => !o.suppressedBy.any kept).map (·.work)

theorem dropped_reset_restores (observations : List Observation)
    (gone : ∀ o ∈ observations, ∀ e ∈ o.suppressedBy, kept e = false) :
    visible kept observations = observations.map (·.work) := by
  unfold visible
  congr 1
  apply List.filter_eq_self.mpr
  intro o ho
  simp only [Bool.not_eq_true']
  exact List.any_eq_false.mpr (fun e he => by simp [gone o ho e he])

end CovenStorelogData.Pending
