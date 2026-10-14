import CovenStorelogData.Model

namespace CovenStorelogData

open CovenStorelog (Audience)

/-- The row-deletion counterexample selects every odd generation in the circle,
including hidden rows. Entry-only deletion does not author these changes. -/
def deleteChanges {W Col : Type} (st : CovenMerge.St W Row Col) (circle : Nat)
    (row : Row) : Option (CovenMerge.Change Col) :=
  if row.audience = .circle circle ∧ st.gen row % 2 = 1 then
    some ⟨.del, st.gen row, fun _ => false⟩
  else none

inductive Deletion (W : Type) where
  | ready
  | committed (write : Option W)
  | uploaded
  | published (entry : Nat)
  | finished
  | pending
  deriving DecidableEq, Repr

/-- A deletion's write is exactly the present rows selected in its transaction;
no write is allocated when that selection is empty. -/
def IsDeletion {W Col : Type} (writes : CovenMerge.Writes W Row Col)
    (st : CovenMerge.St W Row Col) (circle : Nat) : Option W → Prop
  | none => ∀ r, deleteChanges st circle r = none
  | some w => ∀ r, writes.chg w r = deleteChanges st circle r

/-- The counterexample commits the ordinary row write and the journal together.
The write still exists if publication fails, the operation is abandoned, or
the entry is dropped later. -/
def commitDeletion {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (st : CovenMerge.St W Row Col)
    (circle : Nat) (write : Option W) (_valid : IsDeletion writes st circle write) :
    CovenMerge.St W Row Col × Deletion W :=
  (match write with | none => st | some w => CovenMerge.step writes st w,
   .committed write)

/-- Entry publication must follow storage of the row write. A circle with
no present row has no row write to upload. -/
inductive PublishDeletion {W : Type} : Deletion W → Deletion W → Prop where
  | empty : PublishDeletion (.committed none) .uploaded
  | stored (w : W) : PublishDeletion (.committed (some w)) .uploaded
  | entry (e : Nat) : PublishDeletion .uploaded (.published e)

/-- Polling a dropped attempt restarts at row deletion; permission and target
existence are checked against the new replay before the retry commits.
An already finished journal is absent, so a later drop cannot restart it. -/
def pollDeletion {W : Type} (result : CovenStorelog.Result) : Deletion W → Deletion W
  | .published e =>
      if e ∈ result.kept then .finished
      else if e ∈ result.dropped then .ready
      else .published e
  | state => state

def retryDeletion {W : Type} (s : CovenStorelog.State) (member circle : Nat) : Deletion W :=
  if (CovenStorelog.lookup s.circles circle).isNone then .finished
  else if CovenStorelog.inCircle s circle member then .ready else .pending

/-- Restart changes only the journal, never the already committed data. -/
theorem dropped_delete_keeps_write {W Col : Type} [DecidableEq W]
    (M : CovenMerge.Writes W Row Col) (s : CovenMerge.St W Row Col) (w : W)
    (circle : Nat) (valid : IsDeletion M s circle (some w))
    (r : CovenStorelog.Result) (e : Nat) (dropped : e ∈ r.dropped) (notKept : e ∉ r.kept) :
    ((commitDeletion M s circle (some w) valid).1, pollDeletion (W := W) r (.published e)) =
      (CovenMerge.step M s w, Deletion.ready) := by
  simp [commitDeletion, pollDeletion, dropped, notKept]

/-- Breaking migrations commit the schema and migration write before a
snapshot and its raise. The device-log marker has no row changes (§17.1).
A reset has no preliminary row write; its snapshot precedes the entry. -/
inductive Publication (W : Type) where
  | migration (write : W) (version : Nat) (snapshots : List CovenStorelog.SnapshotId)
  | reset (snapshot : CovenStorelog.SnapshotId)
  deriving DecidableEq, Repr

inductive PublicationEvent (W : Type) where
  | migrate (write : W)
  | uploadSnapshot (snapshot : CovenStorelog.SnapshotId)
  | publish (action : CovenStorelog.Action)
  deriving DecidableEq, Repr

def publicationOrder {W : Type} : Publication W → List (PublicationEvent W)
  | .migration w v snapshots => .migrate w :: snapshots.flatMap fun s =>
      [.uploadSnapshot s, .publish (.raiseSchema v s)]
  | .reset s => [.uploadSnapshot s, .publish (.reset s)]

/-- Operation_step's reset exception: a competing reset settles the request,
instead of authoring a new reset that would override the earlier winner. -/
def resetFinished (result : CovenStorelog.Result) (e : Nat) : Bool :=
  e ∈ result.kept || e ∈ result.dropped

/-- The exact exclusion conditions in write_boundary.rs. -/
inductive Boundary (W : Type) where
  | schema (version : Nat) (audience : Audience) (included : List W)
  | reset (entry : Nat) (audience : Audience) (included : List W)

/-- The recorded cause does not contain a boundary's coverage positions.
Resets record none: they ignore parts without a loss (§19.3). -/
inductive Exclusion where
  | schema (version : Nat)
  deriving DecidableEq, Repr

/-- A raise excludes an uncovered part authored at an older version. -/
def exclusion {W : Type} [DecidableEq W] (w : W) (version : Nat)
    (audience : Audience) : Boundary W → Option Exclusion
  | .schema v a included =>
    if a == audience && version < v && w ∉ included then some (.schema v) else none
  | .reset .. => none

/-- A reset ignores an uncovered part whose author had not read its entry,
even if it read every write the snapshot covers. -/
def ignores {W : Type} [DecidableEq W] (w : W) (audience : Audience)
    (storeRead : List Nat) : Boundary W → Bool
  | .reset e a included => a == audience && w ∉ included && e ∉ storeRead
  | .schema .. => false

theorem reset_publication_order {W : Type} (s : CovenStorelog.SnapshotId) :
    publicationOrder (W := W) (.reset s) = [.uploadSnapshot s, .publish (.reset s)] := rfl

/-- A rotation fixes its entry and copies together, confirms the copies, stores
the entry, then observes replay. A dropped attempt returns to preparation;
only a kept entry permits completion. Crashes preserve this durable phase. -/
inductive RotationPhase where
  | prepare | copies | entry | replay | received | finish
  deriving DecidableEq, Repr

def rotationStep (kept : Bool) : RotationPhase → Option RotationPhase
  | .prepare => some .copies
  | .copies => some .entry
  | .entry => some .replay
  | .replay => some .received
  | .received => some (if kept then .finish else .prepare)
  | .finish => none

theorem dropped_rotation_restarts : rotationStep false .received = some .prepare := rfl

theorem rotation_finish_requires_kept (kept : Bool)
    (h : rotationStep kept .received = some .finish) : kept = true := by
  cases kept <;> simp_all [rotationStep]

end CovenStorelogData
