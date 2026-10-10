import CovenStorelogData.Convergence
import CovenStorelogData.Keys
import CovenStorelogData.Snapshots

/-! Stored effects outside replay. See ../../storelog-data.md for the source
correspondence and the distinction between current key selection and custody. -/

namespace CovenStorelogData

open CovenStorelog

/-- store_log_keys.rs::needed includes dropped removals, but not dropped
creations. Acquiring a copy never removes material already in custody. -/
def neededKeys (log : Log) (result : Result) : List Key :=
  (result.kept ++ result.dropped.filter (fun e => isRemoval (log e).action))
    |>.flatMap (introduced log)

def acquireKeys (log : Log) (result : Result) (copies : Copies)
    (member device : Nat) (held : List Key) : List Key :=
  if !running result.state member device then held else
    held ++ (neededKeys log result).filter
      (fun k => (k, member) ∈ copies && k ∉ held)

theorem custody_persists (log : Log) (result : Result) (copies : Copies)
    (member device : Nat) (held : List Key) (key : Key) (h : key ∈ held) :
    key ∈ acquireKeys log result copies member device held := by
  simp only [acquireKeys]
  split
  · exact h
  · exact List.mem_append_left _ h

/-- The effective introductions select current keys. A kept no-op does not
introduce a current key (effects.rs and §11). Historical custody is separate. -/
def currentKeys (log : Log) (bound : Nat) (result : Result) : List Key :=
  (effectiveEntries log bound result).foldl (fun keys e =>
    (introduced log e).foldl (fun keys k =>
      k :: keys.filter (fun old => old.audience != k.audience)) keys) []

/-- §16.6: a local fact names either owned bytes or the user's original.
Forgetting an original removes its registration, never the user's bytes. -/
inductive Source where
  | owned | original
  deriving DecidableEq, Repr

structure LocalFile where
  row : Row
  source : Source
  name : Nat
  deriving DecidableEq, Repr

/-- file_write.rs::retain_rows deliberately excludes deleted circles even
when their present generation and whole-row loss retain the file reference.
This is the one-row/no-move projection of its identity check. -/
def retainLocalFiles {W Col : Type} (log : Log) (s : State W Col)
    (files : List LocalFile) : List LocalFile :=
  files.filter fun file => decide (s.data.gen file.row % 2 = 1) &&
    match file.row.audience with
    | .store => true
    | .circle c => !deletedCircle log s.log.result c

structure FileDevice (W Col : Type) where
  state : State W Col
  files : List LocalFile

/-- store_log.rs::apply → retain_rows → FileRemovals::finish. Successful
completion deletes the owned bytes whose fact was removed. Re-keeping only
recomputes the row; neither it nor the upload queue recreates the source. -/
def fileEntry {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (log : Log) (bound : Nat)
    (s : FileDevice W Col) (e : Nat) : FileDevice W Col :=
  let next := step writes log bound s.state (.entry e)
  ⟨next, retainLocalFiles log next s.files⟩

theorem forgotten_file_stays_absent {W Col : Type} (log : Log) (s : State W Col)
    (files : List LocalFile) (file : LocalFile) (absent : file ∉ files) :
    file ∉ retainLocalFiles log s files := by
  intro h
  exact absent (List.mem_filter.mp h).1

/-- §8.4 permits a local child with ON DELETE CASCADE. SQL materialization
deletes that child when its synced parent is hidden, and cannot reconstruct it.
Local tables are intentionally outside the synced convergence guarantee. -/
def cascadeLocal {W Col K : Type} [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (log : Log) (s : State W Col) (parents : List Row) : List Row :=
  parents.filter (observe schema writes log s).view.shown

/-- Provider grants exist before add-member/set-access records. §13 and
operation_steps.rs revoke every recorded access, even in dropped entries. -/
def recordedAccess (entry : Entry) : Option (Nat × String) :=
  match entry.action with
  | .create access => some (entry.author, access)
  | .addMember m _ access | .setAccess m access => some (m, access)
  | _ => none

def accesses (log : Log) (received : List Nat) (member : Nat) : List String :=
  received.filterMap fun e => (recordedAccess (log e)).bind fun (m, access) =>
    if m = member then some access else none

structure AccessState where
  grants : List String
  notices : List String
  confirmed : List String
  deriving DecidableEq, Repr

/-- Provider access still in use is preserved; an S3 notice is recorded
without that provider-account check. Confirmation is permanent by key id.
The caller is the storage owner/admin running the recorded revocation job. -/
def revokeAccess (s3 : Bool) (log : Log) (received : List Nat) (result : Result)
    (target : Nat) (s : AccessState) : AccessState :=
  let recorded := accesses log received target
  if s3 then { s with notices := s.notices ++ recorded.filter (fun key =>
      key ∉ s.confirmed && key ∉ s.notices) }
  else { s with grants := s.grants.filter (fun access =>
    access ∉ recorded || result.state.access.any (fun (m, a) =>
      CovenStorelog.member result.state m && a == access)) }

def confirmAccess (key : String) (s : AccessState) : AccessState :=
  ⟨s.grants.filter (· != key), s.notices.filter (· != key), key :: s.confirmed⟩

/-- restore_codes.rs installs the S3 credential and synced restore code before
publishing SetAccess. Later replay changes the recorded access, not this local
credential. The string identifies both the credential and its encoded code. -/
structure CredentialDevice where
  log : Device
  credential : String

def credentialEntry (log : Log) (bound : Nat) (s : CredentialDevice) (e : Nat) :
    CredentialDevice :=
  { s with log := CovenStorelog.step log bound s.log e }

/-- snapshot_load.rs clears locally reported stuck objects when committing a
new reset boundary. Peer reports survive; removing the boundary has no inverse. -/
structure ResetJudgments where
  resets : List Nat
  localReports : List Nat
  peerReports : List Nat
  deriving DecidableEq, Repr

def reloadJudgments (log : Log) (bound : Nat) (result : Result)
    (s : ResetJudgments) : ResetJudgments :=
  let resets := (boundaryEntries log bound result).filterMap fun (e, _) =>
    match (log e).action with | .reset _ => some e | _ => none
  { s with resets := resets
           localReports := if resets.all (· ∈ s.resets) then s.localReports else [] }

/-- Rust keeps removed member tombstones where Appendix C erases members.
A failed addition alone creates no tombstone and schedules no revocation. -/
def removedMembers (log : Log) (bound : Nat) (result : Result) : List Nat :=
  (effectiveEntries log bound result).foldl (fun removed e => match (log e).action with
    | .removeMember m _ => m :: removed
    | .addMember m _ _ => removed.filter (· != m)
    | _ => removed) []

/-- A receipt schedules the side effect only on a newly kept removal, or a
new access record for a removed member. Finishing the job does not create an
inverse job when that removal later drops (removal_work in operation_steps.rs).
All accesses in the examples are distinct; no invite also holds the grant. -/
def accessEntry (s3 : Bool) (log : Log) (bound : Nat)
    (before : Device) (s : AccessState) (e : Nat) : Device × AccessState :=
  let after := CovenStorelog.step log bound before e
  let received := (List.range bound).filter after.received
  let targets := after.result.kept.filterMap fun k =>
    if k ∈ before.result.kept then none else match (log k).action with
      | .removeMember m _ => some m
      | _ => none
  let late := (recordedAccess (log e)).toList.filterMap fun (m, _) =>
    if m ∈ removedMembers log bound after.result then some m else none
  (after, (targets ++ late).foldl
    (fun s m => revokeAccess s3 log received after.result m s) s)

/-- Replay-derived projections, including current key ids, cannot depend on
which received entries were temporarily kept. This covers every Action
constructor, not a separately enumerated list of metadata operations. -/
theorem derived_effects_converge {α : Type} (project : Result → α)
    (log : Log) (bound : Nat) {a b : List Nat}
    (ca : CausalOrder log a) (cb : CausalOrder log b)
    (same : ∀ e, e ∈ a ↔ e ∈ b) :
    project (a.foldl (CovenStorelog.step log bound) (CovenStorelog.initial log bound)).result =
      project (b.foldl (CovenStorelog.step log bound) (CovenStorelog.initial log bound)).result := by
  rw [storelog_converges log bound ca cb same]

end CovenStorelogData
