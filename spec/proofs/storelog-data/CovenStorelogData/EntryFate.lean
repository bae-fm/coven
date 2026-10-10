import CovenStorelogData.Retention

/-! The entry-derived alternative. Immutable inputs are retained; publication,
provider requests and app observations are separate events, not replay effects.
See ../../storelog-data.md#entry-derived-alternative for its contract. -/

namespace CovenStorelogData.EntryFate

open CovenStorelog

/-- Local inputs must be supplied explicitly: neither a file's bytes nor an
unsynced child can be reconstructed from the store log. Snapshot payloads and
pre-migration inputs also remain available even when their entry is dropped. -/
structure Resources (W Col : Type) where
  sources : List LocalFile
  children : List Row
  uploaded : List Nat
  snapshots : List (StoredSnapshot W Col)
  localReports : List Nat
  peerReports : List Nat

/-- Current state exposed by the alternative. Sources and uploaded bytes are
retained even while hidden. Children are a view over retained local rows, never
an irreversible SQLite cascade. Reports are suppressed, never erased. -/
structure View (W Col : Type) where
  data : CovenMerge.Device W Row Col
  sources : List LocalFile
  children : List Row
  uploaded : List Nat
  eligibleSnapshots : List SnapshotId
  localReports : List Nat
  peerReports : List Nat
  desiredAccess : List String

def desiredAccess (result : Result) : List String :=
  result.state.access.filterMap fun (m, access) =>
    if member result.state m then some access else none

/-- A reset suppresses local judgments only while effective. Actual failed
objects and signed peer posts are inputs in their own right, not store entries. -/
def reports (log : Log) (bound : Nat) (result : Result) (original : List Nat) : List Nat :=
  if (boundaryEntries log bound result).any (fun (e, _) =>
      match (log e).action with | .reset _ => true | _ => false)
  then [] else original

def view {W Col K : Type} [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (log : Log) (bound : Nat) (resources : Resources W Col)
    (s : CovenStorelogData.State W Col) : View W Col :=
  let data := observe schema writes log s
  { data
    sources := resources.sources
    children := resources.children.filter data.view.shown
    uploaded := resources.uploaded
    eligibleSnapshots := (resources.snapshots.filter fun snapshot =>
      snapshotKeyAllowed log s.log.result snapshot && allowsSnapshot log bound s.log.result snapshot
    ).map (·.id)
    localReports := reports log bound s.log.result resources.localReports
    peerReports := resources.peerReports
    desiredAccess := desiredAccess s.log.result }

/-- After replay has decided fate, none of these effects reads the dropped
list or receipt history. Keep historical entries to verify authority, but
compute effects from the kept replay alone. This is not a claim that replaying
a history with causal evidence deleted would have the same author views. -/
theorem effects_ignore_dropped {W Col K : Type}
    [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (log : Log) (bound : Nat) (resources : Resources W Col)
    (s : CovenStorelogData.State W Col) (received : EntrySet) (dropped : List Nat) :
    view schema writes log bound resources
      { s with log := ⟨received, { s.log.result with dropped }⟩ } =
    view schema writes log bound resources s := rfl

/-- No circle-delete write occurs in this machine. Every interleaving of entry
receipts and ordinary writes is the existing coupled machine; only its view
touches resources. This theorem includes local children, file sources, uploaded
bytes, snapshot eligibility, reports, rows, losses and desired provider access.
It assumes identical local inputs, readable writes and a fixed schema. -/
theorem views_converge {W Col K : Type} [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (valid : CovenMerge.Valid writes) (log : Log) (bound : Nat)
    (resources : Resources W Col) (authors : W → Author)
    {a b : List (Event W)} (ha : History writes log bound authors a)
    (hb : History writes log bound authors b)
    (entries : ∀ e, e ∈ CovenStorelogData.entries a ↔ e ∈ CovenStorelogData.entries b)
    (data : ∀ w, w ∈ applied a ↔ w ∈ applied b) :
    view schema writes log bound resources
        (a.foldl (CovenStorelogData.step writes log bound) (CovenStorelogData.initial log bound)) =
      view schema writes log bound resources
        (b.foldl (CovenStorelogData.step writes log bound) (CovenStorelogData.initial log bound)) := by
  have he := storelog_converges log bound (history_orders ha).1 (history_orders hb).1 entries
  have hw := CovenMerge.merge_converges valid (history_orders ha).2 (history_orders hb).2 data
  rw [fold_components, fold_components]
  simp only [CovenStorelogData.initial] at *
  rw [he, hw]

/-- Retention of required originals is unconditional. Eligibility may reverse;
that is not permission to erase the inputs of another possible replay. This
deliberately does not claim §3's bounded-storage guarantee. -/
theorem retained_inputs {W Col K : Type} [DecidableEq W] [DecidableEq Col] [DecidableEq K]
    (schema : Schema W Col K) (writes : CovenMerge.Writes W Row Col)
    (log : Log) (bound : Nat) (resources : Resources W Col)
    (s : CovenStorelogData.State W Col) :
    (view schema writes log bound resources s).sources = resources.sources ∧
    (view schema writes log bound resources s).uploaded = resources.uploaded := ⟨rfl, rfl⟩

/-- Every rebuild starts from retained inputs. Snapshot changes and immutable
write headers use the existing boundary/exclusion definitions; a losing raise
cannot leave an accumulated rejected-write list behind. `original` is the
pre-boundary state, not the last materialized view. Payloads must be readable. -/
def rebuild {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (log : Log) (bound : Nat) (result : Result) (audience : Audience)
    (snapshots : SnapshotId → Snapshot W Col) (original : Snapshot W Col)
    (receivedWrites : List W) : Reloaded W Col :=
  let base := match selectedSnapshot log bound result audience with
    | none => original
    | some id => snapshots id
  reload writes headers audience (selectedBoundaries log bound result snapshots) base receivedWrites

/-- Equality concerns the received set, not destructive filtering of causal
evidence: a kept write or entry can have read a dropped entry. -/
theorem rebuild_converges {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (log : Log) (bound : Nat) (audience : Audience)
    (snapshots : SnapshotId → Snapshot W Col) (original : Snapshot W Col)
    (receivedWrites : List W) {a b : List Nat}
    (ha : CausalOrder log a) (hb : CausalOrder log b)
    (same : ∀ e, e ∈ a ↔ e ∈ b) :
    rebuild writes headers log bound
        (a.foldl (CovenStorelog.step log bound) (CovenStorelog.initial log bound)).result
        audience snapshots original receivedWrites =
      rebuild writes headers log bound
        (b.foldl (CovenStorelog.step log bound) (CovenStorelog.initial log bound)).result
        audience snapshots original receivedWrites := by
  rw [storelog_converges log bound ha hb same]

/-- The provider cannot share a transaction with local replay. Grant/revoke
requests carry absolute targets; completion can interleave with a newer replay
or a request from another device. No provider errors are needed here. -/
structure AccessRequest where
  granted : Bool
  deriving DecidableEq, Repr

def requestAccess (result : Result) (who : Nat) : AccessRequest :=
  ⟨member result.state who⟩

def completeAccess (request : AccessRequest) : Bool := request.granted

theorem incompatible_access (a b : Result) (who : Nat)
    (different : member a.state who ≠ member b.state who) :
    ¬ ∃ actual : Bool, actual = member a.state who ∧ actual = member b.state who := by
  rintro ⟨_, ha, hb⟩
  exact different (ha.symm.trans hb)

end CovenStorelogData.EntryFate
