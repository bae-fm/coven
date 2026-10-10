import CovenStorelogData.Operations

namespace CovenStorelogData

open CovenStorelog

/-- Reconstruct effective entries with replay's own functions. Kept no-ops
introduce neither a boundary nor a current key, and create no tombstone. -/
def effectiveEntries (log : Log) (bound : Nat) (result : Result) : List Nat :=
  ((List.range bound).foldl (fun (acc : CovenStorelog.State × List Nat) e =>
    if e ∉ result.kept || alreadyInPlace acc.1 (log e) then acc else
    match effect acc.1 e (log e) with
    | none => acc
    | some next => (next, acc.2 ++ [e])) (CovenStorelog.State.empty, [])).2

/-- snapshot_boundaries.rs::entries skips kept no-ops. Remembering every
once-kept boundary would be a different model. -/
def boundaryEntries (log : Log) (bound : Nat) (result : Result) : List (Nat × SnapshotId) :=
  (effectiveEntries log bound result).filterMap fun e => match (log e).action with
    | .raiseSchema _ snapshot | .reset snapshot => some (e, snapshot)
    | _ => none

/-- The named boundary snapshot is always an admissible selection. Newer
snapshots must cover that entry; their own prefix verification is outside the
model, as it is outside the two imported proofs. -/
def selectedSnapshot (log : Log) (bound : Nat) (result : Result) (a : Audience) :
    Option SnapshotId :=
  ((boundaryEntries log bound result).filter (fun p => p.2.audience == a)).getLast?.map Prod.snd

inductive Disposition where
  | apply
  | migration
  | lost (version : Nat)
  deriving DecidableEq, Repr

structure Header (W : Type) where
  version : Nat
  past : List W
  disposition : Disposition

/-- A snapshot carries its audience's merge state and frozen losses. The
included list denotes its authenticated device-write positions. -/
structure Snapshot (W Col : Type) where
  data : CovenMerge.St W Row Col
  included : List W
  frozen : List (CovenMerge.LossRecord W Row Col)
  rejected : List (W × Exclusion)

/-- A whole-write loss retains the rejected write's identity and its cause;
its values are the changes of that write. -/
structure Reloaded (W Col : Type) where
  data : CovenMerge.St W Row Col
  frozen : List (CovenMerge.LossRecord W Row Col)
  rejected : List (W × Exclusion)

/-- download.rs::accepted: migration markers have no changes, explicit lost
writes are recorded, and boundaries reject old-schema or reset-orphaned parts.
The caller supplies authenticated, readable parts in causal order. -/
def applyDownloaded {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (audience : Audience) (boundaries : List (Boundary W))
    (state : Reloaded W Col) (w : W) : Reloaded W Col :=
  let header := headers w
  let rejection := match header.disposition with
    | .migration => none
    | .lost version => some (.schema version)
    | .apply => (boundaries.find? (excludes w header.version audience header.past)).map Boundary.cause
  match header.disposition, rejection with
  | .migration, _ => state
  | _, some cause => { state with rejected := state.rejected ++ [(w, cause)] }
  | _, none =>
    let projected := CovenMerge.project writes Row.audience (· == audience)
    { state with data := CovenMerge.step projected state.data w }

/-- snapshot_load.rs replaces the selected audience's state before replaying
uncovered stored and queued writes. Migration changes survive only when the
selected snapshot contains them. -/
def reload {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (audience : Audience) (boundaries : List (Boundary W))
    (snapshot : Snapshot W Col) (waiting : List W) : Reloaded W Col :=
  (waiting.filter (· ∉ snapshot.included)).foldl
    (applyDownloaded writes headers audience boundaries)
      ⟨snapshot.data, snapshot.frozen, snapshot.rejected⟩

def selectedBoundaries {W Col : Type} (log : Log) (bound : Nat) (result : Result)
    (snapshots : SnapshotId → Snapshot W Col) : List (Boundary W) :=
  (boundaryEntries log bound result).filterMap fun (e, id) =>
    let included := (snapshots id).included
    match (log e).action with
    | .raiseSchema v _ => some (.schema v id.audience included)
    | .reset _ => some (.reset e id.audience included)
    | _ => none

/-- A completed reload uses the current replay, not the earlier replay which
started downloading. Rust checks expected_entries in the replacement transaction. -/
def reloadSelected {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (log : Log) (bound : Nat) (result : Result) (audience : Audience)
    (snapshots : SnapshotId → Snapshot W Col) (waiting : List W) : Option (Reloaded W Col) :=
  (selectedSnapshot log bound result audience).map fun id =>
    reload writes headers audience (selectedBoundaries log bound result snapshots)
      (snapshots id) waiting

/-- Once the entry sets agree, a replay-selected reload forgets which
boundaries each device had previously kept. This includes frozen and rejected
losses; it assumes the same downloaded/queued write sequence. -/
theorem selected_reload_agrees {W Col : Type} [DecidableEq W]
    (writes : CovenMerge.Writes W Row Col) (headers : W → Header W)
    (log : Log) (bound : Nat) (audience : Audience)
    (snapshots : SnapshotId → Snapshot W Col) (waiting : List W)
    {a b : List Nat} (ca : CausalOrder log a) (cb : CausalOrder log b)
    (same : ∀ e, e ∈ a ↔ e ∈ b) :
    reloadSelected writes headers log bound
        (a.foldl (CovenStorelog.step log bound) (CovenStorelog.initial log bound)).result
        audience snapshots waiting =
      reloadSelected writes headers log bound
        (b.foldl (CovenStorelog.step log bound) (CovenStorelog.initial log bound)).result
        audience snapshots waiting := by
  rw [CovenStorelog.storelog_converges log bound ca cb same]

/-- With the same selected snapshot and admissible effective merge histories,
the order of uncovered parts cannot change any row, cell or merge loss. The
condition concerns effective changes: migration markers are not such changes. -/
theorem snapshot_converges {W Col : Type} [DecidableEq W]
    {writes : CovenMerge.Writes W Row Col} (valid : CovenMerge.Valid writes)
    {covered : W → Prop} (closed : CovenMerge.Closed writes covered)
    {snapshot : CovenMerge.St W Row Col} (correct : CovenMerge.IsSpec writes covered snapshot)
    {a b : List W} (ca : CovenMerge.CausalFrom writes covered a)
    (cb : CovenMerge.CausalFrom writes covered b) (same : ∀ w, w ∈ a ↔ w ∈ b) :
    a.foldl (CovenMerge.step writes) snapshot = b.foldl (CovenMerge.step writes) snapshot := by
  apply CovenMerge.isSpec_unique (CovenMerge.foldl_isSpec valid closed correct ca).1
  exact CovenMerge.isSpec_congr (fun w => by rw [same w])
    (CovenMerge.foldl_isSpec valid closed correct cb).1

end CovenStorelogData
