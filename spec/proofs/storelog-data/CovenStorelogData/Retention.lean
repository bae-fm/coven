import CovenStorelogData.EntryEffects

namespace CovenStorelogData

open CovenStorelog

/-- A checked snapshot prefix plus its immutable contents. `number` in the
imported SnapshotId abstracts the complete path, ordered lexicographically.
The included list represents every position covered, without duplicates. -/
structure StoredSnapshot (W Col : Type) where
  id : SnapshotId
  uploader : Nat
  key : Key
  entries : List Nat
  contents : Snapshot W Col

/-- snapshot_boundaries.rs::allows does not require a snapshot's former
boundaries to remain kept; only the latest current boundary constrains it. -/
def allowsSnapshot {W Col : Type} (log : Log) (bound : Nat) (result : Result)
    (s : StoredSnapshot W Col) : Bool :=
  match ((boundaryEntries log bound result).filter
      (fun p => p.2.audience == s.id.audience)).getLast? with
  | none => true
  | some (e, id) => s.id == id || e ∈ s.entries

/-- snapshot_catalog.rs chooses only keys named by kept entries, including
kept no-ops. Dropped-removal keys still open writes, but not snapshots. -/
def snapshotKeyAllowed {W Col : Type} (log : Log) (result : Result)
    (s : StoredSnapshot W Col) : Bool :=
  s.key ∈ result.kept.flatMap (introduced log)

def snapshotCandidates {W Col : Type} (log : Log) (bound : Nat) (result : Result)
    (audience : Audience) (stored : List (StoredSnapshot W Col)) : List (StoredSnapshot W Col) :=
  stored.filter (fun s => s.id.audience == audience && snapshotKeyAllowed log result s &&
    allowsSnapshot log bound result s)

/-- The first catalog item: greatest coverage, then lexicographically first
path. Folding this comparison avoids modelling the catalog's sorting internals. -/
def preferredSnapshot {W Col : Type} (candidates : List (StoredSnapshot W Col)) :
    Option (StoredSnapshot W Col) :=
  candidates.foldl (fun best next => match best with
    | none => some next
    | some old => if next.contents.included.length > old.contents.included.length ||
        (next.contents.included.length == old.contents.included.length &&
          next.id.number < old.id.number) then some next else some old) none

/-- snapshot_retention.rs pins unfinished local publications and *currently*
effective boundaries, and deletes only this device's superseded snapshots.
The chosen prefix need not have a readable schema: retention reads prefixes.
The caller has checked the audience is currently readable. -/
def pruneSnapshots {W Col : Type} [DecidableEq W]
    (log : Log) (bound : Nat) (result : Result) (own : Nat)
    (pending : List SnapshotId) (chosen : StoredSnapshot W Col)
    (stored : List (StoredSnapshot W Col)) : List (StoredSnapshot W Col) :=
  let pinned := pending ++ (boundaryEntries log bound result).map Prod.snd
  stored.filter fun s => !(s.id.audience == chosen.id.audience &&
    snapshotKeyAllowed log result s && s.id != chosen.id && s.uploader == own &&
    s.id ∉ pinned && s.contents.included.all (· ∈ chosen.contents.included))

/-- snapshot_boundaries loads every effective boundary's named prefix before
choosing any replacement, even when a newer snapshot could supply the rows. -/
def boundaryInputsAvailable {W Col : Type} (log : Log) (bound : Nat) (result : Result)
    (audience : Audience) (stored : List (StoredSnapshot W Col)) : Bool :=
  (boundaryEntries log bound result).all fun (_, id) =>
    id.audience != audience || stored.any (fun s => s.id == id)

theorem pinned_snapshot_survives {W Col : Type} [DecidableEq W]
    (log : Log) (bound : Nat) (result : Result) (own : Nat)
    (pending : List SnapshotId) (chosen s : StoredSnapshot W Col)
    (stored : List (StoredSnapshot W Col)) (present : s ∈ stored)
    (pinned : s.id ∈ pending ++ (boundaryEntries log bound result).map Prod.snd) :
    s ∈ pruneSnapshots log bound result own pending chosen stored := by
  simp only [pruneSnapshots, List.mem_filter]
  exact ⟨present, by simp [pinned]⟩

/-- Device-log retention: coverage counts consumed positions, including
skipped/excluded writes. Removal changes who must have posted, not coverage.
`allPosted` and `aged` are the two §15 alternatives, not entry finality. -/
def mayDeleteWrite {W Col : Type} [DecidableEq W] (w : W) (audiences : List Audience)
    (coverage : List (StoredSnapshot W Col)) (allPosted aged : Bool) : Bool :=
  audiences.all (fun a => coverage.any (fun s => s.id.audience == a &&
    w ∈ s.contents.included)) && (allPosted || aged)

/-- database_file_retention.rs reads synced/merge rows, not frozen losses.
`filesOf` reads uploaded references from the retained row cells. Encrypted or
invalid retained objects prevent absence from being established at all. -/
def uploadedReferences {W Col : Type} (rows : List Row) (columns : List Col)
    (fileOf : W → Option Nat) (s : Snapshot W Col) : List Nat :=
  rows.flatMap fun row => columns.filterMap fun col =>
    (s.data.cell row col).bind fileOf

def mayDeleteFile (file : Nat) (localReferences waitingReferences : List Nat)
    (retainedReferences : Option (List Nat)) : Bool :=
  match retainedReferences with
  | none => false
  | some references => file ∉ localReferences && file ∉ waitingReferences && file ∉ references

/-- Purging physical history cannot be inverted from replay alone: if two
different originals have the same retained representation, a deterministic
restorer cannot reconstruct both. This is information loss, not scheduling. -/
theorem no_inverse_of_erasure {α β : Type} (forget : α → β) (restore : β → α)
    (a b : α) (different : a ≠ b) (erased : forget a = forget b) :
    ¬ (restore (forget a) = a ∧ restore (forget b) = b) := by
  rintro ⟨ha, hb⟩
  exact different (ha.symm.trans ((congrArg restore erased).trans hb))

end CovenStorelogData
