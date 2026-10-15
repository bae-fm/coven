import StorelogJson
import CovenStorelogData.ReplayEffects
import CovenStorelogData.Snapshots
import CovenStorelogData.CurrentData
import StorelogDataEffects
import PendingJson
import CovenStorelogData.RetentionSafety
import CovenStorelogData.LocalSources
import CovenStorelogData.Retention

/-! Compare replay realization, effective snapshot boundaries and circle
reloads with Rust. The adapter calls the model's functions on checked causal
histories. It also runs the pending adapter and the reload's choice
(`PendingJson`). -/
open Lean CovenStorelog CovenStorelogData

private def run (j : Json) : Except String Json := do
  let input ← StorelogJson.read j
  let H := input.history
  let views ← StorelogJson.checkedViews input
  let received ← StorelogJson.received j input
  let results ← received.mapM fun subset => do
    let raw := ReplayPolicy.replay H.membership input.window input.bound input.bound
      (entrySet subset) views
    let result := CurrentReplay.finish raw
    let kept := (List.range input.bound).filter (· ∈ raw.kept)
    let realized := ReplayEffects.applyKept (ReplayPolicy.realize views)
      H.membership.log kept CovenStorelog.State.empty
    if realized.map (fun state => (CurrentReplay.finish ⟨state, [], []⟩).state) != some result.state then
      throw "kept entries did not realize the replay result"
    let boundaries := boundaryEntries H.membership.log input.bound result
    let selected := (boundaries.map (fun p => p.2.audience)).eraseDups
    let selected := selected.mergeSort fun a b => decide (StorelogJson.audienceNumber a ≤ StorelogJson.audienceNumber b)
    pure (Json.mkObj [
      ("realized", toJson true),
      ("boundaries", toJson (boundaries.map fun (e, id) =>
        Json.arr #[toJson e, toJson (StorelogJson.audienceNumber id.audience), toJson id.number])),
      ("selected", toJson (selected.filterMap fun a =>
        (selectedSnapshot H.membership.log input.bound result a).map fun id =>
          toJson [StorelogJson.audienceNumber a, id.number]))])
  pure (toJson results)

/-- §14.4: after each received batch, exactly the members and circles for
which `CurrentData.needsReload` holds. The compared histories make no raise
or reset, so only returning circle readability decides.

Each batch also folds `CurrentData.circleReplay` over every member and circle
seen so far, with no reload ever committing, and reports the pairs still
reloading. A circle left unreadable is `outside`, so its reload no longer
waits (§19.1). -/
private def reloads (j : Json) : Except String Json := do
  let input ← StorelogJson.read j
  let H := input.history
  let views ← StorelogJson.checkedViews input
  let received ← StorelogJson.received j input
  let state (subset : List Nat) : State :=
    (CurrentReplay.finish (ReplayPolicy.replay H.membership input.window input.bound
      input.bound (entrySet subset) views)).state
  let initial : CurrentData.CircleReader Nat Nat :=
    ⟨⟨CovenMerge.St.init, [], []⟩, [], .available⟩
  let mut before := State.empty
  let mut previous : List Nat := []
  let mut readers : List ((Nat × Nat) × CurrentData.CircleReader Nat Nat) := []
  let mut results := #[]
  for subset in received do
    let after := state subset
    let pairs := after.circles.flatMap fun (c, circle) =>
      (circle.members.eraseDups.filter fun m =>
        CurrentData.needsReload H input.window input.bound previous before after m c).map
        fun m => Json.arr #[toJson m, toJson c]
    let seen := (readers.map (·.1) ++ after.circles.flatMap fun (c, circle) =>
      circle.members.eraseDups.map fun m => (m, c)).eraseDups
    let prior := readers
    readers := seen.map fun (m, c) =>
      let reader := ((prior.find? (·.1 == (m, c))).map (·.2)).getD initial
      ((m, c), CurrentData.circleReplay H input.window input.bound previous before after m c
        reader)
    let waiting := readers.filterMap fun ((m, c), reader) =>
      match reader.status with
      | .reloading _ => some (Json.arr #[toJson m, toJson c])
      | _ => none
    results := results.push (Json.mkObj [
      ("returns", Json.arr pairs.toArray), ("waiting", Json.arr waiting.toArray)])
    before := after
    previous := subset
  pure (toJson results)

/-- §16.5: `RetentionSafety.fileDeletable` per case. `references` is null
when some retained data cannot be read, else the file ids each kind of
retained reference names. -/
private def fileDeletable (j : Json) : Except String Json := do
  let file ← j.getObjValAs? Nat "file"
  let refs := j.getObjValD "references"
  let get (kind : String) : Except String (List Nat) := refs.getObjValAs? (List Nat) kind
  let references : Option RetentionSafety.References ← match refs with
    | .null => pure none
    | _ => do
      let rows ← get "rows"
      let losses ← get "losses"
      let snapshots ← get "snapshots"
      let logs ← get "logs"
      let localRows ← get "localRows"
      let waitingWrites ← get "waitingWrites"
      let uploadQueues ← get "uploadQueues"
      let reversible ← get "reversible"
      pure (some ⟨rows, losses, snapshots, logs, localRows, waitingWrites, uploadQueues,
        reversible⟩)
  pure (toJson (RetentionSafety.fileDeletable file references))

/-- `RetentionSafety.LocalSources`: one user-provided original's record
through a row's life. The circle deletion is id 0 and the deleting write id 1;
`final` makes every id so far final, and the stated rule then cleans up a
hidden row; the implemented one never does. After each event, whether the record is still kept under the
stated rule and under the implemented one; a released record stays released. -/
private def sources (j : Json) : Except String Json := do
  let events ← j.getObjValAs? (Array String) "events"
  let mut row : RetentionSafety.RowState := .live
  let mut kept : List Nat := []
  let mut final : List Nat := []
  let mut stated := true
  let mut implemented := true
  let mut out := #[]
  for event in events do
    match event with
    | "deleteCircle" => row := .hidden 0; kept := 0 :: kept
    | "dropDeletion" => kept := kept.erase 0
    | "final" => final := [0, 1]
    | "deleteRow" =>
      if row != .live && !(row == .hidden 0 && !kept.contains 0) then
        throw "deleting a row that is not current"
      row := .deleted 1; kept := 1 :: kept; final := final.erase 1
    | other => throw s!"unknown source event {other}"
    let keptF := fun e => kept.contains e
    let finalF := fun e => final.contains e
    -- Only the stated rule cleans a hidden row up; Rust never does.
    let cleaned := RetentionSafety.cleanup keptF finalF row
    stated := stated && !RetentionSafety.mayDelete keptF finalF
      (RetentionSafety.localSource 0 (RetentionSafety.sourceNeed cleaned))
    implemented := implemented && !RetentionSafety.mayDelete keptF finalF
      (RetentionSafety.localSource 0 (RetentionSafety.implementedNeed row))
    out := out.push (Json.mkObj [("stated", toJson stated), ("implemented", toJson implemented)])
  return toJson out

/-- §15/§18.1: the listed snapshots of one audience that a retention
evaluation keeps. Each of `stored` has a path `[device, number]` and its
covered writes as `[device, number]` pairs; `pending` numbers this device's
unfinished publications. The model chooses the current newest loadable
snapshot with `preferredSnapshot` and keeps what `pruneSnapshots` keeps. A
one-entry log creates the store and introduces its key, so no boundary or
key decides; a path is numbered device first, as the path order is. -/
private def prunes (j : Json) : Except String Json := do
  let own ← j.getObjValAs? Nat "own"
  let pair (p : Json) : Except String (Nat × Nat) := do
    match (← p.getArr?).toList with
    | [a, b] => pure (← a.getNat?, ← b.getNat?)
    | _ => throw "expected a pair"
  let number (p : Nat × Nat) : Nat := p.1 * 2 ^ 32 + p.2
  let stored ← (← j.getObjValAs? (Array Json) "stored").toList.mapM fun s => do
    let path ← pair (← s.getObjVal? "path")
    let writes ← (← s.getObjValAs? (Array Json) "writes").toList.mapM pair
    let snapshot : StoredSnapshot Nat Nat := StoredSnapshot.mk ⟨.store, number path⟩ path.1
      ⟨0, .store⟩ [] ⟨CovenMerge.St.init, writes.map number, [], []⟩
    pure (path, snapshot)
  let pending := (← j.getObjValAs? (List Nat) "pending").map fun n =>
    (⟨.store, number (own, n)⟩ : SnapshotId)
  let log : Log := fun _ => ⟨0, own, [], .create ""⟩
  let result : Result := ⟨State.empty, [0], []⟩
  let snapshots := stored.map (·.2)
  let kept := match preferredSnapshot (snapshotCandidates log 1 result .store snapshots) with
    | none => snapshots
    | some chosen => pruneSnapshots log 1 result own pending chosen snapshots
  pure (toJson ((stored.filter fun (_, s) => kept.any (·.id == s.id)).map fun (p, _) =>
    Json.arr #[toJson p.1, toJson p.2]))

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let output := do
    let j ← Json.parse input
    if let .ok cases := j.getObjVal? "files" then
      return toJson (← (← cases.getArr?).mapM fileDeletable)
    if let .ok cases := j.getObjVal? "prunes" then
      return toJson (← (← cases.getArr?).mapM prunes)
    match j.getObjVal? "reloads", j.getObjVal? "circles", j.getObjVal? "effects",
        j.getObjVal? "pending", j.getObjVal? "choices", j.getObjVal? "sources" with
    | .ok histories, _, _, _, _, _ => toJson <$> (← histories.getArr?).mapM reloads
    | _, .ok circles, _, _, _, _ => toJson <$> (← circles.getArr?).mapM StorelogDataEffects.circles
    | _, _, .ok effects, _, _, _ => toJson <$> (← effects.getArr?).mapM StorelogDataEffects.run
    | _, _, _, .ok cases, _, _ => toJson <$> (← cases.getArr?).mapM PendingJson.run
    | _, _, _, _, .ok cases, _ => toJson <$> (← cases.getArr?).mapM PendingJson.choice
    | _, _, _, _, _, .ok cases => toJson <$> (← cases.getArr?).mapM sources
    | .error _, .error _, .error _, .error _, .error _, .error _ =>
        let histories ← j.getObjValAs? (Array Json) "histories"
        toJson <$> histories.mapM run
  match output with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error err => (← IO.getStderr).putStrLn err; pure 1
