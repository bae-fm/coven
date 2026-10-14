import StorelogJson
import CovenStorelogData.CurrentData
import CovenStorelogData.EntryFateLive
import CovenStorelogData.CurrentExamples

/-! The database boundary accepts checked entry fates and authenticated write
inputs. Replay itself is compared separately by StorelogDataRunner. -/
namespace StorelogDataEffects
open Lean CovenStorelog CovenStorelogData

private structure InputWrite where
  row : Nat
  generation : Nat
  kind : CovenMerge.Kind
  columns : List Nat
  past : List Nat
  version : Nat
  storeRead : List Nat

private def readWrite (j : Json) : Except String InputWrite := do
  let kind ← match ← j.getObjValAs? String "kind" with
    | "insert" => pure CovenMerge.Kind.ins
    | "update" => pure CovenMerge.Kind.upd
    | "delete" => pure CovenMerge.Kind.del
    | _ => throw "unknown row change"
  pure ⟨← StorelogJson.nat j "row", ← StorelogJson.nat j "generation", kind,
    ← j.getObjValAs? (List Nat) "columns", ← j.getObjValAs? (List Nat) "past",
    ← StorelogJson.nat j "version", ← j.getObjValAs? (List Nat) "storeRead"⟩

private def cause : Exclusion → Json
  | .schema version => Json.mkObj [("schema", toJson version)]

/-- Finite projections inspect the model's actual generations, setters, losses,
rejected writes and pending observations, including each committed app value. -/
def run (j : Json) : Except String Json := do
  let raw ← j.getObjValAs? (Array Json) "writes"
  let decoded ← raw.mapM readWrite
  let some first := decoded[0]? | throw "data case must contain a write"
  let write (w : Nat) := decoded[w]?.getD first
  let row (key : Nat) : Row := ⟨0, key, .store⟩
  let writes : CovenMerge.Writes Nat Row Nat := {
    ts := id
    past := fun w a => a ∈ (write w).past
    chg := fun w r => if r == row (write w).row then
      some ⟨(write w).kind, (write w).generation, fun c => c ∈ (write w).columns⟩ else none }
  let headers (w : Nat) : Header Nat :=
    ⟨(write w).version, (write w).past, .apply, (write w).storeRead⟩
  let snapshot (covered : List Nat) : Snapshot Nat Nat :=
    ⟨covered.foldl (CovenMerge.step writes) CovenMerge.St.init, covered, [], []⟩
  let originals ← j.getObjValAs? (List Nat) "original"
  let original := snapshot originals
  let boundaries ← j.getObjValAs? (Array Json) "boundaries"
  let actions ← boundaries.mapM fun b => do
    let number ← StorelogJson.nat b "snapshot"
    match ← b.getObjValAs? String "kind" with
    | "reset" => pure (Action.reset ⟨.store, number⟩)
    | "schema" => pure (Action.raiseSchema (← StorelogJson.nat b "version") ⟨.store, number⟩)
    | _ => throw "unknown boundary"
  let log : Log := fun e => ⟨0, e, [], if e = 0 then .create "owner" else
    actions[e - 1]?.getD (.create "owner")⟩
  let sources ← (← j.getObjValAs? (Array Json) "snapshots").mapM fun s => do
    pure (← StorelogJson.nat s "number", snapshot (← s.getObjValAs? (List Nat) "covered"))
  let snapshots (id : SnapshotId) : Snapshot Nat Nat :=
    ((sources.toList.find? (fun s => s.1 == id.number)).map Prod.snd).getD original
  let rows := (decoded.toList.map (·.row)).eraseDups.mergeSort (· ≤ ·)
  let columns := (decoded.toList.flatMap (·.columns)).eraseDups.mergeSort (· ≤ ·)
  let cases ← j.getObjValAs? (Array Json) "cases"
  let mut trace : List EntryFate.AppValue := []
  let mut result := #[]
  for c in cases do
    let kept ← c.getObjValAs? (List Nat) "kept"
    let kept := 0 :: kept
    let some state := ReplayEffects.applyKept (fun s e entry => effect s e entry)
      log kept State.empty | throw "invalid checked effects"
    let replay : Result := ⟨state, kept, []⟩
    let received ← c.getObjValAs? (List Nat) "received"
    let rebuilt := EntryFate.rebuild writes headers log (actions.size + 1) replay .store
      snapshots original received
    let observations ← (← c.getObjValAs? (Array Json) "observations").mapM fun o => do
      let subject ← StorelogJson.nat o "subject"
      let resets ← o.getObjValAs? (List Nat) "resets"
      pure (Pending.Observation.mk ⟨.operation subject, 0, [⟨false, .refused⟩]⟩ resets)
    let visible := Pending.records (Pending.visible (· ∈ kept) observations.toList)
    let subjects := visible.filterMap fun r => match r.subject with
      | .operation n => some n | _ => none
    let value : EntryFate.AppValue := ⟨!rebuilt.rejected.isEmpty, subjects, false⟩
    trace := EntryFate.tell trace value
    result := result.push (Json.mkObj [
      ("rows", toJson (rows.map fun r => Json.mkObj [
        ("row", toJson r), ("generation", toJson (rebuilt.data.gen (row r))),
        ("setters", toJson (columns.filterMap fun col =>
          (rebuilt.data.cell (row r) col).map fun w => [col, w])),
        ("losses", toJson (columns.flatMap fun col => received.filterMap fun w =>
          (rebuilt.data.lost (row r) col w).map fun (g, winner) => [col, w, g, winner]))])),
      ("rejected", toJson (rebuilt.rejected.map fun (w, why) => Json.arr #[toJson w, cause why])),
      ("pending", toJson subjects),
      ("trace", toJson (trace.map fun v => Json.arr #[toJson v.lost, toJson v.stuck]))])
  pure (toJson result)

/-- The current-data model's deletion/reversal history, with data arriving
before or after the deletion. Only the merge input changes with write receipt.
A local child exists as a retained resource only when the app saw its parent;
its presence is `EntryFate.View.children`: the retained children whose parent
the current view shows, never an irreversible cascade. -/
def circles (j : Json) : Except String Json := do
  let states ← j.getObjValAs? (Array Json) "states"
  let child ← j.getObjValAs? Bool "children"
  toJson <$> states.mapM fun state => do
    let arrivals ← state.getObjValAs? (List Nat) "arrivals"
    let received ← state.getObjValAs? Bool "write"
    let original := if received then CurrentExamples.original else CovenMerge.St.init
    let data := CurrentData.observe DeliveryExamples.schema DeliveryExamples.writes
      CurrentExamples.circleHistory 30 7 (List.range 5 ++ arrivals) original
    let row := DeliveryExamples.row
    let cause := if (data.losses row none).isSome then
      match CurrentData.circleLossCause CurrentExamples.circleHistory 30 7
        (List.range 5 ++ arrivals) row with
      | some (.deletedCircle entry) => some entry
      | _ => none
      else none
    let children := (if child then [row] else []).filter data.view.shown
    pure (Json.mkObj [("shown", toJson (data.view.shown row)),
      ("generation", toJson (data.merged.gen row)),
      ("setter", toJson ((data.merged.cell row ()).isSome)), ("cause", toJson cause),
      ("child", toJson !children.isEmpty)])

end StorelogDataEffects
