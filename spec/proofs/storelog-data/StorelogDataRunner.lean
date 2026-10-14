import StorelogJson
import CovenStorelogData.ReplayEffects
import CovenStorelogData.Snapshots
import StorelogDataEffects

/-! Compare replay realization and effective snapshot boundaries with Rust.
The adapter calls the model's selection functions on checked causal histories. -/
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

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let output := do
    let j ← Json.parse input
    match j.getObjVal? "circles", j.getObjVal? "effects" with
    | .ok circles, _ => toJson <$> (← circles.getArr?).mapM StorelogDataEffects.circles
    | _, .ok effects => toJson <$> (← effects.getArr?).mapM StorelogDataEffects.run
    | .error _, .error _ =>
        let histories ← j.getObjValAs? (Array Json) "histories"
        toJson <$> histories.mapM run
  match output with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error err => (← IO.getStderr).putStrLn err; pure 1
