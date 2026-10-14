import StorelogJson

/-! Execute the checked store-log histories through the shared replay model. -/
open Lean CovenStorelog StorelogJson

private def run (j : Json) : Except String Json := do
  let input ← StorelogJson.read j
  let H := input.history
  let window := input.window
  if (← j.getObj?).contains "checks" then
    let checks ← j.getObjValAs? (Array Nat) "checks"
    return toJson (checks.map fun T => Horizon.recovered H.membership window input.bound T)
  let views ← StorelogJson.checkedViews input
  let received ← StorelogJson.received j input
  let circles := sorted (((List.range input.bound).filterMap fun i => match (H.log i).action with
    | .makeCircle c _ _ => some c
    | _ => none).eraseDups)
  pure (toJson (received.map fun subset =>
    let r := ReplayPolicy.replay H.membership window input.bound input.bound
      (entrySet subset) views
    let causes := circles.filterMap fun c =>
      (CurrentReplay.deletionCause H.log views r c).map fun e => [c, e]
    let late := (List.range input.bound).filter fun e =>
      e ∈ subset && Finality.tooLate H.membership window input.bound (fun _ => true) e
    project (CurrentReplay.finish r) causes late))

def main : IO UInt32 := do
  let stdin ← IO.getStdin
  let stdout ← IO.getStdout
  let input ← stdin.readToEnd
  let output := do
    let j ← Json.parse input
    let histories ← j.getObjValAs? (Array Json) "histories"
    let results ← histories.mapM run
    pure (toJson results)
  match output with
  | .ok result => stdout.putStrLn result.compress; pure 0
  | .error err =>
    (← IO.getStderr).putStrLn err
    pure 1
