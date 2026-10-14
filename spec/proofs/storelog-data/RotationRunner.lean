import StorelogJson
import CovenStorelogData.KeyPhases
import CovenStorelogData.Operations

/-! Exercise publication phases and first-send selection on the Rust test's
actual reserved rotation and its recorded past. -/
open Lean CovenStorelog CovenStorelogData

private def phaseNumber : RotationPhase → Nat
  | .prepare => 0
  | .copies => 1
  | .entry => 2
  | .replay => 3
  | .received => 4
  | .finish => 5

private def run (j : Json) : Except String Json := do
  let input ← StorelogJson.read j
  let _ ← StorelogJson.checkedViews input
  let received ← StorelogJson.received j input
  let some before := received[0]? | throw "missing recorded past"
  let e ← StorelogJson.nat j "rotation"
  if e ≥ input.bound then throw "rotation outside history"
  let who ← StorelogJson.nat j "self"
  let device ← StorelogJson.nat j "device"
  let audience ← StorelogJson.readAudience j
  let held ← j.getObjValAs? (List Nat) "held"
  let key (e : Nat) : Except String KeySelection.Key := do
    if e ≥ input.bound then throw "key outside history"
    let some (a, k) := (input.history.log e).action.introduction | throw "not an introduction"
    pure ⟨e, a, k⟩
  let custody ← held.mapM key
  let raw ← j.getObjValAs? (List (List Nat)) "copies"
  let copies ← raw.mapM fun pair => do
    let [entry, recipient] := pair | throw "copy needs introduction and recipient"
    pure (← key entry, recipient)
  let p : KeyPhases.CaughtUp := ⟨before, ⟨custody, copies⟩⟩
  let mut phase := some RotationPhase.prepare
  let mut rows := #[]
  for _ in List.range 7 do
    let confirmed := phase == some .received || phase == some .finish || phase.isNone
    let receipt : Except Pending.Reason Unit :=
      if confirmed then .ok () else .error (.dropped .notAllowed)
    let rotated := KeyPhases.rotate input.history input.window input.bound who device p e
      (if phase == some .prepare || phase == some .copies then .error (.dropped .notAllowed)
       else .ok ()) receipt
    let pass := match rotated with | .ok next => next | .error _ => p
    let selected := (KeyPhases.firstSend input.history input.window input.bound who audience pass []).map
      (fun attempt => attempt.key.key.id)
    rows := rows.push (Json.mkObj [("step", toJson (phase.map phaseNumber)),
      ("selected", toJson selected)])
    phase := phase.bind (rotationStep true)
  pure (toJson rows)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let cases ← (← Json.parse input).getArr?
    toJson <$> cases.mapM run
  match result with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
