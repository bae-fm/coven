import StorelogJson
import CovenStorelog.CurrentKeys

/-! Execute the checked store-log histories through the shared replay model. -/
open Lean CovenStorelog StorelogJson

private def hashJson (h : CurrentReplay.KeyHash) : Json :=
  toJson (h.toList.map (·.toNat))

/-- C12 `receivedIntroductions` and `hashFor` for one received set. -/
private def introductionsJson (input : StorelogJson.Input) (subset : List Nat) : Json :=
  let is := CurrentKeys.receivedIntroductions input.history input.window input.bound
    (entrySet subset)
  let identities := (is.map (·.identity)).eraseDups
  Json.mkObj [
    ("introductions", toJson (is.map fun i => Json.arr #[toJson i.entry,
      toJson (audienceNumber i.audience), toJson i.key.id, hashJson i.key.keyHash])),
    ("hashes", toJson (identities.map fun k => Json.arr #[toJson (audienceNumber k.audience),
      toJson k.id, match CurrentKeys.hashFor is k with
        | some h => hashJson h
        | none => Json.null]))]

private def readBytes (j : Json) (name : String) : Except String CurrentReplay.KeyBytes := do
  let bytes ← j.getObjValAs? (Array Nat) name
  if bytes.any (· > 255) then throw "key bytes must contain 32 bytes"
  let bytes := bytes.map UInt8.ofNat
  if h : bytes.size = 32 then pure ⟨bytes, h⟩ else throw "key bytes must contain 32 bytes"

private def readKeyId (j : Json) : Except String CurrentKeys.KeyId := do
  pure ⟨← readAudience j, ← j.getObjValAs? Nat "key"⟩

private def readCopy (j : Json) : Except String CurrentKeys.Copy := do
  let opened ← match j.getObjVal? "opened" with
    | .ok .null | .error _ => pure none
    | .ok payload => pure (some ⟨← readKeyId payload, ← readBytes payload "bytes"⟩)
  pure ⟨← readKeyId j, ← j.getObjValAs? Nat "recipient", opened⟩

/-- C12 `acceptedCopy` with the identity hash: each copy's accepted payload. -/
private def copiesJson (input : StorelogJson.Input) (self : Nat)
    (copies : Array CurrentKeys.Copy) (subset : List Nat) : Json :=
  let is := CurrentKeys.receivedIntroductions input.history input.window input.bound
    (entrySet subset)
  toJson (copies.map fun copy => match CurrentKeys.acceptedCopy id is self copy with
    | some p => Json.arr #[toJson (audienceNumber p.key.audience), toJson p.key.id,
        hashJson p.bytes]
    | none => Json.null)

private def run (j : Json) : Except String Json := do
  let input ← StorelogJson.read j
  let H := input.history
  let window := input.window
  if (← j.getObj?).contains "checks" then
    let checks ← j.getObjValAs? (Array Nat) "checks"
    return toJson (checks.map fun T => Horizon.recovered H.membership window input.bound T)
  if (← j.getObj?).contains "copies" then
    let _ ← StorelogJson.checkedViews input
    let received ← StorelogJson.received j input
    let copies ← (← j.getObjValAs? (Array Json) "copies").mapM readCopy
    let self ← j.getObjValAs? Nat "self"
    return toJson (received.map (copiesJson input self copies))
  if (← j.getObj?).contains "introductions" then
    let _ ← StorelogJson.checkedViews input
    let received ← StorelogJson.received j input
    return toJson (received.map (introductionsJson input))
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
