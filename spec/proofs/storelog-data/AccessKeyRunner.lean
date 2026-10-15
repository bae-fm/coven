import StorelogJson
import CovenStorelogData.AccessKeys

/-! Compare Rust's S3 deletion journal with `Access.S3`: replay with admin
replacement authority, the keys each received set requires, and the two
clearing events. Provider deletions use operation-scoped admin access, which
Rust's deleting call requires by its type; reads name the key they concern. -/
open Lean CovenStorelog CovenStorelogData CovenStorelogData.Access.S3

private def sortedKeys (keys : List String) : List String :=
  keys.mergeSort (fun a b => decide (a ≤ b))

private def failure : DeleteFailure → String
  | .adminRequired => "adminRequired"
  | .deleteFailed _ => "deleteFailed"
  | .observationMissing => "observationMissing"
  | .wrongKey => "wrongKey"
  | .stillPresent => "stillPresent"
  | .observationFailed _ => "observationFailed"

private def readObservation (j : Json) : Except String (Option Access.S3.Observation) := do
  match j with
  | .null => pure none
  | _ =>
    let key ← j.getObjValAs? String "key"
    let reply : Except Pending.Reason Access.S3.Presence ←
      match ← j.getObjValAs? String "reply" with
      | "absent" => pure (Except.ok .absent)
      | "present" => pure (Except.ok .present)
      | "failed" => pure (Except.error (.storage .byPerson))
      | other => throw s!"unknown read reply {other}"
    pure (some ⟨key, reply⟩)

private def readEvent (bound : Nat) (j : Json) : Except String Event := do
  match j.getObjVal? "entries", j.getObjVal? "confirm", j.getObjVal? "deletion" with
  | .ok entries, _, _ =>
      let entries ← fromJson? (α := List Nat) entries
      if entries.any (· ≥ bound) then throw "received entry outside history"
      pure (.entries entries)
  | _, .ok key, _ => pure (.confirmAccessKeyDeleted (← fromJson? key))
  | _, _, .ok deletion =>
      let key ← deletion.getObjValAs? String "key"
      let deleted : Except Pending.Reason Unit :=
        if ← deletion.getObjValAs? Bool "deleted" then .ok () else .error (.storage .byPerson)
      pure (.providerDeletion key true deleted (← readObservation (← deletion.getObjVal? "observation")))
  | _, _, _ => throw "unknown event"

private def run (j : Json) : Except String Json := do
  let input ← StorelogJson.read j
  let H := input.history
  let events ← (← j.getObjValAs? (Array Json) "events").mapM (readEvent input.bound)
  let mut journal := Journal.empty
  let mut results := #[]
  for event in events do
    if let .entries incoming := event then
      let received := (journal.received ++ incoming).eraseDups
      for e in received do
        if !(H.log e).past.all (· ∈ received) then throw "received set is not causally closed"
    let (next, reply) := applyEvent H input.window input.bound journal event
    journal := next
    let reply := match reply with
      | .ok () => "ok"
      | .error why => failure why
    results := results.push (Json.mkObj [
      ("pending", toJson (sortedKeys (keys journal))), ("reply", toJson reply)])
  pure (toJson results)

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let output := do
    let j ← Json.parse input
    let histories ← j.getObjValAs? (Array Json) "histories"
    toJson <$> histories.mapM run
  match output with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error err => (← IO.getStderr).putStrLn err; pure 1
