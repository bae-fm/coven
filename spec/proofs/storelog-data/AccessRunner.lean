import CovenStorelogData.Access
import Lean

/-! Compare serialized access requests and completed revocation attempts with
Rust's durable journal, actual provider grants and pending projection. -/
open Lean CovenStorelogData CovenStorelogData.Pending

private def project (device operation : Nat) (s : Access.Account) : Except String Json := do
  let pending ← (records [Access.work device operation s]).mapM fun record => do
    let .operation id := record.subject | throw "expected operation subject"
    let .accessRemains account := record.reason | throw "expected completed revocation result"
    pure (Json.mkObj [("operation", toJson id), ("reporter", toJson record.reporter),
      ("account", toJson account), ("reason", toJson "accessRemains")])
  pure (Json.mkObj [("actual", toJson s.actual), ("pending", toJson pending)])

private def runRetained (j : Json) : Except String Json := do
  let account ← j.getObjValAs? String "account"
  let device ← j.getObjValAs? Nat "reporter"
  let operation ← j.getObjValAs? Nat "operation"
  let retained ← j.getObjValAs? (Array Bool) "retained"
  if retained.isEmpty then throw "at least one provider reply required"
  let mut state := Access.adopt false ⟨account, true, true, none⟩
  let mut results := #[]
  for i in List.range retained.size do
    if i > 0 then
      let some retried := Access.retryFailed state | throw "retry requires a failed request"
      state := retried
    let some sending := Access.start state | throw "request cannot start"
    let reply := if retained[i]! then .error (.accessRemains account) else .ok ()
    let some completed := Access.complete sending reply | throw "request cannot complete"
    state := completed
    results := results.push (← project device operation state)
  pure (toJson results)

private def projectPhase (s : Access.Account) : Json :=
  let (phase, target) := match s.request with
    | none => ("idle", Json.null)
    | some (.ready t) => ("ready", toJson t)
    | some (.sending t) => ("sending", toJson t)
    | some (.failed t _) => ("failed", toJson t)
  let reason := match Access.reason s with
    | none => "none"
    | some (.providerPending .grant _) => "grant"
    | some (.providerPending .revoke _) => "revoke"
    | some (.accessRemains _) => "retained"
    | _ => "unexpected"
  Json.mkObj [("intended", toJson s.intended), ("actual", toJson s.actual),
    ("phase", toJson phase), ("target", target), ("reason", toJson reason)]

private def required (state : Option Access.Account) (message : String) : Except String Access.Account :=
  match state with
  | some state => .ok state
  | none => .error message

private def runPhases (j : Json) : Except String Json := do
  let initial ← j.getObjValAs? Bool "initial"
  let events ← j.getObjValAs? (Array String) "events"
  let mut state : Access.Account := ⟨"cat@example.com", initial, initial, none⟩
  let mut results := #[projectPhase state]
  for event in events do
    state ← match event with
      | "grant" => pure (Access.adopt true state)
      | "revoke" => pure (Access.adopt false state)
      | "start" => required (Access.start state) "start needs a ready request"
      | "success" => required (Access.complete state (.ok ())) "completion needs a sent request"
      | "retained" => required (Access.complete state (.error (.accessRemains state.name))) "failure needs a sent request"
      | "retry" => required (Access.retryFailed state) "retry needs a failure"
      | _ => throw "unknown access event"
    results := results.push (projectPhase state)
  pure (toJson results)

private def run (j : Json) : Except String Json :=
  match j.getObjVal? "events" with
  | .ok _ => runPhases j
  | .error _ => runRetained j

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let parsed ← Json.parse input
    let cases ← parsed.getArr?
    toJson <$> cases.mapM run
  match result with
  | .ok output => (← IO.getStdout).putStrLn output.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
