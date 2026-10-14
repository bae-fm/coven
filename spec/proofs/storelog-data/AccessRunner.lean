import CovenStorelogData.Access
import Lean

/-! Compare completed provider revocation attempts with Rust's operation and
pending queries. The adapter calls Access's transitions and pending projection;
the comparison ends at completed attempts, not the scheduling of requests. -/
open Lean CovenStorelogData CovenStorelogData.Pending

private def project (device operation : Nat) (s : Access.Account) : Except String Json := do
  let pending ← (records [Access.work device operation s]).mapM fun record => do
    let .operation id := record.subject | throw "expected operation subject"
    let .accessRemains account := record.reason | throw "expected completed revocation result"
    pure (Json.mkObj [("operation", toJson id), ("reporter", toJson record.reporter),
      ("account", toJson account), ("reason", toJson "accessRemains")])
  pure (Json.mkObj [("actual", toJson s.actual), ("pending", toJson pending)])

private def run (j : Json) : Except String Json := do
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

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let parsed ← Json.parse input
    let cases ← parsed.getArr?
    toJson <$> cases.mapM run
  match result with
  | .ok output => (← IO.getStdout).putStrLn output.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
