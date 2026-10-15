import CovenStorelogData.Pending

/-! §18, E5: how coven's own operations retry as their failure clears. -/
namespace CovenStorelogData.Pending

/-- How a local failure clears: database, disk and key-custody failures on
their own; a cryptographic or operation-state failure never. -/
def Failure.clears : Failure → Clears
  | .database | .disk | .keyCustody => .byItself
  | .crypto | .operation => .never

/-- An operation's retry class (§18, E5). Coven's own operations (rotations,
maintenance snapshots, required reloads, provider access) have no app call
to retry them: a failure retries as it clears, under the shared backoff when
it clears on its own, after the person's action when a person must act,
and never when nothing clears it; an attempt replay dropped retries under
backoff. The app's own operations keep `retry`, except that an attempt
replay dropped waits for the app. -/
def operationRetry (covenOwned : Bool) (reason : Reason) : Retry :=
  if covenOwned then
    match reason with
    | .failed f => match f.clears with
      | .byItself => .automatic | .byPerson => .appAction | .never => .never
    | .storage .byPerson => .appAction
    | .storage .never => .never
    | .dropped _ => .automatic
    | _ => retry reason
  else
    match reason with
    | .dropped _ => .appAction
    | _ => retry reason

/-- Coven's own operation waits for the app only on a reason a person must
clear: never on a failure that clears on its own, or on its own dropped
attempt. -/
theorem coven_owned_waits_only_on_people (reason : Reason)
    (waits : operationRetry true reason = .appAction) :
    reason = .storage .byPerson ∨
      ((∀ f, reason ≠ .failed f) ∧ (∀ c, reason ≠ .storage c) ∧ (∀ d, reason ≠ .dropped d) ∧
        retry reason = .appAction) := by
  cases reason with
  | failed f => cases f <;> simp_all [operationRetry, Failure.clears]
  | storage c => cases c <;> simp_all [operationRetry, retry]
  | _ => simp_all [operationRetry]

/-- What clears on its own retries under backoff, and nothing that never
clears is retried. -/
theorem coven_owned_retries_as_it_clears (c : Clears) :
    operationRetry true (.storage c) =
      match c with | .byItself => .automatic | .byPerson => .appAction | .never => .never := by
  cases c <;> rfl

end CovenStorelogData.Pending
