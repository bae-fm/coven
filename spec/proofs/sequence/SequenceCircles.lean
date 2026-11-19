import SequenceInput
import CovenSequence.CircleLifecycle
import CovenSequence.RemovedReload

/-! Circle histories, as the Rust differential runs them through
`checked_change`, decided by `CircleLifecycle.effect`. One case carries many
histories, each from the same start: the `active` members, every other id in
`people` never added, and no circles. Each slot also shows what a device of
each member `devices` lists holds after it (`ownStep`), and what such a device
installs when it reloads to that slot from history's states
(`RemovedReload.reload`). -/
namespace SequenceCircles
open Lean SequenceInput CovenSequence.CircleLifecycle CovenSequence.RemovedReload

def readEntry (people circles : List Nat) (j : Json) : Except String (Entry Nat Nat) := do
  let person := fun (key : String) => do
    let m ← nat j key
    if m ∉ people then throw s!"{key} {m} is not in people"
    return m
  match ← j.getObjValAs? String "kind" with
  | "removeMember" => return .removeMember (← person "member")
  | "admit" | "recover" =>
    let kind ← j.getObjValAs? String "kind"
    let c ← nat j "circle"
    if c ∉ circles then throw s!"circle {c} is not in circles"
    let author ← person "author"
    let member ← person "member"
    let name ← j.getObjValAs? String "name"
    return if kind == "admit" then .admit author member c name else .recover author member c name
  | kind =>
    let author ← person "author"
    let c ← nat j "circle"
    if c ∉ circles then throw s!"circle {c} is not in circles"
    let change ← match kind with
      | "create" => pure (.create c (← j.getObjValAs? String "name"))
      | "rename" => pure (.rename c (← j.getObjValAs? String "name"))
      | "delete" => pure (.delete c)
      | "add" => pure (.add c (← person "member"))
      | "remove" => pure (.remove c (← person "member"))
      | _ => throw s!"unknown circle entry {kind}"
    return .circle author change

def outcomeJson : Outcome Nat Nat → Json
  | .refused .notAllowed => "notAllowed"
  | .refused .goneCircle => "goneCircle"
  | .refused .goneMember => "goneMember"
  | .unchanged => "unchanged"
  | .applied _ => "applied"

def standingJson : Standing → Json
  | .absent => "absent"
  | .active => "active"
  | .removed => "removed"

/-- The state's `standing` and `circles` fields. -/
def stateFields (people circles : List Nat) (s : State Nat Nat) : List (String × Json) := [
  ("standing", toJson (people.map fun m => [toJson m, standingJson (s.standing m)])),
  ("circles", toJson (circles.map fun c => [toJson c, match s.circles c with
    | none => Json.null
    | some x => Json.mkObj [("name", toJson x.name),
        ("members", toJson (x.members.mergeSort (· ≤ ·))),
        ("deleted", toJson (s.deleted c))]]))]

/-- Each member's state as JSON, beside the member. -/
def heldJson (people circles : List Nat) (held : List (Nat × State Nat Nat)) : Json :=
  toJson (held.map fun (m, t) => [toJson m, Json.mkObj (stateFields people circles t)])

/-- One slot: its outcome, history's state after it, the state each device of
`devices`' members holds after it (`ownStep`), and what each installs when it
reloads to it (`reload`). -/
def observe (people circles : List Nat) (s : State Nat Nat) (outcome : Outcome Nat Nat)
    (devices reloads : List (Nat × State Nat Nat)) : Json :=
  Json.mkObj ([("outcome", outcomeJson outcome)] ++ stateFields people circles s ++
    [("devices", heldJson people circles devices), ("reloads", heldJson people circles reloads)])

/-- Each slot of a history, from slot 1: its outcome, the state after it, what
a device of each of `devices` holds after it, and what it installs when it
reloads to it from history's states. -/
def run (people circles devices : List Nat) (init : State Nat Nat)
    (history : List (Entry Nat Nat)) : List Json :=
  let (_, _, steps) := history.zipIdx.foldl (fun (s, held, out) (e, i) =>
    let outcome := effect s (i + 1) e
    let next := outcome.after s
    let held := held.map fun (m, t) => (m, ownStep m t (i + 1) (some e))
    (next, held, out.push (next, outcome, held)))
    (init, devices.map (·, init), #[])
  let states := #[init] ++ steps.map (·.1)
  let historyAt := fun k => states.getD k init
  steps.toList.zipIdx.map fun ((next, outcome, held), i) =>
    observe people circles next outcome held
      (devices.map fun m => (m, reload historyAt (removedIn m) keep (i + 1)))

def readCase (j : Json) : Except String Json := do
  let people ← j.getObjValAs? (List Nat) "people"
  let active ← j.getObjValAs? (List Nat) "active"
  let circles ← j.getObjValAs? (List Nat) "circles"
  let devices ← match j.getObjVal? "devices" with
    | .ok listed => fromJson? listed
    | .error _ => pure []
  distinct people "person"
  distinct devices "device's member"
  distinct active "active member"
  distinct circles "circle"
  if active.any (· ∉ people) then throw "an active member is not in people"
  if devices.any (· ∉ people) then throw "a device's member is not in people"
  let histories ← (← j.getObjValAs? (List (List Json)) "histories").mapM
    (·.mapM (readEntry people circles))
  let init : State Nat Nat :=
    ⟨fun m => if m ∈ active then .active else .absent, fun _ => none, fun _ => none⟩
  return toJson (histories.map (run people circles devices init))

end SequenceCircles
