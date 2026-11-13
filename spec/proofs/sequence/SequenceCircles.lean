import SequenceInput
import CovenSequence.CircleLifecycle

/-! Circle histories, as the Rust differential runs them through
`checked_change`, decided by `CircleLifecycle.effect`. One case carries many
histories, each from the same start: the `active` members, every other id in
`people` never added, and no circles. -/
namespace SequenceCircles
open Lean SequenceInput CovenSequence.CircleLifecycle

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

def observe (people circles : List Nat) (s : State Nat Nat) (outcome : Outcome Nat Nat) :
    Json :=
  Json.mkObj [
    ("outcome", outcomeJson outcome),
    ("standing", toJson (people.map fun m => [toJson m, standingJson (s.standing m)])),
    ("circles", toJson (circles.map fun c => [toJson c, match s.circles c with
      | none => Json.null
      | some x => Json.mkObj [("name", toJson x.name),
          ("members", toJson (x.members.mergeSort (· ≤ ·))),
          ("deleted", toJson (s.deleted c))]]))]

/-- Each slot of a history, from slot 1: its outcome and the state after it. -/
def run (people circles : List Nat) (init : State Nat Nat) (history : List (Entry Nat Nat)) :
    List Json :=
  (history.zipIdx.foldl (fun (s, out) (e, i) =>
    let outcome := effect s (i + 1) e
    let next := outcome.after s
    (next, out.push (observe people circles next outcome))) (init, #[])).2.toList

def readCase (j : Json) : Except String Json := do
  let people ← j.getObjValAs? (List Nat) "people"
  let active ← j.getObjValAs? (List Nat) "active"
  let circles ← j.getObjValAs? (List Nat) "circles"
  distinct people "person"
  distinct active "active member"
  distinct circles "circle"
  if active.any (· ∉ people) then throw "an active member is not in people"
  let histories ← (← j.getObjValAs? (List (List Json)) "histories").mapM
    (·.mapM (readEntry people circles))
  let init : State Nat Nat :=
    ⟨fun m => if m ∈ active then .active else .absent, fun _ => none, fun _ => none⟩
  return toJson (histories.map (run people circles init))

end SequenceCircles
