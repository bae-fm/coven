import SequenceInput
import CovenSequence.InviteLifecycle

/-! One invite's events, as the Rust differential drives them, through
`InviteLifecycle`'s steps under `Rules.rust`. Each event takes steps only
when `enabled` allows them, so every run is a path of `Steps`. The pass
takes its steps in the order the Rust's pass does: it applies the slots that
landed for this device, runs the app's admin change, then advances the
invite until the attempt returns. Storage always answers. -/
namespace SequenceInvites
open Lean CovenSequence.InviteLifecycle

/-- Another admin's slot that has landed and that this device applies at
its next pass. -/
inductive Landed where
  | demote
  | stop
  deriving DecidableEq

structure Run where
  world : World
  landed : List Landed
  /-- Each landed once at most. -/
  demoted : Bool
  removed : Bool
  /-- The code has been shown, so the joiner can send its request. -/
  shown : Bool
  sent : Bool

/-- Take the step if it is enabled. -/
def take (c : Config) (w : World) (a : Act) : World × Bool :=
  if enabled Rules.rust c w a then (act Rules.rust c w a, true) else (w, false)

/-- Advance the invite until the pass's attempt returns. `during` are steps
other writers commit while the attempt reads the request: they run between
that read and the write after it. Returns whether they ran. -/
def attempt (c : Config) (during : List Act) : Nat → World → World × Bool
  | 0, w => (w, false)
  | fuel + 1, w =>
    if !enabled Rules.rust c w .read then (w, false) else
    let w := act Rules.rust c w .read
    match w.read with
    | none => (w, false)
    | some r =>
      if w.expired && expires r.stage then attempt c during fuel (take c w .lapse).1 else
      let reading := r.stage == .waiting false && !during.isEmpty
      let w := if reading then during.foldl (fun w a => (take c w a).1) w else w
      let w := match r.stage with
        | .approving => (take c w .bind).1
        | _ => w
      let applies := atRead w r
      let next := (take c w .commit).1
      let moved := next.row.map (·.step) != some r.step
      let goesOn : Bool := applies && moved && match r.stage with
        | .grant | .waiting true | .settled => false
        | .waiting false => c.delivery == .inPerson
        | _ => true
      if goesOn then
        let (w, ran) := attempt c (if reading then [] else during) fuel next
        (w, ran || reading)
      else (next, reading)

/-- A pass, with other writers' steps committed while it reads the request,
or after it when it reads none. -/
def pass (c : Config) (run : Run) (during : List Act := []) : Run := Id.run do
  let after := fun (w : World) => during.foldl (fun w a => (take c w a).1) w
  if run.world.stopped then return { run with world := after run.world }
  let mut w := run.world
  for slot in run.landed do
    w := (take c w (match slot with | .demote => .demote | .stop => .stop)).1
  let run := { run with landed := [] }
  if w.stopped then return { run with world := after w }
  w := (take c w .runOp).1
  let (next, ran) := attempt c during 16 w
  w := if ran then next else after next
  let shown := run.shown || w.row.any fun r => match r.stage with
    | .waiting _ | .approving => true
    | _ => false
  return { run with world := w, shown }

/-- `ok`, `refused`, or `skipped` when the event doesn't apply. -/
def event (c : Config) (run : Run) (name : String) : Except String (Run × String) := do
  let app := fun (a : Act) =>
    let (w, ok) := take c run.world a
    ({ run with world := w }, if ok then "ok" else "refused")
  match name with
  | "pass" => return (pass c run, "ok")
  | "raceCancel" => return (pass c run [.cancel], "ok")
  | "raceExpire" => return (pass c run [.tick, .expiry], "ok")
  | "approve" => return app .approve
  | "decline" => return app .decline
  | "cancel" => return app .cancel
  | "retry" => return app .retry
  | "ack" => return app .ack
  | "ackOp" => return app .ackOp
  | "administer" =>
    if run.world.op.isSome then return (run, "skipped") else return app .administer
  | "send" =>
    if !run.shown || run.sent || run.world.row.isNone then return (run, "skipped")
    let (w, _) := take c run.world .send
    return ({ run with world := w, sent := true }, "ok")
  | "expire" =>
    let w := (take c run.world .tick).1
    return ({ run with world := (take c w .expiry).1 }, "ok")
  | "demote" =>
    if run.demoted then return (run, "skipped")
    return ({ run with landed := run.landed ++ [.demote], demoted := true }, "ok")
  | "remove" =>
    if run.removed then return (run, "skipped")
    return ({ run with landed := run.landed ++ [.stop], removed := true }, "ok")
  | _ => throw s!"unknown invite event {name}"

def listedJson : Option Listed → Json
  | none => .null
  | some .granting => toJson "granting"
  | some (.waiting false) => toJson "waiting"
  | some (.waiting true) => toJson "requested"
  | some .approving => toJson "approving"

def entryJson : Option (Option Outcome) → Json
  | none => .null
  | some none => toJson "running"
  | some (some .landed) => toJson "landed"
  | some (some .rejected) => toJson "rejected"

def retryJson : Option Retry → Json
  | none => .null
  | some .auto => toJson "auto"
  | some .app => toJson "app"
  | some .never => toJson "never"

def observe (w : World) (result : String) : Json :=
  Json.mkObj [
    ("result", toJson result),
    ("invite", listedJson (listed Rules.rust w)),
    ("binding", entryJson w.outcome),
    ("op", entryJson w.op),
    ("pending", retryJson w.pending),
    ("journal", toJson w.row.isSome),
    ("request", toJson w.request),
    ("accepted", toJson ([("approve", Act.approve), ("decline", .decline), ("cancel", .cancel),
      ("retry", .retry), ("ack", .ack), ("ackOp", .ackOp)].filterMap fun (n, a) =>
        if accepted Rules.rust w a then some n else none))]

def readCase (j : Json) : Except String Json := do
  let delivery ← match ← j.getObjValAs? String "delivery" with
    | "sent" => pure Delivery.sent
    | "inPerson" => pure .inPerson
    | _ => throw "unknown delivery"
  let c : Config := ⟨delivery, ← j.getObjValAs? Bool "records"⟩
  let events ← j.getObjValAs? (List String) "events"
  let mut run : Run := ⟨made, [], false, false, false, false⟩
  -- The app's write made the invite; the pass after its commit shows it.
  run := pass c run
  let mut out := #[observe run.world "ok"]
  for name in events do
    let (next, result) ← event c run name
    run := next
    out := out.push (observe run.world result)
  return toJson out

end SequenceInvites
