import CovenStorelogData.Pending
import Lean

/-! §19.1 / E5: Rust records each observed subject's first unmet condition
through its pending store, ends covered write waits when a snapshot load
installs positions, replaces a class of records with each completed listing
(a snapshot listing keeping this device's refusals), and replaces its entry
waits with each completed store-log scan. This adapter runs the same steps
through `Pending.Local.step` and reports `Pending.pending` with each record's
`retry`. Arrays encode subjects, reasons and conditions positionally. -/
namespace PendingJson
open Lean CovenStorelog CovenStorelogData.Pending

private def nat (j : Json) (i : Nat) : Except String Nat := do (← j.getArrVal? i).getNat?
private def str (j : Json) (i : Nat) : Except String String := do (← j.getArrVal? i).getStr?

private def audience (n : Nat) : Audience := if n = 0 then .store else .circle (n - 1)
private def audienceNumber : Audience → Nat | .store => 0 | .circle c => c + 1

def subject (j : Json) : Except String Subject := do
  match ← str j 0 with
  | "write" => pure (.write (← nat j 1) (← nat j 2))
  | "entry" => pure (.entry (← nat j 1) (← nat j 2))
  | "keyCopy" => pure (.keyCopy (audience (← nat j 1)) (← nat j 2) (← nat j 3))
  | "positions" => pure (.positions (← nat j 1))
  | "agreement" => pure (.agreement (audience (← nat j 1)) (← nat j 2))
  | "snapshot" => pure (.snapshot ⟨audience (← nat j 1), ← nat j 2⟩)
  | other => throw s!"unknown pending subject {other}"

def subjectJson : Subject → Json
  | .write device number => Json.arr #["write", toJson device, toJson number]
  | .entry device number => Json.arr #["entry", toJson device, toJson number]
  | .keyCopy a key member =>
      Json.arr #["keyCopy", toJson (audienceNumber a), toJson key, toJson member]
  | .positions device => Json.arr #["positions", toJson device]
  | .agreement a peer => Json.arr #["agreement", toJson (audienceNumber a), toJson peer]
  | .snapshot id => Json.arr #["snapshot", toJson (audienceNumber id.audience), toJson id.number]
  | _ => Json.null

private def prerequisite (j : Json) : Except String Prerequisite := do
  match ← str j 0 with
  | "object" => pure (.object (← str j 1))
  | "deviceRegistration" => pure (.deviceRegistration (← nat j 1))
  | "reload" => pure (.reload (audience (← nat j 1)))
  | "positions" => pure (.positions (← nat j 1))
  | "schemaPublication" => pure (.schemaPublication (audience (← nat j 1)) (← nat j 2))
  | other => throw s!"unknown prerequisite {other}"

private def prerequisiteJson : Prerequisite → Json
  | .object path => Json.arr #["object", toJson path]
  | .deviceRegistration id => Json.arr #["deviceRegistration", toJson id]
  | .reload a => Json.arr #["reload", toJson (audienceNumber a)]
  | .positions device => Json.arr #["positions", toJson device]
  | .schemaPublication a v => Json.arr #["schemaPublication", toJson (audienceNumber a), toJson v]
  | _ => Json.null

private def dropReason : String → Except String DropReason
  | "landedTooLate" => pure .landedTooLate
  | "noAdminLeft" => pure .noAdminLeft
  | "notAllowed" => pure .notAllowed
  | other => throw s!"unknown drop reason {other}"

private def dropJson : DropReason → Json
  | .landedTooLate => "landedTooLate"
  | .noAdminLeft => "noAdminLeft"
  | .notAllowed => "notAllowed"
  | _ => Json.null

private def failure : String → Except String Failure
  | "database" => pure .database
  | "disk" => pure .disk
  | other => throw s!"unknown local failure {other}"

private def failureJson : Failure → Json
  | .database => "database"
  | .disk => "disk"
  | _ => Json.null

def reason (j : Json) : Except String Reason := do
  match ← str j 0 with
  | "missing" => pure (.missing (← str j 1))
  | "waits" => pure (.waits (← prerequisite (← j.getArrVal? 1)))
  | "keyUnavailable" => pure (.keyUnavailable (audience (← nat j 1)) (← nat j 2))
  | "updateRequired" => pure (.updateRequired (← nat j 1))
  | "noStorage" => pure .noStorage
  | "storage" => pure (.storage (← (← j.getArrVal? 1).getBool?))
  | "refused" => pure .refused
  | "invalidPositions" => pure .invalidPositions
  | "dropped" => pure (.dropped (← dropReason (← str j 1)))
  | "disagrees" => pure .disagrees
  | "failed" => pure (.failed (← failure (← str j 1)))
  | "paused" => pure .paused
  | "removed" => pure .removed
  | other => throw s!"unknown pending reason {other}"

def reasonJson : Reason → Json
  | .missing path => Json.arr #["missing", toJson path]
  | .waits p => Json.arr #["waits", prerequisiteJson p]
  | .keyUnavailable a key => Json.arr #["keyUnavailable", toJson (audienceNumber a), toJson key]
  | .updateRequired v => Json.arr #["updateRequired", toJson v]
  | .noStorage => Json.arr #["noStorage"]
  | .storage retryable => Json.arr #["storage", toJson retryable]
  | .refused => Json.arr #["refused"]
  | .invalidPositions => Json.arr #["invalidPositions"]
  | .dropped d => Json.arr #["dropped", dropJson d]
  | .disagrees => Json.arr #["disagrees"]
  | .failed f => Json.arr #["failed", failureJson f]
  | .paused => Json.arr #["paused"]
  | .removed => Json.arr #["removed"]
  | _ => Json.null

def retryName : Retry → String
  | .automatic => "automatic" | .afterUpdate => "afterUpdate"
  | .appAction => "appAction" | .never => "never"

private def conditions (j : Json) : Except String (List Condition) := do
  (← j.getObjValAs? (Array Json) "conditions").toList.mapM fun c => do
    pure (Condition.mk (← (← c.getArrVal? 0).getBool?) (← reason (← c.getArrVal? 1)))

private def scan (j : Json) : Except String Scan := do
  match ← str j 0 with
  | "positions" => pure .positions
  | "snapshots" => pure (.snapshots (audience (← nat j 1)))
  | other => throw s!"unknown listing class {other}"

/-- A step is one subject's observation (`subject`, `reporter`, `conditions`),
a snapshot load's installed positions (`cover`: `[device, number]` pairs), a
completed store-log scan's waits (`entries`: `[[device, number], reason]`
pairs), or a completed listing (`listing` class and its `observed` subjects
with their conditions). A case names the local `device` and its `steps`; the
output lists the visible records after each committed step. -/
def step (j : Json) : Except String Step := do
  if let .ok s := j.getObjVal? "subject" then
    return .observe ⟨← subject s, ← j.getObjValAs? Nat "reporter", ← conditions j⟩
  if let .ok jump := j.getObjValAs? (Array Json) "cover" then
    return .cover (← jump.toList.mapM fun p => do pure (← nat p 0, ← nat p 1))
  if let .ok waits := j.getObjValAs? (Array Json) "entries" then
    return .entries ⟨← waits.toList.mapM fun w => do
      let e ← w.getArrVal? 0
      pure ((← nat e 0, ← nat e 1), ← reason (← w.getArrVal? 1))⟩
  let observed ← (← j.getObjValAs? (Array Json) "observed").toList.mapM fun o => do
    pure (← subject (← o.getObjVal? "subject"), ← conditions o)
  pure (.listing ⟨← scan (← j.getObjVal? "listing"), observed⟩)

def run (j : Json) : Except String Json := do
  let mut state : Local := ⟨← j.getObjValAs? Nat "device", fun _ => 0, []⟩
  let mut out := #[]
  for s in ← j.getObjValAs? (Array Json) "steps" do
    state := state.step (← step s)
    let .ok visible := pending (.available state.work) | throw "an available database failed"
    out := out.push (Json.arr (visible.map fun r =>
      Json.arr #[subjectJson r.subject, toJson r.reporter, reasonJson r.reason,
        toJson (retryName (retry r.reason))]).toArray)
  pure (Json.arr out)

end PendingJson
