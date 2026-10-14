import CovenQueue.Checked
import CovenQueue.RowMerge
import CovenQueue.Retired
import CovenQueue.RaiseProgress
import CovenQueue.Conversion
import Lean

/-! JSON adapter for the queue differentials. Inputs are checked records in
timestamp order; every verdict and merged row is computed by the model's own
`classifyChange` and `mergeRows`. Values are abstract: a change sets the first
`values` columns, as `rowMetadata` states. -/
open Lean CovenMerge CovenQueue

private def writeId (j : Json) : Except String WriteId := do
  let pair ← fromJson? (α := Nat × Nat) j
  pure ⟨pair.1, pair.2⟩

private def readKind (n : Nat) : Except String Kind :=
  match n with | 0 => pure .ins | 1 => pure .upd | 2 => pure .del | _ => throw "operation"

private def readCause (j : Json) : Except String (LossCause Nat) := do
  match (j.getObjValAs? Nat "schema").toOption with
  | some version => pure (.schemaChange version (← j.getObjValAs? Nat "write"))
  | none => pure (.dependency (← j.getObjValAs? Nat "write") (← j.getObjValAs? (List Nat) "sources"))

private def readChange (j : Json) : Except String RowChange := do
  let values := List.replicate (← j.getObjValAs? Nat "values") 0
  let frozen ← match j.getObjValD "frozen" with
    | .null => pure none
    | cause => some <$> readCause cause
  pure {
    row := ← j.getObjValAs? Nat "row"
    generation := ← j.getObjValAs? Nat "generation"
    operation := ← readKind (← j.getObjValAs? Nat "kind")
    provenance := ⟨← j.getObjValAs? (Option Nat) "source",
      ← j.getObjValAs? (List (Nat × Nat)) "setters"⟩
    content := match frozen with
      | some cause => .frozen cause values
      | none => .values values }

private def readDisposition (j : Json) : Except String Disposition :=
  match j with
  | .str "ordinary" => pure .ordinary
  | .str "migration" => pure .migration
  | j => do pure (.lost (← j.getObjValAs? Nat "lost"))

private def readWrite (j : Json) : Except String CheckedWrite := do
  let read ← (← j.getObjValAs? (Array Json) "read").mapM writeId
  let body ← (← j.getObjValAs? (Array Json) "changes").mapM readChange
  pure ⟨⟨⟨← writeId (← j.getObjVal? "id"), ← j.getObjValAs? Nat "timestamp",
      read.toList, ← j.getObjValAs? (List Nat) "storeRead"⟩,
    ← j.getObjValAs? Nat "version", ← readDisposition (← j.getObjVal? "disposition"),
    body.toList⟩, none⟩

private def readBoundary (j : Json) : Except String Boundary := do
  pure ⟨← j.getObjValAs? Nat "entry", (← (← j.getObjValAs? (Array Json) "covers").mapM writeId).toList⟩

private def readView (j : Json) : Except String View := do
  let raise ← match j.getObjValD "raise" with
    | .null => pure none
    | r => do pure (some ⟨← readBoundary r, ← r.getObjValAs? Nat "version"⟩)
  let reset ← match j.getObjValD "reset" with
    | .null => pure none
    | r => some <$> readBoundary r
  let drops ← (← j.getObjValAs? (Array Json) "drops").mapM fun d => do
    pure (← d.getObjValAs? Nat "write", ← d.getObjValAs? Nat "row", ← readCause (← d.getObjVal? "cause"))
  pure ⟨raise, reset, drops.toList⟩

private def causeJson : LossCause Nat → Json
  | .schemaChange version write => Json.mkObj [("schema", toJson version), ("write", toJson write)]
  | .dependency write sources => Json.mkObj [("write", toJson write), ("sources", toJson sources)]
  | .write write => Json.mkObj [("replacing", toJson write)]
  | .rules _ => Json.mkObj [("rules", toJson true)]

private def verdictJson : CovenQueue.ChangeVerdict → Json
  | .applied => Json.mkObj [("kind", "applied")]
  | .frozen cause => Json.mkObj [("kind", "frozen"), ("cause", causeJson cause)]
  | .ignored entry => Json.mkObj [("kind", "ignored"), ("entry", toJson entry)]
  | .refused _ => Json.mkObj [("kind", "refused")]

private def rowJson (st : St Nat Nat Nat) (writes rows cols : Nat) (r : Nat) : Json :=
  let generation := st.gen r
  let gw := (List.range (generation + 1)).filterMap fun g =>
    (st.genWrite r g).map fun w => toJson [g, w]
  let cells := (List.range cols).filterMap fun c => (st.cell r c).map fun w => toJson [c, w]
  let lost := (List.range cols).flatMap fun c => (List.range writes).filterMap fun w =>
    (st.lost r c w).map fun (g, replacer) => toJson [c, w, g, replacer]
  let _ := rows
  Json.mkObj [("id", toJson r), ("gen", toJson generation), ("gw", toJson gw),
    ("cells", toJson cells), ("lost", toJson lost)]

/-- The receiver's own-window verdicts (§10): `Receive.withinWindow` under the
kept retirements, each write's device and first publication time. -/
private def windowLate (records : Array CheckedWrite) (stored : Array Nat)
    (retirements : List CovenMerge.Receive.Retirement) : Nat → Bool :=
  let policy : CovenMerge.Receive.Policy Nat := {
    boundary := ⟨fun _ => false, fun _ => false, by simp⟩
    logs := ⟨fun w => (records[w]?.map (·.record.identity.id.device)).getD 0,
      fun w => (records[w]?.map (·.record.identity.id.number)).getD 0⟩
    reset := none
    readEntry := fun _ _ => false
    schema := fun _ => .eligible
    retirements
    stored := fun w => stored[w]?.getD 0
    viewRetired := fun _ => false }
  fun w => !CovenMerge.Receive.withinWindow policy w

private def readRetirement (j : Json) : Except String CovenMerge.Receive.Retirement := do
  pure ⟨← j.getObjValAs? Nat "device", ← j.getObjValAs? Nat "entry", ← j.getObjValAs? Nat "stored"⟩

private def receive (j : Json) : Except String Json := do
  let records ← (← j.getObjValAs? (Array Json) "writes").mapM readWrite
  let stamps := records.toList.map (·.record.identity.timestamp)
  if !(stamps.zip stamps.tail).all (fun (a, b) => a < b) then
    throw "writes must be in increasing timestamp order"
  let view ← readView j
  let snapshot ← j.getObjValAs? (List Nat) "snapshot"
  let received ← j.getObjValAs? (List Nat) "received"
  let rows ← j.getObjValAs? Nat "rows"
  let cols ← j.getObjValAs? Nat "columns"
  for w in snapshot ++ received do
    if w ≥ records.size then throw "write index outside the history"
  let conversion ← match j.getObjValD "conversion" with
    | .null => pure none
    | c => do
      let convert ← c.getObjValAs? Bool "convert"
      pure (some (⟨← c.getObjValAs? Nat "version",
        if convert then some id else none⟩ : Conversion))
  if good : dataOK records then
    let original := recordHistory records
    let checked := checkedData records good
    -- A selected raise's receive rule interprets late records first (§17.1).
    let interpreted : (h : History) ×' DataValid h := match conversion with
      | some c => ⟨receivedHistory c view original checked, receivedValid c view original checked⟩
      | none => ⟨original, checked⟩
    let h := interpreted.1
    let valid := interpreted.2
    match j.getObjValD "retirements" with
    | .null =>
      let start := snapshot.foldl (step (mergeInput h valid view)) St.init
      let st := mergeRows h valid view start received
      let changes := received.flatMap fun w => (changeResults h valid view w).map fun (ch, answer) =>
        Json.mkObj [("write", toJson w), ("row", toJson ch.row), ("verdict", verdictJson answer)]
      pure (Json.mkObj [("dataValid", toJson true), ("changes", toJson changes),
        ("rows", toJson ((List.range rows).map (rowJson st records.size rows cols)))])
    | kept =>
      -- Each write's own window under the receiver's final kept retirements.
      let retirements ← (← kept.getArr?).toList.mapM readRetirement
      let stored ← j.getObjValAs? (Array Nat) "stored"
      if stored.size != records.size then throw "one storage time per write"
      let late := windowLate records stored retirements
      let start := snapshot.foldl (step (windowInput h valid view late)) St.init
      let st := windowRows h valid view late start received
      let changes := received.flatMap fun w =>
        (windowResults h valid view late w).map fun (ch, answer) =>
          Json.mkObj [("write", toJson w), ("row", toJson ch.row), ("verdict",
            match answer with
            | some answer => verdictJson answer
            | none => Json.mkObj [("kind", "dropped")])]
      pure (Json.mkObj [("dataValid", toJson true), ("changes", toJson changes),
        ("rows", toJson ((List.range rows).map (rowJson st records.size rows cols)))])
  else
    pure (Json.mkObj [("dataValid", toJson false)])

/-! Raise mode: one device's queue and its captured raise, driven by events.
Each event applies the model's own queue functions (`Queue.commit`,
`RaiseUpload.migrate`, `Queue.prepare`, `World.send`, `World.land`,
`Queue.finish`) under the `Uploadable` gate with the latest local migration's
version, and the raise advances only when `RaiseUpload.Confirmed` holds. A
later migration replaces the captured plan, as `RaiseUpload.Local.migrated`
does. Bytes are the attempt's identity: sealing and
real encodings are outside this comparison. -/

private def sealAttempt : Seal := fun a => [a.record.identity.id.number, a.format]

private structure Raising where
  world : World
  stage : Option RaiseUpload.Stage
  kept : Nat
  app : Nat
  publication : Nat
  plan : Option (RaiseUpload.Plan × Nat)

/-- One upload pass. `fault` 1 loses the first request before it lands;
2 loses the reply of the first request that stores new bytes, as the
provider's lost completion does; an already occupied path is confirmed by
comparing bytes instead. Each head first needs the gate. -/
private def uploadPass (kept publication : Nat) : Nat → Nat → World → World
  | 0, _, s => s
  | fuel + 1, fault, s =>
    match s.queue.waiting with
    | [] => s
    | p :: _ =>
      if !decide (Uploadable kept publication p.record) then s else
      let s := { s with queue := s.queue.prepare 1 [1] }
      match s.queue.head with
      | none => s
      | some a =>
        let s := s.send
        if fault = 1 then s else
        let occupied := (s.stored a.record.identity.id.number).isSome
        let s := s.land sealAttempt a
        if fault = 2 && !occupied then s else
        if s.stored a.record.identity.id.number = some (sealAttempt a) then
          uploadPass kept publication fuel (if fault = 2 then 2 else 0)
            { s with queue := s.queue.finish }
        else s

private def raiseStep (s : Raising) (event : Json) : Except String Raising := do
  let kind ← event.getObjValAs? String "event"
  let identity (n : Nat) : Identity := ⟨⟨s.world.queue.device, n⟩, n, [], []⟩
  match kind with
  | "write" =>
    let q := s.world.queue.commit ⟨identity 0, s.app, .ordinary, []⟩
    let world : World := { s.world with queue := q }
    pure { s with world := world }
  | "migrate" =>
    let version ← event.getObjValAs? Nat "version"
    let q := RaiseUpload.migrate s.world.queue version (identity 0)
    let through := q.reserved
    let covers := (List.range through).map fun n => (⟨q.device, n + 1⟩ : WriteId)
    let plan : RaiseUpload.Plan := ⟨through, covers, [], []⟩
    let world : World := { s.world with queue := q }
    let captured : Raising := { s with world := world, app := version, publication := version }
    pure { captured with stage := some RaiseUpload.Stage.captured, plan := some (plan, version) }
  | "upload" =>
    let fault ← event.getObjValAs? Nat "fault"
    let world := uploadPass s.kept s.publication (s.world.queue.waiting.length + 1) fault s.world
    pure { s with world := world }
  | "raise" =>
    match s.stage, s.plan with
    | some .captured, some (plan, version) =>
      let own := plan.covers.all fun id => id.device == s.world.queue.device &&
        0 < id.number && id.number ≤ plan.through
      if plan.through ≤ s.world.queue.settled && own then
        pure { s with stage := some RaiseUpload.Stage.entryStored, kept := max s.kept version }
      else pure s
    | _, _ => pure s
  | "crash" => pure { s with world := s.world.crash }
  | _ => throw "unknown raise event"

private def raisingJson (s : Raising) : Json :=
  let waiting := s.world.queue.waiting.map fun p =>
    let tried := match p with | .tried _ => true | .untried _ => false
    let kind := match p.record.disposition with
      | .migration => "migration" | .ordinary => "ordinary" | .lost _ => "lost"
    Json.mkObj [("number", toJson p.record.identity.id.number), ("tried", toJson tried),
      ("version", toJson p.record.version), ("kind", toJson kind)]
  let stage := match s.stage with
    | none => "none" | some .captured => "captured" | some .entryStored => "entryStored"
    | some _ => "requested"
  Json.mkObj [("settled", toJson s.world.queue.settled), ("waiting", toJson waiting),
    ("stage", toJson stage), ("kept", toJson s.kept)]

private def raise (j : Json) : Except String Json := do
  let events ← j.getObjValAs? (Array Json) "events"
  -- Before any raise the store is at the version its writes were made with.
  let app ← j.getObjValAs? Nat "app"
  let start : Raising := ⟨⟨⟨1, 0, []⟩, fun _ => none, [], .empty⟩, none, app, app, 0, none⟩
  let mut s := start
  let mut out := #[]
  for event in events do
    s ← raiseStep s event
    out := out.push (raisingJson s)
  pure (toJson out)

private def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "mode" with
  | "receive" => receive j
  | "raise" => raise j
  | _ => throw "unknown queue runner mode"

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
