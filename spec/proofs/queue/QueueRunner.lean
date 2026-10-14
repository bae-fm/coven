import CovenQueue.Checked
import CovenQueue.RowMerge
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
  if good : dataOK records then
    let h := recordHistory records
    let valid := checkedData records good
    let start := snapshot.foldl (step (mergeInput h valid view)) St.init
    let st := mergeRows h valid view start received
    let changes := received.flatMap fun w => (changeResults h valid view w).map fun (ch, answer) =>
      Json.mkObj [("write", toJson w), ("row", toJson ch.row), ("verdict", verdictJson answer)]
    pure (Json.mkObj [("dataValid", toJson true), ("changes", toJson changes),
      ("rows", toJson ((List.range rows).map (rowJson st records.size rows cols)))])
  else
    pure (Json.mkObj [("dataValid", toJson false)])

private def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "mode" with
  | "receive" => receive j
  | _ => throw "unknown queue runner mode"

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
