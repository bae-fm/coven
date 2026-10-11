import CovenMerge.Boundary
import Lean

/-! JSON adapter for differential tests. All merge and removal computation calls
    the existing model; this file changes no theorem or proof. Values in the
    model are setter identities. Reference substitution and SQL constraint
    evaluation are inputs, as in Appendix B's stated proof boundary. -/

open Lean CovenMerge

private def natField (j : Json) (name : String) : Except String Nat :=
  j.getObjValAs? Nat name

private def stampField (j : Json) : Except String Nat := do
  let s ← j.getObjValAs? String "ts"
  match s.toNat? with
  | some n => pure n
  | none => throw "ts must be a natural number encoded as a string"

private structure InputChange where
  row : Nat
  change : Change Nat

private structure InputWrite where
  id : Nat
  stamp : Nat
  past : Array Nat
  changes : Array InputChange

private def readChange (j : Json) : Except String InputChange := do
  let row ← natField j "row"
  let gen ← natField j "gen"
  let cols ← j.getObjValAs? (Array Nat) "cols"
  let kind ← match ← natField j "kind" with
    | 0 => pure Kind.ins
    | 1 => pure Kind.upd
    | 2 => pure Kind.del
    | _ => throw "unknown change kind"
  pure ⟨row, ⟨kind, gen, cols.contains⟩⟩

private def readWrite (j : Json) : Except String InputWrite := do
  let id ← natField j "id"
  let stamp ← stampField j
  let past ← j.getObjValAs? (Array Nat) "past"
  let changes ← (← j.getObjValAs? (Array Json) "changes").mapM readChange
  pure ⟨id, stamp, past, changes⟩

private structure InputRow where
  id : Nat
  present : Bool
  refs : List (CovenMerge.Ref Nat)
  checkFails : Bool
  deleted : Bool
  claims : List (Claim Nat)
  rank : Nat

private def readRef (j : Json) : Except String (CovenMerge.Ref Nat) := do
  pure ⟨← natField j "parent", ← j.getObjValAs? Bool "stale"⟩

private def readClaim (j : Json) : Except String (Claim Nat) := do
  let con ← j.getObjVal? "con"
  let terms ← con.getObjValAs? (Array String) "terms"
  let predicate ← con.getObjValAs? (Option String) "partial"
  pure ⟨⟨terms.toList, predicate⟩, ← natField j "value", ← stampField j,
    ← j.getObjValAs? Bool "other"⟩

private def readRow (j : Json) : Except String InputRow := do
  pure ⟨← natField j "id", ← j.getObjValAs? Bool "present",
    (← (← j.getObjValAs? (Array Json) "refs").mapM readRef).toList,
    ← j.getObjValAs? Bool "check", ← j.getObjValAs? Bool "deleted",
    (← (← j.getObjValAs? (Array Json) "claims").mapM readClaim).toList,
    ← natField j "rank"⟩

private def ruleNumber : Rule → Nat
  | .foreignKey => 0
  | .check => 1
  | .deletedCircle | .deletedCircleEntry _ => 2
  | .otherAudience => 3
  | .unique => 4

private def namedRuleJson (rule : Rule) : Json :=
  match rule with
  | .deletedCircleEntry entry => Json.mkObj [("kind", toJson (2 : Nat)), ("entry", toJson entry)]
  | rule => Json.mkObj [("kind", toJson (ruleNumber rule))]

private def mergedFields (st : St Nat Nat Nat) (writes : Array InputWrite)
    (r cols : Nat) : List (String × Json) :=
  let generation := st.gen r
  let gw := (List.range (generation + 1)).filterMap fun g =>
    (st.genWrite r g).map fun w => toJson [g, w]
  let cells := (List.range cols).filterMap fun c =>
    (st.cell r c).map fun w => toJson [c, w]
  let lost := (List.range cols).flatMap fun c =>
    (writes.toList.mergeSort (fun a b => a.id ≤ b.id)).filterMap fun w =>
      (st.lost r c w.id).map fun (g, replacer) => toJson [c, w.id, g, replacer]
  [("id", toJson r), ("gen", toJson generation),
    ("gw", toJson gw), ("cells", toJson cells), ("lost", toJson lost)]

private def mergedJson (st : St Nat Nat Nat) (writes : Array InputWrite)
    (rows : Array Nat) (cols : Nat) : Json :=
  toJson (rows.map fun r => Json.mkObj (mergedFields st writes r cols))

private def readBoundary (j : Json) (writes : Array InputWrite) :
    Except String (SnapshotBoundary Nat × Array Nat) := do
  let covered ← j.getObjValAs? (Array Nat) "covered"
  let snapshotInputs ← j.getObjValAs? (Array Nat) "snapshot"
  for w in covered ++ snapshotInputs do
    if !(writes.any (·.id == w)) then throw "boundary names an unknown write"
  for w in snapshotInputs do
    if !covered.contains w then throw "snapshot input is not covered"
  let B : SnapshotBoundary Nat := {
    covered := covered.contains
    inputs := fun w => snapshotInputs.contains w && covered.contains w
    inputs_covered := by intro w h; simp only [Bool.and_eq_true] at h; exact h.2 }
  pure (B, snapshotInputs)

private def runReset (j : Json) (M : Writes Nat Nat Nat)
    (writes : Array InputWrite) (order : Array Nat) : Except String Json := do
  let (B, snapshotInputs) ← readBoundary j writes
  let read ← j.getObjValAs? (Array Nat) "read"
  let reset ← j.getObjValAs? (Option Nat) "reset"
  let rows ← j.getObjValAs? (Array Nat) "rowIds"
  let cols ← natField j "columns"
  for w in read do
    if !(writes.any (·.id == w)) then throw "reset read names an unknown write"
  let snapshot := snapshotInputs.toList.foldl (step M) (St.init : St Nat Nat Nat)
  let st := match reset with
    | some entry => reloadReset M { B with entry } (fun w _ => read.contains w) snapshot order.toList
    | none => reloadSnapshot M B (fun _ => true) snapshot order.toList
  pure (Json.mkObj [("rows", mergedJson st writes rows cols), ("frozen", toJson ([] : List Json))])

private def frozenJson (loss : FrozenLoss Nat Nat Nat) (cols : Nat) : Json :=
  let r := loss.record
  let values := (List.range cols).filterMap fun c => (r.values c).map fun w => toJson [c, w]
  let cause := match r.cause with
    | .schemaChange version write => Json.mkObj [("schema", toJson version), ("write", toJson write)]
    | .write write => Json.mkObj [("write", toJson write)]
    | .dependency write sources => Json.mkObj [("write", toJson write), ("sources", toJson sources)]
    | .rules rules => Json.mkObj [("rules", toJson (rules.map namedRuleJson))]
  Json.mkObj [("row", toJson r.row), ("gen", toJson r.generation),
    ("values", toJson values), ("cause", cause)]

private def runSchema (j : Json) (M : Writes Nat Nat Nat)
    (writes : Array InputWrite) (order : Array Nat) : Except String Json := do
  let (B, snapshotInputs) ← readBoundary j writes
  let rows ← j.getObjValAs? (Array Nat) "rowIds"
  let cols ← natField j "columns"
  let verdicts ← (← j.getObjValAs? (Array Json) "schema").mapM fun v => do
    let id ← natField v "id"
    let verdict ← match ← natField v "kind" with
      | 0 => pure SchemaDisposition.eligible
      | 1 => do pure (SchemaDisposition.excluded (← natField v "version"))
      | 2 => pure SchemaDisposition.losingMigration
      | _ => throw "unknown schema disposition"
    pure (id, verdict)
  for w in writes do
    if !(verdicts.any (·.1 == w.id)) then throw "missing schema disposition"
  let supplied ← (← j.getObjValAs? (Array Json) "values").mapM fun v => do
    pure (← natField v "write", ← natField v "row", ← v.getObjValAs? (Array Nat) "cols")
  let values := fun w r c =>
    if (supplied.find? (fun v => v.1 == w && v.2.1 == r)).any (fun v => v.2.2.contains c)
    then some w else none
  let schema := fun w => match verdicts.find? (·.1 == w) with
    | some (_, s) => s
    | none => .eligible -- outside the finite write domain
  let frozen ← (← j.getObjValAs? (Array Json) "snapshotFrozen").mapM fun loss => do
    let row ← natField loss "row"
    let gen ← natField loss "gen"
    let version ← natField loss "version"
    let write ← natField loss "write"
    pure (excludedLoss write row gen (values write row) version)
  let snapshot : MergeSnapshot Nat Nat Nat :=
    ⟨snapshotInputs.toList.foldl (step M) St.init, frozen.toList⟩
  let inputs := fun (st : St Nat Nat Nat) => ({
    rows := rows.toList, present := fun r => decide (st.gen r % 2 = 1),
    refs := fun _ => [], checkFails := fun _ => false, inDeletedCircle := fun _ => none,
    claims := fun _ => [], rank := id } : EntryInputs Nat Nat)
  let result := boundaryDevice M B none (fun _ _ => false) schema values inputs snapshot order.toList
  let excluded := writes.toList.flatMap fun w => rows.toList.filterMap fun r => result.excluded w.id r
  pure (Json.mkObj [("rows", mergedJson result.current.merged writes rows cols),
    ("frozen", toJson ((result.snapshotFrozen ++ excluded).map (fun loss => frozenJson loss cols)))])

private def readEntries (j : Json) (I : Inputs Nat Nat) : Except String (EntryInputs Nat Nat) := do
  let entries ← j.getObjValAs? (Array (Nat × Nat)) "entries"
  for (r, _) in entries do
    if !I.rows.contains r then throw "entry names an unknown row"
  pure { I with inDeletedCircle := fun r => (entries.find? (·.1 == r)).map (·.2) }

private def readInputs (j : Json) : Except String (Array InputRow × Inputs Nat Nat) := do
  let rows ← (← j.getObjValAs? (Array Json) "rows").mapM readRow
  let I : Inputs Nat Nat := {
    rows := rows.toList.map (·.id)
    present := fun r => (rows.find? (·.id == r)).any (·.present)
    refs := fun r => match rows.find? (·.id == r) with
      | some x => x.refs
      | none => []
    checkFails := fun r => (rows.find? (·.id == r)).any (·.checkFails)
    inDeletedCircle := fun r => (rows.find? (·.id == r)).any (·.deleted)
    claims := fun r => match rows.find? (·.id == r) with
      | some x => x.claims
      | none => []
    rank := fun r => match rows.find? (·.id == r) with
      | some x => x.rank
      | none => 0 }
  pure (rows, I)

private def run (j : Json) : Except String Json := do
  let mode ← j.getObjValAs? String "mode"
  if mode == "migration" then
    let (rows, I) ← readInputs j
    let named ← readEntries j I
    let final ← j.getObjValAs? (Array Nat) "final"
    let selected := migrationRemoves named final.contains
    return toJson (rows.map fun r => Json.mkObj [("id", toJson r.id), ("selected", toJson (selected r.id))])
  let writes ← (← j.getObjValAs? (Array Json) "writes").mapM readWrite
  let order ← j.getObjValAs? (Array Nat) "order"
  for w in order do
    if !(writes.any (·.id == w)) then throw "order names an unknown write"
  let M : Writes Nat Nat Nat := {
    ts := fun w => match writes.find? (·.id == w) with
      | some x => x.stamp
      | none => 0
    past := fun w a => match writes.find? (·.id == w) with
      | some x => x.past.contains a
      | none => false
    chg := fun w r => do
      let x ← writes.find? (·.id == w)
      let c ← x.changes.find? (·.row == r)
      some c.change }
  if mode == "reset" then return ← runReset j M writes order
  if mode == "schema" then return ← runSchema j M writes order
  let (rows, I) ← readInputs j
  let cols ← natField j "columns"
  let st := order.toList.foldl (step M) (St.init : St Nat Nat Nat)
  let v ← match mode with
    | "boolean" => pure (view I)
    | "entries" => do pure (entryView (← readEntries j I))
    | _ => throw "unknown merge runner mode"
  let output := rows.map fun r =>
    Json.mkObj (mergedFields st writes r.id cols ++ [
      ("removed", toJson (v.removed r.id)),
      ("rules", if mode == "entries" then toJson ((v.rules r.id).map namedRuleJson)
        else toJson ((v.rules r.id).map ruleNumber))])
  pure (toJson output)

def main : IO UInt32 := do
  let stdin ← IO.getStdin
  let stdout ← IO.getStdout
  let input ← stdin.readToEnd
  match Json.parse input >>= run with
  | .ok result => stdout.putStrLn result.compress; pure 0
  | .error err =>
    let stderr ← IO.getStderr
    stderr.putStrLn err
    pure 1
