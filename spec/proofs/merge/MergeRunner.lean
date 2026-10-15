import CovenMerge.Boundary
import CovenMerge.OwnerAdmission
import CovenMerge.ForeignKeyRecords
import CovenMerge.ForeignKeyAuthoring
import Lean

/-! JSON adapter for differential tests. All merge and removal computation calls
    the existing model; this file changes no theorem or proof. Values in the
    model are setter identities. Reference substitution and SQL constraint
    evaluation are inputs, as in Appendix B's stated proof boundary. The
    `creation` mode reports each row's selected original creation and the
    `effects` mode recomputes foreign-key consequences from resolved
    references (B18). -/

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

private def readRule (j : Json) : Except String Rule := do
  match ← natField j "kind" with
  | 0 => pure .foreignKey
  | 1 => pure .check
  | 2 => pure (.deletedCircleEntry (← natField j "entry"))
  | 3 => pure .otherAudience
  | 4 => pure .unique
  | _ => throw "unknown removal rule"

private def readCause (j : Json) : Except String (LossCause Nat) := do
  match ← j.getObjValAs? String "kind" with
  | "write" => pure (.write (← natField j "write"))
  | "rules" => pure (.rules ((← (← j.getObjValAs? (Array Json) "rules").mapM readRule).toList))
  | "schema" => pure (.schemaChange (← natField j "version") (← natField j "write"))
  | "dependency" =>
    pure (.dependency (← natField j "write") (← j.getObjValAs? (Array Nat) "sources").toList)
  | _ => throw "unknown loss cause"

private def causeJson : LossCause Nat → Json
  | .write w => Json.mkObj [("kind", "write"), ("write", toJson w)]
  | .rules rules => Json.mkObj [("kind", "rules"), ("rules", toJson (rules.map namedRuleJson))]
  | .schemaChange version w =>
    Json.mkObj [("kind", "schema"), ("version", toJson version), ("write", toJson w)]
  | .dependency w sources =>
    Json.mkObj [("kind", "dependency"), ("write", toJson w), ("sources", toJson sources)]

/-- A stored loss: `frozen`, its row, generation, the column of a cell loss,
each value's setter by column, and its cause. -/
private def readLoss (j : Json) : Except String (PendingLoss Nat Nat Nat) := do
  let values ← j.getObjValAs? (Array (Nat × Nat)) "values"
  let record : LossRecord Nat Nat Nat := ⟨← natField j "row", ← natField j "gen",
    ← j.getObjValAs? (Option Nat) "column", fun c => (values.find? (·.1 == c)).map (·.2),
    ← readCause (← j.getObjVal? "cause")⟩
  if ← j.getObjValAs? Bool "frozen" then pure (.frozen (freeze record)) else pure (.active record)

private def lossJson (cols : Nat) (r : LossRecord Nat Nat Nat) : Json :=
  let values := (List.range cols).filterMap fun c => (r.values c).map fun w => toJson [c, w]
  Json.mkObj [("row", toJson r.row), ("gen", toJson r.generation), ("column", toJson r.column),
    ("values", toJson values), ("cause", causeJson r.cause)]

/-- A migration that drops tables and columns: `freezeDropped` over the
stored losses with `droppedBy`, then the live records the dropped state
leaves. A cell loss stays active unless affected; a hidden row of a kept
table keeps its removed-row record under `dropSchema`, whatever was frozen. -/
private def runFreeze (j : Json) : Except String Json := do
  let cols ← natField j "columns"
  let droppedRows ← j.getObjValAs? (Array Nat) "droppedRows"
  let droppedColumns ← j.getObjValAs? (Array (Nat × Nat)) "droppedColumns"
  let losses ← (← j.getObjValAs? (Array Json) "losses").mapM readLoss
  let table := fun r => droppedRows.contains r
  let column := fun r c => droppedColumns.contains (r, c)
  let affected := droppedBy (List.range cols) table column
  let frozen := (losses.map (freezeDropped affected)).toList.filterMap fun
    | .frozen loss => some loss.record
    | .active _ => none
  let mut active : Array (LossRecord Nat Nat Nat) := #[]
  for loss in losses do
    match loss with
    | .frozen _ => pure ()
    | .active record =>
      match record.column, record.cause with
      | some _, _ => if !affected record then active := active.push record
      | none, .rules rules =>
        if !table record.row then
          let st : St Nat Nat Nat := { St.init with
            gen := fun r => if r = record.row then record.generation else 0
            cell := fun r c => if r = record.row then record.values c else none }
          let v : View Nat := ⟨fun _ => false, fun r => r == record.row,
            fun r => if r = record.row then rules else []⟩
          match lossRecord (dropSchema st table column) v record.row none with
          | some live => active := active.push live
          | none => throw "a hidden row has no removed-row record"
      | none, _ => throw "an active whole-row loss is a removal"
  pure (Json.mkObj [("frozen", toJson (frozen.map (lossJson cols))),
    ("active", toJson (active.toList.map (lossJson cols)))])

open Ownership.ForeignKey in
private def readAction (j : Json) (name : String) : Except String Action := do
  match ← j.getObjValAs? String name with
  | "cascade" => pure .cascade
  | "setNull" => pure .setNull
  | "restrict" => pure .restrict
  | "noAction" => pure .noAction
  | _ => throw "unknown foreign-key action"

open Ownership.ForeignKey in
private def readReference (j : Json) : Except String (Reference Nat Nat Nat) := do
  pure ⟨← natField j "parent", ← natField j "parentGeneration", ← natField j "child",
    ← natField j "childGeneration", (← j.getObjValAs? (Array Nat) "columns").toList,
    (← j.getObjValAs? (Array (Option Nat)) "oldKey").toList, ← readAction j "onDelete",
    ← readAction j "onUpdate", (← j.getObjValAs? (Array Nat) "parentColumns").toList⟩

open Ownership.ForeignKey in
private def readEffect (j : Json) : Except String (Effect Nat Nat Nat) := do
  let kind ← match ← natField j "kind" with
    | 0 => pure Kind.ins
    | 1 => pure Kind.upd
    | 2 => pure Kind.del
    | _ => throw "unknown effect kind"
  let cells ← (← j.getObjValAs? (Array Json) "cells").mapM fun cell => do
    pure (← natField cell "column", ← cell.getObjValAs? (Option Nat) "value")
  pure ⟨← natField j "row", ← natField j "generation", kind, cells.toList⟩

open Ownership.ForeignKey in
private def effectJson (e : Effect Nat Nat Nat) : Json :=
  let kind : Nat := match e.kind with
    | .ins => 0
    | .upd => 1
    | .del => 2
  Json.mkObj [("row", toJson e.row), ("generation", toJson e.generation), ("kind", toJson kind),
    ("cells", toJson (e.cells.map fun (c, v) => Json.mkObj [("column", toJson c), ("value", toJson v)]))]

-- Each derived row names its chain of rows from a direct root. Each edge
-- selects the consequence of its parent's recomputed payload on that child
-- row; the payload itself is always recomputed by `evaluate`.
open Ownership.ForeignKey in
private def runEffects (j : Json) : Except String Json := do
  let references := (← (← j.getObjValAs? (Array Json) "references").mapM readReference).toList
  let directs ← (← j.getObjValAs? (Array Json) "direct").mapM readEffect
  let schema := resolvedSchema references
  let direct : Nat → Option (Effect Nat Nat Nat) := fun r => directs.find? (·.row == r)
  let results ← (← j.getObjValAs? (Array Json) "derived").mapM fun d => do
    let path ← d.getObjValAs? (Array Nat) "path"
    let proposed ← readEffect (← d.getObjVal? "effect")
    let some root := path[0]? | throw "an empty derivation path"
    let derivation := path.toList.drop 1 |>.foldl (fun (acc : Option (Derivation Nat)) child =>
      acc.bind fun parent =>
        (evaluate schema direct parent).bind fun (row, payload) =>
          ((schema.consequences row payload).findIdx? (·.1 == child)).map (.foreign parent ·))
      (some (.direct root))
    let evaluated := derivation.bind (evaluate schema direct)
    pure (Json.mkObj [
      ("effect", match evaluated with
        | some (_, e) => effectJson e
        | none => Json.null),
      ("verifies", toJson (derivation.any fun d => verifies schema direct d proposed.row proposed))])
  pure (toJson results)

-- Local owner admission (B18): the write's direct effects and its derived
-- rows' derivations, as `effects` reads them, with each row's table and
-- owner, the owned tables and the writing member. `Admission.check` decides
-- each row for the local write, numbered 0; row `r`'s creator is write
-- `r + 1`, authored by that row's owner.
open Ownership Ownership.ForeignKey Ownership.Admission in
private def runAdmission (j : Json) : Except String Json := do
  let references := (← (← j.getObjValAs? (Array Json) "references").mapM readReference).toList
  let directs ← (← j.getObjValAs? (Array Json) "direct").mapM readEffect
  let derived ← (← j.getObjValAs? (Array Json) "derived").mapM fun d => do
    pure (← d.getObjValAs? (Array Nat) "path", ← readEffect (← d.getObjVal? "effect"))
  let rows ← (← j.getObjValAs? (Array Json) "rows").mapM fun r => do
    pure (← natField r "row", ← natField r "table", ← natField r "owner")
  let owned ← j.getObjValAs? (Array Nat) "owned"
  let writer ← natField j "author"
  let schema := resolvedSchema references
  let plain : Nat → Option (Effect Nat Nat Nat) := fun r => directs.find? (·.row == r)
  let derivation := fun (path : Array Nat) =>
    (path.toList.drop 1).foldl (fun (acc : Option (Derivation Nat)) child =>
      acc.bind fun parent =>
        (evaluate schema plain parent).bind fun (row, payload) =>
          ((schema.consequences row payload).findIdx? (·.1 == child)).map (.foreign parent ·))
      (path[0]?.map .direct)
  let P : Policy Nat Nat Nat Nat := {
    table := fun r => ((rows.find? (·.1 == r)).map (·.2.1)).getD 0
    owned := fun t => owned.contains t
    creator := fun r => r + 1
    author := fun w => if w == 0 then writer else
      ((rows.find? (·.1 == w - 1)).map (·.2.2)).getD 0 }
  let origin : Nat → Origin Nat := fun r =>
    match (derived.find? fun (path, _) => path.back? == some r).bind (derivation ·.1) with
    | some d => .foreign d
    | none => .direct
  let E : Evidence Nat Nat (Effect Nat Nat Nat) := {
    payload := fun w r => if w != 0 then none else
      match derived.find? fun (path, _) => path.back? == some r with
      | some (_, effect) => some effect
      | none => plain r
    execution := fun _ => {
      direct := fun r => (plain r).map fun e => ⟨e, []⟩
      origin := origin
      schema := schema } }
  pure (toJson (rows.map fun (r, _, _) =>
    Json.mkObj [("row", toJson r), ("check", toJson (check P E 0 r))]))

-- Receipt of one write (B18): the receiver's reconstructed execution, as
-- `admission` reads it, and the data sources `Admission.dependencies`
-- combines. Each derived row names one path per foreign-key action on it,
-- combined with `together` in execution order; a row with a direct step as
-- well starts that combination with its own direct effect. External sources
-- are numbered apart from rows, as changes of write 1; `droppedExternal` and
-- `prior` name the external and own changes whose verdict already drops them.
-- A direct row's proposed payload is its direct effect unless `payloads`
-- names the net change its steps do not end at, which only a row whose steps
-- cause actions must not do. `store` gives a cascaded key as the child's
-- column stores it. For each row:
-- `Admission.check`, `Admission.guard`'s cause, the roots its derivation
-- reaches and the sources `dependencies` gives it.
open Ownership Ownership.ForeignKey Ownership.Admission in
private def runReceive (j : Json) : Except String Json := do
  let references := (← (← j.getObjValAs? (Array Json) "references").mapM readReference).toList
  let directs ← (← j.getObjValAs? (Array Json) "direct").mapM readEffect
  let derived ← (← j.getObjValAs? (Array Json) "derived").mapM fun d => do
    pure (← d.getObjValAs? Nat "row", ← d.getObjValAs? (Array (Array Nat)) "paths",
      ← d.getObjValAs? Bool "direct", ← readEffect (← d.getObjVal? "effect"))
  let rows ← (← j.getObjValAs? (Array Json) "rows").mapM fun r => do
    pure (← natField r "row", ← natField r "table", ← natField r "owner")
  let owned ← j.getObjValAs? (Array Nat) "owned"
  let writer ← natField j "author"
  let external ← (← j.getObjValAs? (Array Json) "external").mapM fun e => do
    pure (← natField e "row", ← e.getObjValAs? (Array Nat) "sources")
  let droppedExternal ← j.getObjValAs? (Array Nat) "droppedExternal"
  let prior ← j.getObjValAs? (Array Nat) "prior"
  let payloads ← (← j.getObjValAs? (Array Json) "payloads").mapM readEffect
  -- A cascaded key as the child's column stores it (SQLite affinity).
  let store ← (← j.getObjValAs? (Array Json) "store").mapM fun e => do
    match (← e.getArr?).toList with
    | [c, a, b] => pure (← c.getNat?, ← a.getNat?, ← b.getNat?)
    | _ => throw "a stored value is [column, value, stored]"
  let stored := fun (c : Nat) (v : Option Nat) => v.map fun v =>
    ((store.find? fun e => e.1 == c && e.2.1 == v).map (·.2.2)).getD v
  let schema : Schema Nat (Effect Nat Nat Nat) := { resolvedSchema references with
    consequences := fun r p => ((resolvedSchema references).consequences r p).map fun (row, e) =>
      (row, { e with cells := e.cells.map fun cell => (cell.1, stored cell.1 cell.2) }) }
  let plain : Nat → Option (Effect Nat Nat Nat) := fun r => directs.find? (·.row == r)
  let path := fun (path : Array Nat) =>
    (path.toList.drop 1).foldl (fun (acc : Option (Derivation Nat)) child =>
      acc.bind fun parent =>
        (evaluate schema plain parent).bind fun (row, payload) =>
          ((schema.consequences row payload).findIdx? (·.1 == child)).map (.foreign parent ·))
      (path[0]?.map .direct)
  let derivation := fun (r : Nat) (paths : Array (Array Nat)) (direct : Bool) =>
    let actions := paths.toList.map path
    let start : Option (Derivation Nat) := if direct then some (.direct r) else none
    actions.foldl (fun acc action => match acc, action with
      | none, a => a
      | some d, some a => some (.together d a)
      | some _, none => none) start
  let P : Policy Nat Nat Nat Nat := {
    table := fun r => ((rows.find? (·.1 == r)).map (·.2.1)).getD 1000000
    owned := fun t => owned.contains t
    creator := fun r => r + 1
    author := fun w => if w == 0 then writer else
      ((rows.find? (·.1 == w - 1)).map (·.2.2)).getD 0 }
  -- A path that names no schema consequence still marks the row derived;
  -- its unevaluable certificate fails the check, as an unrooted cycle would.
  let origin : Nat → Origin Nat := fun r =>
    match derived.find? (·.1 == r) with
    | some (_, paths, direct, _) => match derivation r paths direct with
      | some d => .foreign d
      | none => .foreign (.foreign (.direct r) 1000000)
    | none => .direct
  let E : Evidence Nat Nat (Effect Nat Nat Nat) := {
    payload := fun w r => if w != 0 then none else
      match derived.find? (·.1 == r) with
      | some (_, _, _, effect) => some effect
      | none => (payloads.find? (·.row == r)).orElse fun _ => plain r
    execution := fun _ => {
      direct := fun r => (plain r).map fun e => ⟨e, []⟩
      origin := origin
      schema := schema } }
  let fkSources := fun r => match origin r with
    | .direct => []
    | .foreign d => ForeignKey.sources schema (direct P E 0) d
  let D : Dependencies Nat Nat := {
    changes := rows.toList.map (fun (r, _, _) => (0, r)) ++
      (external.toList.flatMap fun (_, sources) => sources.toList).map ((1 : Nat), ·)
    sources := fun w r => if w != 0 then [] else
      (((external.find? (·.1 == r)).map (·.2.toList)).getD []).map ((1 : Nat), ·) ++
        (fkSources r).map ((0 : Nat), ·) }
  -- A direct row's net change must end where its steps end only when its
  -- steps cause actions (`checkSteps`, decided as `check` of the relaxed
  -- evidence).
  let causes := fun (w r : Nat) => w == 0 && rows.any fun (x, _, _) => (fkSources x).contains r
  let E := relaxed E causes
  let priorCause : Nat → Nat → Option (Cause Nat Nat Nat) := fun w r =>
    if (w == 0 && prior.contains r) || (w == 1 && droppedExternal.contains r) then
      some (.dependency []) else none
  let causeJson : Option (Cause Nat Nat Nat) → Json
    | none => Json.null
    | some (.notOwner owner) => Json.mkObj [("kind", "notOwner"), ("owner", toJson owner)]
    | some .foreignKey => Json.mkObj [("kind", "foreignKey")]
    | some (.move _ owner) => Json.mkObj [("kind", "move"), ("owner", toJson owner)]
    | some (.dependency sources) => Json.mkObj [("kind", "dependency"),
      ("sources", toJson (sources.map fun ((w, r) : Nat × Nat) => [w, r]))]
  pure (toJson (rows.map fun (r, _, _) =>
    Json.mkObj [("row", toJson r), ("check", toJson (check P E 0 r)),
      ("guard", causeJson (guard P E D priorCause 0 r)),
      ("roots", match origin r with
        | .direct => toJson ([] : List Nat)
        | .foreign d => toJson (roots d)),
      ("sources", toJson ((D.sources 0 r).map fun ((w, row) : Nat × Nat) => [w, row]))]))

-- The receiver's classification of one write's steps (B18,
-- `Classification.cause` with `heldRule`): every step as D5 records it, in
-- execution order, and the references each step's row holds at that step, as
-- the receiver replays them. For each step: the position of its cause, or
-- null for a direct step. Cells and parents compare in one canonical order.
open Ownership Ownership.ForeignKey Ownership.Classification in
private def runClassify (j : Json) : Except String Json := do
  let readNamed (j : Json) : Except String (Nat × Nat × Nat × Nat) := do
    match (← j.getArr?).toList with
    | [c, n, r, g] => pure (← c.getNat?, ← n.getNat?, ← r.getNat?, ← g.getNat?)
    | _ => throw "a named parent is [column, reference, row, generation]"
  let normalize (s : Recorded Nat Nat Nat Nat) : Recorded Nat Nat Nat Nat :=
    { s with
      effect := { s.effect with cells := s.effect.cells.mergeSort fun a b => a.1 ≤ b.1 }
      parents := s.parents.mergeSort fun a b =>
        a.1 < b.1 || (a.1 == b.1 && (a.2.1 < b.2.1 || (a.2.1 == b.2.1 &&
          (a.2.2.1 < b.2.2.1 || (a.2.2.1 == b.2.2.1 && a.2.2.2 ≤ b.2.2.2))))) }
  let steps ← (← j.getObjValAs? (Array Json) "steps").mapM fun step => do
    let effect ← readEffect (← step.getObjVal? "effect")
    let parents ← (← step.getObjValAs? (Array Json) "parents").mapM readNamed
    let becomes ← step.getObjValAs? (Option (Nat × Nat)) "becomes"
    pure (normalize ⟨effect, parents.toList, becomes⟩)
  let refs ← (← j.getObjValAs? (Array Json) "refs").mapM fun entry => do
    let i ← natField entry "step"
    let held ← (← entry.getObjValAs? (Array Json) "held").mapM fun h => do
      let store ← (← h.getObjValAs? (Array Json) "store").mapM fun e => do
        match (← e.getArr?).toList with
        | [c, a, b] => pure (← c.getNat?, ← a.getNat?, ← b.getNat?)
        | _ => throw "a stored value is [column, value, stored]"
      let carried ← (← h.getObjValAs? (Array Json) "carried").mapM fun cell => do
        pure (← natField cell "column", ← cell.getObjValAs? (Option Nat) "value")
      let others ← match ← h.getObjVal? "others" with
        | Json.null => pure none
        | others => do pure (some (← (← others.getArr?).mapM readNamed).toList)
      let reference ← readReference (← h.getObjVal? "reference")
      let name ← natField h "name"
      pure (⟨reference, name, store.toList, carried.toList, others⟩ : Held Nat Nat Nat Nat)
    pure (i, held.toList)
  let rule : Rule Nat (Held Nat Nat Nat Nat) (Recorded Nat Nat Nat Nat) :=
    ⟨fun s => s.effect.row, fun h p => (heldConsequence h p).map normalize⟩
  let t : Trace Nat (Held Nat Nat Nat Nat) (Recorded Nat Nat Nat Nat) := {
    steps := steps.toList
    refs := fun i r => (((refs.find? (·.1 == i)).map (·.2)).getD []).filter (·.ref.child == r) }
  pure (toJson ((List.range steps.size).map fun i => cause rule t i))

private def run (j : Json) : Except String Json := do
  let mode ← j.getObjValAs? String "mode"
  if mode == "freeze" then return ← runFreeze j
  if mode == "effects" then return ← runEffects j
  if mode == "admission" then return ← runAdmission j
  if mode == "receive" then return ← runReceive j
  if mode == "classify" then return ← runClassify j
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
  if mode == "creation" then
    let rows ← j.getObjValAs? (Array Nat) "rowIds"
    let st := order.toList.foldl (step M) (St.init : St Nat Nat Nat)
    return toJson (rows.map fun r =>
      Json.mkObj [("id", toJson r), ("creator", toJson (st.genWrite r 1))])
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
