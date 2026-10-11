import CovenMerge.Reset
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

private def runReset (j : Json) (M : Writes Nat Nat Nat)
    (writes : Array InputWrite) (order : Array Nat) : Except String Json := do
  let covered ← j.getObjValAs? (Array Nat) "covered"
  let snapshotInputs ← j.getObjValAs? (Array Nat) "snapshot"
  let read ← j.getObjValAs? (Array Nat) "read"
  let reset ← j.getObjValAs? (Option Nat) "reset"
  let rows ← j.getObjValAs? (Array Nat) "rowIds"
  let cols ← natField j "columns"
  for w in covered ++ snapshotInputs ++ read do
    if !(writes.any (·.id == w)) then throw "boundary names an unknown write"
  for w in snapshotInputs do
    if !covered.contains w then throw "snapshot input is not covered"
  let B : SnapshotBoundary Nat := {
    covered := covered.contains
    inputs := fun w => snapshotInputs.contains w && covered.contains w
    inputs_covered := by intro w h; simp only [Bool.and_eq_true] at h; exact h.2 }
  let snapshot := snapshotInputs.toList.foldl (step M) (St.init : St Nat Nat Nat)
  let st := match reset with
    | some entry => reloadReset M { B with entry } (fun w _ => read.contains w) snapshot order.toList
    | none => reloadSnapshot M B (fun _ => true) snapshot order.toList
  pure (Json.mkObj [("rows", mergedJson st writes rows cols), ("frozen", toJson ([] : List Json))])

private def run (j : Json) : Except String Json := do
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
  let mode ← j.getObjValAs? String "mode"
  if mode == "reset" then return ← runReset j M writes order
  let rows ← (← j.getObjValAs? (Array Json) "rows").mapM readRow
  let cols ← natField j "columns"
  let st := order.toList.foldl (step M) (St.init : St Nat Nat Nat)
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
  let v ← match mode with
    | "boolean" => pure (view I)
    | "entries" => do
      let entries ← j.getObjValAs? (Array (Nat × Nat)) "entries"
      for (r, _) in entries do
        if !(rows.any (·.id == r)) then throw "entry names an unknown row"
      let named : EntryInputs Nat Nat :=
        { I with inDeletedCircle := fun r => (entries.find? (·.1 == r)).map (·.2) }
      pure (entryView named)
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
