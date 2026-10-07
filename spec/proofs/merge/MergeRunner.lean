import CovenMerge.Removal
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
  | .deletedCircle => 2
  | .otherAudience => 3
  | .unique => 4

private def run (j : Json) : Except String Json := do
  let writes ← (← j.getObjValAs? (Array Json) "writes").mapM readWrite
  let order ← j.getObjValAs? (Array Nat) "order"
  let rows ← (← j.getObjValAs? (Array Json) "rows").mapM readRow
  let cols ← natField j "columns"
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
  let v := view I
  let output := rows.map fun r =>
    let generation := st.gen r.id
    let gw := (List.range (generation + 1)).filterMap fun g =>
      (st.genWrite r.id g).map fun w => toJson [g, w]
    let cells := (List.range cols).filterMap fun c =>
      (st.cell r.id c).map fun w => toJson [c, w]
    let lost := (List.range cols).flatMap fun c => writes.toList.filterMap fun w =>
      (st.lost r.id c w.id).map fun (g, replacer) => toJson [c, w.id, g, replacer]
    Json.mkObj [
      ("id", toJson r.id), ("gen", toJson generation),
      ("gw", toJson gw), ("cells", toJson cells), ("lost", toJson lost),
      ("removed", toJson (v.removed r.id)),
      ("rules", toJson ((v.rules r.id).map ruleNumber))]
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
