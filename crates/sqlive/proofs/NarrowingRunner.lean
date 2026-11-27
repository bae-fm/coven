import SqliveNarrowing.Json
import SqliveNarrowing.References
import SqliveNarrowing.WellFormed
import SqliveNarrowing.Rules.Unread

/-! The model's runner: one case per process on stdin, the answer on stdout;
a case it can't read fails on stderr with a nonzero exit.

Input: `schema` (`tables`, `views`), `statement` (a query tree whose FROM
items name the shapes sqlive reads whole), the database `before` and `after`
a write, the write's row `changes` in order, and whether a question that
judged the write `stopped` at sqlive's step limit. Output: whether the write's
net change of each row (`net`) reruns the statement the model plans
(`Statement.pruned`, `Statement.rerunsReferences`), whether the statement and it
are well formed (`Statement.wellFormed`), whether the model reads it whole,
each table reference's scope, and the statement's rows before and after. -/
open Lean SqliveNarrowing

def rowsJson (rows : List Row) : Json :=
  toJson (rows.map fun r => r.map fun v => (v.map fun x => (toJson x)).getD Json.null)

def run (input : Json) : Except String Json := do
  let schema ← Json.readSchema (← input.getObjVal? "schema")
  let s ← Json.readStatement schema (← input.getObjVal? "statement")
  let db ← Json.readDb schema (← input.getObjVal? "before")
  let db' ← Json.readDb schema (← input.getObjVal? "after")
  let changes ← Json.readChanges schema input
  let stopped ← input.getObjValAs? Bool "stopped"
  -- sqlive plans the statement without the columns nothing reads
  -- (`Statement.pruned`).
  let planned := s.pruned
  let scopes := planned.split.tableScopes (db.split planned)
  let shown := planned.itemNumbers.filterMap fun i => do
    let t ← planned.table? i
    let scopeOf ← scopes
    let scope ← scopeOf i
    pure (s!"{(schema.tables.getD t default).1}#{i}", Lean.Json.str (toString (repr scope)))
  pure <| Json.mkObj [
    ("reruns", toJson (planned.rerunsReferences db (net changes) db' stopped)),
    ("wellFormed", toJson (s.wellFormed && planned.wellFormed)),
    ("whole", toJson scopes.isNone),
    ("scopes", Json.mkObj shown),
    ("before", rowsJson (s.result db)),
    ("after", rowsJson (s.result db'))]

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result =>
    (← IO.getStdout).putStrLn result.compress
    return 0
  | .error err =>
    (← IO.getStderr).putStrLn err
    return 1
