import SqliveNarrowing.Json
import SqliveNarrowing.WellFormed

/-! The model's runner: one case per process on stdin, the answer on stdout;
a case it can't read fails on stderr with a nonzero exit.

Input: `schema` (`tables`, `views`), `statement` (a query tree whose FROM
items name the shapes sqlive reads whole), the database `before` and `after`
a write, the write's row `changes` in order, and whether a question that
judged the write `stopped` at sqlive's step limit. Output: whether the write
reruns the statement (`Statement.rerunsLimited`), whether the statement is well formed
(`Statement.wellFormed`), whether the model reads it whole, each table's
scope, and the statement's rows before and after. -/
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
  let scopes := s.tableScopes db
  let shown := (List.range schema.tables.length).filterMap fun t =>
    (scopes.bind fun scopeOf => scopeOf t).map fun scope =>
      ((schema.tables.getD t default).1, Lean.Json.str (toString (repr scope)))
  pure <| Json.mkObj [
    ("reruns", toJson (s.rerunsLimited db changes db' stopped)),
    ("wellFormed", toJson s.wellFormed),
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
