import CovenQueue.ConversionChain
import Lean

/-! Execute the migration-list plan and whole-write admission for eligible
uncovered ordinary writes. Appending decimal digits makes converter order observable. -/
open Lean CovenQueue

private def run (input : Json) : Except String Json := do
  let version ← input.getObjValAs? Nat "version"
  let raise ← input.getObjValAs? Nat "raise"
  let values ← input.getObjValAs? (List Nat) "values"
  let steps ← (← input.getObjValAs? (List Json) "steps").mapM fun step => do
    let version ← step.getObjValAs? Nat "version"
    let breaking ← step.getObjValAs? Bool "breaking"
    let digit ← step.getObjValAs? (Option Nat) "digit"
    pure (⟨version, breaking, false,
      digit.map fun d xs => xs.map fun x => x * 10 + d⟩ : Chain.Step)
  let original : Record :=
    ⟨⟨⟨2, 1⟩, 1, [], []⟩, version, .ordinary,
      [⟨0, 1, .ins, ⟨none, []⟩, .values values⟩]⟩
  let received := (Chain.plan Chain.byStatements (fun _ => false) steps raise original).record
    (fun _ => none) original
  let verdict := classify ⟨some ⟨⟨1, []⟩, raise⟩, none, []⟩ ⟨received, none⟩
  let lost : Option Nat := match verdict with
    | .excluded _ version => some version
    | _ => none
  pure <| Json.mkObj [("applied", toJson (verdict == .applied)),
    ("values", toJson (received.body.flatMap RowChange.values)), ("loss", toJson lost)]

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
