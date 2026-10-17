import SequenceJson

open Lean

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= SequenceJson.run with
  | .ok result =>
    (← IO.getStdout).putStrLn result.compress
    return 0
  | .error err =>
    (← IO.getStderr).putStrLn err
    return 1
