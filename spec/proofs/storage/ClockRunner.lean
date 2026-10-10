import CovenStorage.Clocks
import Lean

/-! JSON adapter for Rust differential tests of the existing clock model. -/

open Lean CovenStorage.Clocks

private def readTimestamp (j : Json) : Except String Timestamp := do
  let parts ← fromJson? j (α := Array Nat)
  if parts.size != 3 then throw "timestamp needs milliseconds, counter and device"
  let ms := parts[0]!
  let counter := parts[1]!
  if ms ≥ msLimit || counter ≥ counterBase then throw "timestamp out of range"
  let tick := ms * counterBase + counter
  if h : tick < tickLimit then pure ⟨⟨tick, h⟩, parts[2]!⟩
  else throw "timestamp out of range"

private def timestampJson (s : Timestamp) : Json :=
  toJson [s.ms, s.counter, s.device]

private def run (j : Json) : Except String Json := do
  let wall ← j.getObjValAs? Int "wall"
  let device ← j.getObjValAs? Nat "device"
  let latest ← readTimestamp (← j.getObjVal? "latest")
  let incoming ← (← j.getObjValAs? (Array Json) "incoming").mapM readTimestamp
  let snapshot ← j.getObjValAs? Bool "snapshot"
  let causes ← j.getObjValAs? Bool "causes"
  let receiver : Receiver := ⟨wall, latest, 0⟩
  let result := if snapshot then
      { receiver with latest := loadSnapshot latest incoming.toList, applied := incoming.size }
    else incoming.foldl (fun r s => receive r s causes true) receiver
  let next := match stamp wall device result.latest with
    | .ok s => timestampJson s
    | .error .clockOutOfRange => Json.null
  pure <| Json.mkObj [("latest", timestampJson result.latest),
    ("applied", toJson result.applied), ("next", next)]

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
