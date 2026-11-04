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

private def stampJson : Except StampError Timestamp → Json
  | .ok s => timestampJson s
  | .error .clockOutOfRange => Json.null

/-- A device with no latest timestamp makes its first stamp; it receives
nothing first, since the model's receiver always holds a latest timestamp. -/
private def runFirst (j : Json) (wall : Int) (device : Nat) : Except String Json := do
  let incoming ← j.getObjValAs? (Array Json) "incoming"
  if !incoming.isEmpty then throw "a device with no latest timestamp receives nothing"
  pure <| Json.mkObj [("latest", Json.null), ("applied", toJson 0),
    ("next", stampJson (stampFirst wall device))]

private def run (j : Json) : Except String Json := do
  let wall ← j.getObjValAs? Int "wall"
  let device ← j.getObjValAs? Nat "device"
  let latestJson ← j.getObjVal? "latest"
  if latestJson.isNull then return ← runFirst j wall device
  let latest ← readTimestamp latestJson
  let incoming ← (← j.getObjValAs? (Array Json) "incoming").mapM readTimestamp
  let snapshot ← j.getObjValAs? Bool "snapshot"
  let causes ← j.getObjValAs? Bool "causes"
  let receiver : Receiver := ⟨wall, latest, 0⟩
  let result := if snapshot then
      { receiver with latest := loadSnapshot latest incoming.toList, applied := incoming.size }
    else incoming.foldl (fun r s => receive r s causes true) receiver
  pure <| Json.mkObj [("latest", timestampJson result.latest),
    ("applied", toJson result.applied), ("next", stampJson (stamp wall device result.latest))]

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  match Json.parse input >>= run with
  | .ok result => (← IO.getStdout).putStrLn result.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
