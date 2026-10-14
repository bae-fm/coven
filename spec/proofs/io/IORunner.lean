import CovenIO.Execution
import Lean

/-! The IO differential runner. Each case names the executable transition it
exercises; the Rust harness feeds the actual decision path's inputs and
compares its observed results with this output. -/
open Lean CovenIO

/-- A writer publishes `count` key copies through `Publication`, possibly
losing the first completion reply, then a reader with `start` retained copies
executes `readLog` over the materialized exact paths. -/
private def discovery (j : Json) : Except String Json := do
  let count ← j.getObjValAs? Nat "count"
  let start ← j.getObjValAs? Nat "start"
  let lost ← j.getObjValAs? Bool "lost"
  let failed ← j.getObjValAs? Bool "failed"
  let bytes := (List.range count).map fun n => [n + 1]
  let reserved : Publication.Writer := ⟨bytes, [], 0⟩
  let interrupted : Publication.Writer := if lost && count > 0 then
    { reserved with published := bytes.take 1 } else ⟨bytes, bytes, count⟩
  let settled : Publication.Writer := ⟨bytes, bytes, count⟩
  let world : World := fun _ => Publication.objects settled .copy 1 (fun _ => 0)
  let result := readLog world (fun _ => if failed then some .network else none)
    (fun _ => false) .copy 1 (count + 2) 0 start
  let requests := result.requests.map fun request => match request with
    | .get (.log .copy 1 n) => n
    | _ => 0
  return Json.mkObj [
    ("before_published", toJson interrupted.published.length),
    ("before_confirmed", toJson interrupted.confirmed),
    ("published", toJson settled.published.length),
    ("confirmed", toJson settled.confirmed),
    ("position", toJson result.position),
    ("requests", toJson requests),
    ("failed", toJson (result.stop == .failed .network))]

/-- `Snapshot.counts`: a landing at most one day after the signed catch-up
sample counts; a later one is ignored (`late_snapshot_ignored`). -/
private def counts (j : Json) : Except String Json := do
  let caught ← j.getObjValAs? Nat "caught"
  let stored ← j.getObjValAs? Nat "stored"
  let snapshot : Snapshot := ⟨0, 0, 1, stored, caught, []⟩
  return toJson snapshot.counts

/-- `catalogCurrent .writes`: late landings are filtered before choosing the
snapshot with the most covered writes, ties going to the smaller path. -/
private def catalog (j : Json) : Except String Json := do
  let audience ← j.getObjValAs? Nat "audience"
  let entries ← j.getObjValAs? (Array Json) "snapshots"
  let snapshots ← entries.toList.mapM fun s => do
    let audience ← s.getObjValAs? Nat "audience"
    let writer ← s.getObjValAs? Nat "writer"
    let number ← s.getObjValAs? Nat "number"
    let stored ← s.getObjValAs? Nat "stored"
    let caught ← s.getObjValAs? Nat "caught"
    let positions ← s.getObjValAs? (List (Nat × Nat)) "positions"
    pure (Snapshot.mk audience writer number stored caught positions)
  return match catalogCurrent .writes snapshots audience with
    | some s => toJson [s.writer, s.number]
    | none => Json.null

private def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "check" with
  | "discovery" => discovery j
  | "counts" => counts j
  | "catalog" => catalog j
  | check => throw s!"unknown IO check {check}"

def main : IO UInt32 := do
  let input ← (← IO.getStdin).readToEnd
  let result := do
    let parsed ← Json.parse input
    let cases ← parsed.getArr?
    toJson <$> cases.mapM run
  match result with
  | .ok output => (← IO.getStdout).putStrLn output.compress; pure 0
  | .error error => (← IO.getStderr).putStrLn error; pure 1
