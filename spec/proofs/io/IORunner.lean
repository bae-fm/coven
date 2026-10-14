import CovenIO.Execution
import CovenIO.Trace
import CovenIO.Downloads
import CovenIO.Retired
import CovenIO.PassPhases
import CovenIO.SnapshotWrites
import CovenIO.Waiting
import Lean

/-! The IO differential runner. Each case names the executable transition it
exercises; the Rust harness feeds the actual decision path's inputs and
compares its observed results with this output. -/
open Lean CovenIO

private def cacheTransition (cache : Cache) (event : CacheEvent) : Except String Cache :=
  match cacheStep cache event with
  | some next => .ok next
  | none => .error "invalid cache transition"

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

private def saved (j : Json) : Except String (Option Nat) := do
  match j.getObjVal? "saved" with
  | .ok .null => pure none
  | .ok value => some <$> value.getNat?
  | .error _ => pure none

/-- `needsSnapshots`: no checkpoint, or the sample plus elapsed time has
reached `S + 29 days` (`exact_boundary_discovers`). -/
private def needs (j : Json) : Except String Json := do
  let observed ← j.getObjValAs? Nat "observed"
  let elapsed ← j.getObjValAs? Nat "elapsed"
  return toJson (needsSnapshots (← saved j) observed elapsed)

/-- `readRecentLog` over writer 0's write log, whose present numbers are
1..count except `deleted`. Each request adds `step` to elapsed time after
`base`, so the terminal miss is judged when it is used. -/
private def recent (j : Json) : Except String Json := do
  let count ← j.getObjValAs? Nat "count"
  let deleted ← j.getObjValAs? (List Nat) "deleted"
  let observed ← j.getObjValAs? Nat "observed"
  let base ← j.getObjValAs? Nat "base"
  let step ← j.getObjValAs? Nat "step"
  let store : Store := (List.range count).foldl (fun store n =>
    if deleted.contains (n + 1) then store else put store (.log .write 0 (n + 1)) [n + 1] 0) empty
  let result := readRecentLog (fun _ => store) (fun _ => none) (fun _ => false) 0 (count + 2) 0 0
    (← saved j) observed (fun t => base + step * (t + 1))
  let stop := match result.stop with
    | .miss => "miss" | .snapshots => "snapshots" | _ => "other"
  return Json.mkObj [("stop", toJson stop), ("position", toJson result.position)]

/-- Ranges batch `Trace.cacheStep` byte addresses. Reopen preserves the cache;
explicit eviction removes bytes but preserves Downloads' permanent refusal. -/
private def retained (j : Json) : Except String Json := do
  let actions ← j.getObjValAs? (Array Json) "actions"
  let mut cache : Cache := ⟨[], []⟩
  let mut download : Downloads.State := .unfinished [] .authentication
  let mut output := #[]
  for action in actions do
    let mut fetched := 0
    match ← action.getObjValAs? String "op" with
    | "read" =>
      let start ← action.getObjValAs? Nat "start"
      let stop ← action.getObjValAs? Nat "end"
      for offset in List.range' start (stop - start) do
        let address : ByteAddress := ⟨.log .write 1 1, ⟨10, 0, 0⟩, offset⟩
        if address ∉ cache.retained then
          cache ← cacheTransition cache (.fetch address)
          cache ← cacheTransition cache (.commit address)
          fetched := fetched + 1
    | "reopen" => cache ← cacheTransition cache .reopen
    | "evict" =>
      for address in cache.retained do
        cache ← cacheTransition cache (.evict address)
      download := Downloads.evict download 0
    | "wait" => pure ()
    | action => throw s!"unknown cache action {action}"
    output := output.push (Json.mkObj [
      ("fetched", toJson fetched), ("retained", toJson cache.retained.length),
      ("refused", toJson (Downloads.pending download == some .authentication))])
  return toJson output

/-- Whole-object dispositions project the actual provider, authentication and
local byte-commit results into Downloads' ordered checks. A transport failure
wins even when an earlier chunk already proved the contents invalid. -/
private def download (j : Json) : Except String Json := do
  let fetched ← j.getObjValAs? Bool "fetched"
  let authenticated ← j.getObjValAs? Bool "authenticated"
  let hashMatches ← j.getObjValAs? Bool "hash"
  let diskCommitted ← j.getObjValAs? Bool "disk"
  let attempt : Downloads.Attempt := {
    path := .log .write 1 1
    fetched := if fetched then .ok () else .error .network
    authenticated, hashMatches, diskCommitted
    databaseCommitted := true
    range := ⟨0, [1]⟩
    chunks := 1
    last := true
    pin := false
  }
  let result := Downloads.apply (.unfinished [] .network) attempt
  let automatic := match Downloads.pending result with
    | none => false | some reason => Downloads.automatic reason
  let refused := match Downloads.pending result with
    | none => false | some reason => !Downloads.automatic reason
  return Json.mkObj [("complete", toJson (Downloads.completed result)),
    ("refused", toJson refused), ("automatic", toJson automatic)]

private def positions (j : Json) : Except String Json := do
  let actions ← j.getObjValAs? (Array Json) "actions"
  let mut cache : Cache := ⟨[], []⟩
  let mut output := #[]
  for action in actions do
    cache ← cacheTransition cache .reopen
    let size ← action.getObjValAs? Nat "size"
    let revision ← action.getObjValAs? Nat "revision"
    let mut fetched := 0
    for offset in List.range size do
      let address : ByteAddress := ⟨.positions 1, ⟨size, 0, revision⟩, offset⟩
      if address ∉ cache.retained then
        cache ← cacheTransition cache (.fetch address)
        cache ← cacheTransition cache (.commit address)
        fetched := fetched + 1
    output := output.push (toJson fetched)
  return toJson output

private def post (j : Json) : Except String Json := do
  let confirmed ← j.getObjValAs? (Option (List Nat)) "confirmed"
  let present ← j.getObjValAs? Bool "present"
  let current ← j.getObjValAs? (List Nat) "current"
  return toJson (changedPost confirmed present current)

/-- Compare the shared real sender with Trace's executable transitions. Rejected
requests preserve the prior state; an unfinished phase never renews it. -/
private def sendGate (j : Json) : Except String Json := do
  let actions ← j.getObjValAs? (Array Json) "actions"
  let mut sender : Sender := ⟨none, none⟩
  let mut results := #[]
  for action in actions do
    let event ← match ← action.getObjValAs? String "op" with
      | "begin" => IOEvent.beginPass <$> action.getObjValAs? Nat "time"
      | "checked" => do
        let time ← action.getObjValAs? Nat "time"
        let sending ← action.getObjValAs? Bool "send"
        let retired ← action.getObjValAs? Bool "retired"
        pure (.checked time (if sending then .send else .stopped) retired)
      | "invalidate" => pure .invalidate
      | "request" => do
        let time ← action.getObjValAs? Nat "time"
        pure (.request .outside time (.replace (.positions 1) [1]))
      | other => throw s!"unknown send action {other}"
    let next := gateStep sender event
    results := results.push next.isSome
    if let some value := next then sender := value
  return toJson results

/-- `Retired`: a retired writer is drained only when the sample before its
reads exceeds `F + freshFor + day` (`retirement_closed`) and its entry, copy
and write logs each reach a terminal miss (`drain_complete`'s `Scan`), the
logs executed by `readLog` in that order. `polling` then excludes it. -/
private def drain (j : Json) : Except String Json := do
  let finalAt ← j.getObjValAs? Nat "final"
  let sample ← j.getObjValAs? Nat "sample"
  let writer ← j.getObjValAs? Nat "writer"
  let known ← j.getObjValAs? (List Nat) "known"
  let logs ← j.getObjValAs? (Array Json) "logs"
  let kinds := [Kind.entry, Kind.copy, Kind.write]
  let misses ← (kinds.zip logs.toList).mapM fun (kind, log) => do
    let count ← log.getObjValAs? Nat "count"
    let start ← log.getObjValAs? Nat "start"
    let failed ← log.getObjValAs? Bool "failed"
    let store : Store := (List.range count).foldl
      (fun store n => put store (.log kind writer (n + 1)) [n + 1] 0) empty
    let result := readLog (fun _ => store) (fun _ => if failed then some .network else none)
      (fun _ => false) kind writer (count + 2) sample start
    pure (result.stop == .miss)
  let waited := decide (finalAt + freshFor + day < sample)
  let drained := waited && misses.all id
  let life : Nat → Retired.Life := fun w => if w = writer then .removed else .active
  let after := if drained then Retired.retire life writer else life
  return Json.mkObj [("drained", toJson drained),
    ("polled", toJson (Retired.polling after known))]

/-- `PassPhases.run` over IO `readLog`. Phase 1 reads the entry log, then
the copy log once the entries reach their miss: a failed read ends the pass,
a refused entry or an unreadable copy prefix leaves phase 1 incomplete
(`entries_only_reuses_previous`). The previous pass's catch-up
started at second 0; this pass starts `elapsed` seconds later. Write 1 is
attempted or untried; with an attempted write 1, write 2 is untried.
Reports whether an untried write had its first attempt and whether an
attempted write was resent (`incomplete_only_retries`, `gated`). -/
private def phases (j : Json) : Except String Json := do
  let observe (kind : Kind) (field : String) : Except String Execution := do
    let log ← j.getObjVal? field
    let count ← log.getObjValAs? Nat "count"
    let failed ← log.getObjValAs? Bool "failed"
    let refused ← log.getObjValAs? Bool "refused"
    let store : Store := (List.range count).foldl
      (fun store n => put store (.log kind 1 (n + 1)) [n + 1] 0) empty
    pure (readLog (fun _ => store) (fun _ => if failed then some .network else none)
      (fun _ => refused) kind 1 (count + 2) 0 0)
  let attempted ← j.getObjValAs? Bool "attempted"
  let elapsed ← j.getObjValAs? Nat "elapsed"
  let entries ← observe .entry "entry"
  let copies ← observe .copy "copy"
  let unreadable ← (← j.getObjVal? "copy").getObjValAs? Bool "unreadable"
  let plan : PassPhases.Plan := {
    writer := 1, start := elapsed, phase := ⟨entries, copies, !unreadable, .send, false, elapsed⟩,
    copies := [], shares := [], entry := none, admission := none, operations := [],
    attempted := if attempted then [1] else [], untried := if attempted then [2] else [1],
    resumed := [], clock := fun _ => elapsed }
  let sent := (PassPhases.run plan ⟨none, some ⟨0, 0, .send, false⟩⟩).sent.map (·.work)
  return Json.mkObj [
    ("first_attempt", toJson (sent.any fun w => match w with | .first _ => true | _ => false)),
    ("retried", toJson (sent.any fun w => match w with | .retry _ => true | _ => false))]

/-- `SnapshotWrites` driven by an actual device's trace: log reservations,
landings and confirmations, local captures, and snapshot requests. A request
is admitted exactly when `Step.request`'s precondition holds; every landed
snapshot reports whether all its covered numbers are published. -/
private def snapshotWrites (j : Json) : Except String Json := do
  let events ← j.getObjValAs? (Array Json) "events"
  let mut s : SnapshotWrites.State := SnapshotWrites.initial
  let mut snaps : Array Snapshot := #[]
  let mut out := #[]
  for event in events do
    let kind ← event.getObjValAs? String "event"
    let writer := (event.getObjValAs? Nat "writer").toOption.getD 0
    let log := s.logs writer
    match kind with
    | "reserve" =>
      s := SnapshotWrites.changeLog s writer
        { log with reserved := log.reserved ++ [[log.reserved.length + 1]] }
      out := out.push (Json.mkObj [])
    | "land" =>
      match log.reserved.drop log.published.length with
      | bytes :: _ =>
        if log.confirmed ≠ log.published.length then throw "landing before confirmation"
        s := SnapshotWrites.changeLog s writer { log with published := log.published ++ [bytes] }
      | [] => throw "landing an unreserved number"
      out := out.push (Json.mkObj [])
    | "confirm" =>
      if log.published.length ≠ log.confirmed + 1 then throw "confirming without a landing"
      s := SnapshotWrites.changeLog s writer { log with confirmed := log.confirmed + 1 }
      out := out.push (Json.mkObj [])
    | "capture" =>
      let positions ← event.getObjValAs? (List (Nat × Nat)) "positions"
      let snap : Snapshot := ⟨0, writer, snaps.size, 0, 0, positions⟩
      let own := decide (snap.position writer ≤ log.reserved.length)
      let others := positions.all fun (w, n) => w == writer ||
        decide (n ≤ (s.logs w).published.length)
      snaps := snaps.push snap
      s := { s with captured := snap :: s.captured }
      out := out.push (Json.mkObj [("captured", toJson (own && others))])
    | "request" =>
      let index ← event.getObjValAs? Nat "snapshot"
      let some snap := snaps[index]? | throw "request names no capture"
      let admitted := decide (snap ∈ s.captured) &&
        decide (snap.position snap.writer ≤ (s.logs snap.writer).confirmed)
      if admitted then s := { s with requested := snap :: s.requested }
      out := out.push (Json.mkObj [("admitted", toJson admitted)])
    | "landSnapshot" =>
      let index ← event.getObjValAs? Nat "snapshot"
      let some snap := snaps[index]? | throw "landing names no capture"
      if !decide (snap ∈ s.requested) then throw "landing an unrequested snapshot"
      s := { s with landed := snap :: s.landed }
      let published := snap.positions.all fun (w, _) =>
        decide (snap.position w ≤ (s.logs w).published.length)
      out := out.push (Json.mkObj [("published", toJson published)])
    | _ => throw "unknown snapshot write event"
  pure (toJson out)

/-- `Waiting`: the delay after `failures` unsuccessful attempts, the restart
arm `max 0 (min (T - now) W)`, and whether a timer whose ordinary deadline is
`ordinary` and whose provider cooldown ends at `provider` is due at `at`,
before and after an app retry (`app_retry_keeps_cooldown`). -/
private def waiting (j : Json) : Except String Json := do
  let failures ← j.getObjValAs? Nat "failures"
  let deadline ← j.getObjValAs? Int "deadline"
  let now ← j.getObjValAs? Int "now"
  let wait ← j.getObjValAs? Nat "wait"
  let ordinary ← j.getObjValAs? Nat "ordinary"
  let provider ← j.getObjValAs? Nat "provider"
  let time ← j.getObjValAs? Nat "at"
  let timer : Waiting.Timer := ⟨ordinary, provider⟩
  return Json.mkObj [
    ("delay", toJson (Waiting.delay failures)),
    ("restart", toJson (Waiting.restart deadline now wait)),
    ("due", toJson (Waiting.due timer time)),
    ("retried", toJson (Waiting.due (Waiting.appRetry timer time) time))]

private def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "check" with
  | "discovery" => discovery j
  | "counts" => counts j
  | "catalog" => catalog j
  | "needs" => needs j
  | "recent" => recent j
  | "retained" => retained j
  | "download" => download j
  | "positions" => positions j
  | "post" => post j
  | "send_gate" => sendGate j
  | "drain" => drain j
  | "phases" => phases j
  | "snapshot_writes" => snapshotWrites j
  | "waiting" => waiting j
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
