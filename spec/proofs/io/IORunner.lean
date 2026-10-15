import CovenIO.Execution
import CovenIO.Trace
import CovenIO.Downloads
import CovenIO.Retired
import CovenIO.PassPhases
import CovenIO.SnapshotWrites
import CovenIO.Waiting
import CovenIO.WaitingEvidence
import CovenIO.WaitingReopen
import CovenIO.PrerequisiteRows
import CovenIO.Wakes
import CovenIO.Writers
import CovenIO.DroppedRegistrations
import CovenIO.RaiseCoverage
import CovenIO.RetentionDeadline
import CovenIO.RaiseHistory
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

/-- `Deletable` on the catalog's current snapshot: one write of `writer`
numbered `number` in audience 0, covered or not, with every dependency
final or not, each active reader's posted position for that writer, and
storage times in seconds. Thirty storage days count at equality. -/
private def deletable (j : Json) : Except String Json := do
  let number ← j.getObjValAs? Nat "number"
  let covered ← j.getObjValAs? Bool "covered"
  let final ← j.getObjValAs? Bool "final"
  let readers ← j.getObjValAs? (List (Nat × Nat)) "posted"
  let write : Write := ⟨0, number, ← j.getObjValAs? Nat "stored", [0]⟩
  let current : Current := fun a =>
    if covered && a == 0 then some ⟨0, 0, 1, 0, 0, [(0, number)]⟩ else none
  let posted (reader _writer : Nat) : Nat :=
    ((readers.find? (·.1 == reader)).map (·.2)).getD 0
  return toJson (decide (Deletable current (readers.map (·.1)) posted
    (← j.getObjValAs? Nat "time") (final = true) write))

/-- `RetentionDeadline.refreshDue` and the historical-witness-only trigger
`witnessedRefreshDue`, for one write of `writer` 0 numbered `number` with a
part in audience 0, under the landed `snapshots` of audience 0 (as in
`catalog`), each active reader's posted position for writer 0, and storage
times in seconds. -/
private def refresh (j : Json) : Except String Json := do
  let number ← j.getObjValAs? Nat "number"
  let readers ← j.getObjValAs? (List (Nat × Nat)) "posted"
  let snapshots ← (← j.getObjValAs? (Array Json) "snapshots").toList.mapM fun s => do
    pure (Snapshot.mk 0 (← s.getObjValAs? Nat "writer") (← s.getObjValAs? Nat "number")
      (← s.getObjValAs? Nat "stored") (← s.getObjValAs? Nat "caught")
      (← s.getObjValAs? (List (Nat × Nat)) "positions"))
  let write : Write := ⟨0, number, ← j.getObjValAs? Nat "stored", [0]⟩
  let state : SnapshotPublication.State :=
    ⟨← j.getObjValAs? Nat "time", readers.map (·.1), [write], [], snapshots, [], []⟩
  let posted := readers.map fun (reader, position) => (reader, 0, position)
  let final ← j.getObjValAs? Bool "final"
  let readable ← j.getObjValAs? Bool "readable"
  return Json.mkObj [
    ("due", toJson (RetentionDeadline.refreshDue .writes state posted final readable write 0)),
    ("witnessed", toJson
      (RetentionDeadline.witnessedRefreshDue .writes state posted final readable write 0))]

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

/-- A request named by the Rust trace: its actor (`outside` by default) and
kind (a positions replacement by default). File kinds name writer 1's file 1;
resuming a recorded session queries its status. -/
private def traceRequest (action : Json) : Except String IOEvent := do
  let time ← action.getObjValAs? Nat "time"
  let actor ← match (action.getObjValAs? String "actor").toOption.getD "outside" with
    | "pass" => pure Actor.pass
    | "upload" => pure Actor.upload
    | "outside" => pure Actor.outside
    | other => throw s!"unknown actor {other}"
  let request ← match (action.getObjValAs? String "kind").toOption.getD "positions" with
    | "positions" => pure (Request.replace (.positions 1) [1])
    | "create" => pure (Request.create (.file 1 1) [1])
    | "begin" => pure (Request.beginUpload 1 1)
    | "part" => pure (Request.uploadPart 1 1 [1])
    | "finish" => pure (Request.finishUpload 1 1)
    | "resume" | "status" => pure (Request.status (.file 1 1))
    | "read" => pure (Request.get (.file 1 1))
    | other => throw s!"unknown request kind {other}"
  pure (.request actor time request)

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
      | "request" => traceRequest action
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

/-- `SnapshotPublication.step` over one audience driven by a device trace:
writes landing, a catch-up (`beginCatchup`, then `read` of every writer
until its terminal miss, all at one time), `prepare`, `land` and ticks. A
catch-up first drops its writer's earlier session that never prepared: that
session is local state the device discarded. Each event reports whether its
step succeeded; a catch-up also reports the positions it reached, a landing
whether it counts, and every event the `catalogCurrent .writes` selection. -/
private def publication (j : Json) : Except String Json := do
  let writers ← j.getObjValAs? (List Nat) "writers"
  let events ← j.getObjValAs? (Array Json) "events"
  let order := SnapshotOrder.writes
  let mut s := SnapshotPublication.initial writers
  let mut out := #[]
  for event in events do
    let time ← event.getObjValAs? Nat "time"
    let kind ← event.getObjValAs? String "event"
    let writer := (event.getObjValAs? Nat "writer").toOption.getD 0
    let number := (event.getObjValAs? Nat "number").toOption.getD 0
    let mut fields : List (String × Json) := []
    let mut ok := false
    match kind with
    | "write" =>
      if let some next := SnapshotPublication.step order s time (.write writer [0]) then
        s := next
        ok := true
    | "catchup" =>
      let fresh := { s with catching := s.catching.filter (·.snapshot.writer != writer) }
      if let some begun := SnapshotPublication.step order fresh time (.beginCatchup 0 writer number) then
        let mut t := begun
        let mut complete := true
        for logWriter in writers do
          let mut missed := false
          for _ in [0:4096] do
            if missed then
              break
            match SnapshotPublication.step order t time (.read writer logWriter) with
            | none =>
              complete := false
              missed := true
            | some next =>
              t := next
              missed := (t.catching.find? (·.snapshot.writer == writer)).any
                (·.missed.contains logWriter)
          if !missed then
            complete := false
        s := t
        ok := complete
        let positions := match t.catching.find? (·.snapshot.writer == writer) with
          | some c => (c.snapshot.positions.filter (fun (p : Nat × Nat) => p.2 > 0)).mergeSort
              (fun a b => decide (a.1 ≤ b.1))
          | none => []
        fields := [("positions", toJson positions)]
    | "prepare" =>
      if let some next := SnapshotPublication.step order s time (.prepare writer) then
        s := next
        ok := true
    | "land" =>
      if let some next := SnapshotPublication.step order s time (.land 0 writer number) then
        s := next
        ok := true
        let counted := (s.landed.head?.map (·.counts)).getD false
        fields := [("counts", toJson counted)]
    | "tick" =>
      if let some next := SnapshotPublication.step order s time .tick then
        s := next
        ok := true
    | other => throw s!"unknown publication event {other}"
    let current := match SnapshotPublication.current order s 0 with
      | some snap => toJson [snap.writer, snap.number]
      | none => Json.null
    out := out.push (Json.mkObj ([("ok", toJson ok), ("current", current)] ++ fields))
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

/-- `Waiting.live` for one operation's wait: monotonic `advance`s (the wall
clock moves with them), `back`s that set the wall clock back, `commit`s by
other work changing one prerequisite, automatic `attempt`s, each starting only
when `Session.ready` and failing on its `prereq` after other work committed the
prerequisites in `raced` while it ran, `reopen`s, and `cut`s: an attempt
started when due, with `raced` commits, then the store reopened before it
ended. An attempt's own commits are no evidence. The runner tracks the wall
clock and the wait's persisted wall deadline `T`, and gives each reopen the
`jump` that makes the model's restart arm `max 0 (min (T - wall) W)`. Reports,
per attempt or cut, whether it was due and the persisted failure count
afterward (`evidence_due_once`, `evidence_keeps_backoff`,
`racing_commit_wakes`, `unrelated_commit_ignored`, `reopen_within_wait`,
`reopen_mid_attempt_restarts`). -/
private def evidence (j : Json) : Except String Json := do
  let actions ← j.getObjValAs? (Array Json) "actions"
  let mut s : Waiting.Session := ⟨0, 0, none, none⟩
  let mut wall : Int := 0
  let mut persisted : Int := 0
  let mut out := #[]
  for action in actions do
    let op ← action.getObjValAs? String "op"
    let jump : Int := match s.armed with
      | some a => (a.deadline : Int) - s.now - (persisted - wall)
      | none => 0
    let started : List Waiting.Life ← if s.ready && (op == "attempt" || op == "cut") then do
        let raced ← action.getObjValAs? (List Nat) "raced"
        pure (.event .start :: raced.map fun q => .event (.commit q))
      else pure []
    let mut events : List Waiting.Life := []
    match op with
    | "advance" =>
        let seconds ← action.getObjValAs? Nat "seconds"
        events := [.event (.tick seconds)]
        wall := wall + seconds
    | "back" => wall := wall - (← action.getObjValAs? Nat "seconds")
    | "commit" => events := [.event (.commit (← action.getObjValAs? Nat "prereq"))]
    | "attempt" =>
        let prereq ← action.getObjValAs? Nat "prereq"
        if !started.isEmpty then events := started ++ [.event (.finish prereq)]
    | "reopen" => events := [.reopen jump]
    | "cut" => events := started ++ [.reopen jump]
    | other => throw s!"unknown evidence action {other}"
    let some next := Waiting.live s events | throw "invalid wait transition"
    if op == "attempt" && !started.isEmpty then
      persisted := wall + ((next.armed.map Waiting.Armed.wait).getD 0)
    if op == "attempt" || op == "cut" then
      out := out.push (Json.mkObj [("due", toJson s.ready),
        ("failures", toJson ((next.armed.map (·.failures)).getD 0))])
    s := next
  return toJson out

open PrerequisiteRows in
private def prereqOf (j : Json) : Except String Prereq := do
  match ← j.getObjValAs? String "kind" with
  | "entry" => pure (.entry (← j.getObjValAs? Nat "device") (← j.getObjValAs? Nat "number"))
  | "key_copy" => pure (.keyCopy (← j.getObjValAs? Nat "writer") (← j.getObjValAs? Nat "number"))
  | "write" => pure (.write (← j.getObjValAs? Nat "device") (← j.getObjValAs? Nat "number"))
  | "device" => pure (.device (← j.getObjValAs? Nat "device"))
  | "peer_positions" => pure (.peerPositions (← j.getObjValAs? Nat "device"))
  | "reload" => pure (.reload (← j.getObjValAs? String "audience"))
  | "finality" => pure .finality
  | "boundaries" => pure .boundaries
  | "provider_access" => pure (.providerAccess (← j.getObjValAs? String "account"))
  | "operation" => pure (.operation (← j.getObjValAs? Nat "id"))
  | other => throw s!"unknown prerequisite {other}"

open PrerequisiteRows in
private def prereqJson : Prereq → Json
  | .entry d n => Json.mkObj [("kind", "entry"), ("device", toJson d), ("number", toJson n)]
  | .keyCopy w n => Json.mkObj [("kind", "key_copy"), ("writer", toJson w), ("number", toJson n)]
  | .write d n => Json.mkObj [("kind", "write"), ("device", toJson d), ("number", toJson n)]
  | .device d => Json.mkObj [("kind", "device"), ("device", toJson d)]
  | .peerPositions d => Json.mkObj [("kind", "peer_positions"), ("device", toJson d)]
  | .reload a => Json.mkObj [("kind", "reload"), ("audience", toJson a)]
  | .finality => Json.mkObj [("kind", "finality")]
  | .boundaries => Json.mkObj [("kind", "boundaries")]
  | .providerAccess a => Json.mkObj [("kind", "provider_access"), ("account", toJson a)]
  | .operation id => Json.mkObj [("kind", "operation"), ("id", toJson id)]

open PrerequisiteRows in
private def patternJson : Pattern → Json
  | .boundaries => Json.mkObj [("table", "_coven_boundaries"), ("key", Json.mkObj [])]
  | .row r =>
    let (table, key) : String × List (String × Json) := match r with
      | .storeLog d n => ("_coven_store_log", [("device", toJson d), ("number", toJson n)])
      | .input kind w n => ("_coven_log_inputs",
          [("kind", match kind with | .entry => "entry" | .key => "key"),
           ("writer", toJson w), ("number", toJson n)])
      | .entryUpload d n => ("_coven_store_log_uploads", [("device", toJson d), ("number", toJson n)])
      | .keyUpload w n => ("_coven_key_uploads", [("writer", toJson w), ("number", toJson n)])
      | .position d => ("_coven_positions", [("device", toJson d)])
      | .upload d n => ("_coven_uploads", [("device", toJson d), ("number", toJson n)])
      | .landing d n => ("_coven_write_landings", [("device", toJson d), ("number", toJson n)])
      | .registration d => ("_coven_devices", [("device", toJson d)])
      | .peerPositions d => ("_coven_peer_positions", [("device", toJson d)])
      | .reload a => ("_coven_data_reloads", [("audience", toJson a)])
      | .finality => ("_coven_finality", [])
      | .boundary k => ("_coven_boundaries", [("key", toJson k)])
      | .access a => ("_coven_provider_access", [("account", toJson a)])
      | .operation id => ("_coven_operations", [("id", toJson id)])
    Json.mkObj [("table", toJson table), ("key", Json.mkObj key)]

/-- `PrerequisiteRows.Prereq.rows`: the rows, by table and primary key, a wait
on one prerequisite watches (`reading_frame`, `named_row_matters`,
`only_partial_advances`). -/
private def prerequisiteRows (j : Json) : Except String Json := do
  let prereq ← prereqOf (← j.getObjVal? "prereq")
  return toJson (prereq.rows.map patternJson)

open PrerequisiteRows in
private def objectOf (j : Json) : Except String PrerequisiteRows.Object := do
  match ← j.getObjValAs? String "kind" with
  | "entry" => pure (.entry (← j.getObjValAs? Nat "device") (← j.getObjValAs? Nat "number"))
  | "key_copy" => pure (.keyCopy (← j.getObjValAs? Nat "writer") (← j.getObjValAs? Nat "number"))
  | "write" => pure (.write (← j.getObjValAs? Nat "device") (← j.getObjValAs? Nat "number"))
  | "stored" => pure .stored
  | other => throw s!"unknown object {other}"

/-- `PrerequisiteRows.needs`: what a wait for one recorded condition watches,
given the pending reload operation a reload wait names. -/
private def waitRows (j : Json) : Except String Json := do
  let reason ← j.getObjVal? "reason"
  let parsed : PrerequisiteRows.Reason ← match ← reason.getObjValAs? String "kind" with
    | "object" => pure (.object (← objectOf (← reason.getObjVal? "object")))
    | "catch_up" => pure .catchUp
    | "registration" => pure (.registration (← reason.getObjValAs? Nat "device"))
    | "schema_publication" => pure .schemaPublication
    | "reload" => pure (.reload (← reason.getObjValAs? String "audience"))
    | "positions" => pure (.positions (← reason.getObjValAs? Nat "device"))
    | "entry_finality" => pure .entryFinality
    | "key_unavailable" => pure .keyUnavailable
    | "provider_access" => pure (.providerAccess (← reason.getObjValAs? String "account"))
    | "storage" => pure .storage
    | "other" => pure .other
    | other => throw s!"unknown reason {other}"
  let reload ← j.getObjValAs? (Option Nat) "reload"
  let needs := PrerequisiteRows.needs parsed reload
  return Json.mkObj [("prereqs", toJson (needs.prereqs.map prereqJson)),
    ("key", toJson needs.key), ("connection", toJson needs.connection)]

/-- `Wakes.step` over the write queue's commits: app writes insert, attempts
update, confirmations remove the oldest row. A `take` is a pass start: it
reports whether the observation saw a commit since the previous start, then
runs a pass (`write_never_lost`, `bookkeeping_retains_none`). -/
private def wakes (j : Json) : Except String Json := do
  let actions ← j.getObjValAs? (Array Json) "actions"
  let mut s := Wakes.initial []
  let mut written := 0
  let mut out := #[]
  for action in actions do
    let mut events : List Wakes.Event := []
    match ← action.getObjValAs? String "op" with
    | "write" =>
      written := written + 1
      events := [.commit (.insert written)]
    | "update" => events := [.commit .update]
    | "remove" =>
      match s.queue with
      | w :: _ => events := [.commit (.remove w)]
      | [] => throw "removing from an empty queue"
    | "take" =>
      out := out.push (toJson s.observed)
      events := [.command, .take, .capture, .finish]
    | other => throw s!"unknown wake action {other}"
    let some next := Wakes.run s events | throw "invalid loop transition"
    s := next
  return toJson out

/-- `Writers.known`: the received entries, by writing device and registered
device, and remembered folders give the write logs probed, less drained ids
and the reader itself. No replay verdict is an input. -/
private def writers (j : Json) : Except String Json := do
  let entries ← j.getObjValAs? (Array Json) "entries"
  let listed ← j.getObjValAs? (List Nat) "listed"
  let drained ← j.getObjValAs? (List Nat) "drained"
  let own ← j.getObjValAs? Nat "self"
  let blank : CovenStorelog.Entry := ⟨0, 0, [], .rotateKey .store 0⟩
  let log ← entries.mapM fun e => do
    let writer ← e.getObjValAs? Nat "writer"
    let action := match ← e.getObjValAs? (Option Nat) "registers" with
      | some device => CovenStorelog.Action.addDevice 0 device
      | none => .rotateKey .store 0
    pure ({ blank with device := writer, action } : CovenStorelog.Entry)
  let life : Nat → Retired.Life := fun w => if w ∈ drained then .drained else .active
  let known := Writers.known (fun w => log.getD w blank) (List.range log.size) listed
  return toJson ((Retired.polling life known).filter (· ≠ own) |>.mergeSort)

/-- `Retired.observeFiles` folded over one removed writer's file-folder
listings: each either fails or completes with the file ids it found. After
each listing, the scan is `null` while pending or the ids its completion
recorded; a completed scan is never replaced (`completed_scan_retained`). -/
private def fileScan (j : Json) : Except String Json := do
  let writer ← j.getObjValAs? Nat "writer"
  let listings ← j.getObjValAs? (Array Json) "listings"
  let mut scan := Retired.FileScan.pending
  let mut results := #[]
  for listing in listings do
    let result : Except Failure (List Path) ←
      match listing.getObjValAs? (List Nat) "found" with
      | .ok ids => pure (.ok (ids.map (Path.file writer)))
      | .error _ => do
          let _ ← listing.getObjValAs? Bool "failed"
          pure (.error .network)
    scan := Retired.observeFiles scan result
    results := results.push (match scan with
      | .pending => Json.null
      | .complete paths => toJson (paths.filterMap fun
          | .file _ id => some id
          | _ => none))
  return Json.arr results

/-- `Writers.retirements` over Rust's replay verdicts: the devices that kept
retirements and dropped registrations of unregistered devices retire, late
registrations among the dropped ones as in §9's timed replay. Entry `i` is
the `i`th received entry; its storage time is irrelevant here. -/
private def retirementDevices (j : Json) : Except String Json := do
  let entries ← j.getObjValAs? (Array Json) "entries"
  let kept ← j.getObjValAs? (List Nat) "kept"
  let dropped ← j.getObjValAs? (List Nat) "dropped"
  let blank : CovenStorelog.Entry := ⟨0, 0, [], .rotateKey .store 0⟩
  let log ← entries.mapM fun e => do
    let author ← e.getObjValAs? Nat "author"
    let device ← e.getObjValAs? Nat "writer"
    let action ← match ← e.getObjValAs? String "action" with
      | "create" => pure (CovenStorelog.Action.create "")
      | "addDevice" => do pure (.addDevice author (← e.getObjValAs? Nat "device"))
      | "removeDevice" => do pure (.removeDevice 0 (← e.getObjValAs? Nat "device"))
      | "removeMember" => do pure (.removeMember (← e.getObjValAs? Nat "member") [])
      | _ => pure (.rotateKey .store 0)
    pure ({ blank with author, device, action } : CovenStorelog.Entry)
  let result : CovenStorelog.Result := ⟨CovenStorelog.State.empty, kept, dropped⟩
  let retired := (Writers.retirements (fun w => log.getD w blank) result id).flatMap (·.devices)
  return toJson retired.eraseDups.mergeSort
/-- `RaiseHistory.Held` for one part audience: under every possible boundary
(`null` for none, or a named snapshot's `[writer, number]`), the newest
counting snapshot it admits covers the write. A snapshot admits under a
named boundary when it is that snapshot or its signed view read the entry
naming it (`reads`). A raise entry may name a snapshot that is not listed;
only its readers are then admitted. -/
private def held (j : Json) : Except String Json := do
  let audience ← j.getObjValAs? Nat "audience"
  let writer ← j.getObjValAs? Nat "writer"
  let number ← j.getObjValAs? Nat "number"
  let entries ← j.getObjValAs? (Array Json) "snapshots"
  let parsed ← entries.toList.mapM fun s => do
    let snapshotWriter ← s.getObjValAs? Nat "writer"
    let snapshotNumber ← s.getObjValAs? Nat "number"
    let stored ← s.getObjValAs? Nat "stored"
    let caught ← s.getObjValAs? Nat "caught"
    let positions ← s.getObjValAs? (List (Nat × Nat)) "positions"
    let reads ← s.getObjValAs? (List (Nat × Nat)) "reads"
    pure (Snapshot.mk audience snapshotWriter snapshotNumber stored caught positions, reads)
  let landed := parsed.map Prod.fst
  let find (path : Nat × Nat) : Except String Snapshot :=
    match landed.find? (fun s => s.writer == path.1 && s.number == path.2) with
    | some s => .ok s
    | none => .ok ⟨audience, path.1, path.2, 0, 0, []⟩
  let reads := (← parsed.mapM fun (s, named) => named.mapM fun path => do
    pure (s, ← find path)).flatten
  let possible ← (← j.getObjValAs? (Array Json) "possible").toList.mapM fun b =>
    match b with
    | .null => pure none
    | path => do some <$> find (← fromJson? path)
  let write : Write := ⟨writer, number, 0, [audience]⟩
  return toJson (possible.all fun b =>
    decide (RaiseCoverage.RaiseHistory.CoversIn
      (RaiseCoverage.RaiseHistory.selected .writes landed reads audience b) write audience))

private def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "check" with
  | "discovery" => discovery j
  | "counts" => counts j
  | "deletable" => deletable j
  | "refresh" => refresh j
  | "catalog" => catalog j
  | "needs" => needs j
  | "recent" => recent j
  | "retained" => retained j
  | "download" => download j
  | "positions" => positions j
  | "post" => post j
  | "send_gate" => sendGate j
  | "drain" => drain j
  | "file_scan" => fileScan j
  | "writers" => writers j
  | "retirements" => retirementDevices j
  | "phases" => phases j
  | "snapshot_writes" => snapshotWrites j
  | "publication" => publication j
  | "waiting" => waiting j
  | "evidence" => evidence j
  | "prerequisite_rows" => prerequisiteRows j
  | "wait_rows" => waitRows j
  | "wakes" => wakes j
  | "held" => held j
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
