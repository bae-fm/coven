import CovenIO.Trace
import CovenIO.Downloads
import CovenIO.Retired
import CovenIO.WaitingReopen
import CovenIO.Wakes
import Lean

open Lean CovenIO

private def cacheTransition (cache : Cache) (event : CacheEvent) : Except String Cache :=
  match cacheStep cache event with
  | some next => .ok next
  | none => .error "invalid cache transition"

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
        let address : ByteAddress := ⟨.slot 1, ⟨10, 0, 0⟩, offset⟩
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
    path := .slot 1
    fetched := if fetched then .ok () else .error .network
    authenticated, hashMatches, diskCommitted
    databaseCommitted := true
    range := ⟨0, [1]⟩
    chunks := 1
    last := true
    pin := false
  }
  let result := Downloads.apply (.unfinished [] .network) attempt
  let retry := (Downloads.pending result).map Downloads.retry
  return Json.mkObj [("complete", toJson (Downloads.completed result)),
    ("refused", toJson (retry == some .afterUpdate)),
    ("next_call", toJson (retry == some .nextCall))]

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

/-- `Retired.Folder.step` folded over one removed writer's folder events: an
upload of a file id, a pass's listing and the app's cleanup listing, each of
which completes or fails. After each event, `null` while no listing has
completed, else the candidate file ids in increasing order. -/
private def fileScan (j : Json) : Except String Json := do
  let events ← (← j.getObjValAs? (Array Json) "events").toList.mapM fun e => do
    if let .ok file := e.getObjValAs? Nat "upload" then
      return Retired.FolderEvent.upload file
    if let .ok completes := e.getObjValAs? Bool "pass" then
      return .pass completes
    if let .ok completes := e.getObjValAs? Bool "clean_up" then
      return .cleanUp completes
    throw "unknown folder event"
  let mut f := Retired.Folder.initial
  let mut results := #[]
  for event in events do
    f := f.step event
    results := results.push (if f.listed then toJson (f.candidates.mergeSort (· ≤ ·)) else Json.null)
  return Json.arr results

private def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "check" with
  | "retained" => retained j
  | "download" => download j
  | "waiting" => waiting j
  | "evidence" => evidence j
  | "wakes" => wakes j
  | "file_scan" => fileScan j
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
