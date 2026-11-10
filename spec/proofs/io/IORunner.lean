import CovenIO.Trace
import CovenIO.Downloads
import CovenIO.Keep
import CovenIO.Retired
import CovenIO.WaitingReopen
import CovenIO.Wakes
import CovenIO.SyncState
import CovenIO.RetryAnswer
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
local byte-commit results into Downloads' ordered checks, for a read or a pin
of a file nothing is pending for. A transport failure wins even when an
earlier chunk already proved the contents invalid. A pin's attempt is its
prepare, and the write that follows a successful one commits it. -/
private def download (j : Json) : Except String Json := do
  let fetched ← j.getObjValAs? Bool "fetched"
  let authenticated ← j.getObjValAs? Bool "authenticated"
  let hashMatches ← j.getObjValAs? Bool "hash"
  let diskCommitted ← j.getObjValAs? Bool "disk"
  let pin ← j.getObjValAs? Bool "pin"
  let attempt : Downloads.Attempt := {
    path := .slot 1
    fetched := if fetched then .ok () else .error .network
    authenticated, hashMatches, diskCommitted
    databaseCommitted := true
    range := ⟨0, [1]⟩
    chunks := 1
    last := true
    pin
  }
  let prepared := Downloads.apply (.idle []) attempt
  let retry := (Downloads.pending prepared).map Downloads.retry
  let result := if pin && retry.isNone then Downloads.commitPin prepared [attempt.range] true
    else prepared
  return Json.mkObj [("complete", toJson (Downloads.completed result)),
    ("refused", toJson (retry == some .afterUpdate)),
    ("next_call", toJson (retry == some .nextCall))]

/-- A pin of a two-chunk file fails, offline (`network`) or over a second
chunk that fails authentication (`refused`), and then a read succeeds: the
first chunk alone, or both chunks in order, which completes the file. What
stays pending is `Downloads.read_settles`. -/
private def readSettles (j : Json) : Except String Json := do
  let reason ← match ← j.getObjValAs? String "before" with
    | "network" => pure Downloads.Reason.network
    | "refused" => pure Downloads.Reason.authentication
    | other => throw s!"unknown pin failure {other}"
  let whole ← j.getObjValAs? Bool "whole"
  let attempt (index : Nat) (pin : Bool) (failure : Option Downloads.Reason) :
      Downloads.Attempt := {
    path := .slot 1
    fetched := if failure == some .network then .error .network else .ok ()
    authenticated := failure != some .authentication
    hashMatches := true
    diskCommitted := true
    databaseCommitted := true
    range := ⟨index, [1]⟩
    chunks := 2
    last := index == 1
    pin
  }
  let pinned := Downloads.apply (.idle []) (attempt 1 true (some reason))
  let first := Downloads.apply pinned (attempt 0 false none)
  let read := if whole then Downloads.apply first (attempt 1 false none) else first
  let retry := (Downloads.pending read).map Downloads.retry
  return Json.mkObj [("refused", toJson (retry == some .afterUpdate)),
    ("next_call", toJson (retry == some .nextCall))]

/-- A kept pin's attempt (`Keep.reads`): whether it reads storage, for an
app file whose request keeps coven's copy, which does or doesn't check out,
or for a user's file. -/
private def keep (j : Json) : Except String Json := do
  let kind ← match ← j.getObjValAs? String "kind" with
    | "app" => pure Keep.Kind.app
    | "user" => pure Keep.Kind.user
    | other => throw s!"unknown file kind {other}"
  let checks ← j.getObjValAs? Bool "checks"
  let kept := kind == .app
  let reads := Keep.reads kind kept checks
  return Json.mkObj [("storage", toJson (reads.contains .storage))]

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
`reopen_mid_attempt_restarts`).

`WaitingNetwork` adds the reports, which an owner acts on between attempts:
a `signal` is the network report, ending a wait for the network
(`Waiting.network`), and a `retry` the person's retry, ending any wait. Each
saves the wait it ends with no full wait left, so a later reopen arms none. A
report that arrives during an attempt is a report right after it. -/
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
    | "signal" => events := [.event (.report false)]
    | "retry" => events := [.event (.report true)]
    | "attempt" =>
        let prereq ← action.getObjValAs? Nat "prereq"
        if !started.isEmpty then events := started ++ [.event (.finish prereq)]
    | "reopen" => events := [.reopen jump]
    | "cut" => events := started ++ [.reopen jump]
    | other => throw s!"unknown evidence action {other}"
    let some next := Waiting.live s events | throw "invalid wait transition"
    if op == "attempt" && !started.isEmpty then
      persisted := wall + ((next.armed.map Waiting.Armed.wait).getD 0)
    if (op == "signal" || op == "retry") && next.armed != s.armed then
      persisted := wall
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

/-- `Retired.Folder.step` folded over one gone writer's folder events: an
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

namespace RetryAnswerCase
open CovenIO.RetryAnswer

private def outcomeName : Outcome → String
  | .synced => "synced"
  | .failed => "failed"
  | .coolingDown => "coolingDown"
  | .notSyncing => "notSyncing"

private def phaseName : Phase → String
  | .unasked => "unasked"
  | .waiting => "waiting"
  | .inPass => "inPass"
  | .answered o => outcomeName o

private def effectName : Effect → String
  | .pass => "pass"
  | .coolingDown => "coolingDown"
  | .failed => "failed"
  | .notSyncing => "notSyncing"

/-- An event: its name, or `{"held": null | "cooldown" | "failure"}` or
`{"finish": "synced" | "failed" | "stopped"}`. -/
private def event (j : Json) : Except String Event := do
  if let .ok name := j.getStr? then
    return ← match name with
      | "ask" => pure .ask
      | "report" => pure .report
      | "start" => pure .start
      | "stop" => pure .stop
      | "plan" => pure .plan
      | "wait" => pure .wait
      | "begin" => pure .begin
      | "connected" => pure .connected
      | other => throw s!"unknown retry event {other}"
  if let .ok held := j.getObjVal? "held" then
    if held.isNull then return .held none
    return ← match ← held.getStr? with
      | "cooldown" => pure (.held (some .cooldown))
      | "failure" => pure (.held (some .failure))
      | other => throw s!"unknown hold {other}"
  if let .ok finish := j.getObjValAs? String "finish" then
    return ← match finish with
      | "synced" => pure (.finish .synced)
      | "failed" => pure (.finish .failed)
      | "stopped" => pure (.finish .stopped)
      | other => throw s!"unknown pass end {other}"
  throw s!"unknown retry event {j.compress}"

/-- `RetryAnswer.step` from `initial` over `events`. After each, whether the
model takes the event (`step` gives a state; the state stays otherwise), where
every report number's request stands, and the effect a retry made then would
have. -/
def run (j : Json) : Except String Json := do
  let events ← (← j.getObjValAs? (Array Json) "events").mapM event
  let mut s := initial
  let mut out := #[]
  for e in events do
    let next := step s e
    if let some t := next then s := t
    let requests := (List.range s.reported).map fun m => phaseName (s.request (m + 1))
    out := out.push (Json.mkObj [("applied", toJson next.isSome),
      ("requests", toJson requests), ("effect", toJson (effectName s.effect))])
  return toJson out

end RetryAnswerCase

namespace SyncStateCase
open CovenIO.SyncState

private def failures : List (String × CovenIO.SyncState.Failure) := [
    ("providerUnavailable", .providerUnavailable),
    ("accessNotRegistered", .accessNotRegistered),
    ("accessNotGranted", .accessNotGranted),
    ("dropboxAppFolderNotShareable", .dropboxAppFolderNotShareable),
    ("catchUpRequired", .catchUpRequired),
    ("network", .network),
    ("corrupted", .corrupted),
    ("authentication", .authentication),
    ("permissionDenied", .permissionDenied),
    ("notFound", .notFound),
    ("alreadyExists", .alreadyExists),
    ("conflict", .conflict),
    ("containerNotFound", .containerNotFound),
    ("regionMismatch", .regionMismatch),
    ("storeExists", .storeExists),
    ("clockSkew", .clockSkew),
    ("ownLocationRequired", .ownLocationRequired),
    ("quotaExceeded", .quotaExceeded),
    ("rateLimited", .rateLimited),
    ("invalidConfiguration", .invalidConfiguration),
    ("refused", .refused),
    ("protocol", .protocol),
    ("invalidPath", .invalidPath),
    ("invalidRange", .invalidRange),
    ("notStoreOwner", .notStoreOwner),
    ("inviteeIsStoreOwner", .inviteeIsStoreOwner),
    ("sharingRefused", .sharingRefused),
    ("accountIdUnavailable", .accountIdUnavailable),
    ("invitationMismatch", .invitationMismatch),
    ("sessionMismatch", .sessionMismatch),
    ("sessionExpired", .sessionExpired),
    ("sessionIdentityUnknown", .sessionIdentityUnknown),
    ("invalidPart", .invalidPart),
    ("singleRequestTooLarge", .singleRequestTooLarge),
    ("copyTooLarge", .copyTooLarge),
    ("encoding", .encoding),
    ("file", .file),
    ("memberKeysMissing", .memberKeysMissing),
    ("bug", .bug)]

private def reasons : List (String × Reason) := [
    ("missing", .missing),
    ("waits", .waits),
    ("keyUnavailable", .keyUnavailable),
    ("updateRequired", .updateRequired),
    ("noStorage", .noStorage),
    ("refused", .refused),
    ("writesRefused", .writesRefused),
    ("providerPending", .providerPending),
    ("accountInUse", .accountInUse),
    ("accessRemains", .accessRemains),
    ("fileLost", .fileLost),
    ("fileSource", .fileSource),
    ("originalMissing", .originalMissing),
    ("originalChanged", .originalChanged),
    ("rejected", .rejected),
    ("paused", .paused),
    ("removed", .removed),
    ("memberKeysReplaced", .memberKeysReplaced)]

private def locals : List (String × Local) := [
    ("database", .database), ("diskFull", .diskFull), ("diskDenied", .diskDenied),
    ("diskOther", .diskOther), ("keyCustody", .keyCustody),
    ("crypto", .crypto), ("operation", .operation), ("shutdown", .shutdown),
    ("bug", .bug)]

private def named {α : Type} (table : List (String × α)) (what name : String) : Except String α :=
  match table.lookup name with
  | some value => .ok value
  | none => .error s!"unknown {what} {name}"

private def failure (j : Json) : Except String CovenIO.SyncState.Failure := do named failures "failure" (← j.getStr?)

/-- A reason: its name, or `{"storage": failure, "answered": bool}`,
`{"failed": local}` or `{"cleanup": [operation, cleanup]}`. -/
private partial def reason (j : Json) : Except String Reason := do
  if let .ok name := j.getStr? then return ← named reasons "reason" name
  if let .ok f := j.getObjVal? "storage" then
    return .storage (← failure f) (← j.getObjValAs? Bool "answered")
  if let .ok l := j.getObjValAs? String "failed" then return .failed (← named locals "local failure" l)
  if let .ok pair := j.getObjValAs? (Array Json) "cleanup" then
    if h : pair.size = 2 then return .cleanup (← reason pair[0]) (← reason pair[1])
  throw s!"unknown reason {j.compress}"

/-- A failed pass: `offline`, `updateRequired`, `removed`, `deviceIdInUse`,
`other`, or `{"storage": failure}`, `{"operation": reason}`,
`{"blocked": reason}`. -/
private def passFailure (j : Json) : Except String PassFailure := do
  if let .ok name := j.getStr? then
    return ← named [("offline", .offline), ("updateRequired", .updateRequired), ("removed", .removed),
      ("deviceIdInUse", .deviceIdInUse), ("other", .other)] "pass failure" name
  if let .ok f := j.getObjVal? "storage" then return .storage (← failure f)
  if let .ok r := j.getObjVal? "operation" then return .operation (← reason r)
  if let .ok r := j.getObjVal? "blocked" then return .blocked (← reason r)
  throw s!"unknown pass failure {j.compress}"

def stateName : State → String
  | .notSynced => "notSynced"
  | .syncing => "syncing"
  | .waitingToSync => "waitingToSync"
  | .synced => "synced"
  | .offline => "offline"
  | .offlineUnsent => "offlineUnsent"
  | .signedOut => "signedOut"
  | .noAccess => "noAccess"
  | .storageFull => "storageFull"
  | .folderMissing => "folderMissing"
  | .updateNeeded => "updateNeeded"
  | .removed => "removed"
  | .needsReset => "needsReset"
  | .clockWrong => "clockWrong"
  | .failed => "failed"

def waitName : SyncWait → String
  | .notSyncing => "notSyncing"
  | .coolingDown => "coolingDown"
  | .blocked => "blocked"
  | .paused => "paused"
  | .metered => "metered"
  | .retrying => "retrying"
  | .queued => "queued"

/-- An upload: `{"progress": "moving" | "stored" | "queued", "bound": bool,
"retrying": bool, "record": reason or null}`. -/
private def upload (j : Json) : Except String Upload := do
  let progress ← named [("moving", Progress.moving), ("stored", .stored), ("queued", .queued)]
    "progress" (← j.getObjValAs? String "progress")
  let recordJson ← j.getObjVal? "record"
  let record ← if recordJson.isNull then pure none else some <$> reason recordJson
  return { progress, record
           bound := ← j.getObjValAs? Bool "bound"
           retrying := ← j.getObjValAs? Bool "retrying" }

/-- `SyncState.state` of one set of facts: `storage` (`none`, `signedOut`,
`connected`), `last` (`synced` or `{"failed": pass failure}`), `moving`,
`downloading`, `uploads`, `unsent`, `running`, `cooling`, `paused`,
`unmeteredOnly`, `metered` and the pending `records`' reasons. Reports the state's name as `state`, and, for
waiting to sync, what it waits for as `wait`: the held-back pass's wait, else
the first waiting upload's. -/
def run (j : Json) : Except String Json := do
  let storage ← named [("none", Storage.none), ("signedOut", .signedOut), ("connected", .connected)]
    "storage" (← j.getObjValAs? String "storage")
  let lastJson ← j.getObjVal? "last"
  let last ← match lastJson.getStr? with
    | .ok "synced" => pure LastPass.synced
    | _ => pure (LastPass.failed (← passFailure (← lastJson.getObjVal? "failed")))
  let records ← (← j.getObjValAs? (Array Json) "records").toList.mapM reason
  let uploads ← (← j.getObjValAs? (Array Json) "uploads").toList.mapM upload
  let facts : Facts := {
    storage, last, passing := false, uploads, records, lastSynced := none
    moving := ← j.getObjValAs? Bool "moving"
    downloading := ← j.getObjValAs? Bool "downloading"
    unsent := ← j.getObjValAs? Bool "unsent"
    running := ← j.getObjValAs? Bool "running"
    cooling := ← j.getObjValAs? Bool "cooling"
    paused := ← j.getObjValAs? Bool "paused"
    unmeteredOnly := ← j.getObjValAs? Bool "unmeteredOnly"
    metered := ← j.getObjValAs? Bool "metered" }
  let shown := state facts
  let wait := match shown, facts.waits with
    | .waitingToSync, w :: _ => toJson (waitName w)
    | _, _ => Json.null
  return Json.mkObj [("state", toJson (stateName shown)), ("wait", wait)]

end SyncStateCase

private def run (j : Json) : Except String Json := do
  match ← j.getObjValAs? String "check" with
  | "retained" => retained j
  | "download" => download j
  | "read_settles" => readSettles j
  | "keep" => keep j
  | "waiting" => waiting j
  | "evidence" => evidence j
  | "wakes" => wakes j
  | "file_scan" => fileScan j
  | "sync_state" => SyncStateCase.run j
  | "retry_answer" => RetryAnswerCase.run j
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
