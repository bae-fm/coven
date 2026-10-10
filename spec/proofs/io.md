# IO and observable work

The executable Lean package in [`io/`](io/) models [one sync pass](../sync-pass.md),
[§3.1](../coven.md#31-io-bounds), storage observations, retention and file work.
It uses Appendix B's Lean 4.34.1 toolchain and the standard library, without
Mathlib. `CovenIO/Axioms.lean` audits every named theorem; `scripts/check.sh`
builds the package and rejects unfinished proofs and additional axioms.

The publication-time snapshot invariant, per-miss deadline, preceding-unit
clock observation, retirement verdict and gated-send deadline are checked.
Two stronger claims have checked counterexamples: checking snapshot coverage
before an upload does not serialize concurrent publications, and a merely
nondecreasing provider clock need not advance at the rate of an elapsed timer.
The conditions and remaining boundaries are stated below.

## Storage and execution

`Storage` contains exact paths for the three numbered logs, snapshots by
audience and writer, files, positions and clock objects. Objects retain bytes,
publication time and revision. `put` preserves occupied bytes and time;
`replace` changes the revision even with equal bytes and time. `ProviderStep`
allows nondecreasing storage time and concurrent publication, replacement and
deletion. Entries and key copies cannot be replaced or deleted. Status and
listing responses contain metadata only; GET and range reads return bytes.
Exact paths stand for provider object ids; adapter id lookup remains outside.
`serve` executes reads and mutations against that state. `served_transition`
connects each served request to the provider relation, and `serve_occupied`
checks that a refused create preserves the existing object.

`World` supplies storage at each request event. Event order is separate from
provider timestamps. `readLog` executes next-number reads, records requests,
retains successful responses, and distinguishes a miss, transport failure,
permanent refusal and exhausted execution budget. `executed_scan` proves
that a successful execution realizes `Scan`. Exhaustion and failed requests
never certify completeness.

Publication, retention, cache and provider transitions describe separate
boundaries. Their proofs compose through explicit prefix, deletion and
observation hypotheses; this is not an implementation of the entire sync
owner. `CompletedPass` requires completed scans of its known writers. It does
not prove that an indefinitely growing log eventually yields a miss, or that
a provider implements complete paginated listings.

The shared request trace distinguishes the pass, upload worker and calls
outside the loop. Pass requests exclude file uploads, including parts and
session completion; eager file downloads remain in the pass. Every worker
request checks the same completed catch-up and identity evidence. Native
multipart storage behavior and asynchronous scheduling remain outside the
model; session requests are represented for their gate and request accounting.

## Discovery obligations

**1. Gap-free publication — proved.** `Publication.reachable_valid`,
`publication_fixed`, `reservations_irrevocable` and `exact_path_prefix` give
the published sequence 1 through k with fixed reserved bytes. Publication
k+1 requires confirmation through k. Crash, lost-reply retry and cancellation
cannot remove a reservation. `interleaved_valid` covers independent writers
and all three log kinds. These are publication-history statements: retention
can leave holes in surviving write objects. Actual encoding and provider
confirmation remain implementation obligations; occupied-path comparison
has the storage package's `settled_iff_equal` contract.

**2. Discovery — proved with completion conditions; the request-only claim
has a checked counterexample.** `discovery_complete`, `delivery_from_pass`
and `complete_pass` cover every target prefix that remains available during
the scan. `provider_prefix_available` derives this for permanent logs from
`ProviderRun`. `writer_discovered` connects permanent entry 1 to a complete
folder observation. `permanent_receipt_exact` proves equality with the original
object's bytes and metadata. Registrations learned during the pass must be
included before constructing `CompletedPass`.

`recent_prefix_available` derives write availability from an active reader's
honest posted position, its qualifying checkpoint, and §15 deletion eligibility
at each read. Concurrent appends need not stop for these prefix results.
Received bytes are distinct from application: causes, keys, schema and complete
validation still gate application.

`Examples.refused_scan_incomplete` stores two entries. GET 1 succeeds but its
bytes are refused; entry 2 remains unread. There is no failed provider request.
Thus “no failed request” alone is insufficient. A completed terminal scan and
successful durable retention are the conditions used in the theorems.

**3. Current snapshots (item 19) — proved at publication; checked
counterexamples for concurrent uploads and an earlier selection.**
`current_matches_listing` connects the current map to the retained audience's
latest storage time, with smaller path breaking ties. Each audience has one
current snapshot. Coverage means its signed positions include the write.
Old snapshot deletion leaves this current projection unchanged.

`RetainStep.publish` checks that a candidate covers the current snapshot at
publication. `coverage_never_shrinks` proves that all later current snapshots
dominate their predecessors. `deleted_write_covered` and
`selection_covers_deleted` prove current coverage of every deleted write
through arbitrary allowed transitions. No additional selected-snapshot
dominance hypothesis remains. `Examples.incomparable_latest` rejects Ben's
incomparable snapshot against Ana's current one;
`incomparable_deletion_impossible` rejects that state in `RetainRun`.

That publication-time rule is stronger than checking before sending.
`SnapshotPublication` separates preparation from landing. Its checked
`concurrent_snapshot_history` has one audience, two snapshots and one deletion:
both writers prepare while there is no current snapshot; Ana's lands covering
Ana 1; Ana 1 is deleted at its 30-day deadline; Ben's lands one second later,
covering Ben 1 only. Both preparation checks and the deletion check passed.
`concurrent_snapshot_loses_deleted_write` proves the resulting invariant false.
This failure requires neither a timestamp tie nor an inaccurate listing.
No cross-device publication serialization is specified by the two rules.

Even enforcing the publication-time rule does not protect a snapshot already
selected by a reader. `loaded_snapshot_can_become_stale` selects coverage
through write 1, then publishes coverage through write 2 and deletes write 2.
The reader's selected snapshot does not cover the deleted suffix.
`selected_snapshot_suffix_available` therefore uses the current coverage at
each observation, proved by `RetainRun`, rather than assuming a fixed selection
dominates all future snapshots. Keeping a selection usable during reload,
fetching its bodies and atomically loading the common point across audiences
remain outside this completeness theorem.

**4. Recent-return misses (item 20) — proved with a clock-advance bound;
checked counterexample without it.** `needsSnapshots` evaluates
`T + elapsed >= S + 29 days` at the miss. `readRecentLog` preserves failures
and refusals and returns `snapshots` when a terminal miss expires.
`accepted_miss_is_recent` proves that accepted misses passed this test at
their own time; `pass_crosses_deadline` now produces snapshot discovery.
Equality at 29 days takes that route too (`exact_boundary_discovers`).

`miss_before_retention` uses the one-day margin to prove storage time is
strictly before S+30 days, provided storage time at the miss is at most
`T + elapsed + one day`. `recent_return_complete` uses this fact at the
terminal miss, at any point in a pass, to exclude both deletion alternatives.
It also needs an active reader, honest posted positions, a qualifying saved
checkpoint and the published prefix. `recent_prefix_available` supplies the
stronger all-read-instances form. Unresolved history cannot refresh the
checkpoint (`unresolved_preserves_checkpoint`); pending files alone can.

The clock bound is not a consequence of §4's nondecreasing publication times.
`clock_jump_defeats_recent_miss` checks checkpoint 0, a write and sample at
second 1, elapsed 0, then a provider advance to day 30 plus one second.
Deletion is permitted and the executable scan accepts its resulting miss.
A physical relationship between provider time and the monotonic timer,
including sample age and sleep, must establish the bound. Without it the
unconditional recent-return claim fails. `snapshot_next_number_wrong` and
`away_write_miss` retain the independent reasons that old snapshots and writes
cannot be discovered by next number after deletion.

**5. Finality input (item 21) — proved without a publication fence.**
`observationTime` subtracts one positive provider unit from the clock object's
storage time. `old_immutable_present` proves backward through `ProviderRun`
that an immutable object with an earlier timestamp was already present at
that clock publication. It permits arbitrarily many equal-time publications.
`through_time` consequently supplies all permanent objects through the
preceding-unit cutoff from completed discovery of that publication frontier.

`Refinement.finality_precondition` constructs the actual
`CovenStorelog.Horizon.CompleteOld` input of `Horizon.current_stability` in
[C10](storelog.md#c10-finality-by-storage-time). It takes immutable path/time
representation and completed entry receipt, with no separate timestamp fence.
`Examples.timestamp_not_frontier` checks that the later time-10 entry is
outside the corrected cutoff 9, so that example no longer contradicts it.
If natural-number subtraction underflows, the old prefix is empty; no finality
is certified for an entry. Provider units are positive multiples of the model's
base time unit. Subsecond adapter representation remains outside the model.
The quiet-window test, causal validity, decoding and complete paginated
folder/entry reads are still required; this proof does not infer them from a
successful clock request alone.

**6. Positions as discovery bounds — checked counterexample.**
`Examples.positions_not_index` uploads write 9 while the confirmed post
still says 8. An exact GET returns 9. A crash between these requests changes
neither stored object. Posted positions protect retention; they do not bound
discovery.

**7. Retired devices (items 15, 16 and 18) — verdict and timed closure
proved; complete drains require successful discovery.**
`Retired.writeAllowed` uses only immutable write storage time and recorded
reads, and the kept entries that retire its device. Retirement includes
removal of the device, removal of its member and replacement of its id.
`write_verdict_agrees` proves agreement for the same kept entries and reads,
independently of list order. `kept_retirement_rejects` rejects a read retirement
or landing strictly after its 30-day deadline. `dropped_retirement_restores`
removes that exclusion when no other kept retirement blocks the write.
`retired_write_boundary_and_restore` checks equality, one second beyond,
restoration after dropping the retirement, and the recorded-read rejection.
Ordinary authority, schema and reset eligibility remain separate.

This rule applies only to writes. Entries retain §9's permanent deadline
against any unread entry, including one replay drops. No retirement-dependent
entry revival is introduced. Applying these write verdicts to data and
persisting their reversible effects remain outside IO.

`Retired.final_retirement_seen` invokes the actual C10 stability theorem:
a completed catch-up containing the old prefix keeps a final kept retirement.
`Retired.Publication` records the completed catch-up, send, landing, successful
shared gate and one-day request-duration bound. A catch-up started at or after
finality observes retirement; an earlier one can authorize a send only until
five minutes after its start. `publication_before_deadline` proves every such
landing is before finality plus one day and five minutes.
`last_send_settles` checks a last permitted integer-second send with a full-day
publication delay. The bound starts at finality, never the retirement's
publication or catch-up completion.

`retirement_closed` derives `ClosedAfter` from these publication witnesses and
the immutable provider transition relation. It applies to entries, writes and
key copies, including exposure to other recipients. `Retired.drain_complete`
uses that derived closure; it no longer takes `ClosedAfter` as a hypothesis.
The drain waits for a storage observation strictly after the deadline and
then completes its scans. Entries and copies derive availability from
permanent storage. Writes still need the current-snapshot/recent-read
availability conditions above; the snapshot races prevent an unconditional
write-drain claim for arbitrary concurrent histories.

The timing argument uses a common duration scale for finality, catch-up start,
send and landing. An implementation must relate its monotonic timer to storage
time and enforce the remote one-day publication bound, including lost replies.
Client timeout alone is insufficient. Provider clock jumps remain outside
that duration interpretation, just as they defeat the recent-return rule.
`replacement_needs_landing_frontier` retains the counterexample to the weaker
conditions of stopping only upon reading replacement and 30-day requests.

`drained_not_polled` and `drained_not_named` exclude the drained writer from
later idle log probes. Surviving files can still name that writer.
The model does not implement the atomic durable transaction saving finality
`F`, all three successful terminal observations and the drain mark, nor replay
invalidation of obsolete marks. A correctly established final basis cannot
drop. Failed or incomplete scans do not certify completion.

`failed_scan_not_complete` and `completed_scan_retained` model the durable
one-time removed-device file-folder scan. Orphan enumeration, restoration of
access and reference-safe deletion remain implementation obligations.

## Requests and retained observations

**8. Cost and reuse — proved for modeled traces.** `idle_two_listings`,
`idle_gets` and `idle_requests` give exactly two listings and 3N next-number
GETs. N is active devices, including this one, plus retired devices not
yet drained. Drained ids add no log probes; their permanent folders and
positions still occupy listing pages. The trace depends on no history or
file count. Three devices give 11 requests (`idle_three_devices`).
Each listing is one complete logical page here; extra provider pages and
native calls must be charged separately.

`scan_cost` counts one read per received number plus the terminal miss.
`fetch_ahead_cost` adds at most limit−1 already issued probes **per log**.
It assumes that the scheduler respects that capacity; it does not prove an
asynchronous scheduler. With k new objects, these are k additional reads
plus bounded probes. Induced publications, file ranges, deletes, clock probes
and access work remain separate requests. `failed_request_counted` retains
a failed request in the execution trace.

`no_duplicate_download` prevents refetching retained byte addresses without
explicit eviction, through arbitrary cache histories including reopen.
Addresses contain path, size, stored time, revision and byte offset, so
overlapping file ranges share addresses. `shared_in_flight` excludes a second
reservation for a byte being fetched. Provider requests batch these events;
actual range formation is outside the model. Partial failures can retry
uncommitted bytes, with repeated bytes charged. The revision theorem covers
equal-size, equal-time replacements.

**9. Send gates and out-of-band uploads (items 17 and 18) — proved for
the shared trace.** `send_requires_check` and `check_precedes_send` extract
successful identity, active membership and completed catch-up evidence for
every send. The elapsed limit is strict and measured from catch-up start;
completion cannot refresh that start. Starting another catch-up keeps the
last completed evidence, with its original expiry. Reopen, reconnect and reset
invalidate it. `expired_sends_nothing`, `invalidation_clears` and
`fresh_gate_boundaries` cover expiry, absent evidence and a catch-up that
itself takes five minutes. Times stand for monotonic readings including
sleep; obtaining those readings is an input. The gate checks that start,
completion and send occur in that order.

`pass_no_file_uploads` proves the pass's request projection contains no file
creates, replacements, upload-session starts, parts or completions.
`upload_worker_gated` proves every upload-worker request passes the shared
gate, including confirmation reads, retries, parts and session completion.
`upload_parts_use_gate` checks the last permitted second and expiry.
`upload_uses_last_completed_catchup` checks an upload while another catch-up
is pending, using the still-fresh completed one.
The same trace accounts for calls outside the loop and clock replacement.
A request queued before expiry has no exception.

The literal statement “the pass contains no file transfers” is false:
`pass_can_download_file` is a checked pass trace with an eager file-range read,
as required by sync-pass step 7. The chosen reading of item 17 is no file
**uploads** in the pass. Captured attachment keys, source validation, actual
transfer limits, worker scheduling and its independence from pass completion
remain outside this trace model. Moving requests to the worker does not
remove them from total IO accounting.

## Downloads and their visible outcomes

**10. Downloads and pending records — proved at the transaction boundary.**
`Downloads.first_unmet`, `one_pending` and `pending_replaced` provide one
current reason per unfinished subject/reporter. Readers, pinning and eager
work use the same state. There is no second progress source.

Outcomes include no connection, network/provider failure, absent file with
active uploader, removed/replaced uploader, reported missing/changed source,
missing row key, authentication/hash failure, disk failure and database failure.
`key_wait_names_parent` records the encrypted write/snapshot subject before
a file reference exists.

`temporary_uses_shared_delay` and `permanent_never_retries` separate temporary
retry from app/update work. `eviction_preserves_refusal` preserves permanent
reasons when bytes are evicted. `completed_eviction_does_not_refill` does not
requeue completed eager work after budget eviction.

`committed_range_checked` requires authentication, the final whole-file hash
when completing, and disk/database success before committing a range.
`completion_has_all_ranges` requires every chunk before completion.
`progress_never_decreases` counts committed verified ranges; eviction is
separate, and `committed_ranges_unique` prevents counting a range twice.
`failure_atomic` preserves earlier progress, records the failure
and publishes no completed file or pin. `disk_failure_keeps_progress` checks
two committed ranges and disk-full on the last range. Cryptographic and
persistence results are inputs, not implementations of those boundaries.

**11. Waiting — proved for monotonic execution.** `delay_positive`,
`delay_cap` and `delay_at_cap` cover 1,2,4,…,256,300-second delays.
`one_second_bound` gives W·(1+floor(t/1s)); `capped_bound` gives
W·(1+floor(t/300s)) after all counted waits reach the cap. These quantify
over start schedules with the required spacing, in any chosen interval.
Each wait's pages, SDK retries and request allowance remain charged.

`restart_bounded` proves 0 ≤ max(0,min(T−now,W)) ≤ W for every integer
clock value. Provider cooldown W can exceed 300. `app_retry_keeps_cooldown`
prevents an app retry from shortening it. The forward-clock example returns
zero: no real-time cooldown guarantee under arbitrary forward clock jumps
is asserted. Persistence, monotonic scheduling, HTTP-date interpretation
and failure to persist a delay remain Rust obligations.

## Delivery refinement

**12. Delivery — proved up to validated inputs; application prerequisites
remain outside IO.** `pass_supplies_input` derives retained bytes from a
completed scan. `merge_step_realized` calls actual `CovenMerge.step`;
`storelog_step_realized` calls `CovenStorelog.step`;
`coupled_write_consumed` checks actual `CovenStorelogData.receiveWrite`
guards. The compatibility receiver does not replace current replay or key
selection.

Remaining assumptions are successful decoding/authentication; valid write
metadata; causal application order; author authority; running membership;
the header key and required part keys; no pending reload; schema/reset
eligibility; and correct snapshots installed atomically at the common point.
These are the connected packages' hypotheses, not consequences of receiving
bytes. Snapshot selection blocks unconditional refinement after long absence.

## Rust checks outside the model

These obligations are retained. This package adds no Rust tests and changes
no Rust behavior.

- Count actual pages, metadata/body bytes, name lookups, redirects, native
  asset parts, SDK retries, publications, confirmations, deletions and access
  requests on every provider. Vary unchanged history/files, devices and pages.
  Incomplete scans preserve the committed catalog/reports; errors are not absence.
  The three-device warm idle case is 11 native requests on S3, Drive, Dropbox,
  OneDrive and CloudKit with one-page listings. One new S3 note with transfer
  limit one and its positions PUT makes 13. Drive hits charge the parent/name
  lookup and body download separately; unknown-name misses charge the lookup.
- Exercise retained headers, signed prefixes, peer revisions, invalid-position
  verdicts, unfinished encrypted inputs and file-reference completeness across
  passes, reopen and eviction. Changed immutable size/time is a protocol failure.
  Checked prefixes are not checked bodies; header checks confer no author authority.
- Test numbered cancellation, lost replies, crashes before posting, invalid key
  copies followed by valid ones, and other-recipient exposure. Invite cursors do
  not replace earlier copy history. After request deletion, refresh copies and
  membership before reporting decline. Known required gaps remain pending.
- Ana's old phone has write 8 in flight when its replacement lands. Check
  publication exactly 30 storage days later and after that deadline. Repeat
  for device and member removal, independently of provider cutoff. Retain
  excluded inputs while reversal is possible; dropping the retirement can
  restore the write, never an entry rejected by §9's permanent rule. Equal
  immutable times and kept entries must yield equal verdicts across arrival
  orders and device clocks.
- Check the freshness gate immediately before every send, including the file
  worker, out-of-loop calls and SDK retries. Exercise just before and exactly
  at five minutes, sleep, reopen, reconnect, failed catch-up and a catch-up
  taking five minutes. Test a retirement that drops and returns before
  finality, and a last permitted send whose publication takes one day even
  after a lost reply. No client timeout alone proves remote cancellation.
- Test clock sampling, exactly 30 days, suspension, failed probes and checkpoint
  posting. Read-only identity discovery precedes clock replacement and is reused.
  Exercise concurrent snapshot publication, a selection becoming stale during
  reload, per-miss expiry and provider clock advance. Same-tick publications
  must stay outside the preceding-unit finality observation.
- Exercise due-only snapshot growth/coverage, unchanged waits with backoff,
  indexed cached header facts, multiple audiences and atomic common-point reloads.
  Share retained inputs across consumers and verify authors before applying data.
- Inject interruptions, withheld keys, absent uploaders/sources and bad checks,
  especially **disk-full at cache publication through the real reader, database
  and disk**. Preserve previous cache facts, return the typed disk cause, retain
  `Failed(LocalFailure::Disk)`, and publish neither a pin nor positions claiming
  completion. Authenticate before exposing bytes and check the whole hash before
  releasing a sequential read's last buffer.
- Count custody unlocks, full-log decodes/replays, writer holds and durable
  commits; a warm idle pass performs none. Check populated-table query plans,
  namespace cache totals, shared ranges, upload-source whole-file/chunk checks
  and provider-confirmed multipart progress. Initialization, interruption and
  eviction have separate counts; unchanged catalogs have no pass-end expiry.
- Check identical database queries for direct reads and subscriptions of uploads,
  eager/pin progress, pending work and reset notices. Reopen reports committed
  progress before storage reconnects. Failed database reads cannot return empty
  lists or completed work. One-request uploads retire only after confirmation.
- Ben's laptop is removed. Ana polls all three logs until its kept removal
  has been final for more than one day and five minutes, then reads to their
  misses and records the drain locally. Ana's restored phone uses the same
  wait for its old id's replacement. Test equality, expiry, reopening with
  retained `F`, delayed finality and reads made before the qualifying `T`.
  Later idle passes and reopen issue no probes for a drained id. With three
  devices and one-page listings, Ben's completed drain changes 11 requests
  to 8; a pending drain keeps all three probes.
- Fail each drain read and its final commit. Retain progress, leave the mark
  absent and retry under shared backoff. Known gaps, permanent refusal and
  outstanding fetch-ahead prevent completion; cached writes waiting for keys
  remain pending independently of discovery. Replay dropping a removal must
  clear its timing/mark and resume polling in the same transaction. A kept
  final alternative basis still supports a mark; a new device inherits none.
- Retain drained entries for finality input and drained copies for exposure
  and key selection. Test the last permitted send and one-day publication
  against the post-finality wait. These checks must establish the complete entry
  receipt and write-availability conditions above, including concurrent
  snapshot changes during discovery.
- Confirm cutoff includes in-flight sessions, orphan scans survive reopen, a new
  access epoch requires a new scan, and rows, losses, history and non-final effects
  continue protecting file deletion.

Chosen readings: snapshot coverage is enforced at publication for the
invariant theorem; its pre-send implementation is checked separately and
fails under concurrency. Path ties choose the smaller path. A write miss is
recent only at its own observation, with a strict 29-day limit and an explicit
provider/elapsed-time bound. Finality uses the preceding provider unit, with
underflow certifying no old entries. Retirement timing begins at finality and
uses a shared duration scale. Dropping one retirement restores admission only
when no other kept retirement blocks it. Out-of-band file work means uploads;
eager downloads remain in the pass. Numbering starts at 1, completion requires
retained input and terminal observation, and byte receipt is not application.
