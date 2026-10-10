# IO and observable work

The executable Lean package in [`io/`](io/) models [one sync pass](../sync-pass.md),
[§3.1](../coven.md#31-io-bounds), storage observations, retention and file work.
It uses Appendix B's Lean 4.34.1 toolchain and the standard library, without
Mathlib. `CovenIO/Axioms.lean` audits every named theorem; `scripts/check.sh`
builds the package and rejects unfinished proofs and additional axioms.

**Several unconditional claims fail.** A latest snapshot by total write count
need not cover a deleted write. A pass can cross the retention deadline while
reading. Equal provider timestamps do not establish a completed time interval.
Successful requests can encounter a permanent refusal. The checked histories
below are results, not assumptions hidden inside proofs.

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

**3. Long absence and new devices — retention proved; selected-snapshot
completeness has a checked counterexample.** `deleted_write_covered` proves
that every part of every deleted write has a retained covering snapshot,
through any sequence of publications and permitted deletions. Deleting a
snapshot requires another retained snapshot covering all its positions.

That invariant does not justify selecting only the snapshot with the greatest
sum of positions. In `Examples.incomparable_latest`, Ana's snapshot covers
Ana 1; Ben's covers Ben 1–2. Both are retained and valid. Ana 1 reaches 30
storage days and is legally deleted. `deletion_history_reachable` checks
the retention transitions. `latest_selected_incomparable` selects Ben's
snapshot, which advances Ben but covers none of Ana. GET Ana 1 then misses
(`newest_snapshot_misses_deleted_write`). This uses one audience, so it
fails before common-point loading becomes relevant.

`selection_covers_deleted` proves the sufficient condition: selected snapshots
dominate **all** retained coverage. This is an additional condition, not a
consequence of §15's total-count ordering. `selected_snapshot_suffix_available`
derives availability of the uncovered suffix through concurrent retention;
`discovery_complete` then applies to its reads. Selecting, fetching and atomically
loading usable snapshots across audiences, including intervening writes,
remains outside the general completeness claim. The model does not silently
replace the specified selection rule.

**4. Recent-return misses — proved at observation time; unconditional misses
have checked counterexamples.** `recent_write_protected` rules out both
deletion alternatives when the reader is active, its posted position does
not exceed committed consumption, and storage time is strictly before
S+30 days. Unconsumed writes published since the qualifying pass have times
at least S. A missing or invalid post is conservatively zero.

`Examples.snapshot_next_number_wrong` deletes snapshot 2 after a covering
snapshot 3; GET 2 cannot discover 3. `away_write_miss` deletes an unconsumed
write at 30 days while Ben is away. `pass_crosses_deadline` starts at day 29
with checkpoint day 0, but reads a day-1 write after deletion at day 31.
Starting before the deadline is insufficient; the rule must still hold when
a miss is used. `exact_boundary_discovers` sends equality at 30 days to
snapshot discovery. `unresolved_preserves_checkpoint` prevents unfinished
history from refreshing S. A pending file alone need not block S.

**5. Finality input — proved for a completed publication frontier; literal
timestamp completeness has a checked counterexample.** The scans supply
all permanent entries published before their qualifying folder observation.
`through_time` translates that to all entries with stored time at most T
only under the stated fence: those entries were already published before
the observation. Nondecreasing timestamps alone do not imply this.

`Examples.timestamp_not_frontier` observes T=10 and lists folders, then an
unknown writer publishes entry 1 with the same stored time 10. No clock goes
backward. The earlier listing cannot contain that writer. Closing an entire
provider timestamp tick, or an equivalent publication fence, remains outside
the modeled contract; no finality advance is claimed without it.

`Refinement.finality_precondition` produces the exact
`CovenStorelog.Horizon.CompleteOld` hypothesis named `hs` by
`Horizon.current_stability` in [C10](storelog.md#c10-finality-by-storage-time).
Its `hq` quiet-window evidence and `hv` causal validity remain separate.
This uses the current strict-boundary, tied-time theorem, not the earlier
`Finality` model's distinct-time convention.

**6. Positions as discovery bounds — checked counterexample.**
`Examples.positions_not_index` uploads write 9 while the confirmed post
still says 8. An exact GET returns 9. A crash between these requests changes
neither stored object. Posted positions protect retention; they do not bound
discovery.

**7. Retired devices — proved conditionally.** `Retired.drain_complete`
combines a completed scan, required write availability and `ClosedAfter`:
no new object from that writer can land after cutoff. Removal needs confirmed
provider cutoff, including already issued upload sessions. Closure applies
while that access epoch remains closed. Restoring access must restore polling;
the replay-to-drain-state transition is outside this model.

For replacement, `ReplacementWindow` explicitly requires every old-copy send
to precede its reading replacement, including attempted retries, and every
such landing to be at or before the drain frontier. `replacement_closed`
derives closure using the provider's publication witnesses.
`replacement_needs_landing_frontier` shows why “each request takes at most
30 days” alone is insufficient: replacement at day 0, finality/drain at 31,
last send at 31, replacement read and landing at 32. Stopping upon the read
and bounded request duration both hold, but do not justify the earlier drain.

`drained_not_polled` and `drained_not_named` exclude a drained writer from
subsequent idle log requests, including after reopening saved state. This
models the IO audit's **Decisions item 15**; its earlier finding numbered 15
concerns cache writes. Literal “no request names the device” would also
prohibit reading surviving files or deleting retained history. A checked
file-range example shows that broader reading conflicts with fixed paths.

`failed_scan_not_complete` and `completed_scan_retained` model the durable
one-time removed-device file-folder scan. Correct orphan enumeration,
access restoration epochs and reference-safe deletion still need Rust tests.

## Requests and retained observations

**8. Cost and reuse — proved for modeled traces.** `idle_two_listings`,
`idle_gets` and `idle_requests` give exactly two listings and 3N next-number
GETs. N is the filtered active/undrained set, including this device. The
trace depends on no history or file count. Three devices give 11 requests
(`idle_three_devices`). Each listing is one complete logical page here;
extra provider pages and native calls must be charged separately.

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

**9. Identity before sends — proved.** `send_requires_check` and
`check_precedes_send` require a successful completed check before any
create/replace request in that pass. Pass start or invalidation clears
authority; a failed check cannot grant it. Clock objects share this gate.
This restates storage's `sends_require_check` shape using its actual `Gate`
type. Complete own-counter evidence, out-of-loop serialization and reset
implementation remain outside the trace proof.

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
- Test clock sampling, exactly 30 days, suspension, failed probes and checkpoint
  posting. Read-only identity discovery precedes clock replacement and is reused.
  Address the checked snapshot-selection and timestamp-frontier failures above.
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
- Confirm cutoff includes in-flight sessions, orphan scans survive reopen, a new
  access epoch requires a new scan, and rows, losses, history and non-final effects
  continue protecting file deletion.

Chosen readings: numbering starts at 1; durations use storage seconds; abstract
path ids preserve path ordering; completion requires terminal observation and
committed input; recent means recent at each read; replacement needs a settled
landing frontier; “stop polling” concerns logs; byte receipt does not imply
application. Every additional condition is stated above.
