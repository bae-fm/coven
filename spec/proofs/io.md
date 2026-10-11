# IO and observable work

The executable Lean package in [`io/`](io/) models [one sync pass](../sync-pass.md),
[§3.1](../coven.md#31-io-bounds), storage observations, retention and file work.
It uses Appendix B's Lean 4.34.1 toolchain and the standard library, without
Mathlib. `CovenIO/Axioms.lean` audits every named theorem; `scripts/check.sh`
builds the package and rejects unfinished proofs and additional axioms.

The fixed-target reload proposal terminates per audience and preserves every
required audience write through snapshot coverage or retained log input.
A combined deadline proves deletion for witnessed writes with bounded reader,
publication and retention services. Checked counterexamples remain for writes
with no first witness and replacement snapshots at different audience positions.
This is a proposal, not a change to the spec or Rust.

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

**3. Snapshot coverage proposal — revision 3.** This checks a proposal;
§15 and Rust are unchanged. The proposal keeps these four rules:

> a) Publish a snapshot only immediately after a catch-up that reached every
> writer's next-number miss with nothing pending. At preparation it must
> cover the current newest snapshot of that audience.
>
> b) Its signed prefix records the storage sample T_c taken before catch-up
> reads. A landing more than one day after T_c does not count for loading,
> selection or coverage. Abandon the upload after a day and prepare afresh;
> an abandoned request may still land remotely. This imposes no bound on
> request duration.
>
> c) Delete only when a historical counting snapshot covering each part
> landed more than a day ago, and each part's current newest counting
> snapshot covers it. Finality and the positions-or-thirty-day gate still apply.
>
> d) A loading device refreshes the catalog after a missing GET. Coverage
> through the missing number, including another audience's coverage, can
> establish a published and deleted hole. Failed requests are not absence.
> Active devices keep the posted-position and recent-return protections;
> this rule adds no snapshot listing to their ordinary sync.

Revision 3 adds the requested repairs:

- **R1:** freeze a finite required position for every known writer at reload
  start. Commit when that target is realized. New tail writes never raise it;
  discover them during subsequent sync. Apply d) only inside the target.
- **R2:** after establishing deletion, reload if the loaded audience's current
  counting snapshot reaches the number; otherwise skip that number. The same
  decision applies to deletion evidence already established by the selection's
  fresh catalog, without GETting the known deleted path again.
- **R3:** retention runs every pass. A final, gated write with historical
  coverage but missing current coverage requests a fresh snapshot under a)
  for that part audience in the same or next pass, if the device can read it.

Exactly one day after T_c counts; deletion needs strictly more than a day
since its witness landed. Superseding a witness does not restart that age.
The three orders are write count, T_c and storage time, with the smaller path
breaking ties. Historical coverage in R3 is read per part audience. None of
these rules supplies a first snapshot where no witness exists.

**Combined fixed-target loader — proved per audience.**
[`FixedReload.lean`](io/CovenIO/FixedReload.lean) executes a finite round over
numbered paths in the frozen prefixes. A snapshot-covered number needs no GET;
an uncovered number is read, or skipped only with the deletion certificate
above. A covered miss restarts with the current snapshot. A miss without a
certificate returns `uncertified`, never completion. Deletion certificates
identify writer and number; the decision does not inspect the absent object's
encrypted audience parts.

`terminates` joins execution, retry progress and completeness in one theorem.
A round processes a finite list. A retry identifies a target number deleted
since that round's catalog selection. The next catalog contains this deletion:
its current snapshot either includes the part or certifies that this audience
has no part. Thus that number cannot cause another retry. `deletion_decreases`
proves a strict decrease in the remaining undeleted target numbers. There are
at most **N retries and N+1 rounds**, where N is the number of target log numbers.
No quiescence, advancing provider clock or bound on request duration is needed.

`committed_contains` proves that every positive target number belonging to the
loaded audience is in the committed snapshot's coverage or has an actual GET
receipt retained by the committing round. `prefix_committed` states this for
every published write through the frozen per-writer positions. `scan_spec`
and `skipped_has_no_part` prove that cross-audience skips cannot lose a relevant
part. `combined_race_commits` instantiates the source certificates and executes
one retry followed by a commit on the old stale-selection history.

The source obligations are explicit: a fixed finite writer/prefix inventory,
unique published numbered paths, faithful validated snapshot state, the
`SnapshotHistory.Run` deletion invariant, deletion evidence retained over time,
and a complete fresh certificate after every absent GET inside that published
prefix. Snapshot bodies, catalogs and required reads must finish successfully;
permanent refusal, unavailable keys and never-published reservations are not
successful inputs. `Loads.commit` is the applied-history transaction boundary,
not a SQL transaction implementation. Snapshot body checking and causal row
application remain inputs. The target metadata in the proof names the real
published writes; discovering the finite inventory is outside this loader.

**Deleted-write safety — preserved.** `SnapshotHistory.deleted_stays_covered`
continues to prove current coverage of every deleted part after arbitrary
counting publications and deletions, under every order. If a witness landed
at B, a counting snapshot landing after B+day must have T_c>B, so a successful
catch-up includes the witnessed write. `SnapshotHistory.Sound` carries that
catch-up and faithful-construction evidence. The loader uses the invariant;
it does not assume that successive snapshots dominate each other.

The detailed `SnapshotPublication.execute` histories and `readLoadingLog` keep
the earlier reader for comparison. The revised executable reader is
`FixedReload.scan`. The detailed publication model and certified catalog model
remain separate; arbitrary unfinished publication histories are not converted
into successful catch-up certificates. The stale-selection instance supplies
both histories explicitly. Physical snapshot removal and durable preservation
of historical evidence remain outside the catalog model.

**Bounded storage — proved for witnessed writes; the every-write statement
still fails.** `RetentionDeadline.bounded_witnessed_storage` is one theorem
joining R3's publication, selection, all part audiences, retention and DELETE.
Let B bound the historical witness landing times for every part. Let G be a
time from which finality and the positions-or-thirty-day gate hold. Let P bound
pass spacing, U bound the required successful counting-snapshot publication,
and D bound successful deletion completion, all in storage-time units. Set

```
R = max(G, B + day + 1)
deletion deadline = R + 3P + U + D
```

The model uses integer seconds; `+1` is the tick needed for strict maturity.
Retention observes the condition by R+P, starts its refresh in that or the next
pass by R+2P, completes it by R+2P+U, and deletes on a pass no later than
R+3P+U. U is an end-to-end bound including catch-up, preparation, retries and
confirmation of a **counting** publication; bounding only an upload request
would not bound the preceding catch-up. Once coverage already holds, the
existing `scheduled_deletion_bound` gives the sharper R+P+D deadline.

Current coverage at deletion is a conclusion, not a premise of the new theorem.
`Service` records the due pass's existing selection or the required refresh
under a), its catalog extension and publication deadline. `fresh_covers` proves
that every counting arrival after R includes the old write. If the previous
selection omitted it, `refresh_outranks` proves the refresh wins: under count
order it dominates the previous selection and adds a missing position; under
T_c and storage order its time is strictly later. `refresh_selected` rules out
older uncovered contenders, and `covered_selection_preserved` handles further
publication. Count comparison requires `Normalized`: the encoded count equals
the sum of positions over the known writers, without duplicate writer entries.
The executed `retainWrites` checks finality per candidate, includes the eligible
write, and leaves non-final writes stored (`nonfinal_write_kept` and
`mixed_finality_retention`). D gives the eligible write's removal time.
`quiet_deadline` instantiates the whole theorem on the stranded-write history
with P=1, U=0 and D=4, yielding storage day 30 plus seven seconds.

The theorem assumes a reader and authorized retention owner available to supply
these bounded services for each required audience. With **no reader ever
online** for an audience missing current coverage, its refresh has no completion
bound: every log object with such a part can remain forever, even if its other
parts are covered, finality holds and thirty days have elapsed. The checked
`quietPasses` example with `readable = false` shows this. Absence of a reader
does not block deletion of writes whose every part already has mature current
coverage, provided the authorized deletion service runs. Snapshot objects,
files, total bytes at an unbounded write rate, and unavailable deletion owners
are outside this per-write deadline theorem.

`no_first_witness` is a smaller failure of the every-write claim: one final,
thirty-day-old write, no snapshots, a readable audience, successful retention
every second. `refreshDue` is false forever because R3 requires a historical
witness. The Lean `example` exceeds any proposed deadline while the write
remains. This applies in all three orders, even with bounded P and D; U is
irrelevant because no publication is requested. The smallest further change
is to **drop the historical-witness precondition from the refresh trigger**:
request a covering snapshot whenever a final, gated part lacks current
coverage, including its first snapshot. `coverageDue` and its checked example
show precisely that changed decision. The new witness still needs c)'s one-day
age; revision 3 itself does not establish an all-write deadline.

**Multiple audiences — atomic installation is necessary, but R1–R3 still
lack a common-cut guarantee.** Per-audience visible commits fail with one write
having two audience parts and a waiting write that has read it. Installing the
first part allows the waiting write to claim a past absent from the other
audience. `AtomicReload` contains this one-write checked counterexample.
`atomic_waiting_ready` proves the positive transaction-boundary statement:
when every staged audience realizes the frozen target and that target covers
the waiting write's recorded past, one atomic installation satisfies that past.
New local waiting writes must retain that remote-past bound; this theorem does
not silently enlarge the target to admit arbitrary new remote dependencies.

Even all-at-once visibility is insufficient for independently selected
replacement snapshots. `replacement_exceeds_target` executes this history in
all three orders: publish write 1 with two parts and a snapshot of audience 1
through 1. Start with target 1. Publish write 2 with both parts and audience 0's
snapshot through 2. Age both witnesses and delete write 1. Audience 0's loader
must reload through 2; audience 1's snapshot through 1 already meets the frozen
target. Both loaders finish, but the staged states are at **2 and 1**. Write 2
is still in storage. An atomic commit of these states violates §15's common
point, and a new waiting write claiming past 2 lacks audience 1's second part.
Fetching 2 repairs this history but enlarges the supposedly fixed common target.

The smallest missing condition is on replacement selection: **install a
replacement only together with all readable audience states at one common cut
at least as high as the fixed required target**. A complete snapshot bundle
at that cut needs no tail scan before commit. `prepare` preserves the visible
state; `commit` exposes the entire staged view in one transition.
`closed_replacement_commits`
proves common positions and the waiting-past condition for such a bundle.
R1–R3 neither provide these bundles nor preserve the history needed to assemble
a frozen cut after deletion. Their unconditional multi-audience termination
claim is therefore not proved. Retention pins would instead need a completion
bound of their own to preserve the storage deadline; indefinite pins are not
a solution to both requirements.

**Counterexamples and races rechecked.** The following results quantify over
all three orders where ordering is involved:

- Moving tail: `moving_tail_finishes` commits the frozen prefix even against
  arbitrary future storage. The repeatable old moving-tail history is no
  longer a fixed-target loader failure.
- Other-audience hole and repeated missing-store-snapshot reload:
  `other_audience_finishes` skips the deleted circle write and receives store
  write 2. No store snapshot is required.
- Retention every pass but missing current coverage: `quiet_stall_repaired`
  requests the refresh, executes the existing catch-up/publication history
  under a), and deletes the stranded write when an audience reader is present.
- Fresh-selection and stale-selection deletion races:
  `selection_races_finish` executes the miss, replacement and completed round.
  Snapshot dominance alone still does not protect a paused reader; the loader
  is what repairs the race.
- Cached pre-GET catalog: `cached_catalog_cannot_commit` returns `uncertified`.
  The stale catalog is excluded by the fresh-certificate hypothesis, not
  interpreted as an empty tail.
- Incomparable preparation remains forbidden; concurrent incomparable
  publication remains possible. `old_races_preserved` checks the publication,
  symmetric-deletion exclusion and in-flight age exclusion. Late uploads
  remain ignored by `late_snapshot_ignored`; exact-day boundaries are unchanged.
- Omitted cleanup in `no_cleanup_deadline` and `busy_cleanup_unbounded` is
  excluded by retention every pass; `scheduled_cleanup_runs` and
  `busy_cleanup_runs` execute the due deletions. Ranking alone still cannot
  repair the old stranded write; `quiet_stall_repaired` supplies R3's action.
- Snapshot-number gaps, positions not bounding discovery, permanent refusal,
  clock jumps without the storage-clock assumption, and the retirement and
  finality boundary examples keep their results in the sections below. None
  is repaired or assumed away by R1–R3.

Outside the new results: changing membership/writer inventories, cryptography,
schema/reset eligibility, unresolved local reservations, actual database
transactions, provider listing implementations and snapshot body availability.
The successful-operation and bounded-service hypotheses are explicit; a)
does not itself prove that a busy snapshot producer ever reaches all misses.

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
The environmental clock-rate assumption in
[§4](../coven.md#4-storage-providers-and-access), together with §15's sampling
and sleep-inclusive elapsed time, establishes this bound. Without it the
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

### Joined revocation

**The conditional §3 guarantee is proved across the two packages.**
`Revocation.gated_first_send_revocation` takes an arbitrary `IOTrace` prefix
ending in the request being proved. `last_send_checked` extracts that request's
actual completed catch-up. `completed_catchup` joins its start and completion
times to `CompletedPass` and constructs the existing `KeyPhases.CaughtUp`
through `KeyPhases.begin`, using cached receipts plus decoded scan hits.
There is no separate definition of key usability or selection.

If an entry removing a member was final before that catch-up began, the entry
remains kept in the view used for sealing. The data package's
`Revocation.kept_removal_excludes` then derives exclusion from every audience,
unless a later kept addition restores the member. The selected key has no copy
addressed to that member in storage at the send, provided no such copy appears
after the pass's observation. The sender differs from the removed member.
Selection uses `KeyPhases.firstSend`, which chooses the newest usable held key;
the request carries that attempt's bytes. A successful rotation can extend
the caught-up view: `rotation_retains` proves it keeps both earlier receipt sets.
The theorem takes those receipt-preservation conditions for the sealing view.

The named hypotheses make the boundaries explicit:

- **Receipt and storage:** complete writer discovery and terminal scans within
  this catch-up, retained earlier entries and copies (including completed
  drains), available permanent prefixes, exact immutable paths and publication
  times, successful entry authentication, and all copy routing facts. A box
  addressed to someone else is included without opening or accepting its key.
  `permanent_observed` derives individual receipt from the actual scan and
  provider evolution. `copyCoverage` accounts for every stored copy at the
  request, dividing it between observed and later copies.
- **Finality:** causal validity, a quiet qualifying thirty-day window, the
  removal's strict old age and kept verdict, and both observations containing
  the complete old prefix. `caughtUp_complete_old` supplies the catch-up's
  prefix through `Refinement.finality_precondition`, using a positive provider
  unit and a clock sample preceding the entry reads. Finality's storage cutoff
  and the monotonic instant of its establishment are separate parameters.
  No later kept addition may restore the removed membership.
- **Sending:** the actual gate has accepted identity and active status, the
  catch-up completed, and its start is less than five minutes before the send.
  It is a new sealing attempt produced by `firstSend`, with custody supplied
  by the caller. `noLaterCopy` covers the interval through this send. Taking
  `later` over the write's whole readable lifetime gives the stronger lifetime
  statement, with that stronger premise.

The storage-clock rate assumption in §4 is needed for §15's recent-write
availability test; permanent membership entries and copies need no age-based
retention argument. §10's one-day bound is a bound on remote publication,
including a lost reply, not a local timeout. `retired_copy_settles` connects an
actual copy request in `IOTrace` to `Retired.publication_before_deadline`:
with retirement observed by every catch-up starting at or after finality,
its landing is strictly before finality plus one day and five minutes.
That timing lemma names `storageDuration`, `requestBound` and `finalityStops`.
As in `Retired.Publication`, those durations must share a scale; §4's allowance
for forward drift alone does not identify storage age with monotonic elapsed
time. Neither duration assumption is a substitute for `noLaterCopy`.

**The unconditional claim fails, including after finality.**
`RevocationExamples` contains checked histories and anonymous Lean examples:

- `in_flight_after_finality` has five entries: creation, Ana’s second device,
  concurrent addition of Ben and rotation to K1, then removal of Ben. Ben’s
  initial K0 copy precedes admission. The rotation has not read his addition,
  so its only required K1 copy is for Ana. After learning the addition, Ana
  reserves a historical K1 copy for Ben; removal then occurs.
  `prerequisite_copies_published` checks the initial-copy and rotation rules.
  K1 has no published copy for Ben yet. After removal becomes final, its gated
  request starts. Another device's catch-up misses that copy, selects K1, and
  first-sends after it lands. The copy request takes three seconds; the sender's
  catch-up is still fresh. `reserved_copy_survives` checks the allowed sharing
  and the actual fixed reservation/publication transitions. The entry scan,
  finality certificate, provider evolution and both request traces are checked.
  `joined_guarantee_instantiated` applies the joined theorem at completion,
  before that landing; the later send fails only its no-later-copy premise.
- `removal_during_catchup` misses the removing writer's next entry at second 3.
  Removal lands at 4, catch-up completes at 5, and the gate permits the old key.
  Completing every scan does not mean reading everything published before
  the last scan finishes.
- `nonfinal_removal_blocks_exposed_key` refuses an exposed key immediately;
  finality is not needed for that observation's protection.
  `nonfinal_removal_reverses` checks two admins' concurrent removals: Ana is
  initially absent, then returns when her earlier removal of Ben arrives.
  A non-final verdict cannot supply the theorem's stable-removal premise.
  The re-addition example keeps a final removal while a later addition returns
  the member, demonstrating why the no-return condition is separate.
- `retired_write_lands` checks a request started before retirement, landing
  afterward within one day and admitted by §10. The same device cannot start
  another send with a catch-up that reports retirement. A separate example
  checks admission exactly thirty days after retirement and rejection one
  second later; the admission deadline is not permission to keep sending.
  `retired_in_flight_copy` checks the last allowed integer-second send and
  the full one-day landing delay against the strict drain bound.
- The retry example passes the fresh gate while retaining the old exposed
  key. A retry is not a new sealing attempt.

These do not contradict any sentence of current §3. Its promise explicitly
requires “provided no copy of the sealing key is made for them after that
pass's copy observation”; §11 says the later-copy window is accepted.
For removal during catch-up, §11 explicitly permits old readability for writes
sent before the sender learned of removal. §9 allows non-final replay reversal,
§10 permits the retired write's bounded late landing, and §3 requires fixed
retry bytes. These are counterexamples to removing those qualifications.

Cryptographic sealing, decoding/authentication implementations, provider
adapters, durable transactions and Rust equivalence remain outside these
proofs. `IOTrace.checked` alone has no receipt payload; the bridge therefore
requires the completed scans and their time bounds explicitly. It does not
infer successful reads from the event's name. The key/bytes association is the
existing receipt model's `Attempt`, not a proof about actual ciphertext.


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
bytes. The fixed-target loader repairs the cross-audience hole. Independent
replacement snapshots still block unconditional common-point refinement.

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

## Readings and limits

**§3 coverage:** this model proves conditional discovery, send gates, retention
eligibility, modeled pending outcomes and §3.1 request/reuse/wait bounds. Its
snapshot proposal has conditional coverage and reload-progress results, and a
deletion deadline under explicit coverage, scheduling and completion bounds.
Neither snapshot-suffix availability nor a cleanup deadline is proved unconditionally.
Its delivery results stop at validated inputs: authentication and sealing are
supplied, so confidentiality, forgery resistance and the secrecy interpretation
of revocation assume correct cryptography, including signatures and key derivation.
Atomic SQLite/cache publication and §2's provider behavior are assumed; modeled
provider transitions do not prove that real providers satisfy them.

Chosen readings: the proposed snapshot rule is checked at preparation, with
T_c sampled before catch-up reads, strict one-day deletion age, inclusive
one-day upload admission, and historical covering evidence. Path ties choose
the smaller path. Reload freezes its published prefix, checks a fresh catalog
after an uncertified hole, and skips a deleted number outside the loaded audience.
Current retention coverage is derived from the required refresh. Historical
witnesses, reader availability, and bounds on passes, complete counting
publication and deletion remain explicit hypotheses. Audience installation is
atomic; independent replacements still lack a common-cut guarantee. A write miss is
recent only at its own observation, with a strict 29-day limit and an explicit
provider/elapsed-time bound. Finality uses the preceding provider unit, with
underflow certifying no old entries. Retirement timing begins at finality and
uses a shared duration scale. Dropping one retirement restores admission only
when no other kept retirement blocks it. Out-of-band file work means uploads;
eager downloads remain in the pass. Numbering starts at 1, completion requires
retained input and terminal observation, and byte receipt is not application.
