# IO and observable work

The executable Lean package in [`io/`](io/) models [one sync pass](../sync-pass.md),
[§3.1](../coven.md#31-io-bounds), storage observations, retention and file work.
It uses Appendix B's Lean 4.34.1 toolchain and the standard library, without
Mathlib. `CovenIO/Axioms.lean` audits every named theorem; `scripts/check.sh`
builds the package and rejects unfinished proofs and additional axioms.

**Finality with delayed listings — proved under the provider requirement.**
`listingMargin` is one hour for every provider. `observationTime` is
`T′ = T − M − δ`, with `M = 1 hour` and positive provider unit δ.
`ListingComplete` requires every object in the queried folder stored strictly
before the listing's provider time minus M. For device discovery that means
the permanent entry 1 records, without listing log contents. The listing may
occur after the clock sample T.

[`ListingFinality`](io/CovenIO/ListingFinality.lean) connects that requirement
to completed exact reads. `completed_prefix` obtains each old writer's entry 1
from the listing and every subsequent number from a terminal scan or retained
receipt. It proves receipt of **every entry at or before T′**, including entries
at the inclusive window endpoint. The hypotheses name gap-free ordered
publication, immutable paths, provider evolution, completed scans and validated
receipt. Recent writers may be missing. No read-after-write listing is assumed.

`quiet_agrees` proves that the received entries give exactly the full history's
inclusive quiet-window verdict. `complete_old` supplies the actual store-log
horizon's receipt premise; `finality_safe` invokes its current replay stability
theorem, including kept and dropped dispositions. `qualifying_window` requires a
quiet window in the full history, and `at_or_after_horizon_not_final` excludes
entries at or after H. Underflow certifies no final entry. Provider units are
positive duration values; the one-hour policy accommodates units up to an hour.

**Why the extra provider tick is required.**
[`ListingMarginExamples.Endpoint`](io/CovenIO/ListingMarginExamples.lean)
preserves the checked counterexample to using T minus M alone. Creation lands
at 0, registration A at second 1, and registration B at day 30 plus 2 seconds.
B has not read A, so it is late and permanently dropped. A conforming listing
at day 30 plus one hour plus 2 seconds may omit B at exactly one hour old.
Using T minus M would mark A final without any qualifying window in the full
history. `no_earlier_qualifying_window` rules out an earlier certificate too.

`revised_prefix_from_scans` applies the general discovery theorem to those
actual scans. `revised_prefix_from_retained` checks the same prefix using
cached receipts with no new reads; a complete drained history needs no scan.
`revised_endpoint_safe` checks T′ one second earlier: A remains
outside the strict horizon. `next_tick_sees_late` checks that B must appear on
the next tick and blocks that window. The original example never reversed A's
replay result; it exposed a false qualifying-window claim.

**One-hour listing margin — the unconditional “never judged late” claim
fails even for a conforming provider.**
[`ListingMarginExamples.Conforming`](io/CovenIO/ListingMarginExamples.lean)
checks creation at second 0 and two first-entry device registrations:

- A lands at 01:00:01, having read creation.
- B catches up at 01:00:02. The listing shows only creation's writer;
  its exact entry reads reach a terminal miss. A's folder is still hidden.
  B reads creation and lands at 01:00:02 without reading A.
- At T = 01:00:03 a reader's listing omits both registrations. Both are newer
  than T′ = 00:00:02. At 02:00:03 the reader returns; its listing and exact
  GETs expose both. B is at the new inclusive cutoff 01:00:02 and is late because
  A landed earlier and B did not read it.

Every listing uses a five-second visibility delay. `listing_conforms`
proves that this rule contains every object stored before T minus one hour;
`conforming_observations` applies it to the author and both reader samples.
`provider_history`, `entries_represented`, `causal_history` and
`author_catchup` check the actual provider transitions, immutable times,
causal log and exact scan. The author follows the permitted delayed listing;
the stronger assumption that an online attempt reads every earlier landing
does not hold under that provider contract.

`recent_omission_can_be_late` uses the existing `Finality.late`,
`Finality.tooLate` and `Horizon.quiet`. A stays non-late after delayed receipt.
B is late, but it is not rejected by the thirty-day landing deadline;
replay keeps both registrations.
The margin keeps B outside the initial observation; it does not remove B's
late verdict when the endpoint reaches B's stored time. B blocks the quiet
window through its stored time plus thirty days, including equality. The
first later qualifying provider sample is B's stored time plus thirty days
plus one hour plus two seconds. Receipt time does not restart that wait.

Creation plus these two changes is enough: one change supplies the unread
earlier entry and the other lacks its read. This is a real late entry under
§9, caused by allowed listing lag during catch-up, not a late verdict caused
solely by delayed receipt. The literal claim that a recently omitted entry
is never judged late is therefore false. `one_tick_misses_prefix` also checks
that the same conforming five-second
lag omits an entry inside the prefix claimed by T minus one provider tick.
The one-hour margin excludes that entry from the earlier certificate.

**Why the provider requirement is necessary.** `ListingMarginExamples`
also checks creation plus two concurrent resets: A lands on day 1, B on day 2,
and both read only creation. A wins their conflict when both are received.
At provider time day 32 plus one hour plus two seconds, subtracting the margin
and one provider tick gives an observation at day 32 plus one second and a
horizon at day 2 plus
one second. The thirty-day window is quiet, and B is strictly before the
horizon. If the listing omits A's device, the receiver keeps B. Receiving A
afterward drops B. This changes a result treated as final; it does not merely
delay advancement of the horizon.

`provider_history`, `entries_represented`, `causal_history`, `online_attempts`
and `causal_receipts` check publication, immutable storage times, recorded
reads and both receipt orders. `missing_old_folder` uses the actual IO listing
and exact-read functions: the listing omits an object older than the margin,
although an exact GET finds it and the known writer's next-number GET misses.
`listing_violation_reverses_final` and the literal Lean `example` use the
existing `Horizon` and `CurrentReplay` functions, including the strict old
boundary. Neither reset is rejected by the thirty-day landing rule.

`missing_required_prefix` identifies the failed premise of
`Horizon.current_stability`: receipt of every old entry. The window is truly
quiet even with A present, so quietness alone cannot repair the missing
receipt. Creation plus one change cannot give this missing-conflict history:
receiving that change requires its creation, leaving no third entry to arrive.
This is a counterexample to tolerating a listing-contract violation, not to
the conditional stability theorem or to a provider that honors the margin.

Existing retention deadlines require finality as an input; these histories
do not disprove those conditional bounds. The shifted bounds are stated below.
Native provider measurements, persistence and cleanup are outside these
histories. [`FinalityRunner.lean`](io/FinalityRunner.lean) provides the IO
differential: [two Rust tests](../../crates/coven-sync/src/finality_tests.rs)
check observation endpoints and the missing-old-prefix counterexample, and
[a third](../../crates/coven-sync/src/store_log_finality_tests.rs) compares delayed
discovery receipts and the resulting finality horizon. `scripts/check.sh` builds
`finalityRunner` and supplies `COVEN_IO_LEAN` to all three
`io_differential_finality` tests. This does not establish general IO equivalence.

The common-cut reload proposal stages all readable audiences, fills their parts
to the maximum selected positions, and installs them together. It terminates
after deletions stop, with at most one further replacement per audience.
Retention requests first witnesses as well as replacement coverage. Its bounds
require successful reader and deletion services; an offline audience can keep
a shared write object stored. This remains a proposal, not a spec or Rust change.

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

**The writer set ignores replay verdicts — proved, with the old rule's
counterexample.** [`Writers`](io/CovenIO/Writers.lean) gives one writer set
for all three logs. `known` holds the remembered folders and every device a
received store-log entry was written from, which a registration registers.
Kept and dropped entries count alike; no verdict is an input.
`known_monotone` shows that more entries or folders never remove a writer.

`registered_named` connects this set to the store-log model's replay. If the
replay of a set of entries registers a device, an entry of that set names it.
The proof follows each effect (`effect_devices`) through a complete pass and
restarts (`scan_registered`, `settleN_registered`). A write counts only if
the replay of its own recorded view registers its device (§7.1).
`counting_write_discovered` then proves that every reader holding that view's
entries probes the device, or has drained it, whatever its own replay keeps
or drops.

[`WritersExamples`](io/CovenIO/WritersExamples.lean) checks §7.1's history.
Ben's phone registers itself while Ana concurrently removes Ben. The phone's
write reads its own registration and counts. A reader that receives the write
first holds the phone among its replay's devices; a reader that receives the
removal first does not. Under the old rule, which probed only the devices the
current replay registers, the second reader never discovered the write.
`replay_devices_miss_counting_write` shows both orders and that `known` holds
the phone in each.

`known_requests` keeps the idle bound: two listings and three GETs per known
writer not yet drained, and none naming a drained writer.
`dropped_registration_polled` counts that history's pass: Ana's device, Ben's
laptop and the phone give 11 requests. Draining is the only way a known
writer leaves the polled set.
[`IORunner.lean`](io/IORunner.lean)'s `writers` case computes `known` less
drained writers and the reader;
[a Rust test](../../crates/coven-sync/src/write_object_tests.rs) compares the
write logs each reader actually probes, in both arrival orders.

**A dropped registration retires its device — decided and proved, with the
old rule's histories checked.** Under kept retirements alone, a device named
only by dropped registrations never drained. Kept retirements are a device
removal, and a member removal joined through the devices that member's kept
registrations name, so neither names such a device.
[`DroppedRegistrationExamples`](io/CovenIO/DroppedRegistrationExamples.lean)
checks two histories. In the first (§9's table, row 5), Carol's addition
drops in replay and her device's registration drops with it; no kept entry
retires that device
(`carol_device_needs_dropped_registration`). In the second, Ben's phone
registers while Ana removes Ben; the removal retires only the devices Ben's
kept registrations name, and the phone is not one
(`phone_needs_dropped_registration`). Both devices
stay known, so every reader polled them forever and counted them in `N`.
Their writes, which count by their own recorded view (§7.1), had no landing
deadline either.

The rule chosen: a device that no kept entry registers is retired by each
dropped entry that registered it, at that entry's storage time.
[`DroppedRegistrations`](io/CovenIO/DroppedRegistrations.lean) proves what it
gives. `dropped_registration_retires` puts such a registration among the
retirements, and `kept_registration_not_retired` keeps a device that some
kept entry registers out of it. `late_after_dropped_registration` makes a
write of that device published more than 30 days after the registration late
on every reader, and `dropped_verdict_agrees` makes the verdict a function of
the replay result (the same received entries give the same result, in any
arrival order). `drained_device_writes_late` makes the drain exact for
writes: once the registration's drop is final, which needs a sample more than
30 days after it, no write of the device published at or after that sample
counts. `registered_or_retired` closes the gap: every device a received
registration registers is either registered by a kept entry or retired, so it
leaves the polled set once its drain completes (`dropped_drain_stops_polling`
keeps the request bound). `phone_write_window` checks the 30-day boundary on
the phone.

**One entry registers each device id — decided and proved, with the
counterexample checked.** Two open ends remained after the rule above. The
drain is exact only if the device sends nothing once the registration's drop
is final, and the late verdicts the retirement causes are final only if no
kept entry ever registers that id again. Replay alone gives neither: a later
entry can register the same id.
[`RegisteredOnceExamples`](io/CovenIO/RegisteredOnceExamples.lean) checks the
history (`reregistration_lifts_retirement`): Carol's device registration drops
with her addition, Ana adds Carol again, and the same device registers again
after reading everything. Replay keeps the second registration, so nothing
retires the device any more; a reader that had already drained it never reads
that second entry in the device's own folder, while every other reader keeps
it. The history registers the id twice (`carol_again_registers_twice`).

The rule chosen (§10): one entry ever registers a device id, the
installation's addition of itself or creation, whatever replay decides about
it. [`RegisteredOnce`](io/CovenIO/RegisteredOnce.lean) states it as
`RegistersOnce` and works in §9's timed replay, whose dropped entries include
the late ones (`policy_disposed`, `policy_kept_lt`).
`final_unkept_registration` proves no kept entry registers the id again: once
the registration is final and not kept, every later replay holding the
certified old prefix keeps no registration of it, because that entry is the
only candidate and C10 fixes its verdict. `final_registration_retires` makes
the device retired in every such replay, at the registration's own storage
time, so the window verdicts it causes are final with it.
`catchup_after_final_unbound` and `registration_sees_final` give the send gate
its part: a catch-up of the device started after that finality finds it
unbound, which is `Retired.Publication`'s `seesFinal` for a gate that computes
its binding from its own replay. `registration_closed` and
`registration_drain_complete` then close the device's logs after the wait:
every object there is the registration itself, stored before the finality
sample, or a gated send landing within five minutes and a day after it.
`carol_registers_once` checks the rule on Carol's history.

A registration that landed too late is one replay never keeps, so the same
theorems retire its device: under the rule it never registers that id again,
so draining on the late entry stops nothing. `late_phone_retired` checks a
phone whose only registration lands just past 30 days after an entry it never
read: replay drops it, no kept entry registers the phone, and the late
entry retires it at its own storage time. No write of such a device counts,
since no recorded view registers it; the retirement only ends its polling.

What stays outside the model: that the installation computes its binding from
its own complete replay (Rust's `Binding::Device` needs a kept registration in
its catch-up), and that it never makes a second registration (Rust refuses
one before reserving an entry, with the stored registration's reason).

[`IORunner.lean`](io/IORunner.lean)'s `retirements` case computes the retired
devices from Rust's replay verdicts, late entries among the dropped ones, and
[`io_differential_dropped_registration_retires`](../../crates/coven-sync/src/write_object_tests.rs)
compares them with the devices each reader records finality for, on the
phone history, in both arrival orders;
[`io_differential_late_registration_retires`](../../crates/coven-sync/src/store_log_landing_tests.rs)
does the same for a registration that landed too late, on the late device and
on a reader that received it.

**Snapshot positions name published writes — proved.**
[`SnapshotWrites`](io/CovenIO/SnapshotWrites.lean) composes the existing
`Publication.Writer` events across devices with snapshot capture, request and
landing. A capture may include unuploaded own writes. A snapshot request waits
until its own position is confirmed; remote positions come from published log
prefixes or previously published snapshots. Crashes retain those facts, and
later app commits may extend the queue without changing the captured cut.
The same gate applies to ordinary and raise snapshots.

`snapshot_covers_published` proves that every positive covered number has
actual bytes in the historical publication sequence, even for a snapshot that
later fails counting admission. `inherited_position_published` supplies the
induction when another snapshot is the capture's remote input.
`covered_miss_published` therefore rules out calling an unpublished local
reservation a deleted hole on the strength of another audience's coverage.
This is historical publication evidence: retention can remove the live object.

`CommonReload.Source.selectedBound` is now a theorem, not a supplied field.
The source supplies checked publication executions and catalog membership;
its frontier is computed from those logs. `Source.numbered` obtains real bytes
at every positive prefix number and applies the source's decoding/validation
certificate. The existing common-cut safety and termination proofs use these
derived bounds. Authentication, faithful snapshot contents, decoding and
retention's absent-object certificate remain explicit source obligations.

`SnapshotWriteExamples.pending_run` reaches Ana's two-part write 1, captured in
the store snapshot while its log path is absent; `pending_unpublished`: the gate
refuses it. Without that gate the same prefix passes `coveredMiss` while the other
audience has no snapshot: the checked failure of publishing unuploaded coverage.
`two_audience_raise_keeps_circle_part` publishes write 1 first; a loader gets
its circle part from the actual log. The 2/1 replacement history in
`replacement_fills_other_audience` also has checked publication/confirmation
executions and still commits both audiences through 2, under all three orders.

`io_differential_snapshot_writes` in `coven-sync` compares a breaking
migration's raise on a real device with the runner's `snapshot_writes` check.
Ana's and Ben's app writes become reservations, their upload passes landings
and confirmations, Ana's migration a reservation for its marker and a capture
of her raise's actual positions, and each raise attempt a request. Over 32
generated histories the Rust request is sent exactly when `Step.request`'s
precondition admits it, both admitted and refused requests occur, every
capture satisfies `Captured`, and every landed raise covers only published
numbers. `io_differential_ordinary_snapshot_writes` makes the same comparison
for two devices' ordinary snapshots: each capture follows its author's
completed catch-up and may hold its own unsent writes, and over 32 histories
its request is sent exactly when the model admits it.

`io_differential_publication` runs `SnapshotPublication.step` (the runner's
`publication` check) over two devices' actual publications at controlled
storage times: writes landing, a catch-up sampled fresh (`beginCatchup`, then
`read` of every writer to its miss), preparation, with the other device's
write or whole snapshot landing in between at the same time, sending, and
ticks of up to a day and a second. Over 24 histories the positions each
catch-up reaches, each preparation's admission (including refusals because a
counting snapshot landed after the catch-up), whether each landing counts, and
the `catalogCurrent .writes` selection after every step agree. A catch-up
first drops its author's earlier session that never prepared, local state the
device discarded; an attempt Rust abandons before sending has no landing, as
one the model leaves uploading.

**The preparing device: waits and abandonment — proved.**
[`SnapshotAttempts`](io/CovenIO/SnapshotAttempts.lean) adds the preparing
device to `SnapshotPublication`: the newest storage time it observed (never
ahead of storage, `observed_before`), the listed snapshots whose prefix it
cannot verify, and its attempts, each active until confirmed counting or
abandoned. Its preparation runs the remote `prepare` action, so the
publication results above still apply.

- `known_prepare_is_global`: when no listed snapshot of the audience is
  unverified, the device's check against the newest snapshot it can read is
  §15's check against the true current newest. `unverified_newest_counterexample`
  is the history without the wait: Ben's three-write snapshot is listed but
  unverified by Ana; her new snapshot dominates what she reads, fails §15's
  check, and after it lands Ben's snapshot is still every updated reader's
  selection.
- `stale_sample_never_counts`: a preparation whose sample the device already
  observes as more than a day old could never count, since it lands no
  earlier than storage time. `fresh_catchup_may_prepare`: a catch-up begun
  now samples a time the device can count, so the wait ends.
- `abandon_enabled`: an attempt past its day by observation, or with a
  confirmed late landing, can always be abandoned, freeing its writer to
  prepare again. `abandoned_never_counts_later`: a request of an attempt
  abandoned on time that lands afterwards carries that attempt's sample and
  never counts. `retry_new_path`: a new catch-up cannot take a path still
  uploading or landed. `retry_counts`: a retry whose request lands within a
  day of its fresh sample counts and is confirmed.

Bounded successful service is the explicit hypothesis of `retry_counts`;
nothing bounds catch-up or upload durations. No Rust comparison runs these
device steps: the time a Rust device observes is its session's provider
sample, which the publication differential's event trace does not carry, so
the abandonment decisions stay covered by Rust tests
(`an_attempt_counts_through_its_day_and_is_abandoned_after_it`,
`a_late_landing_is_abandoned_and_ignored_by_readers`).

The [queue proposal](queue.md#spec-edits-required-by-this-proposal) keeps old
upload records unchanged and drains the captured prefix before the raise.
It includes the empty migration marker in that prefix; later app writes wait
for a kept raise. This package proves the resulting publication and loader
boundary, not the application migration or choice of the winning raise.

**A never-counting snapshot can be deleted — proved.**
[`LateDeletion`](io/CovenIO/LateDeletion.lean): `late_deletion_unseen`
shows the current selection, a fresh catalog's deletion certificate
(`coveredMiss`) and mature coverage are the same with or without a listed
snapshot that does not count, and `late_landing_never_counts` that a landing
more than a day after its sample is one. So the author may delete its own
attempt once it confirms that landing (§15); Rust tests cover the deletion
recorded with the abandonment and its resumption. For an attempt abandoned
before any landing was confirmed (T224), `abandoned_landing_unseen` shows a
request landing at or after the abandonment never counts and its deletion is
unseen, and `counting_abandoned_landed_before` that a landing which counts was
stored before the abandonment, so the cleanup, which deletes only a landing
that does not count, never removes a snapshot a reader may select. When an
absent path is settled (the listing margin past the abandonment) is a Rust
rule; a request landing after it stays in storage and never counts.

**Raise snapshots judged only by what they cover — checked counterexample.**
A migration captures one raise snapshot per readable audience in its own
transaction, so a newer snapshot landing afterwards cannot be taken in, and
§15's preparation rule (cover the audience's current newest) would block the
raise. The proposed exemption (T200, option A) lets the raise snapshot skip
that rule, count only for the writes it covers, and leave the rest to the
logs for peers to convert. [`RaiseCoverage`](io/CovenIO/RaiseCoverage.lean)
adds the kept raise's boundary to the selection: once a raise is kept, only
its named snapshot and snapshots that read the raise entry are usable
(`boundaryCurrent`, Rust `snapshot_boundaries::allows`).

`option_a_loses_write` is the failing history. Ana migrates at time 0 and
captures the store and Gifts; Carol, the first member of Work (which Ana
cannot read) to update, captures Work. Ben's old-schema write 1, with a part
in all three audiences, lands at 100, and his snapshot of each audience
covering it lands at 200. Every device posts past it, and a day later Ben
deletes it under every §15 gate (`Eligible`). The raise snapshots land two
days in, after a completed catch-up, and the raises are kept. In all three
audiences the current newest is now the raise snapshot, which stops before
write 1: a fresh catalog certifies the deletion and the reload skips the
write as having no part there. Without the boundary the same reload would
load Ben's snapshot. `raises_absent_at_deletion` shows no raise snapshot was
listed when Ben deleted, so no retention rule that looks at listed snapshots
could have held the write.

`capture_unsound` places the cause: stamped with the catch-up time before
sending, the raise snapshot omits a write stored before that time, which is
exactly the `SnapshotHistory.Sound` premise `deleted_stays_covered` rests on.
Stamped with its capture time instead, it is sound but lands more than a day
later and never counts (`capture_stamp_never_counts`), so a raise naming it
never becomes usable.

`kept_late_raise_loses_write` shows a second gap that holds under the current
rules. Ana's store raise snapshot is prepared after a completed catch-up with
nothing landed, is sound and counts, and lands at 60; her raise entry is kept
only after Ben's later old-schema write was covered by his newer snapshot and
deleted. The boundary then excludes Ben's snapshot and the reload skips the
write. Here the raise snapshot was listed when Ben deleted, so retention that
held writes a listed, not yet kept raise snapshot omits would keep it.

**Recapture, the retention hold and its end — proved.** The rules that
replace option A are in [`RaiseHistory`](io/CovenIO/RaiseHistory.lean). A
raise snapshot is prepared like any snapshot, right after a completed
catch-up whose storage sample is its `T_c`, so it is
`SnapshotHistory.Sound`. A migration's capture is sent only when that
catch-up applied nothing past it; otherwise the device captures again, and
its later local writes join the captured prefix.
`unchanged_capture_prepares` shows such a capture meets §15's preparation
rule; storelog-data's `capture_prepared_final` shows the retirement-verdict
gate then holds for the capture as well. Its signed prefix marks it as a raise
snapshot, so retention can see it before any entry names it.

Deleting a write then needs, for the store and each part audience, coverage under every
boundary replay could still select (`Held`): the current one, the one final
entries fix, every raise or reset entry not ruled out, and every listed
counting raise snapshot that a raise could still name, each with the
snapshots whose signed view read the entry naming it. The model's history
(`Step`) lets snapshots land with the entries their views read, raise and
reset entries publish, replay keep an entry's boundary among the possible
ones, finality rule boundaries out, a listed raise snapshot expire, time pass
and writes be deleted under the hold. Selection under one boundary and how a
landing changes it are in [`RaiseSelection`](io/CovenIO/RaiseSelection.lean);
the checked histories are in
[`RaiseHistoryExamples`](io/CovenIO/RaiseHistoryExamples.lean).

A reset entry (T225) is a boundary like a raise entry, and replay can keep a
reset that is dropped now until it is final, so the hold checks it. Under a
kept reset only its snapshot and the writes that read its entry count
(§19.3); a write that read the entry was stored after it. So the model
judges a deleted write under a reset boundary only where it counts there
(`Applies`): its snapshot covers it, or it was stored at or after the reset
entry. A reset snapshot is unmarked and needs no hold while it is listed
before its entry: every write deleted by then was stored before the entry,
so under the reset it counts only if the snapshot covers it, and the
snapshot is then its only usable selection (`publish_valid`). A reset's
named snapshot must mark no raise (`usable`).

A raise entry may name any snapshot: one not listed yet, one that never
counts, or one whose prefix does not mark the raise (T216). Replay can keep
it; the reload uses its boundary only once that snapshot is listed, counts
and marks the raise (`usable`), and otherwise refuses and waits, as Rust's
`snapshot_boundaries` does. A raise entry is stored at most a day after the
`T_c` it carries (§9's landing rule; `publish`'s premise). So once storage
time is a day past a listed raise snapshot's `T_c` and no entry names it,
`expire` removes it from the possible boundaries.

- `deleted_stays_covered`: in every reachable state, under whatever usable
  boundary replay keeps, the store's and each part audience's newest usable counting snapshot
  covers every deleted write that counts under it (always under a raise or no
  boundary, `applies_unless_reset`). `reload_never_skips` turns this into the
  reload: a missing deleted write is loaded from a snapshot or the reload
  waits; it is never skipped as having no part there, except a write a kept
  reset ignores.
- The key step, `late_landing_covers`: a counting sound snapshot that lands
  after a deletion covers the deleted write, because its catch-up began after
  the write's mature witness landed. This is why a raise snapshot, or a
  reader of a raise entry, landing after the deletion is safe; `Good` keeps,
  for a boundary whose named snapshot has not landed, that its readers'
  selection is empty or covers, so the named snapshot's later landing keeps
  coverage (`landed_selection`).
- `held_without_raises`: with no raise pending the hold is exactly §15's
  existing current-coverage gate.
- `listed_raise_hold_ends`: a day after a listed raise snapshot's `T_c` with
  no entry naming it, expiry is a step of every history and leaves every
  other boundary in place. `expired_stays_out`: afterwards it is never a
  possible boundary again, no published raise entry names it and replay never
  keeps it, since no raise entry stored later counts (a reset naming it is
  ruled out with it, and a raise snapshot is never usable as a reset's). `abandoned_raise_released` and
  `abandoned_raise_expires`: in the kept-late history, Ana's device is gone
  before her entry; at Ben's deletion time her snapshot has expired and the
  hold is the current coverage his snapshot gives.
- `recapture_keeps_write`: in the option-A history, Ana's recaptured raise
  snapshots are sound and every audience's reload loads Ben's write 1.
  `kept_late_held`: in the kept-late history, Ana's listed raise snapshot is
  a possible store boundary that omits Ben's write, so the hold refuses the
  deletion.
- `dropped_reset_held` and `dropped_reset_skips` (checked counterexample to a
  hold without non-final resets, which Rust had): Carl's final reset `F` and
  his kept reset `C` both select Ben's snapshot, which covers his write 5.
  Ana's concurrent reset `R` is dropped now but not final; her reset
  snapshot was prepared before `F`'s entry landed and holds her own writes
  through 50, so under `R` it outranks Ben's snapshot and omits his write.
  Every snapshot is sound. A hold over `C` and `F` alone deletes write 5;
  replay then keeps `R` (Carl's reset drops), write 5 counts under it since
  Ben read `R`, and the reload skips it. `Held`, which includes `R`, refuses
  the deletion. Rust's `io_differential_held` comparison does not cover which
  boundaries are possible; `a_dropped_reset_that_is_not_final_holds_what_its_selection_omits`
  replays this history through `held_coverage`.

The Rust comparison `io_differential_held` (runner check `held`) computes
Rust's selection under each possible boundary on generated listings; a
boundary naming an unlisted snapshot admits only that entry's readers.

Two premises are proved separately. A final reset Rust leaves out of the
possible boundaries is never the boundary again (the `finalize` premise;
[`FinalReset`](io/CovenIO/FinalReset.lean)): dropped and final, replay never
keeps it. Kept and final, every kept entry ordered before it that is not final
is concurrent with it, and since concurrent kept entries never conflict, it
repeats that reset (`before_final_reset_repeats_it`); so the replayed state
after the reset is the one the final entries alone give, and an entry after
it that was effective there stays effective (`final_reset_never_returns`). The
Rust comparison `io_differential_boundary_select` checks that
`StoreLog::boundaries` keeps exactly the entries `FinalReset.select` finds
effective, over every sequence of up to four raises and resets. Each snapshot
is named by at most one entry, of the kind its prefix marks (the `publish`
premise, and why keying readers by named snapshot, as the model does, agrees
with Rust keying them by entry;
[`BoundaryNames.one_entry_per_snapshot`](io/CovenIO/BoundaryNames.lean)):
every preparation reserves a fresh number of its device's snapshot sequence,
and a raise or reset operation publishes one entry naming its own
preparation, assuming §2's members who are not hostile to their store.

Outside the model: that Rust replay never makes a raise at or
below the audience's finally kept version its boundary (the `finalize`
premise): a lower raise ordered after it is already in place (`Boundary::schema_in_place`),
an equal one with another snapshot conflicts with it when concurrent, and a
device prepares a raise entry only while no raise to its version or above is
kept in its replay (`schema_sync`), so it never authors one after reading such
a raise; that Rust's retention expiry uses a complete store-log catch-up whose
endpoint `T′` is past the day, so every entry able to name the snapshot was
received (§4's listing requirement); how the catch-up is completed and
confirmed; and the liveness of a raise itself, which needs its snapshot and
entry to land within the day after some completed catch-up.

**3. Snapshot coverage proposal — revision 4.** This checks a proposal;
[§15](../coven.md#15-snapshots) and Rust are unchanged. The publication and
deletion rules remain:

> a) Publish a snapshot only immediately after a catch-up that reached every
> writer's next-number miss with nothing pending. At preparation it must
> cover the current newest snapshot of that audience. Before sending, confirm
> every own write through its captured position; every remote position must
> already have publication evidence. This applies to raise snapshots too.
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
> this adds no snapshot listing to their ordinary sync.

[`SnapshotTiming`](io/CovenIO/SnapshotTiming.lean) checks the sample in the
existing `SnapshotPublication` actions. `begin_samples_time` takes provider
time before any session reads. `run_samples_before_time` and
`read_uses_prior_sample` prove that every successful read in a reachable run
uses that preceding sample; provider ticks may be equal. `read_preserves_sample`
covers reloads too. `prepare_preserves_sample` copies it into the remote
request, and `land_preserves_sample` keeps it at publication.
`completed_catchup_covers_old` separately proves position coverage, not timing.
The model has no signed bytes, retry sends, local publication confirmation or
abandonment state, so it does not prove signed-prefix preservation through
retries or the required one-day abandonment and restart. `late_snapshot_ignored`
proves only that catalog selection ignores a late landing; `act` retains
outstanding remote requests and permits a new path while an old one can land.
The Rust comparison `io_differential_deletable` runs coven's deletion
decision for one covered or uncovered write against `Deletable`: finality,
each active reader's posted position, and thirty storage days counting at
equality. `io_differential_retain_writes` compares the whole decision,
`Eligible` with `MatureCoverage`, through `retainWrites` on generated
concurrent histories: writes of two writers with parts in one or two
audiences, counting and late landings in any order, incomparable
selections, reader posts, finality that differs by writer, and passes at and
just past the one-day and thirty-day thresholds. The model keeps every
landing; coven keeps only the listed snapshots plus its own witness
evidence, so a superseded snapshot may leave the listing once another listed
one covers all it covers. Both delete the same writes at the same passes,
and each deleted write stays covered by the newest selections
(`SnapshotHistory.deleted_stays_covered`). Every snapshot in these histories
is listed at some pass before it leaves: coven cannot use a witness it never
checked, and waits for a known one to mature instead.
`io_differential_refresh_due` compares coven's request for a
fresh snapshot with `RetentionDeadline.refreshDue` on generated catalogs,
including late landings, a newer snapshot that leaves the write out and an
audience with none; there the historical-witness-only trigger
`witnessedRefreshDue` never asks (`no_first_witness`).
`io_differential_storage_deadline` runs passes at fixed spacings, with
publication and deletion inside the pass (`U = D = 0`), over a write that no
snapshot covers at first, in one audience or two: each audience's first
witness lands by its `B_a`, and the write goes by `R + 3P + U + D`, after
every witness is more than a day old (`bounded_storage`). The model's
`offlinePart` remains the case with no bound.

The required lower bound is frozen at reload start: the device's applied
positions, which hold its own writes and their past. New app writes see the old
committed view during staging, so their remote past remains within that bound
(`AtomicReload.staging_past_bounded`).
Waiting local writes apply in causal order on top of the installed remote cut.
They do not read staged rows or silently add new remote requirements.

**Candidate A — chosen; common-cut safety and termination proved.**
[`CommonReload.lean`](io/CovenIO/CommonReload.lean) computes each writer's
target as the maximum of the frozen requirement and every selected audience's
snapshot position. It runs the existing `FixedReload.scan` through that finite
prefix for every audience. Covered parts need no GET. Actual log receipts fill
the remaining parts. A certified deleted number causes that audience to select
its current counting snapshot when that snapshot reaches the number; otherwise
the write has no part there and is not the store's (`unreached_not_held`).
Deletion evidence in the selection's catalog needs no repeated GET.
`committed_store_holds` proves no number is passed: every positive number at
the committed cut is within the store's start, which carries its write's
metadata, or was received by an actual GET. The no-part step itself stays:
snapshot metadata names no part audiences, and in
`AwayReadExamples.no_part_needs_no_snapshot` no Gifts snapshot ever reaches
Ana's deleted store-only write, so a reload waiting for one would wait forever.

Keep selected snapshots fixed until a deleted hole forces their replacement.
A replacement changes only that audience, then recomputes the common target
and rescans to it. New tail writes and unrelated new snapshots do not change
selections. Targets can increase on replacement; they are not an endlessly
refreshed maximum of the remote catalog. This is the reading of A used here.
No remote retention pin is introduced. Locally loaded snapshot bodies remain
available throughout staging, as do receipts used by the committing round.

`scan_spec` proves each completed audience has coverage or an actual successful
GET for every relevant write through the target. It also proves that retained
receipts belong to that finite prefix. `committed_prefix` states this for every
published write within the writer positions. `committed_number` also rules out
unpublished gaps hidden by those positions. `committed_safe` proves safety for
every completed execution, including during continued deletion.
`common_cut_commits` joins those input certificates with one `AtomicReload.commit`:
every audience is at the same positions, and **every** waiting write's recorded
remote past is included.
The selected snapshots cannot exceed that cut. Row application, validation,
keys and schema/reset handling remain separate prerequisites, not consequences
of receiving bytes or assigning a position.

`replacement_fills_other_audience` executes the previous failure under all
three orders. Write 1 has two parts and is deleted after counting snapshots
cover it. Audience 0 replaces its state through 2, while audience 1's selected
snapshot remains through 1. The new target is 2; the reader obtains audience
1's part of write 2 from the log. The two staged states are therefore at 2.
The original `replacement_exceeds_target` remains a checked counterexample to
committing the independent fixed-target loaders without this fill.

The Rust comparison `io_differential_common_point` (runner check `reload`)
drives coven's reload rounds over generated histories: snapshots land and
writes are deleted between rounds, each catalog is listed after the round's
reads, and the runner repeats `CommonReload.scan`, one `FixedReload.scan` per
audience, replacing the retried audience from the catalog after its round.
Both sides report the replaced audience of each round, the final selections,
the common target and each audience's log receipts, and they agree. Rust
learns a deletion only from an absent read and a catalog of the readable
audiences covering the number, while the model's selection catalog already
knows every earlier deletion. The histories delete a write only while the store
and its part audiences cover it, and keep an audience that lacks a deleted
write, and has no part in it, from ever covering it later, so that knowledge
decides nothing the reads do not.

**Kept state — proved.** An audience whose existing state the reload keeps
starts at this device's positions (`CommonReload.State.kept`). It knows no
deletion, so it reads every write it lacks up to the target. A deleted one its
current newest snapshot reaches replaces it by that snapshot, like any
selection; otherwise the write has no part there.
`FixedReload.scan_spec_with` proves the scan for any start that already
covers the deletions it knows: a selection with its catalog's deletions
(`selected_known`), or a kept state with none (`known_covered`). The common-point
theorems keep their statements: `committed_safe`, `committed_number` and
`common_cut_commits` hold with kept starts, each kept state holding only
published writes (`Valid`, `start_bound`). A kept audience counts as stale
until replaced, so the termination bound below is unchanged.
`kept_state_replaced` executes a store kept with none of Ana's writes while
Gifts is selected after write 1 was deleted: a selection from that catalog
would already account for the deletion, but the kept state reads write 1,
finds it deleted, and is replaced by the store's snapshot through 2; Gifts
then reads write 2. The Rust comparison generates kept audiences at random
positions and agrees on every history, including replacements of kept states.

**Termination bound.** Let A be the number of readable audiences. After the
last deletion, a retry can only come from a kept audience or one whose
selection predates that deletion. Its replacement catalog includes every deleted number: the
snapshot covers every relevant deleted part, and the catalog certifies that
any remaining deleted number has no part in that audience. That audience
cannot request another replacement, regardless of later writes or snapshots.
`retry_stale` and `stale_decreases` prove this decrease; `terminates` obtains
an executed commit in at most **A replacements and A+1 finite rounds** from a
round boundary after deletion cessation. A fresh initial selection after that
point needs no replacement. Each round enumerates a finite cut, even if the
next replacement increases it. Snapshot positions need not increase monotonically.

`eventually_terminates` also covers a start before the last deletion. With event
index Q at deletion cessation and start event S, its bound is
`max(0, Q-S) + A` replacements. Event indices count observations, not storage
seconds. The bound does not require log appends or snapshot publication to
stop, nor a bound on their positions. It does require finite successful catalog,
snapshot-body and required log reads. No elapsed-time bound is claimed without
bounds on those operations. Continued deletion can still force replacements;
there is no unconditional fixed-target retry bound for this growing-cut loader.

**Candidate B — usable with A as fallback, not required.** A counting bundle
whose common positions reach the frozen requirement already supplies the cut.
`bundle_complete` proves its scans complete with no log GETs, and
`closed_replacement_commits` proves the atomic visibility and waiting-past
conditions. A bundle behind the frozen requirement still needs log filling.
If no publisher's bundle covers every readable audience, use A across the
whole readable audience set, including audiences loaded from other bundles;
committing bundle groups independently would recreate the common-point failure.
Thus B with this fallback inherits A's safety and bound. Publication of bundles
is unnecessary for either result.

Having a reader for each audience does not guarantee a publisher covering their
union. The checked example uses two circle audiences, omitting their shared
store audience: existing publishers read `{0}` and `{1}`, and neither supplies
a complete bundle for a new loader of `{0,1}`. The loader cannot publish its own bundle before obtaining its state. Waiting only
for a complete bundle supplies no termination bound. Preferred bundles must
also pass the ordinary counting/body checks; their grouping does not extend
the one-day upload allowance or delay per-audience retention.

**Candidate C — satisfied by A's construction.**
`closed_replacement_commits` is the required safety condition, not by itself a
method for finding a cut. `common_cut_commits` supplies that cut from independent
snapshots and actual log receipts after deletion. It can move above the frozen
requirement and does not reconstruct an unavailable older cut. The construction
needs neither a co-reading publisher nor indefinite retention pins.

**Retention trigger — R3' includes first witnesses.** Retention runs every
pass. For every final write that passes the positions-or-thirty-day gate,
request a fresh snapshot under a) in that or the next pass for the store and
each readable part audience whose current newest counting snapshot does not
cover it.
Historical coverage is not a precondition. `refreshDue` implements this rule.
`witnessedRefreshDue` preserves the historical-only trigger for the checked
`no_first_witness` failure: one old final write, no snapshots, and an online
reader forever unable to request its first witness under the old rule.
`first_snapshot_deleted` executes the changed request decision, catch-up,
publication, witness aging and successful retention for that same write.

Exactly one day after T_c counts; deletion needs strictly more than a day
since witness landing. Superseding a witness does not restart its age.
All three orders are checked: write count, T_c and storage time, with the
smaller path breaking ties. `SnapshotHistory.deleted_stays_covered` continues
to prove current coverage of every deleted part through arbitrary counting
publications. A counting arrival after a witness's one-day age must have
caught up after the witnessed write. Successive snapshots need not dominate
one another at arrival.

**Storage deadlines — per audience, conditional on an available reader.**
Let G be a time from which finality and the positions-or-thirty-day gate hold.
Let P bound pass spacing, U bound complete successful counting publication
(including catch-up, preparation, failed attempts and confirmation), and D bound
successful DELETE completion. Merely bounding an upload request is insufficient.
Rule a) does not prove a busy publisher reaches all misses; completion is part
of U. An eventually available reader supplies a deadline relative to its
availability, not a fixed deadline measured from G regardless of its return.

For already witnessed writes, `bounded_witnessed_storage` preserves exactly
the prior bound. If all historical witnesses landed by B, put

```
R = max(G, B + day + 1)
deletion deadline = R + 3P + U + D
```

The new loader imposes no extra delay or hypothesis on this theorem. Where
current coverage already holds, `scheduled_deletion_bound` still gives
`R + P + D`. `quiet_deadline` retains the witnessed history's result at
day 30 plus seven seconds. `service_covers` derives current coverage from the required
refresh, including selection races under all three orders. For count order,
`Normalized` equates the encoded count with positions over the writer inventory.

For audience a without an assumed witness, let O_a be when its reader can
supply those successful bounded services, and W the write's stored time. Set

```
H_a = max(G, O_a, W + 1)
B_a = H_a + 2P + U
R_a = B_a + day + 1
```

`AudienceRetention.first_witness` obtains a counting covering snapshot by B_a
from `WitnessService`, including an empty initial catalog. Its due observation
either finds current coverage or requests the needed snapshot. No first witness
is assumed. `audience_deadline` proves mature historical coverage and current
coverage for that audience by `R_a + 2P + U`, under the subsequent coverage
service. The `W+1` accounts for strict-before catch-up evidence at integer storage
times. A known first witness can give an earlier bound. The fresh witness's
one-day age cannot be omitted.

For physical deletion of the complete write, use `B = max_a B_a` over **the
store and all its part audiences**, then the same `R + 3P + U + D` formula. `bounded_storage`
connects the first-witness services to the existing retention and DELETE theorem.
`first_snapshot_deadline` instantiates the entire theorem on an initially
unsnapshotted write. Finality is checked per write; `nonfinal_write_kept` and
`mixed_finality_retention` still keep ineligible candidates stored.

There is **no storage bound for an audience with no available reader** when its
coverage is missing. Nor can an online audience alone bound removal of a log
object shared with such an audience. `offline_reachable` and the checked
`offlinePart` example use the smallest shared-object case: one write, two parts,
a counting store snapshot (audience 1), and an offline circle (audience 0)
with no reader or snapshot. Device 0 authors the write and goes offline; device
1 can read only the store and publishes a complete bundle for that readable set.
This respects B's publication rule as well as A and C.
Every deadline can pass while retention keeps that object. Under an independent
physical per-audience reading, none of A, B or C meets all three requested
properties. The smallest further qualification is to bound audience coverage separately and require successful
services for every part when bounding the indivisible object's deletion. If a
physical storage bound independent of other part audiences is required instead,
the deletion unit must change to independently removable parts. No such format
or spec change is made here. Already mature current coverage can permit deletion
even while its readers are offline, provided an authorized deletion owner runs.

**Existing histories and model boundaries.** The per-audience `FixedReload`
proofs remain, including their N-retry bound for a fixed prefix.
The moving-tail, other-audience hole, fresh/stale selection races and cached
catalog failure retain their checked results. `old_races_preserved` still
excludes symmetric deletion and late counting publication. Snapshot-number
gaps, permanent refusal, clock and retirement examples are unchanged.

`CommonReload.Source` supplies a fixed writer inventory, unique published
numbered paths, exact finite prefix enumeration, validated snapshots, the
`SnapshotHistory.Run` deletion invariant, retained deletion evidence and fresh
complete certificates after absent GETs. Checked write/snapshot publication
executions supply the gap-free frontiers and derive each selected cut's bound;
peer posted positions do not establish them. `availableFrom`
limits each prefix's missing-read obligation to times after it was selected,
so later publication is not mistaken for earlier deletion. Writer discovery,
unknown reservations, catalog pagination and snapshot-body availability remain
source obligations. Physical snapshot removal and durable witness preservation
are outside the catalog model. Failed requests, unavailable keys and permanent
refusal do not certify success. Actual database transactions, row merge,
cryptography, changing membership and schema/reset eligibility remain outside
these new IO proofs. The bounded-storage result concerns write objects, not
snapshot objects, files or total bytes at an unbounded write rate.

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

**4a. Away reads past deleted numbers — proved; retention keeps the store
covering every deleted write.** Retention deletes a write only once the store's
current newest counting snapshot covers it as well as each part audience's,
with a mature witness (`Write.holders`: the store and the part audiences, in
`Covered`, `MatureCoverage` and `Sound`). Every member reads the store, and its
snapshots carry every consumed write's metadata.
[`AwayReadExamples`](io/CovenIO/AwayReadExamples.lean) checks the history that
needed this. Ana's write 1 has only a Gifts part and her write 2 only a store
part; Dave reads only the store and joins later. Deleted under Gifts' coverage
alone, write 1 left no store snapshot naming it, and Dave's GET of it missed
forever while write 2 stayed stored; `gifts_only_deletion_refused` shows that
deletion is no longer `Eligible`, and `ReloadExamples.other_audience_hole_refused`
that the old history's deletion step is refused.

[`AwayRead.scan`](io/CovenIO/AwayRead.lean) is the away read under the rule. On
a miss it lists the store's snapshots: if the current newest reaches the
number, it loads that snapshot (`reload_spec`); otherwise the miss is terminal.
It never passes a number.

- `store_reaches_deleted`: the store's current newest reaches every deleted
  write, from `SnapshotHistory.deleted_stays_covered`.
- `miss_spec`: up to a terminal miss, every number was received by an actual GET.
- `terminal_unstored`: the terminal number was not stored at its GET; had it
  been, it would be deleted and the store's snapshot would reach it. This
  holds for a write with no part, a migration marker, too.
- `store_covered_never_passes`: no number up to the terminal miss is passed,
  and every write past it was stored no earlier than the miss, so a checkpoint
  sampled before the reads protects it. `away_complete` follows.
- `away_read_reloads` and `away_read_receives` run Dave's history: his miss of
  write 1 loads the store's snapshot, and he then receives write 2.

Rust's away path lists each readable audience's snapshots once on return and
loads the newest that advances (§15), rather than listing the store after each
miss; `a_store_reader_joining_after_a_circle_only_write_is_deleted_receives_later_writes`
in [`snapshot_return_tests.rs`](../../crates/coven-sync/src/snapshot_return_tests.rs)
runs Dave's history through real retention and that path. The reload passes no
number either: every deleted write it lacks is held by the store's selection or
kept state, whose metadata advances the position (`committed_store_holds`).

**5. Finality input — proved with the one-hour listing requirement.**
`observationTime` subtracts the one-hour margin and one positive provider unit
from the clock object's storage time. `old_immutable_present` proves backward
through `ProviderRun` that an immutable object with an earlier timestamp was
already present. Arbitrarily many publications may share a provider tick.
`through_time` supplies objects at or before the revised endpoint.

`Refinement.finality_precondition` constructs
`CovenStorelog.Horizon.CompleteOld` from completed receipts.
`ListingFinality.completed_prefix` derives those receipts from a conforming
listing and completed scans, including the entire inclusive recent window;
`quiet_agrees` supplies the full-history quiet test needed by
[C10](storelog.md#c10-finality-by-storage-time). These results require causal
validity and authenticated receipt, not a successful clock request alone.
`Examples.timestamp_not_frontier` still excludes same-tick later publication.
Underflow certifies no old entry. Native subsecond timestamp representation
and provider conformance remain implementation obligations.

`shifted_age` proves `stored + duration < T′` exactly when
`stored + duration + M + δ < T`. `sample_shift` proves that the same endpoint
needs M more provider-time units than the preceding-unit rule.
`quiet_after_last_late` gives the corresponding conditional finality bound:
after the last late entry at L, the window is quiet when
`T > L + 30 days + M + δ`. Later genuine concurrency can still delay it.

`storage_complete` supplies the data package's actual complete-prefix check.
`retained_inputs_bound` applies `StorageFinality.bounded_storage` when every
dependency satisfies `stored + 30 days + M + δ < T`, the observed window
is quiet and the other retention checks pass. Exactly the inputs still
required remain; the bound relative to required data is unchanged.

All conditional storage bounds above take G from actual established finality
and the reader/age gate. With that gate shifted from G to `G + M`:

- Already covered writes use `max(G + M, B + day + 1) + P + D`.
- Witnessed writes needing coverage use `max(G + M, B + day + 1) + 3P + U + D`.
- First witnesses use `H_a = max(G + M, O_a, W + 1)`, then
  `B_a = H_a + 2P + U`; audience coverage is ready by
  `H_a + 4P + 2U + day + 1`.
- Whole-write deletion uses `B = max_a B_a` and
  `max(G + M, B + day + 1) + 3P + U + D`.

`covered_deadline_shift`, `witnessed_deadline_shift`,
`first_witness_deadline_shift`, `audience_deadline_shift` and
`storage_deadline_shift` prove that each formula increases by at most M.
These retain the existing successful-service and reader-availability premises;
no unconditional storage deadline is introduced. The provider tick is already
in the original observation delay and is retained in the revised rule.
`retired_publication_shift` keeps the post-finality drain bound at established
finality plus one day and five minutes; shifting that finality time by M shifts
its deadline by M. Snapshot maturity, thirty-day write admission, and the
29-day recent-return check use their original storage times and do not shift.

**6. Positions as discovery bounds — checked counterexample.**
`Examples.positions_not_index` uploads write 9 while the confirmed post
still says 8. An exact GET returns 9. A crash between these requests changes
neither stored object. Posted positions protect retention; they do not bound
discovery.

**7. Retired devices (items 15, 16 and 18) — verdict and timed closure
proved; complete drains require successful discovery.**
`Retired.writeAllowed` uses only immutable write storage time, whether the
replay of the write's own `store_log_read` retires its device, and the kept
entries that retire its device. Retirement includes removal of the device,
removal of its member and replacement of its id. `writeAllowed_iff` states it:
a write whose own view retires its device never counts, and every other write
must land within 30 days of each kept retirement of its device, read or not.
`write_verdict_agrees` proves agreement for the same kept entries and view,
independently of list order. `kept_retirement_rejects` rejects a landing
strictly after a kept retirement's 30-day deadline, and
`view_retirement_rejects` a write whose own view retires its device.
`read_retirement_judged_by_time` shows reading a retirement excludes nothing
by itself. `dropped_retirement_restores` removes an exclusion when no other
kept retirement blocks the write. `retired_write_boundary_and_restore` checks
equality, one second beyond, restoration after dropping the retirement, and
the own-view rejection. `read_unretired_write_counts` checks that a write
whose own view keeps no retirement of its device is judged by a kept
retirement's window.
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
[`IORunner.lean`](io/IORunner.lean)'s `drain` case executes `readLog` over the
three logs, the strict wait after `F` and `Retired.polling`;
[a Rust test](../../crates/coven-sync/src/write_drain_tests.rs) compares the
actual drain mark and the next pass's probes at and past that wait, with a
failed read in each log.

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
one-time removed-device file-folder scan. The Rust comparison
`io_differential_file_scan` folds `observeFiles` over failed and completed
listings after a cut-off.

`Folder` adds the uploader's access to that scan. The uploader publishes into
its folder only while its access is not confirmed taken away; a retention
lists the folder only after that cut-off, and the device's one owner runs a
retention whole, so no transaction recording access lands inside a listing.
The transaction that records any of the access restored forgets the scan.

- `scan_names_every_file`: however uploads, restores, revocations and
  retentions interleave, a completed scan stands for a cut-off still in force
  and names every file the folder holds.
- `revoked_listing_completes`: after a revocation, one retention whose listing
  completes records such a scan.
- `retention_forgetting_counterexample`: forgetting the scan only when a
  retention next sees the access restored, as before T210, keeps an empty scan
  across a restore, an upload and a new revocation between two retentions, so
  the uploaded photo is never listed.

The Rust comparison `io_differential_folder_restore` drives Carol's removal,
Ana's revocation of her account, Ben moving to her account and back, uploads
and listings that complete or fail, and compares after each event whether a
scan exists and whether it found every upload. Orphan enumeration beyond the
folder and reference-safe deletion remain implementation obligations.

## Requests and retained observations

**8. Cost and reuse — proved for modeled traces.** `idle_two_listings`,
`idle_gets` and `idle_requests` give exactly two listings and 3N next-number
GETs. N counts the known writers not yet drained, including this one:
active, removed and replaced devices, and devices named only by dropped
registrations until a dropped or late registration drains them
(`Writers.known_requests`, `Writers.dropped_drain_stops_polling`). Drained ids add no log probes; their permanent folders and
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

[`io_differential_upload_worker`](../../crates/coven-sync/src/file_upload_retry_tests.rs)
runs the IO runner's `send_gate` check over the real upload worker's requests
with the upload actor: session begin, parts crossing the five-minute bound,
resumption after each new catch-up, and the comparison read after a lost
create reply. Each request's refusal by the shared gate matches `gateStep`.

The literal statement “the pass contains no file transfers” is false:
`pass_can_download_file` is a checked pass trace with an eager file-range read,
as required by sync-pass step 7. The chosen reading of item 17 is no file
**uploads** in the pass. Captured attachment keys, source validation, actual
transfer limits, worker scheduling and its independence from pass completion
remain outside this trace model. Moving requests to the worker does not
remove them from total IO accounting.

### One pass's phases

**A pass's work uses its own first phase — proved for the modeled pass.**
[`PassPhases`](io/CovenIO/PassPhases.lean) runs one pass over the same gate.
Phase 1 reads the entry and copy logs with `readLog`. It is complete when
both logs reach a terminal miss and every copy's clear prefix decodes; only
then does it record its catch-up, started at the pass's start. The pass then
sends, in this order: copies reserved earlier, this pass's new shares, the
reserved entry, due operations (each operation's copies before its entry),
attempted writes, untried writes, operations resumed by those uploads, and
positions. Every ordinary request goes through Trace's `gateStep`. A refused
request ends the pass's sending; that work waits for the next pass.

What each theorem shows:

- `gated`: every request except an admission was accepted by the gate, so
  its catch-up completed with a sending identity and started less than five
  minutes before the request.
- `current_uses_this_pass`: work decided from the current membership and
  copies — a new share, a new entry, an admission or a first attempt — uses a
  catch-up started within this pass. `caught_uses_this_pass` says the same
  for every request of a pass whose phase 1 completed.
- `incomplete_only_retries`: after an incomplete phase 1 the pass decides
  nothing new. It only resends copies and the entry reserved before the pass
  and attempted writes, and posts positions, each under the last completed
  catch-up from before the pass. That catch-up must still be fresh.
- `begin_iff_complete`: the data package's `KeyPhases.begin`, which
  `firstSend` needs, succeeds exactly when phase 1 is complete. So first
  attempts select keys from this pass's membership and copies.
- `sent_prefix`: what a pass sends is a prefix of the order above. Copies go
  up in number order, an operation's copies before its entry, and writes in
  queue order: an untried write never precedes an attempted one.
- `keys_before_data`: if any write goes up, every planned phase-2 copy and
  entry went up before it, including a needed rotation's copies and entry.
- `admission_rescans` and `admission_identity`: an entry published under
  §10's admission exception uses a membership-complete catch-up of a member
  that is not retired and has no ordinary authority yet. Every ordinary
  request after it uses a catch-up that started after it, so that observation
  includes the admission.
- `invite_after_reserved`: publishing and confirming every reserved copy in
  number order confirms all of them, from any reachable copy log, including
  one with a lost reply. An invite that reads its copy number after this names
  a number at or after every copy reserved before it.
  `unsettled_cursor_precedes_reserved` checks that reading it first names an
  earlier number.

**Deciding phase 1 from its entries alone — checked counterexample.**
`entries_only_reuses_previous`: the previous pass's catch-up started at
second 0. This pass starts at second 10 and reads every entry and copy to its
miss, but one copy's clear prefix does not decode. If the pass counts phase 1
as complete because its entries completed and the earlier catch-up is still
fresh, write 1's first attempt goes up with the previous pass's copies. The
copy this pass could not read may expose that key. Deciding from the pass's
own observation sends only positions. `failed_sends_nothing`: a failed read
in phase 1 ends the pass with nothing sent.

[`IORunner.lean`](io/IORunner.lean)'s `phases` case runs `PassPhases.run`
for the Rust test `io_differential_pass_phases`
([`operation_pass_tests.rs`](../../crates/coven-sync/src/operation_pass_tests.rs)).
It covers a failed entry or copy read, a refused entry, an unreadable copy
prefix, an attempted write from an earlier pass, and that pass's catch-up
being fresh or five minutes old. The unreadable prefix with a fresh earlier
catch-up is `entries_only_reuses_previous`: the pass makes no first attempt. It compares whether an untried write got its first attempt and whether
the attempted write was resent.

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
  unit, the one-hour margin and a clock sample preceding the entry reads. Finality's storage cutoff
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
proofs. `IOEvent.checked` alone has no receipt payload; the bridge therefore
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
retry from app/update work; `permanent_storage_never_retries` keeps a
permanent provider failure for the app. `eviction_preserves_refusal` preserves permanent
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
[`io_differential_waiting`](../../crates/coven-sync/src/retry_waits_tests.rs)
runs the IO runner's `waiting` check against the Rust delay, restart arm and
due decisions before and after an app retry.

**Evidence-triggered attempts — proved, with §3.1's unchanged-wait bound.**
`WaitingEvidence.lean` models one operation's wait as `retry_waits.rs` and
`operation_evidence.rs` run it. The wait names the prerequisite its attempt
ended on, and is due when its monotonic deadline passes or when a commit
changing that prerequisite arrives after the wait began. A commit changing any
other prerequisite leaves it as it was (`unrelated_commit_ignored`). An attempt
that fails again continues the count (`evidence_keeps_backoff`), and the wait
it arms is not due again until its prerequisite changes or its delay passes
(`evidence_due_once`).

`evidence_rate` proves that in any run starts are at most one, plus one per
commit that woke a wait, plus the elapsed time over the delay: every start
after the first is paid for by a full delay or by one such commit. A wait
whose prerequisite no commit changes is §3.1's unchanged wait, and
`unchanged_rate` proves §3.1's bound for it: at most `1 + floor(t / d)`
starts (`unchanged_one_second` for 1 second, `unchanged_capped` for 300 once
nine failures are counted; `unchanged_waits_rate` sums `W` waits).

T190 checked the rule this replaces, where any commit to a prerequisite table,
or any operation advancing, counted for every waiting operation: three
operations advancing in one drive started a capped wait three times at one
instant, where §3.1 allows one. `unrelated_burst_starts_none` runs the same
three commits, now to other prerequisites, and no attempt starts;
`own_commits_each_wake` shows three commits to the wait's own prerequisite
each paying for a start.

A commit landing while an attempt runs, after the attempt read its
prerequisite, is evidence against the wait the attempt ends in when it changes
the prerequisite that wait names: the wait begins when the attempt started.
`racing_evidence_due` and `racing_commit_wakes` prove the wait is then due at
once, and `racing_commit_wakes_at_once` checks it on a capped wait whose delay
is 300 seconds; `unraced_waits_full_delay` checks that the same attempt
without the racing commit, or with one changing another prerequisite, waits
that delay. The attempt's own commits are no evidence events in the model:
Rust attributes each commit an attempt makes to that attempt's `CommitScope`
and its wait observes only commits outside it, so they cannot make the
attempt's own wait due at once.

[`io_differential_waiting_evidence`](../../crates/coven-sync/src/operation_evidence_tests.rs)
runs the real operation owner against the model: other work posts devices'
positions, attempts fail waiting on one device's positions after other work
posted some while they ran, the wall clock is set back, and the store reopens,
sometimes in the middle of an attempt.

**The rows a wait names — proved.** In Rust a wait names prerequisites
(`PrerequisiteRows`: an entry, key copy or write, a device's registration, a
peer's positions, an audience's reload and the reload operation it waits for,
the finality horizon, the boundaries, another operation, a provider account's
access), the key a `KeyUnavailable` wait lacks, or the storage connection. Each
prerequisite names whole rows of the local schema, by table and primary key.
[`PrerequisiteRows`](io/CovenIO/PrerequisiteRows.lean) models those rows and
what the waiting work reads through them: every row as it is, except a write's
applied position, of which it reads only whether it reaches the write.

The rows are exactly what the waiting work reads. A commit that changes what it
reads changes one of the rows, so it wakes the wait and nothing unblocking is
missed (`reading_frame`, `unblocking_commit_named`). Every row named can by
itself change what it reads, so no row is watched for nothing
(`named_row_matters`). The only evidence that changes nothing read is a
partial advance: a write's writer position moving without reaching the write
(`only_partial_advances`, `partial_advance_counts`). Each one is a waking
commit that `evidence_rate` charges for, and while positions only rise there
are fewer of them than writes still missing before the awaited one
(`partial_advances_bounded`); a reload can set positions back, so this count
holds between reloads. A condition that names no row, key or connection (an
object only storage supplies, such as a snapshot or request, or a local
failure) waits for time alone (`time_only`).
[`io_differential_prerequisite_rows`](../../crates/coven-database/src/prerequisite_rows_tests.rs)
compares the tables and keys `PrerequisiteRows::reads` watches with the model's
rows, and
[`io_differential_wait_rows`](../../crates/coven-sync/src/operation_evidence_tests.rs)
compares what each kind of recorded condition names. That every path which
satisfies a prerequisite commits to one of its rows is a Rust obligation:
`a_write_wait_counts_a_partial_advance_of_its_writer`,
`a_wait_observes_its_rows_outside_its_attempt` and
`a_key_wait_wakes_when_its_key_arrives` check a write's partial advance, a
peer's positions and the key a wait lacks.

**Reopening — proved.** [`WaitingReopen`](io/CovenIO/WaitingReopen.lean) adds
reopening to the session. A wait persists its wall-clock deadline `T` and its
full wait `W`. Reopening arms `max 0 (min (T - now) W)` on the monotonic clock,
keeps the failure count and the prerequisite, and watches that prerequisite
from the reopen on: a commit before the reopen is never seen, so a wait woken
before closing is not woken after (`reopen_forgets_evidence`). An attempt
running when the store closed is lost, and its operation's previous wait comes
back.

A wait whose prerequisite was satisfied before the reopen is not stalled: under
any wall clock the restored wait is due within its full wait, through any
commits that follow (`reopen_within_wait`, `reopened_due`). With no forward
wall-clock jump, starts across any number of reopens are at most §3.1's count
plus one per reopen (`reopen_rate`, `reopen_unchanged_rate`). The extra start
is the lost attempt starting again, since its previous wait may already be due
(`reopen_mid_attempt_restarts`); a reopen with no attempt running adds nothing
(`idle_reopen_keeps_count`). Within one uninterrupted run this is exactly
`unchanged_rate`. A wall clock moved forward can make a persisted deadline look
past and end a wait at once (`forward_jump_ends_wait`), which is why §3.1
states the restart bound only without one.

**A raise and a reload never wait on each other — proved.** A queued entry's
publication waits while a reload is pending, so a raise whose own queued
entry has not published records a wait on that reload, naming the reload
operation. A raise snapshot also waits for its own captured prefix, such as
its migration marker, to be stored, and in a pass it is prepared only after
step 6's write-log catch-up. Only step 6 sends or receives writes and catches
up on them, and a pending reload holds step 6 back, so a wait on a write or a
write-log catch-up is a wait on the reload too (`step_six_waits_on_reload`).
A store-log catch-up, which §10's publication gate waits on, runs before
step 6, so a raise waiting on one keeps its place
(`store_log_catch_up_keeps_place`). The worker
used to skip every reload while a raise held the writer slot, so each waited
on the other (`DriveOrder.old_cycle`); the next rule yielded only to a
condition naming the reload, so a raise waiting on its write still held the
reload back (`named_only_cycle`), and the one after it did not yield to a
raise preparing after the write-log catch-up (`write_only_cycle`). Now a
raise that waits on a reload, a write or a write-log catch-up yields its
place, and the reload runs
(`no_cycle`); a raise waiting on anything else still goes first
(`raise_keeps_its_place`). The drive reads the raise's condition when it
reaches each reload, since the raise may have recorded it in the same drive.
[`DriveOrder`](io/CovenIO/DriveOrder.lean) models only this choice, not the
rest of the drive.
[`a_raise_waiting_on_a_reload_yields_to_it`](../../crates/coven-sync/src/operation_evidence_tests.rs)
runs the real drive each way: the reload is skipped while the raise waits on
positions, and runs once the raise's condition names it, a write or a
write-log catch-up;
[`a_raise_stopped_by_the_publication_gate_keeps_its_place`](../../crates/coven-sync/src/operation_evidence_tests.rs)
keeps the reload waiting behind a raise whose send met the gate.

**Wakes — proved.** `Wakes.lean` models the sync loop and its write-queue
observation (`sync_loop.rs`, `Database::sync_changes`). `write_never_lost`:
in every reachable state, a queued write that no pass has read keeps a wake,
whatever raced it. `bookkeeping_retains_none`: an attempt's update or a
confirmed upload's removal leaves the wake as it was. `racing_passes`: after a
pass during which any commits and commands arrived, before or after it read
the queue, the loop runs exactly one more pass if one of them was an app write
or `sync_now`, and none otherwise (`one_pass` for the wake itself).
[`io_differential_wakes`](../../crates/coven-database/src/database_changes_tests.rs)
compares the model's observation with the real one over random writes,
attempts, confirmations and pass starts. The loop's select and its
`take_changed` call before reading the queue are covered by the Rust test
`a_write_racing_a_pass_retains_one_pass_and_own_commits_retain_none`, not by a
differential. A failed pass keeps the writes it read for the idle schedule,
which the model leaves outside.

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
bytes. The common-cut loader supplies aligned snapshot/log input certificates
under its source and completion hypotheses; it does not prove row application.

## Rust checks outside the model

These obligations extend beyond the finality differential described above.
The model does not establish them.

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
  must stay outside the finality observation. Check omissions at exactly
  T minus M and receipt through T minus M minus δ.
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
- The reserved store-log entry is held after an incomplete phase 1 by the
  pass's own settling step, while an operation already publishing its reserved
  entry and copies resends them. Both send only fixed attempts, as
  `incomplete_only_retries` allows, but the two paths differ.

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

Chosen readings: a snapshot can be captured before its own writes upload,
but its request waits for confirmation through the captured position. A raise
has the same gate as an ordinary snapshot, and does not wait for subsequent
app writes. Remote coverage comes from published input. The other proposed
snapshot rule is checked at preparation, with
T_c sampled before catch-up reads, strict one-day deletion age, inclusive
one-day upload admission, and historical covering evidence. Path ties choose
the smaller path. Reload freezes its required lower bound and fills to the
maximum selected snapshot positions, retaining selections until a deleted hole forces replacement.
Fresh catalogs certify holes; the store holds every deleted number. The
common cut is installed atomically and preserves every waiting remote past.
Current retention coverage and first witnesses are derived from reader services;
reader availability and bounds on passes, complete counting publication and
deletion remain explicit. Whole-object deadlines require the store and every part audience.
A write miss is recent only at its own observation, with a strict 29-day limit and an explicit
provider/elapsed-time bound. Finality uses T minus one hour minus a positive provider unit, with
underflow certifying no old entries. Retirement timing begins at finality and
uses a shared duration scale. Dropping one retirement restores admission only
when no other kept retirement blocks it. Out-of-band file work means uploads;
eager downloads remain in the pass. Numbering starts at 1, completion requires
retained input and terminal observation, and byte receipt is not application.

Pass phases: phase 1 is complete only when both logs end in a terminal miss
and every copy prefix decodes; a refused entry, exhausted scan or unreadable
prefix leaves it incomplete and the pass continues, while a failed read ends
the pass with nothing sent. After an incomplete phase 1, fixed attempts —
copies and the entry reserved before the pass, and attempted writes — may be
resent under the last completed fresh catch-up; new shares, new entries,
admission and first attempts wait. Operations need phase 1 completed with
sending authority; a pass that completed it without that authority holds its
reserved copies, admits its registration, observes again, then makes first
attempts but runs no operation. A refused request ends the pass's sending.
Requests are sequential, at clock readings no earlier than the pass's start;
the admission theorem needs those readings to be nondecreasing. Waiting
operations, reloads, snapshots, retention and file work are outside this pass
model.

## Spec publication edit

In §15, before “Before uploading a snapshot, its writer opens the sealed
temporary file”, add: a snapshot may be captured with unuploaded own writes,
but it must not be sent until storage confirms every own write through its
captured position. Remote positions require publication evidence from checked
log objects or snapshots obeying this same rule. The requirement applies to
ordinary snapshots and snapshots named by a raise; later app writes do not
extend the captured cut or delay its publication.

Keep §15's inference that a covered missing number was published and deleted,
with the existing fresh-catalog and retention requirements. A signed prefix
alone is insufficient if its publisher can claim an unuploaded reservation.
The queue write-up lists the §17.1 sentences and §18.1 sequencing that must
change together. No spec files are edited by this proposal.
