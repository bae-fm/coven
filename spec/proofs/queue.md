# Device upload queues and resumable operations

This package checks durability and atomicity in [§3](../coven.md#3-guarantees),
the queue rules in [§6](../coven.md#6-syncing-writes),
[§7](../coven.md#7-order), [§10](../coven.md#10-device-identity),
[§11](../coven.md#11-keys), [§15](../coven.md#15-snapshots),
[§17.1](../coven.md#171-host-application), [§18](../coven.md#18-operations),
and [§19.3](../coven.md#193-resetting-a-store).
The Lean source is in [queue/](queue/), using Lean 4.34.1 without Mathlib.

The schema-exclusion and conversion model is a proposal implementing the
data-dependency principle together with upload before a schema raise. It
supersedes §17.1's inherited exclusion through recorded reads and conversion
of untried upload records; the normative spec and Rust are unchanged. The edit
checklist below identifies the required changes.

There are checked limits to the requested claims. Storage accepting retries
does not help a write that never reaches its first attempt. And §6 and §14.6
allow a departed circle member to first-send an old edit with the old key.
Both have Lean examples below. Further examples distinguish crash retries
from replay restarts, and visible storage objects from atomic application.

## The model

A committed record has a device, number, timestamp, recorded reads, schema
version, disposition and plaintext row changes. Each change retains its row
identity, generation, operation, generation-source write and replaced cell
setters. Its content is either ordinary values or a frozen original change
with a loss cause. Commit assigns the next number.
Only the head can be attempted or acknowledged. The first attempt fixes the
format and all sealing key ids. Migration preserves every queued record,
including untried plaintext, in its original schema. Receive conversion produces
a separate interpretation; it changes neither the upload record nor its attempt.
Only uncovered late ordinary writes use the winning raise's converter. The
raising device's captured old writes upload first and enter the snapshot;
other devices' late writes are interpreted on receipt. After a competing raise,
the original author uses that same receive path when rebuilding its data.

The queue retains plaintext and key choices, not ciphertext. An arbitrary
deterministic encoding function produces the bytes. Sending, storage accepting
the bytes, and the device learning that they are stored are separate events.
An earlier request can land after a failure or a read that found nothing.
An occupied path succeeds only after a complete equal-byte comparison.
The process also has an uncommitted write and a received reply in memory.
`Step.crash` discards both and preserves the committed queue and storage.
Requests already handed to the provider can land before or after the crash;
they need not land at all. Receiving an equal-byte reply and committing the
acknowledgement are separate steps. A lost reply requires another comparison.

An operation has an `_coven_operations` row with an id, kind-specific data
and `last_step`. Rotation data fixes its key, hash, sealed copies, entry
plaintext and sealing key. Snapshot data fixes the captured database state,
key and sealed bytes. Copy checkpoints refine the grouped copy step in §18.1;
the snapshot upload and final verification are separate checkpoints.

A local draft, its durable file or custody value, and the journal transaction
are separate. The artifact must survive a crash before the transaction can
advance `last_step`. A crash after storing it but before that transaction
leaves the earlier checkpoint and the fixed artifact available for retry.
Remote creates, their replies, checkpoint transactions and final verification
are also separate. Finishing deletes the row. The model retains its id and
data afterward only as evidence about the execution, not as another table.

Each audience has its own selected raise and reset. The received history
supplies checked author authority and immutable-object validation results.
`classify` decides the write's own admission: applied, schema-excluded,
reset-ignored or refused. Reads still determine receive readiness, including
implicit earlier writes by the same device. They do not propagate exclusion.
`recorded_reads_do_not_exclude` checks that changing a recorded write frontier
cannot change this admission decision.

`classifyChange` uses the shared
[`DataDependency.dependsOnDropped` definition](merge.md#b17-receive-order-progress-and-dropping-by-data),
through its `retained` decision. A change loses if its generation source or a
cell setter it replaced has a dropped change at that row. It never uses the
source write's other rows to decide. Recursion carries losses through later
changes without dropping an unrelated change in the same write. One D5 row
change is the unit, including all its columns; a delete retains an identity
even with no scalar values.

The adoption-exclusion list and read-based conversion refusal are removed.
No read-adoption marker is needed for data dependency. The selected raise/reset,
snapshot coverage and `store_log_read` remain for their own boundary rules.
Authentic provenance and admission facts remain retained merge evidence,
separate from dismissible loss display records. Loss dismissal cannot change
them. This requires retained evidence beyond today's compact D5/D7 records;
it is not a claim that the current wire bytes provide previous setter ids.
`View.drops` carries the selected snapshot's per-change loss verdicts;
`captureDrops` derives them from actual classified changes. A consumed position
covered by a later snapshot cannot redeem a previously excluded source.
`snapshot_drop_stays_dropped` and `snapshot_loss_preserves_cause` prove that
retaining this evidence preserves the drop and its original cause. This is
per-change evidence, not an adoption marker or a list searched by a frontier.
`captured_loss_original` traces every captured verdict to its original change;
`capture_loss_lookup` preserves its exact cause. `captured_losses_converge`
proves that receipt order and duplicate receipt identities cannot change any
lookup result in the captured evidence.

## What is proved

**Devices agree on whole-write and per-change decisions.** `verdict_agreement`
compares judgments with the same checked history and selected boundaries.
`every_write_judged` constructs an answer when prerequisites are available and
not refused. `changes_converge` includes the complete original changes and
their frozen causes, for any receipt lists containing the same writes.
`applied_has_no_dropped_source` checks that every applied change's data sources
survived. An excluded old-version write freezes its own changes; an eligible
write freezes only its dependent changes. Existing frozen causes and original
values are preserved.

**Rows and losses converge together.** `mergeInput` in `RowMerge.lean` feeds precisely
`classifyChange`'s applied changes to the existing row merge. `change_applied_iff`
and `mergeInput_effective` connect that classifier to the shared data rule.
`mergeInput_valid` derives generation validity after filtering from valid
original writes and authentic author provenance. It needs no retained-witness
hypothesis. `rows_and_losses_converge` proves equality of all merge fields and
the original-change loss records for causal receipt orders of the same set,
including replay after a correct snapshot with closed consumed coverage.

`row_value_accounted` quantifies over original setters. Each has one of the
ordinary merge outcomes (shown, knowingly replaced, lost cell, removed-row
loss) or its entire original change is frozen. `change_value_accounted` also
covers deletes without scalars and direct schema-excluded changes. Reset-ignored
parts remain the explicit no-loss exception; permanent refusals block receipt.

**Conversion agrees with receipt per change.** `receive` first determines
candidate schema admission without changing values. It follows the original
data sources to determine losses, then invokes the converter only on surviving
changes in eligible received records. `dependent_change_not_converted` preserves
original loss values; `independent_change_converted` converts unrelated changes
in that same write. `convertChange_metadata` preserves row identity, generation,
operation and source identities. Missing converters mark the interpretation
lost without changing its body (`missing_converter_preserves_values`).
Covered records, refused records, reset-ignored records, migration markers and
records for a different selected conversion never invoke the callback.

`conversion_retention_agrees` proves that recording those losses cannot alter
any retention decision, including a chain through a partially lost write.
`converted_change_matches_receiver` proves each converted/frozen row gets the
same verdict as the data decision preceding conversion. `receive_matches_evaluation`
connects the received record's admission to the shared classifier.
`receive_agreement` and `receive_losses_converge` compare independent receipt
certificates from the same original history and selected view, without assuming
a preconverted history. Read prerequisites gate readiness; their verdicts are
never passed to the converter. `every_write_received` constructs a certificate
when the checked history has no permanent refusals. `converted_verdicts_converge`
connects these results to the classifier's agreement theorem.

`rebuild` installs interpreted data while retaining the upload history.
Across successive migrations, `conversions_preserve_uploads`,
`conversions_preserve_attempt` and `conversions_preserve_untried` preserve every
upload record, including its schema, body, disposition and attempt choices.
`reload_matches_receiver` checks the author's interpretation against its peer's,
independently of local attempt status. `conversions_preserve_frozen` fixes every
already-frozen change, including its original values, generation and cause.
`lost_never_reconverted` prevents a later callback from reviving a previously
excluded interpretation.

**Old writes upload before the raise.** `RaiseUpload.migrate` appends the
migration's empty log marker after the existing queue, preserving all earlier
records. `post_migration_behind_marker` puts subsequent app writes behind it.
The snapshot captures through that marker. It does not include later app writes.
The marker carries no row changes; those travel in the snapshot alone.

The gate `Uploadable kept publication through` is Rust's: `publication` is
the version of the latest local breaking migration (zero for none), and
`through` the newest own write a current capture covers (zero for none). A
marker always sends; a record older than `publication`, or numbered within a
capture, sends while the store is older, since the latest capture covers it;
a later record at `publication` waits for a kept raise to it. `old_schema_head_enabled` proves that an updated app
can first-send the old head while the kept store version is still old
(`old_uploadable`). `old_prefix_and_marker_enabled` extends that permission
through the migration marker. The marker must send before the kept raise:
otherwise its own snapshot position would recreate a dependency cycle.
`new_schema_send_requires_kept` applies the actual send guard to ordinary
app writes at the latest migration's version, including retries. The
supplied store version must come from a kept entry, not a merely published
entry.

**Successive unpublished migrations (proved).**
[RaiseCaptures](queue/CovenQueue/RaiseCaptures.lean) adds the device's
local state beside its queue: the app's version, the latest migration's
version and the number of its marker. App writes take the app's version; a
migration to a newer version appends its marker and replaces the capture
with one through it, as Rust's `raise_capture::capture` does; every other
queue event reserves no number. A write made after one marker and before the
next is newer than the first marker's version, and no raise to that version
will ever be kept. Comparing it with the kept version alone, as the gate
did before T187, holds it, and with it the later raise, forever. In every
such history `local_step_keeps_records` keeps the queued records byte for
byte (events append or drop only the confirmed front), every record through
the latest marker passes the gate, and `later_capture_never_deadlocks`
constructs the confirmed prefix and the published latest raise.
`after_latest_marker_waits` keeps app writes after the latest marker behind
a kept raise to its version. `between_write_joins_later_prefix` and
`between_write_raise_publishes` check Ana's case: write 1, marker 2, write 3,
marker 4, write 5 with the store at 1; records 1 to 4 upload and the raise
to 3 publishes, and 5 waits.

**A capture taken again joins later writes (proved).** A raise snapshot is
prepared only right after a completed catch-up (§15). When that catch-up
applied something the capture lacks, Rust's `raise_capture::prepare` captures
the audience again through the device's newest write, with no new marker.
`LocalStep.recapture` moves the captured cut to that write;
`recapture_admits_prefix` shows it changes no record and lets every queued
own write through it pass the gate, and `later_capture_never_deadlocks`
covers histories with recaptures too. `recaptured_write_joins_prefix` and
`recaptured_raise_publishes` check Ana's case: write 1, marker 2, write 3,
recapture through 3, write 4 with the store at 1; records 1 to 3 upload, the
raise to 2 publishes, and 4 waits.

**A migrated device judges received writes as its peers will (proved).**
Before any raise to its version is kept, a migrated device judges a received
older-version write under its own captures, as though each capture's raise
were kept ([RaiseProvisional](queue/CovenQueue/RaiseProvisional.lean); Rust
`WriteBoundary::judging`). `provisional_record_agrees` and
`provisional_change_agrees` show the interpreted record and every per-change
verdict, losses included, depend on the raise's coverage and version, not on
the entry naming it; only the whole-write verdict carries the entry as a
label (`provisional_verdict_agrees`). In the `Judging` history, writes are
judged under the current capture and a capture taken again includes every
judged write (it is taken from the current state). `judged_under_published`
shows every judged write is covered by the current capture or was judged
under it, so `provisional_matches_kept`: each judged write the published
raise snapshot leaves out has exactly the per-change verdicts peers compute
once that raise is kept. Writes it covers reach peers inside the snapshot,
whose contents are the device's own state (its fingerprint check, §15: an
implementation obligation, not modeled here). If another raise wins,
`reload_matches_receiver` rebuilds the device's state from its unchanged
records.

`prefix_uploads` constructs a finite schedule of the actual queue's preparation,
send, provider landing, reply and acknowledgement events. `no_raise_deadlock`
then publishes the snapshot and its entry with an arbitrary new-schema tail
still queued. These are existence proofs of an enabled successful schedule,
not a claim that the provider or scheduler must cooperate. Their prerequisites
are usable keys, an updated app, a usable identity, and absent or equal-byte
paths. An occupied path with different bytes remains a stale-copy failure.
The ordinary fair-run settlement theorem still states the environmental
conditions for eventual progress.

`publication_after_confirmation` retains the confirmation at the snapshot
request through every queue event and crash. `published_raise_covers_published`
recovers stored original bytes for every covered own number, and retained
publication evidence for every remote number. `entry_has_snapshot` places the
fixed snapshot before the fixed entry. The IO package supplies the induction
from remote snapshot positions to published write numbers. Capture may precede
confirmation; snapshot upload may not. No empty-queue requirement is used.

**A competing raise uses the same receive rule.** If another device wins before
Ana's old writes land, Ana still sends their original records. After loading
the winning snapshot, all devices convert eligible late ordinary writes with
the same supplied callback. Without a callback they pass the number and retain the original values as
losses. With a callback, only changes whose generation or replaced-setter
sources were dropped become losses; unrelated changes still convert.
Reset-ignored parts
and losing migration markers produce no schema loss. The local derived values
of Ana's losing migration are replaced, not promoted to shared losses.
`competing_raise_converges` constructs a legal evaluation for Ana while leaving
her upload history unchanged; `receive_losses_converge` compares it to peers.
`concurrent_raise_old_write` checks both callback outcomes after the other
raise is kept, while the upload record stays in its old schema.
This agrees on interpreted records, verdicts and losses. `rows_and_losses_converge`
then applies to their row merge under its authored-data and generation premises;
store-log winner selection remains outside this package.

**No write is applied twice in the resulting audience.**
`nothing_applied_twice` covers arbitrary receipt lists, including duplicates.
Snapshot identities start passed; `covered_not_applied_again` checks that a
later original upload cannot apply a second copy. `consume_idempotent` checks
repeated delivery. A reload replaces the materialized state atomically. This
claim counts contributions to that state, not how often rebuilding computes
them or how many subscription callbacks a device emits.

**Commit and upload order hold.** `reachable_invariants` proves contiguous
queue numbers and stored bytes for every acknowledged number, by induction
over every queue event. `uploads_in_order` then proves a send cannot pass an
earlier unstored write. `conversion_preserves_identity` covers all conversions.
`Run.attempt_persists` proves that, until its number settles, a tried head keeps
the same complete attempt across any sequence of commits, sends,
late landings, failures and crashes. `Run.retries_identical` applies any deterministic sealer
to those fixed choices. `confirmation_requires_equal` checks settlement;
`occupied_never_replaced` checks create-only storage.

**Queued writes eventually settle under explicit assumptions.**
`every_queued_write_settles` applies to an infinite run, even while new commits
extend its tail. Its `Fair` assumptions apply only after a supplied time,
which can be after the last crash. Writes committed before that time still
settle. Safety holds even with infinitely many crashes; progress requires:

- The installation remains usable and syncing. Required updates, published
  raises, reloads and keys eventually let each waiting head become tried.
- Storage eventually accepts a retried head's original bytes.
- Once those bytes are stored, confirmation and the local acknowledgement
  transaction eventually succeed.

These are theorem parameters, not declared axioms. Settlement means removal
from the upload queue after storage confirmation. It does not mean an excluded
or reset-ignored write changes any rows, or that every peer has downloaded it.

**Operations resume with their recorded data.**
`Operations.fixed_data` and `publication_retries_identical` prove that crashes
and retries preserve the key, hash, paths and bytes. `rotation_entry_after_copies`
proves every required copy is stored before the entry can appear, even before
the publisher receives its reply. `visible_rotation_has_copies` reads the
entry independently of the publisher's journal and recovers the fixed
audience, key id and hash, with all prerequisite copies already stored.

`snapshot_stored_checked` proves that any published snapshot has the recorded
bytes and opens to the captured state under its recorded key.
`failed_snapshot_check_blocks_publication` rules out uploading a snapshot
that fails that local check. `snapshot_crash_needs_new_verification` proves
that losing the final verification reply requires another check before the
row can be deleted. Verification uses §15's complete-byte comparison option;
no checksum collision assumption is needed.

**A fair publication attempt finishes exactly once.**
`rotation_resumes_and_finishes` and `snapshot_resumes_and_finishes` prove
that the row is eventually deleted once, retaining the fixed local artifact
and all fixed published bytes. `finishes_at_most_once` needs no fairness.
`Operations.Fair` applies only after a supplied time, which may follow the
last crash. It requires successful local persistence/checks and journal
transactions, storage acceptance, confirmation/checkpoint transactions, and
final verification or kept-entry completion. These are explicit assumptions
about each remaining step, not an assumption that the whole operation
finishes. The proof advances through the finite recorded sequence.

For rotation this is one publication attempt whose entry can be kept.
`finished_rotation_requires_kept` makes that condition explicit. §18's replay
restart replaces the attempt and can block on fresh validation; it is not a
crash retry of the same entry. The checked restart below is why the theorem
does not claim one fixed key across arbitrary replay restarts. Exactly once
means one completion transaction, not one network request or one callback.

**A received write is never half visible.**
`AtomicReceive.never_half_visible` covers a receiver executing individual
row changes inside an uncommitted transaction, with crashes between any two.
Its visible rows and consumed position are always either the old database
or the entire new database. A crash discards the transaction; commit exposes
all its changes and position together. Repeated receipt cannot begin an
already consumed write. The checked two-row history crashes after changing
only its first row inside the transaction, then verifies whole-write commit.

**A converted edit cannot revive a migration-retired row.**
`migrateHiddenRows` creates ordinary deletes and captures whole frozen rows
as one migration result. `hidden_row_retired_and_frozen` proves the delete
advances a live row to its next even generation, keeps its generation record,
and captures all supplied values with their original setters.
`retired_edit_loses` calls the existing merge model's `step`. For any row
generation, the migration's ordinary delete advances it and retains the
generation record. The converted edit at the old generation leaves the row
deleted and records its value as lost to that deletion.
`retired_edit_cannot_touch_readded` also covers a subsequent re-add.
`migrated_hidden_row_rejects_late_edit` connects that result to the deletes
produced by `migrateHiddenRows`.
The concrete `RetiredRow` history has an insert, the migration delete and a
converted concurrent edit. Its metadata satisfies `CovenMerge.Valid`.
This is the retired-row history called counterexample 5: it no longer revives
the row or lacks a generation to lose to.

Rows hidden only by a non-final circle deletion are not retired.
`provisional_row_preserved` checks that their values and merge records stay
unchanged and no frozen loss is created. The inputs to that choice
are the removal causes after leaving out provisional circle effects; computing
those causes, including descendants, belongs to the merge model.
Frozen rows retain old column names, scalar values and original setters,
without live parent references.

**First attempts use eligible keys.** `first_key_usable` and
`first_key_excludes_known_recipient` prove that a current audience member's
selection cannot use a key known to have reached an excluded member.
`retired_never_selected` and `exposure_persists` keep that knowledge through
replay changes. `first_key_never_delivered` states the stronger physical
delivery claim with its needed premise: this device knows every delivery of
the candidate keys to that removed member. Encryption itself is not proved.

**Reload and stale-copy gates hold.** New writes into a reloading audience
return `AudienceReloading`; this is §3's exception to write availability,
not a wait inside the write call. A failed reload leaves it unchanged. The queue's
commit event requires `AudiencesReady` for the affected audience list;
`reloading_blocks_commit` rules out a reloading audience in that list. Only a
successful replacement makes the target boundary available to new writes.
Every send in the queue's `Step` requires a successful identity check.
`ready_owns_counters` proves that custody agrees, the device is not replaced,
and storage has not passed any local reservation. Observed counters include
authenticated snapshot and posted positions, so deleted log objects can still
prove a stale copy. A failed read leaves work pending; it is not evidence of empty storage.

## Availability and commit refusals

[§3](../coven.md#3-guarantees) promises that commit needs no network, while
allowing a write to fail local checks. After a reset, a schema raise requiring
reload, or skipped-history recovery, a write touching the reloading audience
returns `AudienceReloading` until atomic replacement succeeds (§14.4, §15,
§19.3). This is an availability exception, not a suspended call.
[`reloading_blocks_commit` and `reloading_refused`](queue/CovenQueue/Verdicts.lean)
prove the gate and its refusal result; `failed_reload_unchanged` preserves it
when loading fails. Other audiences remain writable.

[`Step.commit`](queue/CovenQueue/Upload.lean) requires only a staged transaction
and `AudiencesReady` for the supplied affected audiences. It has no network,
key, upload-progress or provider-success precondition. This is how the model
defines commit, not a separate proved availability theorem; neither prompt
return by Rust nor absence of network waits in its implementation is proved.
`commit_valid` proves queue numbering, not availability. Membership, deletion,
references, schema/SQL rules, file validation, timestamp/size limits and local
persistence errors are the [other local refusal boundaries](../coven.md#3-guarantees);
this model does not implement their full commit checks. Schema publication and
key readiness gate attempts, not `Step.commit`.

## Checked histories and limits

`DataExamples` checks both forms of an excluded received record: one creates
the missing generation, the other replaces a cell in a surviving generation.
Its exclusion comes from a missing converter, with original upload bytes
unchanged. A later callback cannot redeem that lost interpretation.
The next late write changes two rows. Its retained row converts and applies;
its dependent row freezes unchanged, with the exact failed sources. A later
write based on those two rows keeps the same distinction. A dependent delete
with no scalars still gets a recorded loss. The implicit own-device read does
not exclude the write. A second raise covers the excluded source's position
but preserves its original schema cause and both outcomes in the mixed write.
`Examples` also checks independent own successors and
two successive raises; pre-reset writes and a later post-reset write;
snapshot-covered duplicate delivery; failed reload; stale custody, counters
and replacement; an upload that seems to fail and lands later; failed and
unequal comparison reads; and a locally captured write whose storage slot is
still empty. `RaiseExamples` retains the upload-before-raise histories:
`raise_publishes_with_new_tail` confirms old write 1 and empty marker 2 before
publishing, leaving new-schema write 3 queued. Ben's 9 and 10 lose without a
callback and convert with one, independently of upload attempts; his
post-adoption 11 applies. `DataExamples` supplies the generation/setter-dependent
case, including an unrelated row that still converts in the same write.
The late-landing history is also proved reachable by the queue events.
`crash_late_accept_and_lost_reply` checks acceptance after a crash and a
second crash between reply receipt and acknowledgement. `crash_retry_settles`
checks the subsequent confirmation. Another example distinguishes a draft
lost before commit from a committed write retained after it.

**Storage acceptance alone fails.** `storage_acceptance_alone_insufficient`
has one untried queued write that never reaches a first attempt. Every retried write
is eventually accepted because there are no retries, yet the queue never
settles. A Lean `example` proves that no time in this run settles its first
number. This is why readiness and confirmation are separate assumptions.

**The unrestricted removal-key claim fails.**
`departed_member_counterexample` has two circle members who received key K.
Ben queues an edit, then learns Ana removed him. §6 and §14.6 permit his first
attempt with K, which Ben already received. The Lean `example` witnesses a
selected key and an excluded member who received it. This exception exposes
no new circle data to Ben; it still contradicts the unrestricted claim.
The adjacent remaining-member example selects the replacement key instead.

**A fixed key across replay restarts fails.**
`Operations.Examples.restart_changes_key` has copies for two members and
their rotation entry stored, followed by a supplied replay drop. One recipient
is now excluded. `restart_still_needs_key` calls the queue's key selector and
proves the old key is ineligible even if its introduction remains authorized;
so there is still a reason to rotate. §18 restarts
at the first step, choosing a fresh key and a new entry number.
The old copies and entry remain in storage. A Lean `example` includes both the
reachable publication history and this restart, and proves that the journal's
data changes. Crash retries within either attempt still preserve its bytes.
`publication_without_kept_entry` separately checks that accepting every object
does not make a dropped entry complete the operation. §3's finish wording
needs the successful prerequisites described by §18 and the fairness premises;
it does not guarantee recovery from permanent rejection without app action.

**Invisibility until publisher completion fails literally.**
`copies_visible_before_finish` witnesses §18.2's stored copy before its entry.
`snapshot_visible_before_finish` has one locally checked snapshot stored while
its publisher still has an unfinished journal row. Another device can open
that complete snapshot before the publisher commits its verification result.
Both have reachable Lean `example` histories. Thus §18's wording about never
seeing a half-done operation cannot mean hiding all physical objects until
row deletion. The proved reading is that introductions follow their copies,
complete snapshots pass their reader checks, and writes apply atomically.

`OperationExamples` also checks crashes before a local artifact's persistence,
between that persistence and the journal update, after sending a copy but
before storage accepts it, after copy confirmation, after entry storage,
and after snapshot verification but before its completion transaction.

## Readings and boundaries of the proof

**§3 coverage:** this model proves crash preservation and fixed retries, eventual
queue settlement and completion of one recorded publication attempt under
fairness, verdict agreement, and receiver atomicity at an assumed transaction
boundary. Authentication, sealing and opening are supplied inputs; interpreting
its eligible-key results as secrecy requires correct cryptography. It proves
neither confidentiality nor forgery resistance. Atomic, durable SQLite
transactions, durable file/custody writes and §2's provider behavior are assumed,
with eventual provider success stated separately in the progress hypotheses.

- Operation proofs concern one writer and one recorded publication attempt.
  Membership, authorization, reserved paths, valid sealed-copy construction,
  retained keys and replay verdicts are supplied inputs. The storage state
  includes only this attempt's reserved paths. An input assumption says that
  encoding and reading an entry preserve its audience, key id and hash;
  the cryptography is not proved. The snapshot reader includes signature, parsing and captured
  state checks as a supplied function. Replay replacement is modeled for the
  checked restart; stabilization of repeated replacements is not proved here.
- A successful local persistence event means the file or custody value is
  durable before its journal checkpoint. The model does not implement file
  flushing, keychain calls, SQLite transactions, operation-id allocation,
  multipart providers, retention or Rust execution. Crash safety relies on
  those persistence boundaries; a damaged database or lost identity is outside.
- Receiver atomicity starts with a completely checked write whose
  prerequisites are available.
  It models row changes and their position, not SQL execution or object parsing.
  It does not claim storage hides an unfinished publication's complete objects.
- Verdict agreement is per audience. One write may be ignored in a reset
  circle and applied in the store; an example checks both answers.
- A valid boundary supplies consumed coverage and separate per-change
  verdicts. Local captured coverage does not establish storage acceptance.
  Publishing the raise requires confirmation of its covered own prefix and
  publication evidence for every remote position. Only confirmation of the
  original bytes settles an upload slot.
- Reset ignoring takes precedence over schema loss in that audience. Covered
  history is supplied by the snapshot. Consumed positions remain separate
  from retained data sources. An otherwise eligible change built on an ignored
  source becomes a data loss; reading its position alone has no effect.
- Conversion functions and removal-cause calculation are inputs. The model
  proves which row changes may be converted and that identity, provenance,
  generations, frozen originals and every upload record survive. It does not
  execute
  arbitrary application migration SQL or validate its output. Successful
  callbacks change values only in this model; renaming columns and retaining
  parent-reference generations remain implementation obligations.
- The checked affected-audience list and current sync-pass key and identity
  evidence are inputs to queue events. Extracting those facts from row bytes,
  custody and storage is outside this package. Marking the first attempt fixes
  its choices before the request is sent, as §6 requires.
- History indices are a fixed chronological enumeration, separate from wire
  device/number identities. Columns are positions in the abstract scalar list;
  rows stand for table, key and audience. `DataValid` checks unique row changes,
  earlier sources and source presence. Row-merge theorems additionally require
  the original `Valid` and `Authored` contracts from the merge package, with
  exactly the queue's provenance. Extracting complete, authentic generation
  and replaced-setter sources from author state is not proved here. Existing
  read frontiers, scalar equality and canonical generation winners cannot
  supply that missing evidence. No premise assumes dropped sources survived.
- Agreement assumes the same checked history, selected boundaries and retained
  snapshot drop evidence.
  Store-log replay, boundary selection, finality, retention, full multi-audience
  snapshot alignment, cryptography, byte parsing, SQLite and Rust behavior
  are outside this package. It retains all inputs and deletes no stored objects.
  Database transitions model atomic commits: a crash before one preserves
  its input, and a crash after one preserves its entire result. Database
  corruption and loss of device custody are outside this crash model.
- Permanent refusal stops that log and its dependents. Unknown prerequisites
  wait. The total-verdict theorem applies when the required history is
  available and not stopped by a permanent refusal; progress does not promise
  app updates, missing keys or reader activity without an assumption.
- Stale-copy detection is checked before sends. The replacement installation
  and deliberate discard of unsent work are outside the progress theorem.
  Two live copies racing after a successful check remain the race §10 names.

## Spec edit checklist and Rust obligations

No normative spec, API, format or Rust file is changed by this proposal.

- **§10:** keep each retired device write's own inclusive publication window
  and the hard post-retirement-read refusal. Drop an outside-window write
  without rescue by later readers. Record per-change data losses in otherwise
  eligible dependents. State finality with the retiring entry and send gate;
  [merge B17](merge.md#b17-receive-order-progress-and-dropping-by-data) proves it.
- **§17.1:** remove inherited exclusion, the whole queued-tail rule and the
  redefinition of recorded reads as applied inputs. Preserve every upload in
  its original schema, including untried records. Qualify exclusion of an
  uncovered old-version write by receive conversion: convert eligible changes,
  freeze only changes with dropped generation/setter sources. Without a callback,
  exclude the interpretation with its original values. Rewrite Ben's 9/10
  example with both a dependent row and an unrelated retained row. Keep
  reset precedence and atomic reload; preserve source verdicts separately
  from dismissible losses. No callback changes log bytes.
- **§15/§17.1:** distinguish capture from publication. Capture the old prefix
  plus its empty migration marker; confirm every covered own upload before
  uploading the snapshot, then publish the raise entry. Remote positions need
  retained publication evidence from checked logs or valid snapshots. Apply
  that publication gate to ordinary snapshots too; the IO write-up proves the
  resulting loader guarantee. Remove permission to publish unuploaded writes.
- **§6/§18.1/§19.2:** replace untried-queue conversion with receive-side
  interpretation of late records, independent of attempt status. Explicitly
  permit the empty migration marker to upload before the raise; its row effects
  travel only in the snapshot. Ordinary new-schema writes remain behind it
  and wait for a kept raise, whether this device's or a competing device's,
  unless a later local migration's capture covers them.
  Do not require an empty queue before publication.
- **E13:** change conversion eligibility and the `Migration::writes` callback
  wording from eligible writes to eligible changes. Specify that one received
  interpretation can
  contain converted changes and frozen original changes; a callback never
  receives a dependent change. Preserve write identity, row generation,
  operation, provenance, old names/values and parent-reference identity as
  appropriate to each outcome. A failed callback aborts the transaction.
- **D5:** remove header lost-disposition inheritance through `had_read`.
  Specify authentic generation-source and replaced-setter evidence and a
  per-change frozen representation alongside applying changes in received data
  and snapshots. Whole-write lost disposition records a direct no-converter
  outcome in the interpretation; the original log record is never rewritten.
  A frozen delete must retain its original write/row identity even without scalars.
  Decide the concrete encoding; current D5 has no old-setter ids. D7, snapshot
  fingerprints and the loss API must preserve the corresponding evidence and
  original dependency cause. §3's open retirement question must be replaced
  with the proved original-value accounting rule.

For the queue implementation, capture and validate sources at authoring and
retain them through snapshots, conversion and replay. Use a shared data
classifier for waiting and incoming changes, with retirement/schema/theft
supplying only their own base admission policies. Keep consumed read positions
for readiness, never for deciding data loss. Run callbacks only after resolving
source changes, and preserve all upload records byte for byte. Atomically commit
received interpretations, frozen originals, merged rows, source verdicts and
consumed positions; failure must leave the prior state available to its initiator.

Exercise generation loss, setter-only loss, unrelated peer reads, implicit own
reads, mixed changes, chains through mixed writes, no-converter cases, repeated
raises, empty-valued deletes, snapshot reload, loss dismissal, replay changes
and both receive orders. Compare the new classifier with the proved row merge,
not with a reconstruction in tests. The existing Rust differential deliberately
keeps its prior boundary path; it does not check this proposed data policy.

The empty-marker exception makes upload before the raise possible. Conversion
is relative to the winning raise, so its original author also receives a late
write after a competing raise. These readings preserve the same interpretation
on every device without converting the raising device's own upload queue.

`CovenQueue/Axioms.lean` audits the main results. `scripts/check.sh` rebuilds
the package, rejects unfinished proofs and foreign axioms, and runs the
Rust/Lean differential tests below.

**Rust comparison.** `QueueRunner.lean` reads checked records in timestamp
order, decides `DataValid` with `Checked.lean`'s finite check, and returns
`classifyChange` for every received change together with `mergeRows` from the
snapshot's covered records. Each change sets the first `values` columns, as
`rowMetadata` states, so the Rust generator's updates set `a` or `a` and `b`.
`queue_reset_differential_histories` in `coven-database` generates 64 causal
histories with several writes per device, implicit own reads, a reset snapshot
covering the first writes, and later authors who did or did not read the reset;
some that read it have not yet reloaded and build on ignored data. Each history
runs twice, once with the uncovered writes inside the real reset reload
(`load_snapshots`) and once received afterwards (`receive`), in sampled causal
orders. It compares every change's retained verdict (`_coven_authored_changes`),
the dependency causes and sources of the frozen losses, generations, generation
setters, cell setters and ordinary cell losses, and requires every position to
be consumed. Snapshot evidence, conversion and replay rebuilds have their own
families; scalar values, encryption and store-log selection are inputs.

Given a selected raise and its `Conversion`, the runner first interprets the
history with `receivedHistory` and `receivedValid` (`receive`, `rebuild`),
then reports `classifyChange` and `mergeRows` on that interpretation.
`queue_conversion_differential_histories` generates 48 raise histories, each
with and without a converter: a receiver that applied the covered prefix
before its app migrated holds a kept raise to version 2 covering it, then
receives late old-schema writes and new-schema writes. Authors who migrated
may still hold their own late old-schema data, so changes built on excluded
writes occur. Both schema-excluded and dependency losses must occur, and the
same verdicts, frozen causes and rows are compared
(`converted_change_matches_receiver`, `rows_and_losses_converge`). The callback
is the identity here; renamed columns and references are Rust-tested.

`Retired.lean` composes a write's own retirement window with this classifier,
as merge's `Receive.base` composes `withinWindow` with the boundary verdict:
`windowChange` drops every change of a late write with no loss of its own,
and `windowLoss` judges sources under `windowState`, so otherwise eligible
changes using a late write become dependency losses. `windowInput_effective`
identifies the merge input with B17's `effective` history,
`window_rows_and_losses_converge` gives equal rows and verdicts for equal
consumed sets in any causal order, and `window_none_classify` recovers
`classifyChange` when nothing is late. Given kept retirements and each
write's storage time, the runner decides lateness with `Receive.withinWindow`
and reports `windowResults` and `windowRows`.
`queue_retirement_differential_histories` generates 48 histories in which
devices 104 and 105 are retired by entries stored at 0 and 5 days and publish
writes one second either side of their thirty-day deadlines. Authors either
already rebuilt without late writes or still hold them, so chains of changes
built on late writes occur. The real receiver applies the writes at their
publication times while replay keeps and drops the two entries between
arrivals, rebuilding in each replay transaction, then settles on the
history's final fates. Each history runs in two arrival orders; it compares
every change's retained verdict, including `dropped`, the frozen dependency
losses and the merge rows with the model under the final kept retirements,
requires every position consumed and no audience left waiting for a reload,
and requires the two orders to agree (`receive_runs_agree`,
`replay_restores_by_own_window`). Retirements of covered writes, snapshot
contributions and finality are Rust-tested.

Run them with:

```sh
COVEN_QUEUE_LEAN="$PWD/spec/proofs/queue/.lake/build/bin/queueRunner" \
  cargo test -p coven-database queue_ -- --ignored --nocapture
```

The runner's raise mode drives one device's queue through app writes,
breaking migrations, upload passes, raise publication and crashes, using
`Queue.commit`, `RaiseUpload.migrate`, `Queue.prepare`, `World.send`,
`World.land` and `Queue.finish` under the `Uploadable` gate; the raise
advances only when `RaiseUpload.Confirmed` holds. A pass's injected fault
loses the first request before it lands, or the reply of the first request
that stores new bytes, as the provider's lost completion does; an occupied
path settles by equal bytes (`confirmation_requires_equal`).
`queue_raise_differential_histories` in `coven-sync` runs 48 generated
histories on a real device with memory storage and compares, after every
event, the settled count, each waiting record's number, tried flag, schema
version and kind, the raise stage and the kept version. It also checks that a
migration leaves every queued record's bytes unchanged. Before any raise the
model's kept version is the version the store's writes were made with.

The gate is Rust's (`device_log_sync::uploadable`): a marker always sends;
with no local breaking migration everything sends; otherwise a record older
than the latest local migration's version sends, and one at that version
waits for a kept raise to it. A later migration replaces the captured plan.
The generator lets a migration follow app writes made after an earlier,
still unpublished migration, and the test asserts that some of its 48
histories do. Before each raise event the Rust device's clock moves past the
longest retry wait: the model's raise event is a pass at which the raise is
due, and a wait armed before a reopen would otherwise outlast the fixed
clock (§3.1's waiting, which the model does not time).

```sh
COVEN_QUEUE_LEAN="$PWD/spec/proofs/queue/.lake/build/bin/queueRunner" \
  cargo test -p coven-sync queue_raise_differential_histories -- --ignored --nocapture
```
