# Device upload queues and resumable operations

This package checks durability and atomicity in [§3](../coven.md#3-guarantees),
the queue rules in [§6](../coven.md#6-syncing-writes),
[§7](../coven.md#7-order), [§10](../coven.md#10-device-identity),
[§11](../coven.md#11-keys), [§15](../coven.md#15-snapshots),
[§17.1](../coven.md#171-host-application), [§18](../coven.md#18-operations),
and [§19.3](../coven.md#193-resetting-a-store).
The Lean source is in [queue/](queue/), using Lean 4.34.1 without Mathlib.

There are checked limits to the requested claims. Storage accepting retries
does not help a write that never reaches its first attempt. And §6 and §14.6
allow a departed circle member to first-send an old edit with the old key.
Both have Lean examples below. Further examples distinguish crash retries
from replay restarts, and visible storage objects from atomic application.

## The model

A committed record has a device, number, timestamp, recorded reads, schema
version, disposition and plaintext body. Commit assigns the next number.
Only the head can be attempted or acknowledged. The first attempt fixes the
format and all sealing key ids. Conversion can change an untried record's
body, version and disposition; it cannot change its identity or recorded reads.

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
The evaluator computes applied, excluded, reset-ignored or refused. Its
dependency list includes implicit earlier writes by the same device.

Adoption also retains which effects were discarded. `adoptionFrom` derives
that list from the earlier view's checked verdicts. It is separate from loss
values, so dismissing a loss cannot erase it. Later raises still respect those
earlier cuts in state inputs. Nothing new is added to the wire format:
§17.1 requires reconstructing these facts from the author's recorded store-log
past and retained boundary inputs.

## What is proved

**Devices agree on verdicts.** `verdict_agreement` compares two independent
evaluations through the dependency graph. Given the same checked records,
selected boundaries and retained adoption facts, they have the same answer for
each write they can judge. `every_write_judged` constructs an answer for every
write in an available history without permanent refusals. The proof covers
the author's waiting writes and incoming writes through the same classifier.
It does not assume devices receive the same information at the same instant.

`rebuild` converts waiting records in causal order, so a converted predecessor
can support its successor, while an excluded attempted predecessor cannot.
`reload_matches_receiver` proves that the author's result equals the receiving
device's evaluation of those resulting records. Successive migrations each
carry their own version's view; `conversions_preserve_attempt` keeps every
already-tried record unchanged through the whole sequence.

**Applied writes do not use discarded inputs.**
`no_applied_discarded_input` combines exclusion inheritance and the reset rule.
`no_applied_discarded_read` covers every recorded read in a causal history:
its effects were discarded at an earlier adoption, or it carries no excluded
or ignored input into the applied write.
Without adopting the responsible boundary, a write cannot apply after reading
an excluded input. After adoption, the passed number does not carry the old
value into new writes. `own_cause` checks that earlier own writes participate.
The reset proof requires the chosen snapshot's consumed history to be causally
closed, as §6 and §15 require. `retained_verdicts_justified` checks the earlier
verdicts used by `adoptionFrom`.

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
the same complete attempt across any sequence of commits, migrations, sends,
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

`Examples` checks an excluded attempted head and its converted successor;
an eligible conversion with retained peer inputs; post-adoption writes;
two successive raises; pre-reset writes and a later post-reset write;
snapshot-covered duplicate delivery; failed reload; stale custody, counters
and replacement; an upload that seems to fail and lands later; failed and
unequal comparison reads; and a covered write whose storage slot is still
empty. The late-landing history is also proved reachable by the queue events.
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
- A valid boundary's coverage determines represented effects; it never
  establishes storage acceptance. Only the original upload settles a slot.
- Reset ignoring takes precedence over schema loss in that audience. Covered
  history is supplied by the snapshot, and adopted discarded positions do not
  become state inputs again. Retained adoption facts are rebuilt from the
  author's earlier view, not guessed from the newest raise alone.
- Conversion functions and removal-cause calculation are inputs. The model
  proves which queue records may be converted and that identity and tried
  bytes survive; it does not execute arbitrary application migration SQL or
  validate its output. The generation theorem assumes conversion preserves
  the row's generation, as D5 and E13 require.
- The checked affected-audience list and current sync-pass key and identity
  evidence are inputs to queue events. Extracting those facts from row bytes,
  custody and storage is outside this package. Marking the first attempt fixes
  its choices before the request is sent, as §6 requires.
- Agreement assumes the same checked history and selected boundaries.
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

`CovenQueue/Axioms.lean` audits the main results. `scripts/check.sh` rebuilds
the package, rejects unfinished proofs and foreign axioms, and continues to
run the existing Rust/Lean differential tests. This package adds no Rust
changes and no new differential test.
