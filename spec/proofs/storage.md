# Storage identity, files and clocks

The Lean package in [`storage/`](storage/) models [§4](../coven.md#4-storage-providers-and-access),
[§6](../coven.md#6-syncing-writes), [§7.2](../coven.md#72-timestamps),
[§10](../coven.md#10-device-identity), [§16](../coven.md#16-files), and the
waiting-write rules in [§17](../coven.md#17-schema-changes).
The format boundaries are [D6, D8–D12](../format.md#d6-store-log-entries);
the app results follow [E5](../api.md#e5-storage-and-sync) and
[E8](../api.md#e8-files-and-the-cache).

Two claims fail without qualifications. Reset deliberately loses
unsent edits. Also, §10's detection checks do not establish that every restored
database gets a new device id. A backup can erase the record of an encryption
attempt without leaving any evidence in storage. Content-bound nonces separate
the resulting plaintexts without relying on that detection.

All named results are audited by `CovenStorage/Axioms.lean`. The package uses
Lean 4.34.1 and its standard library, without additional axioms or unfinished
proofs. `scripts/check.sh` builds it and runs the audit alongside the other
proofs. `clockRunner`, `createRunner`, `identityRunner` and `filesRunner`
expose the model to the Rust differential tests run by that script. The create-once comparison drives
the S3 adapter against the test server through every profile (AWS, GCS and R2),
and the OneDrive adapter through Graph. It checks occupied-path refusals and
stored bytes for single-request and multipart uploads.

## Restore, another live copy, and writes

**Unqualified no-loss fails.** `unsent_restore_loss` starts with one committed
data write, still queued. Restoring onto an installation without matching
custody triggers §10's reset. The old database becomes unavailable; the new
device loads storage, which does not contain that write. Its queue and applied
history do not contain the edit. The example also checks the reset reason.
One unsent write suffices; no competing write is needed.

`two_live_copies_race` checks the other permitted loss. Two copies reserve
write 1 under device 7, with different values. Both pass the identity check
before either publishes. Storage keeps the first value. Complete-byte comparison
resets the losing copy, discarding its queued value. §3 and §10 describe this
loss explicitly; it is not a claim that their qualified durability guarantee
promises otherwise. Both histories have literal Lean `example` declarations.

**Stored objects are preserved, and receipt does not apply an identity twice.**
`stored_history_preserved` proves that any sequence of create requests, including
requests with different bytes at an occupied path, preserves the first object's
bytes and storage time. `settled_iff_equal` proves that only complete equal bytes
settle an occupied upload. Read errors leave it pending. `retry_preserves_time`
checks repeated publication.

`receipt_no_duplicates` and `receipt_members` prove, for every delivery list,
that each delivered identity appears once and that none disappears. A loaded
snapshot supplies effects together with their identities. Reset replaces the
old applied state with loaded history; it does not add the snapshot's effects
to discarded rows. `reset_loads_once` proves the same result after a reset and
any subsequent deliveries, even repeated ones. `commit_reserves_next` checks
that committing a write reserves its next number with its applied identity.

The application model records atomic effects by identity. It does not reproduce
the row merge, schema exclusions or reset-selected row values. These theorems
do not promise eventual delivery, successful replacement registration, or
preservation of deliberately discarded local edits.

## Replacement judged by recorded reads

`ReplacementRead` proves the recorded-read condition in §10 and D6:
the add-device entry names the old id but carries no log ends.
`registration_authority` requires the same member and a distinct new id.
`accepts_iff` and `before_read_counts` check whether the object's recorded
past includes a kept replacement of its device. This is one admission
condition, not the complete landing verdict.
`replacement_order_irrelevant` covers concurrent replacements without
combining counters.

§10 also requires a write to land no more than 30 storage days after every
kept entry retiring its device. Retirement includes member removal, device
removal and replacement. Exactly 30 days is allowed; later writes are
excluded while that retirement is kept. Dropping it recomputes admission
from retained inputs. Entries instead keep §9's permanent rule against any
unread entry, kept or dropped; they never revive after a time-based drop.
These landing checks do not depend on the provider request-duration assumption.

The data rule is proved separately in
[the merge receive model](merge.md#b17-receive-order-progress-and-dropping-by-data).
An out-of-window write stays dropped. Each otherwise eligible change using
its generation or replaced cell setter becomes a recorded loss; unrelated
changes remain eligible. Recorded write frontiers do not decide dependency.
`retirement_verdict_final` proves window-verdict stability once the deciding
retirement and preceding entry prefix are final, under the post-read send gate.
Later readers cannot change it. `send_gate_publication_bound` proves the one-day
plus five-minute drain bound from finality. Both proofs state their store-log,
catch-up and provider assumptions explicitly. This proposal changes neither
`ReplacementRead` nor the normative spec files; the merge write-up lists the
required wording and Rust obligations.

**Stored bytes are preserved; receipt is conditional.** `stored_object_survives` combines
create-once storage with receipt: later publications preserve the original
bytes and time. Its receipt result assumes the other admission checks
already passed; it does not model the 30-day write deadline. There is no
upper write or entry number. `racing_upload_kept` checks an old copy whose
write 2 lands after a replacement that saw only write 1.
Ana's restored phone can therefore register a replacement while her old
phone's write 2 is in flight: both stored writes remain receivable, provided
the old phone made them before reading its replacement and each passes
its landing and ordinary admission checks.

**No duplicate application and agreement are proved.**
`no_duplicate_application` covers arbitrary deliveries, including repeats.
`receipt_after_restore` covers a loaded snapshot followed by repeated log
delivery. `devices_converge` gives the same applied identity set for the same
objects and kept replacements, whatever their order or repetition. Atomic
effects by identity and immutable stored bytes are the same abstraction used
above; this is not a second proof of the row merge or eventual delivery.

**Post-read objects are rejected.** `after_read_rejected` covers writes and
store-log entries: reading any kept replacement of the old id excludes the
object. `old_copy_stops` makes observation enter reset and prevents further
commits or sends from that copy. `observed_replacement_blocks_send` covers
every send kind, including a backup whose local read positions were erased.
`reset_uses_new_path` gives the resetting copy its own fresh id and write 1.
`restored_and_live_copies_stop` checks these transitions together.

**Unqualified no-loss still fails.** `ReplacementRead.Examples.unsent_loss`
checks one committed, unstored write discarded on reset. Two copies can also
pass the check and publish different bytes to write 1: the first value remains,
the other copy resets (`colliding_copies_lose_one_value`). Both have literal
Lean `example` witnesses. These are the same losses §10 explicitly permits;
the landing deadline separately decides whether a stored pre-read object's
effects count.

**“A restored copy never sends” fails.** In `restored_copy_can_send`, a backup
contains one queued write. Restoring on the installation that still holds its
custody id leaves no larger stored counter and no replacement to discover.
The check passes and the write is sent under the old id. The literal Lean
`example` needs one write and one send, with no earlier attempted write.
Missing custody, a larger observed counter, or an observed replacement blocks
the send. This supports the qualified wording: a restored copy that is behind
storage never sends. A same-installation backup with intact custody and
nothing newer in storage continues as that device; content-bound nonces
do not depend on detecting that restore.

The readings chosen are: “before it read” means the immutable recorded past
of the copy that made the object, not whether another copy has read it;
any kept replacement in that past excludes an old-id object; a write's
`store_log_read` and an entry's expanded `had_read` supply that past.
Publication after replacement can count when creation preceded that read
and the landing check passes. Agreement requires the same received objects,
their storage times and replayed retirements.
Restore includes replacing the database from a backup while custody survives;
it does not imply that the app's explicit reset path was invoked.

Authentication, honest recording of reads, membership authority, entry landing
rejection, the write landing deadline and which retirements replay keeps
remain inputs. The write deadline is a required verification obligation,
not proved by `ReplacementRead`: check removal and replacement, equality
at 30 days, a later landing, and reversal restoring only the write. Replaying a
changed replacement set rebuilds admission from retained objects. Files and
snapshots do not have these write/entry read fields, so their own acceptance,
physical cleanup, fresh-id generation and replacement registration liveness
are outside this variant. Their outgoing sends still use the shared gate.

## Identity checks and reset

`checkIdentity` models the identity evidence supplied to §10: non-backed-up
custody, observed counters, authenticated snapshot and posted coverage,
replacement and removal. Its `Evidence.listed` input represents complete
counter evidence; the model does not execute provider discovery.
`send_covers_all_evidence` proves that a successful check has no
observed counter beyond the local reservations, from any of those sources.
The examples include a deleted log whose snapshot or posted positions still
expose the rollback. A reserved upload already present in storage is allowed.

`sends_require_check`, `failed_scan_sends_nothing` and the identity gate's
no-send theorem cover writes, entries, snapshots, files, key copies
and positions. The caller supplies a completed observation or its failure,
never a successful partial scan. §6 and §15 obtain evidence through numbered
log reads, positions and conditional snapshot discovery. Establishing that
these requests supply the model's complete evidence, including after log
deletion, is an [IO verification obligation](io.md#discovery-obligations).
The model has no key-copy counter and does not prove that added check.
`IdentityRunner` executes `checkIdentity` and `ReplacementRead.observe`/`gate`
for the Rust differential tests of coven-sync's pre-send check; key-copy
counters stay equal on both sides of those cases. It also runs `beginReset`,
`retryReset`, `finishReset` and the first `commitWrite` against coven's
recorded reset, failed `reset_device` retries and the published replacement.
It executes `ReplacementRead.accepts` and `load` against coven-sync's write
authority and entry replay for old-id objects whose recorded pasts read any
subset of kept, concurrent and unauthorized replacements. Rust judges each object in its own
recorded view, as §7.1 judges removal; those cases keep the same replacements
in every view, so the model's single kept set applies to all of them.

`Identity` also contains a separate model with recorded replacement ends.
Its `ends_*`, `combined_end_keeps_completed` and `beyond_end_waits` results
concern that model, not §10's admission rule. The `ReplacementRead` results
above establish the recorded-read condition without those ends; neither
policy is inferred from the other one's theorems.

`reset_unavailable`, `reset_uses_fresh_id` and `reset_discards_queue` describe
the bootstrap boundary: retry keeps the chosen replacement; failure cannot
reopen the discarded database; successful loading starts with the fresh id.
`reset_first_write_new_path` proves that its first write is number 1 at a
different path from every old write. `registration_retry_number` gives a new
entry number after a too-late registration without changing the fresh device id.
Authority checks, §9's late-entry rejection and finality are inputs from
store-log replay. These reset results do not establish eventual successful
registration. `ReplacementRead.reset_uses_new_path` and
`receipt_after_restore` establish the fresh path and duplicate-free receipt
for the replacement rule without recorded ends.

## Nonces

Here “reuse” means encrypting different protected inputs under one key and
nonce. Retrying identical inputs is required by §6 and is harmless in this
model. A nonce is represented by its D11 inputs: key, path, cleartext
prefix, section, index and chunk plaintext. Plaintext stands for its SHA-256
digest in the same collision-free abstraction used for file hashes below.
Actual SHA-256, HMAC and truncation to 192 bits are outside the model. A finite
nonce cannot be literally injective over arbitrary plaintext; these theorems
prove separation of the inputs, not the absence of real cryptographic collisions.

**Nonce exclusivity is proved without a history restriction.**
`nonce_exclusivity` makes equal nonce contexts imply equal chunk plaintext and
cleartext prefix. `different_plaintext_separates_nonces` states the converse
separation. `attempts_nonce_exclusive` applies it to any pair of attempts,
including attempts erased from all surviving databases. There is no premise
about fresh paths, durable reservations, rollback, or the number of live copies.

Ana backs up her registered device before reserving write 1. She commits A
and encrypts it, but it never lands. She restores the database while its
non-backed-up custody survives, then commits B at write 1 with different bytes.
Every §10 check passes. `single_live_restore_nonce_separation` executes this
history through `BackupRun`: both attempts remain in its encryption history,
and their content-bound contexts differ. `two_live_copies_race` checks the same
separation when two copies are alive together; create-once storage and the
loser's deliberate reset still have their specified effects.

`rollback_conversion_changes_plaintext` checks that converting an apparently
untried backup also changes its nonce. `entry_chunks_and_prefixes_separated`
covers a store-log entry and changed authentication data. The general theorem
covers arbitrary chunk sections, indices and prefixes. Restore examples model
one affected chunk; they do not implement streaming or an entire migration.

**Identical retries are proved.** `retry_fixed` retains plaintext, key and
format across migration or changed choices. `retries_identical` applies any
deterministic encryption function to those same inputs and obtains the same
bytes. `fresh_path_separates_nonces` also retains the independent path separation.

**A content-free recipe fails.** `content_free_rollback_nonce_reuse` checks
Ana's same two-attempt history using only key, path, section and index as
nonce inputs, and proves that they agree for different plaintexts. Its literal
Lean `example` checks the witness; `fewer_than_two_attempts_no_reuse` proves
that one attempt cannot show it. This counterexample explains why D11 binds
content.

The detection limitation remains separate: non-backed-up custody can survive
database rollback, so the listed §10 checks cannot always detect a restore.
Known restoration routed through reset produces a new id. Nonce separation
under the content-bound recipe needs neither outcome.

## Files

**Status agreement is proved** by `status_agreement`: the same fixed reference,
storage observation, uploader state and authenticated report give the same
answer. Unrelated reports and local caches, pauses, timers and queues do not
participate. `presence_wins`, `read_error_not_missing`,
`absent_active_uploading` and `status_cases` check the priority and error cases.
The devices must have the same replayed uploader state. Equal file storage
alone cannot make a device that has not read a replacement know about it.

**Completion never changes a row.** `attachment_atomic` fixes the whole file
reference and queues it in the attaching transaction. `completion_changes_no_row`
proves that finishing any attempt changes neither rows nor the app-write count.
`completion_retires_both` removes its queue entry and pending record together;
`completion_idempotent` covers a repeated completion.
`older_upload_cannot_restore_photo` checks §16.1's example: photo B replaces A,
then A finishes uploading, leaving B in the row. `unequal_bytes_keep_queue`
prevents a collision from retiring that upload.
`finish_reports_result` preserves the typed error or reset request for the caller.
`FilesRunner` executes `attach` and `finish` for coven-sync's differential test,
which attaches app-provided files, retargets or deletes their rows while an
upload is in flight, and compares the settlements, the write count, each row's
fixed reference and the queue. Row deletion is an environment step in the runner.

**Retried file chunks are fixed.** `encrypted_chunk_is_captured` proves that a
successfully checked chunk has the captured plaintext, key, path, size and index.
`file_retries_fixed` proves that two accepted retries produce identical symbolic
encrypted chunks. `changed_source_not_encrypted` rejects a changed chunk even
if the source changed after an earlier whole-file check.

The queue's captured plaintext chunks are proof-only values standing for the
recorded hashes. The implementation need not retain their bytes. Hash equality
is modelled as content equality; SHA-256 collision resistance, filesystem reads,
whole-file and modification-time checks, multipart transport and actual encryption
are outside the model. So are random file-key generation, reference validation
against a row, cache eviction, and physical file deletion.

## Clocks

**Clock skew does not hold application or make storage ages early.**
`no_clock_hold` proves that a write or entry with available causes and keys
applies for every receiving wall clock, including a negative one and an incoming
maximum timestamp. `waiting_is_not_seen` keeps pending input out of the latest
timestamp. `snapshot_upper` proves that loading adopts all applied timestamps.

`age_never_early` and `finality_age_never_early` prove that an observed lower
bound on provider time cannot declare the corresponding age before the provider's
actual time. `wall_jump_changes_no_age_or_retry` makes wall-clock changes irrelevant
to storage age and the monotonic retry deadline. `retry_delay_bounded` proves
the five-minute ceiling within one run. These results assume §4's common,
nondecreasing provider clock; they do not establish it for a real provider.

§9 uses `T′ = T - M - δ` for the finality window, with one common
`M = one hour` and positive provider time unit δ. Underflow cannot advance
finality. This is still a lower bound for the age theorem. The
[IO listing proof](io/CovenIO/ListingFinality.lean) derives completeness
through T′ from §4's strict listing guarantee and completed exact reads;
a clock lower bound alone does not establish that receipt.

The persisted restart rule in §16.5 is outside these theorems: save deadline
T and the full wait W, then arm a monotonic delay of `max(0, min(T - now, W))`
on reopen. Ana restarting halfway through an eight-second wait keeps the
remaining delay. Moving her clock backward cannot make that restart delay
exceed eight seconds; moving it forward may shorten it. Provider cooldowns
retain their full W even above the ordinary five-minute cap.

**Successful stamps order every write after everything it read.**
`stamp_after_every_read` and `stamp_after_snapshot` prove this for arbitrary
read sets. `observe_upper` and `snapshot_upper` establish the remembered maximum.
`wall_ahead_uses_zero_counter` and `wall_behind_increments` prove the two stamping
branches. Incrementing the packed 48-bit milliseconds and 16-bit counter carries
overflow into the next millisecond. Device id breaks remaining ties.

`representable_stamp_exists` proves that stamping succeeds whenever the wall
time fits and a later packed timestamp exists. `clock_boundaries` checks the
counter carry, pre-epoch wall time, and both range errors. A clock beyond the
format's range fails a new write explicitly; the claim is no sync hold, not
unlimited future timestamps. S3 authentication's clock-skew error is likewise
outside the application-clock model.

## Readings and boundaries

**§3 coverage:** this model proves preservation of stored bytes, duplicate-free
receipt and fixed retries, and checks durability's explicit reset losses. Its
clock results supply causal stamp ordering; its nonce results separate symbolic
inputs, not real cipher outputs. Confidentiality and forgery resistance assume
correct ciphers, signatures, hashes and key derivation; authenticated objects
and sealing are inputs. Atomic SQLite transactions, durable custody and §2's
provider behavior are assumed. Eventual delivery and full data convergence
require the [other models' hypotheses](../coven.md#3-guarantees).

- A backup may restore the database on an installation whose custody survives.
  A known restore routed through new-device bootstrap is a different case.
- The no-loss claim is tested literally. The proved preservation claims cover
  published bytes and loaded/delivered identities, with §10's local loss exposed.
- File status compares the same reference and replayed device state as well
  as storage and signed reports. When both removal/replacement and a source
  report explain absence, the device state supplies the reason; E8 lists the
  reasons but does not choose between simultaneous explanations.
- “Past the latest timestamp” means a strictly later wall-clock millisecond.
  Negative wall time follows the increment branch. Timestamp storage has its
  specified bound; abstract device ids stand for distinct 64-bit ids.
- Storage-age checks use §9 and §15's current endpoints: entry drop strictly
  after 30 days, retention at 30 days, finality strictly older than 30 days,
  with both recent-window endpoints included. `storage_age_boundaries` and
  `finality_window_partition` check them. [C10](storelog.md#c10-finality-by-storage-time)
  uses a different boundary convention; this package does not import or extend
  that finality proof. Invite expiry in §12.2 remains the inviting device's
  separate clock rule, outside these storage-age comparisons.
- Fresh ids, key uniqueness, verified signatures, complete reads, atomic local
  transactions, snapshot contents, and eventual provider success are boundary
  conditions. Storage deletion and retention safety, full membership replay,
  row merge, and Rust correspondence are not proved here.

## Signed roots and row authorship

`RowAuthorship` is a proposal for the owner-table decision, without normative
spec or format edits. It models one audience part of a contributing write:
a nonempty tree of its original cell and operation leaves, verification of
that part's root in the signed header, and a snapshot multiproof. The decided header lists one
keyed Merkle root per part. `Hashing` is supplied for a fixed audience-key
context; `Signatures.verifies` abstracts header verification and selection of
that part's authenticated root. Each readable part instantiates the argument.
A leaf is domain-separated from a branch, and
left/right order is authenticated. A hidden subtree opens no row leaf; the
model makes no confidentiality claim about its hash. Tree shape and canonical
serialization are format obligations;
no concrete byte layout or cryptographic implementation is introduced.

**Authenticity is proved under two explicit assumptions.**
`Hashing.collisionFree` says equal abstract hashes have equal tagged inputs.
`Signatures.unforgeable` says verification for a claimed member, write identity
and root implies the member is that identity's admitted author and actually
signed a body with that root. `signed` records the actual signing history;
it is not a snapshot publisher's choice. The identity includes the store and
device context; replay supplies admitted member keys. An author can sign
several bodies for one identity. `signature_security_allows_two_bodies`
checks this case under the same signature-security contract. Selecting one
body per admitted write remains a merge-admission obligation; the authorship
proof does not assume that selection or that authors avoid signing conflicts.
These are parameters, not Lean axioms. Literal collision-free hashing is an
idealization, not a claim that a finite SHA-256 digest is mathematically
injective on all byte strings.

`revealed_authentic` proves every opened leaf belongs to the original tree,
including against altered tree shapes. `snapshot_authorship` combines that
result with signature verification. `owned_row_true_author` requires an
opened creation leaf for the claimed row identity, rather than trusting a
creator field or an opaque hash. It proves the claimed member is that
creating write's author. `cell_true_author` checks a surviving cell against
its original writer's opened cell leaf, binding the exact row, column, value,
generation and insert/update kind.
The caller must apply these checks to every owned row and required setter.
Selecting the canonical creation for competing insertions of one logical key
uses the merge admission rule described in B18; authenticating
one creation does not prove it was the first or winning creation.

**Withholding a removal preserves the later signed incarnation.** The merge
proposal in [B18](merge.md#b18-owner-tables-and-row-authenticity) keeps earlier
deletes as generation evidence when a later incarnation exists. Its trial
therefore retains the original insertion and update setters, without altering
their signed row, generation, kind or scalar. Actual rejection still excludes
changes using rejected evidence and records their losses. The storage model
and leaf format are unchanged: `snapshot_authorship`, `cell_true_author`,
`multiproof_complete` and `multiproof_minimal` apply to the same original
leaves. The original creation marker remains the owner evidence.

The merge proof preserves every present row's generation and winning cells
for every valid causal history. It checks Ben's parent deletion becoming
a loss while Ana's reinserted child remains visible, plus reference-update,
audience-transfer and third-row cascade histories. This does not imply final
owner visibility under every ordinary removal rule: B18 retains a checked
three-row, four-write set-null/unique counterexample. Authentication and proof
completeness do not prevent a valid merge rule from hiding an authentic row.
Historical generation and setter evidence needed for these trials must remain
available independently of dismissible losses; this package does not prove
snapshot retention supplies all of it.

**Each cell change is a separate leaf.** `Change` has distinct creation,
cell and deletion constructors. `insertLeaves` in `CellProof.lean` expands an
insert to one creation marker plus one leaf per written column/value pair.
Updates have only cell leaves;
deletes have one deletion marker. `leaves_per_change` proves those counts.
Thus a part has `L = C + I + D` leaves for C written cells, I inserted rows
and D deleted rows. The row key includes table and audience; generation
and change kind are inside the authenticated leaf. Creation markers contain
no scalar payload, and a delete cannot masquerade as a cell or creation.
Canonical column ordering, uniqueness of columns and parity validation remain
format/admission obligations.

**Completeness and hash minimality hold at cell granularity.** `prune` reveals
requested leaves and replaces each maximal absent subtree by one hash.
`prune_root` reconstructs the signed root; `multiproof_complete` gives exactly
the requested leaves, and `all_present_checkable` verifies them with the
original signature. These generic theorems apply to the new `Change` type.
`cell_complete` and `creation_complete` check the independent selection of
current cells and permanent creator evidence. `overwritten_not_opened` proves
that an unrequested old scalar is absent from the openings. Old values still
needed by loss records or provenance are a separate retention obligation.

`multiproof_minimal` proves that no proof in this fixed-tree format revealing
exactly the same leaves has fewer hashes. `hashes_le_absent` bounds the proof
by one digest per absent **leaf**, including overwritten cells and unneeded
operation markers. `untouched_no_hashes` gives zero when all leaves are open.
`absent_subtree_one_hash` replaces a missing subtree by one digest. Adjacent
leaves crossing a tree split can need separate hashes; adjacency alone is
insufficient. This is not a lower bound against other encodings or external
dictionaries. A write contributing no needed leaf need not appear at all.

**A surviving cell needs no overwritten sibling values.** The checked
`overwritten_value_needs_no_opening` example uses creation values `(7, 8)`
with only the first still needed. Its proof opens the creation marker and 7,
hides 8 with one hash, and reconstructs the exact creation root.
`creation_without_initial_values` retains only the marker after both cells
are overwritten. `kind_bound` checks that changing the insert/update tag
changes the root. The older `merged_cells_need_original` example remains a
counterexample to row-sized leaves, not to this format.

**Per-cell authentication overhead is proved in tree levels.** `openAt`
opens one position or rejects a malformed path. `openAt_spec` proves that it
reveals only that leaf, reconstructs the root and uses exactly h sibling
hashes for depth h. `every_leaf_openable` covers every leaf; `per_cell_overhead`
gives **64 + 32h bytes** for an isolated proof using a 64-byte write signature
and 32-byte hashes. For a format choosing a balanced tree with height
`ceil(log2 L)`, that is at most `64 + 32 ceil(log2 L)` bytes. The proved bound
uses actual height; no balancing algorithm is implemented here.

The signature is shared per contributing write, and a multiproof shares
sibling hashes, so this is not a charge of 64 bytes for every cell in a
snapshot. With S contributing writes and H hidden subtrees, signature and
hash material totals `64S + 32H` bytes. Positions, row/column identities,
generation/kind tags, the current values and signed header/root-list framing
are additional; their encoding is not fixed by this model. A creation marker
is additional evidence per row, not an extra copy of its initial values.

**Format boundaries remain explicit.** D5/D7 must define cell/creation/delete
encoding, creation and setter identity, proof positions and malformed-proof
rejection. D9/D11 must bind metadata and the ordered part-root list without
weakening ciphertext/path authentication. The decided format stores 32 bytes
per part, keys each root with its audience key to prevent outsiders testing
row guesses, and permits signature verification without opening unreadable
parts. Header parsing, root selection, key separation and that confidentiality
claim remain outside this fixed-part authenticity argument. The model does
not prove the existing encrypted
chunk format permits a streaming row-root signature with zero additional
bytes. Historical key selection, schema conversion, original-byte encoding,
complete snapshots, latest-write selection, Merkle construction memory use,
retention and Rust correspondence are outside these proofs. A publisher
cannot alter a checked opening, but can still omit history; authentication
alone is not a completeness or freshness proof. B18 lists the spec/API edits.
