# Storage identity, files and clocks

The Lean package in [`storage/`](storage/) models [§4](../coven.md#4-storage-providers-and-access),
[§6](../coven.md#6-syncing-writes), [§7.2](../coven.md#72-timestamps),
[§10](../coven.md#10-device-identity), [§16](../coven.md#16-files), and the
waiting-write rules in [§17](../coven.md#17-schema-changes).
The format boundaries are [D6, D8–D12](../format.md#d6-store-log-entries);
the app results follow [E5](../api.md#e5-storage-and-sync) and
[E8](../api.md#e8-files-and-the-cache).

Two requested claims fail without qualifications. Reset deliberately loses
unsent edits. Also, §10's detection checks do not establish that every restored
database gets a new device id. A backup can erase the record of an encryption
attempt without leaving any evidence in storage.

All named results are audited by `CovenStorage/Axioms.lean`. The package uses
Lean 4.34.1 and its standard library, without additional axioms or unfinished
proofs. `scripts/check.sh` builds it and runs the audit alongside the other
proofs. There is no Rust comparison for this package.

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

## Checks and closed logs

`checkIdentity` models the checks §10 actually lists: non-backed-up custody,
complete object listings, authenticated snapshot and posted coverage, replacement,
and removal. `send_covers_all_evidence` proves that a successful check has no
observed counter beyond the local reservations, from any of those sources.
The examples include a deleted log whose snapshot or posted positions still
expose the rollback. A reserved upload already present in storage is allowed.

`sends_require_check`, `failed_scan_sends_nothing` and
`blocked_gate_emits_nothing` cover writes, entries, snapshots, files, key copies
and positions. The caller supplies a completed scan or its failure, never a
successful partial listing. Proving that a provider adapter performs that
complete scan is outside this package.

`replacement_has_owner` requires the same member and a distinct new id.
`ends_comm`, `ends_assoc`, `ends_idempotent` and `ends_keep_both` prove that
concurrent replacement ends combine independently by maximum, regardless of
receipt order or repetition. `combined_end_keeps_completed` shows that an
object included by either replacement remains consumable. `beyond_end_waits`
blocks later objects pending a replacement that includes them. Such objects
are not deleted by this model.

`reset_unavailable`, `reset_uses_fresh_id` and `reset_discards_queue` describe
the bootstrap boundary: retry keeps the chosen replacement; failure cannot
reopen the discarded database; successful loading starts with the fresh id.
`reset_first_write_new_path` proves that its first write is number 1 at a
different path from every old write. `registration_retry_number` gives a new
entry number after a too-late registration without changing the fresh device id.
Authority checks, §9's late-entry rejection and finality are inputs from
store-log replay. An observed end contributes only when its replacement is
kept. This package does not prove that a late registration eventually succeeds,
or that someone eventually extends a closed end after a racing upload.

## Nonces

Here “reuse” means encrypting different protected inputs under one key and
nonce. Retrying identical inputs is required by §6 and is harmless in this
model. A nonce is represented by its D11 inputs: key, path, section and index.
The HMAC computation and chance of cryptographic collisions are not modelled.

**Fixed durable attempts are safe.** `retry_fixed` proves that a tried write
keeps its plaintext, key and format even after migration or an update changes
the choices for new writes. `durable_attempts_nonce_safe` proves that fresh
path assignments and retries preserve one assignment per path over every
history built from those steps. `one_writer_nonce_safe` then makes equal nonce
contexts imply equal attempts. `fresh_path_separates_nonces` covers a reset
whose new device id gives different paths.

**The claim that only two live copies can break this fails for the specified
detection mechanism.** `single_live_restore_nonce_reuse` checks this history:

1. An already registered device backs up its database before reserving its
   first data write.
2. It commits write A, reserves number 1, and encrypts its first attempt.
   That attempt does not land. The backup contains neither its reservation
   nor its attempted flag.
3. The app stops. The database backup is restored on the same installation.
   Its existing custody id survives; custody was not copied from the backup.
4. The app commits different write B, reserving number 1 again. Its storage
   counters have not advanced and its custody id matches, so every listed
   §10 check passes. It encrypts B with the same key, path, section and chunk
   index. Both attempts use format 2; no schema or format change is needed.

There is only one live installation in `BackupRun`. Its encryption history
contains both attempts. A literal Lean `example` checks the complete history;
the named theorem also checks the equal nonce contexts and different plaintexts.
`fewer_than_two_attempts_no_reuse` proves that fewer than two attempts cannot
contain unequal attempts. This history uses two app commits and two encryptions;
the device's existing registration is background history.

The erased reservation and attempt record are the missing evidence.
`rollback_conversion_changes_plaintext` also checks that §17.1's conversion of
an apparently untried backup can change plaintext under the same context;
retaining the attempted flag would prevent that conversion. This supplementary
example checks the queue operation, not an entire migration and snapshot history.

§7.2 says every restored copy gets a fresh id. If restoration is always known
and routed through that reset, the counterexample is excluded. §10 does not
state a restore notification or a custody value that advances with encryption
attempts. Non-backed-up custody need not disappear when the database alone is
restored. Thus the promised fresh-id outcome does not follow from the listed
detection checks. No theorem claims unconditional nonce safety across rollback.

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
`completion_retires_both` removes its queue entry and blocker together;
`completion_idempotent` covers a repeated completion.
`older_upload_cannot_restore_photo` checks §16.1's example: photo B replaces A,
then A finishes uploading, leaving B in the row. `unequal_bytes_keep_queue`
prevents a collision from retiring that upload.
`finish_reports_result` preserves the typed error or reset request for the caller.

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
maximum timestamp. `waiting_is_not_seen` keeps blocked input out of the latest
timestamp. `snapshot_upper` proves that loading adopts all applied timestamps.

`age_never_early` and `finality_age_never_early` prove that an observed lower
bound on provider time cannot declare the corresponding age before the provider's
actual time. `wall_jump_changes_no_age_or_retry` makes wall-clock changes irrelevant
to storage age and the monotonic retry deadline. `retry_delay_bounded` proves
the five-minute ceiling. These results assume §4's common, nondecreasing provider
clock; they do not establish it for a real provider.

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
