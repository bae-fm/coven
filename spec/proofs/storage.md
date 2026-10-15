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
the resulting plaintexts without relying on that detection. Comparing own
snapshot numbers with the whole operation sequence would be weaker still
than §10's per-kind comparison (`operation_ids_hide_newer_snapshot`, below).

A third result is a checked counterexample to a proposed rule: resetting on
a mismatched S3 key delivery path can leave a made key live and unrecorded
([S3 key deliveries](#s3-key-deliveries)). A fourth is a checked
counterexample to a rule Rust followed until T188: resetting when an own
key-copy number holds undecodable bytes discards unsent edits and still
leaves the first phase incomplete ([Undecodable own key copies](#undecodable-own-key-copies)).
Replacing such a copy by its publisher, the rule that followed, is proved
safe and completes every reader's first phase once the replacement is kept,
but two checked histories show damage it never reaches
([Replacing a damaged key copy](#replacing-a-damaged-key-copy)).

All named results are audited by `CovenStorage/Axioms.lean`. The package uses
Lean 4.34.1 and its standard library, without additional axioms or unfinished
proofs. `scripts/check.sh` builds it and runs the audit alongside the other
proofs. `clockRunner`, `createRunner`, `identityRunner` and `filesRunner`
expose the model to the Rust differential tests run by that script. The create-once comparison drives
the S3 adapter against the test server through every catalogued profile,
and the OneDrive adapter through Graph. It checks occupied-path refusals and
stored bytes for single-request and multipart uploads. For profiles publishing
by copy it compares the creates that copy with `CopyPublication.publish`:
multipart creates on AWS and GCS, and every create on SeaweedFS, which
publishes every object by copy. On each of them it races three assembled
sessions on one name in two arrival orders against `CopyPublication.run`:
which copy wins, the stored bytes, and that no temporary is left.

## Copy publication

[`CopyPublication`](storage/CovenStorage/CopyPublication.lean) models §4's
multipart publication by copy. A session assembles its bytes at a temporary
named by the session. Temporaries are keyed by session, not by a D10 path, so
no published read or listing can reach one. One copy, conditioned on an absent
destination, publishes the temporary with the copy's own time and the
session's marker; the temporary is then deleted. The copy is assumed atomic:
it checks and writes the destination in one step.

- `copy_is_create`: a copy changes the published objects exactly as `create`
  does, so `create_preserves` and `stored_history_preserved` apply to it.
  `copy_outcome` gives its result: refused when the name is occupied.
- `stage_hidden` and `discard_hidden`: assembling or deleting a temporary
  changes no published object.
- `race_one_winner`: sessions racing on a free name, each with its temporary
  assembled, publish in their arrival order. The first copy publishes its own
  bytes and marker; every other copy refuses the condition. Every temporary
  is deleted. `race_occupied` covers a name that was already taken.
- `lost_reply_settles`: after a copy whose reply was lost, reading the
  destination finds this session's marker whatever else is staged or
  published in between. `other_marker_refuses` covers another session's object.
- `marker_settles_bytes`: when each session's bytes are fixed, a destination
  carrying this session's marker holds this session's bytes.
- `check_then_write_two_winners` is a checked example of why atomicity is the
  premise. A completion that checks a free name, then writes it, lets two
  racers both report success while the later one's bytes stay. This is how
  SeaweedFS 4.48's conditional multipart completion behaved in certification
  suite 4.

A profile can publish every object by copy, single-request creates
included (`S3CopyPublication::Every`), so that every publication of a name
is a conditional copy: a run of staged temporaries and copies. The provider
assumption is then copy against copy only: the provider applies the
conditional copies of one name one at a time, each checking and writing the
destination in one step. Nothing is assumed of a conditional PUT racing a
copy.

- `every_copy_at_most_one`: in any run of stagings and copies, in any arrival
  order, at most one copy publishes a given name. `copied_at_most` adds that
  none does when the name was already occupied; `publish_elsewhere`,
  `publish_unchanged`, `publish_occupies` and `publish_occupied` are its
  steps.
- `put_racing_copy_two_winners` is a checked example of why PUTs stay
  outside the assumption: a conditional PUT checks a free name, a copy of it
  lands, then the PUT writes. Both are accepted, and the copy's session no
  longer finds its marker. SeaweedFS 4.48's unversioned bucket accepted both
  writers of this race in 14 of 200 suite 5 trials, which is why its profile
  publishes every object by copy.

Every temporary must also go: a temporary, or a native multipart upload that
will assemble one, that no record names stays in the reserved namespace for
good. `CopyPublication.Recorded` models the caller's durable records next to
the bucket. A create is recorded with its destination and fixed bytes until
it settles; a temporary is staged under a name; a crash is a step that never
comes. The rule (T218) stages only under a recorded name and drops a record
only with nothing staged under it. A single-request create's temporary is
named by the create itself, and a multipart session is recorded before its
upload is initiated.

- `staged_recorded`: under the rule, in every history, whatever crashes
  interrupt, every staged temporary is named by a record the caller still
  holds (`step_covered` is the step).
- `retry_clears`: a record's retry, staging again under the same name,
  copying and deleting, leaves nothing staged there, whatever an earlier
  crashed attempt left.
- `failed_deletion_retry_accepted`: a create whose copy landed but whose
  deletion failed retries with a fresh marker under the same name; its copy
  is refused, the destination still holds the create's own bytes, so comparing
  bytes accepts it, and its deletion clears the name.
- `attempt_naming_strands` is a checked example of T208's naming: a create
  staged under a fresh attempt id crashes, its retry stages, publishes and
  deletes under another id, and the record is dropped. The first temporary
  stays and no record names it. A multipart upload initiated before its
  session was recorded is the same history.

Outside the model: whether a real provider's copies are atomic against each
other, which the suite's races test live; the 5 GiB single-copy limit, which
the adapter enforces before any request; the provider's metadata copying
of the marker; collisions of the SHA-256 naming, taken as injective; and a
caller that abandons a create without retrying it, such as a device reset.

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
retirement and preceding entry prefix are final, with no assumption about
what the write read.
Later readers cannot change it. `send_gate_publication_bound` proves the one-day
plus five-minute drain bound from finality. Both proofs state their store-log,
catch-up and provider assumptions explicitly. This proposal changes neither
`ReplacementRead` nor the normative spec files; the merge write-up lists the
required wording and Rust obligations.

### Each object judged in its own view

coven-sync judges an old-id write or entry by replaying its own recorded
past: the object is excluded when that replay keeps a replacement of its
device. `accepts` above instead uses one kept set, the full replay's, for
every object. The two agree whenever each object's view keeps exactly the
replacements of that set it read (`agrees_with_kept_set`), which covers every
history where no replacement is kept in one replay and dropped in another.

They can differ in both directions, and the full replay can change after an
object is judged. `ReplacementViews` models the per-object rule,
`acceptsView`, with replay as a parameter. Its one assumption,
`SetDetermined`, is that replaying a set of entries gives one result whatever
their order; the store-log model proves this as `storelog_converges`. Objects
are judged once every entry they read has arrived (`Ready`), and a path names
one entry (`OnePerPath`).

- `view_order_irrelevant`: devices holding the same entries, in any arrival
  order, give each object the same verdict.
- `verdict_never_rejudged`: once an object is ready, later arrivals never
  change its verdict, even when the full replay then keeps or drops other
  replacements. `view_stable` is the step: later arrivals add nothing to its
  view.
- `views_converge`: devices with the same entries and objects apply the same
  identities. `applied_never_retracted`: an applied identity stays applied as
  entries and objects keep arriving.
- `unstopped_copy_counts`: an object whose copy had not stopped, because its
  own replay kept no replacement of its id, always counts. A copy stops
  exactly when its replay keeps one (§10), so admission and stopping use one
  judgement.

Two checked histories show where the single kept set goes wrong.
`dropped_replacement_keeps_verdict` is the history coven-sync pins in
`a_dropped_replacement_restores_the_old_id_without_rejudging_recorded_pasts`:
Ana registers device 5 while Ben's restored copy, unaware, registers 5
replacing his phone 1. Phone 1's entry made after reading Ben's replacement is
excluded, and stays excluded once Ana's registration arrives and the full
replay drops the replacement; the single set would accept it then.
`full_set_excludes_unstopped_write` is the other direction: Carol removes Ana
concurrently with Ana registering device 5, so the full replay drops Ana's
registration and keeps Ben's replacement. Phone 1's write read both
registrations but not the removal; its own replay kept Ana's and dropped
Ben's, so the phone had not stopped. The per-object rule counts the write;
the single set would exclude a write its copy could not yet know was
replaced. Both histories use a small replay that keeps and drops what the
real one does on these entries. The `admission` differential runs both
histories through Rust's real replay and checks those verdicts.

§10's 30-day window follows the same rule. A write whose own replay keeps
its retirement never counts; every other write is bounded by the 30 days of
each kept retirement of its device, read or not. On the second history the
write is therefore judged by the replacement's window alone
([IO `read_unretired_write_counts`](io.md)).

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
a replacement that the replay of that past keeps excludes an old-id object
(`acceptsView`; `accepts` is the special case above); a write's
`store_log_read` and an entry's expanded `had_read` supply that past.
Publication after replacement can count when creation preceded that read
and the landing check passes. Agreement requires the same received objects,
their storage times and replayed retirements.
Restore includes replacing the database from a backup while custody survives;
it does not imply that the app's explicit reset path was invoked.

Authentication, honest recording of reads, membership authority, entry landing
rejection, the write landing deadline and replay itself remain inputs;
`acceptsView` assumes only that replay depends on the set of entries. The write deadline is a required verification obligation,
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
`IdentityRunner` executes `checkIdentity` and `ReplacementRead.observe`/`gate`
for the Rust differential tests of coven-sync's pre-send check, with every
counter, key copies included, below, at and beyond its reservation. It also
runs `beginReset`, `retryReset`, `finishReset` and the first `commitWrite`
against coven's recorded reset, failed `reset_device` retries and the
published replacement, and `resetUnconnected` against the reset of a store
that never set up storage. Its `sequence` check runs `IdentityCounters.Sequence`
against the snapshot reservation coven-database records as preparations
reserve numbers among other operations started and finished.
It executes `ReplacementRead.acceptsView` and `loadViews` against coven-sync's
write authority and entry replay for old-id objects whose recorded pasts read
any subset of kept, concurrent and unauthorized replacements, and for the two
histories in [Each object judged in its own view](#each-object-judged-in-its-own-view).
Each case hands the model every object's recorded past and the registrations
Rust's replay of that past keeps.

**Key copies, snapshot numbers and the storage-less reset.**
[IdentityCounters](storage/CovenStorage/IdentityCounters.lean) models the
local sequences the reservations come from. A sequence reserves the next
number for an object of the counted kind or for something else, and storage
lists only own objects published from reserved numbers; deletion only
removes. `listed_within` proves a listing never passes the sequence, and
`later_numbers_fresh` that every number reserved after a check exceeds
everything listed then. `send_of_within` is the converse of
`send_covers_all_evidence`: evidence within every reservation sends.

- Key copies (proved). A writer reserves its copies gap-free from their own
  sequence and confirms them in order, so its counter is that sequence
  (`key_copies_counted`; Rust `key_upload::last_reserved`). `live_copy_sends`
  proves that a live copy's own stored copies and snapshots never reset it,
  whatever it deleted, and `key_copy_ahead_resets` that a stored copy beyond
  the reservation does.
- Snapshot numbers (proved, with a checked counterexample for an earlier
  rule). Snapshots have their own sequence: each preparation reserves the
  next number, so every reservation is a snapshot number and the counter is
  the sequence, as for key copies (`key_copies_counted`; Rust
  `_coven_snapshot_reservation`). An abandoned attempt keeps its number
  reserved and the next attempt takes a new path (§15). `live_copy_sends`
  proves a live copy is never reset by it and `snapshot_ahead_resets` that a
  listing beyond it resets. Comparing with a sequence that also numbers other
  operations, as Rust did before T162 when a snapshot took its operation's
  id, is weaker: `operation_ids_hide_newer_snapshot`. Ana's backup holds operations
  1 to 5. Her live copy then writes store snapshots 6 and 7 and deletes 6,
  which 7 covers. Restored with custody intact, the backup starts a snapshot
  operation (6) and another operation (7) before its first check. Storage
  lists snapshot 7, which the restored copy never made, yet 7 does not pass
  the sequence, so it would send; compared with its last snapshot number, 6,
  it resets. Had it sent, its snapshot operation 6 would find that path
  empty and create it: two different snapshots would have had one path, and
  no byte comparison sees it. A restored copy that starts as many snapshot
  operations before its check as its newer self wrote still passes, the
  limit §10 already names ("the checks cannot detect every restore").
- A store that never set up storage (proved). It published nothing and has
  no stored history, so its reset is the new-device path with nothing to
  load and no registration (`unconnected_reset_is_new_device`, Rust
  `reset_unconnected`): an empty database under the fresh id
  (`unconnected_reset_empty`) whose first write is number 1 at a path no old
  write used (`unconnected_first_write_new_path`). Setting up storage
  afterwards is `StoreCreation`, below.

**Two installations of a never-connected store.**
[StoreCreation](storage/CovenStorage/StoreCreation.lean) models setting up
storage for a store that has never had it. Its creation entry is entry 1 of
the creating device. After the storage-less reset the original installation
and the reset copy hold the store under different device ids, so the two
creation entries have different paths and create-once never compares them.

- Setup that writes its creation without reading the store folder, as Rust
  did before T163, leaves two creation entries when both set up one
  location (`blind_setup_two_creations`, checked).
- Setup that first lists the store's entries and refuses one by another
  device creates the store once when the listing includes the first
  creation (`listed_setup_one_creation`, proved), and this device's own
  retried creation passes (`retry_passes`). But the listing is not a lock:
  two setups whose listings both come before the other's creation both pass
  (`concurrent_setups_two_creations`, checked).
- So every setup now first claims one create-once path, the creation marker
  `created` (T205, D10), holding its store and device, and goes on only when
  the stored marker is its own. `Run` and `step` model any number of setups
  whose claim and creation steps interleave freely; `Inv` holds in every
  reachable state (`step_inv`, `run_inv`, `fresh_inv`, proved).
  - From a location with no creation entry, at most one device's creation
    entry is ever stored, in every interleaving
    (`marked_setups_create_once`, proved). A marker already stored there is
    allowed; only its own device can pass it.
  - A setup whose claim was refused stores no creation entry
    (`refused_setup_no_creation`, proved), and its refused claim leaves
    storage exactly as it was (`refused_claim_writes_nothing`, proved).
  - A retried claim by the device whose marker is stored passes again
    (`own_marker_passes`, proved).
  - T163's interleaving, both setups claiming before either writes, now
    creates the store once and refuses the second
    (`concurrent_marked_setups_one_creation`, checked).

Create-once of the marker is the model's atomic `create`; each provider's
create-once write, the marker's encoding and the provider check's test
object are outside the model. Rust's `storage_setup_tests.rs` runs the
sequential history (`a_second_installation_never_creates_a_store_already_created_here`),
the concurrent one with both claims before either write
(`concurrent_setups_of_one_store_create_it_once`) and the retried claim
(`a_retried_setup_passes_its_own_creation_marker`).

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

## S3 key deliveries

`KeyDelivery` models §13's S3 key replacement from the admin desktop's side:
journal a fresh attempt id, make a key under that attempt's name (§4), seal
it to the member, publish it at `<store>/access-keys/<member>/<attempt>` (D10),
then record it in a set-access entry. On an occupied delivery path it follows
§10: equal bytes count as delivered, different bytes reset this copy as a new
device with an empty database. A database copy is a backup, the database a
restore overwrites, or a second live clone. One counter supplies fresh
attempt ids, key ids, sealing randomness and new device ids.

**A delivery path is this device's own path.** `occupant_is_own_copy`
proves, for every history of every device's copies, that whatever occupies
a delivery path was sent by the device whose database journals that attempt.
The attempt id is fresh, only copies of that database journal it, and a copy
publishes only at its own attempt. `reset_is_own_copy` applies this to the
reset: different bytes there come from another copy of the same device,
which is §10's stale-copy condition. `uncopied_never_resets` proves that a
database that is never backed up, restored or cloned never takes this reset,
and that every key it makes stays recorded or journaled.

**Resetting on a delivery path loses a made key.** In
`restored_before_seal_orphans_key`, Ana's desktop makes key 101 for Ben and
is backed up before sealing it. The live database seals and delivers; the
backup is then restored. Nothing was written to a numbered log since the
backup, so §10's counted evidence passes. The restored copy re-seals with
fresh randomness, finds different bytes and resets. §10's reset reads
nothing from the discarded database, so key 101 is now live at the provider,
in no kept entry and in no surviving journal: nothing records it or will
delete it. §13 says discarding a replacement leaves no key unrecorded; this
reset discards one that does.

Sealing bound to its inputs would not remove the loss. With it the same
restore resends the stored bytes and records the key
(`bound_sealing_continues`), as does a backup taken after sealing with today's
sealing (`restored_after_seal_continues`). But in
`restored_before_mint_orphans_key` the backup predates the key: the restored
copy's mint deletes key 101 under the attempt's name and makes 103. The
stored delivery holds the deleted key, so counting it would leave Ben unable
to install the recorded one; the comparison is right to refuse it. The reset
then orphans 103 under either sealer. The loss comes from the reset
discarding a made key, not from the comparison.

**T203: the reset keeps the key pending deletion (proved).** §10's reset now
reads the old database's journal and keeps its made key, and every deletion
it already owed, pending `DeleteAccessKey` in the new database (`deliver`'s
reset branch; each database copy's pending deletions are `World.pending`,
copied by a clone and lost with a dropped database). The pre-T203 reset is
kept as `discardingReset` for the two counterexamples above.
- `reset_keeps_accounted`: the reset step never leaves a live key outside a
  kept entry, a surviving journal or a pending deletion.
- `accounted_step`, `accounted_run`: no step but losing a database outright
  (`drop`) does either, in any history of backups, clones and resets.
- `restored_before_seal_keeps_key_pending` and
  `restored_before_mint_keeps_key_pending` (checked): both restore points now
  end with the made key pending deletion in the reset copy.

So deliveries join §10's own-path list. A database lost before its key is
recorded, rather than reset, still orphans the key; in Rust a database too
damaged to read is that case. Rust's "made key that the loaded store log
records as an active member's current access is in use" exception is outside
the model; there the key is in `recorded` anyway.

The model leaves out the store log, entry admission, the old key's deletion,
the member's side, failed comparison reads, the discard call, invites (whose
made keys sit in the journal the same way) and other members writing the
path. The reading chosen for §10's "nothing newer in storage" is that a
delivery stored after the backup counts as newer, so the reset itself is
permitted; the loss is what it discards.

**T204: listing the provider's keys (checked counterexample).**
[KeyListing](storage/CovenStorage/KeyListing.lean) adds T204's catch-up as
decided: an admin lists the store's keys, which coven names by store and
member, and makes every key no kept entry records pending deletion, leaving
out only the key its own database is making.
- `catchUp_accounts` (proved): after any surviving admin copy's completed
  catch-up, every live key is recorded, journaled by that copy or pending
  deletion there, also when the minting database was lost
  (`lost_database_key_found_by_listing`, checked).
- `catch_up_deletes_in_flight_key` (checked counterexample): Ana makes key
  101 for Ben and delivers it; before her set-access entry is kept, Carol's
  catch-up lists 101 as unrecorded. Ana records 101 as Ben's access, and
  Carol deletes what her catch-up made pending. Ben's recorded current key
  is gone, so his devices that install it are locked out. An outstanding
  invite's key, which no entry records until the person joins, goes the
  same way. A key being made on another admin device is not distinguishable
  by the listing as decided, so the rule is not in §13.

## Undecodable own key copies

T182 made a pass's scan reset the device with `SlotMismatch` when a copy
number it reserved, `<store>/keys/<device>/<n>`, holds an object whose clear
prefix does not decode (`scan_store_log_once` in
[`store_log_batch.rs`](../../crates/coven-sync/src/store_log_batch.rs)). Its
reasoning: this installation seals only decodable copies, so the object must
be another live copy's, and waiting on it as an unreadable copy would leave
every first phase incomplete.

[OwnCopyDamage](storage/CovenStorage/OwnCopyDamage.lean) runs several
devices' database copies over one store's key-copy logs. A copy is a live
database, a backup, the database a restore overwrites, or a second live
clone. Each copy reserves its next number with fixed sealed bytes, publishes
it create-once, and compares an occupied path as §10 says. It reads every
writer's copy log and keeps the bytes it first read, as `discovery::scan`
retains its input. Its first phase is complete only when every copy it holds
decodes, as in `read_copies`. The provider may corrupt any stored object,
which §2 assumes it does not. The scan follows either T182's rule or a
waiting rule that treats the object like any other unreadable copy.

**Who put the object there (proved).** `occupant_own_or_damaged` proves,
for every history under either rule, that an object at
`<store>/keys/<d>/<n>` was published by a copy holding device id `d`: this
database, a backup or a clone of it, never another device. It decodes unless
the provider damaged it. Every copy of every database seals only decodable
bytes, so an undecodable object, stored or as some copy read it, is always
damage and never another copy's object (`unreadable_is_damage`). T182's
trigger therefore fires only on damage at this device's own path
(`trigger_is_damage`). Its comment, "holds another live copy's object", is
never the case.

**The reset does not help (checked counterexample).** In
`damage_resets_uncopied`, Ana's phone (device 7) and Ben's laptop (device 8)
each have one database, never backed up, restored or cloned. Ana edits a
note, which waits unsent. She publishes key copy 1, and the provider corrupts
it before anyone reads it. T182's rule resets her phone although no other
copy of its database exists, and the reset discards her edit. Her new device
101 reads every writer's log, device 7's included, finds the same bytes, and
its first phase is still incomplete, as Ben's is. In general a reset changes
no stored object and the new device has read nothing, so its first scan
either resets again or leaves its first phase incomplete
(`reset_never_completes`, from `fresh_reading_incomplete`).

**Waiting loses nothing (proved).** Under the waiting rule, a database that
is never copied never resets, whatever the provider damages
(`uncopied_waiting_never_resets`). On the same history Ana keeps her device
and her edit, with the same incomplete first phase
(`damage_waits_with_edits`). A device that read the copy before the damage
keeps the bytes it read and is not held at all
(`read_before_damage_keeps_bytes`). Waiting does not weaken §10's
comparison: a clone that seals copy 1 with other randomness still finds Ana's
copy there and resets (`live_copies_still_reset`).

So damage at an own copy number never resets, and on its own it leaves the
read incomplete like any other writer's unreadable copy; T206 then lets its
publisher replace it ([below](#replacing-a-damaged-key-copy)). A number beyond the reservations is different:
its existence proves another copy reserved it, whatever its bytes, since a
database that is never copied has nothing stored past its reservations (the
invariant behind `uncopied_waiting_never_resets`). It still resets as
storage ahead (`key_copy_ahead_resets`). §10 and §11 state this rule, and
Rust follows it: the scan's own-number reset is gone.

The model leaves out the store log, key selection, sharing, positions, the
drain of a replaced id (whose retained history keeps the damaged object, so
it leaves the phase incomplete as well), failed reads and the `require_complete`
catch-up. Corruption here changes the bytes of a stored object; it never
creates one. Someone planting bytes at an empty path is outside the model:
§2 trusts members on storage but lets a thief with storage credentials be
hostile. Undecodable planted bytes are not another copy's object either.

## Replacing a damaged key copy

Waiting on damage, as above, waits for good: create-once forbids rewriting
the path, and every reader keeps reading the same bytes. T206's rule: the
device that published the copy reseals the same key to the same recipient
under its next number and, once that copy is stored, publishes a store-log
entry naming the damaged number and the replacement. A reader skips an
undecodable copy that such a kept entry of the copy's writer marks.

[CopyReplacement](storage/CovenStorage/CopyReplacement.lean) extends the
model above. A copy now carries routing facts: its audience, key id,
recipient and exact sealing key (D9), which is what §11's exposure rule
reads. Each database remembers the facts of the copies it confirmed, as
Rust keeps their bytes. The entry names numbers only, so its writer is the
device whose log holds it. Like every send, reserving the replacement and
publishing the entry need the device's own last reading to be complete.
In that reading the publisher's record of its own confirmed copy stands in
for undecodable bytes.

**Skipping hides nothing (proved).** In every history a marked copy holds
damaged, undecodable bytes, and the replacement its writer named holds a
copy by that writer with the same routing facts (`marked_is_damage`). A
reader skips only bytes it read as undecodable, which are damage, and the
replacement carries what they held (`skipped_is_damage`). A reader that
read the copy before the damage keeps decodable bytes and still observes
them. A device can mark only its own log's copies, and it marks only a
copy it read as undecodable.

**The publisher's record is exact (proved).** What a database recorded for
its own confirmed copy is what that copy held: a reading of the path,
decodable or not, has the recorded facts (`own_record_matches`). So the
publisher's observation, with its record standing in, is the one the
undamaged copy would give, and the publisher keeps its sending authority.

**Readers complete (proved).** Once every listed undecodable object is
marked, or is the reader's own recorded copy, that reader's next scan
completes its first phase, whatever bytes it retained
(`complete_once_marked`). A publisher whose reading is complete, with
nothing else reserved and its next number free, marks the copy in three
steps: reserve the replacement with its entry, publish the copy, publish
the entry (`publisher_marks`). `replaced_copy_completes` runs it: Ana's
phone publishes copy 1 to Ben, the provider corrupts it, Ben waits, Ana's
phone completes on its record, replaces the copy with copy 2, and Ben
completes and observes the same facts at copy 2. A replacement damaged in
turn is replaced again (`damaged_replacement_replaced_again`), and Ben,
whose entries name his own log, cannot mark Ana's copy
(`non_owner_cannot_mark`).

**Where it does not reach (checked counterexamples).** The rule needs the
publisher to read its copy after the damage. In `owner_read_first_stalls`
Ana's phone read copy 1 before the provider corrupted it, keeps the bytes
it read, never sees the damage, and Ben waits however often both scan. In
`cross_damage_stalls` Ana's and Ben's copies are both corrupted before
either reads them: each one's record stands in for its own copy, but the
other's leaves its reading incomplete, so neither has the sending
authority a replacement needs, and neither ever completes. A removed or
replaced device sends nothing, so its damaged copies stay as well. §11
states the rule and these limits.

Rust follows the rule: `_coven_published_key_copies` keeps every confirmed
copy's bytes; `CopyReading::settle` (`copy_replacement.rs`) skips marked
copies and stands the device's own confirmed bytes in for its undecodable
copies, which start a `ReplaceKeyCopy` operation; sealing its entry reseals
the stood-in copy's prefix at the entry's replacement number.
`the_publisher_replaces_a_damaged_copy_and_readers_complete` and
`another_device_cannot_mark_a_copy` (`copy_replacement_tests.rs`) run the
histories. The model leaves out replay (the entry's authority is that its
author is a member, and it conflicts with nothing), sharing, key selection,
positions and failed reads; the store-log model does not know the entry.

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

**A wrong-size object is damage, not presence or absence.** Status takes the
file's complete encrypted size, which the row's plaintext size fixes.
`available_iff` proves that status says Available exactly when storage holds
an object of that size at the fixed path. `wrong_size_is_damage` proves that
any other size gives an integrity error, whatever the uploader's state or
reports, so it is never Available, Uploading or Missing.
`absence_not_available` keeps a confirmed absence from ever reading as
Available.

**Reads agree.** `rangeReply` is what a provider that passes coven-storage's
conformance check answers for one range: exactly those bytes together with the
complete object's size (§4: a ranged read still describes the complete
object), a confirmed absence, or a refusal for a range reaching past the
object's end. `fetch` checks that size against the file's, as status does.
`wrong_size_read_is_damage` proves that an object of any other size is an
integrity error to every read, longer objects included, and
`past_end_is_damage` that a refusal past the end is the same damage.
`present_never_absent` proves that a present object is never read as absent.
`complete_object_reads` proves the rule never refuses a genuine file: a
complete object returns exactly the requested bytes for every range inside
its size. `read_absence_matches_status` proves that a read meeting a confirmed
absence fails with the same Uploading or Missing verdict a status call gives,
and `read_failure_kept` that a provider failure keeps its category.
`read_agrees_with_status` puts these together: for every storage observation
and every range inside the file, a read returns bytes exactly when status says
Available and fails as damage exactly when status does. `damage_cases` and
`longer_object_is_damage` check small objects; the second is an object whose
leading bytes are all there but whose reported size is one too many.

Every provider adapter reports the complete size on a ranged read: S3 and the
HTTP providers from the reply's `Content-Range` total, checked against the
object's metadata, Dropbox from its download metadata, and CloudKit from the
record. coven-storage's conformance suite, run against the S3, Drive,
Dropbox and OneDrive test servers and the in-memory provider, refuses a
ranged read whose metadata is not the complete object's; CloudKit's adapter
test checks the same.

**Completion never changes a row.** `attachment_atomic` fixes the whole file
reference and queues it in the attaching transaction. `completion_changes_no_row`
proves that finishing any attempt changes neither rows nor the app-write count.
`completion_retires_both` removes its queue entry and pending record together;
`completion_idempotent` covers a repeated completion.
`older_upload_cannot_restore_photo` checks §16.1's example: photo B replaces A,
then A finishes uploading, leaving B in the row. `unequal_bytes_keep_queue`
prevents a collision from retiring that upload.
`finish_reports_result` preserves the typed error or reset request for the caller.

**Deleting a row never loses or misattributes its upload.** `deleteRow` is an
ordinary app write: it clears the row and counts as a write, and
`delete_keeps_uploads` proves it leaves the queue and pending records alone.
`delete_only_that_row` proves it touches no other row. `steps_keep_upload`
proves that any sequence of attachments and row deletions keeps a queued
upload and its pending record. `inflight_upload_settles` takes an upload from
the queue, lets any such sequence run, then settles it: the settlement writes
no row and changes no write count, so a deleted row stays deleted and a row
attached again keeps its new file; a stored result retires exactly the queue
entries with this upload's reference; and any other result keeps the upload
queued. `deleted_row_upload_finishes` and `reattached_row_keeps_new_upload`
check the two histories.

`FilesRunner` executes `attach`, `deleteRow` and `finish` for coven-sync's
`st_differential_files`, which attaches app-provided files, retargets or
deletes their rows while an upload is in flight, and compares the settlements,
whether each deletion and settlement changed the write count, each row's fixed
reference and the queue. Its `status` cases serve `st_differential_file_status`:
every storage observation, uploader state and source report through
coven-sync's status rule, then absent, failing, complete, longer and
truncated objects through the real status call and whole-file read, which
agree on each. Those read
cases hand the model the ranges the read actually requested and compare its
`fetch` result with the read's.

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
- The file's complete encrypted size is an input. Deriving it from the
  plaintext size, and the header check that keeps every read range inside it,
  are outside the model; the differential test exercises both in Rust.
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
