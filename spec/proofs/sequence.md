# One store sequence

The Lean package in [`sequence/`](sequence/) checks the proposed replacement of
per-device store logs and write logs with one numbered sequence per store. The
normative spec does not describe the sequence yet; this package checks the
design before the spec changes. It uses Lean 4.34.1 and its standard library,
without additional axioms or unfinished proofs. All named results are audited by
`CovenSequence/Axioms.lean`, and `scripts/check.sh` builds the package and runs
the audit with the other proofs.

Slot `<store>/seq/<n>` is claimed create-once. It holds an admin entry, one
device's batch of writes for a sync pass (one part per audience it writes to),
a key copy, a snapshot announcement for one audience ("audience `A` as of slot
`c` is at path `p`"), or a schema raise of one audience, which announces its
snapshot the same way. The audiences are the store and each circle. Every
device applies slots in strict order.

Five results are checked counterexamples, and each one is the reason for a rule:

- **Snapshots cover the last raise.** A snapshot announced after a raise must
  cover that raise; C6 alone is not enough
  ([Snapshots and deletion](#snapshots-and-deletion)).
- **Readers and claimants re-read the floor.** Deleted slot paths are free
  again for create-once, so a claimant and a reader each read the floor after
  the slots they read ([Claiming a slot](#claiming-a-slot)).
- **Compare bytes after "already exists".** Without the compare, a winner
  claims again and lands twice.
- **Resolve the recorded claim on restart.** A restart that does not resolve
  it lands an operation twice ([Operations](#operations)).
- **Undo after every artifact request ends.** Otherwise a late create leaves
  an artifact behind.
- **Settle from the per-device claim number.** Settling a deleted slot's claim
  from the snapshot's state alone loses or duplicates writes, and re-applies
  undone admin changes
  ([Settling a claim](#settling-a-claim-whose-slot-was-deleted)).

## The model

[`Model`](sequence/CovenSequence/Model.lean) keeps every type abstract: devices,
members, unique keys, paths, batches, rows, audiences, and admin operations
other than membership (roles, renames, access, circle membership), whose state
is `aux`. The rules are abstract too: authority, who may write to an audience,
the effect of other operations, the merge, the converter of old-version parts,
and the database part of a migration.

For each audience `A`, a device's state has three parts:

- **The ledger** holds what admin slots and key copies decide, the same for
  every audience. That is members, devices and their removal, member key
  generations, `aux`, key copies and refused admin slots. It also holds, per
  device, the last claim number of the slots it claimed: every device numbers
  every slot it claims with one counter, and every slot records its number.
- **`A`'s audience ledger** holds the first slot that claimed each unique value
  or owner row of `A`, `A`'s announced snapshots, its schema version, its last
  raise, and its refused parts.
- **`A`'s rows** come from the abstract merge. A part at an older version is
  converted to `A`'s version. A valid raise of `A` at slot `r` covering `c`
  loads the raiser's migrated rows as of `c`. It then replays the counted `A`
  parts of slots `c+1` to `r-1`, converted to the new version.

Only admin slots and key copies change the ledger, besides the claim number
every slot records, and only slots with a part or announcement for `A` change
`A`'s other two parts. A slot or part invalid at
its position is refused: it is consumed, and only its refusal is recorded.
Removing a member removes every device registered to it, so re-adding the
member does not revive them.

These are the validity rules:

- **A part counts** when its device is registered and not removed, its member
  is a member, the part was made under that member's current key generation,
  the device may write to `A`, and the part's version is one `A` has reached.
- **A raise of `A`** must cover every snapshot of `A` announced before it (C6)
  and `A`'s last raise, and must raise the version.
- **A snapshot of `A`** must cover less than its own slot and must cover `A`'s
  last raise (the version rule).

## Claiming a slot

[`Claim`](sequence/CovenSequence/Claim.lean) models the provider's slots as a
store that only fills empty slots. "Created" is answered only when the slot
holds the claimant's bytes. "Already exists" and a lost reply can come at any
time, including to the claimant that won. The protocol: "created" means won.
Otherwise read the slot: own bytes mean won, other bytes mean lost, and an empty
slot means send the same bytes again.

- `conclude_correct`: a claimant that concludes `won` holds the slot forever.
  One that concludes `lost` sees other bytes there forever, so its own create
  can never land.
- `occupant_claimed`: every occupied slot holds the bytes of a claimant that
  created it.
- `exactly_one_claimant`: with bytes unique per attempt, every occupied slot has
  one claimant. That claimant concludes `won` whenever it concludes anything,
  and every other claimant concludes `lost`.
- `no_compare_lands_twice` is a checked counterexample to the protocol without
  the compare. The winner is answered "already exists", concludes `lost`, and
  claims the next slot with the same batch, which then occupies two slots.
- `stale_create_wins_deleted_slot` is a checked counterexample.
  1. Slot 1 is claimed and covered by a snapshot, the floor is raised, and
     slot 1 is deleted.
  2. A stale create of slot 1 is then stored and truthfully answered
     "created". It was sent before its claimant saw the claim, or after the
     device slept past the grace period.
  3. The claimant concludes it won a slot no device at the floor reads, and a
     reader below the floor reads the stale bytes where the genuine content
     was different.
- `floor_check_sound`: bytes read at a slot are its genuine first bytes, and
  stay so, when a later floor read has not passed the slot.

[`Retention`](sequence/CovenSequence/Retention.lean) applies this to the full
storage model:

- `claim_floor_check`: a claimant that saw its bytes at slot `i` and then, after
  any storage steps, reads a floor whose first kept slot is not past `i` holds
  the genuine slot `i`. If the floor has passed `i`, the claim is void and the
  batch is claimed again.

## Convergence

[`Converge`](sequence/CovenSequence/Converge.lean):

- `prefix_determines_state`: for every audience, two sequences that agree on
  slots `1..k` give the same ledger, audience ledger and rows after `k`.
- `valid_prefix`: whether slot `n` is valid, as an admin slot or for any
  audience, depends only on slots `1..n-1`. What an operation validated before
  claiming is what every device checks when it applies the slot.
- `readers_agree` (in `Reader`): two sound readers of one audience at the same
  position hold the same state, on any good storages, for example before and
  after a deletion.

## The removal cut-off

[`Causality`](sequence/CovenSequence/Causality.lean) records each write's
original device, number, timestamp, schema, bytes and `readThrough`.

- `authored_readThrough`, `catchup_preserves_originals`: commit records the
  applied slot; subsequent catch-up preserves every queued original.
- `ordered_application_causal`, `past_transitive`, `receivers_agree`: under
  number-ordered publication and monotone authored reads, strict slot and
  within-slot write order consumes every cause first. Receivers agree on
  exactly the authored prefix plus own predecessors, including predecessors
  outside that prefix. A later claim does not add intervening remote writes.

[`Cutoff`](sequence/CovenSequence/Cutoff.lean):

- `cutoff`: a batch at slot `k+1` is refused in every audience it has a part
  for when its device was cut off at an earlier applied slot `j`. The cut-off
  is one of:
  - the device's removal;
  - the removal of the member it was registered to;
  - the replacement of that member's key at or above the generation the batch
    was made under.

  Each audience only records the refusal, so no unique value or owner row is
  taken and its rows are unchanged. The ledger only records the slot's claim
  number, and every device agrees by `prefix_determines_state`.

A batch is cut off when it was "made under a replaced key". A batch that the
member's other devices make under the new key counts.

## Unique values and owner rows

[`Unique`](sequence/CovenSequence/Unique.lean), per audience:

- `first_slot_wins`: starting with no owners, a key's owner after `k` slots is
  `i` exactly when slot `i ≤ k` is the first counted part claiming it. It has
  none exactly when no counted part up to `k` claims it. Owner creation uses
  the same rule.
- `owner_stays`: an owner, once chosen, never changes. A long-offline device's
  late claim loses whatever its timestamp.

## Snapshots and deletion

[`Reader`](sequence/CovenSequence/Reader.lean) models a reader of one audience
`A` following storage. Storage holds slot records and snapshot bodies. It also
holds one floor record: for each audience, the snapshot it relies on (coverage
and path), plus the first kept slot. A ghost record keeps each slot's first
bytes, its genuine content. A reader steps in four ways:

1. **Fetch:** it fetches slot records into a buffer.
2. **Apply:** it applies its next slot from the buffer once a floor read after
   the fetch shows the slot at or above the first kept slot. A slot with a part
   or announcement for `A` needs `A`'s key. Other audiences' parts are not
   read; the slot's clear metadata says they are not `A`'s.
3. **Raise:** it applies a valid raise of `A` by loading the raise body and
   replaying the buffered slots after the raise's coverage, all checked against
   the floor.
4. **Reload:** with `A`'s key, it reloads from the floor's snapshot for `A` when
   that covers more than it applied.

[`Retention`](sequence/CovenSequence/Retention.lean) models what happens to
storage meanwhile:

- **Claims:** a claim of the next slot, its raise body uploaded first.
- **Stale creates:** a stale create of an earlier slot, which is stored only if
  that slot was deleted.
- **Floor write:** retention writes the floor record conditionally and never
  backwards. Each audience's snapshot is validly announced with its body
  stored, and the first kept slot is covered by every audience's snapshot.
- **Deletion:** retention then deletes slots below the first kept slot, and
  bodies no floor names and no raise above the floor needs.
- **Uploads:** a body uploaded to a fresh path.

There is no separate step that advertises a floor before it takes effect. The
reader's floor read after its fetch, and the claimant's after its claim, are
what make one conditional floor write safe.

`Good` is what these steps keep true:

- at or above the first kept slot, records are genuine;
- each audience's floor snapshot was validly announced, and its body is the
  state it covers;
- a valid raise's body, when present, is the raiser's migrated rows, and it
  stays present while the floor has not covered the raise.

[`Deadline`](sequence/CovenSequence/Deadline.lean):

- `bounded_retention`: successful snapshot service U, grace G, maintenance
  spacing P (including a successful conditional floor write), and deletion
  service D delete a ready prefix within U+G+P+D. Coverage witnesses must stay
  usable through the floor update; no device acknowledgements occur in the model.
- `uncovered_has_no_deadline`: missing coverage in any relevant audience
  prevents the retention gate at every time. `cut_delete_allowed` connects
  the service's floor bound to the concrete storage deletion step.

- `read_ok`, `reads_ok`: every reader step from a sound reader gives a sound
  reader, whatever was deleted or recreated below the floor. Retained slots are
  not assumed genuine; the floor check makes the buffered ones genuine.
- `store_good`, `stores_good`: claims, stale creates, floor writes, deletions and
  uploads keep storage good.
- `store_reader`: storage steps keep every sound reader sound.
- `read_progress`, `reaches_head`: with its audience's key, a sound reader below
  the head can fetch and step forward, and some run reaches the head.
- `joiner_reaches_head`: a joiner of `A` that loads the floor's snapshot for `A`
  and follows storage reaches the state of a device that applied every slot.

**The raise hold.** The proof of `read_progress` needs every valid raise of `A`
above the floor's coverage to cover the floor's snapshot. C6 gives this for a
snapshot announced before the raise. The version rule gives it for one
announced after: such a snapshot covers the raise. So no retention hold is
needed.

- `ReaderExamples.slot4_refused`: a snapshot covering slot 2, announced at slot
  4 after a raise at slot 3, meets C6 and every other condition, and the
  version rule refuses it. A device can produce it: it captures at slot 2, the
  raise takes slot 3, and its refused claim re-validates and claims slot 4.
- `ReaderExamples.stale_floor_strands_joiner` is a checked counterexample. A
  floor naming that snapshot has a true body and genuine kept slots. A joiner
  with the key still never moves past slot 2 while the head is 4, because slot
  3 is a valid raise whose replay needs the deleted slot 2.

## Audiences and devices

[`Device`](sequence/CovenSequence/Device.lean) models a device with the list of
audiences it follows, a key per audience, and one reader per audience. A
reader may move only while no other reader of the device is behind it. When a
reader's next slot, checked against the floor, is its audience's and the device
has no key for it, the device stops entirely.

- `keyless_passes_only_others` (in `Reader`): a reader of `A` without `A`'s key
  passes only slots that have no part or announcement for `A`.
- `keyless_stops` (in `Reader`): such a reader whose next slot is `A`'s never
  moves.
- `device_stops`: a device following `B` without `B`'s key, whose `B` reader's
  next slot is `B`'s, never moves that reader. Even before it notices, every
  other reader applies at most up to that slot, or loads its own floor
  snapshot.
- `stopped_stays`: once the device has stopped, nothing moves: no reader
  applies a slot and none reloads.
- `dev_ok`: device steps keep every followed reader sound.
- `device_reaches_head`: on good storage, a running device holding the keys of
  the audiences it follows, and no others, can run every reader to the head. A
  device outside a circle has no reader for it, applies the circle's slots
  without reading their circle parts, and reaches the right state for every
  audience it reads.
- `deletion_keeps_reachable`: this holds under any interleaving of claims,
  stale creates, floor writes, deletions, uploads and reader steps:
  - storage stays good;
  - every reader stays at a prefix state of its audience;
  - a device holding its audiences' keys never stops, and can still run every
    reader to the head of the current storage, where each holds the full state
    of its audience.

  Deleting covered slots never changes any device's reachable state.

## Operations

[`Operation`](sequence/CovenSequence/Operation.lean) models one admin operation
as a journal entry:

- it records each pre-claim artifact before sending its create;
- it records the slot it claims before sending the claim, and resolves that
  claim before doing anything else;
- its bytes name the operation and the slot.

A crash loses nothing the steps use. Every step reads the journal and storage,
and a restart takes the same steps. Sent claim requests survive crashes and can
land at any later time. So can artifact creates, until each ends by landing,
failing or being cancelled. The undo runs only after every create it sent has
ended. It deletes by recorded name and confirms absence before deleting the
journal. Other devices claim slots at any time. Validation (`allowed`) reads
only slots `1..n-1`.

- `step_inv`, `step_req`, `steps_inv`: every step keeps the invariants. One of
  them is that every create still in flight is recorded while the journal
  exists, and outlives it only for a landed operation.
- `operation_outcome`: over any interleaving, the operation's bytes occupy at
  most one slot. A landed operation's bytes are at its slot; the slots before
  it were filled when it validated, and validation held against them. An
  undone operation has no bytes in the sequence, now or later, and none of its
  recorded artifacts remains in storage.
- `forgetful_lands_twice` is a checked counterexample. A restart that does not
  resolve its recorded claim validates again after the first claim landed. It
  then claims the next slot and lands there too.
- `eager_undo_leaks` is a checked counterexample. An undo deletes an artifact
  while its create is in flight, confirms absence and ends. The create then
  lands, leaving the artifact of an undone operation in storage. An S3 key mint
  that cannot be cancelled is covered by the reclaim of keys tagged with their
  device, outside this model.

## Settling a claim whose slot was deleted

[`Settle`](sequence/CovenSequence/Settle.lean). Each device numbers every slot
it claims with one counter: batches, admin entries, key copies, announcements.
Before each create, the journal records the slot and the exact bytes. On
reopen, the device reads the slot and compares bytes. If the floor has passed
the slot, it settles from the floor's snapshot instead. Each snapshot records,
per device, the last claim number it includes, and a claim landed exactly when
that number covers it. Admin claims settle by the same rule as batches.

The first part abstracts slots to what settlement reads: a claim's device, its
number, an attempt nonce and a value. `Own` holds what the device knows of its
claim `n`: its claims carry numbers up to `n`, and only the recorded attempt
can be in the sequence, because every earlier attempt was settled as not
landed.

- `lastClaim_ge`: the snapshot's record covers number `n` exactly when some
  claim of the device up to its coverage reaches `n`.
- `settle_correct`: settlement says `landed` exactly when the claim is in the
  sequence. `notLanded` means it is nowhere in it, and `retry` means its slot
  is still empty.
- `lands_once_never_lost`: a device that claims again exactly when settlement
  says `notLanded` ends with the claim at exactly one slot. For a batch its
  writes land once; for an admin entry its change lands once. That
  re-establishes `Own` for its next return.

The second part reads the record from `Model`. The ledger keeps each device's
last claim number, so every snapshot body carries it.

- `claims_ge`: the ledger's record covers `n` exactly when some slot of the
  device up to that point carries a claim number reaching `n`.
- `settle_from_floor`: when the floor has passed the recorded slot, the floor's
  snapshot body for any audience records the claim's number exactly when the
  slot holds the recorded bytes. With `claim_floor_check` for a kept slot,
  every claim settles correctly from storage.

Two checked counterexamples show why the number is needed:

- `state_alone_cannot_settle`: in one history the device's batch took slot 1 and
  was then overwritten; in the other, another device took slot 1. The
  snapshot's state is the same in both, so any rule that settles from the state
  is wrong in one. It either claims a landed batch again, landing its writes
  twice, or drops an unlanded one, losing them. The per-device record tells the
  two apart.
- `admin_state_cannot_settle`: the device's claim adds a member, and a later
  slot removes the member. If another device's identical entry took the slot
  instead, the ledger after the removal is the same apart from claim numbers.
  Settling by presence says "not landed" in both, so in the first history the
  device adds the member again, against the removal. The claim numbers differ.

**Another copy using this device id.** Each copy records an attempt before
sending it and creates only what it recorded, signed with its id.

- `foreign_slot_means_copy`: from empty storage, a slot signed with a copy's id
  whose bytes that copy's journal never attempted was created by another copy
  using the same id.

## Assumptions and what is not modelled

- Snapshot and raise bodies are honest: each is the announcer's state as of its
  coverage, or the raiser's migrated rows.
- Admin slots and key copies are readable by every member device. So are a
  slot's device, claim number and each part's audience: they are sealed under
  the store key, which every member holds, and the provider sees only the slot
  kind and key-copy routing.
- Unique values and owner rows are per audience; uniqueness across audiences is
  not modelled.
- A part at a version above its audience's is refused. A device whose app
  cannot apply a slot stops before it; that stop is not modelled.
- Circle membership changes are not modelled as a device starting or stopping
  an audience: a device starts following an audience as a joiner of it.
- `Model` takes each slot's claim number as given (`Rules.number`); that a
  device's numbers increase is a hypothesis of `settle_from_floor`, not
  derived from a device's counter.
- Not modelled:
  - Drive's head file and its compare-and-swap;
  - unannounced snapshot bodies and orphaned batch objects;
  - releasing a unique value;
  - joiners reading slots before they hold the store key;
  - the merge's internals, which the merge package covers.
