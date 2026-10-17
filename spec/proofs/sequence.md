# One store sequence

The Lean package in [`sequence/`](sequence/) backs the normative sequence rules:
[claims and application (§6)](../coven.md#6-syncing-writes),
[unique claims (§8.5)](../coven.md#unique-constraints),
[owner selection (§8.8)](../coven.md#88-owner-tables),
[admin prefixes (§9)](../coven.md#9-members-and-roles),
[cut-offs (§10)](../coven.md#10-device-identity),
[audience reads (§14.4)](../coven.md#144-writes),
[snapshots and retention (§15)](../coven.md#15-snapshots), and
[operation journals (§18)](../coven.md#18-operations), in the order described by
[one sync pass](../sync-pass.md). Each result has the assumptions and limits
below.
It uses Lean 4.34.1 and its standard library,
without additional axioms or unfinished proofs. All named results are audited by
`CovenSequence/Axioms.lean`, and `scripts/check.sh` builds the package and runs
the audit with the other proofs.

Slot `<store>/seq/<n>` is claimed create-once. It holds an admin entry, one
device's batch of writes for a sync pass (one part per audience it writes to),
a key copy, a snapshot announcement for one audience ("audience `A` as of slot
`c` is at path `p`"), a preparation announcement, or a schema raise or reset
that activates its matching preparation. The audiences are the store and each circle. Every
device applies slots in strict order.

Checked counterexamples explain the rules:

- **Earlier live losers retain their claims.** Deleting the winner lets an
  earlier live loser return before a new claimant
  ([Unique values and owner rows](#unique-values-and-owner-rows)).
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
other than membership and roles (renames, access, circle membership), whose state
is `aux`. Authority for membership and store resets is concrete in `Authority`;
other rules remain abstract: who may write to an audience,
the effect of other operations, the merge, the converter of old-version parts,
and the database part of a migration.

For each audience `A`, a device's state has three parts:

- **The ledger** holds what admin slots and key copies decide, the same for
  every audience. That is members and roles, devices and their removal, member key
  generations, `aux`, key copies and refused admin slots. It also holds, per
  device, the last claim number of the slots it claimed: every device numbers
  every slot it claims with one counter, and every slot records its number.
- **`A`'s audience ledger** holds the first slot that claimed each unique value
  or owner row of `A`, `A`'s announced snapshots and preparations, its schema version,
  its last raise or reset, its last reset, and its refused parts.
- **`A`'s rows** come from the abstract merge. A part at an older version is
  converted to `A`'s version. A valid raise of `A` at slot `r` covering `c`
  loads the raiser's migrated rows as of `c`. It then replays the counted `A`
  parts of slots `c+1` to `r-1`, converted to the new version. A reset loads
  its chosen rows and discards that suffix. Subsequent writes pass the
  authored-read reset gate before conversion.

Only admin slots and key copies change the ledger, besides the claim number
every slot records, and only slots with a part or announcement for `A` change
`A`'s other two parts. A slot or part invalid at
its position is refused: it is consumed, and only its refusal is recorded.
Removing a member removes every device registered to it, so re-adding the
member does not revive them.

These are the validity rules:

- **An admin entry** requires an active author, except that a fresh device may
  author its own `addDevice` entry. Registration requires a current member and
  an id with no historical binding. Only the installation itself may register
  that id, even when its member is an admin. The member's current-key signature
  authenticating the fresh device is checked outside the model. Other entries
  still require an active device and the supplied authority policy.
- **A part counts** when its device is registered and not removed, its member
  is a member, the part was made under that member's current key generation,
  the device may write to `A`, and the part's version is one `A` has reached.
- **A raise or reset of `A`** requires a matching announced preparation and
  audience authority. Its coverage must include every earlier announcement
  (C6) and the last raise or reset. A raise must increase the version.
- **Every announcement of `A`** covers less than its own slot, never decreases
  announced coverage and covers the last raise or reset (the version rule).

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
  the genuine slot `i`. If the floor has passed `i`, the bytes do not settle the
  claim: use the snapshot's per-device claim number
  ([settlement](#settling-a-claim-whose-slot-was-deleted)) before deciding to claim again.

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

## Authored causal past

[`Causality`](sequence/CovenSequence/Causality.lean) records each write's
original device, number, timestamp, schema, bytes and `readThrough`.

- `authored_readThrough`, `catchup_preserves_originals`: commit records the
  applied slot; subsequent catch-up preserves every queued original.
- `ordered_application_causal`, `past_transitive`, `receivers_agree`: under
  number-ordered publication and monotone authored reads, strict slot and
  within-slot write order consumes every cause first. Receivers agree on
  exactly the authored prefix plus own predecessors, including predecessors
  outside that prefix. A later claim does not add intervening remote writes.

## The removal cut-off

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

These are permanent abstract claims. Unique values that can be released use
[`LiveClaims`](sequence/CovenSequence/LiveClaims.lean) instead:

- `first_live_claim`, `winner_counted_setter`, `within_slot`: choose among
  merged live rows left by the first removal pass, by the greatest
  `(slot, write number)` of each value's current input setters, then by
  primary-key rank. Each selected claim is a counted setter. Expressions and
  partial predicates supply their input setters at this boundary.
- `deleted_releases`, `first_after_release`: deletion or removal before
  uniqueness releases the claim. The first claim after release wins when no
  earlier live claimant survives. `older_loser_returns` checks why that
  qualification is necessary: claims 1 and 2 precede deletion of 1; 2 returns
  ahead of a new claim 3. This agrees with §8.5's returning-loser rule.
- `readers_agree`: applying the selector to sound sequence readers' merged
  rows gives the same winner. Later removal does not rerun uniqueness.

[`Owners`](sequence/CovenSequence/Owners.lean):

- `selected_creation`, `lower_write_number_wins`: counted generation-zero
  creations select by slot and then write number before permission filtering.
- `later_claim_preserves_creator`, `move_preserves_owner`, `retained_creator`:
  subsequent claims, updates, deletion, reinsertion and audience moves retain
  the selected original write and its authenticated member. The move's slot
  does not become the creation slot.

## Authority and roles

[`Authority`](sequence/CovenSequence/Authority.lean) specializes admission
without changing entry effects or the ordered ledger.

- `every_prefix_has_admin`, `last_admin_refused`: from the creation binding
  containing an admin, every applied prefix retains one; a candidate effect
  leaving none is refused.
- `applied_membership_authorized`: each applied add, remove or role change
  was authored by an active admin in the preceding slot state.
- `store_reset_authorized`: a store reset requires an admin in that state;
  circle resets and schema raises retain their own audience authority policy.

[`Registration`](sequence/CovenSequence/Registration.lean) proves the device
rules independently of the supplied authority policy:

- `self_registration_valid`, `self_registration_applied`: a fresh device can
  register itself to a current member when that policy authorizes the entry;
  no previously active device of the member is required.
- `registration_self_authored`: every applied registration is authored by the
  device it registers, for every authority policy.
- `registration_at_most_once`: two applied registrations of one id have the
  same positive slot number. `registration_never_repeated` and
  `initial_registration_never_repeated` also cover subsequent attempts after
  removal and ids already bound by store creation. Refused attempts do not
  reserve an id; historical bindings never disappear.
- `unregistered_unremoved`: starting without removed unregistered ids, every
  prefix preserves that property. A fresh id therefore has no removal to undo.

[`SequenceApply.member_without_active_device_can_register`](sequence/SequenceApply.lean)
specializes this to the runner's per-member policy: after any prefix, a current
member with no active device can register a fresh one. It becomes active, while
all of that member's previously registered devices stay inactive. The initial
ledger must contain an admin; no active admin device is required.

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
   read; the slot's metadata, sealed under the store key and readable by every
   member, says they are not `A`'s. The provider cannot read that metadata.
3. **Raise or reset:** it loads the matching prepared body. A raise replays
   buffered slots after coverage, checked against the floor; a reset replaces
   rows without replaying that suffix.
4. **Reload:** with `A`'s key, it reloads from the floor's snapshot for `A` when
   that covers more than it applied.

[`Retention`](sequence/CovenSequence/Retention.lean) models what happens to
storage meanwhile:

- **Claims:** a claim of the next slot, its preparation body uploaded before
  announcement and still present when activated.
- **Stale creates:** a stale create of an earlier slot, which is stored only if
  that slot was deleted.
- **Floor write:** retention writes the floor record conditionally and never
  backwards. Each audience's snapshot is validly announced with its body
  stored, and the first kept slot is covered by every audience's snapshot.
- **Deletion:** retention then deletes slots below the first kept slot, and
  bodies no floor names and no raise or reset above the floor needs.
- **Uploads:** a body uploaded to a fresh path.

There is no separate step that advertises a floor before it takes effect. The
reader's floor read after its fetch, and the claimant's after its claim, are
what make one conditional floor write safe.

`Good` is what these steps keep true:

- at or above the first kept slot, records are genuine;
- each audience's floor snapshot was validly announced, and its body is the
  state it covers;
- a valid boundary's body, when present, is its prepared rows, and it stays
  present while the floor has not covered the boundary.

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

**The coverage argument.** The proof of `read_progress` needs every valid raise or reset of `A`
above the floor's coverage to cover the floor's snapshot. C6 gives this for a
snapshot announced before the entry. The version rule gives it for one
announced after: such a snapshot covers the entry. So no retention hold is
needed.

- `ReaderExamples.slot4_refused`: a snapshot covering slot 2, announced at slot
  4 after a raise at slot 3, meets C6 and every other condition, and the
  version rule refuses it. A device can produce it: it captures at slot 2, the
  raise takes slot 3, and its refused claim re-validates and claims slot 4.
- `ReaderExamples.stale_floor_strands_joiner` is a checked counterexample. A
  floor naming that snapshot has a true body and genuine kept slots. A joiner
  with the key still never moves past slot 2 while the head is 4, because slot
  3 is a valid raise whose replay needs the deleted slot 2.

[`Preparations`](sequence/CovenSequence/Preparations.lean):

- `ordinary_covers_boundary`: an ordinary announcement covers every preceding
  applied raise and reset. The generalized `read_progress` and `reaches_head`
  prove progress with both kinds under the same floor rule.
  `ordinary_matches_schema_reset` proves the captured schema and reset
  equal the state immediately before announcement.
- `announcement_uploaded`, `activation_requires_preparation`: upload precedes
  the announcement; activation requires an earlier matching counted
  announcement. `activated_is_usable` admits its coverage only after activation.
- `preparation_no_effect`, `abandoned_supplies_no_coverage`,
  `abandoned_not_usable`: a separately announced preparation that never takes
  effect changes neither rows nor schema/reset and supplies no usable snapshot.
- `reset_replaces`: reset loads the chosen rows without replaying pre-reset
  writes after the capture. Body honesty and the batch reset filter are
  explicit model boundaries.

[`Deadline`](sequence/CovenSequence/Deadline.lean):

- `bounded_retention`: successful snapshot service U, grace G, maintenance
  spacing P (including a successful conditional floor write), and deletion
  service D delete a ready prefix within U+G+P+D. Coverage witnesses must stay
  usable through the floor update; no device acknowledgements occur in the model.
- `uncovered_has_no_deadline`: missing coverage in any relevant audience
  prevents the retention gate at every time. `cut_delete_allowed` connects
  the service's floor bound to the concrete storage deletion step.

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

## Queued writes during reload and raises

[`QueueReload`](sequence/CovenSequence/QueueReload.lean) keeps published rows
separate from the local queue of full originals. A successful reload installs
all followed audiences at one target and rebuilds from the queue present at
installation, including local commits made while loading.

- `queued_originals_preserved`, `write_during_reload`: installation preserves
  every original field and includes writes committed during loading with their
  original applied slot. Ineligible originals stay queued for publication.
- `queued_exactly_once`, `no_queued_duplicate`, `covered_not_reapplied`: each
  eligible original enters the rebuilding fold once by write number; consumed
  writes are excluded. `commit_numbered` and `during_numbered` preserve the
  counter invariant that establishes unique numbers.
- `install_twice`: repeating installation rebuilds from the published base;
  it cannot apply the queue over its previous local effects.
- `reload_sound`, `new_circle_keeps_queue`, `audiences_can_load`: every followed
  audience supplies its own sound loaded prefix, including newly readable
  circles; each retains eligible unclaimed originals. `reaches_target` in
  `Reader` proves loading can stop at a fixed target below the current head.
- `ordinary_commit_allowed`, `affected_commit_refused`: ordinary reload admits
  local commits; reset, breaking-raise and newly-readable reloads block writes
  into the affected audience. `during_preserves_queue` proves admitted commits
  only extend the queue and preserve the previously visible applied slot.

[`RaiseQueue`](sequence/CovenSequence/RaiseQueue.lean) represents capture,
confirmation, body upload, preparation announcement and raise activation as
separate persisted states. Confirmation means the captured writes slot has
landed under the existing claim protocol; it precedes the raise slot (§17.1).

- `captured_prefix`, `later_writes_do_not_delay`, `confirm_captured`: publication
  selects the frozen prefix through the marker, regardless of later appends or
  their eligibility, then removes exactly that prefix from the local queue.
- `preparation_waits_for_confirmation`, `activation_waits_for_announcement`:
  preparation upload requires confirmed originals, and activation requires the
  uploaded body's announcement. `raise_without_waiting` reaches activation
  with any later queue; `tail_waits` keeps that queue waiting in between.
- `originals_preserved`, `confirmation_not_repeated`: the combined published
  and queued originals retain their order and all fields through the protocol,
  and confirmation cannot publish the prefix twice.
- `recapture_includes_waiting`: a later breaking migration captures intervening
  writes and both markers, leaving only writes after its marker waiting.
- `late_original_converted`, `reload_matches_receiver`, `conversion_agreement`:
  interpretation uses the original schema and the applied version. Queue
  replay agrees with the actual next sequence writes slot under the same
  admission/reset gates; conversion never rewrites the publication queue.

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

This operation model retains sequence slots. Its results are safety properties,
not a promise that storage requests or contention eventually succeed. Settlement
after slot deletion is proved separately below.

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
Before each create, the journal records the claim number, slot and exact bytes. On
reopen, the device reads the slot and compares bytes. If the floor has passed
the slot, it settles from the floor's snapshot instead. Each snapshot records,
per device, the last claim number it includes, valid or refused, and a claim
landed exactly when that number covers it. Every slot kind uses this rule.
The device settles its current claim before starting another; contention retries
retain that number, and new claims take increasing positive numbers.

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
- `lands_once_never_lost`: once the recorded slot is occupied, retaining a landed
  claim or successfully claiming again exactly when settlement says `notLanded`
  leaves the claim at exactly one slot. The theorem assumes the new claim succeeds;
  it does not establish eventual success under contention.

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

## JSON runner

Build with `cd spec/proofs/sequence && lake build`; the executable is
`.lake/build/bin/sequenceRunner`. It reads one JSON object to EOF on stdin and
prints one JSON answer. Invalid input reports an error on stderr and exits 1.
`SequenceTests.lean` checks cases through the JSON entry point during the build.
All fields below are required; identifiers are natural numbers, members lie in
`0..member_count-1`, and slots, claim numbers and write numbers are positive.

**`settle`** calls `Settle.settle`. Bytes are opaque strings compared exactly;
`current: null` means absent. `last_claims` contains `[device, number]` pairs;
an omitted device has never claimed (number zero). The floor is read after the
slot. Below `first_kept`, only the snapshot record decides, even if bytes match.

```json
{"kind":"settle","recorded":{"device":0,"number":2,"slot":4,"bytes":"claim-2"},"current":"claim-2","floor":{"first_kept":5,"last_claims":[[0,1]]}}
```

Answer: `"notLanded"`; the other answers are `"landed"` and `"retry"`.

**`read`** calls `Reader.checkedSlots`, whose next-slot check supplies the
buffer and floor premises of `ReadStep.apply`. It returns the consecutive
fetched prefix after `applied`, stopping at the first gap, irrespective of
fetch order. If the floor has passed the next slot it requests reload and
applies nothing. This projects ordinary application with the audience key;
key waits, prepared-body loads and snapshot contents are outside this case.

```json
{"kind":"read","applied":1,"fetched":[3,1,2,5],"first_kept":2}
```

Answer: `{"reload":false,"apply":[2,3]}`.

**`apply`** evaluates `ledgerAt`, `audAt` and `rowsAt` at every prefix, using
`Authority.withRoles` and `LiveClaims.winner`. The initial membership must
contain an admin; initial devices refer to active members. Each audience gives
its starting `version` and the devices allowed to write to it. The runner uses
§9's per-member authority: members register their own fresh devices, remove
their own devices and replace their own keys. Admins may also remove another
member's device or replace that member's keys; only admins add or remove members
and change roles. Registration must be authored by the fresh device itself,
including for admins. Other admin entries need an active author. Membership
must retain an admin. Conversion is the identity; audience writer lists and
conversion are fixture parameters, not additional store rules.

Slots occupy positions `1..slots.length`. Each has `device` and `claim`, with
strictly increasing claim numbers per device. An admin slot is
`{"kind":"admin","device":0,"claim":1,"entry":{"kind":"removeDevice","device":1}}`.
Entry kinds are `addMember` (`member`, `role`), `removeMember` (`member`),
`changeRole` (`member`, `role`), `addDevice` (`device`, `member`), `removeDevice`
(`device`), and `replaceKey` (`member`); roles are `"admin"` or `"member"`.
For example, device 2 registers itself to current member 1 with
`{"kind":"admin","device":2,"claim":1,"entry":{"kind":"addDevice","device":2,"member":1}}`.
Device 2 must have no existing binding, even a removed one. It may register when
member 1 has no active devices. The enclosing `device` must equal the entry's
`device`; another author is consumed as a refused admin slot. The JSON input
supplies already-authenticated entries: it carries no signatures, and the
current member-key signature proving device 2 speaks for member 1 is assumed
verified before this call. A refused registration records its claim number but
does not bind the id; a subsequent valid self-registration can succeed.
A writes slot carries `key_gen` and at most one part per declared audience:

```json
{"kind":"apply","member_count":1,
 "initial":{"members":[{"member":0,"role":"admin","key_gen":0}],"devices":[{"device":0,"member":0}]},
 "audiences":[{"audience":0,"version":1,"writers":[0]}],
 "slots":[{"kind":"writes","device":0,"claim":1,"key_gen":0,"parts":[
   {"audience":0,"version":1,"unique_claims":[7],"owner_claims":[9],"writes":[1],
    "rows":[{"row":9,"live":true,"value":7,"inputs":[[1,1]]}]}]}]}
```

`unique_claims` and `owner_claims` are separate permanent first-slot claim
namespaces. `writes` lists original write numbers in that part. `rows` supplies
complete replacements for affected merged rows **after the first removal pass,
before uniqueness**: `row` is also its primary-key rank, `live` is presence,
`value` is a unique value or null, and `inputs` lists `[slot, write-number]`
setters for all of that value's inputs. Setters must name existing writes in
the same audience at or before the update; refused setters are excluded by
`LiveClaims`. Empty inputs cannot claim. This instantiates the model's abstract
merge boundary; it does not implement row conflict resolution. Losing rows stay
live, and setting `live:false` releases the claim. No uniqueness pass deletes
those rows from the model's merge input.

The answer has `ledger`, `audiences`, and `slots`; each item of `slots` has
`slot`, `ledger`, and `audiences` for that prefix. The ledger contains `members`
(`[member, role]`), all registered `devices` (`[device, member]`, including
removed devices), `removed` device ids, `key_generations` (`[member, generation]`
for the whole member universe), nonzero `last_claims` (`[device, number]`), and
`refused` admin slot numbers. Each audience contains `audience`, `version`,
`unique_claims` and `owner_claims` (`[key, first-slot]`), `live_winners`
(`[value, row-or-null]` for every value retained in merged rows), `rows` in the
input row shape, and `refused` part slot numbers. Tables are sorted by id;
refusals are ascending. The example yields `last_claims:[[0,1]]`,
`unique_claims:[[7,1]]`, `owner_claims:[[9,1]]`, `live_winners:[[7,9]]`, and no
refusals, both at the final state and slot 1. Empty `slots` returns the initial
state and an empty prefix list.

**`past`** decides `Causality.Past` for each pair of supplied writes. Each write
has `author` (device), `number`, containing `slot`, and `read_through` at
authorship. Input must respect `Causality.Ordered`: publication follows the
read, own-number order never reverses slots, and authored reads never decrease.
A slot has one author; a device/write-number pair is unique. Output lists
zero-based input indices, preserving input order, independently of slot order:

```json
{"kind":"past","writes":[{"author":0,"number":1,"slot":1,"read_through":0},{"author":1,"number":1,"slot":2,"read_through":1},{"author":0,"number":2,"slot":3,"read_through":0}]}
```

Answer: `[[],[0],[0]]`. The last write reads its own predecessor but not the
intervening remote write. Only supplied writes can appear in an answer.

## Assumptions and what is not modelled

- Entry signatures are checked outside the model against the member's current
  signing key before the slot. In particular, a fresh device's self-registration
  is authenticated as speaking for the member named by `addDevice`; naming a
  member alone does not prove that authority. The JSON runner assumes this
  check has succeeded and does not model signatures. Store creation supplies
  the initial member and device bindings.
- Snapshot bodies are honest: ordinary bodies contain the covered state;
  raise bodies contain migrated rows; reset bodies contain the chosen rows.
  Floor reload uses ordinary snapshots; prepared bodies are consumed at their
  matching entries. Direct floor loading of a prepared body is not modelled.
- Admin slots and key copies are readable by every member device. So are a
  slot's device, claim number and each part's audience: they are sealed under
  the store key, which every member holds, and the provider sees only the slot
  kind and key-copy routing.
- Unique values and owner rows are per audience; uniqueness across audiences is
  not modelled.
- Ordinary receive parts at a version above their audience's are refused.
  Captured publication and migration effects are separate in `RaiseQueue`;
  batch decoding into individual original writes is not modelled. A device whose app
  cannot apply a slot stops before it; that stop is not modelled.
- Reload models changed readable audiences and their local write gates.
  Key delivery, circle invitation and the policy choosing the followed set
  remain abstract; a newly readable audience must load its own prefix.
- `Model` takes each slot's claim number as given (`Rules.number`); that a
  device's numbers increase is a hypothesis of `settle_from_floor`, not
  derived from a device's counter.
- Not modelled:
  - concrete authority policies beyond membership, role changes, the runner's
    device/key actions and store resets;
  - Drive's head file and its compare-and-swap;
  - orphaned batch objects and provider request timing;
  - joiners reading slots before they hold the store key;
  - the merge's internals, source-verdict calculation and converter callbacks,
    which the merge and queue packages cover. `Inputs` extracts merged
    uniqueness setters; queue admission and the core batch reset filter are
    parameters. The local queue makes the authored-read reset gate explicit.
