# One store sequence

The Lean package in [`sequence/`](sequence/) backs the normative sequence rules:
[claims and application (§5)](../coven.md#5-the-sequence),
[unique claims (§9.2)](../coven.md#92-keys-and-unique-values),
[owner selection (§9.6)](../coven.md#96-owner-tables),
[admin prefixes (§10)](../coven.md#10-members-roles-and-devices),
[cut-offs (§14.1)](../coven.md#141-the-cut-off),
[audience keys and revocation (§12)](../coven.md#12-keys-and-encryption),
[audience reads (§15.6)](../coven.md#156-writes-into-circles),
[a circle's name, members and deletion (§15.5, §15.8, §15.9)](../coven.md#155-running-a-circle),
[snapshots and retention (§16)](../coven.md#16-snapshots-the-floor-and-deleting-history), and
[operation journals (§11)](../coven.md#11-admin-operations), in the order described by
[a sync pass (§6)](../coven.md#6-a-sync-pass). Each result has the assumptions and limits
below.
It uses Lean 4.34.1 and its standard library,
without additional axioms or unfinished proofs. All named results are audited by
`CovenSequence/Axioms.lean`, and `scripts/check.sh` builds the package and runs
the audit with the other proofs.

Slot `<store>/seq/<n>` is claimed create-once. It holds an admin entry, one
device's batch of writes for a sync pass (one part per audience it writes to),
a key copy, a snapshot announcement for one audience ("audience `A` as of slot
`c`, at schema version `v`, is at path `p`"), a preparation announcement, or a schema raise or reset
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
- **Cancel only before the point of no return.** A cancel after the claim is
  sent strands a claim that lands later ([Operations](#operations)).
- **Settle from the per-device claim number and slot digest.** Settling a deleted slot's claim
  from the snapshot's state alone loses or duplicates writes, and misreports
  a completed admin operation. A number without a digest can confirm another copy's
  bytes after deletion
  ([Settling a claim](#settling-a-claim-whose-slot-was-deleted)).

## The model

[`Model`](sequence/CovenSequence/Model.lean) abstracts devices,
members, unique keys, paths, batches, rows, audiences, and admin operations
other than membership, roles and the store name. Circle renames, access and
circle membership use `aux`. Authority for membership, store renames, store
resets, circle actions and provider access is concrete in `Authority`. A
circle's own entries are concrete in a separate model, `CircleLifecycle`
([A circle's name, members and deletion](#a-circles-name-members-and-deletion)).
Other rules remain abstract: who may write to an audience, the effect of other
operations, the merge, the converter of old-version parts, and the database
part of a migration.

For each audience `A`, a device's state has three parts:

- **The ledger** holds what admin slots and key copies decide, the same for
  every audience. That is the current store name, active members and roles,
  retired member ids, devices and their removal, member key generations, `aux`,
  key copies and refused admin slots. It also holds, per
  device, the greatest included claim number and the SHA-256 of that slot's
  complete bytes. Later slots replace the digest on an equal number; lower
  numbers preserve the existing record. A device with no included claim has
  `none`. Historical write permission is retained at each applied prefix.
- **`A`'s audience ledger** holds the first slot that claimed each unique value
  or owner row of `A`, `A`'s announced snapshots and preparations, its schema version,
  its last raise or reset, its last reset, and its refused parts.
- **`A`'s rows** come from the abstract merge. A part at an older version is
  converted to `A`'s version. A valid raise of `A` at slot `r` covering `c`
  loads the raiser's migrated rows as of `c`. It then replays the counted `A`
  parts of slots `c+1` to `r-1`, converted to the new version. A reset loads
  its chosen rows and discards that suffix. Subsequent writes pass the
  authored-read reset gate before conversion.

Only admin slots and key copies change the ledger, besides the claim evidence
and preceding write permission every slot records. Only slots with a part or
announcement for `A` change `A`'s other two parts. A slot or part invalid at
its position is refused: it is consumed, and only its refusal is recorded.
Removing a member retires its id permanently and removes every device registered
to it. Re-inviting that person requires a fresh member id. An active member
whose devices were all removed keeps its member id when registering a fresh device.

These are the validity rules:

- **An admin entry** requires an active author, except that a fresh device may
  author its own `addDevice` entry. Registration requires a current member and
  an id with no historical binding. Only the installation itself may register
  that id, even when its member is an admin. A registration may name an earlier
  device of the same member that it replaces, as a reset or a restore in place
  does; that device is removed by it. The member's current-key signature
  authenticating the fresh device is checked outside the model. Other entries
  still require an active device and the supplied authority policy. Adding a
  member requires an id that is neither active nor retired.
- **A part counts** when its device is registered and not removed, its member
  is a member, the part was made under that member's current key generation,
  the device could write to `A` at the originals' `readThrough`, that prefix
  precedes the landing slot, and the part's version is one `A` has reached.
- **A raise or reset of `A`** requires a matching announced preparation and
  audience authority. Its coverage must include every earlier announcement
  (C6) and the last raise or reset. A raise must increase the version.
- **Every announcement of `A`** covers less than its own slot, never decreases
  announced coverage and covers the last raise or reset (the version rule). It
  names the schema version its body is at, which `A` has reached; a raise's
  preparation may name at most the version it raises to. Coven names the
  breaking version (coven.md §16.3), as the model has no additions. `A` starts
  at its initial ledger's version, its base: zero for the store, and for a
  circle the breaking version its creation entry recorded.

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
- `applied_attempt_won`: a claimant that applied its own attempt before
  claiming (store creation's slot 1, §10.3) and concludes `won` finds storage
  holding exactly the applied bytes forever, so its applied slot is every
  reader's. One that concludes `lost` finds other bytes there forever, so its
  applied slot never matches the store's.
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
  claim: use the snapshot's per-device claim number and digest
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

## Authored circle permission

[`Authorship`](sequence/CovenSequence/Authorship.lean) proves
`write_history_prefix` and `authored_permission_eq`: retained permissions are
exactly those at the author's applied prefix. `write_permission_at_authored_prefix`
checks that permission while applying the current store, device and key cut-offs.
`SequenceApply.AuthorshipExample` checks Ben's unread removal from Gifts: his
old original counts, one authored after reading removal is refused, and device
removal still cuts off both. `sending_and_apply_agree` connects the accepted
old original to `Keys.sendingKey`'s departed-circle choice.

## The removal cut-off

[`Cutoff`](sequence/CovenSequence/Cutoff.lean):

- `cutoff`: a batch at slot `k+1` is refused in every audience it has a part
  for when its device was cut off at an earlier applied slot `j`. The cut-off
  is one of:
  - the device's removal;
  - a registration of another installation in its place;
  - the removal of the member it was registered to;
  - the replacement of that member's key at or above the generation the batch
    was made under.

  Each audience only records the refusal, so no unique value or owner row is
  taken and its rows are unchanged. The ledger records the slot's claim
  number, digest and preceding write permission. Every device agrees by
  `prefix_determines_state`.

- `cut_counts_false` and `own_writes_never_refused`: once slot `j` cuts a
  device off, the ledger before every later slot says its batches don't count,
  so a device that applied slot `j` stops sending. A device that claims slot
  `k+1` only after applying `1` to `k` and sends only while its ledger counts it
  has no cut-off at any slot `j ≤ k`: its own writes are never refused by its
  own cut-off. A refused write on a device is another device's, and what a
  removal or a replacement costs the stopped device is its unsent queue.

A batch is cut off when it was "made under a replaced key". A batch that the
member's other devices make under the new key counts.

The model's ledger has one `removed` flag for a device removed and one
replaced. Rust keeps them apart, as the stop each device reports differs
(coven.md §14.1), and records a later removal of a replaced device as a
change; the model refuses that removal, which changes nothing either device
counts. The report a refused batch makes, naming the entry that decided it,
is Rust's (`WritesRefused`).

## Audience keys and revocation

[`Keys`](sequence/CovenSequence/Keys.lean) projects the same authenticated
sequence into introductions, copies and other slots. An introduction is
projected only from an applied creation or rotation. Every authenticated copy
retains its exact audience, key id, recipient and sealing credential, including
an unbound candidate's copy. `credentials` uses the sequence ledger's `member`
and `keyGen` fields, with circle membership supplied by the audience policy. Custody is
separate from both authorization and introduction.

- `introduction_slot`, `copy_slot`, `publish_prefix`: every retained event has
  its actual positive sequence position; the finite runner and sequence fold
  agree. `accepted_from_introduction` excludes acceptance before introduction.
  `introduction_accepts` accepts matching tentative material at that slot;
  missing custody or conflicting commitments do not establish acceptance.
- `copy_order_agrees`: `openCopy` opens a copy as §12.1 does. With the key's
  introduction applied, material that doesn't match its commitment stops the
  device; without it, the key is held as tentative and not accepted. On a
  device holding nothing under a key not yet introduced, opening the copy and
  then, once the introduction applies, opening it again ends as applying the
  introduction first does: the key held and accepted when its material
  matches, a stop when it doesn't.
- `needed_complete`, `needed_after_none`, `needed_exposes_nothing`: `needed`
  lists the copies an audience still needs, of every key introduced for it to
  each current member's current sealing key, leaving out copies already in the
  sequence. Once they are published every current member has every older key,
  none is needed, so a retry sends none, and they expose no key, since each
  goes to a current sealing key.
- `routed_exposure`, `removed_exposure`, `retired_credential_exposure`: every
  key routed to any credential retained by an excluded member/device is
  exposed. Opening or deleting a local copy is irrelevant to that conclusion.
  Exposure is computed from routing and current membership, including
  candidates never bound and credentials retired while their member stays.
- `first_newest_usable`, `first_not_exposed`: a new first attempt selects the
  greatest introduction slot among authorized, accepted, held, unexposed keys.
  The theorem does not assume key ids or copy slots order introductions.
  `metadata_newest_held` selects the newest accepted held store key regardless
  of exposure. `sending_current` connects this selection to Rust's selector;
  its departed-circle branch implements §15.8 for eligible old originals.
- `removal_publishable`, `rotation_publishable`: with one accepted store key
  held, exposure creates no missing encryption dependency. Removal needs only
  the metadata key. Rotation additionally needs its generated material and
  the recipients' current public credentials; no old circle key or recipient
  private key is needed. These are sealing-dependency results for authorized,
  caught-up writers, not promises of custody unlock, provider success or progress
  past a blocked slot. `rotation_prefix_metadata` preserves that metadata key
  before every copy and before the introduction itself.
- `routes_current`, `routes_complete`, `rotation_effect`: the rotation plan
  copies the generated key to each listed current audience member's exact
  credential, then introduces it in the following slot. The member list must
  include every current audience member. Store creation's slot carries the
  creator's copy of its first key, so the copy stands in history with the
  introduction, as an admission's do.
- `rotation_unexposed`, `rotation_usable`, `rotation_selected`: when the key
  is fresh even among tentative copies, membership remains fixed during those
  slots, and the writer holds its generated material, completed rotation makes
  it the selected key. All older introduction slots precede publication.
  `rotation_preserves_old` retains historical acceptance.
- `exposed_header_view`, `metadata_reveals_no_protected_content`,
  `store_key_does_not_open_box`: opening the outer store envelope reveals its
  complete metadata, including administrative operation contents. With public
  routing and lengths fixed, varying separately sealed content whose key is
  unknown does not change the observation. An exposed store key also opens
  store data sealed with that key; it does not supply circle keys or recipient
  private credentials. Encryption and sealed boxes are ideal observation
  functions, not cryptographic implementations or security reductions.
  Distinct key identities represent independently generated, non-aliasing
  secrets under §3's random-generation assumption; hash tokens check material
  under a named key. The model does not prove random-key collision bounds.
- `first_attempt_confidential`, `revocation`, `rotation_revokes`: at the applied
  prefix used for selection, no exposed key opens newly protected content.
  Once that prefix excludes every credential retained by the removed/stolen
  device, none of its routed keys can be selected. Rotation constructs such a
  key; subsequent selections must re-establish the condition at each catch-up.
  Secrets acquired outside the modeled generation/copy protocol are excluded.
- `revoked_write_no_effect` composes the confidentiality claim with `cutoff`:
  late writes by a removed device/member or under a replaced credential have
  no row, unique-value or owner effects, even if their ciphertext still opens.
  Together with `device_stops` and `stopped_stays`, these state §2.5's Revocation
  claim: a blocked reader cannot claim knowledge of a later removal.

[`KeysExamples`](sequence/CovenSequence/KeysExamples.lean) checks the limits:
`retry_readable_after_rotation` preserves old attempted bytes;
`late_copy_exposes_selected_key` permits disclosure by a copy landing after
the selecting read; `departed_circle_original_can_use_exposed_key` preserves
§15.8's first send of previously authored circle edits. An unbound recipient
still exposes a key (`unbound_recipient_exposes_key`). These are counterexamples
to stronger claims, not discrepancies with Rust. `device_removal_keeps_credentials`
proves that removing an installation does not retire its member's shared
secret: excluding a stolen device's retained member credential requires
replacement (or removing that member). Rotation cannot erase previously
downloaded ciphertext or secrets.

[`KeyCustody`](sequence/CovenSequence/KeyCustody.lean) separates the journal's
key identity and public commitment from secret material in custody, for an
audience key and for a member's new pair alike.
`saved_before_prepared` requires saved material before recording its commitment;
`restart_reuses_saved` keeps that material on restart, and `save_keeps_other`
preserves other keys. `journal_depends_only_on_commitment` makes the journal's
observation independent of secrets with the same commitment. Rust tests also
scan the physical SQLite files, whose encoding is outside the model.

[`CopyOrder`](sequence/CovenSequence/CopyOrder.lean) asks whether a copy sits
below every slot that needs it, since a reader that reaches a slot it can't open
stops there, and reading in order never reaches a later copy (`keyless_stops`). It sets `Keys`' projection beside a roster of who
reads each audience with which sealing key, as `effects.rs` and
`circle_effects.rs` keep it. `required` is Rust's `required_copies`. An
admission, admitting or recovering a member (§13.2), carries those copies in
its own slot (`carried`), and so does creation; the projection takes a slot's copies from what it
carries, a key-copy slot's one, an admission's or the creation's. A slot introduces a list of keys
(`Keys.Event.introduce`): an entry's one, or creation's store key and first circle keys. `legal` says a slot followed
the rules at its position: any other entry's required copies are in the prefix
before it, which `operation_outcome` gives when validation asks for them, and
every key sealing the slot is introduced. `Follows` is a history of legal slots
after creation, which carries its creator's copy. `CopyPrecedesNeed` is the claim: for every slot after
creation, every key it needs and every member reading that key's audience just
before it, a slot below carries a copy to the member's current sealing key.

- `audience_complete`, `store_copy_precedes_need`, `copy_precedes_need_inside`:
  every key introduced for an audience has a copy to each reader's current
  sealing key, at every prefix from slot 1 on, when each replacement or
  recovery of a reader comes from another reader. That holds of the store, so
  a store key's copy is always first, an admitted member's in its admission;
  for a circle it is the premise of `copy_precedes_need_inside`.
- `admission_carries_own_copy`: an admission or a recovery also makes the
  member's circle, so its slot introduces the circle's key and carries a copy
  to the member's sealing key; the circle's audience is unused before it
  (`circleFree`), and the member alone reads it. The circle's first reader has
  its copy from the slot that makes the circle, and the claims above are
  unchanged.
- `creation_carries_own_copies`: creation makes the circles its creating write asked for, each read
  by the creator alone, so slot 1 introduces the store's first key and each circle's first key and
  carries the creator's copy of every one. A circle's first reader has its copy from the slot that
  makes it, and the claims above hold of the circles' keys from slot 1 on.
- `leave_selects_new_key`: the entry that removes a member from a circle
  introduces the circle's replacement key, and the copies it requires sit below
  it, to the readers who remain (`required`'s `leaveCircle` case; an entry whose
  author is the member removed is legal only when no other reader remains).
  Take a followed history, a slot whose entry removes `m` from circle `a` and
  introduces `key`, `key` unused before it, every copy of it below the slot
  routed to a reader who remains, and a remaining writer holding its material.
  Then `m` no longer reads `a`, `key` is that writer's first-attempt choice at
  the slot, and no copy of `key` is routed to `m` under any sealing key it
  retains. So no circle write after the removal is sealed with a key `m` holds.
  The premises about copies are what the sender follows (`required`); the model
  does not stop another member from sending a copy of an unintroduced key.
- `races_follow`, `races_keep_order`, `stale_rotation_illegal`
  ([`CopyOrderExamples`](sequence/CovenSequence/CopyOrderExamples.lean)): Ben
  rotates the store key while Ana admits Dan, and Dan replaces his keys among
  the next rotation's copies. Each race is one history of `Follows`, not a case
  of the proof. A rotation that kept the routes it first read is not legal.
- `copy_precedes_need_fails` is a checked counterexample: §12.1's recovery of
  Ben by Ana, an admin outside Gifts.
  1. Ana recovers Ben at slot 10, her admission carrying what she holds, the
     store key.
  2. Carol's write chose the Gifts key and lost slot 10. Her device comes back
     after Ana's rotation of the exposed store key, and a pass sends writes
     first, so it lands at slot 15 with the Gifts key of its first attempt.
  3. Carol's pass then sends Ben's copy, at slot 19. Ben reads Gifts at slot 15,
     and no copy of the Gifts key below it is sealed to his new key.
- `early_scan_stops_below_replacement`: the claim names the sealing key current
  at each slot, but a recovered device reads old slots with its new pair. A key
  scan that ended before the copy can't open Carol's write at slot 9 either,
  and a scan after it can.
- `need_introduced`, `stuck_scan_finds_copy`, `owed_complete`: where the claim
  fails, the stopped reader scans the slots after its stop for its copy
  (coven.md §5.4). Every key a slot needs is introduced by that slot, so the
  copy the scan finds is checked against an applied commitment and its key is
  accepted at once. Publishing `needed`, as a member of the audience does in
  its pass, makes the audience complete: each current reader has a copy of
  every key introduced for it. A reader whose pair is current at a complete
  prefix, scanning through it, finds a copy of every key its stop needs.
  The model doesn't say when that member's pass runs; until it does, the
  reader waits.

`Follows` allows more than Rust does, as it asks nothing of fresh sealing keys
or a write's authority, so `store_copy_precedes_need` and
`copy_precedes_need_inside` cover those histories too. The counterexample's slots
are what Rust claims in that order. No Rust differential runs this model. The
floor is left out: `copies` keeps every copy claimed, as a store snapshot does
for the slots a floor has passed.

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
  ahead of a new claim 3. This agrees with §9.2's returning-loser rule.
- `readers_agree`: applying the selector to sound sequence readers' merged
  rows gives the same winner. Later removal does not rerun uniqueness.
- `landing_agrees`, `queued_loss_stands`, `published_before_queued`: a write
  still in a device's queue ranks after every published claim, in
  write-number order. The device holds published claims from the slots it has
  applied only, and its queued writes land after them in number order. So it
  orders every pair of claims it holds as their landed slots will, and the row
  it selects for a value is the row selected once its queued writes land: a
  row created locally that loses its value before it claims a slot still
  loses it after, unless a claim the device doesn't yet hold arrives.

[`Owners`](sequence/CovenSequence/Owners.lean):

- `selected_creation`, `lower_write_number_wins`: counted generation-zero
  creations select by slot and then write number before permission filtering.
- `later_claim_preserves_creator`, `move_preserves_owner`, `retained_creator`:
  subsequent claims, updates, deletion, reinsertion and audience moves retain
  the selected original write and its authenticated member. The move's slot
  does not become the creation slot.
- `provisional_owner_lands`, `published_owner_first`: a creation still in
  the queue is provisional, ranked as a queued claim is. Among the creations
  it holds, a device picks the owner every receiver picks once its queued
  creations land. A published creation it holds owns the key before every
  queued one, so the device checks its queued changes against that owner,
  which later claims never change. Only a key no published creation holds
  has a provisional owner: another device's creation that lands first owns
  it instead.

## Authority and roles

[`Authority`](sequence/CovenSequence/Authority.lean) specializes admission
without changing entry effects or the ordered ledger.

- `every_prefix_has_admin`, `last_admin_refused`: from the creation binding
  containing an admin, every applied prefix retains one; a candidate effect
  leaving none is refused.
- `applied_membership_authorized`: each applied add, remove or role change
  was authored by an active admin in the preceding slot state.
- `applied_rename_authorized`, `non_admin_rename_refused`: a store rename
  requires an admin in the preceding state; refusal leaves the name unchanged.
- `name_is_latest_applied`: the ledger name equals the creation name followed
  by applied renames in slot order. `name_prefix` proves equal prefixes give
  equal names, including prefixes with refused renames.
- `store_reset_authorized`: a store reset requires an admin in that state;
  circle resets and schema raises retain their own audience authority policy.
- `applied_circle_action_by_member`, `outside_circle_refused`: under
  `withPolicy`, an applied circle action was made by a device whose member
  belonged to the circle in the preceding state, and an admin outside it is
  refused.
- `applied_access_authorized`, `other_kind_refused`: an applied access change
  was made by its own member, or by an admin for an S3 key, and sets the kind
  of access Set up storage recorded in the state before its slot; one of the
  other kind is refused. The circle an action names, circle membership in
  `aux`, the access an entry sets, the kind recorded so far and the operations
  that are Set up storage or invite shares are supplied by `Policy`.
- `access_before_storage_refused`, `share_before_storage_refused`,
  `admission_before_storage_refused`: while no kind is recorded, an access
  change, an invite share and an admission are refused, whoever makes them.
  The admission rule, `withAdmissionAccess`, sits over the roles because
  admissions need an admin before any policy is asked.
- `storage_by_admin`, `storage_set_once`, `second_storage_refused`: an applied
  Set up storage had an admin author and found no recorded kind in the state
  before its slot, and a state that has one refuses another.
  `later_storage_refused` closes it over a sequence: given `Coherent`, which
  says an operation `setUp` names records its kind where none was and no
  operation changes a recorded kind, no Set up storage applies after an
  applied one.

[`Registration`](sequence/CovenSequence/Registration.lean) proves the device
rules independently of the supplied authority policy:

- `self_registration_valid`, `self_registration_applied`: a fresh device can
  register itself to a current member when that policy authorizes the entry;
  no previously active device of the member is required.
- `registration_self_authored`: every applied registration is authored by the
  device it registers, for every authority policy.
- `admission_registers`: an applied admission, admitting or recovering a
  member, registers the device it names for that member, and its author is
  another device: a joining person's first device is the one the admission
  registers (§13.2).
- `registration_at_most_once`: two applied entries registering one id, an
  installation's own registration or an admission, have the same positive slot
  number. `registration_never_repeated` and
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

[`Membership`](sequence/CovenSequence/Membership.lean) proves permanent retirement
independently of the authority policy:

- `retired_member_refused`: no valid admission reuses a retired member id.
- `step_retired_member`, `retired_member_stays`: retirement and absence hold
  together after every subsequent slot and at every later prefix.
- `removed_member_never_active`: once a positive slot applies a member removal,
  that id is retired and absent at every later prefix, including after attempted
  additions, role changes, registrations and key replacements.

The ledger retains retirement history, and an admission checks it regardless
of the supplied authority policy. The runner examples refuse both reuse and a
fresh device registration for the retired id; separate examples admit a fresh
member id and preserve recovery for an active member without devices.

## A circle's name, members and deletion

[`CircleLifecycle`](sequence/CovenSequence/CircleLifecycle.lean) decides the
admin entries that run a circle, and the store's removal of a member, as
Rust's `checked_change` decides them (`crates/coven-sync/src/effects.rs`,
whose circle effects are in `circle_effects.rs`). It
stands alone rather than filling `Model`'s `aux`. Its state is each member
id's standing (never added, a member, removed), each circle's name and
members, and the slot that deleted it. An entry's check refuses it, finds it
already in place, or gives the state its slot commits; the refusals are
`NotAllowed` and `TargetGone` naming the circle or the member, in the order
`checked_change` makes them. A removal's admin authority is `Authority`'s, and
the author's device acting for the author is §10's check, so the model takes
the author's standing alone. A removal from a circle introduces the circle's new
key (§15.8), which its author makes, so a member removing themselves while
others stay is refused as `NotAllowed` (`mayLeave`).

- `name_is_latest_rename`: a circle's name is its creation's, then each valid
  rename's in slot order. A refused rename, by someone outside the circle or
  after its deletion, leaves the name.
- `deleted_target_refused`: once a circle is deleted, a rename of it, an
  addition to it or a removal from it by an active member is refused because
  its target is gone, as is making a circle under its id; a second deletion
  changes nothing.
- `live_has_member`: in every state the entries reach, a live circle has a
  member, so a circle with none is deleted.
- `leaving_deletes`, `member_removal_deletes`: a circle is deleted at the slot
  that takes its last member out, whether the member left it or was removed
  from the store. The circle's members before the slot decide it: it is
  deleted exactly when the member taken out was its only one.
- `deleted_at_slot`: a slot records no deletion but its own.
- `deleted_stays`: once deleted, a circle keeps its record, with no members,
  and its deletion slot at every later prefix, so no later entry adds a member
  to it or takes the deletion back.
- `admission_makes_circle`, `used_circle_refuses_admission`,
  `admission_repeat_unchanged`, `recovery_keeps_old_circles`: an admission of
  a new person or a recovery of a member makes the member's circle in its own
  slot, with the member alone in it; an id already used, live or deleted,
  refuses the entry; the same entry again changes nothing once the circle holds
  the member under that name; and a recovery leaves the member's older
  circles as they were. The Rust differential runs `admit` and `recover`
  histories through the same reader.

These hold for every start whose circles keep `Wf`: a deleted circle was made
and has no members, and a live one has a member. A store with no circles
keeps it.

A write into a circle is refused on the writing device with `NotInCircle`
when its member isn't in the circle or the circle is absent from the
membership the device has applied, and with `DeletedCircle` when that
membership deleted it (§15.6, §15.8, §15.9). These are Rust checks of the
device's committed membership tables in the write's transaction, not
modelled: `CircleLifecycle` proves what that membership is after each
applied slot, and `write_permission_at_authored_prefix` that applying the
write judges it at its author's applied prefix. `coven-database`'s write
tests check both refusals.

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
  stored, and the first kept slot is covered by the snapshot of every
  introduced audience that holds a part or has a snapshot, including deleted
  circles. A circle with neither is not named and never holds the floor back
  (`bare_audience_never_blocks`); the floor still loses no part
  (`floor_loses_no_part`).
- **Deletion:** retention then deletes slots below the first kept slot, and
  bodies no floor names and no raise or reset above the floor needs, and none
  a rebuild starts from.
- **Rebuilds:** a circle that is not deleted and is below the store's version
  (`NeedsRebuild`) is rebuilt from the store's newest ordinary snapshot at a
  version of at most its own (`RebuildSnapshot`). `Good` holds that this
  snapshot's body is in storage with its state and that the first kept slot is
  at most the slot after its coverage, or at most 1 when history holds none
  (`RebuildKept`). A claim takes the same premise about the head it lands at,
  as it does for a body; a floor write takes it for its `first`; a body
  deletion takes it for the body it deletes. `store_good` keeps it. It also
  holds the bodies of the store's raises and resets after the snapshot, through
  the slot before the store's raise past the circle's version, which the
  rebuild replays; a body deletion must spare them.
  `rebuild_store_start` reads it back: a rebuild finds a sound store reader at
  the snapshot's coverage, its body, and every slot after.
- **Uploads:** a body uploaded to a fresh path.

`floorStart` takes one optional coverage per named audience.
`floor_requires_every_audience` rejects missing coverage;
`complete_coverage_advances` and `floor_covered` establish its bounds.
`floorAudiences` keeps the introduced audiences that hold a part or have a
snapshot: `part_audience_blocks_floor` checks one with a part and no snapshot,
and `bare_audience_never_blocks` that one with neither never matters.
`floor_loses_no_part` combines the floor write's rules with it: every slot
below the new `first` that carries a part is covered by the named snapshot of
its audience. How a device starts a circle the floor doesn't name is not
modelled.

A floor write makes two selections per audience. `newest` names the newest
applicable announcement, with no wait: that is where a new device starts.
`choose` bounds `first`: it scans announcements newest first, filtering
applicability and grace before selection. `chosen_mature` proves those gates and `chosen_newest` proves
the slot choice. `choose_dominates` proves that no newer announcement hides an
eligible coverage witness. The same selector
controls replacement-body deletion: `immature_replacements_keep_body` retains
an old body until an applicable replacement passes grace.
`chosen_has_no_mature_replacement` excludes deleting the selected body by that
rule. Announcement history is retained separately from replaceable bodies. Provider timestamps enter as
upper bounds, including their resolution.

`newest_covers_chosen` shows the name covers at least as much as the snapshot
past grace, since coverage never decreases with announcement order.
`floorFirst` sets `first` from `choose`'s coverages through `floorStart`, never
below the current floor's, and leaves it in place while an audience has no
snapshot past grace. `floor_write_rules` shows the combined floor meets
`StoreStep.writeFloor`'s premises on `first` and coverage: neither moves back,
and every name covers `first - 1`. So `store_good` and the reader theorems apply
to a floor whose names are fresh while `first` lags.

`sparedFirst` holds `first` back for the circles that need a rebuild: no
further than each circle's cap, the coverage plus one of its rebuild snapshot
(`RebuildCap`, 1 with none). `Retention.first_spares_rebuild_store`: it is no
further than any cap or than `floorFirst`, never moves back while the current
`first` is within every cap, and is `floorFirst` when no cap binds.
`floor_write_spared`: held back so, the floor still meets `floor_write_rules`'
conclusions. `floor_within_caps_spares_rebuild`: a `first` within every cap
meets the rebuild premises `StoreStep.writeFloor` takes, so `store_good`
applies and the rebuild snapshot and the slots after it stay.

`kept` keeps the announcements at or after the current first kept slot, whose
slots are still in storage. `choose_kept`: choosing among them picks what
choosing among all does, or none when that choice was announced before
`current`. `kept_candidates_same_first`: with each announcement after its
coverage, `floorFirst` over the kept announcements equals `floorFirst` over all
of them, since one announced before `current` covers less than `current - 1`.
`kept_replacement`: a replacement found among the kept announcements is the one
found among all, so choosing among the kept ones deletes no body choosing among
all would keep.

`fresh_head_starves` and `daily_announcements_starve` refute choosing the newest
usable snapshot before checking grace for `first`; the names have no grace to
starve. Daily snapshots keep the old rule waiting
through all fourteen days. `daily_announcements_advance` selects the day-zero
snapshot on day seven, then advances daily.

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
- Each deletion is a `StoreStep.delete` of its own, and `writeFloor` takes no
  premise on the slots below `first` and none on grace. A deletion that fails
  is a run without its step, and a floor written after any grace, or none, is
  a run with its write: `stores_good` and `deletion_keeps_reachable` cover both.
  So a failed deletion holds no later floor back, and correctness needs no
  grace; grace enters only `choose` and `Deadline`.
- `read_progress`, `reaches_head`: with its audience's key, a sound reader below
  the head can fetch and step forward, and some run reaches the head.
- `joiner_reaches_head`: a joiner of `A` that loads the floor's snapshot for `A`
  and follows storage reaches the state of a device that applied every slot.
- `loads_only_floor_snapshot`, `unloadable_floor_snapshot_waits`: the only
  snapshot a reader loads is the one the floor names for its audience, which
  needs the audience's key and the body in storage. Without either the reader
  moves at most to its next slot; no step loads an older snapshot instead.
- Before it holds the store key, a joiner reads slots too, scanning for the
  copy its admission carries. [`JoinScan`](sequence/CovenSequence/JoinScan.lean)
  models that scan: `approved_progress` finds a claimed copy while retention
  deletes the prefix under it, `approved_never_declined` has a check whose
  copies open end the join by publishing the store, since the slot they open
  from is the admission itself; the store's first load applies it
  (`approved_eventually_admitted`); a check that cannot reach the shared
  location before its keys open, with the access the join has proven, ends the
  join as an absent request does (`lost_access_ends`), and keys that opened win
  over that (`opened_wins_over_lost_access`); and
  `JoinScanExamples.exponential_probes_miss_approval` and
  `missing_below_existing_is_deleted` show why a missing slot below a kept
  one is read as deleted history, not probed past.

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
  equal the state immediately before announcement, and
  `ordinary_version_reached` that a counted snapshot's body is at a version
  its audience had reached at its coverage. `ahead_refused` is the checked
  counterexample: a snapshot whose body is a version ahead of its audience
  meets every coverage rule, and only the version rule refuses it.
  `based_counted`: a circle created at breaking version 2 counts a snapshot
  at version 2 with no raise of its own, which a circle with base zero
  refuses, and refuses a raise of it to version 2.
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

- `selection_advances` derives a floor beyond the prefix from eligible coverage
  for every introduced audience, using the actual `choose` and `floorStart`.
- `bounded_retention` combines that result with snapshot service U, grace G,
  maintenance within P, and deletion within D. Maintenance promises to write
  the selected floor; it does not assume advancement. Coverage announced by U
  must remain applicable through the maintenance window. Newer announcements
  have no frequency bound, and no device acknowledgements occur in the model.
- `uncovered_has_no_deadline`: missing coverage in any relevant audience
  prevents the retention gate at every time. `cut_delete_allowed` connects
  the service's floor bound to the concrete storage deletion step.

## Audiences and devices

[`Device`](sequence/CovenSequence/Device.lean) models a device with the list of
audiences it follows, a key per audience, and one reader per audience. A
reader may move only while no other reader of the device is behind it. When a
reader's next slot, checked against the floor, is its audience's and the device
has no key for it, or is a slot the device can't open at all, the device stops
entirely.

- `keyless_passes_only_others` (in `Reader`): a reader of `A` without `A`'s key
  passes only slots that have no part or announcement for `A`.
- `keyless_stops` (in `Reader`): such a reader whose next slot is `A`'s never
  moves.
- `device_stops`: a device following `B` without `B`'s key, whose `B` reader's
  next slot is `B`'s, never moves that reader. Even before it notices, every
  other reader applies at most up to that slot. `missing_key_blocks_reload`
  forbids every floor reload until all followed audience keys are held.
- `unopened_stops`: a kept slot the device can't open at all, its bytes damaged,
  in a newer format than its app reads, or a write at a newer schema version
  than its app runs (coven.md §18.2), stops it too. No reader applies that
  slot, for any audience, or moves past it, and no audience reloads while a
  reader waits there. Which slots a device can open (`opens`) is an input: the
  model reads no bytes, formats or schema versions. Once the app updates and
  opens the slot, `device_reaches_head` runs the device on.
- `stopped_stays`: once the device has stopped, nothing moves: no reader
  applies a slot and none reloads.
- `dev_ok`: device steps keep every followed reader sound.
- `device_reaches_head`: on good storage, a running device holding the keys of
  the audiences it follows, and no others, and able to open every slot, can run
  every reader to the head. A
  device outside a circle has no reader for it, applies the circle's slots
  without reading their circle parts, and reaches the right state for every
  audience it reads.
- `deletion_keeps_reachable`: this holds under any interleaving of claims,
  stale creates, floor writes, deletions, uploads and reader steps:
  - storage stays good;
  - every reader stays at a prefix state of its audience;
  - a device holding its audiences' keys and able to open every slot never
    stops, and can still run every reader to the head of the current storage,
    where each holds the full state of its audience.

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

[`QueueFold`](sequence/CovenSequence/QueueFold.lean) folds the queue again
after a slot of writes only where the slot reaches. A queued write changes only
its own rows, and what it leaves in them depends only on what they held. A slot
of writes changes only the rows it stages. The rows it reaches are those, closed
over the queue: a queued write that changes one of them adds all of its rows.

- `untouched_rows_keep_their_fold`: outside the closure, the queue's fold over
  the state after the slot is the fold the state already holds.
- `reached_rows_refold_alone`, `refold_reached`: inside it, folding only the
  queued writes that reach it, over the slots' state put back there, gives the
  whole queue's fold over the state after the slot.
- `staged_rows_alone_too_small`: the staged rows without the closure are too
  few. A queued write that changes a staged row and another can carry the
  staged row's new value into the other.

A slot that changes membership, cut-offs or boundaries changes how every queued
write applies; the model leaves it out, and the device folds the whole queue
again after one.

[`RaiseQueue`](sequence/CovenSequence/RaiseQueue.lean) represents capture,
confirmation, taking the body, its upload, preparation announcement and raise
activation as separate persisted states. Confirmation means the captured writes slot has
landed under the existing claim protocol; it precedes the raise slot (§18.5).

- `captured_prefix`, `later_writes_do_not_delay`, `confirm_captured`: publication
  selects the frozen prefix through the marker, regardless of later appends or
  their eligibility, then removes exactly that prefix from the local queue.
- `preparation_waits_for_confirmation`, `activation_waits_for_announcement`:
  taking and uploading the body require confirmed originals, and activation
  requires the uploaded body's announcement. `raise_without_waiting` reaches activation
  with any later queue; `tail_waits` keeps that queue waiting in between.
- `originals_preserved`, `confirmation_not_repeated`: the combined published
  and queued originals retain their order and all fields through the protocol,
  and confirmation cannot publish the prefix twice.
- `recapture_includes_waiting`: a later breaking migration captures intervening
  writes and both markers, leaving only writes after its marker waiting.
- `marker_changes_nothing`, `marker_replays_nothing`: the marker, the migration
  write's record, carries no part, so its slot changes no audience's ledger or
  rows and a rebuild applies it as nothing. The migration's effect on rows
  reaches other devices only in the raise's body (`preparedRows`).
- `addition_sends_newest`, `addition_raises_its_version`, `addition_keeps_taken`:
  an update that only adds appends no marker. A capture whose body isn't taken
  yet moves to the update's version, captured through the newest write, so
  every queued original goes out before the raise, which then raises to the
  version the body is taken at. A body already taken goes out at its own
  version. Rust also keeps the moved capture then, which raises again once that
  raise applies; the model's state holds one raise, so that second raise is a
  capture of its own. `unmoved_raise_refused` is the checked counterexample: a
  raise to the old version with a body taken at the new one is refused.
- `late_original_converted`, `reload_matches_receiver`, `conversion_agreement`:
  interpretation uses the original schema and the applied version. Queue
  replay agrees with the actual next sequence writes slot under the same
  admission/reset gates; conversion never rewrites the publication queue.

[`CircleRaise`](sequence/CovenSequence/CircleRaise.lean) shows which device can
raise an audience it reads below its breaking version. It assumes migrating
leaves an audience with no rows empty, whatever the store holds: a migration
changes rows that exist, and a circle made after it has none. A raise body is
the audience's rows migrated, and `Rules.migrate` reads a view of every
audience's rows: its field `migrate_reads` states the rule that an audience's
migrated rows depend only on its own rows and the store's (coven.md §18.1). A
raise of `A` covering `c` migrates `A`'s rows at `c` and the store's at `c`, or
at the slot before the store's raise past `A`'s version when that comes first
(`inputSlot`).

- `below_refused_until_raised`: a device whose breaking version is 2, reading
  a circle at version 1, gets none of its snapshots counted until a raise to
  2 applies; then the same snapshot counts. This is the state a device that
  never raises the circle stays in.
- `untouched_rows`, `fresh_raise_body`: an audience no slot has touched holds
  its initial rows, so the raise body of a circle that has no parts yet is
  its initial rows, which its maker holds whatever version it runs.
- `joiner_cannot_raise`: once another member's snapshot covers a part of the
  circle, a raise must cover it too, and its body is those rows migrated. A
  device that started reading the circle after migrating holds only the part
  it converted on reading, which isn't that body.
- `rebuilt_body_honest`: a device that reads the circle's rows and the store's
  from storage up to the circle's coverage (the store's up to its input slot)
  and migrates a view holding just those two, every other audience at its
  initial rows, gets exactly the raise body prepared at that coverage. This is
  `migrate_reads` and the reader's soundness.
- `reaches_target_kept` (Reader.lean), `rebuilt_body_from_kept`: a reader of
  the store that starts at a kept snapshot's coverage reaches any target below
  the floor's name, given the kept slots and the bodies of the store's raises
  and resets after it. A rebuild from `rebuild_store_start`'s reader and a
  sound circle reader then migrates a view of just those two audiences to the
  raise body, with the store's input slot no later than the slot before the
  store's raise past the circle's version.
- `NeedsStoreRebuild`, `store_rebuild_reads_kept`, `rebuilt_store_body`: the
  store itself is below a device's app version while some device may still
  raise it. Its rebuild is a fresh load: the floor's snapshot of the store,
  the slots and the store's raise and reset bodies after it, through the head.
  Good storage keeps all of that already, so retention needs no rule for the
  device's version, which it can't know. A migration of the store reads only
  the store's rows, so a view holding them alone gives the store's raise body
  prepared at the head; there is no other audience and no input slot below the
  head.
- `rebuilt_raise_counted`: a raise to a higher version, prepared at the
  rebuilt body's coverage and landing with no raise, reset or announcement
  covering more than it between, by an active device allowed to raise, is
  applied; its rows are the rebuilt body with the later parts converted.
- `cross_read_rebuild_differs`: a migration that reads a second circle (it
  fills Gifts rows from Garden's) gives a member of both and a rebuilder of
  Gifts different bodies at the same coverage, so `migrate_reads` fails for it.
  That is why the rule exists.

[`WriteVersion`](sequence/CovenSequence/WriteVersion.lean) gives the sending
side of the rule that a part counts only at a version its audience has reached.

- `step_version`, `version_mono`: no slot lowers an audience's version.
- `waiting_part_counts`: a part sent once the sender's applied prefix shows
  its audience at the part's version meets that rule at any later landing slot.
- `ahead_part_refused`: a circle at version 1 refuses a part at version 2 and
  counts the same part at version 1.
- `closed_step_version`, `closed_version`, `closed_part_never_counts`: an
  audience no one may raise from some slot on, such as a deleted circle, keeps
  its version from that slot, so a part ahead of it never meets the version
  rule at any later landing slot.
- `satisfied_raise_never_counts`: a raise of an audience to a version it has
  reached in the prefix the raising operation validated against, or of an
  audience no one may raise from there on, is refused at every later slot.
  So that operation is already satisfied: it adopts the state it applied and
  undoes its unused preparation (`operation_outcome`), whose announcement
  supplies nothing (`preparation_no_effect`).

[`RaiseBody`](sequence/CovenSequence/RaiseBody.lean) compares the model's raise
body with the one coven writes. The model's is the rows at the raise's coverage,
migrated. Coven's raising device migrates the rows it held when its app
migrated, judges each part landing after that as a late write (coven.md §18.5),
and moves its coverage forward over them.

- `one_part_agrees`: on a part, the two agree when the part's converter does
  what the migration does to it.
- `late_part_differs`: under a migration that declares `Drop`, a part on the
  old app landing after the raising device migrated and before its coverage is
  in the model's body and not in coven's. The honest-body premise below is the
  model's body, so the model's theorems don't cover coven's body for such a
  part.
- `holder_body_is_rebuilt`: a holder that queues a fresh marker and rebuilds
  the circle from storage at the marker's read slot `m'` gets the model's body
  at `m'`, the same as a rebuilder (`rebuilt_body_honest` with `m'` as the
  rebuild slot).
- `write_before_fresh_marker_kept`: a write that landed after the holder's
  first migration and before the fresh marker's read slot is in that body, even
  under `Drop`.
- `write_after_fresh_marker_late`: a part after the marker's read slot is a late
  write, which `Drop` leaves out, as for any rebuilder.

[`SecondRaise`](sequence/CovenSequence/SecondRaise.lean) takes a device that
read a circle after its first breaking migration and runs a second one before
anyone raised the circle. Its migration captures its writes through the new
marker, the held write into the circle among them, and the audiences it read.

- `second_capture_not_body`: if it read the circle converted, capturing the
  circle publishes a raise body the device never held: it never ran the first
  migration on the circle's rows, so its rows migrated again aren't the
  circle's rows migrated.
- `skipped_capture_drops_held`: leaving that circle out sends the held write
  ahead of the circle's version. Its part is refused, and the raise a holder
  makes later covers its slot without it.
- `rebuilt_second_capture_is_body`: if it rebuilt the circle instead, its rows
  are the circle's rows migrated to the first breaking version, and the second
  migration's step on them gives the circle's rows migrated to the second:
  the honest body.

## Operations

[`Operation`](sequence/CovenSequence/Operation.lean) models an admin operation's entry claim,
the claims of its key copies and announcements, and its deletable preparation
in a journal:

- it records each deletable artifact before sending its create;
- it records the slot it claims before sending the claim, and resolves that
  claim before doing anything else;
- its entry bytes name the operation and the slot.

A crash loses nothing the steps use. Every step reads the journal and storage,
and a restart takes the same steps. Sent claim requests survive crashes and can
land at any later time. So can artifact creates, until each ends by landing,
failing or being cancelled. The undo runs only after every create it sent has
ended. It deletes by recorded name and confirms absence before deleting the
journal. Other devices claim slots at any time. Validation (`check`) reads
only slots `1..n-1`, and says the operation may claim, is already satisfied,
or is no longer allowed. An operation already satisfied undoes its deletable
preparation as a refused one does, and ends satisfied with no entry.

Key copies and snapshot announcements are sequence slots, never deletable
artifacts. While it prepares, the operation claims them at the head, one slot
each; a slot another device took first is that device's.

This operation model retains sequence slots. Its results are safety properties,
not a promise that storage requests or contention eventually succeed. Settlement
after slot deletion is proved separately below.

- `step_inv`, `step_req`, `steps_inv`: every step keeps the invariants. One of
  them is that every create still in flight is recorded while the journal
  exists, and outlives it only for a landed operation.
- `operation_outcome`: over any interleaving, the operation's entry occupies at
  most one slot. A landed entry's bytes are at its slot; the slots before
  it were filled when it validated, and validation held against them. An
  undone or already satisfied operation has no entry bytes in the sequence,
  now or later, and none of its recorded deletable artifacts remains in
  storage.
- `preparation_precedes_entry`: each slot the operation's key copies and
  announcements landed in holds them, and a landed entry's slot comes after
  every one of them.
- `undo_final`: once undoing, the operation stays undoing until its undo is
  confirmed, then ends satisfied or rejected as it began. It never claims
  again; a failed undo is a step not taken, so its record stays and it retries.
- `steps_preserve_history`, `undo_keeps_preparation`: operation steps retain
  every occupied history slot. `undone_retains_history` exhibits an undo that
  deletes a snapshot body while its announcement and a key copy remain.
- `forgetful_lands_twice` is a checked counterexample. A restart that does not
  resolve its recorded claim validates again after the first claim landed. It
  then claims the next slot and lands there too.
- `eager_undo_leaks` is a checked counterexample. An undo deletes an artifact
  while its create is in flight, confirms absence and ends. The create then
  lands, leaving the artifact of an undone operation in storage.

[`Setup`](sequence/CovenSequence/Setup.lean) models storage setup, an
operation with one point of no return (§11.6). Its journal starts before its
first write to storage. While it checks the provider it has sent no claim and
committed no connection. Recording the claim it is about to send is its point
of no return. A cancel, a failure or opening after a crash undoes it only
before that point. Past it, the claim settles: landed, setup commits the
connection and finishes; lost, it reads on, and is refused only where every
claim it sent targets a slot another device holds. Test objects and probes
setup writes while checking are space no claim or commit depends on, so they
are not modelled.

- `step_inv`, `steps_inv`: every step keeps the invariant. Checking, setup has
  sent and committed nothing; the connection is committed only once the claim
  has landed.
- `setup_outcome`: over any interleaving, setup's bytes occupy at most one
  slot, a committed connection has its claim landed, and an undone setup has
  no committed connection and no bytes in the sequence, now or later.
- `crash_before_point_undoes`: wherever a run leaves setup checking, opening
  undoes it with nothing claimed or committed, and nothing of it can land
  later.
- `crash_after_point_finishes`: wherever a run leaves setup claiming a slot,
  its own steps settle the claim. Where the slot is empty or holds its bytes,
  it lands, commits and finishes; where another device took it, it reads on.
- `late_cancel_strands_claim` is a checked counterexample. A cancel honoured
  once the claim is sent ends setup undone; the claim then lands, leaving a
  store this device doesn't know it made.

[`MemberKeyReplacement`](sequence/CovenSequence/MemberKeyReplacement.lean)
models the custody decision after a refused self-replacement's child claim is
undone. `unheld_keeps_pause` retains the pause under an unheld binding;
`candidate_installs` selects installation of a winning candidate, still owing
its restore code. `undo_requires_active` permits release only under the held
active pair. Public pairs are equality-preserving tokens; custody writes and
restore-code persistence are outside this decision.

The same file models the race and the procedure:

- `replaced_never_current`, `competing_replacement_refused`: a replacement
  applies only while the pair it expects is current and its new pair was never
  bound. Once one applies, the pair it replaced is never its member's again,
  so a competing replacement expecting that pair, and every retry of it, which
  keeps the pair it expects, is refused at every later slot.
- `writes_straddle_replacement`, `pause_lifts_after_install`: replacing one's
  own keys pauses the app's writes, sends the writes already queued, keeps the
  new pair in custody, claims the replacement, installs the pair and refreshes
  the restore code before lifting the pause. Over any interleaving with other
  devices' slots, every write under the old pair lands before the
  replacement's slot and every write under the new pair after it, so none is
  lost to the cut-off; and once the replacement has landed the device writes
  again only with the new pair kept, installed and its code refreshed. The
  replacement's copies and the undo after a refusal (`resolve`) are outside
  this part.

## Leaving the queue

[`WriteQueue`](sequence/CovenSequence/WriteQueue.lean) models a device's queue
of writes, the writes its applied slots consumed, and the one open claim, which
carries a prefix of the queue (§6.3). The claim's create lands at some time, its
slot applies once it has, settling as landed needs the bytes there, and
finishing, as `claim::finish` does, needs the settlement and the application.

- `nothing_lost`: every write ever committed is in the queue or in the state.
- `applied_once`: the state counts each write once.
- `leaves_only_landed`: a write that is not in the queue is in the state and was
  carried by a claim that landed.
- `visible_exactly_once`: the app's rows, the state and then the queue's writes
  the state does not hold, hold every committed write exactly once.
- `unguarded_finish_loses`: forgetting a settled claim's writes before their slot
  has applied leaves them in neither place.

A claim that is lost to another device's slot is the same claim retried, which
does not change the queue, so the model has no step for it. Which writes a claim
carries is any prefix of the queue; sendable-prefix rules (§17.4, §18.5) are in
`Holds` and `RaiseQueue`.

## The saved restore code's name

[`SavedName`](sequence/CovenSequence/SavedName.lean) models the name a
person's saved restore code carries with the slot that set it (coven.md §13.1).
A device rewrites it only for a later slot than the entry's, and never
creates an absent entry. The entry holds the restore code too, and a rewrite
keeps it, as `follow_name` does in Rust: only setup, joining, restoring,
resetting and member-key replacement save a code over a readable entry.

- `saved_latest`, `never_back`: rewrites from any number of devices, in any
  order, leave the latest slot among them and that slot's name; no rewrite
  moves the entry back.
- `rewrite_keeps_code`, `rewrites_keep_code`: a rewrite leaves the code the
  entry holds, and so do any number of them in any order.
- `absent_stays`: a forgotten entry stays forgotten.
- `overwrite_puts_back_code` is a checked counterexample: a laptop that
  rewrites with its own code, after the phone replaced the member keys and
  saved code 2, puts its code 1 back.
- `slotless_puts_back` is a checked counterexample: overwriting without the
  slot, a phone still behind at slot 1 puts Household back over Family from
  slot 41.

[`SavedEntries`](sequence/CovenSequence/SavedEntries.lean) models the two saved
entries of a store, the S3 key and the restore code with one copy per keychain
group, through the two operations that take several keychain steps (§13.1).

- `forget_cut_listed`, `forget_finishes`: forgetting deletes the key and then
  the code. Cut after any step, no key is saved for a store the listing does
  not show, and running it again from there leaves both entries absent.
  `code_first_strands` is the counterexample for the other order.
- `replace_cut_present`, `replace_cut_ambiguous`, `replace_finishes`: replacing
  an entry writes the first group's copy and then deletes the others. Cut at
  any step, an entry that existed is still present; with copies in several
  groups it reads as ambiguous until the last deletion; and the change run
  again leaves exactly the new bytes. `delete_then_write_loses` is the
  counterexample for deleting first.

The model has the keychain's steps and not the platform: that each step is
atomic, and that the keychain lists the copies of every group, are assumed.

## A member's removal and the saved code

[`PersonLeaves`](sequence/CovenSequence/PersonLeaves.lean) models the pass that
reaches the slot removing the device's member (§14.1). Before the slot's
transaction it deletes the person's saved restore code and S3 key, and from the
first attempt on the keychain refuses to write them again. A failed deletion
fails the pass, which is retried over the same slot. A device saves the code
only while the person has not left.

- `removal_applies_only_after_deletion`: in every reachable world, once the
  removal is applied the entries are deleted and the keychain refuses to write
  them, however many passes failed before.
- `removal_applies_after_follow`: a pass at the removal's slot that deletes the
  entries then applies it.
- `apply_first_leaves_code` is the counterexample: applying first and then
  failing to delete leaves the code offered, and the deletion, which runs for
  the slot that moves the member to removed, has no slot left to run for.

The two entries are one step here; their order is `SavedEntries`.

## Invites and the app's operations

[`InviteLifecycle`](sequence/CovenSequence/InviteLifecycle.lean) models one
invite on the device that made it (§13.2), the operation that binds its
request's pair, and one admin change the app's write journaled (§11.4). Its
steps are every writer that acts on them:

- the app's write steps: approving, declining and cancelling the invite,
  retrying its pending record, acknowledging an outcome, journaling an admin
  change. Each reads and decides in one transaction;
- the expiry's write, which also reads and decides in one transaction;
- a pass, which reads the invite's journal row, does its storage work with no
  transaction open, and writes after. Any other writer may commit in between;
- a crash, which loses the pass's read and nothing durable;
- the joiner writing its request, whose keys may be ones history already
  bound to a member, current or past;
- what a pass learns: the approver's demotion, and a stop for good, from a
  slot it applies or from history showing another copy of this device;
- another admin's device applying that stop, which may come first: it writes
  this device's gone marker and deletes the joiner's request, and the joiner
  sends none after it (§13.2);
- the owner's devices taking back the invite's share: the inviting device once
  it ended the share, any of them once history records the share and the
  inviting device is gone for good, unless the account is a member's by then
  (§13.2, §14.2).

The row carries its step, and every change of the row increases it. A pass's
write, and the pending record it saves for what it found, are conditional on
the step it read. Storage may fail any attempt; the failure is the pending
record. The properties are proved under `Rules.correct`:

- `reachable_inv`: the invariant every reachable state keeps.
- `finding_current`, `commit_acts_on_read`: no write from a stale read. A
  pending record holds what a pass found at the row's current step, and a
  pass's write changes the row only when the row is the one it read.
- `share_recorded`: a share standing at the provider is one history records,
  because the pass records it before the grant (`ShareInv`).
- `settles`: every invite and every operation the app journaled ends, on every
  path, crashes and stops included. From any reachable state, coven's own
  steps, with the app retrying only records it is shown, reach a state with no
  invite listed, no operation running, no pending record, and no share
  standing for an account no member uses. The proof is a
  measure that a step of coven's own lowers from every other state
  (`progress`); it assumes storage eventually answers.
- `forward`: what the app sees never goes back. The listed invite moves from
  granting to waiting, to a request waiting for the app, to approving, to gone;
  the binding's entry from running to ended to acknowledged.
- `offered_accepted`: from `coven_invites`, `coven_operations` and
  `coven_pending` alone, the app's offer of approving, declining, cancelling
  and acknowledging is exactly what the write steps accept, and every record it
  is offered to retry can be retried.
- `approval_runs_binding`: an invite listed as approving is exactly one whose
  binding is listed as running.
- `offered_fresh`, `binding_fresh`: the pass checks the request's keys when it
  opens the request, so the app is offered approving only a request whose keys
  are fresh, and an approval under way binds only such a request (`KeysInv`).
  History then refuses the binding only when this device may no longer
  approve, never for the request's keys.
- `refused_ends`: a pass that finds a request failing its checks ends the
  invite, as a decline does, with no pending record, and the pass goes on to
  put the marker in the request's place. The invite ends without the app's
  retry and without its expiry.
- `refused_approval_ends`: an approval whose binding history refuses ends the
  invite as a decline does, with the refusal as the binding's outcome, and the
  next read and write put the marker in the request's place.

Each rule `correct` has is needed; leaving one out is a checked counterexample:

- `unguarded_finding_is_stale`: a finding saved without the step check lands
  over the app's cancel.
- `forgetful_stop_runs_forever`: a stop that doesn't end the app's operations
  leaves one running forever.
- `stop_strands_approval`: a stop that doesn't end the invites leaves an
  approval under way listed forever; it never expires.
- `split_binding_disagrees`: a binding ended in a transaction of its own shows,
  after a crash, its outcome beside an approval still under way.
- `reclaim_binds_twice`: an approval that journals its binding again after a
  rejected attempt stopped binds twice.
- `approval_shown_as_waiting`: a listing that shows approval under way as a
  waiting request offers approving again, which fails.
- `demoted_approval_offered`: an approve step that checks the applied state
  refuses a request the app was shown as waiting for its decision.
- `unrecorded_share_strands`: a share granted without its record, on a device
  that then stops for good, stands forever: no other device knows of it.
- `unchecked_keys_approved`: with the request's keys checked only when the
  binding applies, the app is offered a request whose keys a member holds, and
  approving it journals a binding history refuses.
- `refused_request_retried`: an invite left waiting on a request that failed
  its checks offers the app a retry that comes back to the same state: the
  request stays at the invite's one path, so the pass refuses it again, and
  the invite stays listed, with its share standing, until the app cancels it
  or it expires.
- `refused_grant_retried`: an invite whose grant the provider refuses for a
  reason only a person can fix, left granting on a pending record, stays
  listed without a code, and the pass never runs it again: only the app
  cancelling it ends it.
- `refused_grant_ends`: under the rules the same refusal ends the invite in
  the pass that found it, as a failed request check does.

`Rules.rust` names the rules the Rust follows; the differential test compares
it.

[`InviteClaims`](sequence/CovenSequence/InviteClaims.lean) models what
`InviteLifecycle` takes as landing in one step: the entries an invite claims
through operations of its own, its share's record and the share's end, and
the device's one open claim they share
(§5.1, §13.2). Each attempt is sent, then lands or loses its slot to another
device; a pass may read this device's own landed slot before it settles the
claim, as one that restarts after a crash between the two does; the app
cancels, and the invite expires, at any point. Every step is one transaction.
Under `Rules.correct` the invite names each operation it starts, and its
settled step settles the claim of every one still journaled, claiming nothing
for it again, and ends them all with the invite:

- `children_end_with_invite`: once the invite's row is gone, none of its
  operations is journaled and no claim is open.
- `settles`: from every reachable state, coven's own steps, its passes, its
  attempts sent again and the expiry, end the invite with no claim open, so
  the device's writes go out. The proof is a measure a step of coven's own
  lowers from every other state (`progress`); it assumes an attempt sent again
  lands when no other device takes the slot.

`Rules.unnamed`, the invite that looked only at history once it ended, has a
counterexample for each way it left a claim open for good:

- `ended_share_strands_claim`: the share's end lands, and the pass reads that
  slot before settling its claim; the invite ends without the claim.
- `cancelled_share_strands_claim`: the app cancels the invite while its share's
  record lands; the share's end it then claims is refused by that open claim,
  and the invite never ends.

[`AccessKeyReplacement`](sequence/CovenSequence/AccessKeyReplacement.lean)
models a member's device taking a new S3 key B in place of A (§14.3):
custody's key, a keychain entry, and the key history records, an admin entry
the device journals and claims. The keychain and the journal share no
transaction, so the device can be killed between any two steps, and the person
can delete A in the provider's console at any time. Under `Rules.correct` the
call journals the entry for B, takes B in custody, and the entry is claimed
with B; an entry that runs while custody holds A ends without claiming:

- `custody_has_record_or_entry`: in every reachable state, custody holds B
  only where history records B or an entry for B is journaled.
- `record_follows_custody`: history records B only where custody holds B.
- `coven_settles`: from every reachable state, coven's own steps, the claim
  landing or the entry ending without claiming, end with no entry journaled
  and custody on the key history records.
- `retry_finishes`: from every reachable state, a call with B journals the
  entry unless one is journaled or recorded, takes B, and the claim lands;
  this assumes the claim lands, as the other models do.

`Rules.keptApart`, which held B back until history recorded it and claimed with
A, has a counterexample:

- `kept_apart_strands`: A is deleted in the console after the entry is
  journaled; nothing is then enabled, so history never records B and
  custody never takes it.

[`JoinStatus`](sequence/CovenSequence/JoinStatus.lean) models what a waiting
join reports across its checks and across the calls that resume it: a call
may stop between checks, or in the check whose copies opened its keys, after
it reported loading. The scan for the copies is part of the wait. The first
check whose copies open the keys, in its admission, records that with the
saved request before it reports loading, and that check ends the join: it
publishes the store, which loads on as a store.
`phases_forward` proves the phases never go back: waiting, then loading, then
done. A check can also find the join's access lost: with no keys opened, it
reports its wait and then done (`lost_access_reports_done`). Each rule is
needed:

- `resumed_waits_again`: with the opened keys kept only in the call, a call
  that stops after finding its admission, before it published, is resumed by
  a call that reports waiting again.
- `scan_shown_as_loading`: a scan that finds no copy, reported as loading,
  puts the next check's wait after it.

[`RestoreRegistration`](sequence/CovenSequence/RestoreRegistration.lean)
models a restore past loading (§13.1, §11.6): the call registers the new
device through an operation whose claim goes through the device's one claim
journal, then publishes the store. The registration's task outlives the
call that started it. Each attempt is sent, then lands or loses its slot to
another device; the app cancels, drops the call or crashes at any point; the
call fails; another device removes the member, or replaces the member's
keys with a pair the restore doesn't hold, after which the registration is
refused and a registered device is gone for good. Every step is one
transaction. Under `Rules.correct` the restore records itself, with its code
and sign-in, before it registers, and the app lists recorded restores to
resume. A cancel waits for the registration's task. A call that ends without
its store, cancelled, its registration refused or its device gone for good,
removes the installation only when nothing in storage names the device as
active and nothing can land: no registration journaled, one refused, or the
device gone for good since, a replacement of the member's keys counted as
well as a removal. Otherwise, and after any other failure, the call ends
and the restore stays listed. Before the record, a failure removes the
installation, as a cancel does. A crash before the record leaves an
installation nothing refers to, no call on it and nothing recorded beside
it; listing unfinished work reclaims it.

- `registered_listed`: while history names the device as active, or a claim
  of its registration is open, the restore is published or listed.
- `settles`: from every reachable state, coven's own steps and the app
  resuming what is listed reach a state with nothing left: the store
  published, or no installation, nothing recorded and no call running. The
  proof is a measure those steps lower from every other state (`progress`);
  it assumes an attempt sent again lands when no other device takes the
  slot.
- `settled_outcome`: there, the device is registered and its store
  published, or it is not active in history and no claim of it is open.
- `ended_call_leaves_nothing_unlisted`: a call that ends other than by a
  crash publishes the store, removes the installation or leaves the restore
  listed; only a crash leaves an installation for a listing to reclaim.

`record` and `wait` are each needed; without either a device stays
registered with no installation published or listed for it, on every path:

- `crashed_registration_ghost`: without the record, the registration lands
  and the device crashes; nothing lists the installation to resume.
- `cancelled_registration_ghost`: a cancel that doesn't wait removes the
  installation while the attempt is on its way, and it lands after.

Without `undo`, a call that fails before the record leaves its installation
for a later listing to reclaim (`failure_keeps_installation`). Without
`stops`, counting only a removal as the device gone, a restore whose
member's keys were replaced after it registered stays listed, and every
resume fails on every path (`replaced_restore_stuck`). Without `reuse`, a
resume journals a second registration rather than run the one its journal
holds. Once the first one's task has failed with its claim open, waiting for
the app to call again, the second's claim meets that open claim and every
resume fails on every path (`forgotten_registration_stuck`).

[`FirstInstallation`](sequence/CovenSequence/FirstInstallation.lean)
models one store's unfinished first installation on a device, which a join,
a restore and a pairing all begin on (§13.4). A join keeps its invite code from
before its request may be sent, and a restore its record from before its
registration may be sent; each lists its call to resume. A join cancelled
before sending, or declined, and a restore cancelled or failed before its
record, remove the installation and everything kept beside it. A join
accepts the provider's share before it sends, and a cancel takes back the
acceptance it records first; a decline's acceptance ends with the invite, and
an approval's becomes the store's access. A join finds its admission, which
registered its device, by the copies it carries; the published store's
first load applies it, counted or refused. The app drops either call at any
point, and the app records the person's profile. A pairing keeps nothing on the device
until its restore records itself, when it becomes a restore; cancelled,
dropped or failed before, it removes the installation. Under `Rules.correct` a
restore refuses an installation holding an invite code, a pairing one holding
an invite code or a restore's record, and a join one holding a restore's
record, a join records that it is accepting before it accepts, a
join publishes the store in the step that finds its admission, a store whose
first load leaves its device never a member stops for good, and the joining
write commits in the step that first applies the device's registration.

- `wrong_account_keeps_join`, `wrong_account_trace`: a join that has not
  proven the account it is signed in as is the account the invite was shared
  with has sent nothing and accepted nothing, and stays listed and
  cancellable.
- `lost_access_after_proof`, `lost_access_leaves_nothing`: a refusal of the
  shared location ends a join only once its account is proven, and the join
  then ends as a declined one, with nothing stranded.
- `never_stranded`: whatever storage may hold of a join's request or a
  restore's registration, and whatever acceptance the joining account
  holds, the call that made it stays listed until it meets its outcome.
- `found_published`: a join that found its admission is a published store,
  listed as no join, with no call on it.
- `refused_stops`: a store a join published whose first load reached the end
  of the sequence counts its device a member; a refused admission stopped it.
- `profile_with_registration`: every reachable world whose loaded history
  registers the device has the person's profile committed, since the joining
  write commits in the step that first applies the registration.
- `pairing_unlisted`, `pairing_refused`, `pairing_cancel_leaves_nothing`: a
  pairing has no code, record, request or registration on the device; it is
  refused an installation holding an invite code or a restore's record; and
  its cancel leaves no installation and nothing stranded.

Without the account check, `unchecked_refusal_misjudged`: the location's
refusal of the first acceptance is one observation for an ended invite and a
wrong account, so whichever reply a join gives it is wrong for one of them.

Each rule is needed. Without the first, one call's removal takes the other's
listing:

- `restore_strands_join`: a restore begun on a waiting join's installation
  is cancelled before its record, and removes the join's code.
- `join_strands_restore`: a join begun on a registering restore's
  installation is cancelled before its request, and removes the restore's
  record.
- `pairing_strands_join`, `pairing_strands_restore`: a pairing begun on a
  waiting join's, or a registering restore's, installation is cancelled
  before its own record, and removes the join's code or the restore's record.

Without the second, `cancel_strands_acceptance`: a join accepts, the app
closes, and the resumed join is cancelled; it removes its code and leaves
the acceptance in the joining account.

Without the third, `loaded_first_lists_admitted`: a join finds its admission
and the call stops while it loads, its storage access lost; the join stays
listed with nothing left to do. Without the fourth,
`refused_syncs_forever`: a store whose first load applied a refused
admission syncs on unregistered, with no step left that would end it.
Without the fifth, `acknowledged_profile_lags`: the app writes the person's
profile in a write of its own after the load, so a registered member exists
without it until that write, and for good if the app quits first.

[`JoiningWrite`](sequence/CovenSequence/JoiningWrite.lean) follows one
member's devices through a join, a restore or a pairing's new device, a lost
phone and a landed write. The join's facts, the name and the circle the
admission made, live in the member's state in history, and the app's joining
write runs as each device's registration applies.

- `every_device_has_profile`: each device of the member whose registration
  applied has the profile, whichever devices were lost and whether or not
  the first write landed.
- `device_record_lost`: with the facts kept only on the joining phone, a
  phone lost before its write lands leaves a restored device registered with
  the member and no profile, in every extension.

[`DeviceReplacement`](sequence/CovenSequence/DeviceReplacement.lean)
models `reset_device` and `restore_device` past loading (§19.3, §13.1): the
new installation, loaded beside the old one under a fresh device id,
registers the fresh id as replacing the old one through the device's claim
journal, swaps into the store's place once the registration applied, and is
published. The registration's task, its attempts and the other devices
behave as in `RestoreRegistration`; another device may remove the member or
replace its keys, after which a registration is refused and a registered
device is gone for good. The app cancels before the swap, drops the call or
crashes at any point, and opens the store whenever no call holds it. Under
`Rules.correct` the replacement records itself before it registers, and the
record refuses the app's opens and lists the replacement to resume; from the
record on a cancel waits for the registration's task and removes the new
installation only when the facts allow it, and a failure after the swap
keeps the swap.

- `old_never_stale`: the old installation never opens once a registration
  replacing it may have landed.
- `never_missing`: while history names the fresh device as the old one's
  active replacement, its installation is there.
- `settles`: from every reachable state, coven's own steps and the app
  resuming what the store listing shows reach a state with nothing left: the
  new installation published, or no call running and nothing recorded. The
  proof is a measure those steps lower (`progress`).
- `settled_outcome`: there, the new installation is published with its
  registration landed, or no claim of it is open and history doesn't name
  it as active.

Each rule is needed:

- `crashed_replacement_opens_old`: without the record, the registration
  lands and the device crashes; opening the store opens the old
  installation, which stops for good.
- `unswapped_replacement_opens_old`: moving the old installation back after
  a failure past the swap removes the record, and the same open follows.
- `cancelled_replacement_missing`: a cancel that doesn't wait removes the new
  installation while the attempt is on its way, and it lands after.

[`StoreDiscard`](sequence/CovenSequence/StoreDiscard.lean) models
`discard_store` (§19.3, api.md E1): the call deletes the device's keychain
entries for the store, its directory and the lock files beside it, in steps
no transaction holds together. The call fails or the device crashes at any
point, and the app discards a listed store again. Under `Rules.correct` the
deletion first renames the directory to its deletion name, which hides the
store and records the deletion, and listing the stores, or a restore or join
of the store, finishes a recorded deletion no call holds.

- `never_broken`: the listing never shows a store whose entries are gone.
- `settles`: from every reachable state, the call's steps and the listing's
  reach a state with nothing left: the store whole, or gone with its entries
  and lock files. The proof is a measure those steps lower (`progress`).

Each rule is needed:

- `entries_first_breaks_store`: deleting the entries first, a crash leaves
  the store listed with no device id or keys.
- `unfinished_deletion_blocks_store`: with nothing finishing a recorded
  deletion, a crash after the rename leaves remains for good, and the
  store's id can't be made again.

[`StoreDeletion`](sequence/CovenSequence/StoreDeletion.lean) models
`delete_store` (api.md E1, §14.1, §11.6): the call journals this device's
removal, which lands and applies, then deletes the store from the device.
The journaled removal lives in the store's database, so deleting the store
before it applies loses it. The call fails or the device crashes at any
point; the app opens the store when nothing refuses it, and an open store's
passes send a journaled removal; the person signs out or in. Under
`Rules.correct` the deletion records itself in the store's directory before
it journals the removal, the record refuses opens and lists the deletion
for a resume, and a failure before the journal removes the record; a store
whose location is recorded without credentials refuses the deletion.

- `never_stranded`: the store never leaves the device without its removal
  applied.
- `never_unlisted`: the app never opens a store whose removal is journaled
  or applied.
- `settles`: from every reachable state, coven's own steps, the app
  resuming the deletion the listing shows and signing in when it must,
  reach a state with nothing left (`progress` lowers a measure).
- `settled_outcome`: there, the store is gone with its removal applied, or
  untouched with no removal journaled.

Each rule is needed:

- `signed_out_strands_device`: signed out, the deletion skips the removal
  and the device stays registered for good.
- `crashed_deletion_reopens_removed`: without the record, a crash after the
  removal lands leaves the store opening as removed, with its data and keys.
- `failed_deletion_removes_store_in_use`: without the record, a call that
  fails after journaling leaves the store open, and its passes send the
  removal while the app uses it.

[`BootstrapCommit`](sequence/CovenSequence/BootstrapCommit.lean) models the
commit a restore or a join makes once it has loaded (§13.4, §11.6): one write
at a time of the device's custody for the store, then publication. The call
runs past its point of no return, so the installation stays listed and the
next call loads and commits again; a failure or a crash keeps what the commit
wrote. Another device may remove this one, after which loading fails and the
facts end the installation. Under `Rules.correct` ending the installation
deletes every device-only entry of its store.

- `never_orphaned`: no custody is left for a store the device doesn't have.
- `published_whole`: a published installation's custody is whole.
- `settles`: from every reachable state, coven's own steps and the app
  resuming what is listed reach a state with nothing left: published, or the
  installation removed with no call running. The proof is a measure those
  steps lower (`progress`).

The rule is needed:

- `cut_commit_orphans_custody`: a crash in the middle of the commit, then the
  facts ending the restore, leaves the custody the commit wrote, the device
  id and keys among it, for a store the device doesn't have.

## Pairings

[`PairingLifecycle`](sequence/CovenSequence/PairingLifecycle.lean) models one
pairing on the device that shows its code (§13.2, "Pairing: one of the
person's devices adds another"), the object at the request's one path, and the
new device that asks. It has the invite's shape: a journal row with a step
counter, conditional pass writes, the app's write steps, the expiry, a crash,
a stop for good. Its steps are every writer that acts on them:

- the app's write steps: approving the request it was shown, declining it, and
  ending the pairing when Add device closes, or forgetting one that ended. Each
  reads and decides in one transaction;
- the expiry, which reads and decides in one transaction too;
- a pass, which reads the pairing's row, does its storage work as separate
  steps with no transaction open (deleting the request, creating the handover,
  putting the marker), and writes after. Any other writer may commit in
  between. Every pass looks at every pairing: it has no doubling wait;
- a crash, which loses the pass's read and nothing durable;
- opening the store, and a reset of the showing device, which carry every
  pairing a closed handle or an old database left to its ending;
- a stop for good, which finishes the row in its own transaction and does no
  storage work, and an admin's device that sweeps the stopped device's requests
  and handovers and writes the gone marker;
- whoever saw the code writing a request of their own at the one path, one
  that fails the pairing's checks or passes them;
- the new device: sending its request, again when the path is empty, finding
  the handover, being cancelled, recording its restore, and registering.

The one path holds a request, the handover that takes its place, or the
marker. `Rules` names each choice the properties depend on: the pass saves its
finding only at the step it read, approving compares the request the app was
shown with the stored one, and an ending replaces whatever is at the path. The
properties are proved under `Rules.correct`, which has all three:

- `reachable_inv`: the invariant every reachable state keeps.
- `finding_current`, `commit_acts_on_read`: no write from a stale read.
- `settles`: from every reachable state, crashes, opens, expiry and stops
  included, coven's own steps and the app's closing of the screen reach a state
  with no row, no pending record, no pass in flight, and at the path the marker,
  or what the sweep left on a device stopped for good. `settled_no_handover`:
  no handover is left there. The proof is a measure a step lowers
  (`progress`).
- `offered_checked`: a request the pairing offered or approved passed its
  checks. `refused_ends`: a request that fails a check ends the pairing in the
  pass that reads it, the marker placed, no pending record.
- `forward`: what the app is shown never goes back, from waiting, to a request
  waiting for it, to approved, to ended, to forgotten. `offered_accepted`: the
  app's offer of approving, declining and closing is what the write steps
  accept. `approved_iff`: it is shown approved exactly while the row journals
  the approval.
- `handover_only_approved`, `journaled_approved`: a handover, journaled or
  stored, is sealed to the key of the request the app approved, which is the one
  the pairing checked and offered. `nothing_before_approval`: before the
  approval nothing at the path carries the member's keys and the new device
  holds none. `handover_owned`: a handover at the path belongs to a pairing not
  yet ended, or to a device stopped for good.
- `claims_zero`: no slot claimed, no access recorded.
- `check_handover_not_ended`: the new device's check that finds the handover
  never reports the pairing ended. `approved_converges`: while the pairing is
  approved, the pass that replaces a request with the handover finishes.
  `request_in_window`: so it does when the new device's request lands between
  the pass's delete and its create, and the new device then opens the handover.
- `cancel_binds_nothing`: cancelling the new device before its restore records
  itself leaves no add-device entry in history.

Each rule is needed:

- `unguarded_finding_is_stale`: a finding from before the app's close is saved
  over the closed row.
- `uncompared_approval_seals_wrong`: the app approves a request it was shown
  that isn't the stored one; the write is accepted and the handover is
  journaled for the stored request's key, one the app never approved.
- `ending_leaves_handover`: an ending that replaces only a request, and
  finishes without the marker in place, forgets a pairing the app closed after
  the handover was stored, with the handover still at the path.

The new device's installation is `FirstInstallation`'s (above).

## Settling a claim whose slot was deleted

[`Settle`](sequence/CovenSequence/Settle.lean). Each copy numbers all its slot
claims with one counter, settles its current claim before starting another,
and records the attempted path and exact bytes before sending. Contention
retries retain the number. Different copies of one device id may use equal
numbers with different bytes; the general settlement proof allows this.

Every snapshot's ledger holds `Option (ClaimRecord Digest)` per device. `none`
means no included claim. A record pairs the greatest included number with the
SHA-256 of that claim's complete slot bytes, valid or refused. The digest is
replaced on an equal number at a later slot and retained on a lower number.
[`Claims`](sequence/CovenSequence/Claims.lean) defines this fold once for both
the abstract settlement history and `Model`'s ledger.

For an in-flight claim numbered `n`, below the floor:

- No record, or `L < n`: `notLanded`.
- `L > n`: `copy`, even if the digest matches; this copy has never claimed above `n`.
- `L = n`: `landed` only when the digest matches, otherwise `copy`.

For a kept slot, equal complete bytes mean `landed` and absence means `retry`.
Different bytes signed by the same id mean `copy`, whatever their claim number;
a different device means `notLanded`. This copy settled its earlier claims
before attempting this slot. Bytes recreated below the floor never override
its record. A read error supplies no settlement input.

- `Claims.witness`, `lastClaim_witness`, `claims_witness`: the number and
  digest in a record came together from an included slot of that device.
- `Claims.ge`, `lastClaim_ge`, `claims_ge`: the record's number reaches a
  positive `n` exactly when an included claim reaches it. These bounds do not
  assume unique or increasing numbers per device id.
- `settle_correct'`: for any number of copies, `landed` implies the recorded
  slot holds this copy's bytes; `notLanded` excludes that attempt; `retry`
  is equivalent to a slot above the head; and `copy` is equivalent to evidence
  of another copy in the source settlement reads.
  The digest premise says that matching complete-byte hashes identify the
  recorded bytes and slot path, including [D9's `after` header](../format.md#d9-sealed-objects).
  It is a cryptographic/encoding assumption,
  not a uniqueness assumption about claim numbers.
- `settle_correct`: under `Own`, the single-copy journal facts, `landed` is
  equivalent to the recorded claim being in the sequence, `notLanded` means
  its claim number is nowhere in the sequence, `retry` means the slot is still
  empty, and `copy` is impossible. These journal facts are not premises of
  `settle_correct'`. Earlier claims must precede the attempted slot.
- `journal_own`, `journal_settles` (in
  [`ClaimJournal`](sequence/CovenSequence/ClaimJournal.lean)): `Own` holds for
  the open claim of one device that keeps the journal rules, in every state
  reached from empty storage. The device numbers claims of every kind with one
  counter, begins one only with none open, sends only the attempt its journal
  records, and after losing keeps the number and records a new attempt at the
  slot after those it then applied. Other devices' slots and its own creates
  landing late interleave freely. So `settle_correct` holds for it with no
  premise about claim numbers. `kind_counters_report_copy`,
  `two_open_claims_report_copy` and `unrecorded_create_looks_like_copy` are
  checked counterexamples: a counter per kind, two claims open at once, or a
  create sent before it is recorded each make the one copy read as another.
- `kept_same_id_copy`: every different same-id kept claim detects a copy,
  including lower claim numbers. `earlier_claim_counterexample` shows why a
  number threshold misses this evidence.
- `lands_once_never_lost`: for one copy, once its attempted slot is occupied,
  retaining a landed claim or successfully claiming again after `notLanded`
  leaves the claim at exactly one slot. The successful new create is a premise;
  this does not prove eventual success under contention.
- `claims_eq`: ledger replay computes exactly the shared record fold.
- `settle_from_floor`: the same safety and copy detection hold using the
  actual ledger in any audience's honest floor snapshot, without claim-number
  uniqueness. `settle_from_floor_single` retains the single-copy equivalence.
  With `claim_floor_check`, kept-slot reads are also genuine.

Three checked counterexamples explain the evidence:

- `copies_fool_number_only`: two copies claim slot 41 with number 1 and
  different bytes; copy B wins. With floor 42, number-only settlement wrongly
  confirms copy A. Digest settlement returns `copy`, including when A's bytes
  were recreated below the floor.
- `state_alone_cannot_settle`: a batch can land and have its value overwritten,
  or lose its slot to another device, leaving identical final state. Looking
  only at that state can duplicate a landed batch or drop an unlanded one.
- `admin_state_cannot_settle`: a store rename followed by a rename back
  leaves the same ledger whether this device's rename landed or another
  device's did. Settling by presence loses the successful outcome of the
  original rename. The per-device records distinguish the histories.

**Another copy using this device id.** Each copy records an attempt before
sending it and creates only what it recorded, signed with its id.

- `foreign_slot_means_copy`: from empty storage, a slot signed with a copy's id
  whose bytes that copy's journal never attempted was created by another copy
  using the same id.

## What a pass reports

[`ReadPass`](sequence/CovenSequence/ReadPass.lean) models what one pass's read
forward reports. The pass reads from its next slot until a slot is missing or
can't be read or applied, the stop, then reads the floor: its first kept slot
and its `at`, the slot its writer had applied through. The floor's first kept
slot gates the commit. Checking the floor itself is `Settle.FloorAnnounced`
against the slots the device holds, which needs them through `at`. A missing
slot is damage when a later slot is known to exist: an existence check found
it, or it is at or below the floor's `at`.

- `checkFirst_blames_honest_floor` is a checked counterexample to checking the
  floor before reporting the stop. `Settle.lostStore` with slot 2 lost: the
  device has applied slot 1, misses slot 2, and the floor, honest in the store in
  good order, names the announcement at 2. Checked against slot 1 it is refused,
  and the pass reports the floor instead of the gap.
- `gap_report_wins`: a stop at or below the floor's `at` is reported, with the
  slots before it applied, whatever the check against the shorter history says.
- `floor_blamed_only_when_held`, `honest_floor_never_blamed`: the floor is
  reported as damaged only once the slots read reach its `at` and the check
  against them fails; storage in good order never fails it.
  `pass_eq_checkFirst`: a stop past the floor's `at` reads as before.
- `gap_below_at_is_damage`: in storage in good order every slot from the first
  kept slot through the head is present, so a gap below the floor's `at` is
  damage.

`locateEnd` models the end search: checks at `start + 1`, `start + 2`,
`start + 4`, … until one misses, then halving.

- `end_search_sees_an_end`: storage whose sequence ends at the search's answer
  answers every check the search makes the same way, so the search answers the
  same there. A missing slot a check lands on is therefore not known to be a gap;
  a later check or a floor's `at` makes it known.
- `locateEnd_finds_end` (with `climb_exact` and `narrow_exact`): when the slots
  after `start` are present exactly up to `e`, and `e` is less than `2 ^ 64`
  past `start`, the search returns `e`. The doubling brackets `e` in at most 64
  checks and the halving then narrows the bracket to it in at most 64 more.
- `checked_gap_reads_as_end`: from slot 10 with slots to 20, a gap at 12, which
  a check lands on, reads as the end at 11; a gap at 13 is passed over by the
  check of 14 and reported.

## The floor a reload checks

[`ReloadFloor`](sequence/CovenSequence/ReloadFloor.lean) models the floor a
reload reads before it commits. The reload has loaded its starts and the kept
slots after them to its target. A floor whose first kept slot has passed the
slot after the lowest start sends it to load again from that floor. Otherwise
the floor's check is `Settle.FloorAnnounced` against the history the reload
holds: through the target, then the kept slots it reads through the floor's
`at`.

- `atTarget_refuses_honest_floor` is a checked counterexample to checking at the
  target. In `Settle.lostStore`, slot 1 renames the store and slot 2 announces a
  snapshot covering it; the honest floor names that announcement, with `at` 2.
  A reload activated by slot 1 targets slot 1; checked against the slots
  through 1, the floor is refused.
- `honest_floor_installs`: reading the kept slots through the floor's `at`
  first, a reload in storage in good order never refuses an honest floor.
  `install_eq_atTarget`: for a floor whose `at` is at most the target, the two
  checks are the same.

## Checking a floor from the history a device holds

[`FloorCheck`](sequence/CovenSequence/FloorCheck.lean) states where the history
a floor is checked against starts. A device has applied the slots through
`through`. While the floor's first kept slot is at most `through + 1`, the check
reads the kept slots after `through` up to the floor's `at`. Once the first kept
slot has passed `through + 1`, it starts from the floor's store snapshot, whose
coverage reaches at least `first - 1`, and reads the kept slots after that
coverage. `checkFrom` is the slot the check starts after. The history through
`checkFrom` is the device's own or the store snapshot's; the model takes the
snapshot's body as honest (`Good.floor_body`).

- `check_reads_kept`: every slot the check reads is at or after the floor's
  first kept slot. `check_from_floor`: in storage in good order each of them
  is present and genuine, and the floor's check, `Settle.FloorAnnounced`,
  accepts an honest floor.
- `history_needs_deleted_slot` is a checked counterexample to reading forward
  from the device's own slots. In `compactStore`, slot 1 adds a member, slots 2
  and 3 announce snapshots covering 1 and 2, and the floor names the one at 3,
  with first kept slot 3; slots 1 and 2 are deleted. A device at slot 1 would
  need slot 2, and against slot 1 alone the floor is refused. Starting from the
  floor's store snapshot, it reads only slot 3 and accepts the floor.
- `check` runs the floor check as the JSON runner does: it reads the slots after
  `checkFrom` through the floor's `at` in order, reports the first one missing,
  and otherwise accepts or refuses the floor. `honest_floor_accepted`: in
  storage in good order it accepts an honest floor.

## Settling a lost floor reply

[`RetentionConfirm`](sequence/CovenSequence/RetentionConfirm.lean) models what
retention concludes when the reply to its conditional floor write was lost. The
floor's revisions are a log (`Log`): each write makes a new revision, and none
moves the first kept slot back, as `StoreStep.writeFloor` requires. A write
conditional on revision `previous` landed when it directly follows `previous`
in the log (`Landed`). Having read and checked the current floor, `confirm`
answers `write` when it is still `previous`, `confirmed n` for a changed floor
whose first kept slot `n` reaches the write's own, and `lost` otherwise.

- `landed_confirms`: a landed write is always confirmed, at the current floor's
  first kept slot, however far later floors moved it.
  `unconfirmed_not_landed`: so `write` and `lost` mean the write is not in the
  log. A confirmed write need not have landed.
- `confirmed_deletes`: the slots below a confirmed `n` are below the floor the
  device read, and deleting them is a `StoreStep.delete` then and after any
  later storage steps, since the first kept slot never moves back.
- `moved_floor_confirms`: Ana's write moves the first kept slot from 3 to 5 and
  lands; Ben's later floor moves it to 9, and Ana confirms at 9. Had Ben's floor
  replaced the one she read first, her write could not land, and she still
  confirms at 9.

The model takes the revision a floor read returns as storage's current one, and
the log's revisions as distinct: a write makes a new revision, and a revision
never reappears.

## Which announcements a device keeps

[`Announcements`](sequence/CovenSequence/Announcements.lean) models the
announcements a device holds and the original slots it rebuilds them from
([§16.8](../coven.md#168-forgetting-announcements)). An announcement has a slot,
an audience, a coverage, a purpose and the body it names. `accepts` is
`announcement_allowed`: coverage below the slot, nothing of the audience past
it, and the state allowing it. `activation` is a raise's or reset's check in
`checked_change`: it finds the announcement by its slot, needs an earlier one of
its kind, the state's permission, a boundary not already in place, and nothing
of the audience past the named coverage. `Checks` stands for what reads the
store's state rather than its announcements; admin entries are never
forgotten, so every device sees the same state there.

`forgettable` is the rule: no applied raise or reset names the announcement,
another of its audience covers more, its slot is below the floor's `first`, an
ordinary one of its audience is newer, no journaled floor capture names it, and
no rebuild reads it. `rebuildReads` lists the slots rebuilds read: for each
circle that is below the store and not deleted, the store's newest ordinary
announcement at a version of at most the circle's. A `Device` applies entries
and, at any point, forgets what is forgettable at the `first`, journal and
rebuild slots it is given. It stores each admin entry's original and an
announcement's only when it accepts it.

- `answer_same`: after any interleaving of entries and forgetting, a device
  accepts or refuses every later announcement, and applies or refuses every
  later raise or reset, exactly as a device holding every announcement does.
  `Agree` is what the two share: the same raises and resets, a part of the same
  announcements, one at least as large for each coverage the full history
  holds, and for each one missing, one covering more. `agree_forget`: an
  announcement at its audience's greatest coverage is never forgettable, so
  forgetting keeps that.
- `rebuilt_same`: replaying a device's stored originals gives its history.
  `filtered_replay` is the step that needs care: the replay without the
  forgotten announcements meets each raise or reset before everything that
  later passed it. `forgettable_of_passed` shows a raise named an announcement
  it would now accept only if that announcement is forgettable too, since the
  one that passed it is older than an ordinary one, below `first` and covers
  more than the named one, and no raise or reset applied after the passing one
  could name it.
- `equal_coverage_kept`: Ana's raise announcement at slot 50 and Ben's ordinary
  one at 60 both cover 49. Her raise entry at 120 applies on a device holding
  both and is refused on one without hers; hers is not forgettable.
- `floor_names_kept`: a floor write meeting `Retention.floor_write_rules` from
  a `first` at least the device's names announcements at or after it, since
  each announcement is after its coverage. `newest_kept`: the newest
  announcement of an audience, and the newest usable one, are never
  forgettable.
- `rebuild_announcement_kept`: the announcement a rebuild starts from is never
  forgettable while its circle stays below the store and not deleted. The
  other theorems quantify over the rebuild slots, so `answer_same` and
  `rebuilt_same` hold with this reason as with the others: a device that keeps
  more answers the same.
- `listing_spares` and `listing_finds_superseded`: a listing of a device's
  snapshot folders deletes no body a kept announcement or an open operation
  names, and every other body it lists.

The model's journal keeps only ordinary announcements, as a floor capture names
only ordinary ones. It takes slots in increasing order and does not model the
state checks, the provider or the order in which a pass runs its steps.

## JSON runner

Build with `cd spec/proofs/sequence && lake build`; the executable is
`.lake/build/bin/sequenceRunner`. It reads one JSON object to EOF on stdin and
prints one JSON answer. Invalid input reports an error on stderr and exits 1.
`SequenceTests.lean` checks cases through the JSON entry point during the build.
All fields below are required; identifiers are natural numbers. The `apply`
case bounds members by `member_count`; slots, claim numbers and write numbers
are positive.

**`settle`** calls `Settle.settle`. Bytes are opaque strings compared exactly.
`recorded` supplies `device`, `number`, `slot`, `bytes`, and `digest`. A present
`current` supplies authenticated `device`, `number`, and `bytes`; `null` means
absent. Equal bytes must have equal authenticated metadata. `last_claims`
contains `[device, number, digest]` triples; omit a device with no included
claim rather than giving it a zero number or invented digest. Duplicate device
records are rejected.

Digest strings are supplied SHA-256 tokens: the runner compares them but does
not implement SHA-256, validate its encoding, or hash the JSON representation.
Fixtures may use symbolic tokens. Authentication, hashing complete sealed
bytes, and path binding are caller responsibilities. The floor is read after
the slot. Below `first_kept`, only the snapshot record decides.

```json
{"kind":"settle","recorded":{"device":0,"number":1,"slot":41,"bytes":"copy-a","digest":"hash-a"},"current":null,"floor":{"first_kept":42,"last_claims":[[0,1,"hash-b"]]}}
```

Answer: `"copy"`; the other answers are `"landed"`, `"notLanded"`, and `"retry"`.

**`read`** calls `Device.checkedSlots`. When `stopped` is true it applies
nothing and requests no reload. Otherwise it uses `Reader.checkedSlots`, whose
next-slot check supplies the buffer and floor premises of `ReadStep.apply`. It returns the consecutive
fetched prefix after `applied`, stopping at the first gap, irrespective of
fetch order. If the floor has passed the next slot it requests reload and
applies nothing. This projects ordinary application with the audience key;
key waits, prepared-body loads and snapshot contents are outside this case.

```json
{"kind":"read","stopped":false,"applied":1,"fetched":[3,1,2,5],"first_kept":2}
```

Answer: `{"reload":false,"apply":[2,3]}`.

**`readPass`** calls `ReadPass.pass`. `applied` is the device's applied slot,
`stop` the slot reading stopped at (after `applied`), `why` `"missing"` or
`"fault"`, `known` the last slot an existence check or a read found, and
`floor` null or `{first, at, named}`, where `named` lists the slots that
announced the floor's snapshots. The floor check passes once the slots held
reach every named slot. The answer is `"reload"`, `{"synced":n}`,
`{"stops":n}` or `"floorDamaged"`.

```json
{"kind":"readPass","applied":3,"stop":5,"why":"missing","known":4,"floor":{"first":1,"at":7,"named":[7]}}
```

Answer: `{"stops":5}`.

**`floorCheck`** calls `FloorCheck.check`, a settling claim's check of the floor
it reads (§5.2, §5.3). `applied` is the device's applied slot, the slot before
its claim's; `covers` is the coverage of the floor's store snapshot, at least
`first - 1`; `floor` is `{first, at, named}`, with `named` the slots that
announced the floor's snapshots; `missing` lists the distinct slots absent from
storage. The floor check passes once the history held reaches every named slot.
The answer is `"accepted"`, `{"stops":n}` or `"floorDamaged"`:

```json
{"kind":"floorCheck","applied":3,"covers":6,"floor":{"first":7,"at":7,"named":[7]},"missing":[4,5,6]}
```

Answer: `"accepted"`; the floor's first kept slot has passed slot 4, so the
check starts after its store snapshot's coverage and never reads 4 to 6.

**`endSearch`** calls `ReadPass.locateEnd` from `start` over the distinct
`present` slots; every other slot is missing. It returns the end found:
`{"kind":"endSearch","start":10,"present":[11,13]}` gives 11.

**`retention`** gives one audience's floor: `Retention.newest` for the name and
`Retention.floorFirst` over `Retention.choose` for `first`. Input `snapshots`
are newest first, with distinct positive `slot`, preceding `covers`, provider
upper-bound `announced_at`, and boolean `usable`. Coverage must not decrease
with announcement order. `grace` and `time` use the same duration ticks, and
`first` is the current floor's. The answer is
`{"named":{"slot":...,"covers":...}|null,"first":...}`. Rust supplies
applicability and provider timestamps from its real daily snapshots.

**`floor`** calls `Retention.floorStart`. Its `coverage` array contains one
number or null per introduced audience. It returns the first kept slot, or
null when coverage is incomplete: `[10,null]` gives null; `[10,8]` gives 9.

**`apply`** evaluates `ledgerAt`, `audAt` and `rowsAt` at every prefix, using
`Authority.withRoles` and `LiveClaims.winner`. The initial state supplies the
creation `name` and membership containing an admin; initial devices refer to
active members. Each audience gives its starting `version` and the devices
allowed to write to it. The runner uses
§10's per-member authority: members register their own fresh devices, remove
their own devices and replace their own keys. Admins may also remove another
member's device or replace that member's keys; only admins add or remove members
and change roles or the store name. Registration must be authored by the fresh
device itself, including for admins. Other admin entries need an active author. Membership
must retain an admin, and an admitted member id must never have been a member.
Conversion is the identity; audience writer lists and
conversion are fixture parameters, not additional store rules.

Slots occupy positions `1..slots.length`. Each has `device`, positive `claim`,
and `digest`. Repeated or decreasing numbers are accepted because copies can
share an id. An admin slot is
`{"kind":"admin","device":0,"claim":1,"digest":"slot-1-hash","entry":{"kind":"removeDevice","device":1}}`.
Entry kinds are `renameStore` (`name`), `admitMember` (`member`, `role`,
`device`, the device the admission registers for the member, never its author),
`removeMember` (`member`), `changeRole` (`member`, `role`),
`addDevice` (`device`, `member`), `removeDevice`
(`device`), `replaceKey` (`member`), and `circleMember` (`audience`, `member`,
`allowed`). Circle changes require an active audience member and an active
target member. Roles are `"admin"` or `"member"`.
For example, device 2 registers itself to current member 1 with
`{"kind":"admin","device":2,"claim":1,"digest":"registration-hash","entry":{"kind":"addDevice","device":2,"member":1}}`.
Device 2 must have no existing binding, even a removed one. It may register when
member 1 has no active devices. The enclosing `device` must equal the entry's
`device`; another author is consumed as a refused admin slot. The JSON input
supplies already-authenticated entries: it carries no signatures, and the
current member-key signature proving device 2 speaks for member 1 is assumed
verified before this call. A refused registration records its claim number and digest but
does not bind the id; a subsequent valid self-registration can succeed.
Audience `writers` are member ids. A writes slot carries `key_gen` and at most
one part per declared audience; each part supplies its authored `read_through`:

```json
{"kind":"apply","member_count":1,
 "initial":{"name":"Household","members":[{"member":0,"role":"admin","key_gen":0}],
            "devices":[{"device":0,"member":0}]},
 "audiences":[{"audience":0,"version":1,"writers":[0]}],
 "slots":[{"kind":"writes","device":0,"claim":1,"digest":"slot-1-hash","key_gen":0,"parts":[
   {"audience":0,"version":1,"read_through":0,"unique_claims":[7],"owner_creations":[[9,1]],"writes":[1],
    "rows":[{"row":9,"live":true,"value":7,"inputs":[[1,1]]}]}]}]}
```

`unique_claims` and the row ids in `owner_creations` supply separate permanent
first-slot claim namespaces. Each `owner_creations` pair `[row, write-number]`
names an original generation-zero creation, before permission filtering; its
number must occur in the part's `writes`. `Owners.establish` chooses among
counted creations by slot and then write number, retaining the original author
and its bound member. `writes` lists original write numbers in that part. `rows` supplies
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
`slot`, `ledger`, and `audiences` for that prefix. The ledger contains the
current `name`, `members` (`[member, role]`), `retired` member ids, all registered `devices`
(`[device, member]`, including removed devices), `removed` device ids, `key_generations` (`[member, generation]`
for the whole member universe), included `last_claims` (`[device, number, digest]`), and
`refused` admin slot numbers. Each audience contains `audience`, `version`,
`unique_claims` and `owner_claims` (`[key, first-slot]`), `owner_creations`
(`[row, slot, write-number, device, member]`), `live_winners`
(`[value, row-or-null]` for every value retained in merged rows), `rows` in the
input row shape, and `refused` part slot numbers. Tables are sorted by id;
refusals are ascending. The example yields `last_claims:[[0,1,"slot-1-hash"]]`,
`unique_claims:[[7,1]]`, `owner_claims:[[9,1]]`,
`owner_creations:[[9,1,1,0,0]]`, `live_winners:[[7,9]]`, and no
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

**`keys`** in [`SequenceKeys`](sequence/SequenceKeys.lean) calls `Keys.publish`,
`accepted`, `exposed`, `firstKey`, `sendingKey`, and `metadataKey`. `slots` is
the key projection at positions `1..slots.length`: an `introduce` event carries
`audience`, `key`, `hash`; a `copy` carries `audience`, `key`, `member`,
`credential`; `other` preserves unrelated positions. `held` supplies
`{audience,key,hash}` custody records. `current` supplies
`{audience,member,credential}` for current audience members, omitting excluded
members. Hashes and exact public credentials are equality-preserving tokens.
`writer` and `store` identify the selecting member and store audience;
`audiences` and `keys` (`{audience,key}`) select the requested observations.
Duplicate custody, binding or query records are rejected.

The answer contains `keys` with `accepted`/`exposed` flags, `selection` for
each audience with `first` (key or null) and `sending` (key, unavailable key,
or `removed`/`notAllowed` error), and `metadata` (key or null). `first` is
restricted to current members. `sending` also permits the old-circle-original
case; eligibility of those originals is checked outside this projection.

**`keyCustody`** calls `KeyCustody.save` and `ready` with `audience`, `key`,
nullable `held`, and `fresh`. The material tokens also stand for their public
commitments in this case. The answer is `{audience,key,hash}`, reusing `held`
when present. `key_custody_preparation_matches_lean` compares real reopened
preparations before and after the custody commit; cryptographic hashing itself
is outside this projection.

**`rotation`** takes the same fields and `rotation:{audience,key,hash,members}`.
The fresh generated key must already be in custody. The runner requires a
current writer, an accepted held store key, and a member list covering every
current audience member. It rejects reuse of an introduced or tentatively
copied key. Output contains the exact copy/introduction `slots`, final
`through`, and the key observations `after`. Membership is fixed across this
plan; interleaved membership changes require another prefix and selection.

**`memberReplacement`** calls `MemberKeyReplacement.resolve` with distinct
`active` and `candidate` tokens and a nullable `current` binding. It returns
`paused`, `active`, and nullable `candidate` after that custody decision.
`member_replacement_binding_matches_lean` compares real competing replacements,
including the SQL write gate and custody, with this result.

**`queueReload`** supplies one of four `scenario` values. `reset` carries the
actual queue's write numbers, schemas, authored-read slots and row keys, the
installed rows and reset, and a subsequent write. `breakingRaise` carries the
rows before and after loading, a candidate key and the queued schema versions.
`newlyReadable` carries membership and row keys before and after loading.
`selfReplacement` carries counted replacement slots, authored-read prefixes and
credential generations. The runner evaluates `QueueReload` with these inputs;
SQL execution, signature checks and extraction from snapshots happen in Rust.

**`circleLifecycle`** runs circle histories through
`CircleLifecycle.effect`, in [`SequenceCircles`](sequence/SequenceCircles.lean).
`people` lists distinct member ids, `active` those that start as members (the
rest were never added), and `circles` distinct circle ids; no circle exists at
the start. `histories` is a list of histories, each a list of entries at slots
1, 2, and so on: `create` and `rename` (`author`, `circle`, `name`), `delete`
(`author`, `circle`), `add` and `remove` (`author`, `circle`, `member`), and
`removeMember` (`member`), the store's removal. Every id must be listed. The
answer has one list per history, with one entry per slot: `outcome`
(`applied`, `unchanged`, `notAllowed`, `goneCircle` or `goneMember`), and the
state after the slot, `standing` (`[member, "absent"|"active"|"removed"]` for
each of `people`) and `circles` (`[circle, null]` for one not made, else
`[circle, {name, members, deleted}]`, with sorted `members` and `deleted` the
deleting slot or null).

**`inviteLifecycle`** runs one invite's events through `InviteLifecycle`'s
steps under `Rules.rust`, in [`SequenceInvites`](sequence/SequenceInvites.lean).
`events` is a list of `pass`, `send`
(the joiner writes its request), `sendUsed` (it writes one whose keys a member
already holds), `approve`, `decline`, `cancel`, `retry`,
`ack`, `ackOp`, `administer`, `expire`, `demote` and `remove` (another admin's
slot, applied at the next pass; for `remove`, his pass ends this device's
requests at once, the owner's other device takes the recorded share back,
and a `send` after it finds the gone marker, or no share, and sends
nothing), `raceCancel` and `raceExpire` (a pass with the cancel or the expiry
committed while it reads the request). A pass applies
landed slots, takes back a share whose end an earlier pass recorded, runs the
admin change, then advances the invite until the attempt returns; storage
always answers. The answer has one entry for the
invite just made and one after each event: `result` (`ok`, `refused` or
`skipped`), `invite` as listed (`granting`, `waiting`, `requested`,
`approving` or null), `binding` and `op` as `coven_operations` lists them
(`running`, `landed`, `rejected`, `stopped` or null), `pending` (`auto`,
`app`, `never` or null), `journal`, `request` (the request object is in
storage), `shared` (the invite's account is shared) and `accepted`, the app's
steps a write accepts now.

## Rust comparisons

The ignored differential tests use `COVEN_SEQUENCE_LEAN` to run the same JSON
cases through `sequenceRunner`. `scripts/check.sh` selects each comparison when
its Rust crate or the sequence package changes and checks the exact test names.
Comparisons enumerate bounded cases or exercise publication histories; merge
and settlement comparisons also include deterministic seeded cases.

- `coven-database` calls the durable claim settlement API and checks its saved
  verdict. Complete-byte digests are computed in Rust; the kept-slot inputs have
  passed the separate sync authentication boundary. `coven-sync` also exercises
  kept-slot settlement with signed equal bytes, missing slots and different
  same-id bytes at lower, equal or higher claim numbers.
- `coven-sync` reads real stored slots while retention publishes a floor, with
  every fetched slot still present. An unavailable required snapshot body exposes
  the reload decision before installation: the applied slot must remain unchanged.
  At or above the floor the reader applies its consecutive fetched prefix without
  reading that body. These cases compare the floor gate, not snapshot contents or
  progress under missing-body storage failures.
- `coven-sync`'s `sequence_read_pass_matches_lean` loses one slot from a store
  whose floor's `at` is past it, or one written after the floor, or one with no
  floor, and compares what the real pass applies and reports with `readPass`.
  `sequence_end_search_matches_lean` compares the real end search over storage
  missing one slot with `endSearch`.
- `coven-sync`'s `sequence_settle_floor_matches_lean` loses Ana's claim to
  Ben's snapshot announcement while her create is held. Ben writes zero to two
  slots after it and writes a floor; in half the cases he snapshots again past
  her claim slot, so the floor's first kept slot passes it. With one of the
  slots after her claim slot lost, or none, it compares what her settling pass
  reports with `floorCheck`.
- `coven-database` applies write slots and compares visible unique winners,
  permanent owner creations (slot, write number, author and member) and consumed
  cut-off refusals at every prefix.
  Histories include competing rows, deletion releasing a value, batches and all
  three cut-offs. `coven-sync` checks administrative admission and then applies
  the result through the database, comparing the name and member/device/key
  state after each slot. Cases include renames before and after demotion or
  promotion, repeated names, and attempted reuse of a removed member id.
  Already-satisfied operations have no name or membership effect in either
  implementation; Rust avoids claiming a new slot for them.
  Registration targets the slot author in Rust's
  format, so cross-device registrations are tested in the Lean JSON tests.
- `coven-sync`'s `circle_effects::tests::circle_lifecycle_matches_lean`
  runs `checked_change` over circle histories from a store with two admins
  and a member, each with a device, and an id never added, and compares each
  slot's verdict, circle names, members, deletion slots and member standing
  with `circleLifecycle`. Every pair of entries over two circles and four ids
  follows a fixed start that makes one circle with two members and one with
  one; 256 seeded histories of 14 entries start from no circles.
- `coven-merge` calls `SlotPast::contains` for every pair of supplied writes,
  including batches and shuffled input order. Histories satisfy the model's
  own-number publication and monotone authored-read premises.
- The four `sequence_queue_*` tests pass their actual reload inputs to the
  runner. Reset cases vary queue length; the other cases compare write
  permission after a breaking raise, newly readable circle rows, and original
  authentication across two credential replacements. JSON tests also vary
  authored-read prefixes, candidate keys and membership to check that those
  inputs affect the result.
- `coven-sync`'s `history::tests::sequence_key_selection_matches_lean` compares
  the actual history selectors, acceptance and exposure over custody subsets,
  introduction prefixes, opposite id/introduction ordering, repeated copies,
  equal/conflicting commitments, mismatched material, removed/unbound members,
  retired credentials and departed-circle originals. Membership facts are
  supplied at the selector boundary; the administration comparison covers
  their application.
- `sequence_sync::sequence_keys::tests::sequence_rotation_matches_lean` calls
  real removal, replacement and rotation operations. It compares the published
  slots and selected keys, opens each recipient's generated box, checks the
  outer metadata key, and reconstructs history from the database's retained
  originals. It covers exposed-store-key publication, consecutive store/circle
  rotations and replaced credentials. `rotation_does_not_need_preceding_circle_material`
  additionally exercises the real installer without old circle custody.
- `coven-sync`'s `invite_lifecycle_matches_lean` drives a real invite on Ana's
  device through each case's events: Ben demotes or removes her device, Carol's
  phone sends the request, a pass is held at its read of the request while
  the cancel or the expiry commits. After each event it compares what Ana's app
  sees, the journal row, the request in storage, and which steps a write,
  rolled back after, accepts. Cases name each gap and fixed bug, then 48
  seeded sequences over every event.

Authentication, row/source validation and the merge are outside the sequence
runner's abstraction. The comparisons supply their inputs at those boundaries;
they do not replace the separate byte-format, merge or queue tests.

## Assumptions and what is not modelled

- Entry signatures are checked outside the model against the member's current
  signing key before the slot. In particular, a fresh device's self-registration
  is authenticated as speaking for the member named by `addDevice`; naming a
  member alone does not prove that authority. The JSON runner assumes this
  check has succeeded and does not model signatures. Store creation supplies
  the initial member and device bindings.
- A write's signature is checked outside the model too, with its device
  owner's signing key as it stood at the write's `read_through`; the first
  device's writes from before storage was set up, at `read_through` 0, are
  checked against slot 1's record of the store's creation. The model checks
  the write permission at `read_through` (`authoredPermission`), not the key
  that signed it.
- A floor read is a success or an absence. A floor read that fails is no step
  of the model, so a device does not apply, reload or skip anything on it
  (§6.1). A slot read that fails stops a pass like a slot it can't open or
  apply (`ReadPass.Stop.fault`). Telling a provider's error from its "not
  found" is Rust.
- Snapshot bodies are honest: ordinary bodies contain the covered state;
  raise bodies contain migrated rows; reset bodies contain the chosen rows.
  A circle's raise body reads only the circle's rows and the store's
  (`migrate_reads`); the Rust side holds the app's migrations to it by running
  them again per audience (coven.md §18.3).
  Coven's raise body differs from that for a part that lands after its raising
  device migrated and whose converter isn't the migration (`RaiseBody`).
  Floor reload uses ordinary snapshots; prepared bodies are consumed at their
  matching entries. Direct floor loading of a prepared body is not modelled.
- A claim lands only when storage holds what rebuilds read once its slot is
  applied (`StoreStep.claim`'s `hrebuild`), as it lands only with its
  preparation body present.
- Admin slots and key copies are readable by every member device. So are a
  slot's device, claim number and each part's audience: they are sealed under
  the store key, which every member holds. Public observations include slot
  kinds and store-key ids, key-copy routing with exact public sealing keys,
  and object kinds, format versions and framing (format.md D9). These are fixed public inputs
  to the model's encryption observations.
- Unique values and owner rows are per audience; uniqueness across audiences is
  not modelled.
- Ordinary receive parts at a version above their audience's are refused.
  Captured publication and migration effects are separate in `RaiseQueue`;
  each abstract part contains originals with one authored prefix. Decoding a
  batch with mixed authored prefixes into individual originals is not modelled.
  A device whose app cannot open a slot stops before it (`unopened_stops`).
- Reload models changed readable audiences and their local write gates.
  Delivery failures, circle invitation and the policy choosing the followed
  set remain abstract; a newly readable audience must load its own prefix.
- `Model` takes authenticated claim numbers and complete-slot digests as
  inputs (`Rules.number`, `Rules.digest`). No SHA-256 implementation or
  cryptographic security proof is included. Hash collision resistance and
  path binding appear as explicit premises of `settle_correct'` and
  `settle_from_floor`. Only the single-copy theorems assume `Own` or its
  equivalent journal facts; `ClaimJournal` derives `Own` from one device's
  journal rules.
- An operation's action is fixed by construction. The verdict function `check`
  that `Operation`'s steps take is one function of the slots for the operation's
  whole life, and its entry bytes name
  the operation and the slot only (`ours n`), so a retry after a lost race
  claims the same action at another slot (§11.2). That the Rust journal keeps
  the target and the approved keys fixed is tested.
- The refusal to remove the member whose account holds the store, or to move
  that member's access (`StoreOwner`, §10.1), is made before a claim and is not
  an entry rule. Slot 1 carrying the key that verifies it, and the store id it
  names matching its path (§10.3), are checks of bytes outside the model.
- The invite lifecycle has its expiry as a step, with no clock in it: the wake
  at the expiry time and the write it makes (§13.2) are Rust. A joining device
  sends no first request once the invite has expired by its own clock, and the
  decline marker is kept a week past the expiry, so a request can find the
  marker gone only from a clock lagging the inviting device's by more than that
  week, which coven assumes none does.
- Not modelled:
  - concrete authority policies beyond membership, role changes, store renames,
    the runner's device/key actions, circle membership changes and store resets,
    and, in `CircleLifecycle` apart from `Model`, a circle's own entries;
  - orphaned batch objects and provider request timing;
  - the merge's internals, source-verdict calculation and converter callbacks,
    which the merge and queue packages cover. `Inputs` extracts merged
    uniqueness setters; queue admission and the core batch reset filter are
    parameters. The local queue makes the authored-read reset gate explicit.
