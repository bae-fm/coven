# One store sequence

The Lean package in [`sequence/`](sequence/) backs the normative sequence rules:
[claims and application (§5)](../coven.md#5-the-sequence),
[unique claims (§9.2)](../coven.md#92-keys-and-unique-values),
[owner selection (§9.6)](../coven.md#96-owner-tables),
[admin prefixes (§10)](../coven.md#10-members-roles-and-devices),
[cut-offs (§14.1)](../coven.md#141-the-cut-off),
[audience keys and revocation (§12)](../coven.md#12-keys-and-encryption),
[audience reads (§15.6)](../coven.md#156-writes-into-circles),
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
- **Settle from the per-device claim number and slot digest.** Settling a deleted slot's claim
  from the snapshot's state alone loses or duplicates writes, and misreports
  a completed admin operation. A number without a digest can confirm another copy's
  bytes after deletion
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
  every audience. That is active members and roles, retired member ids, devices
  and their removal, member key generations, `aux`, key copies and refused admin slots. It also holds, per
  device, the greatest included claim number and the SHA-256 of that slot's
  complete bytes. Later slots replace the digest on an equal number; lower
  numbers preserve the existing record. A device with no included claim has
  `none`.
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
every slot records, and only slots with a part or announcement for `A` change
`A`'s other two parts. A slot or part invalid at
its position is refused: it is consumed, and only its refusal is recorded.
Removing a member retires its id permanently and removes every device registered
to it. Re-inviting that person requires a fresh member id. An active member
whose devices were all removed keeps its member id when registering a fresh device.

These are the validity rules:

- **An admin entry** requires an active author, except that a fresh device may
  author its own `addDevice` entry. Registration requires a current member and
  an id with no historical binding. Only the installation itself may register
  that id, even when its member is an admin. The member's current-key signature
  authenticating the fresh device is checked outside the model. Other entries
  still require an active device and the supplied authority policy. Adding a
  member requires an id that is neither active nor retired.
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
  number and digest, and every device agrees by `prefix_determines_state`.

A batch is cut off when it was "made under a replaced key". A batch that the
member's other devices make under the new key counts.

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
  include every current audience member. Store creation's introduction-before-
  creator-copy exception is represented by ordinary sequence events.
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
key identity and public commitment from secret material in custody.
`saved_before_prepared` requires saved material before recording its commitment;
`restart_reuses_saved` keeps that material on restart, and `save_keeps_other`
preserves other keys. `journal_depends_only_on_commitment` makes the journal's
observation independent of secrets with the same commitment. Rust tests also
scan the physical SQLite files, whose encoding is outside the model.

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

[`Membership`](sequence/CovenSequence/Membership.lean) proves permanent retirement
independently of the authority policy:

- `retired_member_refused`: no valid addition reuses a retired member id.
- `step_retired_member`, `retired_member_stays`: retirement and absence hold
  together after every subsequent slot and at every later prefix.
- `removed_member_never_active`: once a positive slot applies a member removal,
  that id is retired and absent at every later prefix, including after attempted
  additions, role changes, registrations and key replacements.

The ledger retains retirement history, and member addition checks it regardless
of the supplied authority policy. The runner examples refuse both reuse and a
fresh device registration for the retired id; separate examples admit a fresh
member id and preserve recovery for an active member without devices.

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
landed under the existing claim protocol; it precedes the raise slot (§18.5).

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

[`AccessKeyReplacement`](sequence/CovenSequence/AccessKeyReplacement.lean)
models the S3 replacement owner's delivery, child claim and undo. A retry
whose backoff has not ended preserves the journal (`not_due_unchanged`);
a successful due delivery finishes (`due_delivery_finishes`). A refused child
claim reaches its parent's undo (`claim_rejection_reaches_undo`). Failed undo
keeps both the rejection and cleanup failure (`failed_undo_keeps_both_causes`),
including missing admin access after reopening (`reopened_undo_needs_admin`).
Only successful cleanup with admin access ends undo
(`undo_finishes_only_after_success`). Provider outcomes and retry eligibility
are inputs; this does not prove provider progress or Rust task ownership.

[`MemberKeyReplacement`](sequence/CovenSequence/MemberKeyReplacement.lean)
models the custody decision after a refused self-replacement's child claim is
undone. `unheld_keeps_pause` retains the pause under an unheld binding;
`candidate_installs` selects installation of a winning candidate, still owing
its restore code. `undo_requires_active` permits release only under the held
active pair. Public pairs are equality-preserving tokens; custody writes and
restore-code persistence are outside this decision.

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
Different bytes signed by the same id with a number at or above `n` mean `copy`;
a different device means `notLanded`. Lower-number mismatches remain
`notLanded`, as in the single-copy model. Bytes recreated below the floor
never override its record. A read error supplies no settlement input.

- `Claims.witness`, `lastClaim_witness`, `claims_witness`: the number and
  digest in a record came together from an included slot of that device.
- `Claims.ge`, `lastClaim_ge`, `claims_ge`: the record's number reaches a
  positive `n` exactly when an included claim reaches it. These bounds do not
  assume unique or increasing numbers per device id.
- `settle_correct'`: for any number of copies, `landed` implies the recorded
  slot holds this copy's bytes; `notLanded` excludes that attempt; `retry`
  is equivalent to a slot above the head; and `copy` is equivalent to evidence
  of another claim at or above `n` in the source settlement reads.
  The digest premise says that matching complete-byte hashes identify the
  recorded bytes and slot path, including [D9's `after` header](../format.md#d9-sealed-objects).
  It is a cryptographic/encoding assumption,
  not a uniqueness assumption about claim numbers.
- `settle_correct`: under `Own`, the single-copy journal facts, `landed` is
  equivalent to the recorded claim being in the sequence, `notLanded` means
  its claim number is nowhere in the sequence, `retry` means the slot is still
  empty, and `copy` is impossible. These journal facts are not premises of
  `settle_correct'`.
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
- `admin_state_cannot_settle`: a member addition followed by removal leaves
  the same membership whether this device's addition landed or another
  device's did. Settling by presence loses the successful outcome of the original
  addition. Retrying cannot reuse the retired id. The per-device records
  distinguish the histories.

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

**`reselect`** calls `Reload.reselect`, the decision a reload makes after
deletion forces it to change its journaled selection (`Reload.lean`).
`old_target` is the journaled common target, `floor_at` the floor's
authentication prefix, and `covers` the starting coverage of every audience
already chosen. `missing` is the selection whose body was not found, as
`{slot,audience,covers}`, or `null` when the reload's slots fell below the
floor. `floor` lists the floor's announcements in the same shape, at most one
per audience. Every selection's `slot` is positive and above its `covers`.
When `missing` is `null`, or a floor announcement for the same audience is
later and covers at least as much, the answer is the new common target: the
largest of `old_target`, `floor_at` and `covers`. Otherwise it is `null`: no
floor announcement replaces the missing one. Authentication, and the reason the
caller is reselecting, are supplied by the caller.

```json
{"kind":"reselect","old_target":4,"floor_at":6,"covers":[5],"missing":{"slot":4,"audience":0,"covers":3},"floor":[{"slot":6,"audience":0,"covers":5}]}
```

Answer: `6`. With the floor's announcement at slot 6 covering only 2, the answer
is `null`.

**`apply`** evaluates `ledgerAt`, `audAt` and `rowsAt` at every prefix, using
`Authority.withRoles` and `LiveClaims.winner`. The initial membership must
contain an admin; initial devices refer to active members. Each audience gives
its starting `version` and the devices allowed to write to it. The runner uses
§10's per-member authority: members register their own fresh devices, remove
their own devices and replace their own keys. Admins may also remove another
member's device or replace that member's keys; only admins add or remove members
and change roles. Registration must be authored by the fresh device itself,
including for admins. Other admin entries need an active author. Membership
must retain an admin, and an added member id must never have been a member.
Conversion is the identity; audience writer lists and
conversion are fixture parameters, not additional store rules.

Slots occupy positions `1..slots.length`. Each has `device`, positive `claim`,
and `digest`. Repeated or decreasing numbers are accepted because copies can
share an id. An admin slot is
`{"kind":"admin","device":0,"claim":1,"digest":"slot-1-hash","entry":{"kind":"removeDevice","device":1}}`.
Entry kinds are `addMember` (`member`, `role`), `removeMember` (`member`),
`changeRole` (`member`, `role`), `addDevice` (`device`, `member`), `removeDevice`
(`device`), and `replaceKey` (`member`); roles are `"admin"` or `"member"`.
For example, device 2 registers itself to current member 1 with
`{"kind":"admin","device":2,"claim":1,"digest":"registration-hash","entry":{"kind":"addDevice","device":2,"member":1}}`.
Device 2 must have no existing binding, even a removed one. It may register when
member 1 has no active devices. The enclosing `device` must equal the entry's
`device`; another author is consumed as a refused admin slot. The JSON input
supplies already-authenticated entries: it carries no signatures, and the
current member-key signature proving device 2 speaks for member 1 is assumed
verified before this call. A refused registration records its claim number and digest but
does not bind the id; a subsequent valid self-registration can succeed.
A writes slot carries `key_gen` and at most one part per declared audience:

```json
{"kind":"apply","member_count":1,
 "initial":{"members":[{"member":0,"role":"admin","key_gen":0}],"devices":[{"device":0,"member":0}]},
 "audiences":[{"audience":0,"version":1,"writers":[0]}],
 "slots":[{"kind":"writes","device":0,"claim":1,"digest":"slot-1-hash","key_gen":0,"parts":[
   {"audience":0,"version":1,"unique_claims":[7],"owner_creations":[[9,1]],"writes":[1],
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
`slot`, `ledger`, and `audiences` for that prefix. The ledger contains `members`
(`[member, role]`), `retired` member ids, all registered `devices`
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

**`accessReplacement`** calls `AccessKeyReplacement.resume`. It takes `phase`
(`delivering`, `recording`, or `undo`), `due` and `admin` booleans, a nullable
`rejection` number for a refused child claim, and nullable `failure` and
`previous_failure` strings (`network` or `admin`). An `undo` phase also takes
its original `reason` number. The answer contains `retained`, nullable
`rejection`, and nullable `failure`: the operation and its durable pending
cause after the attempt. A finished operation has no durable pending cause.
The Rust comparison `access_replacement_retries_and_undo_match_lean` runs real
delivery retries and rejected-claim cleanup, including reopening without
retained admin access, and compares these observations.

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
  same-id bytes at equal or higher claim numbers.
- `coven-sync` reads real stored slots while retention publishes a floor, with
  every fetched slot still present. An unavailable required snapshot body exposes
  the reload decision before installation: the applied slot must remain unchanged.
  At or above the floor the reader applies its consecutive fetched prefix without
  reading that body. These cases compare the floor gate, not snapshot contents or
  progress under missing-body storage failures.
- `coven-database` applies write slots and compares visible unique winners,
  permanent owner creations (slot, write number, author and member) and consumed
  cut-off refusals at every prefix.
  Histories include competing rows, deletion releasing a value, batches and all
  three cut-offs. `coven-sync` checks administrative admission and then applies
  the result through the database, comparing member/device/key state after each
  slot, including attempted reuse of a removed member id. Already-satisfied operations
  have no effect in Rust; the corresponding structurally refused model slots
  also leave state unchanged. Registration targets the slot author in Rust's
  format, so cross-device registrations are tested in the Lean JSON tests.
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
- Snapshot bodies are honest: ordinary bodies contain the covered state;
  raise bodies contain migrated rows; reset bodies contain the chosen rows.
  Floor reload uses ordinary snapshots; prepared bodies are consumed at their
  matching entries. Direct floor loading of a prepared body is not modelled.
- Admin slots and key copies are readable by every member device. So are a
  slot's device, claim number and each part's audience: they are sealed under
  the store key, which every member holds. Public observations include slot
  kinds and store-key ids, key-copy routing with exact public sealing keys,
  S3 delivery recipients, public sealing keys and authors, and object kinds,
  format versions and framing (format.md D9). These are fixed public inputs
  to the model's encryption observations.
- Unique values and owner rows are per audience; uniqueness across audiences is
  not modelled.
- Ordinary receive parts at a version above their audience's are refused.
  Captured publication and migration effects are separate in `RaiseQueue`;
  batch decoding into individual original writes is not modelled. A device whose app
  cannot apply a slot stops before it; that stop is not modelled.
- Reload models changed readable audiences and their local write gates.
  Delivery failures, circle invitation and the policy choosing the followed
  set remain abstract; a newly readable audience must load its own prefix.
- `Model` takes authenticated claim numbers and complete-slot digests as
  inputs (`Rules.number`, `Rules.digest`). No SHA-256 implementation or
  cryptographic security proof is included. Hash collision resistance and
  path binding appear as explicit premises of `settle_correct'` and
  `settle_from_floor`. Only the single-copy theorems assume `Own` or its
  equivalent journal facts; these are not derived from a device counter.
- Not modelled:
  - concrete authority policies beyond membership, role changes, the runner's
    device/key actions and store resets;
  - orphaned batch objects and provider request timing;
  - joiners reading slots before they hold the store key;
  - the merge's internals, source-verdict calculation and converter callbacks,
    which the merge and queue packages cover. `Inputs` extracts merged
    uniqueness setters; queue admission and the core batch reset filter are
    parameters. The local queue makes the authored-read reset gate explicit.
