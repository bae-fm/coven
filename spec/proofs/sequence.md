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
circle membership use `aux`. Authority for membership, store renames and
store resets is concrete in `Authority`. Other rules remain abstract: who may write to an audience,
the effect of other operations, the merge, the converter of old-version parts,
and the database part of a migration.

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
  that id, even when its member is an admin. The member's current-key signature
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
  - the removal of the member it was registered to;
  - the replacement of that member's key at or above the generation the batch
    was made under.

  Each audience only records the refusal, so no unique value or owner row is
  taken and its rows are unchanged. The ledger records the slot's claim
  number, digest and preceding write permission. Every device agrees by
  `prefix_determines_state`.

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
- `applied_rename_authorized`, `non_admin_rename_refused`: a store rename
  requires an admin in the preceding state; refusal leaves the name unchanged.
- `name_is_latest_applied`: the ledger name equals the creation name followed
  by applied renames in slot order. `name_prefix` proves equal prefixes give
  equal names, including prefixes with refused renames.
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
  stored, and the first kept slot is covered by every introduced audience's
  snapshot, including idle and deleted circles.
- **Deletion:** retention then deletes slots below the first kept slot, and
  bodies no floor names and no raise or reset above the floor needs.
- **Uploads:** a body uploaded to a fresh path.

`floorStart` takes one optional coverage per introduced audience.
`floor_requires_every_audience` rejects missing coverage;
`complete_coverage_advances` and `floor_covered` establish its bounds.
`idle_audience_blocks_floor` checks a circle with no data writes.

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
has no key for it, the device stops entirely.

- `keyless_passes_only_others` (in `Reader`): a reader of `A` without `A`'s key
  passes only slots that have no part or announcement for `A`.
- `keyless_stops` (in `Reader`): such a reader whose next slot is `A`'s never
  moves.
- `device_stops`: a device following `B` without `B`'s key, whose `B` reader's
  next slot is `B`'s, never moves that reader. Even before it notices, every
  other reader applies at most up to that slot. `missing_key_blocks_reload`
  forbids every floor reload until all followed audience keys are held.
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

[`CircleRaise`](sequence/CovenSequence/CircleRaise.lean) shows which device can
raise an audience it reads below its breaking version. It assumes migrating
leaves an audience with no rows empty: a migration changes rows that exist,
and a circle made after it has none.

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

## Operations

[`Operation`](sequence/CovenSequence/Operation.lean) models an admin operation's entry claim
and deletable preparation in a journal:

- it records each deletable artifact before sending its create;
- it records the slot it claims before sending the claim, and resolves that
  claim before doing anything else;
- its entry bytes name the operation and the slot.

A crash loses nothing the steps use. Every step reads the journal and storage,
and a restart takes the same steps. Sent claim requests survive crashes and can
land at any later time. So can artifact creates, until each ends by landing,
failing or being cancelled. The undo runs only after every create it sent has
ended. It deletes by recorded name and confirms absence before deleting the
journal. Other devices claim slots at any time. Validation (`allowed`) reads
only slots `1..n-1`.

Key copies and snapshot announcements are sequence slots, never deletable
artifacts. Their separate publication claims are outside this model; already
published preparation is included in its starting history.

This operation model retains sequence slots. Its results are safety properties,
not a promise that storage requests or contention eventually succeed. Settlement
after slot deletion is proved separately below.

- `step_inv`, `step_req`, `steps_inv`: every step keeps the invariants. One of
  them is that every create still in flight is recorded while the journal
  exists, and outlives it only for a landed operation.
- `operation_outcome`: over any interleaving, the operation's entry occupies at
  most one slot. A landed entry's bytes are at its slot; the slots before
  it were filled when it validated, and validation held against them. An
  undone operation has no entry bytes in the sequence, now or later, and none
  of its recorded deletable artifacts remains in storage.
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
setup writes while checking are space nothing refers to, so they are not
modelled.

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
- a pass's failure that no retry clears: the connection's record that never
  retries holds every later pass until the person resets the device (E5);
- another admin's device applying that stop, which may come first: it writes
  this device's gone marker and deletes the joiner's request, and the joiner
  sends none after it (§13.2);
- the owner's devices taking back the invite's share: the inviting device once
  it ended the share, while no failure holds its passes, any of them once history records the share and the
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
- `approval_runs_binding`: a sent invite listed as approving is exactly one
  whose binding is listed as running.
- `offered_fresh`, `binding_fresh`: the pass checks the request's keys when it
  opens the request, so the app is offered approving only a request whose keys
  are fresh, and an approval under way binds only such a request (`KeysInv`).
  History then refuses the binding only when this device may no longer
  approve, never for the request's keys.
- `refused_ends`: a pass that finds a request failing its checks ends the
  invite, as a decline does, with no pending record, and the pass goes on to
  put the marker in the request's place. The invite ends without the app's
  retry and without its expiry.
- `refused_approval_ends`, `never_held`: an approval whose binding history
  refuses ends the invite as a decline does, an in-person invite's too, and
  the next read and write put the marker in the request's place. No pass
  fails with the refusal, so no failure holds a later pass, and `settles`
  runs every operation to its end.

Each rule `correct` has is needed; leaving one out is a checked counterexample:

- `unguarded_finding_is_stale`: a finding saved without the step check lands
  over the app's cancel.
- `forgetful_stop_runs_forever`: a stop that doesn't end the app's operations
  leaves one running forever.
- `stop_strands_approval`: a stop that doesn't end the invites leaves an
  approval under way listed forever; it never expires.
- `refused_record_strands`: a refused access record that doesn't end its
  invite leaves a pending record no one can clear.
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
  binding applies, a sent invite offers the app a request whose keys a member
  holds, and an in-person invite's pass approves one and journals a binding
  history refuses.
- `refused_request_retried`: an invite left waiting on a request that failed
  its checks offers the app a retry that comes back to the same state, sent
  or in person: the request stays at the invite's one path, so the pass
  refuses it again, and the invite stays listed, with its share standing,
  until the app cancels it or it expires.
- `refused_approval_holds`: a pass that fails with an in-person invite's
  refused approval, once it has ended the invite, leaves the connection a
  record that never retries. Every later pass is held: the app's admin change
  journaled before never runs or ends, and this device never takes the
  invite's share back, until the person resets the device.

`Rules.rust` names the rules the Rust follows; the differential test compares
it.

[`InviteClaims`](sequence/CovenSequence/InviteClaims.lean) models what
`InviteLifecycle` takes as landing in one step: the entries an invite claims
through operations of its own, its share's record, the share's end and a kept
S3 replacement's key record, and the device's one open claim they share
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

[`JoinStatus`](sequence/CovenSequence/JoinStatus.lean) models what a waiting
join reports across its checks and across the calls that resume it: a call
may stop between checks, or in the check whose copies opened its keys, after
it reported loading. The scan for the copies is part of the wait. The first
check whose copies open the keys records that with the saved request before
it reports loading. `phases_forward` proves the phases never go back:
waiting, then loading, then done. Each rule is needed:

- `resumed_waits_again`: with the opened keys kept only in the call, the
  resumed call reports waiting after the call before it loaded.
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
resume fails on every path (`replaced_restore_stuck`).

[`FirstInstallation`](sequence/CovenSequence/FirstInstallation.lean)
models one store's unfinished first installation on a device, which a join
and a restore both begin on (§13.4). A join keeps its invite code from
before its request may be sent, and a restore its record from before its
registration may be sent; each lists its call to resume. A join cancelled
before sending, or declined, and a restore cancelled or failed before its
record, remove the installation and everything kept beside it. The app
drops either call at any point. Under `Rules.correct` a restore refuses an
installation holding an invite code, and a join one holding a restore's
record.

- `never_stranded`: whatever storage may hold of a join's request or a
  restore's registration, the call that sent it stays listed until it meets
  its outcome.

The rule is needed; without it one call's removal takes the other's listing:

- `restore_strands_join`: a restore begun on a waiting join's installation
  is cancelled before its record, and removes the join's code.
- `join_strands_restore`: a join begun on a registering restore's
  installation is cancelled before its request, and removes the restore's
  record.

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
  target. In `Settle.lostStore`, slot 1 adds a member and slot 2 announces a
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
must retain an admin, and an added member id must never have been a member.
Conversion is the identity; audience writer lists and
conversion are fixture parameters, not additional store rules.

Slots occupy positions `1..slots.length`. Each has `device`, positive `claim`,
and `digest`. Repeated or decreasing numbers are accepted because copies can
share an id. An admin slot is
`{"kind":"admin","device":0,"claim":1,"digest":"slot-1-hash","entry":{"kind":"removeDevice","device":1}}`.
Entry kinds are `renameStore` (`name`), `addMember` (`member`, `role`),
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

**`inviteLifecycle`** runs one invite's events through `InviteLifecycle`'s
steps under `Rules.rust`, in [`SequenceInvites`](sequence/SequenceInvites.lean).
`delivery` is `"sent"` or `"inPerson"`, `records` whether approving also
records a kept replacement's S3 key, and `events` a list of `pass`, `send`
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
(`running`, `landed`, `rejected` or null), `pending` (`auto`, `app`, `never`
or null), `journal`, `request` (the request object is in storage), `shared`
(the invite's account is shared), `held` (a pass failed with an error the
connection's record never retries, so no later pass runs; the Rust's sync
loop holds them, and its case runs none) and `accepted`, the app's steps a
write accepts now.

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
- Snapshot bodies are honest: ordinary bodies contain the covered state;
  raise bodies contain migrated rows; reset bodies contain the chosen rows.
  Coven's raise body differs from that for a part that lands after its raising
  device migrated and whose converter isn't the migration (`RaiseBody`).
  Floor reload uses ordinary snapshots; prepared bodies are consumed at their
  matching entries. Direct floor loading of a prepared body is not modelled.
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
  A device whose app cannot apply a slot stops before it; that stop is not modelled.
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
  - concrete authority policies beyond membership, role changes, store renames,
    the runner's device/key actions, circle membership changes and store resets;
  - orphaned batch objects and provider request timing;
  - joiners reading slots before they hold the store key;
  - the merge's internals, source-verdict calculation and converter callbacks,
    which the merge and queue packages cover. `Inputs` extracts merged
    uniqueness setters; queue admission and the core batch reset filter are
    parameters. The local queue makes the authored-read reset gate explicit.
