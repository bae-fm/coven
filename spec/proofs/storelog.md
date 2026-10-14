## Appendix C. Proof of the store log

- Lean checks the store log's terminating replay and entry accounting.
  C1–C9 describe the historical policy; their key
  conflicts and examples are not the current §9 policy. C10 proves finality
  for its stated policy. C11 models D6's key-free removals, key introductions
  and §14.6's empty circles. C12 checks key hashes, copy exposure and
  first-attempt selection against the current replay. C13 adds the stable-member
  proposal and its old-key admission finality proof; C14 lists the implied
  spec and format changes.
- The development is in `spec/proofs/storelog/`, without Mathlib, using
  the toolchain pinned by [Appendix B](merge.md).

### C1 Which entries delete a circle

- Whether an entry deletes a circle is judged in its author's view: a
  deletion, or a removal of the circle's only member there. A store
  removal's circle-key list names only circles left with members, so it
  plays no part in this. Lean: `deletesCircle`.
- E.g. Ana and Ben are admins and share Gifts. Both devices had read that
  state:

  ```
  first stamp    Ana's phone    remove Ben from Gifts
  second stamp   Ana's tablet   remove Ana from the store
  ```

- Neither author saw Gifts with one member, so neither entry deletes it.
  They still conflict: both replace Gifts' key. The earlier circle removal
  applies, the store removal drops, and Gifts keeps Ana. Both remain store
  admins. This holds in every causal arrival order. Lean:
  `Examples.CircleDeletion.shared_key_conflict`, `earlier_rotation_applies`.
- E.g. Ben is alone in his circle and renames it while Ana removes him
  from the store. In Ana's view the removal takes the circle's only
  member, so it deletes the circle and beats the rename, which is dropped.
  Lean: `Examples.CircleDeletion.alone_conflict`,
  `removal_beats_rename`.

### C2 The claim and the model

- Two devices receive the same entries, each in a causal order. They end
  with the same members, roles, storage access, devices, circles and circle
  memberships,
  each audience's schema versions, reset snapshots, kept identities,
  and dropped identities. Lean: `storelog_converges`.
- An entry records its signature's author, writing device, had-read set,
  and action. Its identifier is its unique timestamp. Finite histories
  are numbered from zero without changing timestamp order.
- The actions are creation, adding or removing a member, changing a role or
  storage access,
  adding or removing a device, making, renaming or deleting a circle,
  changing its members, raising a schema version, and resetting an audience.
  A raise or reset names a `SnapshotId`: its audience determines which store
  or circle it affects, with no separate audience field. The model abstracts
  the snapshot's device and number to one identity number; versions are keyed
  by audience. Lean: `Entry`, `Action`, `SnapshotId`, `State`.
- Storage access is an opaque string in the model; the Rust adapter includes
  its provider-account or S3-key tag. Creation and member additions establish
  it, and a member's own set-access entry replaces it. Removed members retain
  their current replayed access. Revocation separately covers every recorded
  access, including dropped entries (§13), and is outside this replay model.
  Lean: `Action.setAccess`, `State.access`.
- A store removal also carries the names of the circles whose keys it
  replaced, as recorded when it was made
  ([§13](../coven.md#13-removing-members-and-devices)). Replaying
  other entries cannot extend that list.
- A *closed set* contains every entry any of its entries had read. A
  *causal order* puts those entries before the entry that read them, as
  in [Appendix B](merge.md#b2-terms).
- `Valid` records the assumptions from
  [§7.1](../coven.md#71-causality),
  [§7.2](../coven.md#72-timestamps), and
  [§9](../coven.md#9-members-and-roles):
  - every had-read entry has a smaller timestamp;
  - each had-read set is closed;
  - a device has read its own earlier entries;
  - creation is the first and only creation entry, and every later entry
    has read it.
- E.g. Ana's tablet adds Ben to Gifts, then Carol. Carol's addition has
  read Ben's. Ana's phone removes Ben having read neither: that removal
  is concurrent with both additions. Lean: `Examples.Gifts.valid_history`,
  `causal_arrivals`, `removal_names_no_circle`.

### C3 Applying the rules

- `scan` replays entries in timestamp order, from creation:
  - skip identities dropped earlier in this replay;
  - check authority in the replay of exactly the entry's had-read set;
  - if its change is already in place, keep its identity and change nothing;
  - otherwise require the objects it needs, an admin remaining, and a win
    against every concurrent applied opponent;
  - if it beats opponents, drop all of them and restart from creation.
- Each new arrival starts a new replay of the entire received set with
  no drops. Drops persist through restarts within that replay.
- Equal effects keep both identities. E.g. Ana and Ben both add Dan:
  Dan is added once, both identities are kept, and neither is dropped.
  Lean: `Examples.equal_adds_combine`.
- Authority comes first even for unchanged effects. E.g. Gifts belongs
  to Ben and is already named Gifts. Ana, an admin outside it, cannot
  apply a rename to Gifts. Lean: `Examples.outsider_cannot_manage`.
- The conflict predicates implement the cases in [§9](../coven.md#9-members-and-roles):
  - different statements about the same member or device; a device entry
    is also about its owner, and set-access is about its author;
  - a store addition against a store removal;
  - deleting a circle against changing it, raising its version, or resetting it;
  - a circle addition against replacement of that circle's key;
  - two replacements of the same store or circle key;
  - different snapshots for the same version raise of one audience, or its reset;
  - a reset against a version raise for that audience.
  - Lean: `memberTarget`, `deviceTarget`, `specialConflict`, `pairConflict`.
- Whether an entry deletes a circle is judged in its author's view
  ([C1](#c1-which-entries-delete-a-circle)).
- The preference is removal or circle deletion, then other actions,
  then admin grants; ties go to the smaller timestamp. This is a strict
  total order, so preferences cannot cycle. Lean: `before_irrefl`,
  `before_trans`, `before_total`, `before_asymm`.
  - E.g. Ben's phone addition beats a concurrent admin grant for Ben;
    both concern Ben, and the grant has lower priority. Lean:
    `Examples.opening_log`, using §9's opening log.

### C4 Circle keys and Gifts

- A store removal conflicts with a circle addition precisely when they
  concern the same member, the circle is named in the removal's
  circle-key list, or the removal deletes the circle in its author's view.
  Lean: `store_removal_circle_add`.
- E.g. Gifts contains Ana alone. Her tablet and phone have read that state:

  ```
  stamp 4   Ana's tablet   add Ben to Gifts
  stamp 5   Ana's tablet   add Carol to Gifts; had read stamp 4
  stamp 6   Ana's phone    remove Ben from the store; had read neither
  ```

- The removal names no circle: Ben was not in Gifts in its author's
  view. Carol's addition does not conflict with it. Ben's addition does,
  because both entries concern Ben. Lean: `Examples.Gifts.removal_names_no_circle`,
  `conflicts`.
- The first pass drops Ben's addition and restarts. Carol's addition
  applies; Gifts ends with Ana and Carol. Ben's addition stays dropped.
  This holds in every causal arrival order. Lean:
  `Examples.Gifts.first_pass`, `carol_stays`.
- E.g. instead Carol already shares Gifts with Ben, and Ana removes Carol
  from the store while Ben adds Dan to Gifts. Ana's removal names Gifts,
  so Dan's circle addition drops; Dan remains a store member. Lean:
  `Examples.store_removal_beats_circle_add`.
- Two store removals compete for the store key; two removals from one
  circle compete for its key. A store removal also competes with a circle
  removal when its recorded key list names that circle. The earlier
  removal wins. Removals of different people from different circles can
  both apply. Repeated removals keep both identities under the already-in-place
  rule, without replacing a key again. Lean: `Examples.same_circle_rotations_conflict`,
  `different_circle_rotations_both_apply`, `store_rotation_beats_later_circle_rotation`,
  `repeated_store_removals_keep_both`, `repeated_circle_removals_keep_both`.

### C5 Restarts terminate

- Every restart preserves all previous drops and adds at least one
  received identity that was not among them: an applied opponent.
  Lean: `scan_progress`.
- The number of received identities not dropped strictly decreases.
  Lean: `restart_decreases`.
- With `k` received entries, at most `k + 1` passes suffice, including
  the final pass. `settle` extracts a completed replay using that proof;
  exhausting a bound cannot return an unfinished state.
  Lean: `settleN_total`, `settle_eq_some`.
- A drop persists even if its winning opponent later drops. E.g. Ana
  and Ben are admins; concurrently, in this timestamp order:

  ```
  Ana adds Carol as admin
  Ben is made a member
  Ben removes Ana
  ```

- The removal first drops Carol's addition. After restarting, removing
  Ana would leave no admin, so the removal drops too. Carol stays absent;
  both the addition and the removal stay dropped. Lean:
  `Examples.first_pass_drops_add`, `second_pass_drops_removal`,
  `losing_removal_discards_add`. The last theorem covers every causal
  arrival order.

### C6 Convergence and entry accounting

- `resolve` is a function of the received set: sort its identities by
  timestamp and run the terminating replay.
- Each author's view is built by the same replay of exactly that entry's
  recorded past. The recursion uses only smaller timestamps.
  Lean: `authorViews_at`, `author_past_causal`, `author_view_reached`.
- `resolve_eq` equates the implementation's view table with that recursive
  definition. Two bounds containing the same received set give the same
  result. Lean: `resolve_bound_independent`.
- **One arrival:** `step` inserts the identity and publishes the resolved
  state and entry dispositions together. Lean: `step_spec`.
- **Induction:** a causal arrival sequence maintains that function's
  result and a closed received set. Lean: `run_isSpec`, `causal_ready`.
- **Uniqueness:** two results for that function and set are equal.
  Lean: `isSpec_unique`.
- **Convergence:** two causal orders of one set agree on the entire state,
  kept and dropped identities.
  Lean: `storelog_converges`.
- Timestamp order is itself a causal order of every closed received set.
  Lean: `timestampOrder_causal`, `full_history_causal`.
- Every received identity is kept or dropped, never both; no other identity
  appears in either list. Lean: `resolve_partition`. These dispositions remain
  tracked internally and are not yet exposed to the app.
- E.g. Ana removes Ben while Ben makes Carol a member. Ben's role change
  is judged in the view where he was still an admin, whatever the arrival order.
  Lean: `Examples.removed_author_view`.

### C7 Invariants

- **An admin exists.** Every nonempty closed history contains creation.
  Creation cannot be defeated, since every later entry has read it.
  Each changing entry preserves an admin; unchanged entries preserve
  the state. Lean: `resolve_created`, `admin_invariant`, `closed_has_admin`.
  - E.g. Ana, Ben and Carol concurrently remove one another. Every pair
    replaces the store key, so Ana's earliest removal applies and Ben's
    and Carol's removals drop. Ana and Carol remain admins. Retrying a
    dropped removal requires a new entry; replay does not produce one.
    Lean: `Examples.three_admins`.
- **Device owners and circle members exist.** Creation and additions
  establish these references. Member removal removes the member's
  devices and memberships in every circle. Lean: `effect_references`,
  `resolve_references`, `devices_have_members`, `circles_have_members`.
  - E.g. Ana removes Carol, who shares Gifts with Ben and has a private
    circle. Gifts keeps Ben, Carol's devices disappear, and her private
    circle is deleted. Ana stays outside Gifts. Lean:
    `Examples.member_removal_updates_circles`.
- **Circles left empty are deleted.** Lean: `withoutMember_members`,
  `circles_nonempty`.
  - E.g. Ben leaves a circle where he was alone; he remains a store
    member and the circle is absent. Lean: `Examples.empty_circle_deleted`.
- **Authority uses the author's view.** Every kept identity passed this
  check, including an unchanged effect. Lean: `authority_uses_author_view`.
- **Device actions use the member's signature.** A member adds their own
  device; its owner or an admin removes it. A removal requires the device
  to belong to its named member in the author's view: an unseen device
  cannot be removed. The model carries that member explicitly; Rust derives
  it from the author's view. Writing-device registration is not an additional
  authority condition, so a new device adds itself.
  Lean: `device_add_authority`, `device_removal_authority`,
  `Examples.device_removal_checks_owner`, `Examples.new_device_registers_itself`.
- **Circle members manage their circle.** An outside store admin cannot
  rename it, change its members, delete it, raise its version, or reset it.
  A store version raise requires store membership; a circle raise requires
  membership of that circle in the author's view. Lean: `circle_delete_authority`,
  `circle_raise_authority`, `Examples.outsider_cannot_manage`,
  `outside_admin_cannot_repeat_raise`, `removed_circle_member_can_raise_concurrently`.
- The device and circle invariants follow from the effects; the replay
  does not add rejection checks to enforce them.

### C8 The examples checked

- `EveryOrder` quantifies over every causal arrival order of an example's
  received set. `every_order` transfers its checked replay result to all
  those orders. Concrete results use `decide`; `validCheck_sound` checks
  their causal metadata, and timestamp order supplies an actual arrival
  order.
- [§9](../coven.md#9-members-and-roles)'s five table rows:
  - Ana adds Dan while Ben promotes Carol: both apply.
    Lean: `Examples.example_add_and_promote`.
  - Ben adds his phone while Ana removes Ben: the removal wins, and the
    phone addition is dropped.
    Lean: `Examples.example_member_removal_beats_phone`.
  - Ben is concurrently made admin and member: member wins.
    Lean: `Examples.example_lower_role`.
  - Ana and Ben remove one another: the earlier removal applies.
    Lean: `Examples.example_mutual_removal`.
  - Ana adds Carol while Ben removes Dan: Carol's addition drops.
    Lean: `Examples.example_carol`.
- §9's other examples: the opening log, equal additions, Gifts, the circle
  and store removals competing for Gifts' key, three removals, a dropped
  removal that leaves Ben's phone free to join, and Carol's addition that
  stays dropped when its opponent drops. Lean: `Examples.opening_log`,
  `equal_adds_combine`, `Gifts.carol_stays`, `CircleDeletion.earlier_rotation_applies`,
  `three_admins`, `dropped_removal_allows_phone`, `losing_removal_discards_add`.
- A causal access replacement supplies a later removal's replayed access record. A
  concurrent removal defeats the replacement and retains the previous access.
  Lean: `Examples.replacement_then_removal`, `removal_defeats_access`.
- [§12.2](../coven.md#122-adding-a-person): Ana approves Carol's
  join request, seals the key to her, then publishes the addition.
  Publication requires the matching seal; sealing requires approval.
  Lean: `publication_requires_seal`, `sealing_requires_approval`,
  `Examples.seal_then_entry`.
- [§13](../coven.md#13-removing-members-and-devices): Carol's
  defeated addition declines her join and prevents her device from
  surviving. Lean: `Examples.carol_join_declined`, `carol_device`.
  Its shared-circle and private-circle cases are checked by
  `member_removal_updates_circles`, `store_removal_beats_circle_add`.
- [§14.3](../coven.md#143-circles): an ordinary member makes
  a circle and becomes its first member; concurrent different names use
  the later timestamp, keeping both rename entries.
  Lean: `Examples.circle_created_by_member`,
  `circle_renamed`.
- [§14.6](../coven.md#146-leaving-a-circle): Ana removes Ben
  from Gifts; Ben stays in the store and his concurrent rename still has
  authority. A key-rotating removal defeats a concurrent circle addition.
  Lean: `Examples.circle_leaving`, `circle_key_rotation`.
- [§14.7](../coven.md#147-deleting-a-circle): Ben deletes Gifts;
  a concurrent rename or reset loses. Lean: `Examples.circle_deleted`,
  `circle_delete_beats_reset`. Appendix B's `example_14_7` checks the
  associated row deletion and loss of Ana's concurrent note.
- [§17](../coven.md#17-schema-changes): the schema version can
  be raised by an ordinary member; different snapshots for the same
  concurrent raise use the earlier entry and drop the other. Identical
  raises keep both identities; a later raise advances the version.
  Lean: `Examples.same_version_snapshot`, `identical_version_raises`,
  `version_raised`.
- Each snapshot of a breaking change has its own raise entry. Store and
  circle versions remain independent; even equal version numbers with
  different snapshots of different audiences do not conflict. Concurrent
  raises of one audience to different versions both keep their identities,
  and the higher version selects its snapshot. Circle raises at the same
  version combine when the snapshots agree; otherwise the earlier entry
  wins. Lean: `Examples.each_audience_has_its_version`, `circle_higher_version_wins`,
  `circle_equal_version_snapshots`.
- A circle raise needs membership in its author's view, including a raise
  whose version and snapshot are already in place. Removing that member
  concurrently does not revoke that authority. Deleting the circle, either
  explicitly or through a store removal judged in its author's view, beats
  a concurrent raise. Lean: `Examples.outside_admin_cannot_repeat_raise`,
  `removed_circle_member_can_raise_concurrently`, `explicit_circle_deletion_beats_raise`,
  `derived_circle_deletion_beats_raise`.
- [§19.3](../coven.md#193-resetting-a-store): concurrent store
  or circle resets use the earlier snapshot and drop the other entry;
  identical resets keep both identities; a later causal reset supersedes
  them. Lean: `Examples.store_reset_tie`, `circle_reset_tie`,
  `equal_resets_combine`, `later_reset`.
- A concurrent reset and schema raise of the same audience use
  the earlier entry, even if they name the same snapshot. Both can apply if one has
  read the other. A circle reset and store raise affect different audiences
  and can both apply concurrently. Lean: `Examples.concurrent_reset_and_raise`,
  `causal_reset_and_raise`, `circle_reset_and_store_raise`. The same rules hold
  for circle raises: a reset of that circle conflicts, while a store reset
  or another circle's reset does not. Lean: `Examples.circle_concurrent_reset_and_raise`,
  `circle_causal_reset_and_raise`, `circle_raise_and_other_reset`.

### C9 What Lean checks, and what is prose

**§3 coverage:** this model proves replay agreement and authority checks (C6–C7,
C11), storage-time finality (C10), and key acceptance/selection conditions (C12).
Authorization assumes an authenticated author; turning key-copy conditions into
confidentiality or revocation requires correct cryptography. Neither §3's first
two confidentiality claims nor outsider/member forgery resistance is proved.
Authentication, sealing, key derivation, atomic SQLite transactions and §2's
provider behavior are assumed; C12's hash uniqueness results additionally take
an explicit injectivity hypothesis, not a proof of SHA-256 security.

- Lean checks the modeled replay, termination, convergence, invariants,
  entry accounting, admission ordering, and store-log example outcomes above.
  `CovenStorelog/Axioms.lean` prints the theorems' axioms; their union is
  `propext`, `Classical.choice`, and `Quot.sound`. No axioms are declared
  by this development.
- `scripts/check.sh` rebuilds it from scratch and audits source and
  printed axioms. The merge proof and its differential test retain their
  own checks.
- The historical effect deletes a circle immediately when a removal empties
  it. C11 postpones the combined-removal case until replay finishes (§14.6).
- Signatures supply a verified author. Seal receipts stand for completed
  writes of the matching encrypted key. Signature verification, encryption,
  key generation and sealing, truthful circle-key lists, storage durability,
  and access revocation are boundaries, not cryptography or storage proofs.
- Versions and resets select snapshots. Executing migrations, migrating or
  losing old-version writes, reloading rows, and judging writes after a
  reset are outside this store-log state. Thus §17's column and migration
  examples and §19.3's row-recovery rules are prose here; no theorem above
  claims to execute them.
- This appendix proves the store-log part of §14's examples. The separate
  [store-log/data coupling](storelog-data.md) reuses this replay and the merge
  model for rows, removal rules, operation ordering, snapshot replacement and
  key availability. It includes checked counterexamples; neither development
  proves encryption.
- Correspondence between these Lean functions and Rust is not proved.
  `scripts/check.sh` builds `storelogRunner` and runs Rust's generated
  differential test against `CurrentReplay.resolve` and `circleCause`, comparing
  state, kept entries, dropped entries, permanent late verdicts and deleted-circle causes. State includes
  the current access of active and removed members; generated histories include
  access updates and repeated access values.
  Arbitrary generated actions read the complete past; concurrent branches cover
  circle renames, deletion, rotations, and resets against version raises in
  both timestamp orders, for the store and circles. The comparison includes each
  audience's selected versions and snapshots; generated histories also cover
  different versions of one audience,
  equal versions of different audiences, and circle deletions against raises.
- Rust retains each applied entry's first publication time and permanent late
  verdict. Entries admitted by storage time retain their author-view check, observed device owner,
  and each removal's affected circles and sole-member classification with its
  immutable bytes. The recorded past is closed before application and never
  changes, so reusing these facts is the
  same check as rebuilding that view. The complete received set still settles
  with fresh replay drops on every arrival, preserving late exclusions even in
  author views; a previous result is reused as
  the new entry's author view only when that entry has read the entire set.
  The differential test compares both batch and incremental Rust replay with
  Lean, and the Rust examples compare every causal arrival order through both
  paths. This optimization is not a separate Lean theorem.
- The runner reads `CurrentReplay.History`: current key-free actions, key ids
  and exact 32-byte commitments, immutable storage and attempt times, and
  explicit causally closed received subsets. Storage admission sees the whole
  supplied history, including unread entries outside a received subset.
  The adapter validates causal metadata and observed device ownership, then
  calls the model directly; it implements no replay or time-admission rule.
  `ReplayPolicy.authorViewTable_eq` proves a materialized author-view table
  equal to the recursive view function. The compiler uses `authorViews_eq_cached`
  for view lookups; membership, conflicts, admission and replay still call the
  same proved functions. `CurrentReplay.resolveReceived_eq`
  proves that sharing this table across received subsets equals calling `resolve`
  for each subset. These equalities are included in the axiom audit.
- The agreement corpus covers 2,048 generated histories, every prefix of three
  causal arrival orders, and the current checked examples for creation,
  rotations, recorded authority, explicit deletion, an empty circle and the
  last admin. Independent device additions and store/circle membership changes
  agree in both timestamp orders, as do contradictory membership additions and
  removals. Duration units are days; generated landings include ties and
  remain within the 30-day window. Separate cases exercise exactly 30 days,
  31 days, tied publication times, and dropped witnesses outside the received set.
  Both Rust replay paths compare admission against the entire supplied storage history,
  including implicit earlier entries on the author's own device.
- Rust uses C11's contradiction predicate, five priority tiers and finishing of
  collectively empty circles. Agreement cases cover concurrent refilling, tier
  boundaries, latest kept removal causes (including unchanged removals), dropped
  deletion restoration, and a kept deletion whose circle creation dropped.
  Storage-time admission agrees with `Finality` through `CurrentReplay`, including
  late additions excluded from the author views of their recipients' later entries.
- Other drop reasons, retained device names and public keys, cryptographic bytes,
  and codecs have separate Rust assertions. The model compares active
  membership, retained access, audience boundaries and entry fates. Replacement
  registrations are outside `CurrentReplay.Action`; the adapter rejects them
  rather than silently forgetting `replaces`. Snapshot fixtures use one writer,
  checked before projecting their writer/number identities to numbers.

### C10 Finality by storage time

The specification stores one optional monotonic horizon H. The required
representation theorem is that the established final set is exactly the
entries stored strictly before H, with ties at H excluded, and that every
advance preserves or increases H. No qualifying window means an empty final
set. [One horizon for current §9](#one-horizon-for-current-9) proves it
(`final_iff_before_horizon`, `horizon_never_back`).

`FinalityReplay`, `ReplayPrefix`, `Finality` and `FinalityExamples` reuse the
terminating replay and effects with a separate policy: no key-only conflicts,
no conflicts between removals or between circle deletion and circle changes;
contradictory member/device statements and competing snapshots remain. The tiers
are store removal (including devices), circle deletion, circle removal, other
changes, then admin grants; ties use the smaller author timestamp.

That development retains its earlier boundary convention and distinct storage
times. The horizon proof below uses current §9: a strict old boundary, both
recent endpoints included, tied provider times, and `CurrentReplay`'s policy.

Author timestamps have arbitrary order consistent with recorded reads. Storage
supplies distinct immutable landing times after path tie-breaking, in duration
units that are not renumbered. `Valid.online` requires every entry visible at
first attempt to have been read; retries preserve that past. W is arbitrary;
examples use 30 days. The finite universe can contain any continuation, including
future entries with earlier author timestamps, so every finite prefix of an
infinite continuation is covered. Storage clock assignment, path ordering,
complete storage reads and delivery buffering are outside the implementation proof.

**Storage-history stability is proved** by `storage_stability`. Every surviving
recent entry read the old prefix by quietness; every surviving future entry read
it by rule 1 (`survivor_reads_old`). The old candidates therefore precede the
others in timestamp replay, without conflicts back into them.
`ReplayPrefix.settle_prefix` proves that later restarts preserve both kept and
dropped identities. `device_stability` covers every further receipt sequence
once the old prefix has arrived; `old_closed` also covers its authority evidence.

**Unqualified device stability fails with two entries plus creation.** Two
devices concurrently reset to different snapshots after reading creation. A has
stamp 1 and lands on day 1; B has stamp 2 and lands on day 2. Neither fails rule 1.
At day 32, (2, 32] is quiet and B meets rule 2. A device with only creation and B
keeps B; receiving A afterward drops B (`delayed_delivery_counterexample`).
`causal_deliveries` checks these receipt orders; `one_change_no_delayed_flip`
proves that one non-creation entry cannot exhibit this missing-old-entry failure.

The storage rule needs no longer window. Qualify device finality with **receipt
of the prefix through T−W and evidence that the window is quiet**. Reading through
T establishes that evidence; an incomplete local list cannot certify absence of
late entries. Receipt of recent entries is otherwise unnecessary for stability.
`cutoff_receipt_needed` shows the receipt cutoff cannot uniformly be moved earlier:
an unread entry exactly at T−W can still change a status. Particular histories
can need less evidence.

**Agreement, progress and rule-1 agreement are proved.** `agreement` gives equal
results and rule-2 selected sets for equal entries at the same storage observation
time. `progress` finalizes the old prefix after W without late landings.
`drop_agreement` ignores receipt order and local clocks; `drop_decided` makes the
judgment permanent after receiving the entry's landing prefix. Incomplete evidence
can disagree: moving B's landing to day 32 makes A a missing 31-day rejection
witness (`missing_drop_witness`). An apparent acceptance is not yet a decision.

`quiet_window_needed` checks the late-entry chain that defeats an age-only rule;
`drop_rule_needed` checks why a backdated retry must be rejected.
`exact_boundaries` checks rejection strictly after W and finality at exactly W,
with the lower window endpoint excluded. These are the earlier model's
boundaries, not current §9's. `Axioms.lean` audits all named results;
the existing proofs check rebuilds these modules.

#### One horizon for current §9

`Horizon` models the current finality test in
[§9](../coven.md#9-members-and-roles). At observation time T it certifies entries
stored strictly before T−W, provided no late entry landed in the inclusive
window [T−W, T]. W is 30 days in the examples. Late entries count in this
check even when their changes have been permanently dropped. Provider times
keep their duration meaning; equal times are allowed and never ordered by path.

The model's observation parameter T is §9's `T′ = T - M - δ`, with one
common `M = one hour` and positive provider unit δ. The provider guarantees
listing visibility strictly before T minus M; the extra tick puts the
inclusive window endpoint inside that guarantee. The later folder listing,
numbered reads and retained receipts supply every entry through T′.
The [IO listing proof](io/CovenIO/ListingFinality.lean) establishes that
receipt and agreement of the observed quiet test with the full history,
then invokes `current_stability`. Underflow certifies no final entry.
The horizon model takes that endpoint unchanged; native provider conformance
remains an assumption.

**The final set is exactly one prefix.** `final_iff_before_horizon` equates
two separately defined things: an entry was certified by at least one
completed quiet check, and its storage time is strictly less than the saved
horizon. Updating that horizon takes the greater of its previous value and
a newly certified cutoff. No per-entry finality flags or list of past checks
is needed by that update; the list in the proof records its history.

**The horizon never moves backward.** `horizon_never_back` covers any later
checks, including repeated or older observations and windows containing late
entries. `final_stays_final` preserves every certified entry.
`failed_check_preserves` leaves the horizon unchanged when the complete scan
fails. It does not treat an incomplete scan as a quiet one.

**Ties stay together.** `ties_together` gives equal finality for equal stored
times. `boundary_not_final` keeps entries exactly at the horizon outside the
final set. `tied_boundaries` checks two resets stored on day 1: neither is
final at day 31, and both are final at day 32. `inclusive_recent_window`
checks a late entry on day 40: it blocks the day-70 check, including when
permanently dropped, and permits advancement on day 71.

**Earlier windows can be recovered.** `recovered_iff` describes the greatest
prefix certified by any qualifying window through an observed time T.
`recovered_never_back` proves that a later observation cannot shrink it.
The definition enumerates integer time units as a mathematical description,
not an implementation requirement. `earlier_window_recovered` checks that
retained history through day 40 recovers day 39's quiet cutoff, even though
the device did not check then and a late entry blocks day 40's own window.
Rust uses the sample's provider resolution for these endpoints too: a late
entry at time L permits the preceding endpoint L−δ, not an instant between
provider ticks.

**The saved prefix has stable replay results.** `horizon_stability` applies
the membership engine's `ReplayPolicy` rules; `key_free_horizon_stability`
transfers that result to `CurrentReplay`'s D6 actions. Once two received sets
contain that prefix, they agree on every prefix entry's kept and dropped result, regardless of
which later entries either has received. The finite history can include any
continuation, including earlier author timestamps and tied landing times.
`survivor_reads_old` proves the needed read relationship; `current_prefix`
uses the existing proof of replay through restarts. `old_closed` places the
prefix's recorded authority evidence in the prefix too, assuming reads name
objects already stored. Equal stored times are permitted in that assumption.

**Recomputing only the latest test fails.** Creation is followed by reset A
on day 1. Day 32 certifies A. Reset B, attempted without reading A, lands
on day 40 and is permanently dropped. The latest window is no longer quiet,
but A remains final. `latest_check_forgets_finality` and its literal Lean
`example` check that a fresh set from day 40 alone loses A, while retaining
the horizon preserves it. This is a counterexample to discarding earlier
certificates, not to §9's once-final rule.

The reading chosen for “the final set” is **everything certified by the
specified quiet-window rule**, including earlier qualifying windows. It does
not mean every entry whose particular result could be proved fixed by some
other argument; for example, permanent time rejection already fixes some
drops before they cross this horizon. Storage times are nonnegative integer
duration units. A successful check means all entries through its observation
time were read and judged, including unknown devices' logs. The model does
not prove provider clock behavior, listing completeness, persistence of the
saved number, or physical cleanup. All named horizon results are included
in `Axioms.lean`. The Rust differential compares recovered cutoffs against
`Horizon.recovered` on integral-day histories with a one-day provider tick,
including tied times and permanently dropped late entries. Cutoffs compare
exactly in that shared unit. Separate Rust tests pin the strict provider-tick boundary,
absence versus zero, failed and interrupted scans, and SQLite persistence.
The IO differential compares `observationTime` and the provider observations
underlying `through_time` with the endpoint supplied to Rust's horizon.
Store-log entries are bounded one-request publications; this is the provider
time assumption needed here, and does not extend to multipart objects.

### C11 Key-free removals and circles left empty after replay

`CurrentReplay.Action` models D6 tags 0, 2, 6, 10 and 15: store and circle
creation and rotation carry a key id and hash; store removal carries only a
member, and circle removal only a circle and member. There is no circle-key
list to check and no `WrongCircleKeys` rejection. For this input, `ReplayPolicy` admits
received entries unless they landed too late, then checks recorded-past
authority and the replay conditions from §9.

The current actions project to the membership engine's existing input. That
projection carries no replacement keys for removals and forgets creation and
rotation hashes, which do not affect membership. `ReplayPolicy` uses the
shared `scan` and `settle`; it never uses the historical key-conflict rules.
The C1–C9 functions retain their original actions and results. The Rust runner
uses `CurrentReplay.History` directly.
`ReplayCompatibility` also retains the data package's historical input:
`Finality.History` selects its original key-list check; the key-free
`CurrentReplay.History` selects no key check. The input type determines this
choice. Every C11 and C12 example and key theorem uses the key-free type.
The state-invariant induction also accepts the chosen effects and conflict
policy, so `CurrentReplay.resolve_safe` and `admin_remains` prove the admin
check for this replay through restarts.

`rotation_authority` proves that every kept rotation's author belonged to
its audience in its recorded past. `rotations_conflict_with_nothing` covers
every action in both directions. `tier_results` checks all five tiers,
including a circle removal classified as deletion in its author's view.
`store_removals_compatible` proves that store removals do not contradict each
other, including repeated removals of the same person.

Ana and Ben can rotate Gifts concurrently; both entries stay. Dan, an admin
outside Gifts, cannot rotate it. Ben's concurrent removal does not erase his
rotation's recorded authority. `CurrentExamples.rotation_entries` and
`removed_rotator_keeps_recorded_authority` check these histories.
`deleted_circle_keeps_rotation` checks that a concurrent circle deletion also
leaves the rotation kept: it changes no membership and recreates no circle.

The effect deletes a circle during replay only for an explicit deletion or a
removal that saw its sole member. Other removals retain an empty member list
until `finish` filters the completed result. Author views use that same
filter. `finished_circles_nonempty` proves no empty circle is exposed as
active; finishing changes no kept or dropped identity.

Ana's phone removes Ben from Gifts while her tablet removes Ana from the
store. Both saw two circle members. The completed replay hides Gifts and
`circleCause` names the later kept removal. Carol's concurrent addition can
still populate it: both removals stay kept and Gifts returns with Carol.
`concurrent_addition_populates_empty_circle` and `empty_circle_cause` check
these outcomes on key-free entries. `explicit_deletion_still_wins` checks
that a deletion still defeats an addition.

`empty_cause_latest` proves that the cause is a kept removal affecting the
circle, with no later such removal kept. “Affecting” uses membership in the
removal's recorded past, including a repeated removal already satisfied in
replay. `equal_received` proves state agreement for equal received sets.

### C12 Key introductions, hashes and exposure

`CurrentKeys` takes introductions from the actual current replay's log and
author views. `kept_key_origin` proves that every key introduced by a kept
entry comes from store creation, circle creation or rotation.
`removals_introduce_nothing` rules out both kinds of removal.
`kept_introduction_received` connects kept introductions to the received,
authorized set used for reading and key selection. Replay dropping an entry
does not itself revoke its authorized key introduction (§11).

**Circle creation needs an exception to prior membership.** The unrestricted
claim “every circle key was introduced by someone already in the circle in
its recorded past” fails with two entries: Ana creates the store, then Gifts.
The Gifts entry is kept and introduces its key; its recorded past has no
Gifts. The entry itself makes Ana its first member.
`CurrentKeyExamples.creation_has_no_prior_circle_membership` and a literal
Lean `example` check this history. `circle_key_authority` proves the spec's
reading: creation requires store membership; every later circle-key rotation
requires membership of that circle in the author's recorded past. An outside
admin's removal introduces no circle key.

**Copy acceptance checks the exact bytes.** `KeyBytes` and `KeyHash` are
32-byte vectors. The hash is a parameter, not an implemented or axiomatized
SHA-256. `accepted_copy_checked` checks the recipient, opened audience/id and
authorized hash; `accepted_copy_authorized` traces that hash to a received
entry with recorded-past authority. Tentative bytes with no such introduction
cannot pass this check.

Under an explicit injectivity hypothesis on the hash, `other_bytes_rejected`
rejects different bytes, and `accepted_copies_unique` permits at most one
accepted byte value for an audience/key id, across recipients and copies.
`accepted_bytes_stable` extends this result to growing introduction sets.
`mismatching_copy_no_key_or_exposure` proves that a recipient's mismatching
copy contributes neither a key nor exposure evidence to that recipient's
check. The byte-uniqueness theorems use the hypothesis as an argument; the
development adds no axioms.

The spec assumes fresh random key ids without specifying conflicting
commitments for the same audience/id. The model refuses acceptance when
received authorized introductions disagree on that hash
(`conflicting_hashes_rejected`). Equal commitments may repeat. This reading
avoids assuming id uniqueness or silently picking one of two commitments.
An id in one audience is distinct from the same id in another audience.

**App-data selection requires this pass's complete listing.** `firstAttempt` combines
the current replay, received authorized introductions, custody and the pass's
copy listing. An incomplete listing gives a listing wait; an excluded sender
gives a membership wait. Otherwise `select` picks the newest usable held key,
ordered by introduction timestamp and then key id (`selected_newest_usable`).
A held candidate must match its authorized hash. An exposed key cannot be a
fallback: `usable_or_pending` and `selected_usable` cover every result.
`replacement_avoids_key_wait` proves that a usable held replacement suffices,
even while old exposed copies remain listed.

A device cannot open boxes addressed to other members. `foreign_copy_counts`
therefore counts such a listed path as exposure without inspecting its
payload, even if that payload is forged. `selected_no_excluded_copy` proves
that every listed copy of the selected key addresses a current audience
member. `first_attempt_safe` states the same guarantee against the actual
current replay, and `first_attempt_authorized` proves its introduction's
authority. This covers any excluded member who can hold a key through this
listing, without assuming that foreign boxes can be decrypted.

`recipient_rejects_forgery` checks both perspectives: Dan rejects a box whose
bytes mismatch its named key, while Ben counts its path to excluded Dan.
`forged_foreign_box_costs_a_rotation` checks the consequence: Ben had a usable
store key; the forged foreign box causes a key wait; one fresh rotation,
sealed only to remaining members, permits sending again. The old and forged
copies stay listed, membership is unchanged, and Ben can still accept the
old genuine key for reading. This is one forged box against one key; it does
not claim progress against an unending sequence of new forgeries.

**Concurrent removals apply independently.** In `concurrent_removals_apply`,
Ana removes Dan while Ben removes Erin; both read the same past. Both entries
are kept, both excluded members are absent, and both admins remain. Ana is
outside Gifts, which contains Ben, Dan and Erin before the removals. Neither
removal creates a store or circle key (`both_removals_mint_nothing`). A copy
to either Dan or Erin makes each old audience key unusable
(`excluded_dan_or_erin_retires_each_audience`); both audiences wait without
falling back (`removals_wait_without_fallback`). Ben then rotates Gifts and
Ana rotates the store: both new keys are usable and selected, despite the
permanent old copies (`remaining_members_rotate`). `last_admin_removal_drops`
checks the exception: with only Ana and Ben as admins, their concurrent
mutual removals keep the earlier one and drop the removal that would leave
no admin.

The model assumes verified entry identities and signatures, the declared
results of opening boxes, immutable recorded pasts, and a truthful complete
listing for the current pass. It does not implement SHA-256, encryption,
random generation, provider access revocation, custody storage, publication
queues or physical deletion. The fresh-introduction sealing exception and
identical attempted retries are outside this first-attempt selection model.
Copies made after the listing remain §11's accepted residual window. All
named current-policy, key and example results are audited in `Axioms.lean`;
Rust correspondence is checked by the current-policy differential
described in C9. The runner's introductions mode also compares Rust's
received authorized introductions and agreed commitments with
`receivedIntroductions` and `hashFor` for every received prefix, including
late, beaten, unauthorized and conflicting introductions. Its copies mode
compares Rust's per-copy acceptance with `acceptedCopy`, using the identity
hash on bytes: copies to this member or another, unopened, committed, other
bytes and another identity. Its select mode compares Rust's first-attempt
selection with `select` as `firstAttempt` applies it to the received
introductions and resolved state, for complete and incomplete listings, copies
to members and outsiders, and committed and other held bytes. Cryptographic
operations are not compared.

#### Store-log entries, positions and clock objects

[`EntryKeys`](storelog/CovenStorelog/EntryKeys.lean) models §11's separate
metadata path. `select` chooses the newest received store-key introduction
whose bytes the device holds and accepts. It uses C12's hash checks and
timestamp/key-id ordering, without consulting copy exposure. `selected_accepted`
and `selected_newest` prove those conditions; `first_attempt_authorized`
connects the selected key to the actual replay's received authorized entries.
The existing exposure-checked `CurrentKeys.select` still governs writes and
snapshots, and its Rust comparison is unchanged.

**Removal and rotation can be sealed with a retained accepted store key.**
`metadata_sealing_available` proves this for every metadata object, including
any removal or rotation entry. It requires at least one accepted store key in
custody; membership alone does not supply missing or damaged key bytes. All
held keys may be exposed. Entry authority, completed catch-up, send freshness
and successful publication are separate conditions; this result does not
promise that every attempted entry is kept or uploaded. A circle rotation's
entry uses a store key too, irrespective of the circle key it introduces.

**Removal does not make later metadata cryptographically unreadable.**
`readable_iff` states the exact boundary for entries, positions and clocks:
the reader must obtain the ciphertext and know its sealing key's exact bytes.
Knowledge includes retained keys and any later disclosures, not just keys
current replay permits the reader to use. `accepted_key_opens` connects a
retained accepted copy to the bytes selected by the writer, under C12's
explicit hash-injectivity hypothesis. `unknown_key_unreadable` proves the
opposite case within this abstraction of correct encryption.

[`EntryKeyExamples`](storelog/CovenStorelog/EntryKeyExamples.lean) checks the
shortest removal prefix: Ana creates the store, adds Ben, then removes Ben.
She remains its admin, and Ben keeps the original store key. Positions and a
clock object sealed after that replay still use it and Ben can open them while
provider revocation is pending (`removed_reads_positions_and_clock`). The
fourth entry rotates the store key, reads the removal, and can itself be sealed
with the original key (`removal_and_rotation_sealable`, `removed_reads_rotation`).
Literal Lean `example`s check both disclosures; `history_valid` and
`online_history` check causal and online histories. These refute immediate
metadata secrecy after removal, not §3's app-write revocation promise.

Receiving the rotation entry without its key still leaves the original key
selected. Once the writer holds the replacement it selects that key for all
three metadata kinds, and Ben's old bytes do not open those objects
(`rotation_changes_only_holders`). A subsequent entry remains readable if its
writer lacks the replacement (`later_entry_still_readable_without_replacement`).
If Ben later obtains the selected replacement, he can open its earlier and
later metadata; even known exposure does not stop its metadata use
(`later_disclosure`). There is no store-wide cutoff at the first rotation.

**The confidentiality sentence depends on actual storage cutoff.** §13
explicitly lets removal return while provider revocation is pending. Read with
that condition, §11 and §3 remain consistent: metadata has no immediate
cryptographic revocation guarantee, while writes and snapshots use the
exposure check. `cutoff_prevents_new_read` proves no read when the ciphertext
is unavailable; `cutoff_does_not_erase_keys` checks that an already obtained
object remains openable. The spec citation makes this condition explicit;
these examples do not require changing the key-selection policy.

**What the sealing key reveals.** Opening a metadata object gives its
contents and nothing more. Entries name keys only by id and hash
(`KeyCommitment`), never by their bytes, so a rotation sealed with a key Ben
knows tells him the new key's id and hash but not the key. Clock objects carry a
device id and store-log positions. Positions also carry fingerprints, each naming its own
audience key ([D8](../format.md#d8-small-frames),
[D11](../format.md#d11-keys-contexts-and-fingerprints)). A fingerprint is a
keyed digest of that audience's app data, so anyone holding its key can test
a guess about the data. Which key it uses therefore decides what the
positions envelope reveals.

[`PositionFingerprints`](storelog/CovenStorelog/PositionFingerprints.lean)
models both readings. The [sync pass](../sync-pass.md#4-observe-keys-and-peers)
says the fingerprint key is usable, which is C12's exposure-checked selection
(`usableFingerprint`); the other reading is the newest key the device reads the
audience with (`readableFingerprint`), the same rule as the envelope's store
key (`envelope_readable`).

**The usable key keeps fingerprints from excluded members.**
`selected_bytes_unknown` proves that no group of members excluded from an
audience obtains, from this pass's listed copies, the bytes of the key C12
selects for it. A copy to them under the selected id is ruled out by
`selected_no_excluded_copy`; a copy under another id would need equal bytes,
which the `Fresh` hypothesis rules out (different key ids commit to different
hashes; the hash need not be injective). `usable_fingerprint_untestable`
concludes that such a group can test no guess against a fingerprint under the
usable key, whatever key seals the positions object carrying it. When no
usable key exists the fingerprint is omitted.

**The newest readable key leaks app data: checked counterexamples.** In
`storeLog` Ana removes Ben from the store and rotates to K1. Carol, not having
read the removal, concurrently rotates to K2 and seals it to Ben too, as §11's
lagging-device example permits. Both rotations are kept; K2 is newer and
exposed, so Ana's writes use K1 and her newest readable store key is K2
(`store_keys`, with `store_history_valid` and `store_online`). Ana's positions
are sealed with K2, which Ben opens while revocation is pending. Her store
fingerprint covers a write sealed with K1, which Ben cannot open. Under K2 the
fingerprint tells Ben whether that write's flag is 1 or 0; under K1 it tells him
nothing (`readable_store_fingerprint_reveals_write`).

`circleLog` shows the same for a circle, where storage revocation never
applies: Ben leaves Gifts but stays in the store, Ana rotates Gifts to G2 and
Carol concurrently to G3, sealed to Ben. Ben opens Ana's positions with the
store key he holds as a member and tests a Gifts write sealed with G2 against
the G3 fingerprint (`circle_keys`, `readable_circle_fingerprint_reveals_write`,
with `circle_history_valid` and `circle_online`). Both have literal Lean
`example`s. So §11's relaxed envelope rule is safe only with fingerprints under
usable keys, and §19.1 cites that reading.

The model assumes correct encryption and supplies ciphertext availability and
the reader's key knowledge as inputs. A fingerprint is abstracted to its key
and the state it digests: holding the key lets a reader test a candidate
state, and lacking it reveals nothing. It does not prove provider cutoff,
cryptographic secrecy of HMAC or SHA-256, eventual key delivery or custody
persistence. Excluded members' knowledge is what this pass's listing gives
them; copies published after the listing are §11's residual window, and
collusion with current members is outside the model. Names, public keys and
the non-secret access handles in D6 entries are membership metadata the
envelope reveals to whoever holds its key. Locally validated fresh
introductions that seal themselves remain outside this received-key path; the
availability proof needs only the existing accepted store key. Credential
replacement, entry signatures and publication gates retain their separate
models and assumptions. Rust picks each fingerprint key with the same
`key_introductions::select` that the runner's select mode compares with
`select`; with no complete copy observation, its current positions wait.
Rust tests replay both counterexample histories and check that Ana's posted
fingerprints use the usable keys while the envelope uses her newest store key. No Rust comparison is
added for metadata sealing. All named results are included in the package's
axiom audit.

#### Posting positions without a fresh copy observation

[`PositionPublication`](storelog/CovenStorelog/PositionPublication.lean)
models step 9 of the [sync pass](../sync-pass.md#9-post-changed-positions-and-finish)
with T164's two rules: an audience with no usable key omits its fingerprint,
and without a fresh, complete key-copy observation the current positions wait
while the confirmed post stands. Fresh means less than five minutes after the
catch-up started, the same window that permits sending (§10). The model
compares this with the other reading, which takes keys from this pass's
completed observation whatever its age, sending still gated by freshness.
Positions, store-log positions, schema and pending reports are abstracted to
two numbers that key choice never reads.

**Holding the post never leaks.** `send_has_fresh_observation` proves the
publication gate implies a fresh observation, so `hold_sends_nothing`: while
positions wait, nothing is sent and storage keeps the post it had.
`sent_through_gate` covers every send under either reading, and
`posted_fingerprints_usable` traces each posted fingerprint to `select`, so
`usable_fingerprint_untestable` applies to it. `omission_keeps_positions`
shows an omitted fingerprint changes nothing else in the post.

**Nothing that reads storage stalls.** `readings_store_alike` proves both
readings store and send exactly the same in every state: with the gate open
their observations coincide (`readings_agree_when_fresh`), and with it closed
neither sends (`closed_keeps_storage`). Peers' retention, finality and
agreement read only posted positions, so the waiting rule cannot delay them.

**This device's return checkpoint can stall: a checked counterexample.**
The readings differ only in confirmation. Step 9 keeps the catch-up's storage
time as the return checkpoint (§15) when every discovered write is realized
and positions are confirmed, and confirming an unchanged post sends nothing.
In `late_step_nine_unconfirmed`, Ana's pass completes catch-up at 0 and
reaches step 9 at 300 seconds, after long file or snapshot work, with
positions unchanged. The pass's observation yields exactly the stored post,
which the other reading confirms without a send; the fresh rule has no
observation, so the pass ends unconfirmed (`waiting_unconfirmed`). At 299
seconds both confirm. `late_passes_stall` extends this to any run of passes
that each reach step 9 after their window: the checkpoint never moves, so
§15's 29-day bound eventually forces snapshot discovery on a device that
synced throughout. `pass_reading_advances` shows the other reading moves it
on every such pass, and `pass_confirms_unchanged` proves it confirms any
unchanged post with no send at any age. A literal Lean `example` checks the
300-second pass.

The model takes the gate's copies as given and does not model bootstrap
publication scopes, the reload's held post, failed requests or retries. It
does not prove that passes finish within the window. Rust follows the pass
reading: `positions_for` takes keys from the send gate's last completed
observation at any age, and a Rust test reaches step 9 after the window,
confirms the unchanged post without a send, moves the checkpoint, and cannot
send a changed one. No Rust comparison is added.

### C13 Stable members and replaceable keys — checked proposal

`CurrentReplay.Keyed` models the stable-member decision separately from the
unkeyed `CurrentReplay` interface used by Rust differential tests. Member ids,
signing keys and sealing keys are independent abstract numbers; equality of
ids does not mean equality of keys. Creation and addition carry initial keys.
`InitialKeys` makes the payload required exactly for those actions. The new
`replaceMemberKeys` action carries a target member and both new public keys.
The signer is a verified public-key identity supplied at the cryptographic
boundary, separate from the entry's stable author id.

`equal_received` and `member_keys_converge` prove that equal received entry
sets produce equal complete results and member keys, regardless of order or
repetition. Author views replay exactly the recorded past using the same
key-aware replay. `ReplayMachine.settleN_total` proves restart termination;
`scan_matches_legacy` checks that its generalized scan specializes to the
historical scan's exact behavior. That historical equation remains available
to existing proofs and the Rust runner. Common membership effects, circle
finishing and the permanent storage-time rejection use the existing functions.

`kept_current_signature` proves that every kept entry uses the signing key
current in its recorded view. `replacement_authority` requires that both the
target and author exist there and that the author is the target or an admin.
The creation signature instead verifies against the initial keys it carries.
Adding an already-present id with different keys is refused; it cannot bypass
replacement. `signer_uses_recorded_view`, `old_unread_entry_counts` and
`addition_cannot_replace` check these distinctions through actual replay.

#### Member additions

An addition's meaning is its whole payload: keys, role and storage access.
`MemberAdditions.lean` states the keyed rule and checks what the first Rust
rule under stable ids broke.

- `authorized_readd_identical`: an addition its author's view authorizes for
  an id already there repeats that member's keys, role and access exactly.
- `addition_in_place`: a kept addition is in place where it applies. Unchanged
  means the id already has its payload; applied, it sets exactly that payload.
- `additions_conflict_iff_different`: two concurrent additions of one id
  conflict exactly when their payloads differ.

T104 first let a re-addition with the same keys overwrite role and access, and
let concurrent additions conflict only on keys or role. `Overwrite` states that
rule over the same replay machine. `overwrite_sets_anothers_access` re-adds Ben
as an admin with Ana's chosen access: the entry stays kept and sets Ben's
access, which a set-access entry by Ana could not do in that view.
`overwrite_keeps_unapplied_addition` adds Dan twice concurrently, differing
only in access: both stay kept in every arrival order, but the later access
holds nowhere and its author is never told. The keyed rule refuses the first
(`keyed_refuses_readd`) and drops the later addition of the second
(`keyed_drops_concurrent_access`). Rust now follows the keyed rule, and
`lean_differential_member_keys` compares both cases (storelog-data.md).
Convergence and the admin invariant for the keyed rule are `equal_received`
and `admin_remains` above.

The keyed rule also requires fresh keys for an addition of an id not active
in the view, including a removed member's return. `authorized_addition_fresh`
states it: such an addition is authorized only when neither of its keys
appears in a pair that view's replay bound. `effect_keeps_used` shows replay
never forgets a bound pair. `return_needs_fresh_keys` drops Ben's return with
his old pair and keeps it with a new one; `new_id_needs_fresh_keys` drops a
new id borrowing Ben's whole pair, his signing key or his sealing key. Rust
keeps the same history (`_coven_bound_keys`), drops such an addition as
`KeysAlreadyUsed`, and `lean_differential_member_keys` compares the bound
pairs with the model's.

Concurrent replacements of the same member conflict even with equal payloads.
`replacements_conflict`, `admin_preferred` and `same_kind_timestamp` prove the
comparison rules. A replacement whose author equals its target is self-replacement,
even when that member is an admin; an authorized different author must be an
admin in the recorded view. `other_admin_beats_admin_self` checks this case.
Removal beats a replacement of that member. Replacements of different members, role/access
changes, device changes and audience rotations do not acquire extra conflicts.
`admin_beats_earlier_self`, `same_kind_earlier_wins` and
`equal_payload_admin_wins` check complete replay outcomes, including restarts.
`any_order` transfers the admin/self result to every receipt order.

`replacement_preserves_membership` proves that replacement changes no role,
device, circle or membership. `admin_remains` preserves the existing last-admin
check throughout replay. `removed_cannot_be_replaced` refuses an absent target
at the point of application, as well as the recorded-view authority check.
`removed_stays_removed`, `removal_beats_replacement` and `last_admin_unaffected`
check removal races, replacement after removal and demotion of the sole admin.
`checked_metadata` checks causal closure, own-device predecessors, unique
creation and online first-attempt/storage facts for the example histories.

The reading requires fresh signing **and** sealing keys: replacing a leaked
credential with itself cannot revoke it. Kept bindings retain their public-key
history for the freshness check. Equal concurrent replacements remain events,
so their winner is determined before publishing the resulting state. Reusing
a previous signing or sealing key is refused. Random generation and collision
resistance are outside this model.

#### Old-key writes converge and their window verdicts become final

`OldKeyObjects` applies the window-only rule to replaced signing keys. The
signature must match the key current in `store_log_read`. Each unread kept
replacement of that member installing a different signing key supplies an
inclusive deadline: first complete publication must be no later than 30
storage days after that replacement. A write stored before the replacement
already satisfies its deadline. Keys from losing concurrent replacements are
covered too, not only the key preceding the winner. A write using the winner's
keys is current; read earlier replacements do not retire later keys current
in the recorded view.

`counts_iff` requires receipt, ordinary validation and every deadline.
`counting_requires_signature` preserves the author, device-owner and
recorded-view signing-key checks; schema/reset/content validation remains a
supplied predicate. `counting_requires_window` proves that every counted
old-key write meets each unread kept replacement's deadline.
`late_never_counts` excludes a late write for every received-write set.
`other_writes_do_not_change_verdict` proves that adding or removing any other
write cannot change this write's admission. Recorded write reads are absent
from the admission function: neither an explicit reader nor an implicit
own-device predecessor can rescue a drop. `counting_converges` proves equal
verdicts for equal entry and write sets and immutable storage facts.

`unread_replacement_deadline` does not assume which entry introduced the
old signing key. `losing_binding_is_retired` checks a write from the losing
self-replacement; `equal_payload_is_current` checks that it remains current
when the admin winner installs its keys. `boundary_inclusive` and
`admission_boundary_inclusive` check acceptance on day 31 and rejection on day
32 after replacement on day 1.

**Replacement finality fixes both window verdicts.**
`replacement_window_final` takes a kept replacement that retires this write
and the final kept prefix through that replacement's storage time, including
ties. Earlier replacement decisions cannot change. Any later replacement has
a deadline no earlier than this one's, so it cannot turn acceptance into
rejection; it cannot remove an earlier rejection either. Later kept entries
may appear or disappear. `final_replacement_finalizes_write` applies this
result to actual `Keyed.resolve` results and `countInReplay`, allowing arbitrary
other write receipts while fixing this write's presence and ordinary checks.
`later_replacements_preserve_verdicts` instantiates the theorem for successive
replacements and every possible write publication time.

This is conditional on entry finality supplying that stable prefix. Keeping
one replacement alone is not sufficient if an earlier replacement can still
change. The general horizon theorem has not been transferred to key-aware
replay; its final-prefix conclusion is an explicit input here. The history
and bound may contain any finite continuation; the write's recorded authority
view remains fixed. Independent schema/reset decisions can still change their
own verdicts.

**The send gate bounds remaining publication.** `writerCurrent` checks the
member, device owner and signing key in the supplied view. Admission uses the
recorded view; every provider request, including a retry, uses completed
current catch-up. `replaced_signer_cannot_send` rejects an old signer after
that observation, even when its recorded signature remains valid.
`send_gate_checks_current_view` checks both outcomes through replay.

`send_gate_publication_bound` assumes catch-up begun after finality refuses
the old signer, each send uses catch-up started less than five minutes ago,
and each provider request settles within one day, including publication after
a lost reply. It proves publication is strictly before finality plus one day
and five minutes. These are monotonic seconds, distinct from the immutable
storage times used for admission. Complete discovery, observation of final
replacement and continued refusal of the old credential are explicit inputs;
the function does not establish those IO facts. The bound permits discovery
to finish; it grants no exception to the write's deadline. Member devices
retain their ids, so finalizing a key replacement does not retire their logs.

**The former two-write failure is a positive example.** Creation on day 0 and
replacement on day 1 leave the finality window [3, 33] empty. The old-key write
lands on day 32, after its day-31 deadline. A current-key write lands on day 34
and counts, while the old write remains dropped.
`replacement_final_window` and `final_replacement_keeps_late_write_dropped`
check this history. The data package adds a store-key rotation on day 2 and
checks the same unchanged verdict with a permitted current-key first send
(`OldWriteWithRotation`). `unreceived_replacement_changes_admission` checks
that receiving a missing replacement changes admission, which is why finality
requires the complete earlier entry prefix. Changed admission requires an
atomic data rebuild.

Changes built on a dropped old-key write use the shared
[`DataDependency.dependsOnDropped` definition](merge.md#b17-receive-order-progress-and-dropping-by-data)
in the merge model. The unit is a row change, using its actual generation
source and replaced cell setters, not the write's recorded-read frontier.
An otherwise admitted dependent change becomes a recorded loss; unrelated
changes in the same write remain eligible. This package supplies the old-key
base admission verdict; it does not duplicate or re-prove the shared data rule.

Entries retain §9's permanent rejection against **any** unread stored entry,
kept or dropped. `entry_deadline` proves the inclusive bound for every kept
unread entry. Reader rescue applies to neither entries nor writes. The grace
window is not immediate protection against forgery by a holder of the old
signing key; honesty and signature verification remain model boundaries.

Encryption, signature verification, entry path/device validation, complete
provider discovery, random ids, SQL rebuilding and physical retention are
outside this proposal. Historical key-copy exposure and replacement rotation
are proved in [storelog-data](storelog-data.md#member-key-replacement).
No Rust correspondence is claimed for the new keyed interface.

### C14 Spec and format checklist for stable members

The normative member-key rules and layouts are in `spec/coven.md`,
`spec/format.md` and `spec/api.md`. Checked boxes below record specification
coverage, not a Rust implementation or a new theorem. The fixture regeneration
in the last item belongs to the queued format implementation; existing Rust
fixtures still encode their earlier layouts. Format 1 is the first unreleased
format; D9 and D14 no longer name version 2. The shared data-evidence encoding
remains an implementation obligation from merge B17. The list distinguishes
the chosen rules from properties the proofs do not establish.

- [x] **§9, §11 and D2:** define `MemberId` as a random 16-byte UUID, like a
  circle id, independent of keys. Add `SigningKey = 32-byte Ed25519 public
  key` with the existing invalid/weak-point checks. Change `MemberKeys` to
  `signing:SigningKey | sealing:SealingKey`. Keep `SealingKey` as 32-byte
  X25519. All member references and their byte ordering use the UUID.
- [x] **D6 author and creation:** retain `author:MemberId` in the entry
  header, now 16 bytes. Creation's existing `admin:MemberKeys` supplies that
  author's initial keys; the header supplies its stable id, so no duplicate
  admin-id field is needed. Verify creation against those keys and the store
  path. For other entries, replay `had_read`, resolve `author` there, and
  verify with that member's current signing key. The model's `signer` is
  verified evidence, not an additional wire field.
- [x] **D6 addition and replacement:** tag 1 becomes
  `member:MemberId | keys:MemberKeys | role:u8 | access:MemberAccess`.
  Add tag **16**, Replace member keys:
  `member:MemberId | keys:MemberKeys`. Leave tag 12 unused and retain all
  existing tag numbers. Replacement carries no audience keys, role, access
  or device changes. Creation/addition establish keys; replacement is the
  only operation changing an active member's keys. Reject reusing previous
  signing or sealing keys.
- [x] **§9 authority/conflicts:** allow the target's current signature or a
  current admin's signature in the recorded view. Classify by stable ids:
  author equal to target is self-replacement even for an admin; a different
  authorized author is an admin replacement. All concurrent replacements of one id
  conflict, including equal payloads; another admin beats self, then the
  earlier timestamp wins. A removal of that member wins. Require the target
  in both the recorded view and replay state; replacement never re-adds it.
  Keep role, last-admin, device and circle rules otherwise unchanged.
- [x] **§7.1, §10, §13 and D5/D6/D9:** authenticate writes from the device's
  stable owner and the current signing key in `store_log_read`; D5 needs no
  extra author or key field. Retain historical public bindings for checking
  past objects. Count an old-key write only within 30 storage days of every
  unread kept replacement it postdates, with the boundary included and no
  reader rescue, including through implicit own-write reads. Apply
  that deadline to keys from losing replacements too: compare with each
  unread winner's installed key, not only its preceding binding. Earlier
  replacements already read do not retire later keys current in that past.
  Preserve entry rejection under §9; a write's read cannot revive a too-late entry.
- [x] **§10, §15, §16.5, §17 and D5/D7:** feed old-key exclusion into the
  shared `DataDependency.dependsOnDropped` rule. An otherwise admitted change
  built on dropped data becomes a recorded loss for that change, preserving
  its original identity and values. Unrelated changes in the same write stay
  eligible. Use actual generation/setter sources, never recorded-read
  frontiers; preserve that evidence through snapshots and conversion as
  described in [the shared data checklist](merge.md#b17-receive-order-progress-and-dropping-by-data).
- [x] **§3, §9, §10, §15, §16.5 and §19:** state that old-key window admission
  is final with a kept retiring replacement and its preceding final entry
  prefix, including ties. Before finality, rebuild admission, rows and losses
  atomically when replacement decisions change. Apply the send gate to old
  signing credentials on every request, including retries: completed catch-up
  after finality refuses them; earlier catch-up and the request-duration bound
  allow no publication at or after finality plus one day and five minutes.
  This bounds outstanding old-key publication, not the continuing device logs.
  Keep independent schema/reset, snapshot coverage and file-retention checks.
  Describe the signing-key grace period accurately; it is not immediate
  signature revocation or a theft-recovery guarantee.
- [x] **§11, D9 and D11 key copies:** keep `member:MemberId` as stable routing
  identity and add the exact recipient `sealing:SealingKey` to the clear
  prefix, after `member`. The complete layout is
  `37:u8 | version:u16 | audience:Audience | key:uuid | member:MemberId |
  sealing:SealingKey | ephemeral:32 | nonce:24 | ciphertext | tag:16`.
  Authenticate that prefix in D11's existing context and require its sealing
  key to match the recipient key actually used by the box. Discovery retains
  both member and sealing key; opening an old copy uses its named credential,
  not a guess from the latest binding. Historical copies never disappear.
- [x] **§11 exposure/selection and §12 sharing:** every kept replacement
  exposes its previous sealing credential regardless of motive. Copies to it
  retire their store/circle keys for first attempts even though the member is
  still present. Catch-up precedes new sealing; route ordinary historical
  and rotation copies to the current non-retired sealing key. Binding-entry
  prerequisite copies instead use the validated candidate's fresh keys, as
  required for bootstrap below. Copies to credentials that never became
  current also expose their audience key, including a losing replacement's
  prerequisite copies to an otherwise-active member id. Rotate each affected
  audience when no safe held key exists; only its current members may rotate it. Historical sharing
  to the new key does not make the old exposed audience key safe again.
  Retried pre-existing ciphertext and copies arriving after observation retain
  §11's residual-window limits.
- [x] **D8 key-copy reports:** distinguish credentials in the report subject:
  `audience:Audience | key:uuid | member:MemberId | sealing:SealingKey`, ordered
  in that field order. A report about an old copy must not satisfy a wait for
  a copy to the replacement key.
- [x] **§12.2–12.3 and D8/D13 invites:** bind a chosen stable id into the
  invite. For a new person the admin generates a fresh id; for recovery the
  admin chooses an existing active member. Add `member:MemberId` after
  `invite:uuid` in both the invite code and the signed join request. The
  request becomes `invite:uuid | member:MemberId | keys:MemberKeys |
  device_name:name`; verify it with its supplied new signing key and require
  its member id to match the invite. Approval emits Add member for a new id,
  or Replace member keys for the existing id, using the request's exact keys.
  Do not let the requester choose which existing person to replace.
- [x] **§12 bootstrap and §13 access:** seal the initial and available
  historical audience keys to the approved new sealing key before publishing
  its binding entry. Bootstrap waits for a kept matching id-and-keys binding,
  then registers the new device under that id. A losing replacement remains
  a rejected join even if its prerequisite copies exist. Rebinding preserves
  role, circle memberships and existing device ids; storage-credential changes
  remain a separate access operation and require provider handling.
- [x] **§12.1 and D13 restore:** put `member:MemberId` after `name:name` in a
  restore-code frame, before `member_keys:bytes`. Keep the existing private
  signing/sealing key blob, now explicitly associated with that id. Refresh
  restore codes and custody after replacement. An old code cannot bind its
  old keys back to the id; recovery requires current keys or admin approval.
- [x] **D7–D9 other signatures:** use the snapshot's signed `store_log`
  coverage as its key lookup view for both signatures, and posted positions'
  signed `store_log` for their signer lookup. Add `store_log_read:EntryPositions`
  after `device` in the clock frame so its signature also identifies a key
  view. Treat unverified view fields only as lookup hints until signature
  verification succeeds. Join requests remain authenticated by their supplied
  keys. Admission/retention rules for old-key snapshots, positions and clock
  objects are outside the checked entry/write rule and must be stated explicitly.
- [x] **D10:** render member ids as canonical lowercase hyphenated UUIDs,
  replacing public-key hex wherever a member is spelled. Keep writer-specific
  key-copy paths and their gap-free sequence; key versions do not create a
  second log or overwrite old copies.
- [ ] **D1, D14 and §17.2:** version the changed layouts and regenerate
  member-bearing fixtures, signed objects, boxes, codes and mutation checks.
  Resolve the appendix's existing inconsistency: its introduction says format
  1, while its D9 example and D14 fixtures name version 2. Do not silently
  reuse a released format number for the changed byte widths. Pin tag 16,
  the stable author id, both key pairs, re-binding invites and copies to old
  and new credentials. Cryptographic labels need not change merely because
  the authenticated prefix now contains the additional fields.
