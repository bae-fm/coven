## Appendix C. Proof of the store log

- Lean checks the store log's terminating replay and entry accounting.
  C1–C9 describe the historical policy still compared with Rust; their key
  conflicts and examples are not the current §9 policy. C10 proves finality
  for its stated policy. C11 models rotation entries and §14.6's empty
  circles, sharing the same replay engine. The executable action type still
  carries removal-key data; it does not check the key-free removal encoding
  or the key-hash field required by D6.
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
  differential test against its `resolve`, comparing state, kept entries,
  and dropped entries. State includes the current access of active and removed
  members; generated histories include access updates and repeated access values.
  The histories include concurrent circle renames and renames against circle
  deletions in both timestamp orders. They also include concurrent key
  replacements and resets against version raises, in both timestamp orders,
  for the store and circles. The comparison includes each audience's selected
  versions and snapshots; generated histories also cover different versions of one audience,
  equal versions of different audiences, and circle deletions against raises.
- Rust retains each applied entry's author-view check, observed device owner,
  and derived circle deletions with its immutable bytes. The recorded past is
  closed before application and never changes, so reusing these facts is the
  same check as rebuilding that view. The complete received set still settles
  with fresh dropped marks on every arrival; a previous result is reused as
  the new entry's author view only when that entry has read the entire set.
  The differential test compares both batch and incremental Rust replay with
  Lean, and the Rust examples compare every causal arrival order through both
  paths. This optimization is not a separate Lean theorem.

### C10 Finality by storage time

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

**The saved prefix has stable replay results.** `horizon_stability` applies
the actual `CurrentReplay` rules. Once two received sets contain that prefix,
they agree on every prefix entry's kept and dropped result, regardless of
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
saved number, physical cleanup, or Rust correspondence. All named horizon
results are included in `Axioms.lean`; the Rust comparison retains its existing
replay functions.

### C11 Rotations and circles left empty after replay

`CurrentReplay` applies the contradiction rules, storage-time rejection and
recorded-past authority. Its exact circle-key-list check belongs to its
executable removal representation, not the key-free removals in D6. `Action.rotateKey` carries
D6's tag-15 audience and key id. `rotation_authority` proves that a kept
rotation's author belonged to its audience in its recorded past.
`rotations_conflict_with_nothing` covers every action in both directions.
Under §11 an outside admin never creates a circle key. Ana removes Carol
from the store; Ben, still in Gifts, rotates Gifts' exposed key. The model's
removal payload is not evidence for this publication order or its hash checks.

Ana and Ben can rotate Gifts concurrently; both entries stay. Dan, an admin
outside Gifts, cannot rotate it. Ben's concurrent removal does not erase the
authority his rotation had when written. `CurrentExamples.rotation_entries`
and `removed_rotator_keeps_recorded_authority` check these histories.
`deleted_circle_keeps_rotation` checks that a concurrent circle deletion also
leaves the rotation kept: it changes no membership and recreates no circle.

The engine takes an effect function. `CurrentReplay.realize` uses the shared
membership effects but deletes a circle during replay only for an explicit
deletion or a removal that saw its sole member. Other removals retain an empty
member list until `finish` projects the completed result. Author views use
that same projection. `finished_circles_nonempty` proves that no empty circle
is exposed as active; the finishing step changes no kept or dropped identity.

Ana's phone removes Ben from Gifts while her tablet removes Ana from the store.
Both saw two circle members. The completed replay hides Gifts and
`circleCause` names the later kept removal. Carol's concurrent addition can
still populate it: the removals stay kept and Gifts returns with Carol.
`concurrent_addition_populates_empty_circle` failed with the historical effect
and passes with the new effect. `empty_circle_cause` checks both results;
`explicit_deletion_still_wins` checks that a real deletion still defeats an add.

`empty_cause_latest` proves that a derived cause is a kept removal affecting
that circle and that no later such removal was kept. “Affecting” uses circle
membership in the removal's recorded past, including a repeated removal that
is already satisfied in replay. `equal_received` covers state agreement.
The generic termination, authority, accounting and prefix proofs apply to the
selected effect. C10's existing theorems keep their original policy, and the
Rust runner keeps `resolve`'s historical effects and conflict rules.

Key bytes, sealing, provider listings and physical deletion remain outside
this package. The data package supplies key selection and retained row inputs.
All named C11 results are included in `Axioms.lean`.
