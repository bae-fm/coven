# Store-log effects on data

The Lean package is [storelog-data](storelog-data/). It imports the merge and
store-log packages. Its executable models distinguish database state, knowledge
of keys, provider requests, physical storage, and what the app reads.

The existing policy models use the store-log package's `CurrentReplay.Action`, `Entry`
and `History` directly. Removals carry only their targets; creation and
rotation carry `KeyCommitment`. `CurrentReplay` supplies received and
recorded-past replay, and `ReplayPolicy` supplies admission and effects for
the data and retention proofs. No local action or replay adapter is needed.
`KeySelection.Key` is the shared `CurrentKeys.Introduction`; authorization
uses its received-introduction catalog, and ordering uses its newest-key
functions. This package adds receipt-based sharing and publication phases,
data loading, provider work and their visibility in the pending list.

Exposure in §11 is conservative: any copy addressed to an excluded member
retires its named key. Ana cannot decrypt a box for excluded Dan to check
its key hash. A forged K2 box can therefore cause an extra rotation, while
a recipient still rejects a key whose hash differs from K2's introduction.
The model's successful receipts do not prove this third-party detection or
the recipient's hash check; both are format/IO verification obligations.

## Results and their assumptions

**Revocation is proved with the stated observation window.**
§11 requires each pass to read every undrained writer's new copies through a
next-number miss alongside the store log. A completed local retirement drain
supplies that writer's complete retained history (§6). Permanent copies and these
reads supply the model's complete copy observation; theorem names use
“listing” for this supplied set and do not prove the numbered discovery
algorithm. [§3](../coven.md#3-guarantees), [§11](../coven.md#11-keys) and the
[three pass phases](../sync-pass.md#requests-in-order) are represented by
`KeyPhases.begin`, `rotate` and `firstSend`. Both membership and key-copy
observations must complete. A currently running audience member publishes
rotation copies and then the entry before ordinary data can use its key.
`rotation_by_remaining_member` proves that the rotator still belongs and
runs; `rotation_requires_publication` proves that both receipts are required;
`HistoricalExamples.rotation_in_second_phase` checks Carol rotating after
Ana's key-free removal of Ben, then first-sending with K2. Provider access
can remain pending while this protected data proceeds.

`CovenIO.Revocation.gated_first_send_revocation` joins this selection to the
actual send in `IOTrace`. Its catch-up started less than five minutes before
the request, including sleep, and the membership and copy scans finished
inside that interval. `completed_catchup` supplies this package's existing
`KeyPhases.CaughtUp` from retained receipts and the completed numbered reads.
`rotation_retains` proves that a successful phase-two rotation preserves both
observations, so selection can use a newly published replacement too.

For a removal final before catch-up started, the joined theorem carries its
kept verdict through §9's finality theorem. `kept_removal_excludes` derives
absence from every audience by replaying the actual kept effects, provided
no later kept addition restores that member. This is not an assumed membership
verdict. With §11's no-later-copy condition, the selected key has no copy
addressed to that member in the represented storage at the send.

The hypotheses are named: causal history; the quiet finality window, old
removal and complete old prefix at both observations; successful authenticated
entry receipt and complete copy routing facts; custody; identity and freshness;
no later kept membership addition; selection by `firstSend`; and copy coverage
through the send, with no later copy of that key for that member. Provider
publication times and the cutoff one hour plus a positive provider unit before
the clock sample supply the old prefix in `caughtUp_complete_old`.
The finality certificate's storage timestamp is
distinct from the monotonic instant when it was established. The
[IO write-up](io.md#joined-revocation) states the timing and decoding boundaries.

§4's bound on storage-clock advance supports §15's recent-write catch-up;
it is not needed to retain permanent entries and copies. §10's one-day remote
request bound is used by `retired_copy_settles` for retirement drains, including
lost replies. It does not establish the no-later-copy hypothesis. Freshness
bounds stale sending without eliminating copies published after observation.

`KeySelection.revocation_between_listings` and `KeyPhases.revocation` prove
that a selected sealing key has no copy for an excluded member in the listed
storage plus later copies, provided `noLaterCopy` holds. The latter connects
the result to the sending device's replay and first-send path, and proves
that the sender differs from the excluded member. The departed author's own
pre-removal circle writes are outside this path and this secrecy promise.
Attempted retries retain their original key and bytes. The absence of a
copy expresses unreadability in the receipt model, not a cryptographic proof.

**Revocation without the window hypothesis fails.** Ana and Ben are admins;
Carol is a member. Three concurrent entries demote Ben, promote Carol, and
let Ben remove Ana. Carol receives the removal and separately rotates K.
Ben's tablet later sees the removal drop on demotion, shares K with the
restored Ana, then sees the removal return when Carol's promotion arrives.
Ben's phone receives promotion before demotion and keeps the removal
throughout. Its complete copy observation still finds Ana's permanent copy and
refuses K (`SecurityExamples.listing_prevents_reuse`).

Move that share after the phone's miss for that writer: it selects K while Ana can obtain
the new copy. `residual_window_counterexample` and a Lean `example` check the
failure and the next observation refusing K. `sharing_history` checks both causal
orders, the intermediate drop and the allowed share; `storage_history_valid`
checks online first attempts and storage landings. Extending `later` to the
whole readable lifetime of a write strengthens the premise to that period;
it cannot prevent later disclosure.

`CovenIO.RevocationExamples.in_flight_after_finality` checks a five-entry
history without a replay reversal. Ana registers a second device, then her
devices concurrently add Ben and rotate to K1 without seeing his addition.
Ben’s initial K0 copy precedes his admission; the rotation publishes K1 for Ana,
its only recorded member. Historical sharing then reserves K1 for Ben before
his removal. `prerequisite_copies_published` checks both publication conditions. The reservation survives until after removal
is final. Its gated request starts before another device's copy miss, and lands
after the miss but before that device's fresh first send with the same key.
The remote request takes three seconds. `caught_up_final_prefix` checks the
actual discovery-to-finality bridge, and `joined_guarantee_instantiated` applies
the joined theorem before that delayed copy lands. Anonymous Lean examples
also check the stronger claim's failure, removal during catch-up, non-final
removal reversal, the retired-write boundary and fixed retries.

None contradicts current §3: it expressly requires no copy after observation,
and §11 accepts that residual window. A removal landing during a pass can
follow its writer's miss; a non-final verdict can reverse; §10 admits an unread
retirement's writes through the inclusive thirty-day landing deadline; retries
keep their original key. Those sentences rule out an unconditional promise
about every send after publication or finality.

**Historical sharing and continued readability are proved.**
`HistoricalExamples.removal_reversal_and_sharing` checks the history in
[§14.4](../coven.md#144-writes): Ana removes Ben, Carol rotates Gifts to K2
and writes, then Ben's earlier concurrent removal of Ana returns Ben.
The history uses S3, where neither admin owns the other's provider account.
Carol is still running and shares K2. `ben_never_stopped` checks that Ben's
receiving installation never stops; it sees its removal only after it drops.
`historical_parts_apply` opens that copy, applies the original part on Ben,
and checks that Carol's already applied value survives the replay.
`membership_keeps_part_visible` checks the resulting app views on both devices.
`historical_snapshot_loads` loads the same snapshot under K2 on both sides
of the membership reversal. Missing K2 produces a key wait.

`KeySelection.authorized_introduction_persists` proves the general rule:
retaining an authorized introduction keeps it authorized regardless of the
new membership replay. `HistoricalKeys.historical_key_stays_readable`,
`parts_stay_loadable` and `snapshots_stay_loadable` connect that rule to
current audience membership and custody. They preserve the ordinary part
checks for historical writer authority, received entry past and schema/reset
boundaries; they do not promise to bypass another missing prerequisite.
`membership_preserves_applied` proves that replay alone never changes the
retained applied data. The snapshot catalog also uses received authorized
introductions, without a kept-entry requirement.

**Entry fate and data convergence are proved for the modeled effects.**
`ReplayEffects.entry_fate` proves that applying the final kept entries alone
and then removing circles left empty reproduces the actual replay's state.
It uses the shared engine and its recorded-past removal effects; it does not
assume the result already equals that fold. `CurrentData.convergence` connects
equal received entries and equal causal, readable writes to equal rows and losses. Original merge inputs remain
intact when membership or deletion changes.

Those equal write inputs must satisfy §10's admission rule. For every kept
entry removing a device or its member, or replacing its id, a write must
not have read that entry and must land no more than 30 storage days after
it. Publication at the deadline counts; a later write is excluded while
the retirement is kept. Equal storage times, recorded reads and kept entries
produce the same verdict; replay recomputes it atomically from retained
inputs. Dropping a retirement can restore a write, but cannot undo §9's
permanent rejection of an entry landing more than 30 days after any unread
entry, kept or dropped. Neither rule needs the request-duration assumption.

`CurrentData.convergence` assumes equal causal write inputs; it does not
prove this storage-time admission calculation. Required checks cover
removed and replaced devices, the exact boundary and a later landing,
different arrival orders, and replay reversal restoring the write without
restoring a permanently late entry. The retained inputs include the write's
first storage time and recorded store-log reads until its verdict is final.

`CurrentExamples.entry_only_deletion_restores` checks §14.7's deletion and its
reversal without a row write. `deleted_return_reloads` includes a circle that
was absent before replay, provided a non-late creation was already applied.
The [returning-circle proof](#returning-circles) establishes why that condition
is sufficient. `passed_position_does_not_lose_part` loads a formerly
skipped part even though its overall position already passed. Missing reload
inputs retain the old data and positions, refuse circle writes and appear in
the pending list. `stale_reload_keeps_positions` prevents an old prepared
reload from replacing a newer entry view. `CurrentData.stopped_forever` proves
that an installation stops receiving after its own removal.

Rotations and replay-empty circles use the shared conflict and effect
functions through `CurrentReplay`; admission has no removal-key prerequisite.
`empty_circle_rows_and_cause`
checks Ana and Ben's concurrent removals: Gifts is hidden, the loss names the
latest kept removal, and Carol's concurrent addition restores its original row
and clears that cause. `ReplayEffects.entry_fate` and `StorageFinality` use the
same effect function, including the finishing projection.

**Key selection is proved against storage evidence.**
`first_attempt_safe` requires current membership, an authorized received
introduction, custody and no observed copy for an excluded member.
`listed_exposure_retires` and `exposure_persists_while_excluded` make permanent
copies disqualify the key for as long as their recipients remain excluded.
A recipient returning no longer disqualifies that key; there is no remembered
retirement bit. `selection_is_newest` proves timestamp and key-id ordering,
and attempted retries retain their key and bytes.

`selectInReplay` derives membership and historical authority from the
key-free replay. `selected_introduction_authorized` connects selection to a received,
authorized introducing action, including tag 15's actual audience and id.
`rotations_coexist` checks Ben and Carol's concurrent rotations after Ana's
removal: both stay kept, and the later usable key is selected. Fresh random ids
and successful key acquisition are inputs. Recipients are members; removing
one device does not remove its member or rotate member keys (§13).

**Provider scheduling and completion are proved for one owner's device.**
[§13](../coven.md#13-removing-members-and-devices) has one scheduling boundary:
`Access.applyEntries` commits the received entries, replay and resulting access
intention together. A failed commit retains the previous state and returns
the failure. `removalCall` only observes whether its entry is kept; it returns
unit without changing the journal or contacting the provider.
`apply_records_intention`, `apply_removal_visible` and
`HistoricalExamples.removal_access_one_path` connect that transaction to the
pending revoke or owner wait, before any provider completion.

`Access.start` refuses another request while one is in flight. A changed
intention leaves that request intact. `completion_matches_or_queues` proves
that successful completion matches the current intention or queues the next
request. `obsolete_completion_queues_opposite` and
`SecurityExamples.serialized_regrant` check revoke followed by re-grant.
`HistoricalExamples.replay_reversal_queues_regrant` connects that sequence
to the atomic entry-application boundary.
Unchanged replay preserves a reported failure, including retained grants.
An explicit due retry uses the current intention and remains visible.
`pending_until_finished`, `owner_wait_visible`, `retained_grant_visible` and
`finished_work_absent` connect those states to the pending query.

The account proof quantifies over one already recorded account; provider account
discovery, provider implementation and coordination between owner devices are
outside it. The existing journal supplies operation ids for the pending subjects.

**Every recorded S3 key is accounted for, and pending deletion requires evidence to clear.**
[AccessKeys](storelog-data/CovenStorelogData/AccessKeys.lean) extends `Access`
with `Access.S3`, using the existing `CurrentReplay.Entry`, membership effects,
conflicts and terminating `ReplayMachine`. This replay changes only set-access authority:
an active member records their own key, or an admin records another active
member's replacement, judged in the entry's recorded view. The target must
also exist at replay. `replacement_authority`, `kept_authorized` and
`other_member_requires_admin` prove these checks at admission and in the
actual replay result. Provider-admin access is a separate operation input;
holding it does not grant a store-log role. Signature authentication supplies
the stable author id, as in the existing access model.

`every_recorded_key_accounted` proves that each received key is either the
member's current access, pending deletion, or already confirmed deleted.
Recording a replacement does not schedule its current winning key for deletion
merely because it was recorded. The old key from an
authorized replacement's recorded view becomes pending immediately, even
when that replacement drops (`replacement_old_pending`). Recording the same
id again does not replace that credential. Every key that replay displaces,
including a losing concurrent replacement, becomes pending too.
These are S3 access keys, distinct from member signing/sealing keys: their
concurrent replacements retain §9's ordinary earlier-timestamp rule, without
the admin-over-self priority for replacing the member's private pair.

`removed_recorded_pending` has no kept-entry or author-identity premise:
removing a member covers every key recorded for them, including a later
received or time-dropped entry. It extends `credential_pending_visible` and
the retained `HistoricalExamples.dropped_access_stays_pending` result.
`received_never_forgotten` preserves those inputs across all modeled events.
The journal commits received entries, replay and new deletion obligations
together, and retains obligations through replay reversal. A returning member
whose key is pending deletion needs a fresh replacement. These S3 obligations
differ from reversible account-sharing intentions.

`pending_record_iff` connects each key to an actual `Pending.records` result
using the existing `credentialWork` and journal operation id.
`no_other_transition_clears` and `history_pending_until_cleared` prove retention
for one event and arbitrary event histories. `history_loss_requires_clearance`
proves that a disappearing pending key must have a clearing event for that id.
There are exactly two such events: explicit `confirmAccessKeyDeleted`, and
successful deletion with provider-admin access followed by an absence read.
`deletion_requires_exact_absence` and `provider_clear_requires_observation`
require the read to name the exact deletion target. A missing read, failed
read, present key, different key id, failed delete, or lack of admin access
cannot clear it. Each failure is returned to the initiator.
`records_clear_only_with_evidence` requires the local commit to succeed too;
`failed_commit_retains` preserves the preceding state on failure.
`confirmed_never_returns` proves that later entries and retries cannot
restore a confirmed notice. No timer, replay or reset can clear it.

[AccessKeyExamples](storelog-data/CovenStorelogData/AccessKeyExamples.lean)
checks two admins replacing Carol's key, and an admin racing Carol's own
replacement, in both receipt orders. Before removal the old and losing keys
are listed; after removal all three ids are listed. The winning key can be
Carol's own earlier replacement (`self_replacement_can_win`).
`removal_before_replacements` checks removal arriving first.
`removal_and_later_record` checks the replacement landing after its thirty-day
limit; `late_access_after_finished_removal` checks its key becoming pending
even after all previously recorded keys were cleared. `checked_metadata`
checks causal closure, own-device predecessors and online attempts for both
replacement histories. `authority_uses_recorded_view` accepts Ben's replacement
concurrent with his demotion, refuses his subsequent replacement after reading
the demotion, and checks that a non-admin can record only their own access.
The deletion examples check both clearing paths and every failed observation
case, including a failed database commit.

There is a checked `example` of key loss if the journal is omitted: create
Ana, add admin Ben, then their concurrent removals of one another. Neither is
the protected storage-account owner. Receiving
Ben's removal first lists Ana's key. Receiving Ana's earlier removal returns
Ana; the query derived solely from current replay loses her key without a
clearing event. `replay_reversal_retains_deletion` checks that the journal
keeps it pending in this same four-entry history. This is a counterexample
to discarding the journal, not to the two permitted clearing rules.

The model covers one device's durable deletion journal and received entries
from multiple authors. It does not prove provider key creation, secret
delivery to remaining devices, admin credential acquisition or destruction,
provider truthfulness, signature verification, invite scheduling, SQL/crash
recovery, provider-account ownership, or coordination between devices' confirmation records. Provider
receipts represent deletion followed by its read; actual IO ordering and
consistency are inputs. The journal holds key ids and never admin credentials.
The unchanged account and historical theorems remain in the axiom audit;
Rust/Lean comparisons keep their original replay interface.

`accessKeyRunner` compares this journal with Rust. Each history is checked by
the shared JSON reader; S3 accesses are compared by key id. After each event,
Lean's `keys` of `applyEvent` must equal the key ids Rust's pending list
names, and the event's reply must match. Rust computes obligations with its
production function over the received entries and their replay, records them
in its database journal, and checks provider deletions with its production
`verify_deletion`. The histories cover two admins' and an admin's and the
member's concurrent replacements in both receipt orders followed by removal,
an unauthorized replacement by a member, a replacement landing after its
thirty days, concurrent removals whose replay returns a member, and recording
the same key again. Every deletion failure and both clearing events are
exercised. Rust's deleting call requires admin access by its type, so the
runner supplies it; `adminRequired` has no Rust counterpart.

**Every modeled pending subject is visible with its first reason.**
`Pending.every_pending_subject`, `only_first_reason`, and `first_unmet` connect
the processing conditions to the list required by
[§19.1](../coven.md#191-noticing) and [E5](../api.md#e5-storage-and-sync).
Observation replaces that subject and reporting device's previous record.
A failed database commit makes reads fail; it cannot return a successful empty
list. Paired calls and subscriptions use the same query. Reset suppression
retains the original observations and restores them when its entries drop.
These proofs concern the modeled work registry. They do not establish that
Rust observes every storage failure, nor model every native error payload.

**Two more ways a record ends are proved safe.** A loaded snapshot can move
this device's write positions past a write that was waiting. `Pending.cover`
runs in that load's transaction: it ends this device's wait for each write
the new positions cover, and keeps a refusal or drop of the write's bytes.
`cover_removes_only_covered_waits` and `cover_keeps_verdicts` prove that
nothing else is touched: no peer's report, no other subject, no verdict.
`covered_wait_gone` proves no covered wait is left. Why the wait is really
over: the download only fetches writes its positions do not cover, and
positions never move back (`raise_covers`, `history_positions_monotone`).
`covered_never_scheduled` and `cover_safe` prove the covered write is never
fetched again, so its wait could never have ended by the write applying.

Each pass reads every peer's positions object, and each audience's snapshot
listing reads every snapshot of that audience. `Pending.replace` lets one
completed listing replace this device's records for that class as a whole.
Given a listing that names exactly the objects storage holds, with the
conditions their processing checks now (`Listing.Complete`),
`listing_decides` proves the record for each subject of the class is that
subject's current first unmet condition. `absent_subject_cleared` covers a
subject with no object: nothing is left to process, so it has no record.
`listing_safe` and `listing_removes_only_ended` prove a record is removed only
when its condition no longer holds. `replace_keeps_others` proves records
outside the class, and peers' reports, stay. This device's own positions
record is outside every class: it belongs to its publication, which clears
it only once storage confirms the upload (`own_positions_kept`). A failed
check of its own listed object is an ordinary observation of that subject.
A failed listing replaces nothing, as in Rust; that a scan really listed
everything is an input.

`Local.step` runs observations, covers, listings and store-log scans. As in
Rust, a listing naming a subject twice or outside its class, or a scan naming
an entry twice or with a reason it does not decide, is refused and changes
nothing.
`history_unique` and `history_one_record` prove every reachable registry holds
one record per subject and reporting device. `every_pending_subject` and
`only_first_reason` quantify over any work list, so they apply unchanged to
every state these steps reach. Checked examples run a cover over a wait, a
refusal and an uncovered wait; a pass that drops two positions records while
another listing's snapshot wait stays; a refused duplicate listing; two
store-log scans; and a snapshot refusal surviving a listing, then clearing
once the snapshot is deleted. Positions moving back on a reset or in-place
reload are outside this model; a reset clears the list atomically with its
reload (§19.1).

**Entry waits are proved to end exactly when the scan stops finding them.**
Each store-log scan decides every entry wait this device recorded.
`Pending.scanEntries` replaces this device's entry records whose reason is a
wait (`Waits`, `KeyUnavailable` or `UpdateRequired`) with the waits the scan
found. A named entry's new wait replaces whatever record it had, as Rust
saves it. `entry_scan_decides` proves that afterwards this device's wait on
an entry is exactly the one the scan found. `entry_scan_safe` proves that no
wait is removed while the scan still finds it; `entry_scan_removes_only_ended`
proves that a removed record was this device's record of an entry whose wait
the scan no longer finds. `entry_scan_keeps_others` and
`entry_scan_keeps_verdicts` keep peers' reports, other subjects, and refusals
and drops of entries the scan does not name.

[PendingScans](storelog-data/CovenStorelogData/PendingScans.lean) computes
those waits from what the scan read (`scanWaits`): a missing key, a newer
format, or, for an opened entry and for this device's own queued entry, the
first entry of its recorded past that has not applied. Rust looks for it in
readiness order: the entry's own previous entry, then each entry it read.
`first_missing_first_unmet` proves that search returns the first unmet
readiness condition, so the record is §19.1's first unmet condition.
`own_entry_waits` and `read_entry_waits` prove each such entry gets its wait,
and `scan_names_only` that no other entry does. `applied_entry_never_waits`
proves an applied entry keeps no wait; `refused_entry_never_named` proves a
refused entry is never named, so its refusal stays. `scanWaits_valid` proves
Rust's commit never refuses the replacement.

Two readings. Rust does not read an entry behind a stopped entry of its own
log (a refusal or a newer format), nor one whose previous entry neither
applied nor opened in this scan. We read §19.1 as giving such an entry no
record of its own: its log's first unread entry carries the record
(`behind_stop_never_named`). An opened entry whose whole recorded past
applied while the store's creation has not waits for that creation with no
record; Rust only logs it, and so does the model. Which entries a scan reads
is an input.

**A refused snapshot is proved never to load.** Every snapshot listing keeps
this device's refusal of each snapshot it still lists
(`Listing.keepingRefusals`, applied by `Local.step`; `listing_keeps_refusal`).
A snapshot no longer listed loses its record (`absent_subject_cleared`).
`PendingScans.catalog` follows Rust's catalog. A refused snapshot whose prefix
verified and counts still adds its positions to the required history
(`refused_prefix_requires_history`), but is never offered for loading
(`refused_never_offered`). `never_loads_refused` proves loading applies only
a snapshot whose complete body checked and which this device had not
refused; `load_refuses_failed` proves each snapshot it does not load failed
its check. `refused_stays_unoffered` follows a history: the refusal a failed load
records keeps that snapshot out of every later catalog while listings still
name it. A newer installed version may retry a refusal (§19.1); loading
here runs one version, so that retry is outside it. Retention across
versions is modeled below.

**Passing over a newer-format snapshot in retention fails (checked
counterexample).** `select` makes the reload and the return check wait with
`UpdateRequired` while any listed prefix is newer than this version reads
(`reload_waits_for_newer`). Retention that instead treats such a snapshot as
unreadable judges deletion against the newest snapshot it can read, as
Rust's log retention did. `newer_snapshot_retention_counterexample` checks
the failure.

Ana (device 1) and Ben (device 2) snapshot the store concurrently. Ana's
snapshot covers her writes through 10 and Ben's through 2. Ben runs a newer
version; his snapshot, in format 2, covers Ana through 3 and himself through
20. It covers more writes, so it is the store's newest (§15). On Ana's
version the reload waits for the update. Retention judges by Ana's own
snapshot, which covers her write 4, and deletes it; the other gates (a
covering snapshot a day old, posted positions or 30 storage days, finality)
can all pass. After the update the reload selects Ben's snapshot, which does
not cover write 4, while Ana's uploaded writes through 10 are in its fixed
target. §15 forbids this deletion, since the current newest counting
snapshot must cover the write; Ana cannot tell which snapshot is newest
without reading Ben's prefix.

`waiting_retention_safe` proves the safe rule: retention that waits like the
reload while any listed prefix is unreadable judges against the snapshot the
reload selects once it proceeds, at this version or any later one, so it
never deletes a write that reload needs. Rust's log retention waits this way
(`snapshot_retention.rs`): an audience whose listing holds a prefix waiting
for an update gives no write coverage, and
`a_newer_format_snapshot_holds_log_retention_like_the_reload` runs the
counterexample. File retention already waited; growth only decides whether
to write another snapshot. Log retention also waits while a prefix waits for
its author's registration
(`a_snapshot_waiting_for_its_authors_registration_holds_log_retention`); this
model does not cover that case.

**Passing over a refused snapshot in retention fails (checked
counterexamples).** [RefusedRetention](storelog-data/CovenStorelogData/RefusedRetention.lean)
gives each installed version its own verdicts on a listed snapshot: whether
it reads the prefix format, whether the prefix check passes, a body refusal
this device recorded, and whether it accepts the key sealing the rows. The
signed positions belong to the object and do not change between versions.

§19.1 makes no refusal final across versions. Every `Refusal` variant has
retry class `AfterUpdate` (`every_refusal_after_update`), and a different
installed version checks the bytes once more whatever check failed
(`every_kind_rechecked`); the refusing version never does
(`same_version_keeps`). So a snapshot one version refuses can be the newest
for a later version, or for a device on a version whose check passes.

The examples use one store. Ana's snapshot covers her writes through 10 and
Ben's through 2; Ben's covers Ana through 3 and himself through 20, so it is
the newest. Ana has uploaded her writes through 10, so her reload's fixed
target includes them. In each, Ana's retention deletes her write 4. A
version whose checks pass selects Ben's snapshot and needs write 4, and
§15's missing-write rule skips it as having no part, so its changes are lost.

- `refused_body_retention_counterexample`: Ana's version refuses Ben's body.
  Rust's log retention judged by its loading candidates, which leave refused
  bodies out, so it judged by Ana's snapshot. §15 as written does not fail
  here: a refused body's verified prefix still establishes history, and the
  current newest is chosen by prefix.
- `missing_key_retention_counterexample`: the same through a snapshot whose
  key introduction Ana's device does not accept, which Rust's candidates
  also leave out.

Rust's log retention now judges by the newest verified prefix that follows
the audience's boundary, whatever this device found in its body or key
(`snapshot_retention.rs`). `a_refused_body_still_decides_log_retention_by_its_prefix`
and `a_snapshot_whose_key_is_not_accepted_still_decides_log_retention` run
both counterexamples on this store: Ana keeps her writes 4 to 10, deletes 1
to 3, and a device whose checks pass selects Ben's snapshot and reads write
4 from her log. Deleting a device's own superseded snapshot still compares
against the newest snapshot it can load, which is safe
(`superseded_deletion_invisible`, below).
- `refused_prefix_retention_counterexample`: Ana's version refuses Ben's
  prefix signature. §15 said a refused prefix "establishes nothing and does
  not hold deletion", so retention judged by Ana's snapshot; her updated
  version verifies the prefix. §15 now holds deletion while a refused
  prefix is listed, and Rust's catalog counts such a prefix as leaving the
  newest unknown (`snapshot_catalog.rs`):
  `a_refused_snapshot_prefix_holds_log_retention_while_it_is_listed` runs
  this counterexample on the store, where Ana keeps all ten writes until the
  listing stops naming Ben's snapshot.

`classified_safe` and `classified_never_skips` prove the general rule.
Retention holds an audience while any listed prefix waits for an update or
is refused with a kind not marked permanent; otherwise it judges by the
newest verified prefix, whatever this device found in that snapshot's body
or key. When it deletes a write, every later version selects a current
newest snapshot covering it, so the reload loads that snapshot, or waits if
it cannot load its body, and never skips the write. The premises: the later
version reads every format and verifies every prefix the earlier one did,
and no later version verifies a prefix refused with a kind marked permanent
(`Sound`). The spec marks no kind permanent, so the rule it supports holds on
every refused prefix: `holding_safe` and `holding_never_skips` need no
classification premise. `as_written_safe_for_bodies` proves §15 as written
safe while no listed prefix is refused. The prefix counterexample shows that
treating the signature refusal as permanent is unsound.

**The reload waits on a newest snapshot it cannot load (proved).**
[ReloadWait](storelog-data/CovenStorelogData/ReloadWait.lean) models the
reload's choice for one audience (`choose`, Rust `Catalog::take_current`
and `load_snapshot`). It waits for an update while a listed prefix is in a
newer format; otherwise it takes the current newest counting snapshot, the
newest verified prefix. It loads that snapshot, or waits on it when this
device holds a refusal of its body, does not accept the key sealing its
rows, or its rows need a newer app schema. It never loads an older one.
`missingDecision` decides a missing write from this choice and has a
waiting outcome of its own.

- `choose_loads_newest`: loading applies only the current newest snapshot,
  after its prefix verified, with no refusal of its body held here, a key
  this device accepts and rows at this app's schema. Refused bytes are never
  loaded, and an older snapshot never stands in.
- `choose_waits_iff` and `waits_listed`: the reload waits on a snapshot
  exactly while it is the current newest, still listed, and this device
  cannot load it.
- `wait_ends_when_newer_lands`: once a newer snapshot this device can load
  lands, the reload loads it. `wait_ends_when_unlisted`: once the listing
  stops naming the snapshot, the reload no longer waits on it; the next
  newest decides. `waits_retry`: the reload's own record retries
  automatically for a body or key wait, so the next pass sees either
  change. It names the snapshot (`Waits(Object(path))`, whose own record is
  the refusal) or the key. A wait for a newer app schema retries after an
  update.
- `choose_never_needs_deleted`: after `RefusedRetention`'s retention
  (`coversHolding`) deleted a write, any later version's reload loads a
  snapshot covering it or waits on one that does.
  `deleted_part_never_skipped` extends this to a reload of several
  audiences: each audience of the write's parts that the reload loads
  covers the write, so none skips its part.

Passing over the newest, as §19.1 said ("passed over for the next latest,
or the logs") and Rust did before T154, fails
(`two_audience_pass_over_counterexample`, checked). Ana's writes 1 and 2
each have a store part and a Gifts part. Her store snapshot and her newer
Gifts snapshot cover both, so retention deletes write 2. Ben's version
refuses the newer Gifts snapshot's body. Passing it over, he loads the
older Gifts snapshot. Rust counts a missing write as deleted when a
selected snapshot covers it (`skipsPart`), and the store's does, so Gifts
skips write 2's part as having none, though Gifts' newest covers it. With
one audience the absence would not count and the reload would fail on the
missing write instead. The waiting reload waits on the refused snapshot.
`a_reload_waits_on_a_newest_snapshot_whose_body_it_refused`
(`snapshot_boundaries_tests.rs`) runs this history in Rust and loads the
newer Gifts snapshot once one lands.

**Three other comparisons against the newest loadable snapshot (checked).**
[ReloadWaitChecks](storelog-data/CovenStorelogData/ReloadWaitChecks.lean)
checks the decisions that still compare against the newest snapshot this
device can load rather than the newest verified one.

- Deleting this device's own superseded snapshot is safe (proved). Rust
  deletes one once the newest snapshot this device can load covers
  everything it covers (`snapshot_retention.rs`). That snapshot's prefix
  verified, and it precedes every other loadable snapshot that follows the
  boundary. `superseded_deletion_invisible` proves that deleting a listed
  snapshot which a verified listed snapshot precedes changes neither the
  reload's choice nor log retention, at this version or any later one: the
  deleted snapshot is never the current newest while the other is listed.
- Snapshot growth is safe (proved). Rust measures growth since the newest
  snapshot this device can load (`snapshot_grew`). Growth only decides
  when to write a snapshot, not what it holds. `landing_keeps_deletions`
  proves that a new snapshot covering everything the current newest
  counting snapshot covers keeps every write retention deleted covered,
  whatever triggered it. That premise is §15's preparation rule (IO
  `canPrepare`), which must compare against the newest verified prefix:
  `loadable_preparation_counterexample` (checked) shows a snapshot that
  covers only what the newest loadable one covers outranking the refused
  newest and losing a deleted write. Rust has no explicit preparation check;
  a completed catch-up supplies it (IO `completed_catchup_covers_old`).
- A reload that needs a write this device refused waits on that write
  (proved, `waiting_on_the_write_loads`). Recording the write's own
  `Refused` record as the reload's reason, as Rust did before T159
  (`pending_logs::required`, then `record_operation_error`), keeps it
  blocked until an update, even once a snapshot covering the gap lands
  (checked counterexample, `refused_write_reload_counterexample`), though
  §15 says "a snapshot covering it can load without reading it". `Refused`
  retries only after an update, and `operation_can_retry` keeps
  `resume_snapshots`, `drive_snapshot` and the app's retry from driving the
  operation again at this version. In the model Carol's reload selects a
  snapshot through Ana's write 3, needs write 4, which her version refused,
  and is blocked. Once a snapshot through write 4 lands, the reload would
  load it. Recording the reload as waiting on the refused write
  (`Waits(Object(path))`), as it waits on a refused snapshot, retries
  automatically and loads the covering snapshot. Rust's reload now records
  that wait (`snapshot_reload.rs`);
  `a_required_stuck_gap_waits_for_a_covering_snapshot_and_keeps_local_edits`
  (`device_log_sync_tests.rs`) runs the history.

**A snapshot's retirement-window verdicts (proved; counterexample
checked; §15's finality wait proved).**
[ReloadVerdict](storelog-data/CovenStorelogData/ReloadVerdict.lean) adds
the check Rust's load transaction makes since T103. A snapshot carries each
covered change it holds with its author's verdict, including `dropped` by
the write's own retirement window (D7 section 5). It names no storage time
for covered writes, so a loader cannot judge that window again. Instead it
compares each carried verdict with the landing verdict this device
recorded for the same write, if any, and a difference fails the load
(`write_landing::agrees`, `SnapshotError::RetirementVerdict`). Nothing is
recorded against the snapshot. The reload's own record is
`Failed(Database)`, which only an app action retries, and a retry selects
again (`chooseVerdict`: `choose` followed by that check; `verdictRecord`,
`verdict_needs_app_action`). The window verdict is merge's
`Receive.withinWindow` over the kept retirements (`lateUnder`).

- `verdict_load_agrees`, `verdict_iff`: a reload loads only `choose`'s
  snapshot, and never one whose carried verdict differs from a verdict this
  device recorded.
- `late_depends_on_old`: a window verdict reads only the kept retirements of
  the write's device stored more than thirty days before the write.
- `prepared_agrees`, `agreeing_never_waits`: when the author's kept
  retirements agree there with this device's, the check never fails and
  the reload is `choose`'s.
- `loaded_verdict_final`: a snapshot prepared once that old prefix was
  final, for every carried write, carries each write's final verdict. A
  device that loads it holds what a device judging each write itself under
  the final kept retirements holds.
- `newer_agreeing_ends_wait`: once the reload runs again, a newer snapshot
  this device can load, whose verdicts agree, ends the failure.

Without that finality the check both stalls and lets divergent verdicts
load (`unfinal_snapshot_counterexample`, checked). `VerdictHistory` is a store-log history in §9's storage-time
replay (`Finality.resolve`). Ana and Ben are admins, Carol a member. Without
reading one another, Ana's tablet makes Carol an admin, Ben demotes Ana, and
Ana's phone removes Ben on day 20, safe only while Carol is an admin. Ben's
device publishes write 1 one second after that removal's thirty-day
deadline. On day 51 Ana has read every entry, her replay keeps the removal
(not yet final), and her snapshot carries write 1 as `dropped`. On day 60
Carol sets her own access, having read Ben's removal but not her promotion.
That change contradicts the promotion and wins, so the final replay drops
the promotion and then Ben's removal, which would leave no admin; the drop
is final from day 91 (`removal_kept_then_dropped`). The final replay admits
write 1.

- Ben recorded his admission while it was not fixed. Every reload fails
  the check on Ana's snapshot, and its record waits for an app action. §15
  requests no newer snapshot in a store with no further writes, since
  Ana's covers every write (`requestsSnapshot`). The reload never ends.
- Rust records a landing only while it is late or not fixed
  (`write_landing::land`, `landingRecord`). Once Ben's admission is fixed,
  and for Carol, who never received the write, there is nothing to compare:
  both load Ana's snapshot and drop write 1, while a device that received
  it keeps its effects. §10's rebuild "including contributions already in
  snapshots" does not happen.

§15 now prepares a snapshot only once every covered write is ready
(`Preparation.ready`, Rust `write_landing::unsettled`). A write is ready when
a final kept retirement excludes it, or when the author received every entry
stored by thirty days before it and each received retirement of its device
stored then is final. A `Preparation` is what the author knows: the
retirements it received, kept or dropped, its finality horizon, and the end
of its complete store-log receipt.

- `ready_agrees`: another replay judges a ready write as the author did, if
  it agrees on every retirement stored before the author's horizon and keeps
  no retirement stored by the author's receipt end that the author did not
  receive. The final replay is one (§9's horizon stability, complete
  receipt).
- `prepared_final`: so a gated snapshot carries each write's final verdict.
- `prepared_never_refused`: a loader that holds the entries stored before
  the author's horizon judges every covered write as the author did, so the
  reload's check passes and `choose`'s snapshot loads. Loading requires the
  snapshot's store-log view first (Rust `SnapshotError::StoreLog`), which
  contains those entries. The check stays as a corruption check.
- `waiting_names`, `ready_of_final`: preparation waits only for a named
  received retirement of the write's device that is not final, or for
  receipt short of thirty days before the write. Further writes never hold
  it.
- `every_history_prepares`: a check completed after every entry's thirty
  days is quiet, so the horizon passes every retirement and each write is
  ready.
- `gated_snapshot_waits_then_loads`, on `VerdictHistory`: whatever checks
  Ana completed by day 51, Ben's demotion of Ana (late, stored on day 40)
  keeps the horizon at or before Ben's removal, so his write is not ready
  (`removal_not_final_by_preparation`, `waits_on_day_51`). On day 91 the
  check is quiet, the horizon passes the removal, her replay drops it
  (`removal_final_by_settled`, `settled_drops_removal`, `ready_on_day_91`),
  and her snapshot carries the write admitted. Ben, who recorded his
  unfixed admission, and Carol, who recorded nothing, both load it.

Readings: the kept retirements at preparation and at the end come from
`Finality.resolve`, the horizon from `Horizon.horizon`. The snapshot's
carried verdicts, the device's recorded ones and the reload's record are
inputs restating Rust, not derived from a database model; so are the gate's
agreement and receipt premises, which `Horizon.horizon_stability` and a
complete catch-up supply. Rust gates this device's own writes, which have no
recorded publication time, on every retirement of its device being final;
that each such write a counting snapshot covers is published within thirty
days of the catch-up's receipt end is a hand argument from §15's one-day
counting deadline, not modeled. A raise snapshot is gated too: it seals the
migration's capture only when nothing has applied since it was taken, so its
replay then keeps the capture's retirements and the gate on the current state
gives the capture's verdicts their final values (`capture_prepared_final`);
otherwise it captures again. Reusing a capture taken before a verdict flipped
would carry the old verdict though the gate passes
(`stale_capture_counterexample`). The model reads the queue package's
`Retired.lean` only through its `late` input: rows and losses under a
verdict are that package's (`window_rows_and_losses_converge`).

What the safe rule costs. A body refusal, or a key this device does not
accept, never holds retention (`retention_ignores_bodies`). A refused prefix
holds every write of its audience (`refused_prefix_holds`) until a newer
version verifies it or the listing stops naming it; then the rule is T149's
`retentionCoversAll` (`holding_without_refusals`). A prefix no version can
verify, such as damaged bytes, stays listed until its author deletes it, and
§15 lets an author delete its own snapshot only once a newer one covers
everything it covers, which no reader can check against an unverifiable
prefix. `holding_judges_by_newest` checks the rule on the store: with Ben's
body refused it keeps write 4 and deletes write 3; with his prefix refused
it keeps both.

Readings: the current newest counting snapshot, in §15's deletion rule and
its missing-write rule, is chosen among verified prefixes whatever their
bodies (§19.1: a damaged snapshot's verified prefix still establishes
required history), and the reload waits on it while it is the current
newest (§19.1, `ReloadWait`). A version's verdict on
a prefix is the same on every device running it. The listing is fixed, as in
`waiting_retention_safe`; new arrivals and snapshot deletion between the two
versions are outside the model, as are the other deletion gates, the
registration wait, reset and raise boundaries, and a later version stricter
than an earlier one.

The Rust comparison `data_differential_pending_records`
([`pending_tests.rs`](../../crates/coven-sync/src/pending_tests.rs)) mixes
the four steps in random histories against the real pending table. A cover
calls the database's covered-write clearing after installing positions. A
positions listing calls `replace_pending_subjects`, the replacement the
positions scan commits, with the subjects that scan decides: every peer
positions record held plus every listed peer. A snapshot listing calls
`replace_local_pending` with the snapshot catalog's audience test; as the
catalog does, it first reads this device's refusals back from `pending()`
and keeps each one `pending_can_retry` refuses for a listed snapshot. A
store-log scan calls `replace_local_pending` with `replace_entry_waits`'s
entry-wait test. The two class tests are restated in the test, not called
from the production code. The device's own positions record is in the
histories and must survive passes. After each step, Rust's `pending()` must
equal `Local.step` followed by `Pending.pending`, retry class included. The
code that builds listings and scans from storage (the positions pass,
snapshot catalog and store-log scan) is not driven by this comparison, and
`PendingScans`' catalog, loading and retention models and `RefusedRetention`'s
retention have no Rust comparison.

The Rust comparison `data_differential_reload_choice`
([`snapshot_reload_tests.rs`](../../crates/coven-sync/src/snapshot_reload_tests.rs))
drives the real snapshot catalog and `Catalog::take_current` against
`ReloadWait.choose`. Each of its cases lists store snapshots by two devices
with random signed positions, in memory storage. Some prefixes are signed
by another member, so this version refuses them; some bodies carry this
device's recorded refusal; some are sealed under a key the log does not
accept; some prefixes are in a newer format. Every prefix's signed
store-log view registers both devices, since a snapshot's signer is found
in that view (D9). Rust's choice (the catalog's
required update, or what `take_current` takes or waits for) must equal
Lean's, and for a wait the reason the real reload operation records must
equal `Choice.reason`. Bodies are never opened, so a load is compared only
on the snapshot chosen; the schema wait, found when a body opens, and
reset and raise boundaries are not in the comparison. Neither is the wait on
a refused write (`attempt`): the reload reads its writes only after a
selected body loads.

**Loss metadata is proved.** `LostValues.pending_exact` contains exactly the
non-final entries in a loss's dependency expression, without duplicates.
The ids are sorted. `pending_not_in_fingerprint` excludes this local metadata
from the snapshot/fingerprint values. `final_loss_stable` proves that the
presence of a loss cannot change when all its dependencies have stable fate.
A checked example changes only finality and delivers the changed pending list
through the same query used by the one-shot read, as
[E4](../api.md#e4-reading) requires. The dependency expressions must describe
all causes affecting the loss; their extraction from full replay is not proved.

**Retention safety is connected to storage-time finality.**
`StorageFinality` reuses C10's `ReplayPrefix.settle_prefix` and `range_split`
with the shared `ReplayPolicy` conflict relation. It uses §9's strict old prefix and
inclusive recent window; tied storage times stay together. Time rejection
checks the complete stored history, including dropped entries and entries
outside an author's past, before computing author views.

`StorageFinality.stability` proves that every received set containing the old
prefix keeps its certified decisions. `retention_preserves_replay` connects
that result to stored inputs: an original needed by any such later replay
remains stored. `progress` proves that more than a window without a late
landing advances finality, without device acknowledgements.
The certificate checks that the complete prefix through the observation time
has arrived; a missing entry blocks it. For §9, that endpoint is
`T′ = T - M - δ`, with one common `M = one hour` and positive provider
unit δ. Underflow certifies no final entry. The
[IO listing proof](io/CovenIO/ListingFinality.lean) supplies the complete
prefix under §4's strict listing guarantee. Its `retained_inputs_bound`
applies this package's bound with dependency ages
`stored + 30 days + M + δ < T`; the retained-input bound is unchanged.
Provider discovery remains an input to this package.
Saved qualifying windows preserve previous finality when a later race starts.
The specification represents their union by one monotonic horizon; this package
still models saved certificates.
The storelog package's C10 horizon section proves the two representations agree. `finality_persists` and
`established_stable` prove both the saved status and its replay meaning persist.
A condition can mention alternatives and joint causes; finality must cover all
of them. `StorageFinality.bounded_storage` proves that, once dependencies have
aged into that window and coverage and other retention checks pass, only
currently required inputs remain.
`retained_count_bound` counts those inputs. This is a bound relative to required
data, not a constant bound on an ever-growing store. Frozen losses and retained
replay inputs both protect file references; unreadable data prevents deletion.
Storage age can satisfy the posted-position alternative without an absent
reader's acknowledgement.
The Rust comparison `data_differential_retention_record` runs coven's
retention decision on every combination of dependency finality, reader
posts and storage age, and compares its record and deletion with
`RetentionSafety.work` and `mayDelete`.

Which entries a deletion depends on is modeled in
`RetentionDependencies.lean`. A device's state (absent, active, replaced or
removed) is replayed from the kept entries that register it (store creation
or an addition), replace it, remove it, or remove the member some entry
registered it for, in the order coven's replay applies them. A covered log
depends on the entries deciding its uploader's state when this device
deletes another device's objects and, short of thirty storage days, on the
entries deciding the state of every named device that is neither an active
reader nor posted past it. Its coverage already holds under every boundary
replay could still select (§15). A superseded own snapshot depends on every
reset and raise of its audience, which this file does not model.

- `state_determined`: two replays that agree on the entries concerning a
  device give it one state.
- `later_state`, `later_active`, `later_removed`: a later replay, with more
  entries interleaved by timestamp and none registering a device the
  received entries name, can only retire a device further.
- `deletion_survives_later_replay`: once a log is deleted, no later replay in
  which every non-final entry may change its fate makes its uploader anything
  but removed, or restores a named reader that has not posted past it, short
  of thirty storage days. No deletion depends on an entry that can still
  drop.
- `files_survive_later_replay`: the same for releasing another uploader's
  files.
- `waits_only_on_finality_and_posts` and `eligible_eventually_deleted`: the
  decision waits on nothing else. With no further arrivals, finality that
  only grows, and storage age that eventually passes thirty days, a log that
  is covered and owned once every received entry is final is deleted at some
  evaluation, whether or not every reader posts.
- `creation_device_counterexample`: a rule that learns devices only from
  additions, replacements and removals misses the creation device. Ben
  removes Ana, whose phone created the store and never posted; that rule
  deletes Ben's covered log, and Ana's earlier concurrent removal of Ben then
  drops his removal of her, leaving her phone an active reader that needs it.
  coven waits for those entries to be final; the Rust test
  `the_creation_devices_member_removal_holds_deletion_until_final` replays it.

Outside the model: a device registered only by an entry this device has not
received starts from a snapshot, which the coverage check covers (§12.1); a
device registers itself once (§10), so a later arrival never registers a
named device again; whether this device is entitled to another member's
objects (§13) decides who deletes, not whether anyone needs them, and is an
input. The Rust comparison `data_differential_retention_dependencies`
replays four histories (Ben removing the creator, concurrent removals, a
replacement and a device removal, a registration a concurrent removal
defeats) under finality patterns, local devices, uploaders, posts, storage
age and entitlement, and compares coven's dependencies, record, deletion
and file release with `Deletion.dependencies`, `record`, `mayDelete` and
`filesReleased`.

The model supplies complete storage metadata as an input; numbered discovery and
authentication are outside the proof. Dependency expressions must name every
entry that can affect an input. Their extraction from arbitrary SQL is outside
the model. The theorem applies to the key-free replay, including
rotations and circles hidden only after replay.

## Entry fate, convergence, and the retained histories

`Model`, `Operations`, `Delivery`, the non-key effects in `EntryEffects`,
`Snapshots` and the earlier retention predicates retain historical executable
behavior. `Keys` and the retained delivery and unavailable-key witnesses use
separate rotations; removals introduce no keys anywhere in this package.
Their counterexamples are
historical witnesses, not assertions that the current spec still requires that
behavior. Their replay uses the historical conflict list. `KeySelection` and
`HistoricalKeys` carry the received-authorization and
current pass rules.

`EntryFate` retains originals and computes current effects from replay. Its
`effects_ignore_dropped`, `views_converge`, and `rebuild_converges` prove the
corresponding conditional results for the historical replay and valid readable
merge inputs. `CurrentReplay`, `ReplayEffects`, `CurrentData` and `StorageFinality` carry the
updated results above. Equal encrypted objects alone do not imply readable inputs when historical keys are missing.

The current spec resolves or narrows the historical witnesses as follows:

- **Circle deletion and rows returning:** §14.7 publishes only an entry.
  `CurrentExamples.entry_only_deletion_restores` checks the row and loss under
  the changed conflicts; `EntryFate.Tests.entry_only_restores` checks retained
  file sources and local children. The preliminary ordinary delete in
  `CircleExamples` is no longer specified.
- **Skipped circle parts:** §14.4 requires reloading skipped history even when
  deletion returns. `CurrentExamples.passed_position_does_not_lose_part`
  checks the replacement behavior; `DeliveryExamples` retains the earlier
  failed history as a historical witness.
- **File sources, local cascades and pre-migration data:** §9, §8.4 and §16.5
  require retaining inputs until final. `EntryFate` checks retained resources;
  `StorageFinality` connects the cleanup rule to replay. It does not execute
  SQL cascades or arbitrary migrations.
- **Frozen file references:** `RetentionSafety.loss_protects_file` rules out
  the predicate error checked by `RetentionExamples.frozen_file_counterexample`.
- **Deleted objects' references:** deleting a retained object makes the files
  it names candidates (`ReferenceCandidates`). Another uploader's object is
  read before it goes (`read_before_delete`), and every file a deleted object
  names is a candidate once it goes whenever its parts open here, whether this
  device had checked it, found it unproven, or never read it
  (`names_become_candidates`). `unread_deletion_counterexample` is the rule
  before T210, which deleted without reading; `stale_unproven_counterexample`
  is the rule that reused an unproven verdict after the missing part opened,
  which Rust's `a_stale_unproven_write_is_read_again_before_it_goes` replays.
  An object whose parts still do not open stays unproven, and §16.5 leaves
  the files it names in storage.
  The Rust comparison `data_differential_file_deletion` runs retention on a
  file named by each kind of retained reference in turn, by none, and behind
  an unreadable snapshot, and compares its deletions with `fileDeletable`.
- **Pruning own snapshots:** retention derives which of this device's
  snapshots go from the listed prefixes and its unfinished publications; a
  finished publication deletes nothing itself (§18.1). The Rust comparison
  `data_differential_snapshot_pruning` publishes a lone snapshot, a
  superseded one, one whose publication is still unfinished, and one another
  device's newest covers or leaves out, then compares what retention keeps
  with `Retention.pruneSnapshots` after `preferredSnapshot`.
- **Snapshot pins returning:** retention must retain every still-possible
  boundary, not just currently selected ones. The dependency theorem covers
  this provided its conditions name all such entries. Automatic discovery of
  every snapshot dependency is outside the model.
- **Snapshots under historical keys:** §9 and §11 retain authorized introduced
  keys for reading, independently of later membership replay.
  `HistoricalExamples.historical_snapshot_loads` checks the rotation history;
  `HistoricalKeys.snapshots_stay_loadable` proves the general key condition.
- **Membership reversals and historical disclosures:** the sealed-copy observation exposes
  earlier disclosures even to devices that never saw the removal
  drop. Copies published after that writer's miss remain the residual window.
  Different custody or observation times can still produce different key choices.
- **Provider revocation and stale requests:** account sharing follows replay
  both ways; serialized requests cover the single-device ordering failure.
  Actual grants during a pending request and irreversible S3 key deletion
  are explicit §4/§13 exceptions, not reversible database effects.
- **Reset-cleared reports:** §19.1 makes suppression reversible until final.
  The model's reset-reversal theorem proves restoration from retained observations.
- **Callbacks and copying a lost row:** E4 promises current query results,
  not matching callback histories. Copying to a different key can duplicate a
  row; E4 now says so. A pending loss can disappear on replay.
- **Losing migration values:** §3 and §17.1 expressly replace these with the
  winning migration's output. `MigrationExamples` remains a checked example
  of this exception, not a counterexample to current durability wording.
- **Stopped members/devices and unavailable historical keys:** §10 expressly
  stops an installation for good. `CurrentData.stopped_forever` checks that gate;
  the legacy `observeEntries` alternative is not the current spec. §11 expressly
  allows required old objects to remain pending when no reachable device can
  supply their key. `KeyExamples.readability_counterexample` checks that case.
- **Registration, private credentials and old secrets:** stopping does not
  erase previously installed data or disclosed secrets. Replay derives public
  access records; it cannot reconstruct provider-issued secret credentials.

## Readings and limits

**§3 coverage:** this model proves conditional data convergence, revocation's
key-copy condition, retention safety/bounds and visibility of modeled pending
subjects, under the inputs listed above. The unreadability interpretation of
revocation and circle access assumes correct cryptography: receipts supply
authentication, sealing and opening, not proofs of ciphers, signatures or key
derivation. §3's provider/outsider confidentiality and forgery resistance are
assumed, as are atomic SQLite transactions and §2's provider behavior; the model
does not establish these properties for the implementation.

“Usable” combines device custody with complete copy observations and current
replay. Each writer's miss bounds that observation; storage can still
change afterward. Historical-key sharing follows the sharing device's current
audience. §3 excludes a departing author's own pre-removal writes; this model's
revocation theorem covers current members' first sends; the witnesses include
Carol's new circle data and Ben's new store data.

Physical cleanup requires all deciding entries to be final and all other
checks to pass. Finality is not inferred from completing an operation or from
an object's age alone. The conditional retention proof quantifies over every
future kept set agreeing on certified entries. It assumes the dependency
expression is complete; it does not infer that expression from arbitrary SQL.

For §9's phrase “requires to exist”, member and device targets remain distinct.
Circle-member changes require their target member; making and explicitly
deleting a circle require their author. The latter follows §14.7's example in
which removing Ben defeats Ben's deletion. Circle-member removal does not
require its author's continued membership, matching §14.6's concurrent
removals example. Historical authority is still checked in the recorded past.

The pending list describes current work. Reset suppression retains its inputs;
new observations can record still-unmet conditions. One-shot reads and
subscriptions share a query; callback histories are outside state equality.
Error categories are modeled, while provider-specific payloads and query
scheduling are not.

Receipts abstract completed sealing, opening and publication. Introductions
carry the shared D6 key commitments. This package does not execute their
SHA-256 check, D10's writer-specific copy paths, fresh-id generation or
actual ciphertext validation; the store-log package separately proves its
parameterized hash and opened-copy checks. Rotation failure receipts
do not grant a usable key; incomplete physical publications and their retries
are outside this pass model. Other first-send gates, complete write causality
and whole-object signature validation are inputs to the modeled key/data boundary.

Encryption, signatures, real provider behavior, SQL execution, crashes, and
general Rust/Lean equivalence remain outside the model. `StorelogDataRunner`
uses the shared checked JSON histories, `ReplayEffects.applyKept`, and
`Snapshots.boundaryEntries`/`selectedSnapshot`. The Rust boundary differential
compares repeated no-ops, concurrent raises and resets, arrival orders and batch
partitions with those functions. SQL tests separately cover retained coverage,
selection reversal, restart, and failed replacement rollback. Checked entries
own one `_coven_boundaries` row each; kept entries determine selection, while
coverage survives a dropped verdict.

The entry-effects differential (`StorelogDataEffects`, Rust
`entry_inputs_tests.rs`) drives the real database through checked
entry fates: a reset and a raise are loaded, then each is dropped and kept again
in both write arrival orders. After every fate it compares each row's
generation, setters and losses, the writes a boundary rejects and the visible
pending records with `EntryFate.rebuild` over `ReplayEffects.applyKept`,
`Pending.visible` and `EntryFate.tell`. The device received the original writes
individually; a write that arrived only inside a loaded snapshot cannot be
rebuilt locally, so Rust reloads instead (an SQL test). A second comparison
runs `CurrentData.observe` and `circleLossCause` on an entry-only circle
deletion and its reversal, with the row's write arriving before or after the
deletion; a local child the app made while it saw the row is compared with
`EntryFate.View.children`, so it returns with the row instead of staying
cascaded. SQL tests cover the actual SQLite actions: cascade and set-null
chains through local tables, a child waiting for its second hidden parent,
explicit local edits made while hidden, repeated replay and release once the
removal is final. They also cover the rest: a failed rebuild leaves the previous replay
and data committed, readers never observe a replay without its data, an
unavailable input keeps the old rows and refuses writes, and inputs and reset
suppressions are released only once every boundary entry of the audience is
strictly before the horizon.

## Returning circles

**The narrower reload rule in [§14.4](../coven.md#144-writes) is proved safe.**
`CurrentData.createdBefore` examines the device's applied entries before the
batch, including dropped entries, and excludes creations permanently rejected
by §9's storage-time rule. `needsReload` requires this evidence when a circle
becomes readable. A creation need never have been kept by this receiver: a
write author's recorded view can keep it while the receiver's view drops it.

`CircleCreation.readable_has_kept_creation` follows the actual kept effects
from the empty state, through `ReplayEffects.entry_fate`, and proves that a
readable circle has a kept creation. It accounts for replay's removal of empty
circles. `kept_not_too_late` proves that every kept entry passes permanent time
admission in every replay, including a write's recorded view.

`CurrentData.passed_part_has_creation` applies those results to a write's
author-in-circle check and §7.1's prerequisite that the receiver has applied
every entry in that write's recorded past. Thus every already passed authorized
circle part implies `createdBefore`, regardless of whether the receiver opened
or skipped it. `passed_part_return_reloads` proves that such a part forces a
reload when the circle becomes readable.
`no_reload_no_skipped_part` proves that if this newly readable circle needs no
reload, no authorized part could have passed before the batch. This covers
both a creation first received in the batch and an earlier creation that landed
too late. No uniqueness assumption about circle creation ids is needed.

`CircleReturnExamples` checks a dropped creation returning, creation and
membership arriving together, and a creation landing at the inclusive deadline
or one tick after it. A later creation of the same id checks that an earlier
time-rejected creation cannot spuriously trigger a reload. The reader examples
check that a fresh circle becomes writable without a reload, while an existing
reload wait still blocks writes. The original deletion, convergence, atomic
reload, failed-reload and stale-reload theorems remain checked.

The result is about the newly readable transition. It assumes that passed
writes satisfied their recorded-past and author-in-circle checks; it derives
their creation dependency rather than assuming it. Authentication, complete
write discovery, storage retrieval, SQL transactions and crashes remain outside
this proof. Required unreadable parts still wait for their key, as modeled by
`HistoricalKeys`. Schema/reset reload triggers remain unchanged and independent
of `createdBefore`; their broader model approximation is outside this refinement.
No counterexample to the narrower rule was found; the existing historical
counterexamples remain in the package.

The circle-return differential (Rust
[`data_differential_circle_returns`](../../crates/coven-sync/src/snapshot_reload_tests.rs))
replays checked histories in which a member leaves and rejoins, a deletion
drops, a creation drops and returns, a creation lands at or after its
deadline, and a rejoined member leaves again or the circle is deleted before
any reload commits, in two arrival orders and three batch widths. After each
batch `StorelogDataRunner` reports exactly the member/circle pairs for which
`CurrentData.needsReload` holds. Rust's `StoreLog::returning_circles` must name
the same pairs, with no extra flag or filtering. The histories contain no
raise or reset: `needsReload` also
holds for every circle member whenever any audience's version or reset
changes, while Rust rebuilds a changed audience from its retained inputs and
reloads it only when they are incomplete. With no reload committing, the
runner also folds `CurrentData.circleReplay` over the batches and reports the
pairs still reloading. Rust must name exactly those: the database records each
returning circle and, in the same replay transaction, removes the requirement
and its pending record for every circle `StoreLogState::readable_circles` no
longer names, which the model marks `outside`. The fold restates that database
rule over the pure replay states; the SQL path is a Rust test
(`a_rejoined_member_reloads_the_part_its_passed_position_skipped`).
`scripts/check.sh` runs all three differentials, rebuilds every selected module and checks `Axioms.lean`;
the audit permits only Lean's own `propext`, `Classical.choice`, and
`Quot.sound`.

## An original's record

**The stated rule is proved safe and bounded, and Rust follows it.**
[`LocalSources`](storelog-data/CovenStorelogData/LocalSources.lean) models
this device's record of a user-provided original (`_coven_user_files`) as a
`localSource` retention input. Its row is live, hidden by a circle deletion
entry, deleted by a write, or physically removed after its deletion became
final (§14.7). A deletion entry can drop and a deleting write can still be
excluded by a retirement window (§10), so both are dependencies that must be
final before the record goes.

The stated rule needs the record while the row is current or could become
current again: always for a live row, while the deletion is not kept for a
hidden row, while the deleting write is not kept for a deleted row, and never
once the row is physically removed. `source_kept_while_row_returns` proves
that no record is released while its row could come back, whatever a later
replay or rejudging keeps of the ids not yet final.
`hidden_kept_until_final` and `deleted_kept_until_final` are the two cases.
`hidden_released_when_final` and `deleted_released_when_final` prove the
record goes once the deciding entry or write is kept and final, and that the
hidden row may then be physically removed; `sources_bounded` says that once
every dependency is final exactly the records of current rows remain.

Rust (`file_write_sources`, T214) follows the stated rule. A row a kept and
final circle deletion hides is cleaned up on this device: it names no
original any more, so its record goes in the transaction that makes the
deletion final. A deleted row keeps its key's records while the deleting
write's window verdict can change, which is while a received write keeps its
landing record; that record goes once the verdict is final (§10), and the
original's record goes with it. This device's own deletes are never rejudged
on it, so they release the record at once. Before T214 a hidden row's record
outlived the deletion's finality and a received delete released it before its
verdict was final.

`StorelogDataRunner`'s `sources` input runs the stated rule over a row's
events.
[`data_differential_local_sources`](../../crates/coven-database/src/file_write_sources_tests.rs)
drives a real database through circle deletion, its drop, finality and a
received row delete, published before or after the horizon's settling
window, and checks the record after every event against the stated rule.
What cleaning up a hidden row removes besides its original's record is
outside the model: Rust keeps its merge row and loss record, which later
writes into the circle still merge into (§14.7). The cost of keeping them is
not modeled.

## Rust differential coverage

[`AccessRunner.lean`](storelog-data/AccessRunner.lean) calls `Access.adopt`,
`retryFailed`, `start`, `complete`, `work` and `Pending.records`. The Rust
[`lean_differential_access_pending`](../../crates/coven-sync/src/operation_pending_tests.rs)
test publishes plain removals and runs the account journal, provider adapter and database pending
query against controlled provider replies. It compares pending operation and
reporter identities, account, retained-access reason and actual provider access
after each completed attempt: immediate success, one to three retained-grant
failures, repeated explicit retries, and eventual success. Both locally requested
and remotely received removals run. Fresh Rust operation ids map to the case's
Lean natural identity; Rust assertions check that the real identity stays fixed.
Provider-specific retained-share payloads remain Rust assertions.

`scripts/check.sh` builds `accessRunner` and supplies `COVEN_ACCESS_LEAN` when
the package or `coven-sync` changes. It also runs rejection cases for malformed
replies and retries after successful completion, and checks that the selected tests exist
before selecting them.

[`lean_differential_access_phases`](../../crates/coven-sync/src/operation_access_tests.rs)
compares ready, in-flight and failed requests, their fixed target, intended and
actual access, and pending reasons after every event. Its histories cover grants,
revokes, cancellation before sending, intention reversals while sending, retained
grants and explicit retries. Rust reopens its database after every event and uses
the production entry replay and provider request steps. Owner waits, shared
accounts, open invites, atomic rollback and held provider replies have Rust tests.
Lost replies, incomplete revocations and invalidated invitation tokens also have
Rust regression coverage. Re-grant after inconclusive failure is outside this
comparison: the model's Boolean actual grant does not represent unknown confirmation.

S3 deletion notices are compared by
[`lean_differential_access_keys`](../../crates/coven-sync/src/access_keys_tests.rs),
which `scripts/check.sh` runs with `COVEN_ACCESS_KEY_LEAN` naming
`accessKeyRunner`.

## Member-key replacement

`MemberKeySelection`, `MemberKeyPhases` and `MemberKeyExamples` extend the
existing `KeySelection` and `KeyPhases` results for the
[stable-member proposal](storelog.md#c13-stable-members-and-replaceable-keys--checked-proposal).
They use `CurrentReplay.Keyed` directly; the unkeyed interfaces and their
Rust comparisons remain available. The normative rules and layouts are in
`spec/coven.md` and `spec/format.md`.

[`lean_differential_member_keys`](../../crates/coven-sync/src/entry_signature_tests.rs)
runs `memberKeyRunner` (`COVEN_MEMBER_KEY_LEAN`) on real sealed entries and key
copies decoded by the production codec. It compares `Keyed.signatureCurrent`
in each entry's recorded view, kept and dropped entries, every installed pair
and active role from `Keyed.resolve`, `currentCredential` for each copy's
member and sealing key, and `MemberKeys.selectInReplay` with Rust's first-attempt
selection. Leaves signed with a key their author's view never bound are Rust
signature refusals and model drops; a self-registration by an id no entry
added is dropped by both. Its honest prefix also re-adds active members,
repeating them or changing their keys, role or access, returns removed
members with their old pair or a fresh one, and adds new ids borrowing an
added member's pair or one of its keys. Every bound pair is compared with
`Keyed`'s history. Two concurrent leaves add one new id with equal or
differing payloads
([`Keyed.Additions`](storelog.md#member-additions)).

[`lean_differential_member_key_replacements`](../../crates/coven-sync/src/member_key_differential_tests.rs)
runs the same runner on histories with replacements. After a linear prefix,
concurrent leaves have Ben replace his own keys and Ana and Carol replace
his, with fresh, identical or reused pairs, or Ana remove him or replace
Carol's keys; Ben's follow-up reads some of them and replaces his keys again
or signs with his current or replaced key. Stored times tie or meet or miss
the 30-day landing deadline, and some cases omit one leaf and what read it.
Rust receives the real sealed entries in two arrival orders through the
production batch. The runner's optional `received`, `writes` and `horizon`
fields add `Keyed.authorized` per entry, every conflicting pair with
`prefer` both ways, and per write `OldKeyObjects.authorized`, `timely`,
`countInReplay` and `writerCurrent`, with finality by
[`KeyedHorizon`](storelog.md#c13-stable-members-and-replaceable-keys--checked-proposal).
The test compares them with signature refusals, kept and dropped entries,
current and bound pairs, each `StoreLogCheck`'s authority, Rust's
`conflict`/`before`, and, for writes from Ben's device, the production
recorded-view authority, the database window verdict and its finality
against an applied store log with that horizon, and the sending check.

A recipient contains a stable member id and the exact sealing public key.
An old copy and a new copy can address the same member while disclosing their
keys to different credentials. `retired` derives exposed sealing keys from
kept replacements and each replacement's recorded view. A member need not
leave the store or a circle for their old credential to be exposed.
`kept_replacement_retires` connects that evidence to actual replay; neither
the stated reason for replacement nor a device's local memory enters it.

`route_current` proves that routing uses the member's current sealing key and
refuses a retired key. `no_new_old_copy` proves that every successful new
historical-share reservation appends only such a recipient. It uses the
existing sharing check for audience membership and custody. `replacement_sharing`
derives both the current binding and the retired keys from the same replay
inside `shareInReplay`; its new destination differs from the old sealing key.
`historical_copy_retained` proves that new sharing preserves every old copy.
This is a statement about newly sealed copies. Already reserved ciphertext
still follows the existing fixed-byte retry rule and may publish later.

The selector composes `KeySelection.select` with rejection of keys having an
observed copy for a retired sealing key or a credential that is not current
for its named member. A prerequisite copy for a losing replacement cannot
borrow that member's continuing authority. It retains the existing custody,
audience, received-introduction authority and newest-key ordering checks.
`selection_avoids_old_key` proves that a selected key has no observed copy to
any retired sealing key; `replacement_selection` obtains that retirement from
a kept replacement in the actual replay. This holds for every audience, not
only the store. A key that the old credential can open through the complete
copy observation cannot be the selected replacement.
`selection_uses_current_bindings` proves that every observed recipient of a
selected key uses that member's current credential.

`KeyPhases.MemberKeys.begin` requires completed membership and copy
observations. A failure returns its pending reason, not a usable pass.
`rotate` verifies the candidate in the caught-up replay, uses the current
bindings for every audience member, and requires publication of the complete
copy set followed by the entry. `rotation_copies_safe` proves that every
constructed rotation copy avoids retired sealing keys;
`rotation_requires_publication` proves that both receipts are required.
`first_send_after_replacement` connects selection to the actual first-send
path after the replacement has been received and kept.

The checked example creates a store and Gifts for one member, replaces that
member's keys, and keeps their membership. Both old audience keys become
unusable for first sends (`replacement_retires_both_audiences`). Historical
sharing targets the new sealing key, but the old copy still exposes the key
(`historical_sharing_uses_new_binding`, `resharing_does_not_unexpose`). The
member rotates both audiences; both rotations publish copies to the new
credential and subsequent selection uses the new audience keys
(`both_rotations_publish_current_copies`, `rotation_selection_avoids_old_keys`).
Either publication failure blocks the result (`failed_rotation_cannot_send`).
A later observed old-credential copy of even the new rotation retires it
(`later_old_copy_exposes_rotation`). `checked_metadata` verifies the example's
causal and online storage history.
`unbound_copy_exposes_rotation` checks the rejection of a copy for a candidate
credential that never became current, even though its named member is active.

`OldWriteWithRotation` connects window-only old-write admission to first-send
selection. Creation on day 0, replacement on day 1 and store-key rotation on
day 2 leave the finality window [3, 33] empty. The old-key write lands late on
day 32 and stays dropped when a current-key write arrives on day 34.
`reader_uses_fresh_rotation` checks publication and selection;
`final_replacement_keeps_late_write_dropped` checks admission and the current
signing-key send gate against the same replay. The old signer is refused and
the current signer may send; neither audience rotation nor another write
changes the old write's deadline.

An otherwise admitted change built on that dropped write becomes a recorded
loss per change under the shared
[`DataDependency.dependsOnDropped` rule](merge.md#b17-receive-order-progress-and-dropping-by-data)
in the merge model. Actual generation sources and replaced cell setters
determine dependence, not recorded reads. Other changes in the same write
remain eligible. Old-key admission supplies a base verdict to that rule;
these member-key examples do not re-prove its data accounting or convergence.

These secrecy statements require all relevant disclosed audience keys to have
copy evidence in the completed observation. They do not prove cryptography,
provider discovery, absence of copies published after the observation, private
key destruction, or protection of already exposed plaintext. A received old
copy can retire a key even if its ciphertext is forged, as in §11's existing
conservative rule. The receipt model does not inspect ciphertext or prove its
hash; it treats the recorded recipient sealing key as the box's routing fact.
Historical reads and exact-byte retries retain their old keys.

The storelog package proves old-key window finality from the stable kept
entry prefix, supplies that prefix from §9's horizon for the keyed replay
([`KeyedHorizon`](storelog.md#c13-stable-members-and-replaceable-keys--checked-proposal)),
and separately proves the send gate's one-day-and-five-minute publication
bound. The general IO discovery theorems have not been transferred to this
proposal: complete catch-up and continued refusal of old credentials remain
explicit inputs. SQL rebuilds and physical retention remain outside these
member-key proofs. The exact spec/format changes, including window-only
admission, per-change losses and credential-bearing copy prefixes and reports,
are listed in [the checklist](storelog.md#c14-spec-and-format-checklist-for-stable-members).
