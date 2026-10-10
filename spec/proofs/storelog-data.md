# Store-log effects on data

This development couples [Appendix B's merge](merge.md) to
[Appendix C's replay](storelog.md), importing both packages by relative path.
It checks the data claims in [§3](../coven.md#3-guarantees), including outcomes
that fail. The failures are executable Lean examples, not assumed properties.

## Results and counterexamples

**Durability fails for migration values.** Two devices share one note with
title “Groceries”. Offline, Ben changes its title to “Shopping”. Both run the
same migration, `UPDATE notes SET body=title`, with an identity converter for
waiting writes. Ana's migration writes body “Groceries”; Ben's writes body
“Shopping”. Their concurrent raises select Ana's earlier snapshot. Reload
preserves Ben's ordinary title edit but discards his migration's body. No write
read that body, no removal rule holds, and no loss records it.
`MigrationExamples.durability_counterexample` and the following Lean `example`
exhibit the missing value and negate `Accounted`. Both the log and writes
satisfy the imported validity conditions. This is one note, one ordinary edit,
two migration writes and two concurrent raises after registration; it needs
neither a crash nor different migration code.

This behavior is specified by [§17.1](../coven.md#171-host-application): a
migration is a write, but its log object has no changes, and a losing migration
changes nothing elsewhere. In Rust,
[`migration_run::finish`](../../crates/coven-database/src/migration_run.rs)
applies the changes locally and clears the marker's parts;
[`snapshot_state::begin`](../../crates/coven-database/src/snapshot_state.rs)
replaces the audience's rows and losses;
[`download::accepted`](../../crates/coven-database/src/download.rs) skips
migration markers. The Rust test
`a_losing_migrations_derived_value_disappears_without_a_loss` executes this
history, including the actual migration SQL, snapshot publication and reload.
The universal durability claim is therefore not a theorem of this model.

**Convergence fails after a deleted circle returns.** Ana has a phone and a
tablet; Ana, Ben and Carol share Gifts. Ben removes Ana from Gifts. Carol writes
one row under Ben's new circle key; Ana's phone skips it and advances its write
position. Ben deletes Gifts without having downloaded Carol's row, so his
deletion authors no row delete. Ana's phone applies the deletion. Ana's tablet,
still at their common prefix, publishes an earlier concurrent removal of Ben
from Gifts. Both Ben entries drop, Gifts returns with Ana and Carol, and Carol
shares Ben's dropped key with Ana. The phone still skips the consumed position;
the tablet reads it. Same entries, write positions, membership and keys, but
different rows, with no loss on the phone. This uses three members, four devices,
three entries after setup and one data write.

[`store_log_sync::apply`](../../crates/coven-sync/src/store_log_sync.rs) schedules
a reload when membership returns only if the previous circle was not deleted.
[`download::apply`](../../crates/coven-database/src/download.rs) skips consumed
positions. `Delivery.lean` couples these tests to key-dependent part application;
`DeliveryExamples.convergence_counterexample`, `same_received_entries` and a
Lean `example` check the history. The Rust test
`a_circle_restored_after_deletion_keeps_a_previously_skipped_part_missing`
uses the actual circle-deletion operation, key sharing and write downloads.

`histories_converge` proves the restricted result for equal *applied* ordinary
write sets: merged rows, app views and losses agree under the merge's validity
conditions. Consumed writes with skipped parts do not meet that assumption.
`ReplayExamples.kept_dropped_rekept` checks a rename kept, dropped by a member
removal, then kept again when an earlier competing removal arrives. The circle's
row is hidden with a loss and then restored. `selected_reload_agrees` proves
replay-selected snapshot replacement independent of entry arrival order;
`snapshot_converges` proves merge-order independence after a valid snapshot.
These are safety results about the same readable, effective data. They do not
prove that equal encrypted objects suffice, or that arbitrary SQL migrations
produce snapshots satisfying the merge invariant.

**The unqualified “its rows return” outcome fails.** Ana and Ben share Gifts
with one note. Ana removes Ben from the circle while Ben, offline, deletes it.
Ben first commits an ordinary delete of that note, then publishes the circle
deletion. Ana's earlier concurrent removal makes the deletion entry drop.
Gifts returns; the note stays deleted. The delete read the insert, so this
particular disappearance satisfies durability. `CircleExamples` checks this
one-note history and [§14.7's](../coven.md#147-deleting-a-circle) complete example:
notes 7 and 8 stay deleted, while concurrent note 9 is hidden with a loss and
returns when the deleted-circle rule stops holding. [§9](../coven.md#9-members-and-roles)
distinguishes these cases. The Rust test
`dropped_circle_deletion_restores_hidden_rows_but_keeps_explicit_deletes`
exercises the distinction.

**Readability fails.** On S3, two admins, Ana and Ben, concurrently remove each
other. Each removal's replacement store key is sealed only to its author. Ben
publishes one ordinary write under his replacement key before hearing from Ana.
Ana's earlier removal wins. Ben's removal drops, but only Ben holds its key.
Ana must read the write and cannot decrypt it. Ben reads his own removal and
stops before redistribution; every subsequent sync stops there too. There is
no active holder to give Ana the key, even with successful storage and unlimited
sync rounds. Two members, two removals and one write suffice after registration.
`KeyExamples.readability_counterexample` proves the key remains unavailable
for every number of redistribution rounds; a Lean `example` checks that the
required part waits. The Rust test
`a_removed_sole_holder_cannot_share_a_dropped_removals_key` reproduces it. Rust
drops the losing removal as `NoAdminLeft`: effect safety precedes conflict
comparison. Lean's reused replay records the same kept/dropped identities.

## Correspondence with the spec and code

`Model.lean` uses the imported `CovenStorelog.step` and `CovenMerge.step`.
An entry reruns replay on the full received set, including entries previously
dropped. Row identity includes table, key and audience. Presence comes from
generation parity. Deleted-circle inputs come from a kept creation and the
absence of the active circle; Rust retains a tombstone where Appendix C erases
the active circle. Other-audience competition uses the generation's introducing
write, with store rows preferred and earlier circle generations winning.
Unique claims are separated by table and audience. The existing three-pass
removal algorithm recomputes both the app view and its active loss records.
These choices follow [§8](../coven.md#8-merge), [§14](../coven.md#14-audiences),
[`store_log::apply`](../../crates/coven-database/src/store_log.rs) and
[`removal::judge_groups`](../../crates/coven-merge/src/removal.rs).

Writes require their causal predecessors and recorded entry past. Authority is
the registered device and active member in that past, as in
[§7.1](../coven.md#71-causality) and
[`write_object::authority`](../../crates/coven-sync/src/write_object.rs).
Current removal does not retract historical authority. Member/device removal
stops sync; it does not author ordinary deletes. Removing the last circle member
does delete the circle in replay, activating the row rule. The examples check
these differences, including device removal while its member remains active.

`Operations.lean` models the paired operations in
[§18.1](../coven.md#181-operations),
[`operation_calls.rs`](../../crates/coven-sync/src/operation_calls.rs) and
[`operation_steps.rs`](../../crates/coven-sync/src/operation_steps.rs):

- Circle deletion commits a row write and journal atomically, using every
  present generation, including hidden rows
  ([`circle_deletion.rs`](../../crates/coven-database/src/circle_deletion.rs)).
  Uploading that write precedes the entry. A dropped attempt restarts at row
  deletion and checks current permission. It does not undo its prior write;
  an already finished operation has no journal to restart.
- A breaking migration commits its data/schema first, then uploads each
  audience's snapshot before its raise. `Snapshots.lean` selects boundaries
  from replay's kept, effective entries using the imported effect functions,
  replaces that audience's state, and reapplies uncovered writes. Explicitly
  lost and boundary-excluded parts retain a write identity and cause;
  migration markers supply no changes. These follow
  [`snapshot_boundaries.rs`](../../crates/coven-sync/src/snapshot_boundaries.rs),
  [`snapshot_reload.rs`](../../crates/coven-sync/src/snapshot_reload.rs),
  [`snapshot_load.rs`](../../crates/coven-database/src/snapshot_load.rs) and
  [`write_boundary.rs`](../../crates/coven-database/src/write_boundary.rs).
- Reset uploads a snapshot before its entry, without a preliminary row write.
  A dropped reset finishes against the winning reset instead of overriding it.
  The model checks [§19.3's](../coven.md#193-resetting-a-store) predicates for
  covered writes, writes following the snapshot, concurrent writes and writes
  whose causal past the snapshot omitted.
- Creating/renaming a circle, adding/removing members or devices, changing
  roles/access and joining/leaving a circle have no preliminary data write.
  Their replay changes affect authority, keys and removal inputs instead.

`Keys.lean` follows [§11](../coven.md#11-keys),
[§14.4](../coven.md#144-writes) and
[`store_log_keys.rs`](../../crates/coven-sync/src/store_log_keys.rs): initial
seals use the author's pre-entry audience minus the removed member; additions
share historical keys; dropped removals distribute to the latest audience.
[`store_log_sync::step` and `apply`](../../crates/coven-sync/src/store_log_sync.rs)
check removal before key acquisition or distribution. `mustRead` follows
`write_object::key_audience_contains`, including membership at introduction
and later kept additions. A missing required key waits; an outside reader
without a key skips the part. The model optimistically lets every running
device obtain every stored copy sealed to its member in each redistribution
round. The failure therefore does not depend on scheduling or download delays.

## Entry fate and stored effects

`EntryEffects.lean` extends the coupled machine with key custody, local file
sources, local cascading children, provider grants, S3 deletion notices and
installed credentials and reset-cleared stuck reports. `Retention.lean` models
snapshot eligibility, selection,
coverage-based deletion and file-reference scanning. The corresponding
`EntryExamples.lean` and `RetentionExamples.lean` check negative properties;
all named results are included in the axiom audit.

There are two distinct claims. **Entry fate** compares the stored result to
applying an operation only while its entry is kept; a removed value must return
or have a loss record. **Convergence across fate** compares different causal
arrival orders with the same entries, ordinary writes, initial resources and
readable audiences. Physical caches, private credentials and local tables are
not promised to be identical between arbitrary devices. Their counterexamples
compare the same initial local resources, or test entry fate alone. Learning a
secret is represented by possession, not merely its selection as the current
key. A member can retain a secret after an honest client stops syncing.

`derived_effects_converge` proves order independence for every projection of
replay, covering all fourteen entry constructors. Combined with
`histories_converge`, this proves the row/removal/loss result when the same
ordinary writes were actually applied. It does not turn an ordinary delete,
physical erasure or a disclosure into a reversible replay effect.

### Operation correspondence

| Operation | Stored effects and response to entry fate | Formal result and alternative |
| --- | --- | --- |
| Create store / initial setup (§4, §9–§12) | Initial store key, identity, credentials, restore code and location; local setup compensates a failed commit. The unique creation root in `Valid` is never defeated by its descendants. | Root stability is imported from `CovenStorelog.Bootstrap`; setup/provider race and compensation are outside that single-store assumption. Keys must be stored; root identity is the agreed prerequisite within this model. |
| Register / bootstrap a device (§12.1) | `bootstrap::load_new_device` registers before loading snapshots. A concurrent member removal can defeat registration after installation; rows and opened keys remain on the stopped device. A never-admitted device does not install the snapshot. | `registration_fate_counterexample`: Ben removes Ana while her new device registers and loads. Metadata is derived, but installed data is not exactly gated by registration. Replay cannot make an installation forget disclosed data. |
| Remove device (§13) | Device metadata and permission to sync are derived. Removal intentionally retains its historical rows and custody rather than authoring deletes. | Metadata holds; `ReplayExamples.device_removal_authority` checks historical writes. Existing keys are not a newly stored effect of removal. Stopping a device is not cryptographic erasure. |
| Change role / rename circle (§9, §14.3) | No preliminary row write or separate stored data effect. Their current values are replay projections. | Holds through `derived_effects_converge`; `ReplayExamples` includes kept/dropped/re-kept rename. |
| Set access / replace S3 credential (§12.1) | The credential and synced restore code commit before SetAccess. Dropping the entry changes recorded access, not the installed credential; finishing removes the journal. A never-kept entry still follows installation; re-keeping does not reinstall anything. | `credential_fate_counterexample`: two devices replace a member's key concurrently; the later replacement stays installed after the earlier entry wins. Public access selection can be derived, but a provider-issued secret cannot be reconstructed from its public id. This is not a promise that private credentials on different devices match. |
| Invite / approve member (§12.2) | Provider grant or S3 credential exists before approval. Store-key copies precede the add entry. Decline/expiry revokes access, but a finished approval does not become a decline when its entry later drops. Bootstrap checks current membership before installing, which cannot retract already disclosed bytes. | `invitation_key_counterexample`: Ben removes Ana concurrently with Ana approving Carol; removal wins, but Carol's sealed store key remains. Two competing entries suffice. Derive membership, but disclosure and provider actions require an irrevocable admission decision or different security semantics. |
| Remove member (§13, §18.1) | Initial store/circle keys are sealed to the author's remaining audience. A dropped removal redistributes them to the current audience; custody persists. Provider revocation or S3 notices finish independently. Ordinary rows are not deleted, except that losing the last circle member activates its derived deletion rule. | `rekept_key_counterexample`, `access_fate_counterexample`, and the inherited missing-holder/skip counterexamples refute the combined claim. Current key ids and row rules can be derived; key disclosure and provider deletion cannot be undone. No operation completion supplies agreement that the entry cannot lose. |
| Create circle (§14.3) | Its key is sealed to the creator before the entry. A drop removes the circle from replay, not the creator's key or sealed object; a re-kept entry reuses that material. A device that never kept creation need not fetch its key. | `creation_key_counterexample`: Ben removes Ana before her concurrent creation in replay. Ana's key remains. Circle existence is derived; custody intentionally outlives selection and is not exactly gated by entry fate. |
| Add circle member (§14.3) | Every held historical key is sealed before the addition. Copies and acquired keys survive even if the addition was never kept. | `addition_key_counterexample`: Ben removes Ana while Ana adds Carol to Gifts; Carol remains a store member and can acquire the already sealed Gifts key although her addition loses. Deriving membership cannot revoke historical disclosure. |
| Remove circle member (§14.6) | Key replacement has the same sharing behavior as store removal. Last-member removal derives circle deletion, without the explicit delete operation's ordinary row write. | `rekept_circle_key_counterexample` refutes secrecy and custody convergence; the deleted-circle rule itself follows replay. The key id can be derived; previously disclosed key material cannot. |
| Delete circle (§14.7) | An ordinary write deletes every locally present generation, including hidden rows, before the entry. It survives never-kept, dropped and re-kept outcomes. The separate deleted-circle rule follows replay. | `CircleExamples` and the entry-fate example distinguish these effects. One note, a deletion and a competing circle removal suffice. Deriving deletion entirely from circle state would preserve the rows under a loss and restore them when the entry drops; it would remove the preliminary ordinary delete. |
| Breaking migration / raise (§17.1) | SQL schema, local data changes, converted waiting writes and frozen losses commit before snapshots and raises. Migration markers have no changes. Replay selects boundaries and reload replaces audience data; it does not reverse arbitrary SQL or credential/file changes. An unattempted queued write can be converted; attempted bytes remain fixed. | Existing migration publication and reload theorems are conditional on readable snapshots. The losing-migration durability failure is not by itself an entry-fate failure: removing a losing migration's output can be the intended effect. Frozen-file retention below refutes the combined files claim. Deriving data transformations requires retaining their inputs/outputs and selecting by replay; arbitrary SQL/schema erasure cannot be inverted from the remaining state (`no_inverse_of_erasure`). |
| Reset (§19.3) | Snapshot precedes entry; no preliminary row write. Kept effective resets select snapshot replacement and write exclusions. Dropped resets finish against the winning reset, and a later re-kept reset again requires its named prefix. A newly applied reset also deletes locally reported stuck-object judgments; peer reports survive. | Selected rows and exclusions hold under `selected_reload_agrees` and the explicit snapshot assumptions. `reset_judgments_counterexample` clears a local report, then receives an earlier concurrent raise which defeats the reset; the report stays absent. Local files/children can also be destroyed by replacement. These refute the unrestricted local-state claim. Selection and report suppression can be derived; erasure must preserve the inputs of every still-possible selection. |
| Reload snapshot (§15, §18.1) | Selected audiences, merge state, losses and waiting writes are replaced atomically after checking the received entry set. A changed set restarts selection. File facts are retained against the replacement, and SQL materialization can cascade into local tables. | Conditional row/loss convergence holds; local source and cascade counterexamples refute full stored-effects convergence. Keep local resources attached to retained merge identities if they must survive a reversible visibility change. |
| Write snapshot / retention (§15, §16.5) | Snapshot ciphertext is stored independently of replay. Only keys introduced by kept entries make catalog candidates. Covered logs may be deleted after all active devices post, or after 30 days. Older own snapshots are deleted by coverage unless currently pinned. | `rotated_snapshot_counterexample`: a removal is kept, a snapshot under its key covers a write, the covered log is deleted, then an earlier removal beats it. The key still opens writes, but the snapshot is excluded. Re-keeping admits it again. Deriving eligibility from durable key/history requirements would change the kept-only filter; deleting irreplaceable inputs requires a stronger invariant than current entry fate. |
| Attach/upload/delete file (§16) | Queued uploads and ordinary file-row writes are independent of entries. Entry-driven visibility changes and snapshot replacement nevertheless discard local sources; uploaded-file deletion scans retained objects, subject to readability. | `file_fate_counterexample` and `frozen_file_counterexample` below refute the unconditional files result. Derive visibility without forgetting sources, and include loss references in retention. File bytes cannot be reconstructed from replay. |
| Local cascade / triggers (§8.4, §8.7) | Materializing a temporarily hidden synced parent executes SQLite's local cascade; no synced write records the local child. Arbitrary app trigger effects are outside the replicated state. | `local_cascade_counterexample` uses one local child and the same two-entry circle history. A derived local view or retained child could follow replay; an actual SQL delete cannot be undone without preserving its input. |

The operation ordering above comes from
[`operation_calls.rs`](../../crates/coven-sync/src/operation_calls.rs),
[`operation_steps.rs`](../../crates/coven-sync/src/operation_steps.rs),
[`operation_invites.rs`](../../crates/coven-sync/src/operation_invites.rs),
[`storage_setup.rs`](../../crates/coven-sync/src/storage_setup.rs),
[`bootstrap.rs`](../../crates/coven-sync/src/bootstrap.rs),
[`restore_codes.rs`](../../crates/coven-sync/src/restore_codes.rs) and
[`schema_sync.rs`](../../crates/coven-sync/src/schema_sync.rs).
The imported publication model uses these same boundaries; an operation's
finished journal is not a permanent record of an effect's eligibility.

### Stored-effect witnesses

**Removal keys can become current after disclosure.** Ana and Ben are admins;
Carol is a member, and Ana has two devices. Three concurrent entries, in replay
order, demote Ben, let Ben remove Ana, and let Ana remove Carol. Initially Ana's
removal is kept, with its key sealed only to Ana and Ben. Ben's removal arrives,
beats it, and Ben shares the dropped key with Carol. Ana's other device's earlier
demotion arrives last: Ben's removal would leave no admin, so it drops and Ana's
removal returns. Carol is removed, but knows the current key. Delivering the
demotion before Ana's removal never shares that key with Carol. Both final copy
sets are fixed points of redistribution; `undisclosed_forever` proves that any
number of later rounds preserves the difference. Three changing entries are
necessary for kept/dropped/re-kept. The circle variant uses Ben's store removal
and Ana's circle removal with the same demotion. The Rust test
`a_rekept_removal_uses_a_key_disclosed_while_it_was_dropped` exercises actual
publication, downloading, custody and encryption, including both delivery orders.

**Revocation survives its cause.** Ana's phone removes Ben concurrently with
Ben removing Carol; Ana's tablet first sees Ben's entry and revokes Carol. When
Ana's earlier entry arrives, Carol returns to the member list, without provider
access. A tablet seeing Ana's entry first never revokes Carol. The S3 variant
retains an obsolete deletion notice; confirming it permanently removes the
credential and notice, with no inverse when the entry drops. Re-keeping removal
repeats revocation idempotently. The model includes the implementation's check
for another active member using the same provider grant. Two competing entries
and three members suffice; no provider error is assumed.

**A row returns without its local file source.** Ana's tablet has a Gifts file
that Ben has not downloaded. Ben deletes Gifts, authoring no ordinary delete of
that file. Ana's phone concurrently removes Ben; that earlier entry wins when
it arrives. The tablet first hides the file under the deleted-circle rule, then
restores its row. [`file_write::retain_rows`](../../crates/coven-database/src/file_write.rs)
forgets its local source while the circle is deleted, and
[`FileRemovals`](../../crates/coven-database/src/file_removals.rs) deletes owned
bytes after the transaction. The same write and opposite entry order preserve
the source. User-original bytes are not deleted, but their registration is lost.
The witness needs one file and two competing entries. A local child of that row
has the same fate under SQL cascade. Replay-derived row loss does not preserve
these separate resources.

**Frozen losses do not protect uploaded files.** A breaking migration freezes
a hidden file-row loss and removes its merge identity
([`migration_state::carry`](../../crates/coven-database/src/migration_state.rs)).
A snapshot retains that loss. Once other reference-bearing logs and snapshots
are gone, the file is absent from visible/waiting rows and from
[`snapshot_references`](../../crates/coven-database/src/database_file_retention.rs),
which scans snapshot values, not frozen losses. The uploader can delete it under
[`snapshot_file_retention.rs`](../../crates/coven-sync/src/snapshot_file_retention.rs).
The loss still names the file; it cannot restore the bytes. The example checks
this omission with one frozen loss, and separately checks that unreadable
retained objects prevent deletion. This is a file-reference predicate witness;
SQL execution and a full migration-to-retention run are not proved in Lean.

**Current snapshot pins are not a finality proof.**
`snapshot_pin_contract_counterexample` gives a valid replay and candidate-prefix
history where a once-kept raise's snapshot loses its pin, is covered and pruned,
then is needed when its entry returns. It also includes a replacement raise and
compares retention before and after final delivery. This is conditional on the
supplied snapshots being loadable and the republication being reachable. The
model does not prove those schema/scheduler preconditions, so this is not counted
as a confirmed end-to-end operation failure. In particular, an incompatible
reset snapshot can stop reload before retention. The confirmed catalog-key
counterexample above does not require a schema change.

The source predicates are
[`snapshot_catalog.rs`](../../crates/coven-sync/src/snapshot_catalog.rs),
[`snapshot_boundaries.rs`](../../crates/coven-sync/src/snapshot_boundaries.rs) and
[`snapshot_retention.rs`](../../crates/coven-sync/src/snapshot_retention.rs).
`pinned_snapshot_survives` proves the actual local retention guarantee; it does
not assume that an effective boundary stays effective forever. Coverage counts
consumed positions, including excluded or skipped parts, rather than proving
that each original value is recoverable.

None of the losing-entry witnesses reaches a globally agreed decision merely
by finishing a local transaction, uploading prerequisites, completing an
operation or waiting 30 days. Replay-derived views can follow entry fate. Keys,
provider actions and physical erasure need retained information, changed
semantics, or an agreement mechanism before an irreversible effect; this model
does not posit such a mechanism.

## Entry-derived alternative

`EntryFate.lean`, `EntryFateKeys.lean` and `EntryFateLive.lean` evaluate a
different contract from the implemented behavior above. Their sibling
`EntryFate_tests.lean` and `EntryFateKeys_tests.lean` contain the checked
histories. The production Rust and its existing counterexamples still describe
the implemented contract; they are not implementations of this alternative.

The alternative keeps §9's per-device paths and replay, including entries
changing from kept to dropped and back. It assumes online administrative calls
and stamps later than every entry the author read. `Valid.past_lt`,
`Valid.own_past` and the causal-order checks enforce those ordering constraints.
The competing calls in these witnesses can all start online after reading their
common prefix, before either publishes. Online does not mean that a read sees
another author's unfinished call, or that publication creates a global order.
There is no global allocation slot, consensus or provider compare-and-swap.

The choices needed to make the proposed rules precise are:

- Circle deletion authors only the entry. The existing deleted-circle rule
  computes visibility and losses. It does not author an ordinary row delete.
- Original merge data, local file sources, uploaded bytes, local children,
  failed-object judgments, snapshot payloads and pre-migration inputs must be
  retained independently of visibility. The model performs no physical pruning
  of these required originals. Snapshot *eligibility* still follows replay;
  losing eligibility cannot erase the sole representation of a write.
  This gives up the unconditional bounded-storage promise. Reclaiming an input
  would require proving that every future allowed replay remains reconstructible.
- A local child is a view over retained local data. Neither a SQLite cascade
  nor set-null may overwrite its only original when a parent is merely hidden.
  Local source facts, local children and failed-object observations are explicit
  inputs: equal entries and writes alone do not even determine these facts.
  Comparisons below give both histories the same local inputs and readable
  audiences, so none of the failures relies on this ordinary device difference.
- Reset suppresses local stuck judgments while its boundary is effective;
  it does not destroy them. Peer reports remain inputs from signed posts.
- Breaking-change exclusions are rebuilt from original write headers and the
  currently effective boundaries. Schema transformations need retained inputs
  too: treating a frozen, once-derived snapshot as an independent original is
  insufficient. A transform must retain its dependencies and be recomputed when
  they change; it cannot feed its last materialized view back into itself.
- Provider grants are *requested* in both directions. Request completion is a
  separate event from local replay. Both always-successful provider calls and
  arbitrary completion order are allowed; no cross-system transaction is assumed.
- Fresh key ids use the creator's device and local number. A device checks
  delivery information it has observed before reusing its current key, replacing
  an exposed key with a fresh, unused id, never a previously retired id.
  Recipients name both a member and a device. Actual past deliveries are a separate,
  irreversible history. An omniscient delivery ledger is not assumed.
- The app's current query result and its history of delivered results are
  separate. Reading a live result can occur between any two entries. Results may
  coalesce, but the API also permits an app to receive every intermediate result.
- The unchanged stop-all rule and its proposed repair are both checked. The
  repair pauses data work while removed but keeps reading store-log evidence.
  Successful evidence reads, available decryption keys and eventual delivery
  are explicit conditions, not consequences of provider re-granting.

### 1. Entry fate: fails as stated

**Current local projections hold under the stated input conditions.**
`effects_ignore_dropped` proves that effect computation ignores the dropped list
and receipt history once replay has produced its kept state. Historical entries
remain evidence for authority checks; deleting that evidence is a different
operation. `views_converge` covers every causal entry/write interleaving in the fixed-schema
coupled machine. Rows and loss records use the existing merge; source files and
uploaded bytes survive hiding; local children, snapshot eligibility, reset
suppression and desired grants are derived. `entry_only_restores` checks one
circle row, its source and local child through deletion and a later drop.
`reports_return` checks a reset losing to a concurrent raise. `rebuild_converges`
proves equality of rebuilt snapshot data and exclusions with the same readable
payloads and write sequence; `snapshot_converges` supplies the existing
order-independent merge theorem for valid effective uncovered writes. These
are conditional results, not a proof that received ciphertext is readable or
that arbitrary SQL snapshots satisfy the input contract.

**Actual provider access fails with one removal after setup.** Ben's device
has received Carol's removal; Ana's has not. The single provider account cannot
be both revoked and granted. `provider_fate_counterexample` proves no actual
grant satisfies both local replays. Even one observer's replay and its provider
cannot change atomically: local commit and remote completion are separate steps.
With two entries, Ben removes Carol, then Ana's earlier concurrent removal of
Ben arrives. Re-grant completes before the older revoke; Carol is active but
revoked (`stale_provider_request_counterexample`). All requests succeeded.
The repair is to promise replay-derived **desired access**, expose pending or
failed provider work, and serialize the owner's explicit grant/revoke work.
Actual access at every device at every point cannot satisfy this property under
the decided storage constraints; changing the wording is necessary.

**The key exception fails with one addition and one unseen delivery.** Ana
adds Carol to Gifts and shares its history key K. Ben has not received the add
or the delivery; his replay excludes Carol and he still uses K. His local check
finds K safe, but Carol already holds it. The addition may later lose to Ben's
concurrent removal of Ana. `unseen_delivery_counterexample` checks the replay,
authority, delivery and failed check. `checked_send_safe` proves the exception
only when observed deliveries include *all* actual deliveries; the witness
shows why storage does not provide that premise. Rotating on every addition
does not retire K on Ben's unread device before its historical copy is shared.
The repair must change the key contract: for example, use a fresh, never-reused
data key per write and specify secrecy relative to that write's recorded
audience, with historical sharing a separate disclosure. A persistent current
key plus retrospective sharing cannot have the universal local-replay promise
without coordinating its retirement with every possible writer. No such
coordination is supplied or silently added here.

Device custody cannot be reduced to member custody. With one device-removal
entry, the member remains while a device that held K is excluded, so K must
also be replaced (`device_custody_matters`). Both devices still hold the same
member secret and can open an envelope addressed to that member
(`member_seal_cannot_distinguish`). That is a cryptographic ability, not a
claim that an honest stopped client fetches the new envelope. A repaired
control channel must distinguish permission to learn entry fate from permission
to obtain data keys; merely restoring all provider reads does not supply that
boundary.

**What the app has been told fails with two entries and one row.** Ben deletes
Gifts, the app receives its hidden row as lost, then Ana's earlier concurrent
removal of Ben drops the deletion. A device receiving the removal first never
reports that loss. Both current lists agree; their equally many delivered
results differ (`notification_fate_counterexample`). `told_persists` proves a
later callback cannot erase an earlier one. The repair is to compare current
query results only, and describe callbacks as observations of a revisable view.
Stuck-list suppression and removal status have the same distinction.

### 2. Convergence: fails for the requested whole state

The notification history above already refutes this property with identical
entries, writes, keys and original local inputs. The repair excludes observation
history from equality and retains the current-result promise.

There is also a key-specific failure. In the existing three-entry
kept/dropped/re-kept removal history, Carol learns K during the drop in one
arrival order but never learns it in the other. The first history must replace
K; the second can keep K. `known_disclosure_rotates` checks safety of the
replacement, and `key_convergence_counterexample` checks different current ids
with the same final entries and writes. Even given complete delivery knowledge,
two devices can independently create different safe replacements at different
paths (`concurrent_refresh_counterexample`). Create-once does not choose between
those paths. A repair must either exclude current key identity from convergence
and allow independently named per-write keys, or add authenticated rotation and
retirement facts to the inputs and specify selection among them. Equal *entries
and writes alone* cannot determine exposure history. Such selection still does
not supply the missing global delivery knowledge in property 1.

### 3. Liveness: fails with the existing stopping conditions

After setup, Ana removes Ben while Ben concurrently removes Carol. Carol reads
her removal and stops. Ana's earlier entry makes Ben's entry drop everywhere
that reads it; Carol never reads it. Two removals suffice. A device-only variant
has Ana's phone remove her tablet while the tablet removes a third device; the
third device stops on the losing removal. `removals_valid` checks both logs and
arrival orders. `member_never_resumes` and `device_never_resumes` prove that
*every* finite number of subsequent successful poll opportunities leaves the
device stopped, establishing an infinite extension too. The failure is the
disabled read, not unfair scheduling. `removal_status_returns` checks the app's
stale Removed result and the repaired result.

The repair is to keep a store-log observer alive independently of data sync.
`observer_receives` proves it processes every offered readable entry;
`observing_resumes` checks both witnesses; `observers_converge` proves identical
finite entry sets yield identical verdicts. This requires eventual successful
control reads. `unreadable_forever` shows why a permanently revoked credential
or missing decryption key still blocks it. Re-grant after a drop must be driven
by an active owner without requiring the excluded device to discover the drop
first. The inherited `KeyExamples.readability_counterexample` still blocks a
key held only by a stopped removed author: making fresh keys cannot decrypt
already-stored ciphertext. A repair needs a readable control channel and a
key-recovery/distribution path that removal does not disable; it may not pretend
that failed key reads are absent entries. With endless new entries, verdicts
may keep changing; eventual stable verdicts require a finite settled history
and fair successful delivery, not merely online authorship.

### 4. API promises: cannot all coexist with the alternative

The relevant contracts are E4's current lost list and irreversible dismissal,
E5's current stuck list and Removed status, E6/E9's operation results, E12's
circle deletion and history keys, and E13/§17.1's breaking-change losses.

- Ordinary rule losses are already reversible in §8/E4. Calling one lost is
  not a promise of permanent absence. Copying its row elsewhere, as §14.7
  suggests, then dropping the deletion leaves both rows (`restore_duplicates`).
  That is an application-visible consequence, not a violation of a nonexistent
  E4 permanence promise. Offer an explicit move with dismissal if the original
  must stay absent; dismissal is an independent ordinary delete and is not
  reversed with the entry. This choice intentionally changes the write set.
- The old-schema write in `schema_loss_returns` is excluded by a raise, then
  restored when an earlier concurrent reset defeats that raise. There are two
  administrative entries and one write. `permanent_schema_loss_counterexample`
  refutes permanence. Entry fate and §17.1's unqualified never-applied promise
  cannot both hold; document these exclusions as reversible, with their cause,
  or make loss an independent explicit irreversible decision and exempt it from
  entry fate. The same dependency issue applies to reset exclusions.
- Freezing a deleted-circle loss cannot hide this conflict. One row, a circle
  deletion, a competing earlier removal and a store raise that read the deletion
  suffice. The deletion drops while the raise stays; the local migration's
  frozen representation still contains only the loss
  (`frozen_dependency_counterexample`). This is a retired-data witness, not a
  proof of arbitrary SQL execution or publication of a deleted circle's snapshot. Retain
  the hidden row and the transformation's inputs, and recompute the loss from
  the current rule. Do not promise permanent retirement for a reversible cause.
- E5's subscriber promises are about current committed results, so reversible
  reset suppression is compatible with them. Remembered callbacks are outside
  that promise. Removed must become a revisable data-sync state if automatic
  resumption is promised, and control observation must remain possible.
- E12's deletion currently promises ordinary row deletes. Entry-only deletion
  changes that promise to hiding every row while the entry is effective.
  E6/E9's successful operation results already describe the observed result,
  not finality: §18 expressly permits a finished entry to drop. Provider
  completion must remain distinguishable from replay's desired access.
  Historical-key sharing in E12 is incompatible with the strict key exception
  as shown above; it needs the changed key contract, not another replay pass.

Thus the four unqualified properties are not jointly implementable as stated.
The model establishes the conditional projection results and the counterexamples;
it does not replace the implemented spec with a claimed universal guarantee.

## Checks and boundaries

`scripts/check.sh proofs` builds this package and audits `Axioms.lean` for
only Lean's `propext`, `Classical.choice` and `Quot.sound`. The CI proofs job
calls that same script. The two imported packages retain their differential
tests; the coupling counterexamples use Rust integration tests and Lean examples.

Values are identified by their setters, as in Appendix B. Schema expression
evaluation, SQL migration execution, encryption/signature correctness, provider
API behavior, crash recovery and Rust/Lean equivalence are not formally proved.
The provider model assumes successful grants/revocations and tracks their durable
results; it does not verify a provider implementation.
Snapshot convergence explicitly assumes valid effective merge contents and
causal uncovered parts; it does not manufacture that invariant for a losing
migration. The counterexamples inspect actual replacement without assuming it.
The ordinary-write durability theorem excludes snapshot replacement and cannot
be used to hide the migration counterexample.
