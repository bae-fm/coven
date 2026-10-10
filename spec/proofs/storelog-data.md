# Store-log effects on data

The Lean package is [storelog-data](storelog-data/). It imports the merge and
store-log packages. Its executable models distinguish database state, knowledge
of keys, provider requests, physical storage, and what the app reads.

The package uses the store-log package's `CurrentReplay.Action`, `Entry`
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
§11 requires each pass to read every known writer's new copies through a
next-number miss alongside the store log. Retained permanent copies and these
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

Every send also requires §10's completed catch-up to have started less than
five minutes ago, measured monotonically with sleep counted. This includes
fixed retries and independent file uploads. The model does not represent
that timer or the one-day provider-request duration assumption; they are
[IO obligations](io.md#discovery-obligations), not consequences of the
revocation theorems. Freshness bounds stale sending without eliminating
copies published after the sender's observation.

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
was absent before replay. `passed_position_does_not_lose_part` loads a formerly
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

Every recorded S3 key, including a later dropped access entry, contributes
until confirmed deleted (`credential_pending_visible`,
`dropped_access_stays_pending`). Confirmed ids never reappear. The account
proof quantifies over one already recorded account; provider account discovery,
provider implementation and coordination between owner devices are outside it.
The existing journal supplies operation ids for the pending subjects.

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
has arrived; a missing entry blocks it. Saved qualifying windows preserve
previous finality when a later race starts. The specification represents their
union by one monotonic horizon; this package still models saved certificates.
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
- **Provider revocation and stale requests:** intended access follows replay
  both ways; serialized requests cover the single-device ordering failure.
  Actual grants during a pending request and irreversible S3 console deletion
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
Rust/Lean equivalence remain outside the model. No Rust code or differential
test changes are part of this package. `scripts/check.sh` rebuilds every module
and checks `Axioms.lean`; the audit permits only Lean's own `propext`,
`Classical.choice`, and `Quot.sound`.
