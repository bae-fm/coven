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
Rust/Lean equivalence remain outside the model. No Rust code or differential
test changes are part of this package. `scripts/check.sh` rebuilds every module
and checks `Axioms.lean`; the audit permits only Lean's own `propext`,
`Classical.choice`, and `Quot.sound`.

## Member-key replacement

`MemberKeySelection`, `MemberKeyPhases` and `MemberKeyExamples` extend the
existing `KeySelection` and `KeyPhases` results for the
[stable-member proposal](storelog.md#c13-stable-members-and-replaceable-keys--checked-proposal).
They use `CurrentReplay.Keyed` directly; the unkeyed interfaces and their
Rust comparisons remain available. The normative rules and layouts are in
`spec/coven.md` and `spec/format.md`; these models still have no Rust differential
for member-key replacement.

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
entry prefix and separately proves the send gate's one-day-and-five-minute
publication bound. The general IO discovery and horizon theorems for the
unkeyed model have not been transferred to this proposal: complete catch-up,
continued refusal of old credentials and the stable entry prefix remain
explicit inputs. SQL rebuilds and physical retention remain outside these
member-key proofs. The exact spec/format changes, including window-only
admission, per-change losses and credential-bearing copy prefixes and reports,
are listed in [the checklist](storelog.md#c14-spec-and-format-checklist-for-stable-members).
