# Store-log effects on data

The Lean package is [storelog-data](storelog-data/). It imports the merge and
store-log packages. Its executable models distinguish database state, knowledge
of keys, provider requests, physical storage, and what the app reads.

## Model boundary for key introductions

The executable removal witnesses carry key introductions on removals. §11
instead uses separate authorized rotations after membership changes. The
model's receipts abstract successful sealing and opening; they do not check
D6's SHA-256 key commitment or the writer-specific D10 paths. The claims
below describe those executable inputs, not a proof of these format checks.

Exposure in §11 is conservative: any copy addressed to an excluded member
retires its named key. Ana cannot decrypt a box for excluded Dan to check
its key hash. A forged K2 box can therefore cause an extra rotation, while
a recipient still rejects a key whose hash differs from K2's introduction.
The model's successful receipts do not prove this third-party detection or
the recipient's hash check; both are format/IO verification obligations.

Ana removes Ben, then Carol rotates K for the remaining members. If a
concurrent replay returns Ben, historical sharing supplies him K. This is
the spec's corresponding disclosure history; the checked witness below
attaches K to the removal instead.

## Results and their assumptions

**Revocation is proved with the stated observation window.**
[§11](../coven.md#11-keys) requires each pass to read every known writer's new
copies through a next-number miss alongside the store log. Retained permanent
copies and these reads supply the model's complete copy observation.
`KeySelection.revocation_between_listings` proves that a key
selected for a first attempt has no copy for an excluded member in storage,
provided no such copy appears after that observation. The theorem names this
residual window as `noLaterCopy`; it assumes neither local memory of a dropped
entry nor a separate retirement table. Failed or incomplete observations
produce no pass (`listing_failure_blocks`). The theorem names use “listing”
for this supplied set; they do not prove the numbered discovery algorithm.

Ana and Ben are admins; Carol is a member. Three concurrent entries, in
stamp order, demote Ben, promote Carol, and let Ben remove Ana. Ben's removal
introduces K, sealed to Ben and Carol. Ben's tablet receives the removal and
the demotion: the removal now drops because it would leave no admin. The tablet
shares K with the restored Ana, as §11 requires. Carol's promotion then arrives
and the removal returns: Carol can remain admin without Ana.

Ben's phone receives the removal, promotion, then demotion, keeping the removal
throughout. Reading the tablet's new copies finds Ana's copy anyway, and
the phone retires K for first attempts (`listing_prevents_reuse`).

**Revocation without the window hypothesis fails.** Move that one share to
after the phone's miss for that writer: it selects K during the pass, while
Ana can obtain the new copy. `residual_window_counterexample` and the
following Lean `example` check this history, including the next observation
refusing K.
`sharing_history` checks both causal orders, the intermediate drop, the allowed
share, and the removal's return. `storage_history_valid` checks online first
attempts and storage landings. After setup the witness has three concurrent
changes, one later sealed copy and one first send. S3 access deletion can still
await console action. The theorem can cover the whole period a write remains
readable by extending `later` to that period; it cannot prevent later disclosure.
Receipts abstract completed sealing and opening, not encryption.

**Entry fate and data convergence are proved for the modeled effects.**
`ReplayEffects.entry_fate` proves that applying the final kept entries alone
and then removing circles left empty reproduces the actual replay's state.
It uses the shared engine and its recorded-past removal effects; it does not
assume the result already equals that fold. `CurrentData.convergence` connects
equal received entries and equal causal, readable writes to equal rows and losses. Original merge inputs remain
intact when membership or deletion changes.

`CurrentExamples.entry_only_deletion_restores` checks §14.7's deletion and its
reversal without a row write. `deleted_return_reloads` includes a circle that
was absent before replay. `passed_position_does_not_lose_part` loads a formerly
skipped part even though its overall position already passed. Missing reload
inputs retain the old data and positions, refuse circle writes and appear in
the pending list. `stale_reload_keeps_positions` prevents an old prepared
reload from replacing a newer entry view. `CurrentData.stopped_forever` proves
that an installation stops receiving after its own removal.

Rotations and replay-empty circles use `CovenStorelog.CurrentReplay` directly;
there is no second replay policy in this package. `empty_circle_rows_and_cause`
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

`selectInReplay` derives membership and historical authority from the shared
replay. `selected_introduction_authorized` connects selection to a received,
authorized introducing action, including tag 15's actual audience and id.
`rotations_coexist` checks Ben and Carol's concurrent rotations after Ana's
removal: both stay kept, and the later usable key is selected. Fresh random ids
and successful key acquisition are inputs. Recipients are members; removing
one device does not remove its member or rotate member keys (§13).

**Provider completion is proved for one owner's device.**
[§4](../coven.md#4-storage-providers-and-access) promises intended access, not
instant equality between every device's replay and the provider. `Access.start`
refuses to start a second request while one is in flight. A changed intention
leaves the in-flight request intact. `completion_matches_or_queues` proves that
successful completion either matches the current intention or records the
required next request. `obsolete_completion_queues_opposite` and
`SecurityExamples.serialized_regrant` check revoke followed by re-grant.
The access-work visibility theorem keeps pending work observable. Confirmed S3 key ids
never reappear in deletion notices. Provider implementation and coordination
between different owner devices are outside this proof.

**Every modeled pending subject is visible with its first reason.**
The model's visibility theorem, `only_first_reason`, and `first_unmet` connect
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
with `CurrentReplay`'s conflict relation. It uses §9's strict old prefix and
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
the model. The theorem applies to the shared current replay, including
rotations and circles hidden only after replay.

## Entry fate, convergence, and the retained histories

`Model`, `Operations`, `Keys`, `Delivery`, `EntryEffects`, `Snapshots` and
`Retention` retain the earlier executable behavior. Their counterexamples are
historical witnesses, not assertions that the current spec still requires that
behavior. Their replay uses the historical conflict list. `Keys.introduced`
also recognizes rotation entries for the current selection model.

`EntryFate` retains originals and computes current effects from replay. Its
`effects_ignore_dropped`, `views_converge`, and `rebuild_converges` prove the
corresponding conditional results for the historical replay and valid readable
merge inputs. The shared `CovenStorelog.CurrentReplay`, `ReplayEffects`,
`CurrentData` and `StorageFinality` modules carry the updated results above. Equal encrypted
objects alone do not imply readable inputs when historical keys are missing.

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
  keys for reading, independently of later membership replay. The executable
  dropped-removal-key witness uses its own removal representation.
- **Re-kept keys and historical disclosures:** the sealed-copy observation exposes
  earlier disclosures even to devices that never saw the introducing entry
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
revocation witness concerns Ben's new writes, not that exception.

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

Encryption, signatures, real provider behavior, SQL execution, crashes, and
Rust/Lean equivalence remain outside the model. No Rust code or differential
test changes are part of this package. `scripts/check.sh` rebuilds every module
and checks `Axioms.lean`; the audit permits only Lean's own `propext`,
`Classical.choice`, and `Quot.sound`.
