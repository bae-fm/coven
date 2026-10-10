# Store-log effects on data

The Lean package is [storelog-data](storelog-data/). It imports the merge and
store-log packages. Its executable models distinguish database state, knowledge
of keys, provider requests, physical storage, and what the app reads.

## Results against the current spec

**The absolute Revocation sentence fails.**
[§3](../coven.md#3-guarantees) says an ex-member cannot read a write first sent
by a device that already knew they had left. [§11](../coven.md#11-keys), however,
only prevents using a key whose exposure that device knows about.

Ana and Ben are admins; Carol is a member. Three concurrent entries, in
stamp order, demote Ben, promote Carol, and let Ben remove Ana. Ben's removal
introduces K, sealed to Ben and Carol. Ben's tablet receives the removal and
the demotion: the removal now drops because it would leave no admin. The tablet
shares K with the restored Ana, as §11 requires. Carol's promotion then arrives
and the removal returns: Carol can remain admin without Ana.

Ben's phone receives the removal, promotion, then demotion. It never sees its
removal drop, and does not know that its tablet shared K with Ana. Both devices
have the same final entries. The phone knows Ana is removed and first sends a
write with K; Ana has K. S3 access deletion can still be waiting for console
action (§13).

`SecurityExamples.revocation_counterexample` and the following Lean `example`
check both causal orders, the intermediate drop and permitted holder, the
returning removal, the phone's key choice, and Ana's receipt. After setup this
needs three changes to make an entry kept, dropped, then kept again, one
unobserved historical-key copy, and one first send. The changes target different
members, so the result does not depend on the old key-conflict rules. The
receipt abstracts completed sealing and opening; encryption is not proved.
The history runs through `CurrentReplay`, with the shrunk conflict relation.
`storage_history_valid` also checks online first attempts and storage landings.

**Entry fate and data convergence are proved for the modeled effects.**
`ReplayEffects.entry_fate` proves that applying the final kept entries alone
reproduces the actual restart replay's state. It uses the shared store-log
engine and its checked effects; it does not assume the result already equals
that fold. `CurrentData.convergence` connects equal received entries and equal
causal, readable writes to equal rows and losses. Original merge inputs remain
intact when membership or deletion changes.

`CurrentExamples.entry_only_deletion_restores` checks §14.7's deletion and its
reversal without a row write. `deleted_return_reloads` includes a circle that
was absent before replay. `passed_position_does_not_lose_part` loads a formerly
skipped part even though its overall position already passed. Missing reload
inputs retain the old data and positions, refuse circle writes and appear in
the blocked list. `stale_reload_keeps_positions` prevents an old prepared
reload from replacing a newer entry view. `CurrentData.stopped_forever` proves
that an installation stops receiving after its own removal.

Two shared-engine gaps prevent claiming the entire requested model. Its action
type has no rotation entry; key selection models their introductions, but
rotation admission and conflicts are not integrated into replay. Its effects
erase empty circles during replay, while §14.6 requires testing emptiness after
replay. `CurrentExamples.empty_circle_dependency_gap` and the following
`example` check two removals followed by a concurrent addition: the inherited
effect drops the addition, whereas §14.6 says it can populate the circle.
This is a model defect, not a counterexample to that section. Fixing these
requires extending the shared replay's action/effect interface.

**The local key rule is proved.** `KeySelection.first_attempt_safe` proves that
selection requires current membership, an authorized received introduction,
custody, no retirement, and no known delivery to an excluded member.
`selection_is_newest` proves the timestamp and key-id ordering.
`observed_exposure_retires` and `retirement_persists` preserve retirement across
later membership changes. `known_excluded_cannot_read` proves the conclusion
about known deliveries. `revocation_with_complete_knowledge` identifies the
extra assumption required to turn it into a conclusion about all deliveries;
the counterexample does not satisfy that assumption. Attempted retries retain
their key and bytes. Several rotation keys can coexist.

The key model takes authorized received introductions as an input. It does not
prove rotation-entry admission or key generation. Recipients are members;
removing one device does not remove its member or rotate member keys (§13).

**Provider completion is proved for one owner's device.**
[§4](../coven.md#4-storage-providers-and-access) promises intended access, not
instant equality between every device's replay and the provider. `Access.start`
refuses to start a second request while one is in flight. A changed intention
leaves the in-flight request intact. `completion_matches_or_queues` proves that
successful completion either matches the current intention or records the
required next request. `obsolete_completion_queues_opposite` and
`SecurityExamples.serialized_regrant` check revoke followed by re-grant.
`blocked_until_finished` keeps pending work observable. Confirmed S3 key ids
never reappear in deletion notices. Provider implementation and coordination
between different owner devices are outside this proof.

**Every modeled blocked subject is visible with its first reason.**
`Blocked.every_blocked_subject`, `only_first_reason`, and `first_unmet` connect
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
previous finality when a later race starts. `finality_persists` and
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

The model supplies complete storage metadata as an input; provider listing and
authentication are outside the proof. Dependency expressions must name every
entry that can affect an input. Their extraction from arbitrary SQL is outside
the model. The shared-engine gaps described above also limit the modeled
replays covered by the finality theorem.

## Entry fate, convergence, and the retained histories

`Model`, `Operations`, `Keys`, `Delivery`, `EntryEffects`, `Snapshots` and
`Retention` retain the earlier executable behavior. Their counterexamples are
historical witnesses, not assertions that the current spec still requires that
behavior. They do not implement the shrunk conflict list or rotation entries.

`EntryFate` retains originals and computes current effects from replay. Its
`effects_ignore_dropped`, `views_converge`, and `rebuild_converges` prove the
corresponding conditional results for the historical replay and valid readable
merge inputs. The `CurrentReplay`, `ReplayEffects`, `CurrentData` and
`StorageFinality` modules carry the updated results above. Equal encrypted
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
- **Snapshots under dropped removal keys:** §9 and §11 retain those keys for
  reading. The old kept-only catalog predicate is not the current rule.
- **Re-kept keys and historical disclosures:** permanent device-local
  retirement rules out known exposed-key reuse. Unobserved disclosures remain
  possible, as the current Revocation counterexample shows. Key custody and
  current key ids need not converge across devices with different knowledge.
- **Provider revocation and stale requests:** intended access follows replay
  both ways; serialized requests cover the single-device ordering failure.
  Actual grants during a pending request and irreversible S3 console deletion
  are explicit §4/§13 exceptions, not reversible database effects.
- **Reset-cleared reports:** §19.1 makes suppression reversible until final.
  `Blocked.dropped_reset_restores` proves restoration from retained observations.
- **Callbacks and copying a lost row:** E4 promises current query results,
  not matching callback histories. Copying to a different key can duplicate a
  row; E4 now says so. A pending loss can disappear on replay.
- **Losing migration values:** §3 and §17.1 expressly replace these with the
  winning migration's output. `MigrationExamples` remains a checked example
  of this exception, not a counterexample to current durability wording.
- **Stopped members/devices and unavailable historical keys:** §10 expressly
  stops an installation for good. `CurrentData.stopped_forever` checks that gate;
  the legacy `observeEntries` alternative is not the current spec. §11 expressly
  allows required old objects to remain blocked when no reachable device can
  supply their key. `KeyExamples.readability_counterexample` checks that case.
- **Registration, private credentials and old secrets:** stopping does not
  erase previously installed data or disclosed secrets. Replay derives public
  access records; it cannot reconstruct provider-issued secret credentials.

## Readings and limits

“Usable” is device-local knowledge, exactly as §11 defines it. The absolute
sentence in §3 is tested separately rather than silently weakened to that
meaning. Historical-key redistribution follows the sharing device's current
audience, as §11 requires. Current membership and historical authority remain
separate inputs.

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

The blocked list describes current work. Reset suppression retains its inputs;
new observations can record still-unmet conditions. One-shot reads and
subscriptions share a query; callback histories are outside state equality.
Error categories are modeled, while provider-specific payloads and query
scheduling are not.

Encryption, signatures, real provider behavior, SQL execution, crashes, and
Rust/Lean equivalence remain outside the model. No Rust code or differential
test changes are part of this package. `scripts/check.sh` rebuilds every module
and checks `Axioms.lean`; the audit permits only Lean's own `propext`,
`Classical.choice`, and `Quot.sound`.
