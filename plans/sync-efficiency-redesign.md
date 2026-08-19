# Sync efficiency redesign

A field store of about twenty releases reached 385 retained commits and a 223 MB
`retained_merge_materializations` table, and a phone's pull cycle took 391
seconds. Neither number is a constant factor away from right. This is the shape
that produces them and the shape that replaces it.

Measurements quoted here are either live (coven main at `9be90d62`, GCS, mac +
phone) or from the two-device acknowledged-history fixture in
`store_history_checkpoint_tests.rs`. Nothing here is estimated.

## What the measurements say

Phone pull, 391 s total, applying ~150 new commits:

| stage | time |
| --- | --- |
| materialize | 244.6 s |
| fetch blobs | 80.5 s |
| prepare retained history | 38.3 s |
| verify commits | 9.3 s |
| fetch commits | 7.7 s |
| fetch heads | 5.4 s |
| load packages | 1.9 s |

Mac, Move-to-Cloud of a 22-file release, publication 11.0 s: `publish packages`
8977 ms of it. (Fixed — the prepared objects now fan out under the transfer
limit. Listed because it is the same disease: a loop that should not have been
serial.)

Retained row, dissected by field across sequences on the fixture:

| field | seq 1 | seq 13 | seq 27 |
| --- | --- | --- | --- |
| `history_evidence` | 13 509 | 51 953 | 94 635 |
| `commit` | 18 324 | 24 833 | 24 8xx |
| `activation_head` | 5 705 | 6 588 | 6 6xx |
| `activation` | 2 296 | 2 300 | 2 3xx |
| `packages` | 2 | 2 | 2 |

Only `history_evidence` moves, by **+6 098 bytes per acknowledging commit**, and
inside it only `chain`, whose entry count runs 2, 3, 4, 5 … one per
acknowledging commit. Everything else is flat.

## Disease 1: a retained row carries the history in front of it

`RetainedVerifiedActivatedAck` holds `chain: BTreeMap<u64, (StoreAckRef,
StoreAck)>` — the device's **entire acknowledgement chain from sequence one** —
and one of these is embedded in every commit that activates an acknowledgement.
On a two-device store nearly every commit does. The row at sequence N therefore
stores N acknowledgements, and the table is O(N²).

It also stores `activating_commit_value: StoreBatchCommit` — a second full copy
of the commit the row is already storing in `input.commit`. `validate_for`
asserts the two are equal, so it is duplication by construction, about 7.3 KB per
acknowledging row.

Both are consequences of one decision: the row was made self-sufficient for
proving contiguity, rather than the *set of rows* being self-sufficient.

### Replacement

A retained row describes its own commit and nothing else:

```rust
pub struct RetainedVerifiedActivatedAck {
    pub acknowledgement: (StoreAckRef, StoreAck),
    pub activating_commit: StoreBatchCommitRef,
}
```

Contiguity comes from the rows, exactly as commit ancestry does. Each
acknowledgement names its predecessor's object; row N-1 holds acknowledgement
N-1. Walking the rows proves the chain without any row carrying a copy of what
came before it — the same reason a commit does not embed its ancestors.

The whole chain still exists in the one place it must: a snapshot's portable
summary, because a device restoring from a snapshot has no rows to walk. That
becomes a separate type, folded once per snapshot generation from the rows the
snapshot covers:

```rust
pub struct RetainedAcknowledgementChain {
    pub chain: BTreeMap<u64, (StoreAckRef, StoreAck)>,
    pub activating_commit: StoreBatchCommitRef,
    pub activating_commit_value: StoreBatchCommit,
}
```

`validate_chain` (contiguity from one) moves to that type and is asserted where
it is meaningful — at the snapshot boundary — instead of being re-proved at every
commit. `activating_commit_value` stays there, where the summary is stating
something a restoring device cannot otherwise check.

The same audit applies to `RetainedMergeMembershipProof`, which is retained per
commit and carries `commit_value`. It is measured flat today, so it is checked,
not assumed, by the invariant test below.

**Target:** the table is single-digit MB at this library size; materializing a
commit costs O(that commit).

## Disease 2: an idle device appends durable history every thirty seconds

385 commits for ~20 releases. Most are acknowledgement commits, two devices
each minting one per cycle.

`stage_acknowledgement` has no "has anything changed?" guard. Every cycle it
reads the frontier, takes sequence N+1, allocates a provider slot, signs, and
publishes a Store commit to activate it — with the same `store_cut`, the same
`device_state`, the same snapshot selection and the same exclusions as the one
before. The only field that differs is `last_sync`.

Checked before proposing this: nothing consumes acknowledgement freshness.
`last_sync` is read only for the founder's *initial* acknowledgement
(`store_authority.rs:231`, `recovery_preparation.rs:192`) and asserted parseable
in one test. The device activity a host renders comes from `visible_heads`, not
from acknowledgements.

### Replacement

Stage an acknowledgement only when its meaningful content — `store_cut`,
`device_state`, `snapshot`, `exclusions` — differs from this device's last
published one. A quiescent store appends nothing.

This is self-correcting rather than a heuristic: an acknowledgement exists to say
"this device has seen this cut". When a peer publishes, this device's frontier
changes, its content differs, and it acknowledges. When nothing is published,
there is nothing to say.

**Product-visible consequence, flagged rather than hidden:** an idle device's
"last synced" reading, if a host derives one from history, stops advancing while
nothing happens. It is derived from heads today, so no host changes — but it is a
behaviour change to validate on device rather than assume.

**Target:** a settled two-device store appends zero commits per cycle.

## Disease 3: pull fetches one object at a time

`publish packages` was the same disease on the write side and is fixed. The read
side has three instances:

- **fetch blobs, 80.5 s.** `prepare_package` verifies each blob binding in a
  serial `for`. Fan out under `transfer_limits().downloads`, keeping the barrier:
  a package is prepared only when all its blobs have landed.
- **fetch commits, 7.7 s.** The announcement walk is inherently sequential —
  each head names the next slot — but the *commit* behind each head is an
  independent fetch, and separate devices' streams are independent walks. Fetch
  commits for a discovered head range in parallel; walk each device's stream
  concurrently.
- **membership, 15 reads over 5 distinct objects per settled cycle.** These
  arrive through `read_protocol_slot`, which addresses a slot rather than an
  exact object, so the object-bytes memo cannot serve them — and a slot is not
  immutable, so remembering a slot read would be a cache that can lie. It gets
  the shape the announcement walk already uses: a verified prefix plus a live
  probe past its end.

**Target:** a settled cycle is a handful of listing round trips. A cycle with N
new commits adds parallel fetches bounded by the transfer limits, plus
milliseconds of local verification.

## Disease 4: verification of immutable artifacts is rebuilt every cycle

The consolidation direction, finished. `StoreCommitVerifier`'s memos are built
with the verifier and the verifier is rebuilt every cycle, so immutable facts are
re-derived on a schedule. The durable tier already holds the same bytes: every
verified protocol object is pinned in `remote_objects`.

`load_exact_object` — the one funnel for reads by reference — serves from the
local pinned objects when present and reaches the provider only for what this
device has never held. Content-addressed, verified on every load, so an answer
from local storage is the answer a read would have produced. The per-cycle
bytes memo added on top of it becomes redundant and is deleted, not kept
alongside.

**Target:** a settled cycle reads the provider only to discover what is new.

## Disease 5: mechanisms that exist because of the diseases

Deleted with the shapes that justified them, not deprecated:

- the per-cycle exact-object bytes memo, once the durable path serves it;
- `load_acknowledgement_proof_chain` on the materialize path, once rows carry one
  acknowledgement (it stays for restore, which genuinely walks a chain);
- `RetainedCommitAuthorities`-style dual paths if any remain after the reshape;
- any read-counting workaround that exists only to make the quadratic tolerable.

The boundary gates stay and shrink. `read_prepared_protocol_slot` joins the
gated set as its callers settle.

## What is not negotiable

Every byte still verifies against its signature chain. The reshape moves *where*
a proof is stated, never whether it is checked:

- a retained row's acknowledgement is still parsed and signature-checked against
  its activated registration on every open;
- chain contiguity is still asserted, at the snapshot boundary, over the folded
  chain;
- a gap or a fork in the rows fails loudly at the fold rather than being repaired.

No migration, no dual shapes, no self-heal. `~/.bae` is wiped between milestones.

## Invariant tests

Extending the two families that already exist rather than adding a third:

1. **DB bytes per retained row are flat across depth.** `canonical_input` for
   commit N is bounded independent of N, and grows only with that commit's own
   operation size. Replaces the current `retained_materialization_rows_do_not_
   repeat_predecessor_history`, whose 1 024-byte tolerance over ten commits hid
   ~600 bytes per commit — it passed throughout.
2. **Materialize cost is O(commit).** Applying commit N touches bytes bounded
   independent of N.
3. **A settled cycle appends no commits.** Two devices, quiescent store, retained
   count unchanged across cycles.
4. **Settled-cycle provider reads stay constant and small**, at every depth —
   the existing `acknowledged_history_depth_does_not_change_what_a_settled_
   cycle_reads`, tightened as the count falls.

## Branch sequence

Storage reshape first: it is what unblocks live validation, and every later
measurement is distorted while rows are quadratic.

1. **Retained row is O(commit).** Split the per-commit acknowledgement from the
   snapshot chain; fold the chain at the snapshot boundary; drop the duplicated
   activating commit value. Invariant tests 1 and 2.
2. **Idle stores append nothing.** Acknowledge on change. Invariant test 3.
3. **Pull fan-out.** Blob fetches, then commit fetches, under the transfer
   limits.
4. **Membership prefix memo.** The last per-cycle re-walk.
5. **Durable verified objects.** `load_exact_object` from local storage; delete
   the per-cycle memo it replaces.
6. **Removal sweep and gate shrink.**
