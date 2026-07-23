# Store commit protocol

## Status

Implemented as the Store foundation for `plans/circles.md`.

Coven has one Store commit protocol. Devices author immutable commits in
independent streams, name every observed dependency exactly, and apply only
commits whose dependencies are fully materialized. Concurrent accepted history
converges through deterministic row application.

There is no runtime protocol mode, global transaction sequencer, mutable shared
head, conditional-head storage requirement, provisional transaction branch, or
alternate wire format.

## Guarantees

- A successful host transaction is durable locally and receives one `WriteId`.
- A disconnected device may continue authoring host transactions.
- A commit activates only through its author's verified successor stream.
- Every commit names its same-stream predecessor and cross-stream dependencies.
- A receiver advances its frontier only after the complete commit is verified
  and applied atomically.
- Devices possessing the same accepted history converge to the same database
  state.
- Write status exposes local, publication, blocking, and resolution state.

Coven does not promise one globally serializable transaction order or
linearizable offline reads. A local transaction may later merge with concurrent
history into a state no device observed at its original commit time.

## Store root

`StoreCreationDescriptor` commits to:

- the provider and exact storage namespace;
- the application schema version and synchronized routing hash;
- the founder identity and Owner grant;
- the exact Store-root and founder-registration slots;
- the founder provider-administrator grant and capability proof;
- the founder membership stream; and
- the founder recovery stream.

The founder signs one `StoreProtocolRoot`. Opening, joining, restoring, and
publishing verify the root hash, root id, exact object reference, provider
binding, routing hash, signature, and founder authority. Coven accepts one
current root shape and does not translate superseded internal formats.

## Commit identity and order

```rust
struct StoreCommitCoord {
    stream_id: AuthorStreamId,
    sequence: u64,
}

struct StoreCommitOrder {
    seq: u64,
    predecessor: Option<StoreBatchCommitRef>,
    dependencies: BTreeMap<AuthorStreamId, StoreBatchCommitRef>,
}
```

Sequence zero is invalid. Sequence one has no same-stream predecessor. Every
later sequence names the exact prior commit. Dependencies are the greatest
exact commits the author had materialized in other streams when the host
transaction committed.

Each reference includes the semantic coordinate, signed commit hash, and
`ExactObjectRef`. Equal coordinates or hashes with different stored-object
facts are not equal authority.

## Host transaction

One SQLite transaction:

1. captures all declared application changes;
1. validates synchronized identities, schema routing, blobs, and foreign keys;
1. records the exact materialized dependency frontier;
1. writes the application rows;
1. writes one durable Store write with its canonical changeset and `WriteId`;
   and
1. commits application and Coven metadata together.

Failure rolls back both application and Coven state. Durable blob spools needed
for publication are written and synced before SQLite commits; failure removes
only files created by that failed attempt.

The application uses the same SQL operation online and offline:

```rust
let receipt = coven
    .sql(|tx| {
        tx.execute("UPDATE notes SET body = ?1 WHERE id = ?2", ("new", id))?;
        Ok(())
    })
    .await?;
```

## Candidate preparation and publication

The oldest durable write is prepared against current verified membership,
device, and stream authority. Preparation allocates every exact immutable
object slot and persists canonical bytes or a reproducible spool before remote
mutation.

A candidate family is derived from:

- Store root;
- author registration;
- `WriteId`;
- sequence; and
- exact predecessor.

Replacements at that competition point share the family. Packages, access
objects, bootstrap objects, and the signed candidate manifest carry it.
Cross-family or undeclared candidate objects are rejected.

Publication:

1. creates and exact-reads every package, blob, and control prerequisite;
1. creates and exact-reads the signed commit;
1. creates the author-stream head in the predecessor-reserved successor slot;
1. reads the accepted head and commit through the pull verifier; and
1. records activation and durable write status atomically.

An ambiguous create is settled by exact readback. Equal canonical bytes complete
the retry. Different valid bytes prove another candidate won that stream slot.
Transport failure alone is never a nonactivation proof.

## Activation and dependency readiness

A physically present package or commit is inert without its verified stream
head. A head is accepted only when:

- its stream activation is rooted in verified membership or device authority;
- its registration and device signature verify;
- its slot, sequence, predecessor, commit, and successor link are exact;
- its author was active at the signed predecessor state;
- its membership and device-state references reproduce; and
- its retained-history summary agrees with the accepted prefix.

A commit enters the ready queue only after:

- its same-stream predecessor is materialized;
- every exact cross-stream dependency is materialized;
- referenced membership, registration, lifecycle, and control authority is
  verified; and
- every required package and blob has been fetched and verified.

Missing dependencies remain explicit pending input. The receiver neither drops
the commit nor advances its frontier. Permanently impossible dependencies
surface through verified exclusion, revocation, abandonment, or retained
history evidence; absence alone is not such proof.

## Atomic application

One incoming commit applies in one SQLite transaction:

1. rechecks the accepted commit and its authority;
1. verifies and decrypts every referenced immutable object;
1. applies Store controls and package rows;
1. materializes blobs and row bindings;
1. validates the final database constraints;
1. records exact history, retained authority, and acknowledgements; and
1. advances the accepted frontier.

Any failure rolls back rows, controls, blobs, ownership, status, and frontier.
Retry reuses the same exact inputs.

## Row convergence

- Explicit dependencies apply before dependent commits.
- Independent ready commits use the protocol's canonical total stamp order
  where an order is required.
- Different-column concurrent updates are three-way merged.
- Same-column concurrent updates use `_updated_at`.
- Deletes win over concurrent edits.
- Foreign-key-dependent changes wait for the exact parent history.
- Non-foreign-key uniqueness and check conflicts reject the incoming changeset
  atomically without frontier advancement.

After a commit's complete dependency frontier is materialized, UPDATE
`NOTFOUND` means an accepted concurrent delete already removed that row. Before
readiness, the UPDATE is not applied. This distinction prevents arrival order
from turning a missing INSERT into a permanent omitted edit.

## Candidate nonactivation and cleanup

A prepared candidate may be replaced or discarded only with exact evidence
that it cannot activate:

- a different verified head in its reserved successor slot;
- an accepted same-author abandonment naming its complete signed bytes;
- an accepted author exclusion whose cut excludes it;
- an accepted membership-grant revocation whose cut excludes it; or
- a verified dependency retraction.

Candidate cleanup persists the proof and exact target list, deletes
candidate-exclusive children before the losing commit, verifies each target
absent, and clears local ownership only in the final transaction.

Activated authority is never candidate cleanup. Objects shared by another
pending or activated candidate remain owned. Failed cleanup leaves the exact
durable operation available for idempotent retry and reports the failure.

## Durable write status

```rust
enum WriteStatus {
    LocalOnly,
    Pending,
    Publishing,
    Published(Box<PublishedPosition>),
    Blocked(WriteBlock),
    Resolved(WriteResolution),
}
```

`CovenHandle` exposes:

- the status for one `WriteId`;
- a subscription to that status; and
- all pending writes with affected row identities.

`Published` names the exact stream commit that made the write visible. A later
verified nonactivation proof moves it to a resolved retraction with the exact
witness. A semantic block is not hidden as a transport retry.

## Snapshots, retained replay, acknowledgements, and reclaim

Snapshots cover exact fully materialized frontiers and retain the authority
needed to verify them. Restore installs a verified image and replays only
dependency-ready accepted history beyond its coverage. The final image,
frontier, membership, device state, and retained replay baseline become current
atomically.

Acknowledgements name exact accepted frontiers and snapshot coverage. Reclaim
requires verified active-device acknowledgement, snapshot, materialization, and
ownership evidence. It produces an exact signed deletion plan, verifies each
remote absence, and commits retirement only after the complete plan succeeds.

No list result, elapsed grace period, later repair pass, or unverified local
summary can authorize deletion.

## Ownership boundary

`sync::store::Store` owns every workflow that interprets Store authority:

- creation and opening;
- host-write planning and publication;
- pull discovery, readiness, verification, and application;
- membership, device joining, exclusion, and recovery;
- acknowledgement, snapshot, restore, retained replay, and reclaim;
- candidate replacement, abandonment, ownership, and cleanup; and
- durable retry and terminal results.

`StoreDatabase` owns atomic Store-specific SQLite transitions, including host
capture before remote Store construction. Shared modules may own closed
canonical bytes, cryptography, immutable transport, SQLite row/image mechanics,
and application result values. Application and cycle code do not continue a
Store operation by reaching around these owners.

## Verification

- bad-order and good-order INSERT/UPDATE delivery converge;
- missing exact dependencies never advance rows or frontier;
- different-column, same-column, delete/edit, foreign-key, uniqueness, and
  check conflicts exercise the production apply path;
- a present commit without its activating head remains inert;
- forged coordinates, references, dependencies, summaries, authority, or
  manifests fail before application;
- every publication and application failure point leaves the prior state or the
  complete new state;
- candidate cleanup deletes only objects excluded by exact proof;
- restore, retained replay, acknowledgement, and reclaim use exact frontiers;
- formatting, strict lint, focused tests, all-feature tests, repository hooks,
  rule review, and data-integrity review pass; and
- code, tests, fixtures, plans, and documentation contain no alternate protocol
  implementation or obsolete Coven-internal reader.

## Circles handoff

Circles extend the same `StoreBatchCommit` with audience packages and control
objects. They do not add another activation coordinate or ordering system.
Audience routing decides which verified package representation applies; exact
commit dependencies still decide when the commit is ready.
