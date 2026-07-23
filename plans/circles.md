# Circles — per-audience sync within one Store

## Goal

Add private, independently managed Circles to Coven's one Store protocol. A
synced row belongs to exactly one audience: the Store, one Circle, or the local
device. Store members receive Store rows. Effective Circle members also receive
that Circle's rows. Local rows never leave their device.

Coven has one protocol throughout this plan: independently authored immutable
device streams whose verified, dependency-ready commits converge through
deterministic application. There is no second protocol engine or runtime mode.

## Forbidden architecture

Circle work must not reintroduce any removed coordinated protocol shape:

- no `Serial`, `WritePolicy`, policy selector, mode flag, or engine dispatch;
- no global mutable head, conditional head advance, provisional branch, or
  branch-conflict result;
- no conditional-write storage capability or capability probe;
- no policy-shaped enum with one surviving variant;
- no alternate package, snapshot, acknowledgement, membership, recovery,
  Circle-control, or database path;
- no compatibility reader, legacy object, fallback, shim, or migration for an
  earlier Coven-internal format; and
- no test, fixture, example, or documentation that constructs or describes the
  removed protocol.

The direct causal types are the product types. Do not retain an abstraction in
case another protocol is added later.

`plans/storage-engine-separation.md` records the completed transition to the
single concrete Store owner. `plans/store-commit-protocol.md`,
`plans/deterministic-discovery.md`, and
`plans/notfound-omit-for-circles.md` define the Store ordering, exact storage,
and dependency-readiness foundations this plan extends. Circle work must never
rebuild the deleted choice.

## Current implementation state

### Complete foundation

- The coordinated protocol, its public configuration, wire variants, database
  state, storage capability, and runtime implementation are removed.
- `sync::store::Store` is the concrete protocol owner.
- Store operation planning, package preparation, candidate publication,
  abandonment, pull, and snapshot workflows live below that owner.
- Application and cycle commands enter the concrete Store owner; the final
  boundary audit found no production command that continues a Store operation
  through raw database or storage access.
- Store commits use immutable per-device streams, explicit dependencies,
  cumulative summaries, and atomic retained replay when late concurrent
  discovery would otherwise violate canonical order.
- Founder Circle creation and rename have durable operations and Store-commit
  activation.
- Circle member addition authors a roster successor, retains the predecessor
  roster needed to verify the Owner grant, inherits metadata without inventing
  a metadata successor, and activates recipient access through the durable
  operation journal.
- Member-addition bootstrap images are created at an atomic materialized Store
  frontier through the authenticated Circle routing projection. The signed
  recipient leaf names the exact image and a canonical set of existing Circle
  blob references; the activating Store commit retains ownership of both.
- Circle images contain application rows and authenticated routing state, not
  source-device transport or ownership tables. Exact blob references are
  extracted before that state is removed and are signed outside the image.
- The signed schema-routing contract records each descendant's explicitly
  selected audience-parent foreign-key column.
- Independent UUID and intentional shared-key identities are validated on host
  capture and incoming changesets.
- Host capture writes authenticated Store mirrors and complete private route
  images; pull recomputes every private routing identifier before application.
- Audience moves publish the Store mirror and destination image without a
  source package.
- Store and Circle row packages are filtered against the winning Store mirror.
  A private route is authenticated against the audience and stamp of its exact
  companion Store transition before the current winner is considered.
- Routing audience and ordinary row content resolve independently. An eligible
  destination image sets the root audience selected by the mirror while normal
  merge rules retain newer concurrent content.
- Move/move, move/edit, delete/move, and move/Local converge after progressive
  or complete concurrent discovery.
- Host commit and remote materialization validate every scoped row's final
  synchronized foreign keys, including unchanged descendants.
- Store snapshots contain only Store rows and their authenticated private
  routes, retain the opaque Store audience mirror and activated Circle control
  indexes, and exclude Local and Circle rows.
- Store snapshot creation validates the projected image before publication.
  Bootstrap and restore run application migrations, validate the resulting
  routing graph, install retained replay authority, and commit as one database
  transaction.

### Required before Circles are complete

- use the authenticated routing boundary for Circle snapshot, bootstrap, and
  restore images;
- complete Circle membership, access, epoch close, rotation, conflict
  resolution, deletion, and durable restart;
- add Circle bootstrap, acknowledgement, snapshot, restore, and reclamation;
- finish audience-aware blob movement and retention;
- expose the application API and errors described here; and
- update all Coven documentation and dependent applications to the finished
  API.

## Completion order

1. **Complete.** Seal the single Store owner boundary.
1. **In progress.** Schema contracts, row identity, host and pull routing
   authentication, destination-only moves, and final-component validation are
   implemented. Store snapshot creation, bootstrap, and restore installation
   use the same authenticated routing boundary. Adversarial routing arrival
   orders converge through independent audience and content resolution. Circle
   images must use and verify the same boundary.
1. Finish Circle authority, roster, metadata, access, epoch, and lifecycle
   operations.
1. Finish Circle packages, pull, bootstrap, acknowledgement, snapshots,
   restore, reclamation, and blobs.
1. Finish application APIs, integration tests, fault injection, documentation,
   and dependent application updates.

Each completed boundary updates this plan when implementation discovers a
different invariant or representation. Status marks alone are not enough.

## Product model

### Store

A Store is the trust and replication root. It contains:

- the signed Store root and immutable schema-routing contract;
- Store membership and device registrations;
- immutable per-device commit streams;
- Store-visible rows and routing metadata;
- Circle public control state; and
- references that activate Store and Circle packages.

All devices apply the same accepted Store commits in the same deterministic
order once they possess the same verified history. Offline writes are local
until their commit is published and activated. Coven does not claim global
serializability or linearizability.

### Circle

A Circle is a private audience inside one Store. It has:

- a permanent random `CircleId`;
- an Owner-controlled roster and display metadata;
- recipient-specific access records;
- an epoch and key generation;
- public signed control state visible to Store members; and
- encrypted packages, snapshots, acknowledgements, and bootstrap images visible
  only to effective Circle members.

Circle names are display metadata, not identity. Renaming never changes the
`CircleId`, keys, rows, or package history.

### Audience

```rust
enum Audience {
    Store,
    Circle(CircleId),
    Local,
}
```

A row has exactly one audience at a time. A person may belong to any number of
Circles. An audience is therefore not a Circle: Store and Local are audiences,
and each Circle supplies one more audience.

Only a synchronized graph root declares or stores an audience. Descendants
inherit it through the graph's selected foreign-key path.

### Effective membership

Circle access requires both:

1. active Store membership; and
1. active Circle roster membership.

Removing a person from the Store immediately removes their effective access to
every Circle even before a later Circle key rotation. Rotation protects future
Circle writes; it cannot make already received history unseen.

## Schema and routing contract

### Root declarations

The application declares synchronized tables and graph roots when the Store is
created.

`gated_by` is the two-audience form:

- `NULL` means Store;
- the reserved local value means Local.

`scoped_by` is the general form:

- `NULL` means Store;
- a `CircleId` means that Circle;
- the reserved local value means Local.

A table cannot declare both. Descendant tables declare the selected parent
foreign key through which they inherit audience.

### Signed immutable contract

The Store root commits to one canonical schema-routing contract containing:

- synchronized table names;
- row-identity columns and canonical encodings;
- each root's gate or scope declaration;
- the selected audience-inheritance foreign keys;
- primary keys, required columns, relevant collations, and unique parent
  structure;
- the synchronized foreign-key graph; and
- blob-column declarations.

The contract hash uses exact canonical bytes and is verified before database
opening, joining, package preparation, package application, snapshot restore,
or bootstrap installation.

Ordinary host-only columns and indexes are outside this contract when they do
not change synchronized identity, routing, constraints, or blob meaning.

Routing topology is immutable for an existing Store. Coven rejects a database
whose declared contract differs from the signed Store root. Greenfield Coven
does not translate an older contract or preserve an alternate reader.

Creating the application schema, Coven marker, routing contract, and internal
tables is one SQLite transaction. Failure leaves none of them installed.

### Row identities

Every table whose rows may be independently created on multiple devices uses a
globally unique row identity. Coven validates declared identities and rejects a
scoped schema that permits ordinary locally allocated integers for independent
creation.

UUID version 4 and version 7 values are accepted for independent row identity.
A declared `SharedKey` is allowed only when the same key intentionally denotes
the same logical row across devices.

Identity validation happens before the host transaction commits and again when
an incoming package is decoded. A reused UUID is the same logical row, not a
second row. Its concurrent contents follow the normal merge and constraint
rules.

### Foreign-key audience invariant

For every synchronized foreign key from child to parent:

- child and parent may have the same audience; or
- the parent may have Store audience.

No Circle row may point into another Circle or Local. No Store row may point
into Circle or Local. No Local row may be required by remotely synchronized
state.

Validation covers the final connected component, not only rows named in the
changeset. It therefore includes unchanged descendants and receiver-only rows
whose validity could be broken by a concurrent move or delete.

The complete incoming application, routing changes, pruning, blob references,
foreign-key checks, and frontier advance commit in one SQLite transaction or
roll back together.

## Private row routing

### Why routing metadata exists

The same logical row can move between Store, Circle, and Local audiences while
offline devices still hold older packages. The receiver needs a Store-visible,
convergent answer to which audience currently owns the row without revealing
the application's real table name or primary key.

### Stable routing identifier

```rust
struct RowRoutingId([u8; 32]);
```

Derive it as an HMAC over a domain-separated, length-prefixed canonical
encoding of `(table_name, row_identity)`. The routing key is derived from the
generation-one Store key and Store root, so every Store member can calculate
the same value. Table names and primary keys never appear in Store-visible
routing rows.

Predictable application identities can still be guessed by a Store member who
holds the routing key. The security documentation must state this leakage.

### Internal tables

The Store-visible mirror contains only convergent routing state:

```sql
CREATE TABLE _coven_audience (
    routing_id TEXT PRIMARY KEY,
    circle_id TEXT,
    _updated_at TEXT NOT NULL
);
```

`routing_id` is the canonical lowercase hexadecimal encoding of
`RowRoutingId`; `circle_id` uses the canonical `CircleId` text encoding.

Meaning:

- no mirror row: the scoped row is deleted or Local;
- `circle_id IS NULL`: Store audience;
- `circle_id = X`: Circle X.

The private map contains the local meaning of a routing identifier:

```sql
CREATE TABLE _coven_row_routes (
    routing_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    row_id TEXT NOT NULL,
    _updated_at TEXT NOT NULL,
    UNIQUE (table_name, row_id)
);
```

The private map travels only in the row's audience package. Local rows have no
mirror or synchronized private-map row.

### Authenticated route boundary

Private route rows are protocol metadata, not ordinary application changes.
Incoming packages may contain only complete route INSERT images. Coven:

1. decodes the declared table and row identity;
1. confirms the table and identity shape against the signed contract;
1. recomputes `RowRoutingId` from those exact values;
1. requires equality with the supplied routing identifier;
1. requires one route for every applicable scoped root and no orphan route;
1. rejects duplicate, partial, UPDATE, and DELETE route operations; and
1. passes only normalized verified route values to application.

The same validation is used for local capture, remote pull, snapshot creation,
snapshot restore, and bootstrap installation. No caller can insert raw routing
changes around it.

### Applying a package

For each accepted Store commit:

1. apply its Store routing mirror changes;
1. determine the winning audience for every referenced `RowRoutingId`;
1. decode private route rows and authenticate each against the audience and
   stamp of its exact companion Store transition;
1. omit row operations whose package audience does not match the winning
   mirror;
1. apply eligible Store and Circle rows with deterministic content merge
   semantics, then set scoped root audiences from the winning mirror;
1. prune locally materialized roots whose mirror names another audience or no
   audience;
1. validate the final foreign-key component and blob references; and
1. record all rows, routing state, controls, and the commit frontier atomically.

Mirror deletes are remove-wins. Other concurrent routing changes use the
protocol's total stamp order. The same accepted history therefore selects the
same audience everywhere.

### Moving and deleting rows

An audience change is an ordinary host SQL update to the declared root audience
column. Coven previews the transaction and computes the inherited closure.

A move publishes only the destination representation:

- full destination row INSERTs;
- full private route INSERTs;
- the Store mirror change; and
- any Store ancestors required by the destination graph.

It does not publish a source-audience DELETE package. Receivers remove stale
source materialization because the winning Store mirror no longer names that
audience. This avoids requiring source recipients to receive a package after
their access has ended.

A delete publishes the audience row delete and a remove-wins Store mirror
delete. No later stale destination package can recreate it.

Local moves update local rows and durable outbound intent atomically. A move
from Local or an unavailable Circle requires every referenced blob's verified
plaintext before the host transaction commits.

## Circle authority

### Public control and private state

Store members can verify a Circle's public control history without learning its
private roster or metadata. Public values contain hashes and exact object refs,
not member identities or names.

Private values include:

- roster entries and heads;
- display metadata and heads;
- recipient access leaves;
- Circle packages, snapshots, acknowledgements, and bootstraps; and
- encrypted Circle keys.

Every signed object is bound to the Store root, Circle id, epoch, author
registration, canonical body hash, and exact predecessor or activation point
required by its role.

### Immutable streams

Roster, metadata, and control changes use immutable per-author streams with:

- checked successor arithmetic;
- exact predecessor refs;
- create-once successor slots;
- explicit causal dependencies; and
- deterministic resolution when concurrent valid successors exist.

A Store commit is the only activation authority. Merely uploading a control,
roster, metadata, access, package, or bootstrap object does not activate it.

The puller exact-loads every referenced object and verifies its canonical bytes,
hash, signature, author authority, predecessor, dependencies, and Store-commit
activation before using it.

### Control state

Circle control is a closed value:

```rust
enum CircleControl {
    ActiveEpoch(ActiveCircleEpoch),
    EpochClose(EpochClose),
    EpochCloseOutcome(EpochCloseOutcome),
    Deleted(DeletedCircle),
}
```

Derived local state is:

```rust
enum CircleState {
    Active,
    Inactive,
    Closing,
    RotationRequired,
    ControlConflict,
    Deleted,
}
```

These are Circle states, not protocol-engine variants.

### Roster and metadata

Roster membership names Store identities and roles. At minimum the roles are
Owner and Member. Display metadata contains the Circle name. The Owner signs
roster, metadata, access, close, resolution, and deletion transitions.

An Owner may sign a transition that removes its own membership because
authority is checked at the predecessor roster, not the result roster.

Concurrent valid roster or control successors are retained and surface
`ControlConflict`. No branch is silently chosen when doing so would choose
membership, keys, or deletion intent. The Owner resolves the conflict with a
signed transition that names the complete conflicting set and the chosen
successor state.

### Recipient access

Each Circle epoch has a random content key. Access is distributed through a
signed access root containing exact refs to recipient-sealed leaves.

Each leaf is bound to:

- Store root and Circle id;
- Circle epoch and key fingerprint;
- recipient identity through a pseudonymous recipient slot;
- an `Active` or `Inactive` disposition;
- exact roster/control authority; and
- an optional bootstrap ref when access becomes active.

The recipient decrypts only its own leaf. Store members can see access object
shape and timing but not the private roster contents.

The local keyring retains every Circle generation the identity legitimately
received. Rotation prevents access to future packages; it does not erase old
keys or history already held by a removed member.

## Lifecycle operations

### Durable operation journal

Every Circle command is a durable operation keyed by `CircleOperationId`:

```rust
enum CircleOperationIntent {
    Create(CreateCircleIntent),
    Rename(RenameCircleIntent),
    AddMember(AddCircleMemberIntent),
    RemoveMember(RemoveCircleMemberIntent),
    ResolveControl(ResolveCircleControlIntent),
    Delete(DeleteCircleIntent),
}

enum CircleOperationProgress {
    Ready(PreparedCircleTransition),
    WaitingForCloseResponses(EpochCloseProgress),
    Blocked(CircleOperationBlock),
}
```

The prepared transition owns every exact immutable object it creates. Remote
publication is idempotent. Local completion records activated state and clears
only the matching journal and candidate-owned files in one transaction.

Replacement or discard requires verified permanent nonactivation. It never
assumes that an unseen object failed to activate.

### Create

Creation prepares a random `CircleId`, first roster, metadata, key, access root,
Owner leaf, control, and activation references. All prerequisites are uploaded
and exact-read before one Store commit activates them. A partial upload remains
owned by the durable operation until activation or proven nonactivation.

### Rename

Rename creates one metadata successor and a control transition that references
it. The Circle id, roster, epoch, key, access, rows, and package history are
unchanged.

### Add or re-add a member

Adding access creates:

- a roster successor;
- a bootstrap image at an exact accepted Store frontier;
- an access leaf sealed to the recipient with the current Circle key and exact
  bootstrap ref;
- an access-root successor; and
- a control transition activated by one Store commit.

The bootstrap contains the Circle state needed at its coverage point and pins
every required Store ancestor, routing fact, control, key generation, and blob.
The member installs it atomically before applying later Circle packages.

The Owner publishes all pending Store writes before taking the bootstrap cut.
Every blob retained by the cut must already have an activated exact Circle
locator; member addition fails if any locator or remote object is absent. The
bootstrap does not regenerate or re-upload those blob bytes. Its activating
Store commit adds ownership of the exact existing objects.

The database image excludes `remote_objects`, blob-locator indexes, and retained
replay state. Recipient installation reconstructs the exact blob bindings and
ownership atomically from the signed bootstrap and its activating Store commit;
it never copies source-device ownership bookkeeping.

The successor control proves its author against the predecessor roster. Its
closed object graph therefore retains the exact predecessor heads as well as
the new roster frontier. Only founder preparation replaces provisional roster
coordinates after exact slots are allocated; later transitions never rewrite
historical grant coordinates.

Re-adding a former member is the same operation with a new active access leaf
and current bootstrap. Prior possession of an old key grants no current
authority.

### Store-member removal

Removing a Store member makes every affected Circle `RotationRequired`.
Publishing new Circle content is blocked until an Owner closes the old epoch and
activates a successor epoch without that identity. Store-visible commits and
unaffected Circles continue.

### Circle-member removal and epoch close

Member removal always closes the old epoch before activating future writes.
The Owner activates an `EpochClose` containing:

- the complete frozen old-epoch control state;
- an exact ref to a Circle-encrypted, Owner-signed removal intent;
- the frozen Store device-state ref;
- every remaining member's active device registration and create-once response
  slot;
- a provisional `CommitFrontier`; and
- one create-once outcome slot.

The encrypted intent names the exact predecessor roster, signed removal, and
resulting roster-state hash. Existing Circle members, including the member being
removed, can verify that the public participant set contains every active device
belonging to the resulting roster. Store members outside the Circle learn that
an epoch is closing and which device slots participate, but not which identity
is being removed.

Active remaining devices freeze old-epoch publication, apply every accepted
old-epoch commit they possess, and publish a signed response with their greatest
contiguous frontier. The Owner may sign a close-specific exclusion for an
unavailable device; exclusion forces that device to reset from the successor
bootstrap before it can acknowledge or write again.

The final cutoff is the component-wise maximum of the provisional frontier and
all accepted responses. A final outcome or cancellation competes at the one
outcome slot. The accepted final outcome activates:

- the roster without the removed member;
- a new Circle key generation;
- active access leaves for remaining members;
- an inactive leaf for the removed member;
- the exact old-epoch cutoff;
- a successor bootstrap; and
- the successor active control.

Packages beyond the accepted old-epoch cutoff are invalid. No later pass repairs
an incomplete close; every step is retained for idempotent retry or fails to its
initiator.

### Delete

Deletion is an Owner-signed terminal control transition. It prevents future
Circle packages, removes current materialization on receiving devices, and
retains the authority spine required to verify historical commits and exact
reclamation.

Deletion does not claim to erase copies already received by members.

## Store commits and audience packages

### One activation coordinate

Every accepted operation is activated by a direct Store commit coordinate:

```rust
struct StoreCommitCoord {
    stream_id: StoreDeviceStreamId,
    sequence: u64,
}

struct StoreCommitOrder {
    predecessor: Option<StoreCommitRef>,
    dependencies: CommitFrontier,
}
```

These are not wrappers around a removed protocol choice.

### Batch shape

One `StoreBatchCommit` may reference:

- an optional Store package;
- one Circle package for each touched Circle;
- Circle control or lifecycle objects;
- blob manifests; and
- exact ownership and cleanup manifests.

A Circle-only or control-only transaction does not synthesize an empty Store
package. Circle packages have no independent sequence, cursor, or activation
head: the enclosing Store commit coordinate orders and activates them.

Every package is encrypted for its audience, signed by the author, bound to the
schema-routing contract and audience key fingerprint, and addressed by an
immutable exact locator.

### Candidate ownership

Prepared commits and packages form a candidate family with a canonical signed
manifest of every exact object the candidate owns. Object keys include their
content hash so alternate candidates cannot collide.

A candidate may be replaced only when the current device-stream successor slot
contains a different verified winner, the author lost exact authority, or a
membership revocation proves it can no longer activate. These direct proof
forms contain no policy suffix or alternate engine case.

An accepted abandonment commit embeds the complete candidate bytes and exact
cleanup manifest. Cleanup exact-deletes only candidate-exclusive objects after
proving no surviving candidate or retained authority owns them. It deletes the
candidate commit last and verifies absence. Any failure retains the durable
operation and exact retry inputs.

Activated authority, shared live objects, roots, registrations, recovery facts,
snapshots, acknowledgements, bootstraps, and reclaim evidence are never treated
as candidate-exclusive merely because one candidate referenced them.

## Host transaction

One SQLite session captures all declared host tables and Coven metadata. Before
commit, Coven:

1. previews the host changeset;
1. validates row identities, old and new audiences, full inherited closures,
   and the final foreign-key component;
1. requires current verified active access for every destination Circle;
1. derives and writes private routes and Store mirror mutations;
1. materializes and verifies every moved blob source;
1. writes destination ciphertext to immutable durable spool files and syncs
   them;
1. records one durable write with its canonical changeset, audience partitions,
   old and new audiences, exact blob locators, spools, dependencies, and
   `WriteId`; and
1. commits application rows and all Coven metadata atomically.

If validation, spool creation, journal insertion, or SQLite commit fails, Coven
removes and directory-syncs only files created by that attempt. A durable write
never relies on downloading source plaintext later.

## Push

1. Refresh Store membership, device registration, Circle controls, access,
   keyrings, and accepted epoch cutoffs.
1. Load the oldest durable write without clearing it.
1. Revalidate its audience partitions, complete row closures, dependencies,
   authority, and destination fingerprints against refreshed signed state.
1. Verify every destination blob spool's exact size and hash.
1. Allocate and persist immutable package, blob, control, commit, and successor
   slots in the durable operation.
1. Upload prerequisites and exact-read their canonical bytes.
1. Publish the signed `StoreBatchCommit` and claim the prepared device-stream
   successor slot.
1. Run the same activation verifier used by pull.
1. Atomically record the accepted frontier and clear only the matching durable
   write and spool files.

If refreshed authority blocks the write, it remains durable with a typed
reason. Repackaging never deletes the previous candidate first. Separate host
transactions never combine.

## Pull

1. Load and verify Store membership and device-registration history needed to
   authorize authors.
1. Read device heads, exact-load dependencies, and build the dependency-ready
   commit queue.
1. Exact-load and verify every referenced Circle control, roster, metadata,
   access, bootstrap, snapshot, and lifecycle object.
1. Fetch the optional Store package and each Circle package for which the
   resulting control gives this identity active access.
1. Verify hashes before decryption, then verify signatures, schema contract,
   key fingerprints, audience, author authority, and epoch cutoff.
1. Verify every referenced blob's physical locator, ciphertext, plaintext size,
   and content hash before applying rows.
1. Apply control state, Store mirror, authenticated routes, eligible rows,
   pruning, blob refs, final-component validation, and frontier advancement in
   one SQLite transaction.
1. Queue Store and Circle acknowledgements for the resulting accepted frontier.

An inactive recipient applies Store-visible control and mirror state, prunes
that Circle's local rows, and does not fetch its private package. A missing,
corrupt, unauthorized, ambiguous, or foreign-key-invalid object aborts the
whole batch without row or frontier advancement.

## Deterministic row merge

The Store protocol preserves causality and converges deterministically:

- explicit dependencies apply before dependent commits;
- independent commits use the canonical total order where an order is needed;
- a late concurrent commit atomically replays retained accepted history before
  frontier advancement;
- different-column updates to one row are three-way merged;
- same-column conflicts use `_updated_at` ordering;
- audience transitions resolve through the Store mirror independently from
  ordinary row-content conflicts;
- deletes win over concurrent edits;
- foreign-key-dependent changes wait for their exact parent history; and
- non-foreign-key uniqueness or check-constraint conflicts reject the incoming
  changeset atomically.

Rejected constraint changes do not advance the local frontier. They surface a
typed deterministic conflict; Coven does not pretend that the offline host
transaction had global serializable admission.

## Acknowledgements

Each active device publishes:

- one Store acknowledgement covering its accepted Store frontier; and
- one Circle acknowledgement for every Circle whose private state it currently
  holds.

Circle acknowledgements are encrypted to the Circle and name exact Store
frontiers, Circle controls, epochs, snapshots, and blob coverage. An inactive
recipient cannot acknowledge private history it did not fetch.

Acknowledgement construction, publication, activation, retry, nonactivation,
cleanup, and durable completion are owned by the concrete Store. Shared code may
encode, sign, upload, exact-read, and store a closed acknowledgement value; it
does not decide coverage or authority.

## Snapshots, bootstrap, and restore

Snapshots are cut only at a fully applied Store-commit frontier.

A Store snapshot contains:

- Store rows;
- Store private routes;
- the opaque audience mirror;
- activated Circle control indexes and reachability proofs;
- the schema-routing contract hash; and
- the exact Store frontier and retained authority needed to verify it.

A Circle snapshot contains:

- rows whose winning mirror names that Circle;
- their private routes;
- the exact Circle control, epoch, key fingerprint, and Store frontier;
- required Store-parent dependencies; and
- the retained authority needed to verify the image.

Local rows never enter a snapshot.

Snapshot image bytes are uploaded and exact-read before signed metadata
activation. Each author uses an immutable per-audience snapshot stream with an
exact predecessor and create-once successor slot.

Restore stages Store and Circle images separately, verifies every byte and
authority chain, and keeps their coverage frontiers explicit. It installs all
selected images, replays the missing commits, runs audience filtering and final
foreign-key validation, then commits one final database and Store frontier
atomically. A partially installed image is never exposed as the current
database.

A Circle bootstrap uses the same verified image machinery but is pinned to one
recipient's access activation. It cannot be reclaimed until that recipient has
acknowledged a later sufficient Circle snapshot or lost authority under exact
signed evidence.

## Reclamation

Reclamation is audience-specific and exact. Eligible targets include:

- Store packages and snapshot images;
- Circle packages, snapshot images, and bootstrap images; and
- audience blob ciphertext no longer referenced by retained history.

The metadata and authority spine required to verify membership, registration,
control, access, activation, epoch cutoffs, candidate nonactivation, and reclaim
evidence is retained.

The Store computes eligibility from verified acknowledgements, snapshot
coverage, materialized history, active recipients, and exact ownership. It
produces a signed exact deletion plan. Physical deletion, readback absence, and
the terminal receipt complete before local retirement state commits.

There is no grace-period guess, tombstone sweep, cancel-and-recreate repair, or
later reconciliation. Failure leaves the exact durable plan available for an
idempotent initiating retry.

## Blobs

A blob follows the audience of the row that references it. Its immutable
locator binds:

- Store root and audience;
- uploader registration;
- logical content identity and audience fingerprint;
- physical object key;
- ciphertext size and hash; and
- plaintext size and hash.

`RowBlobRef` binds a locator to the exact table, row identity, column, audience,
and row stamp. Multi-part blobs include a complete group manifest with whole and
range hashes.

Moving a row creates a destination-audience ciphertext representation. Before
the host transaction commits, Coven must possess and verify the source
plaintext and durably sync the destination spool. Missing material fails with
`BlobMoveRequiresMaterialization`; later publication never downloads or
regenerates it.

Pull verifies every referenced immutable object before applying its row. Lazy
cache policy may discard verified plaintext after application, but never skips
verification.

Browsable storage exposes application-shaped objects and is incompatible with
scoped graphs. Circle-capable Stores use opaque immutable object storage.

## Application API

There is no protocol-mode parameter.

```rust
let coven = Coven::builder(config)
    .synced_tables(vec![
        SyncedTable::new("artists", RowIdentity::IndependentUuid),
        SyncedTable::new("albums", RowIdentity::IndependentUuid),
        SyncedTable::new("playlists", RowIdentity::IndependentUuid)
            .scoped_by("audience_id"),
        SyncedTable::new("playlist_tracks", RowIdentity::IndependentUuid)
            .inherits_audience_through("playlist_id")
            .carries_blob(audio_blob),
    ])
    .migrations(vec![Migration::sql(1, "initial", SCHEMA)])
    .open()?;
```

Host SQL remains ordinary SQL:

```rust
coven.sql(|tx| {
    tx.execute(
        "UPDATE playlists SET audience_id = ?1 WHERE id = ?2",
        (circle_id, playlist_id),
    )?;
    Ok(())
})?;
```

Circle commands return durable operation identities:

```rust
let operation = coven.circles().create("Family").await?;
coven.circles().rename(circle_id, "Household").await?;
coven.circles().add_member(circle_id, identity_id).await?;
coven.circles().remove_member(circle_id, identity_id).await?;
coven.circles().delete(circle_id).await?;
```

Required query and recovery surface:

- list Circles and derived `CircleState`;
- list effective members and roles when authorized;
- inspect a `CircleOperationId`, its durable progress, and typed block reason;
- retry an operation idempotently;
- replace or discard only with verified nonactivation;
- submit or inspect epoch-close responses;
- exclude an unavailable device for one close;
- finish or cancel an epoch close through its one outcome slot;
- inspect local, published, and acknowledged write state; and
- trigger the normal sync cycle after connectivity returns.

The same SQL API works online and offline. Sync status reports whether a durable
write is local, published, blocked by authority, rejected by a deterministic
constraint conflict, or acknowledged. The application does not use a different
write method while offline.

## Errors

Public errors must name the violated fact and carry stable identifiers needed
for display or retry. Required categories include:

- schema-routing contract mismatch;
- invalid independent row identity;
- invalid or cross-audience foreign key;
- nonexistent, inactive, closing, conflicted, rotation-required, or deleted
  Circle;
- missing or invalid recipient access;
- stale epoch or package beyond the accepted cutoff;
- malformed, forged, duplicate, partial, or orphan private route;
- routing-id mismatch;
- missing or corrupt package, snapshot, bootstrap, or blob;
- `BlobMoveRequiresMaterialization`;
- deterministic SQLite constraint conflict;
- candidate activation, nonactivation, ownership, or cleanup failure; and
- durable Circle operation blocked on signed authority or close responses.

No error exposes a removed engine, policy, global-head branch, or compatibility
case.

## Security and privacy limits

- Revocation prevents future access; it cannot make retained plaintext, keys,
  snapshots, or packages unseen.
- Adding a member grants the retained history needed to materialize current
  Circle state.
- The Store-visible mirror leaks pseudonymous row counts, routing changes, and
  timing.
- A Store member with the routing key may test guesses for predictable table and
  primary-key combinations.
- Store commits reveal addressed Circle ids, object timing, sizes, and hashes to
  Store members and the storage provider.
- Recipient access objects reveal their number, size, and update timing even
  when recipient identity is pseudonymous.
- User-supplied object storage can observe provider-level access and object
  metadata.

These are protocol properties and belong in public documentation.

## Implementation map

The concrete Store owns every workflow that interprets Store or Circle
authority:

- host capture and durable write planning;
- package and blob preparation;
- publication and device-stream activation;
- pull discovery, dependency readiness, authorization, and atomic application;
- Store membership, registration, join, exclusion, and recovery;
- Circle control, roster, metadata, access, epoch close, conflict resolution,
  and deletion;
- acknowledgement, snapshot, bootstrap, restore, and reclaim;
- candidate replacement, abandonment, ownership, and cleanup; and
- retries, terminal results, and durable completion.

Shared modules may own closed primitives whose meaning is independent of Store
state: canonical bytes, hashes, signatures, encryption, immutable transport,
SQLite row/image mechanics, and application-result values. They do not choose
authority, readiness, audience, coverage, activation, cleanup, or retry.

Store-specific database transitions live under the Store database owner and are
atomic. Cycle and application entry points call Store operations rather than
reading Store database or storage state directly. Do not add forwarding files
that call an old shared workflow; move the workflow and its tests to its owner.

## Verification

### Removed architecture

Search production code, tests, fixtures, documentation, and plans for every
removed protocol shape. Every match must be an explicit prohibition in a design
document or an unrelated ordinary-language use:

```sh
rg -i "serial|writepolicy|write_policy|coordination|conditional head|provisional branch" \
  crates coven-ffi coven-uniffi docs README.md plans
rg "StoreEngine|CycleEngine|AuthorizedCycleEngine|store_engine" crates
```

No parser, wire enum, schema column, internal table, database branch, storage
trait, API, fixture, or test may preserve it.

### Schema and routing

- canonical contract hashes change for every routing-relevant schema change and
  remain stable for irrelevant host-only changes;
- UUID and intentional shared-key validation use the production host and pull
  paths;
- forged, partial, duplicate, UPDATE, DELETE, missing, and orphan route rows are
  rejected before application;
- local capture, pull, snapshot, restore, and bootstrap derive identical routing
  identifiers;
- move/move, move/edit, delete/move, and Local transitions converge in every
  arrival order;
- final-component validation catches unchanged and receiver-only descendants;
- constraint failure rolls back rows, routing, blobs, controls, and frontier.

### Circle lifecycle

- create, rename, add, re-add, remove, Store-member removal, rotation, conflict
  resolution, close cancellation, device exclusion, and delete run through the
  production Store owner;
- concurrent control and roster successors retain all signed evidence and
  require explicit resolution where intent cannot be merged;
- old-epoch packages at the cutoff apply and packages beyond it fail;
- excluded devices must install the successor bootstrap before writing;
- access leaves cannot be replayed across recipients, Stores, Circles, epochs,
  controls, or bootstraps;
- every crash point resumes from the exact durable operation without guessing
  remote state.

### Packages and data

- Store-only, Circle-only, Local-only, multi-Circle, move, delete, and
  control-only transactions create exactly the required package set;
- packages visible without an activating Store commit are ignored;
- missing or corrupt referenced objects prevent row and frontier advancement;
- inactive recipients prune revoked Circle rows without fetching private
  packages;
- different-column, same-column, delete/edit, foreign-key, and uniqueness
  conflicts exercise the production apply path in both arrival orders;
- blob moves require verified source material and durable destination spools
  before host commit.

### Snapshot, bootstrap, acknowledgement, and reclaim

- mixed Store and Circle snapshot coverage restores only after dependencies are
  satisfied;
- bootstrap installation is atomic with its coverage and access state;
- acknowledgements never claim unfetched Circle history;
- reclaim deletes only exact objects excluded by verified live ownership and
  coverage;
- fault injection at every durable and remote boundary leaves either the prior
  state or the complete new state, with exact retry inputs;
- sabotage proves authority, routing, coverage, and ownership checks are
  load-bearing.

### Repository gates

- formatting;
- strict Clippy;
- focused and all-feature tests;
- repository hooks and CI;
- manual rule review and data-integrity audit;
- searches for stale paths, names, comments, fixtures, and documentation; and
- no compatibility, legacy, fallback, shim, or Coven-internal migration path.

## Documentation and dependent applications

When implementation and verification are complete:

- update the Coven website with the one-protocol replication guarantees,
  audience model, Circle lifecycle, offline status, privacy leakage, errors, and
  code examples;
- remove every coordinated-protocol reference from public documentation;
- update `~/dev/bae` to the released Coven API and verify its build and tests;
  and
- audit other in-scope applications for row-identity, audience, blob, and
  offline-status assumptions.

## Completion condition

Circles are complete when every synchronized row has one authenticated audience;
every Store and Circle operation uses the one concrete causal Store protocol;
membership, access, moves, conflicts, blobs, bootstrap, snapshots,
acknowledgements, restore, and reclaim preserve their invariants under offline
concurrency and failure; and production code, tests, fixtures, plans,
documentation, and dependent applications contain no removed protocol shape.
