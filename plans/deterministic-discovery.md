# Exact successor-locator storage

## Status

Implemented as the storage and discovery substrate for the one Store protocol.

Coven reaches every protocol object through a signed exact reference. Author
streams are immutable successor chains: each accepted node reserves the exact
slot for its successor. No authority, completeness, recovery, or reclamation
decision depends on provider filename enumeration.

There is no mutable shared Store head, provider-wide list-as-authority,
filename winner, alternate storage layout reader, or Coven-internal format
translation.

## Exact storage contract

```rust
enum PhysicalObjectLocator {
    LogicalKey,
    Opaque(String),
}

struct ObjectSlot {
    logical_key: String,
    physical: PhysicalObjectLocator,
}

struct ExactObjectRef {
    slot: ObjectSlot,
    stored_size: u64,
    stored_hash: ObjectHash,
}
```

`ExactSlotStorage` provides:

- slot allocation;
- create-once at an exact slot;
- exact whole and ranged reads;
- exact reads into a file;
- exact deletion; and
- the live provider binding used to verify namespace and principal authority.

Unique-key providers use the logical key as the physical locator. Google Drive
uses a caller-persisted generated file id because one folder may contain
several files with the same name.

Before remote mutation, the caller persists:

- the allocated slot;
- canonical semantic bytes;
- sealed stored bytes or a durable reproducible spool;
- stored size and hash; and
- every domain fact later signed into the parent reference.

Upload never draws new randomness. A retry reproduces the same bytes.

An occupied slot is exact-read. Matching bytes complete an idempotent retry;
different bytes report a typed collision. An ambiguous create or delete result
is settled by exact readback or exact absence. Transport failure is not evidence
that an object exists or does not exist.

Every read verifies:

- the logical and physical locator pair;
- provider namespace and corpus;
- provider metadata that binds the logical key;
- stored size and hash; and
- the domain-specific semantic reference.

A provider range response is transport only. Coven exposes plaintext range
bytes after verifying the complete immutable stored object and plaintext hash,
or from a local immutable object already verified against the same reference.

## Provider binding

The Store root carries one closed provider binding:

```rust
enum StoreProviderBinding {
    S3 {
        endpoint: S3EndpointBinding,
        region: String,
        bucket: String,
        key_prefix: Option<String>,
    },
    GoogleDrive { corpus: GoogleDriveCorpus },
    Dropbox { namespace_id: String },
    OneDrive { drive_id: String, folder_id: String },
    CloudKit {
        container_id: String,
        environment: CloudKitEnvironment,
        owner_name: String,
        zone_name: String,
    },
}
```

Each device registration binds the stable provider principal through which that
device allocated its pending slots. Open, join, restore, publication, and
administration derive the live binding again and require exact agreement.

Provider realization:

- S3 uses create-if-absent, exact GET/HEAD, ranged GET, and exact DELETE. AWS
  derives its account and stable IAM principal through STS. Custom S3 binds the
  canonical endpoint, region, bucket, prefix, and an access-key-id hash.
- Google Drive calls `files.generateIds`, persists the returned id, and creates
  at that id under the signed folder. Reads and deletes use the exact file id
  and verify parent, corpus, application properties, size, and hash. It never
  searches by name or chooses among duplicate names.
- Dropbox uses conflict-fail add inside the signed shared namespace and binds
  the stable account id. App Folder access is rejected.
- OneDrive uses conflict-fail create and exact drive, folder, user, and item
  identities.
- CloudKit uses deterministic record identities, exact change tags, the stable
  current-user record name, and a verified accepted read-write share for a
  distinct participant.

## Capability proof

Store creation runs the production adapter's exact-slot probe before installing
credentials or local Store state. The retained transcript includes:

- a fresh probe id and allocated slot;
- two competing create attempts with exactly one accepted value;
- exact whole and range read hashes;
- exact deletion and final absence; and
- a lost-response create settled by exact readback and cleanup.

The verifier recomputes deterministic payload and range hashes, requires one
normalized create winner, checks the accepted exact reference, and verifies
cleanup. A transport error cannot stand in for a create collision.

Cross-principal admission additionally proves:

- administrator create and joining-principal read;
- joining-principal create and administrator read;
- administrator deletion of the joining-principal object;
- final absence; and
- signatures from both exact device registrations over the attempt-bound
  transcript.

The transcript is bound to one Store, join attempt, administrator grant,
administrator registration, peer registration, and both live provider
bindings. It cannot be replayed for another device.

My Drive and a shared-credential S3 Store reject a different provider principal.
Shared Drive, Dropbox shared namespace, OneDrive shared folder, and CloudKit
share admission require their corresponding cross-principal proof.

## Root reference

Store creation allocates and durably records the root slot before upload.
Local configuration, invitation, and restore information carry:

```rust
struct StoreRootRef {
    store_root_id: ObjectHash,
    store_root_hash: ObjectHash,
    object: ExactObjectRef,
}
```

The acyclic creation order is:

1. prepare `StoreCreationDescriptor`, including root and founder-registration
   slots, provider binding, founder authority, schema routing, membership
   stream, recovery stream, and capability proof;
1. derive the stable Store root id;
1. sign, create, and exact-read the Store root;
1. sign, create, and exact-read the founder registration at its reserved slot;
1. publish and exact-read the founder membership entry and initial
   acknowledgement; and
1. install local Store state atomically only after every root fact verifies.

Missing or conflicting founder bytes leave the durable creation operation
retryable. They do not select another root or authority.

One Google Drive folder may contain several Coven roots and unrelated duplicate
filenames. Only the exact root file id in `StoreRootRef` selects this Store.
Every descendant binds the selected root hash.

## Stream activation

Streams are activated only by a verified root, membership grant, registration,
or Store commit:

```rust
enum StreamActivation {
    GrantAuthorized {
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        grant_id: MembershipGrantId,
        anchor: GrantStreamAnchor,
    },
    DeviceAuthorized {
        store_root_hash: ObjectHash,
        author_registration: StoreDeviceRegistrationRef,
        anchor: DeviceStreamAnchor,
    },
}
```

Grant-authorized domains include membership, Owner recovery, and Circle
control, roster, and metadata. Device-authorized domains include Store commits,
acknowledgements, and snapshots. The activation and domain derive one
`AuthorStreamId`; the host does not choose it.

Each activated anchor contains the first exact slot. Every accepted successor
node contains:

```rust
struct SuccessorLink {
    activation: StreamActivationId,
    predecessor: Option<ExactObjectRef>,
    next_slot: ObjectSlot,
}
```

The first node binds its activation and has no predecessor. Every later node
must occupy the prior node's signed `next_slot` and name the prior exact object.
Before signing a node, the writer allocates and persists its following slot.

A reader starts from the activated first slot or its last verified node,
exact-reads successive slots, and stops at the first absent reserved tail. It
records no progress over absence, corruption, wrong predecessor, wrong
activation, relocated bytes, or invalid signature. A later sync checks that
same tail again.

Two copies of one writer state race at the same reserved slot. Create-once
storage admits at most one node. Independent authors use distinct activated
streams and do not coordinate through shared provider state.

## Commit and object references

Logical keys describe the object's semantic domain. Exact references select the
physical bytes. Candidate objects include their candidate-family and content
hash so alternative candidates cannot collide.

Store commit coordinates are direct author-stream coordinates:

```rust
struct StoreCommitCoord {
    stream_id: AuthorStreamId,
    sequence: u64,
}
```

`StoreBatchCommitRef` composes the coordinate, signed commit hash, and exact
object reference. A `StoreDeviceHead` is the successor node that activates one
exact commit and reserves the next head slot.

Packages and commits uploaded without a verified head remain inert. A matching
hash at another physical locator is not activation authority.

## Candidate ownership

Every prepared commit carries a canonical manifest of candidate-exclusive
objects. The manifest must equal the direct graph in the signed body and remain
inside the derived candidate family.

Local ownership distinguishes:

- candidate commits;
- candidate-exclusive objects;
- retained authority; and
- objects shared by the accepted live set.

Pending, activated, and proved-nonactivating owners are disjoint. A shared
object cannot lose bytes or ownership while another pending or activated
candidate names it.

A losing candidate is removable only after exact nonactivation evidence. The
cleanup journal:

1. persists the proof and complete canonical target list;
1. deletes candidate-exclusive children before parents;
1. deletes the losing commit last;
1. verifies every target absent; and
1. removes local ownership in one final transaction.

Roots, registrations, accepted heads and commits, membership and device
authority, acknowledgements, snapshots, recovery facts, reclaim evidence, and
other activated authority are never candidate cleanup targets.

When all candidate owners of a retained-authority prerequisite are proved
nonactivating, its exact identity, bytes, and complete proof set become inert
protocol evidence. Shared blob or package bytes instead enter the normal
audience reclamation path. No namespace scan decides either transition.

## Device admission and recovery

Device registrations are immutable one-shot objects. Their device ids derive
from the Store root and exact founder, join, or recovery origin. The shared
identity key certifies a distinct device signing key; sibling devices cannot
sign each other's streams.

A later device joins through one durable attempt:

1. an Owner reserves attempt and outcome slots and issues the Store root and
   provider binding;
1. the joining device derives its live provider principal, registration,
   device id, registration slot, and initial stream slots;
1. the Owner publishes the signed attempt and provisional bootstrap;
1. the provider administrator grants access and completes any required
   cross-principal proof;
1. the joiner installs the bootstrap read-only, creates its inert registration
   and initial acknowledgement, and signs readiness;
1. the Owner publishes one create-once activated or cancelled outcome; and
1. a later Store commit activates the exact outcome before the joiner becomes
   writable.

Cancellation and activation compete at one exact outcome slot. Cancellation
cleanup names only attempt-owned inert slots, deletes them exactly, verifies
absence, and publishes a signed receipt. Identity membership and provider
access are separate authorities and are not silently revoked by cancelling a
device attempt.

An excluded device cannot publish another accepted successor. Recovery creates
a new device registration from an active Owner recovery stream; it never
rewrites an old registration. Every membership state and Store commit retains
the exact Owner recovery cursors needed to reject recovery beyond a later grant
retirement.

If a concurrent revocation invalidates already materialized recovery history,
one SQLite transaction rebuilds from retained verified history without that
suffix. The prior database remains current if any part fails.

## Snapshots, acknowledgements, and reclamation

Store and Circle snapshot streams use consecutive successor generations.
Metadata activates one exact content-addressed image only after predecessor,
coverage, control, registration, and image facts verify.

Acknowledgements name:

- the exact accepted Store frontier; and
- the exact snapshot reference adopted for that audience.

A frontier without the covering snapshot cannot authorize reclaim.

Reclaim evidence names one exact package, snapshot image, bootstrap image, or
stored blob. It composes:

- activation and successor evidence;
- snapshot coverage;
- active-device acknowledgements;
- recovery stability;
- audience live-set ownership; and
- current Owner and provider-administrator authority.

The immutable successor spine and every authority object needed to validate
later state remain reachable. Reclamation never uses list completeness, elapsed
time, or a repair sweep.

## Blob ownership

Blob identity remains independent of storage address. `BlobLocator` hashes
canonical logical blob facts. `StoredBlobRef` composes that locator with the
exact stored object.

The candidate's durable blob spool is checked before every create. A row binding
becomes visible only in the same SQLite transaction that verifies the accepted
blob and advances the Store frontier.

Cross-author reuse requires an already activated exact object. The peer reads it
through its admitted provider binding and verifies the original uploader
registration, accepted introduction, namespace, slot, size, stored hash,
plaintext size, and plaintext hash.

Prepared or uploaded objects retain their reproducible local bytes. Remote-only
state requires at least one verified activated owner. Obsolete locators enter
audience reclamation only with the complete live-set proof.

## Publication and response loss

Publication creates every immutable prerequisite, exact-reads it, creates the
signed package and commit, and then creates the author head at its reserved
successor slot. Local activation occurs only after the same verifier used by
pull reads the accepted head and commit.

A lost head-create response is resolved by exact-reading the reserved slot. No
position, journal, cache binding, acknowledgement, or row advances over absent,
corrupt, relocated, or unverified bytes.

There is no copy index, preparing marker, lease, mutable discovery tip, filename
winner, device registry, orphan sweep, or later reconciliation pass.

## Verification

- every provider allocates, create-writes, exact-reads, ranges, and
  exact-deletes one slot through its production adapter;
- provider and principal bindings reject wrong namespace, corpus, folder,
  drive, zone, account, credential, or share facts before Store installation;
- exact-slot and cross-principal transcripts reject altered facts, ambiguous
  races, missing signatures, replay, and incomplete cleanup;
- Google Drive uses generated file ids and performs no name-list authority
  request;
- two writers racing at one successor slot admit one value without overwrite;
- independent author streams publish while disconnected;
- traversal stops without advancing at absence or any exact-reference failure;
- packages and commits without an activating head remain inert;
- candidate replacement retains every object until exact nonactivation proof;
- cleanup fault injection preserves exact retry state and deletes the losing
  commit last;
- device join, cancellation, exclusion, Owner recovery, and grant retirement
  use exact activated authority;
- snapshots, acknowledgements, restore, and reclaim follow exact successor
  references without prefix completeness;
- blob publication, reuse, peer materialization, and reclaim preserve exact
  ownership atomically;
- opening rejects any changed Coven-internal table, column, constraint, index,
  `STRICT`, or `WITHOUT ROWID` declaration without rewriting it; and
- repository searches find no removed copy path, list-completeness authority,
  mutable discovery head, filename winner, device registry, alternate storage
  reader, or Coven-internal migration.
