# Capability-based Store authorization

## Goal

Make Store authorization a capability carried by opaque Rust types.

Only top-level `Store` construction, opening, joining, restoring, and command
boundaries may establish Store authority. Every lower operation receives an
already-established capability and derives narrower operation authority from
it. Lower modules cannot construct a verifier, reload the Store root or current
membership, assemble a local writer from raw rows, or call an alternate raw
authorization helper.

Rust privacy must enforce the boundary. A lower module that tries to import a
raw loader or construct a verifier must fail to compile.

## The distinction this design preserves

Establishing authority and verifying a referenced object are different work.

Establishing authority binds one operation to:

- one exact verified `StoreProtocolRoot`;
- one retained commit-history verifier and its object cache;
- one owner-anchored current `MembershipChain`;
- one local database and storage namespace; and
- when the operation writes, one activated local device registration and its
  derived device signer.

That baseline is established once and flows down.

Input-specific verification remains at the point where the input acquires
meaning. Pull still verifies a discovered head, publication still exact-reads
the accepted commit, Circle activation still verifies its control graph, and
reclaim still verifies its evidence. Those checks use the retained authority
capability. They do not create another root, history, membership, or writer
authority.

## Target capability model

```rust
pub(crate) struct Store {
    database: StoreDatabase,
    storage: Arc<CloudSyncStorage>,
    store_root: StoreRootRef,
}

pub(crate) struct AuthorizedStore<'storage> {
    database: StoreDatabase,
    authority: StoreAuthoritySession<'storage>,
}

struct StoreAuthoritySession<'storage> {
    history: MergeHistoryVerifier<'storage>,
    membership: MembershipChain,
}

pub(super) struct AuthorizedStoreOperation<'operation, 'storage> {
    database: &'operation StoreDatabase,
    authority: &'operation mut StoreAuthoritySession<'storage>,
}

pub(super) struct AuthorizedWriterOperation<'operation, 'storage> {
    store: AuthorizedStoreOperation<'operation, 'storage>,
    writer: LocalStoreWriter,
}

pub(super) struct AuthorizedOwnerOperation<'operation, 'storage> {
    writer: AuthorizedWriterOperation<'operation, 'storage>,
    owner_grant: MembershipGrantId,
}

pub(super) struct AuthorizedProviderAdminOperation<'operation, 'storage> {
    writer: AuthorizedWriterOperation<'operation, 'storage>,
    administrator_grant: ProviderAdminGrantId,
}

struct LocalStoreWriter {
    registration_ref: StoreDeviceRegistrationRef,
    registration: StoreDeviceRegistration,
    device_signer: UserKeypair,
}
```

The capability names and ownership relationships are:

- `Store` owns the pinned unverified local handle.
- `AuthorizedStore` owns the verified root, retained history session, and
  current membership.
- an operation borrows that same session rather than reconstructing it;
- a write operation additionally owns the one verified local writer it needs;
- an Owner or provider-administrator operation additionally carries the exact
  active grant that authorizes that role;
- Owner, provider-administrator, joiner, recovery, Circle, snapshot, reclaim,
  and pull operations derive their input-specific decisions from these
  capabilities.

Capabilities expose domain operations and closed verified results. Their
fields, constructors, raw verifier access, and assembly helpers are private.

## Top-level construction boundaries

The allowed constructors are Store-owned operations:

- `Store::create` publishes and verifies a new root, installs founder
  authority, and returns an initialized `Store`;
- `Store::open` verifies the expected root and returns an initialized `Store`;
- `Store::authorize` verifies the pinned root once, opens one history session,
  loads one owner-anchored current membership chain, and returns
  `AuthorizedStore`;
- `AuthorizedStore::authorize_writer` verifies the activated local
  registration against the retained root and history, derives the device
  signer, and returns `AuthorizedWriterOperation`;
- Store-associated join and restore boundaries verify bootstrap authority
  before a runnable `Store` exists and return closed bootstrap capabilities or
  an initialized `Store`.

Pre-Store joining and restore are not exceptions that expose raw constructors.
They enter associated `Store` boundary functions whose result proves the exact
authority needed by the next operation.

## Rust module and visibility shape

Constructor privacy must follow the module tree because Rust restricted
visibility can name ancestors, not arbitrary sibling modules.

Store authority construction and every command implementation that must invoke
it therefore live under one private owner:

```text
sync/store/
  owner.rs                 Store and capability types; declares owner children
  owner/
    verification.rs        private root and history verifier implementation
    membership.rs          private current-membership construction
    writer.rs              private activated-writer construction
    cycle.rs               authorized cycle operations
    circles.rs             authorized Circle command operations
    joining.rs             initialized and pre-Store join boundaries
    restore.rs             initialized and pre-Store restore boundaries
    snapshots.rs           authorized snapshot operations
    reclaim.rs             authorized reclaim operations
    membership_mutation.rs authorized membership operations
    device_exclusion.rs    authorized exclusion operations
    owner_promotion.rs     authorized promotion operations
    acknowledgements.rs    authorized acknowledgement operations
    publication.rs         authorized write publication operations
    pull.rs                authorized pull operation
```

Rust children may access private items in an ancestor, while sibling modules
may not access each other's private items. Store command implementations that
must establish or narrow authority are therefore children of `owner`.
Authority-interpreting subsystem functions are methods on a capability, or
private helpers called by those methods. Protocol algorithms outside `owner`
receive the opaque capability or a closed verified input. They cannot import a
constructor merely because they live below `sync::store`.

The following implementation details are not re-exported from
`sync::store`, `sync::store::pull`, `sync::store::membership`, or
`sync::store_objects`:

- `MergeHistoryVerifier::new` and every `from_*` constructor;
- `StoreCommitVerifier::new` and every `from_*` constructor;
- raw `StoreProtocolRoot` loaders used to establish Store authority;
- raw current-membership and owner-anchor loaders;
- raw founder and registration loaders used to establish the session;
- `load_local_store_authority`;
- free `*_with_history` authorization entry points;
- fields that expose `MergeHistoryVerifier`, `StoreCommitVerifier`,
  `MembershipChain`, or local writer components;
- test bypass constructors such as `Store::authorize_borrowed`.

Low-level parsing and signature routines remain independently testable inside
their owning private modules. Code outside those modules tests through the real
Store boundary.

## Operation APIs

Application and cycle code call domain operations:

```rust
let store = Store::load(database, storage).await?;
store.rename_circle(device_id, circle_id, name, identity).await?;
```

The command, implemented inside the private `owner` subtree, establishes
capabilities at its boundary:

```rust
impl Store {
    async fn rename_circle(...) -> Result<(), CircleOperationError> {
        let mut authorized = self.authorize().await?;
        let mut operation = authorized.authorize_writer(device_id, identity).await?;
        operation.rename_circle(circle_id, name).await
    }
}
```

Lower work is capability-owned:

```rust
impl AuthorizedWriterOperation<'_, '_> {
    async fn rename_circle(...) -> Result<(), CircleOperationError> {
        let prepared = self.prepare_circle_rename(...).await?;
        self.publish_circle_operation(prepared).await
    }
}
```

There is no lower signature accepting a loose combination such as:

```rust
database: &StoreDatabase,
storage: &dyn SyncStorage,
root: &StoreRootRef,
history_verifier: &mut MergeHistoryVerifier,
membership: &MembershipChain,
registration: &StoreDeviceRegistration,
```

That parameter cluster is the capability. Keeping the values loose permits
mismatched roots, duplicate sessions, and ad-hoc reloads.

## Conversion scope

Every workflow that interprets Store authority uses the capability path:

- Store creation, opening, initialization, and authorization;
- host-write planning, preparation, publication, collision handling, and
  abandonment;
- pull discovery, ancestry, readiness, materialization, retained replay, and
  terminal cleanup;
- membership listing, invitation, removal, conflict resolution, and key
  rotation;
- device registration, joining, exclusion, cleanup, and Owner recovery;
- Owner promotion request, acceptance, and finalization;
- Circle command preparation, publication, close response handling, retry,
  discard, activation, and recovery;
- snapshot authoring, verification, restore, acknowledgement, and reclaim;
- package and blob authority decisions.

Pure canonical encoding, hashing, cryptography, exact-object transport, SQLite
row mechanics, and closed input-specific verification may remain separate.
They accept closed values or borrow a capability; they never establish Store
authority.

## Conversion method

For each workflow:

1. Identify the top-level `Store` operation and the baseline authority it
   requires.
2. Construct `AuthorizedStore`, `AuthorizedStoreOperation`, or
   `AuthorizedWriterOperation` at that boundary.
3. Change every nested function to a method on the appropriate capability or a
   private helper that receives the capability.
4. Remove loose database, storage, root, membership, registration, and verifier
   parameters that duplicate capability fields.
5. Delete the raw wrapper and its re-export after its final caller moves.
6. Search the complete call chain for another constructor or reload.
7. Compile immediately so privacy and borrowing errors reveal incomplete
   conversions.
8. Run the workflow's failure, restart, and sabotage tests before committing
   the boundary.

No forwarding facade remains after a move. The implementation moves to the
capability owner or becomes a private helper behind its method.

## Test shape

Integration and subsystem tests construct the production `Store` and call its
domain operations. Fixtures hold a `Store` or provide a helper that returns
one; they do not assemble authority from `Database`, `SyncStorage`, and
`StoreRootRef`.

Tests outside the private owner do not request an authorization capability:

```rust
let store = fixture.store().await;
store
    .rename_circle(
        &fixture.device_id,
        fixture.circle_id,
        "renamed",
        &fixture.identity,
    )
    .await
    .expect("rename Circle");
```

Tests for a capability constructor or a private protocol algorithm live inside
the private owner module that owns it. No crate-wide `test-utils` feature
exposes raw authorization constructors. A purpose-specific test operation on
`Store` may expose a closed result under `cfg(test)` when an integration test
must exercise a private algorithm; it cannot return a verifier, membership
chain, raw registration, or capability field.

## Atomicity and freshness

One retained capability is a coherent authority snapshot for one operation.
The operation does not silently refresh one component while retaining another.

An operation that requires refreshed remote authority establishes a new
`AuthorizedStore` at its top-level retry boundary. A retry does not mutate the
middle of an existing capability into a different authority snapshot.

Pull is the operation that legitimately advances materialized history and
membership. It updates the owned `AuthorizedStore` session from the exact
history it verified and applied; it does not reload the same state through a
second traversal.

Failure leaves the prior durable state or commits the complete operation.
Capability conversion must not introduce a later reconciliation path, skipped
verification, or default authority.

## Commit boundaries

Commit each cohesive ownership boundary after:

- every dependent production caller and test uses the capability;
- the superseded constructor, loader, wrapper, and re-export are deleted;
- focused tests and strict Clippy pass; and
- searches find no remaining bypass in that workflow.

Rebase onto `origin/main`, fast-forward `main`, and push after each commit.

## Compiler-enforced acceptance criteria

Production code outside the private Store authority owner contains no direct
construction or baseline reload through:

```text
MergeHistoryVerifier::new
MergeHistoryVerifier::from_*
StoreCommitVerifier::new
StoreCommitVerifier::from_*
load_store_protocol_root
load_cycle_membership
load_and_persist_owner_anchor
load_local_store_authority
authorize_borrowed
```

The raw functions and constructors are private and not re-exported. The
compiler rejects an import or call from Circle, membership, snapshot, reclaim,
device-join, Owner-promotion, cycle, application, or external test code.

Production lower-layer functions do not accept loose verifier/root/membership
clusters. They accept a capability or closed verified input.

Repository searches must also show:

- no second authority constructor inside one top-level operation call chain;
- no baseline membership reload after `AuthorizedStore` exists;
- no root reload after the capability contains the verified root;
- no local writer reload after `AuthorizedWriterOperation` exists;
- no raw authorization bypass under `feature = "test-utils"`;
- no forwarding shim retaining an old entry point.

## Behavioral verification

Run the relevant focused suites after each converted workflow, then run:

```sh
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
scripts/check-lib-only.sh
cargo test --all-features
```

Retain and run sabotage coverage proving:

- a mismatched Store root is rejected at capability construction;
- a forged or inactive local registration cannot produce writer authority;
- revoked membership cannot produce the required operation authority;
- input-specific forged commits, heads, controls, snapshots, and reclaim
  evidence still fail at their consumption points;
- one operation reuses its verifier cache instead of fetching the same
  authority objects again;
- failure and retry do not mix two authority snapshots.

## Completion condition

The transition is complete when every Store workflow enters through a
Store-owned capability constructor, every lower authority decision consumes
that capability, raw construction and baseline-loading APIs are inaccessible,
tests use the production boundary, repository searches find no bypass or
repeat, and the complete verification suite passes.
