# Capability-based Store authorization

## Goal

Make every Store workflow executable only through the capability that owns its
verified root, identity, device, membership, and persistence context. Move
workflow bodies onto those capabilities, hide raw constructors, loaders, and
accessors, then let compiler failures identify every caller that must be
converted.

Only top-level `Store` construction, opening, joining, restoring, and command
boundaries may establish Store authority. Every lower operation receives an
already-established capability and derives narrower operation authority from
it. Lower modules cannot construct a verifier, reload the Store root or current
membership, assemble a local writer from raw rows, or call an alternate raw
authorization helper.

Rust privacy must enforce the boundary. A lower module that tries to import a
raw loader or construct a verifier must fail to compile.

Restricted-path visibility (`pub(in …)`) is forbidden. It lets a child grant
access to distant modules instead of placing the capability at the shared
owner. Repository validation rejects the syntax. Items shared with a direct
parent use `pub(super)`; items needed by distant siblings move to their common
owner, and capability operations remain private methods available to that
owner's descendants.

## Binding inventory

Each line records the current home and the complete target method surface for
one owner:

- `Store` — currently in `owner.rs`, with pull and restore entry points
  elsewhere; owns `authorize_history()`, `authorize()`, `authorize_writer()`,
  `pull()`, and `restore()`.
- `AuthorizedStore` — currently in `owner.rs`, while blob and Circle-read
  workflows remain elsewhere; owns `read_blob()`, `open_blob_stream()`,
  `materialize_row_blob()`, `pin_blob()`, and `load_circle_access()`.
- `AuthorizedStoreHistory` — currently in `owner.rs`, while its workflows are
  spread through `abandonment.rs`, `membership/`, and `pull/`; owns
  `load_current_membership()`, `load_membership_at()`,
  `load_retained_outbound_authorization()`,
  `apply_terminal_nonactivation()`, and
  `abandon_excluded_merge_candidate()`.
- `AuthorizedWriterOperation` — currently in `owner/writer.rs`, while its
  workflows are spread through `operations.rs`, `acknowledgements.rs`,
  `snapshot.rs`, and `reclaim/`; owns `prepare_plan()`,
  `prepare_candidate()`, `publish_prepared()`,
  `drain_prepared_store_writes()`, `stage_and_publish_ack()`, and
  `publish_due_snapshots()`.
- `AuthorizedCircleOperation` — not yet represented by a type; its workflow is
  spread through `owner/circles/` and `circle_controls/`; owns
  `resolve_local_access()`, `load_activations()`, `prepare_command()`,
  `publish()`, and `cleanup_candidate()`.
- `AuthorizedDeviceExclusion` — represented in `device_exclusion/mod.rs`,
  derived from `AuthorizedWriterOperation`, and owns `propose()`, `resume()`,
  `prepare_outcome()`, `publish_outcome()`, `publish_candidate()`,
  `replace_candidate()`, and `complete()`.
- `AuthorizedReclaim` — not yet represented by a type; its workflow is in
  `reclaim/mod.rs`; owns `prepare_authorizations()`, `resume()`,
  `verify_authorization()`, `delete_target()`, `prepare_receipt()`, and
  `finish()`.
- `AuthorizedJoin` — represented under `owner/device_join/` and derived from
  `AuthorizedWriterOperation`; its remaining workflow bodies are spread through
  the same subtree; owns
  `prepare_access_request()`, `begin()`, `approve()`, `cancel()`,
  `finalize()`, `drive()`, and `cleanup()`.
- `AuthorizedOwnerPromotion` — not yet represented by a type; its workflow is
  in `owner_promotion/`; owns `load_membership()`, `accept()`, `finalize()`,
  `resume()`, and `cleanup()`.
- `MergeHistoryVerifier` — currently constructible throughout
  `sync/store/pull/history.rs` callers; becomes a private history-capability
  implementation with `load_ref()`, `verify_refs()`,
  `verify_owner_conflict_acceptance()`, and
  `verify_terminal_nonactivation()`.
- `StoreCommitVerifier` — currently constructible throughout Store code from
  `sync/store/pull/ancestry.rs`; becomes a private history implementation with
  `authenticate_bytes()` and `load_ref()`.
- `StoreDatabase` — currently exposes construction and raw SQLite through
  `sync/store/database.rs`; retains only record-specific persistence methods.

The first relocation pass moves each workflow body to this owner and deletes
the free entry point. It does not preserve forwarding functions. Constructor
and accessor privacy is then tightened before callers are converted, making
every remaining bypass a compiler error.

## Relocation ledger

This ledger is the source-first pass. Each listed function currently receives
state already held by its target owner. Its body moves beside that owner before
callers are repaired.

- `pull::prepare_merge_history_successor(database, root, ...)` moves to
  `AuthorizedHistoryOperation::prepare_merge_history_successor(...)`; the
  database and verified root disappear from its arguments.
- `pull::prepare_merge_snapshot_history_summary(database, root, ...)` moves to
  `AuthorizedHistoryOperation::prepare_merge_snapshot_history_summary(...)`.
- `pull::load_retained_merge_outbound_authorization(database, storage, root, ...)` and `pull::load_merge_conflict_resolution_authorization(database, storage, root, ...)` moved onto `AuthorizedHistoryOperation`; the temporary
  retained-authority child type was removed because it carried no additional
  proof and forced restricted-path visibility.
- `store::blob::{read_blob, open_blob_stream, materialize_row_blob, pin}`
  move to `AuthorizedStore`; their database, storage, and root arguments
  disappear. `blob::cache::verify_blob_plaintext` remains a cache primitive
  because it consumes an already-authorized exact blob reference and resolved
  storage protection.
- `operations::{prepare_plan, prepare_candidate, prepare_candidate_borrowed, publish_prepared, upload_commit}` move to `AuthorizedWriterOperation`.
- `writer::preparation::prepare_store_write` and writer publication functions
  move to `AuthorizedWriterOperation`; nested preparation helpers receive the
  writer capability only when they interpret its authority.
- acknowledgement staging, publication, draining, and nonactivation cleanup
  move to `AuthorizedWriterOperation`; exact acknowledgement parsing remains a
  closed verification helper.
- snapshot authoring, publication, stability decisions, and retained-history
  preparation move to `AuthorizedWriterOperation`; snapshot image encoding and
  exact-object opening remain closed helpers.
- reclaim preparation, verification, deletion, receipt publication, and
  completion move to `AuthorizedReclaim`, derived from
  `AuthorizedWriterOperation`.
- Circle command preparation, publication, retry, discard, activation, close,
  and recovery move to `AuthorizedCircleOperation`, derived from
  `AuthorizedWriterOperation` or `AuthorizedStore` according to whether they
  write.
- membership invitation, removal, conflict resolution, and rotation move to
  `AuthorizedOwnerOperation`; exact chain graph parsing remains private history
  machinery.
- device exclusion proposal, resumption, outcome publication, replacement, and
  completion move to `AuthorizedDeviceExclusion`.
- device join begin, approval, cancellation, finalization, drive, and cleanup
  move to `AuthorizedJoin`; `begin()` now owns member eligibility, provider
  administrator resolution, signing, slot allocation, and initial Store-journal
  persistence. The initial journal value is constructed through a closed
  `DeviceJoinJournalRecord::owner_offered` constructor. Pre-Store bootstrap
  enters through an associated `Store` constructor.
- Owner promotion membership loading, acceptance, finalization, resumption, and
  cleanup move to `AuthorizedOwnerPromotion`.
- `protocol_root::{create_store, open_store}` become private bodies of
  `Store::create` and `Store::open`; descriptor encoding and exact-object
  publication remain private protocol-root helpers.
- `cycle::run_single_sync_cycle` becomes a `Store` operation which derives and
  reuses one authorized session across pull, write, acknowledgement, snapshot,
  and reclaim work.

Functions remain dependency-taking helpers only when their inputs are already
closed values and they do not establish, reload, choose, or interpret Store
authority. This includes canonical encoding, hashing, signature checks,
exact-object transport, blob-cache materialization after protection is
resolved, and SQLite row operations.

## Relocation stack

Relocation proceeds depth first. Encountering an unowned workflow suspends its
caller until the nested workflow has moved to its owner. Caller conversion
begins only after these ownership moves are complete.

The required stack order is:

- Store authorization composition retains one owning
  `AuthorizedStoreHistory` containing the Store database and history verifier;
  `AuthorizedStore`, writer operations, and narrower operations borrow that
  retained capability instead of reconstructing `AuthorizedHistoryOperation`
  from its fields;
  - delete `AuthorizedHistoryOperation::new`;
  - make the retained history fields private;
  - delete `MergeHistoryVerifier::commit_verifier`,
    `MergeHistoryVerifier::commit_verifier_ref`, and
    `MergeHistoryVerifier::verification_parts`; callers request a named
    history operation and cannot extract the contained commit authority;
  - bind commit authentication, registration loading, exact announcement-head
    verification, acknowledgement proof loading, device-state verification,
    Circle activation, snapshot evidence, and terminal nonactivation as
    `MergeHistoryVerifier` or `AuthorizedStoreHistory` operations;
  - make Circle, join, reclaim, snapshot, acknowledgement, pull, and other
    narrower Store operations borrow the retained history capability;
  - move each database, verifier, root, membership, identity, and writer
    reach-through to an operation on its owning capability so narrower code
    cannot obtain or reconstruct raw authority;
  - eliminate every `super::…::pull` reach-through from narrower operations;
    history-dependent behavior becomes an `AuthorizedStoreHistory` operation,
    while closed parsing and verification stays private inside the history
    implementation;
  - move `sync::service::prepare_store_payload` beneath
    `AuthorizedWriterOperation`; Store payload preparation must use the bound
    writer and cannot be reached through a generic service namespace;
  - move resolved-membership gating beneath the retained Store capability;
    narrower operations cannot pass `store.membership` to
    `membership::require_resolved_membership` or otherwise re-establish that
    the retained membership is usable;
  - `AuthorizedCircleOperation` retains the complete `AuthorizedStore` instead
    of separately borrowing its history, membership, and identity; Circle
    workflows obtain resolved membership only through the Store capability;
  - move exact membership-chain traversal, activation validation, projection,
    owner-anchor persistence, and keyring authority selection behind
    `AuthorizedStoreHistory`, `AuthorizedStore`, or the pre-Store join
    capability; delete their loose verifier, database, storage, root,
    membership, and identity entry points;
  - reduce `sync::store::membership` to membership-domain values and closed
    algorithms:
    - authorization refresh belongs to `AuthorizedWriterOperation`;
    - membership cursor persistence belongs to `StoreDatabase`, and its
      test-only forwarding wrappers are deleted when their callers move;
    - Store-history error translation belongs beside the private history
      verifier rather than making membership depend on `owner::pull`;
    - keyring-authority selection belongs to the Store, writer, or pre-Store
      join capability that proves the selected references;
    - member listing and conflict projection may remain pure transformations
      over an already-authorized `MembershipChain`;
  - search every narrower operation for direct capability construction and
    delete every alternate assembly path;
- immediate activation is bound beneath
  `AuthorizedWriterOperation`:
  - `activate_store_operation_commit` moved to
    `AuthorizedWriterOperation::activate`;
  - `publish_prepared` and its authority-bearing publication body moved to
    `AuthorizedWriterOperation`;
  - the displaced free activation and publication entry points are deleted;
- `AuthorizedJoin::abandon` owns the complete owner-side abandonment workflow,
  reusing its writer registration, signer, membership, journal, and writer
  activation capability;
- `AuthorizedJoin::accept_registration` owns owner-side registration
  acceptance, using the retained root, current Owner authority, resolved
  provider administrator, writer signer, journal, and activation capability;
- `AuthorizedJoin::cancel` owns owner-side cancellation, verifies its attempt
  and outcome through the retained history verifier, and publishes through the
  writer capability;
- `AuthorizedJoin::finalize` and `AuthorizedJoin::complete_cleanup` own the
  remaining Owner-side activation and cleanup transitions; the Store boundary
  supplies no separate identity;
- provider-administrator continuations derive
  `AuthorizedProviderAdministratorJoin`, which retains the exact active
  provider-admin grants whose administrator is the local writer;
- while binding provider challenge publication,
  `provider::publish_cross_principal_challenge` moves beneath
  `AuthorizedProviderAdministratorJoin` because it consumes Store history and
  the provider publication journal;
- while binding provider cancellation, owner discovery for a join attempt
  moves to `StoreCommitVerifier::load_device_join_attempt_and_owner`; callers
  cannot decode an unverified attempt and assemble its registration proof;
- provider and joiner write revocation are signed by
  `AuthorizedProviderAdministratorJoin`, which retains the active executor
  grant and local writer; replacement terminal insertion moves to the bound
  Store join journal;
- provider access authorization, challenge publication, response completion,
  cancellation, and both producer-revocation paths are bound to
  `AuthorizedProviderAdministratorJoin`; none accepts a separate identity,
  root, membership, database, or history verifier;
- Owner cleanup receipt preparation and activation move to `AuthorizedJoin`;
  terminal verification uses its Store database, and publication uses its
  retained writer;
- exact-slot observation and delete-then-confirm-absent move to
  `ExactSlotStorage`; device-join and provider-probe callers use that single
  storage boundary, and their duplicate free helpers are deleted;
- `observe_device_join_abandonment` is bound to
  `PendingDeviceJoinAuthority::observe_abandonment`;
- `materialize_joined_store_activation` is bound to
  `Store::materialize_joined_store_activation`;
- both public free-function re-exports are deleted;
- return to `AuthorizedJoin::abandon`:
  - use the bound Store journal for reads and transitions;
  - use its existing writer registration, device identifier, identity, and
    signer instead of reloading them;
  - use writer-owned operation preparation and activation;
- bind the remaining owner-side join continuations to `AuthorizedJoin`;
- bind provider-administrator and joiner continuations to their corresponding
  capabilities;
- bind the transport driver to those role capabilities rather than calling
  free Store workflows.

Completed nested bindings remain recorded here because later relocation must
not recreate their free forms:

- `Store::create` owns root-creation sequencing, while `Store::open` and
  `Store::load` share the private `Store::open_protocol_root` operation;
- the `StoreCreation` and `StoreOpening` operation wrappers are deleted, and
  `protocol_root` retains only the closed algorithms those Store operations
  invoke;
- `create_exact_object` and `delete_exact_object` are deleted as forwarding
  wrappers over `SyncStorage`, and `load_exact_object` is private;
- raw Store-root loading and `StoreCommitVerifier::new` are deleted;
- `StoreCommitVerifier` owns founder, registration, acknowledgement, recovery,
  join, exclusion, reclaim, Store-package, and commit verification;
- `CirclePackageAccess` owns Circle-package decryption, its epoch key,
  fingerprint, authorized writers, and Circle identifier;
- `store_objects` retains only generic byte checks and exact membership-object
  algorithms whose supplied inputs contain every authority decision;
- initial Store-journal creation is `StoreJoinJournal::begin`;
- Store-journal lookup is `StoreJoinJournal::load`;
- Store-journal compare-and-swap advancement is
  `StoreJoinJournal::advance`;
- an owner-offered initial value is
  `DeviceJoinJournalRecord::owner_offered`;
- Store operation planning is
  `AuthorizedWriterOperation::prepare_plan` and reuses the writer capability's
  root, registration, device signer, identity, and history verifier.
- the generic `sync::service` namespace is deleted: payload preparation,
  cleanup binding, and write-authority resolution are private writer
  preparation details, while applying a durable deferred local-blob
  disposition is owned by the sync cycle that drains it;
- exact Store-announcement traversal is
  `StoreCommitVerifier::exact_next_announcement_slot`; its traversal helper and
  path type are private beside the verifier, and the free announcement module
  is deleted;
- merge-history authority verification is
  `MergeHistoryVerifier::verify_merge_history_authority`; callers receive its
  closed device-state and membership result rather than invoking a snapshot
  helper;
- device-join attempt loading and history verification are
  `MergeHistoryVerifier::load_verified_device_join_attempt` and
  `MergeHistoryVerifier::verify_device_join_attempt_evidence`; the duplicate
  free loaders and re-exports are deleted;
- pre-publication exclusion rejection is
  `AuthorizedWriterOperation::reject_excluded_merge_candidate`, where the
  candidate is checked against the writer operation's retained database and
  Store root.
- terminal candidate-head verification and author-exclusion proof construction
  are `StoreCommitVerifier` operations; membership-revocation proof
  construction is a `MergeHistoryVerifier` operation; retained-database lookup
  for an author-exclusion proof is an `AuthorizedHistoryOperation`; the free
  `terminal_authority` module is deleted.
- current membership loading and owner-anchor installation are
  `AuthorizedStoreHistory` operations; membership cursor reads and monotonic
  writes are `StoreDatabase` persistence operations, and the raw cycle
  membership loader is deleted.
- per-cycle wrapped-key authority refresh is an
  `AuthorizedWriterOperation::refresh_authorization_state` body; the
  membership-module implementation and its error surface are deleted.
- pre-Store snapshot restore enters through
  `SnapshotBootstrapAuthority::open(...).select(...)`; the selected bootstrap
  retains the same verified history session through founder-registration
  loading and image installation, so restore does not reconstruct a commit
  verifier from a verified root.
- `load_local_store_authority` and its tuple of root, registration reference,
  registration, and device signer are deleted. Store tests use closed
  Store-owned operations for retained-outbound authorization, promotion
  targets, head signing, and snapshot verification.
- Circle snapshot publication is an `AuthorizedWriterOperation` method. It
  uses the retained root, activated registration, device signer, database, and
  storage rather than reloading writer authority during publication.
- Store-write preparation carries the writer capability's retained exact root
  into the database transaction. The transaction verifies the activated
  registration bytes against the already-verified commit author instead of
  reloading the Store root and reconstructing registration authority.
- Pull is an `AuthorizedStore` operation, not a writer operation. Pull-only
  callers therefore retain Store history, membership, identity, database, and
  storage authority without draining registration state or constructing a
  local writer. Test Stores retain their founder device independently from
  arbitrary producer labels, so inspecting founder authority cannot create and
  activate another device.

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

`PendingStoreJoin` is the explicit pre-Store join capability. It retains the
verified Store root and history, Store database, current owner-anchored
membership, durable offer and exact attempt identifier, storage, pending
journal, and joining identity. The journal and identity are owned values, not
caller borrows. The host creates the empty application database before
constructing this capability; construction installs the exact root and owner
anchor and validates the database routing contract. Joiner continuations,
including bootstrap history pull, cancellation closure, cleanup activation
acceptance, and cleanup completion, consume this type until activation
materializes a runnable `Store`; no continuation receives the database,
membership, verifier, root, storage, journal, or identity separately, and none
reconstructs a verifier from a raw root.

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
1. Construct `AuthorizedStore`, `AuthorizedStoreOperation`, or
   `AuthorizedWriterOperation` at that boundary.
1. Change every nested function to a method on the appropriate capability or a
   private helper that receives the capability.
1. Remove loose database, storage, root, membership, registration, and verifier
   parameters that duplicate capability fields.
1. Delete the raw wrapper and its re-export after its final caller moves.
1. Search the complete call chain for another constructor or reload.
1. Compile immediately so privacy and borrowing errors reveal incomplete
   conversions.
1. Run the workflow's failure, restart, and sabotage tests before committing
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
