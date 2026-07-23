# Store protocol ownership

## Goal

Coven has one Store protocol: independently authored immutable device streams
whose verified, dependency-ready commits converge through deterministic Merge
application.

Remove the coordinated Serial protocol completely. There is no policy selector,
second protocol implementation, conditional global head, provisional branch, Serial conflict,
coordination capability, compatibility reader, or migration for earlier Coven
development states.

The remaining Store implementation has one concrete protocol owner. Domain and
cycle entry points call operations on that owner; policy-neutral modules retain
only closed byte, crypto, transport, SQLite, and application-result primitives.
This ownership work completes the Store foundation required by
`plans/circles.md`.

## Current implementation state

- Complete: the Serial protocol, policy selector, coordination capability, and
  alternate wire/database state are absent.
- Complete: `sync::store::Store` is the one concrete protocol owner; operation
  planning, candidate publication, abandonment, package preparation, snapshots,
  acknowledgements, and registration live below it.
- Complete: pull discovery, ancestry, retained-history verification,
  registration and device-lifecycle validation, joining evidence, package
  loading, materialization, and pull tests live below `sync::store::pull`; the
  root `sync::store_pull` module is absent.
- Complete: the device-admission, cancellation, cleanup, restart, journal, and
  transfer protocol lives below `sync::store::device_join`; the root
  `sync::device_join` module and path are absent.
- Complete: the uncalled self-retirement publisher and its wire, storage,
  database, pull, and test state are absent. Exact device exclusion is the one
  device-deactivation mechanism.
- Complete: the Store-owned device-join workflow is divided into exchange,
  journal, authority, Owner, joiner, provider-administrator, cleanup, and error
  modules. Each workflow implementation lives in its owning module; sibling
  dependencies are explicit imports rather than forwarding entry points.
- Complete: the device-join implementation module is private. Initialized
  Owner and provider-administrator commands enter `Store`; local Store journal
  reads enter `StoreDatabase`; the joining device's pending journal and
  bootstrap operations remain a distinct pre-Store boundary.
- Complete: device-exclusion proposal, cancellation, finalization, restart,
  collision handling, cleanup, journal parsing, atomic journal transitions,
  and tests live below `sync::store`; the root workflow and database modules
  are absent, and the concrete Store owns its command methods.
- Complete: Store package reclamation wire values, proof selection,
  authorization, deletion, receipt publication, restart, durable journal, and
  command database transitions live below `sync::store`; the root reclaim
  workflow, journal, and command-transition database modules are absent.
- Complete: exact membership authority discovery, projection, durable cursors,
  invitations, removals, conflict resolution, key wrapping, and key rotation
  live below `sync::store::membership`; the root `sync::membership_ops` and
  `sync::invite` modules are absent.
- Complete: membership publication, invitation, keyring access, removal,
  conflict resolution, and their tests have distinct modules below
  `sync::store::membership::mutation`. Their implementations use explicit
  dependencies; the parent module only exposes the aggregate API.
- Complete: Owner promotion request, acceptance, finalization, authority,
  status, journal validation, atomic database transitions, and tests live below
  `sync::store`; the root workflow and raw `Database` command methods are
  absent.
- Complete: Circle creation, rename, restart, preparation, publication,
  activation verification, journal validation, atomic database transitions,
  and tests live below `sync::store`; the root Circle workflow and verifier
  modules are absent.
- Complete: verified materialization, retained replay, audience activation,
  outbound capture, preparation, publication, abandonment, Circle controls,
  device exclusion, Owner promotion, and membership-mutation transitions live
  on `StoreDatabase`; their former root database modules and forwarding paths
  are absent.
- Complete: production callers and test fixtures pass Store authority
  explicitly. Store tests that require private persistence algorithms live
  beside those algorithms rather than widening visibility or reaching through
  the root database.
- Complete: Store creation, device registration, join challenges,
  acknowledgements, snapshot publication, and their durable transitions live
  on `StoreDatabase`; the former root `Database` workflow modules are absent.
- Complete: Store creation, membership-load, membership-mutation,
  device-exclusion, and snapshot-publication locks are one shared
  `StoreDatabase` runtime. Store workflows no longer acquire those locks
  through raw SQLite.
- Complete: snapshot and membership implementation modules are private. Cycle,
  join, restore, tests, and application reads use Store operations and closed
  Store results rather than reaching through those modules.
- Complete: membership-floor validation is policy-neutral and lives with the
  floor value. Blob tombstone collection requires an authenticated membership
  chain and no longer calls Store membership implementation code or permits an
  authorization-free branch.
- Discovered: cycle and application command boundaries still construct
  `StoreDatabase` from raw `Database`, and some Store modules retain raw
  `StoreDatabase::sqlite()` access for closed SQL, schema models, test fault
  injection, and host-facing reads. The final boundary audit must remove
  operation-continuing raw access without wrapping those operations.
- Complete: revocation-cycle and concurrent-assignment membership conflicts are
  read and resolved through concrete Store and application operations.
  Applications receive opaque choices with member read models; exact heads and
  grant identifiers remain private, and Store rejects stale or foreign choices
  before operation planning.
- Complete: concurrent assignment resolutions preserve a contested grant only
  when every distinct resolver selects it. Disagreement retires every contested
  assignment, while each resolver's prior Owner grant is retired and replaced
  by the signed resolution authority.
- Remaining: route cycle and application commands through the concrete Store
  owner, make Store implementation modules private, reduce temporary
  visibility, and seal the boundary.

## Final dependency direction

```text
application / cycle / domain command
                 |
                 v
        concrete Store owner
                 |
                 v
        policy-neutral protocol,
        transport, bytes, crypto,
        and SQLite primitives
```

There is no runtime protocol dispatch. A Store root describes the Merge
protocol directly, and every Store operation enters the concrete Store owner.

## Boundary laws

- no `Serial`, `WritePolicy`, coordinated-head, conditional-write, or
  provisional-branch production type remains;
- no coordination storage trait, capability probe, configuration, error, mock,
  or provider implementation remains;
- no persisted enum retains a dead Serial variant;
- no parser accepts superseded Serial objects;
- no database schema, journal, recovery path, or fixture retains Serial state;
- no application API exposes a protocol mode or Serial-specific status;
- the Store owner owns authority loading, planning, remote effects, retries,
  conflicts, cleanup, durable completion, and terminal results;
- shared functions accept closed policy-neutral inputs and never continue the
  Store state machine;
- Store-specific database transitions live on the Store database owner and
  commit wholly or roll back;
- old paths are deleted rather than wrapped, aliased, deprecated, or retained.

## Product semantics after deletion

- Devices may author while disconnected.
- Concurrent commits are accepted when their signed authority and dependencies
  verify.
- Concurrent updates to different columns of one row are three-way merged.
- Remaining row conflicts use `_updated_at`; deletes win over edits.
- SQLite foreign-key dependencies wait for their exact parent history.
- Non-foreign-key constraint conflicts reject the incoming changeset atomically.
- Applications requiring a globally admitted transaction order must use a
  separate online authority; Coven does not claim serializability or
  linearizability.

## Removal order

### Protocol vocabulary

Collapse every type whose only distinction was Merge versus Serial:

- remove `WritePolicy` from configuration, roots, journals, receipts, and APIs;
- make commit coordinates, predecessor cuts, frontiers, membership state,
  device state, history summaries, package metadata, and operation references
  direct Merge shapes rather than one-variant enums;
- remove Serial heads, predecessors, positions, authorization snapshots,
  accepted suffixes, conflict values, and branch states;
- remove conditional coordination versions and their signed metadata;
- update canonical encodings, hashes, signatures, parsers, fixtures, and tests
  to the single format.

Do not leave one-variant compatibility enums merely to reduce caller changes.
Use the existing Merge inner value as the direct type where it already names the
domain concept; rename it only when the old name incorrectly exposes a removed
choice.

### Coordination and Serial implementation

Delete:

- `sync/store/serial.rs` and `sync/store/serial/`;
- `database/serial_authorization.rs`;
- `membership_ops/serial.rs` and `membership_ops/serial/`;
- Serial outbound, acknowledgement, snapshot, reclaim, registration, invite,
  exclusion, recovery, and Circle branches;
- coordinated storage traits, adapters, request conditions, capability probes,
  mocks, and public configuration;
- Serial-only tests and fixture builders.

After callers move, delete every helper and field orphaned by these paths.

### Store root and construction

Replace dispatcher shapes with the concrete Store owner:

- `Store` is a concrete owner, not an enum or trait dispatcher;
- construction validates one Store-root format and supplies the Store's real
  dependencies;
- remove accessors used to unpack the Store into mixed workflows;
- cycle invokes Store operations and consumes closed application results;
- tests construct the production Store rather than selecting a policy;
- remove optional coordination and optional membership parameters whose only
  purpose was protocol selection.

### Pull ancestry, registration, and device joining

The Store implementation owns:

- membership-prefix and exact-head verification;
- predecessor and accepted-history evidence;
- device-stream reachability and retained history;
- join authorization, bootstrap history choice, activation, cancellation,
  cleanup, restart, and terminal completion;
- registration lifecycle validation against accepted history;
- device-operation validation and Owner recovery authority.

Delete mixed authority values such as `RegistrationPredecessorAuthority`,
`SerialAuthorizationHistory`, and any remaining optional bootstrap authority.
Shared code may decode exact signed objects and atomically install an
already-authorized bootstrap image.

### Snapshot ownership

The Store implementation owns frontier coverage, membership-prefix proof, author authority,
cadence, publication, stable-cut selection, bootstrap choice, recovery, retry,
and terminal completion.

Shared snapshot code retains image capture, hashing, canonical metadata bytes,
exact-object storage, streaming, byte verification, and installation from a
closed authorized plan.

### Acknowledgement ownership

The Store implementation owns queue interpretation, frontier selection, candidate
construction, publication outcome interpretation, activation, nonactivation,
cleanup, retry, and terminal completion.

Shared code may retain acknowledgement wire values, signing from a resolved
plan, exact-object upload and readback, and queue-row primitives with identical
meaning outside the Store implementation.

### Outbound operations

The Store implementation owns operation planning, candidate construction, immutable
publication, arbitration, abandonment, and completion.

Delete mixed dispatch values including `StoreOperationPreparation`,
`StoreOperationPublicationMode`, policy constructors, and policy-dispatching
publication functions. Shared code retains package/blob byte preparation,
canonical readback, exact-object upload, and closed domain values.

### Store controls

Move each initial and resume path as one connected operation:

- active-device registration;
- member invitation and removal;
- device joining;
- Owner promotion and recovery;
- device-exclusion proposal, cancellation, finalization, abandonment, and
  restart;
- Circle creation, rename, and restart.

Domain modules retain policy-neutral input validation, signed values,
encryption, and durable command inputs. The Store implementation owns history authority,
commit planning, publication, collision handling, retry, cleanup, and terminal
database state.

### Reclamation and tombstones

The Store implementation owns acknowledgement coverage, history proof, materialization proof,
eligible-target selection, authorization, receipt publication, restart, and
terminalization. It produces a closed exact deletion plan.

Shared code may perform physical scans, exact reads, set operations, and exact
deletion; it cannot decide eligibility or authority. Every durable reclaim fact
commits wholly or rolls back.

### Seal the boundary

- make Store implementation modules private;
- expose operations and closed results rather than dependencies;
- remove forwarding methods, shared-to-Store calls, and Store-to-policy-aware
  shared workflows;
- remove temporary visibility, duplicate algorithms, old aliases, stale
  comments, and superseded fixtures;
- organize files by cohesive owner and invariant;
- update `plans/circles.md` and `plans/store-commit-policies.md` to the one
  protocol before Circle implementation resumes.

## Remaining relocation sequence

1. **Complete.** Put verified materialization, retained-materialization opening, Circle and
   stream activation recording, device-state derivation, remote-object
   activation, and join-bootstrap installation behind the Store database
   owner. The root database retains closed row encoding and SQLite operations,
   not Store authority decisions.
1. **Complete.** Put outbound write preparation, publication, collision, abandonment, and
   terminal cleanup transitions behind the Store database owner. Store code
   must not drive those transitions through raw `Database` workflow methods.
1. **Complete.** Put membership mutation, device registration and recovery, snapshot,
   acknowledgement, and join transitions behind the Store database owner.
   Keep host-facing read models on `Database`; Store protocol progress and
   completion transitions live on `StoreDatabase`.
1. **In progress.** Route the remaining cycle and application commands through
   Store operations, remove raw Store workflow methods from `Database`, make
   the remaining Store implementation modules private, move manual membership
   conflict resolution behind Store ownership, and reduce widened helper
   visibility to the exact remaining callers.
1. Run the boundary searches, sabotage tests, focused failure-injection tests,
   strict lint, repository hook, and manual rules review; then update the
   dependent plans to the sealed one-protocol shape.

## Commit boundaries

Each removal commits only when its dependent production callers and tests use
the one-protocol shape, focused tests pass, strict Clippy passes, and searches
find no dead alternative introduced by that boundary. Rebase onto
`origin/main` before each push.

## Verification

Run after each applicable boundary and at the final seal:

```sh
rg -i "serial|writepolicy|write_policy|coordination|conditional head|provisional branch" \
  crates/coven-core coven-uniffi coven-ffi docs README.md
rg "Option<&.*Coordination|coordination: Option|StoreOperationPreparation|StoreOperationPublicationMode" \
  crates/coven-core/src
rg "super::super::|crate::sync::store::merge" \
  crates/coven-core/src/sync --glob '!store/**'
rg "use crate::sync::store_pull::\*|crate::sync::store::package_preparation" \
  crates/coven-core/src/sync/store
```

Every remaining textual match must describe an unrelated meaning or be removed.
Also verify:

- canonical round trips cover the sole wire format;
- opening and joining construct the concrete Store through production paths;
- offline concurrent writes converge in both arrival orders;
- same-column, different-column, delete/edit, foreign-key, and constraint
  conflicts exercise the real apply path;
- join, invitation, removal, recovery, exclusion, snapshot, acknowledgement,
  reclaim, restart, and fault-injection tests use the concrete Store;
- authority checks are load-bearing under sabotage;
- database fault injection covers multi-step durable transitions;
- no compatibility, fallback, legacy, or migration path remains;
- formatting, strict Clippy, focused tests, the repository hook suite, and the
  manual rule review pass.

## Completion condition

The work is complete when Coven contains one Store protocol in its public API,
wire values, database, runtime, tests, fixtures, plans, and documentation; every
Store operation enters the Store owner and stays there through authority,
remote effects, retries, cleanup, and durable completion; and all searches and
verification gates support those statements.
