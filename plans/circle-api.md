# Circle application API and public errors

## Status

Implemented. The `coven.circles()` namespace, public derived `CircleState`,
operation-inspection surface, close-status inspection, recovery commands, and
typed public errors with stable identifiers are on `main`.

## What exists (the substrate, all merged)

- Flat handle methods: `create_circle`, `rename_circle`, `get_circles`,
  `get_circle_members`, `get_circle_operations` (`crates/coven/src/handle.rs`
  ~1304-1345), plus `SyncComponents` methods for `add_circle_member`,
  `remove_circle_member`, `resolve_circle_control`,
  `cancel_circle_epoch_close`, `exclude_circle_close_device`,
  `delete_circle`, `retry_circle_operation` (`sync/cycle.rs`) not yet
  exposed on the handle.
- Internal state: `CircleInfo { Active { …, rotation_required }, Conflicted
  { branches }, Deleted { id } }`; `CircleCurrentState`
  (Active/Closing/Inactive/ControlConflict/Deleted) plus the
  rotation-required derivation; `CircleOperationBlock::AuthorityLost`;
  `WriteBlock::RotationRequired`; `CircleOperationError` typed variants
  (RotationRequired, NotConflicted, ChosenBranchNotRetained, NoCloseToCancel,
  NoCloseToExclude, DeviceNotACloseParticipant, Conflicted, Deleted,
  NotBlocked, ExcludedDeviceMustReset, BrowsableStorage, …);
  `GateError::CircleDeleted` at capture; `WriteStatus` per durable write.

## Design

### 1. The `circles()` namespace

`Handle::circles() -> Circles<'_>` — a borrowed namespace struct (no state,
no Arc, no lifecycle of its own) grouping the Circle surface exactly as the
master plan sketches:

```rust
let id = coven.circles().create("Family").await?;
coven.circles().rename(id, "Household").await?;
coven.circles().add_member(id, identity).await?;
coven.circles().remove_member(id, identity).await?;
coven.circles().resolve(id, chosen).await?;
coven.circles().cancel_close(id).await?;
coven.circles().exclude_close_device(id, device).await?;
coven.circles().delete(id).await?;
coven.circles().retry_operation(operation_id).await?;
coven.circles().discard_operation(operation_id).await?;
coven.circles().list().await?;                 // Vec<Circle>
coven.circles().members(id).await?;
coven.circles().operations().await?;           // inspection, typed blocks
```

The existing flat `*_circle` methods on `Handle` moved into the namespace
(callers updated — greenfield, no deprecated aliases). `discard_operation`
requires verified permanent nonactivation and leaves the operation durable when
that proof is absent.

### 2. Public `CircleState`

```rust
pub enum CircleState {
    Active,
    Inactive,
    Closing,
    RotationRequired { removed_members: Vec<String> },
    ControlConflict { branches: Vec<CircleControlCoord> },
    Deleted,
}
```

Derived per Circle at `list()` time from the existing internals — exactly
one mapping function, tested exhaustively:

- `CircleCurrentState::Active` + rotation derivation empty → `Active`;
  non-empty → `RotationRequired`.
- `Closing` → `Closing` (rotation-required derivation still applies to
  closings whose roster names a removed Store member — plan rule: rotation
  state is visible regardless of close progress; map to
  `RotationRequired` only when Active, since Closing is already the exit
  path — state the choice in the mapping's doc comment).
- `Inactive` → `Inactive`; `ControlConflict` → `ControlConflict`;
  `Deleted` → `Deleted`.

`Circle` (the list item) carries `id`, `name` (None for Deleted — the name
is display metadata a deleted Circle no longer resolves), `role` when
active, and `state: CircleState`. `CircleInfo` remains internal; the public
type is the mapping's output — a mandated mirror at the public-API
boundary, not a duplicate.

### 3. Operation inspection

`circles().operations()` returns each durable operation's id, circle,
intent kind, and progress including the typed block
(`OperationProgress::Blocked(CircleOperationBlock)` re-exported). This is
the "inspect a CircleOperationId, its durable progress, and typed block
reason" surface from the master plan; `retry_operation` is its companion.
Close-response inspection: expose per-close settlement status (which
participant slots hold responses vs exclusions vs nothing) via
`circles().close_status(id)` — read-only, derived from the retained close
state the finalize path already loads.

### 4. Public errors

One public `CircleError` enum on the `coven` crate, mapping the internal
typed variants 1:1 with stable identifiers and carrying the ids needed for
display/retry (circle id, operation id, removed members, close id, branch
coords). Categories per the master plan's Errors section, each an explicit
variant — nonexistent/inactive/closing/conflicted/rotation-required/deleted
Circle; missing or invalid access; stale epoch; blocked operation;
excluded-device-must-reset; browsable-storage. `SyncError` stops being the
Circle catch-all: `circles()` methods return `Result<_, CircleError>`.
Write-path categories (deterministic conflict, blocked write) stay on
`WriteStatus`/`WriteBlock`, which already carry them typed — no
duplication, the API docs point at the split.

No error exposes a removed protocol shape (grep-verified in tests).

**Ratified scope (audit of 43e26ae).** `CircleError` carries an explicit typed
variant for every category that has an internal typed producer today:
browsable-storage, rotation-required, conflicted, deleted, not-conflicted,
chosen-branch-not-retained, no-close-to-cancel, no-close-to-exclude,
device-not-a-close-participant, resolve-to-closing-branch,
excluded-device-must-reset, not-blocked, discard-requires-nonactivation, and
blocked (with the `CircleOperationBlock`). The remaining plan-listed
categories — nonexistent, inactive, and closing Circle, and stale epoch — have
no internal typed refusal producing them yet; they surface through the
`Protocol` catch-all. Adding a public variant with no producer would be
pre-scaffolding, so each such variant lands with the internal refusal that
raises it. `add_member` refusals surface typed like every sibling:
`SyncComponents::add_circle_member` returns `CircleOperationError` directly
(not `SyncCycleFailure`), so browsable-storage, rotation-required, deleted, and
conflicted reach `CircleError` with their ids.

### 5. Docs pass on the public items

Every public item has a doc comment stating its invariant plainly. The Circle
site guide documents the same operation and error surface.

## Tests

1. Namespace round-trip on the production two-device fixture: create →
   rename → add → list (both devices see Active) → remove → close
   completes → list shows successor Active; states asserted at each step.
2. Exhaustive `CircleState` mapping test over constructed
   `CircleCurrentState` + membership inputs (every variant × rotation).
3. Blocked-operation surface: authority-revoked fixture → operations()
   shows typed AuthorityLost; retry_operation round-trips.
4. close_status surface across a close with one response and one exclusion.
5. Error mapping: each internal refusal surfaces its public variant with
   stable identifiers (rename-on-deleted, resolve-on-nonconflicted,
   cancel-without-close, exclude-non-participant, delete-on-conflicted).
6. Grep-style test asserting no public error Display mentions removed
   protocol vocabulary (serial, policy, engine).

## Out of scope

- FFI/bindings (none exist in-repo).
