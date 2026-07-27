# Circles

[Sharing](/docs/sharing) grants the *whole* store to a member: everyone who
holds the store keyring reads every synced row. A Circle narrows that inside one
store. A Circle is a private audience — a named subset of the store's members —
and a row addressed to a Circle reaches only that Circle's members, not the rest
of the store.

Circles do not add a second protocol. Every Store and Circle change rides the
same [immutable per-device commit streams](/docs/sync-model) that carry ordinary
row changes: a Store commit is the only thing that activates a Circle package,
control change, roster change, or bootstrap. What a Circle adds is *who* a row's
package is encrypted for.

Examples use the todos app, with a `Family` Circle inside a store two housemates
share.

## Audiences

Every synced row belongs to exactly one **audience** at a time:

```rust
pub enum Audience {
    Store,
    Circle(CircleId),
    Local,
}
```

- **Store** — every store member receives the row. This is the default and the
  audience of every row in a store with no Circles.
- **Circle(id)** — only the [effective members](#effective-membership) of that
  one Circle receive the row.
- **Local** — the row never leaves the device that wrote it.

A person may belong to any number of Circles, so an audience is *not* a person
and *not* a Circle roster: Store and Local are audiences on their own, and each
Circle contributes exactly one more audience. A [`CircleId`](rustdoc:struct:coven::CircleId)
is a permanent random 128-bit value; renaming a Circle never changes it, its
keys, its rows, or its package history.

## Declaring audience routing

A row's audience is a column on a *synced graph root*, the same way a
[gate](/docs/local-data) is. Only a root declares an audience; its foreign-key
descendants inherit it. Two builder forms exist, and a table uses at most one:

```rust
SyncedTable::new("lists", RowIdentity::IndependentUuid).scoped_by("audience")
SyncedTable::new("todos", RowIdentity::IndependentUuid)
    .inherits_audience_through("list_id")
```

- [`scoped_by(column)`](rustdoc:method:coven::SyncedTable::scoped_by) makes the
  table an **audience root**. The `TEXT` column holds the row's audience: `NULL`
  is Store, the reserved value `local` is Local, and any other value is a
  canonical committed `CircleId`. Moving a row between audiences is an ordinary
  SQL update to that column (see [Moving a row](#moving-a-row)).
- [`inherits_audience_through(column)`](rustdoc:method:coven::SyncedTable::inherits_audience_through)
  makes a descendant table take its audience from the parent row named by the
  foreign key in `column`. Every descendant of an audience root must select its
  inheritance foreign key explicitly; coven never guesses among a table's
  foreign keys.

`scoped_by` is the general form of the two-audience [`gated_by`](/docs/local-data#gated-roots)
(Store or Local only). A table declares one or the other, never both.

For a synchronized foreign key from child to parent, the child and parent may
share an audience, or the parent may be Store — a Circle row may not point into
a different Circle or into Local, and no Store row may point into a Circle or
Local. Coven validates the whole connected foreign-key component on every host
write and every applied package, including unchanged descendants, so a
concurrent move or delete cannot leave a dangling cross-audience reference.

A store that declares any audience root requires an [opaque cloud
home](/docs/encryption#opaque-and-browsable-homes); a browsable home cannot hold
Circles.

### Private routing

A row can move between audiences while other devices still hold older packages,
so the receiver needs a Store-visible, convergent answer to which audience owns
a row right now — without the answer revealing the application's real table name
or primary key. Coven keeps a Store-visible mirror keyed by a `RowRoutingId`: an
HMAC over the row's `(table_name, row_identity)` under a key every store member
can derive. The mirror row carries only the routing id and the owning
`CircleId` (or nothing, for Store or a deleted/Local row). Table names and
primary keys never appear in it.

Because the routing key is derived from store-level material, a store member who
holds it can test guesses for *predictable* table-and-primary-key combinations
against the mirror. This is a stated [privacy limit](#security-and-privacy-limits),
not a defect.

## Effective membership

Circle access requires **both**:

1. active Store membership, and
2. active membership on that Circle's roster.

The effective members of a Circle are the intersection. Removing a person from
the Store immediately ends their effective access to every Circle, even before
the Circle's key rotates — the same store keyring rotation that revocation
performs already withholds new Store content from them, and a removed store
member never fetches or decrypts a Circle package.

When a Circle's resolved roster still names a Store identity that is no longer an
active store member, the Circle is **rotation-required**: publishing new content
into it is refused until an Owner closes the current epoch and activates a
successor roster without that identity. Rename and member-addition are refused
in this state too; member removal, close finalization, and conflict resolution
stay available. A completed close, or re-adding the person to the store, clears
the state.

Rotation protects *future* Circle writes. Like store revocation, it cannot make
already-received history unseen: a former member keeps every Circle package,
key, and snapshot they pulled while they had access.

## The `coven.circles()` API

Circle commands live on a borrowed namespace off the handle,
[`coven.circles()`](rustdoc:method:coven::CovenHandle::circles). Every method is
async and maps internal refusals to a typed [`CircleError`](#errors).

```rust
let circles = handle.circles();

// Create a Circle whose founder is this store identity.
let family: CircleId = circles.create("Family").await?;

// Rename it — the id, keys, roster, rows, and history are unchanged.
circles.rename(family, "Household").await?;

// Add (or re-add) a store member by their Ed25519 public key. This seals them a
// fresh access leaf and a current bootstrap image.
circles.add_member(family, &housemate_pubkey_hex).await?;

// Remove a member. This closes the old epoch and returns the durable operation
// tracking the close (see below).
let op: CircleOperationId = circles.remove_member(family, &housemate_pubkey_hex).await?;

// Delete a Circle. Terminal.
circles.delete(family).await?;
```

`create` returns the new [`CircleId`](rustdoc:struct:coven::CircleId) once the
signed roster, metadata, access set, control, activating Store commit, and local
materialization are all durable. `add_member`, `rename`, and `delete` complete
against the durable operation journal; `remove_member` returns a
[`CircleOperationId`](rustdoc:struct:coven::CircleOperationId) because a member
removal runs the multi-step [epoch close](#member-removal-and-epoch-close) before
it activates.

Re-adding a former member is the same call as `add_member`. Prior possession of
an old Circle key grants no current authority — the re-add issues a new active
access leaf and a current bootstrap.

### Inspecting Circles

```rust
// Every Circle the local identity can see, with its derived state.
let all: Vec<Circle> = circles.list().await?;

// The Circle's current members who remain current store members, with roles.
let members: Vec<CircleMemberInfo> = circles.members(family).await?;
```

[`Circle`](rustdoc:struct:coven::Circle) is what `list` reports per Circle:

```rust
pub struct Circle {
    pub id: CircleId,
    pub name: Option<String>,       // absent while inactive, conflicted, or deleted
    pub role: Option<CircleRole>,   // present only when the local identity holds active access
    pub state: CircleState,
}
```

[`CircleState`](rustdoc:enum:coven::CircleState) is the Circle's derived state,
mapped once from its current control:

- `Active` — a single resolved active epoch whose roster names only current store
  members.
- `Inactive` — the local identity holds no active access: never granted, or
  revoked by a removal it has not re-joined past.
- `Closing` — an [epoch close](#member-removal-and-epoch-close) is in flight;
  authoring new content under the old epoch is frozen until the successor
  activates.
- `RotationRequired { removed_members }` — the roster names store identities that
  are no longer active store members (see [Effective membership](#effective-membership)).
- `ControlConflict { branches }` — the control history forked into concurrent
  valid successors and awaits Owner resolution. Carries the complete retained
  branch set.
- `Deleted` — the control history terminated in an Owner-signed deletion.

[`CircleMemberInfo`](rustdoc:struct:coven::CircleMemberInfo) carries each
member's `pubkey`, [`CircleRole`](rustdoc:enum:coven::CircleRole) (`Owner` or
`Member`), and an `is_self` flag.

### Durable operations

Every Circle command is a durable operation: it survives restart and resumes
from its exact recorded phase without guessing remote state. Inspect the
operations that have not yet activated:

```rust
let pending: Vec<CircleOperationInfo> = circles.operations().await?;
```

[`CircleOperationInfo`](rustdoc:struct:coven::CircleOperationInfo) names the
operation id, its Circle, its
[`kind`](rustdoc:enum:coven::CircleOperationKind) (`Create`, `Rename`,
`AddMember`, `RemoveMember`, `ResolveControl`, `Delete`), and its
[`state`](rustdoc:enum:coven::CircleOperationState):

- `Pending` — prepared, waiting to publish.
- `WaitingForCloseResponses` — a member-removal close is collecting participant
  responses.
- `Finalizing` — the successor commit, outcome, and bootstraps are staged and
  about to publish.
- `Blocked { block }` — the operation cannot publish. The one block reason today
  is [`AuthorityLost`](rustdoc:enum:coven::CircleOperationBlock), meaning the
  author's exact store grant no longer has current write authority. The
  initiator retries it once authority is restored:

```rust
circles.retry_operation(op_id).await?;
```

An initiator can discard an operation only after Coven verifies that its
candidate can never activate:

```rust
circles.discard_operation(op_id).await?;
```

If permanent nonactivation is not proven,
[`discard_operation`](rustdoc:method:coven::Circles::discard_operation) returns
`DiscardRequiresNonactivation` and leaves the durable operation intact.
Discarding an ordinary *host write* that a Circle refused is separate, on
[`WriteStatus`](#offline-and-blocked-writes).

### Control conflicts

Two Owner devices can author concurrent valid control successors — for example,
two renames while offline. Coven never silently picks one when the choice would
decide membership, keys, or deletion intent; instead it retains both and reports
the Circle as `ControlConflict { branches }`. An Owner resolves it by choosing
one branch:

```rust
if let CircleState::ControlConflict { branches } = &circle.state {
    circles.resolve(circle.id, branches[0].clone()).await?;
}
```

[`resolve`](rustdoc:method:coven::Circles::resolve) authors a successor of the
chosen [`CircleControlCoord`](rustdoc:struct:coven::CircleControlCoord) that
names every retained branch as a dependency, so the conflict collapses to that
successor deterministically on every device in either arrival order. Resolution
is the exit path out of a conflict, so it is callable regardless of
rotation-required state; a branch discovered since the command resurfaces as a
new conflict rather than being silently dropped.

## Member removal and epoch close

Removing a member always closes the old epoch before any future write, so that
new content is sealed under a key the removed member never receives. Removal
publishes an epoch close naming every remaining member's device and a reserved
response slot for each. Each remaining device, after pulling, applies every
accepted old-epoch commit it holds and publishes its own applied frontier into
its slot. When every slot is settled, an Owner-signed outcome rotates the Circle
key and epoch, drops the removed member, and publishes a fresh bootstrap for
every remaining member.

Packages beyond the accepted cutoff are invalid: receivers omit them
deterministically, and authoring beyond the cutoff fails to its initiator. No
later pass repairs an incomplete close; every step is retained for idempotent
retry or fails loudly.

Inspect a close in flight:

```rust
let status: CircleCloseStatus = circles.close_status(family).await?;
for participant in status.participants {
    // participant.device_id, participant.settlement:
    //   Responded | Excluded | Pending
}
```

An Owner can settle a stuck close two ways:

```rust
// Exclude an unavailable participant device. It must reset from the successor
// bootstrap before it can write or acknowledge again.
circles.exclude_close_device(family, device_id).await?;

// Cancel the whole close, restoring the frozen epoch byte-for-byte.
let cancel_op = circles.cancel_close(family).await?;
```

Each participant slot holds exactly one of two signed values — the device's own
response, or an Owner-signed exclusion — and create-once semantics decide every
race between them. The one outcome slot likewise holds exactly one of the final
outcome or an Owner-signed cancellation.

## Moving a row

Changing a row's audience is an ordinary SQL update to its `scoped_by` column;
coven previews the transaction, computes the inherited closure, and publishes
only the destination representation — full destination-row inserts, full private
routes, the Store mirror change, and any Store ancestors the destination graph
requires. It does *not* publish a source-audience delete: receivers drop the
stale source materialization because the winning Store mirror no longer names
that audience, which avoids requiring a recipient to receive a package after
their access has ended.

```rust
handle.sql(move |sql| {
    sql.execute(
        "UPDATE lists SET audience = ?1 WHERE id = ?2",
        coven::rusqlite::params![family.to_string(), list_id],
    )?;
    Ok(())
}).await?;
```

A move out of Local or an unavailable Circle requires every referenced blob's
verified plaintext before the host transaction commits; missing material fails
the write rather than deferring a download.

## Offline and blocked writes

The [write status](/docs/sync-model#lifecycle) surface is unchanged for Circle
rows: a host transaction is `LocalOnly`, `Pending`, `Publishing`, `Published`,
`Blocked`, or `Resolved`, whatever its rows' audience. One
[`WriteBlock`](rustdoc:enum:coven::WriteBlock) reason is Circle-specific:
`RotationRequired { circle_id, removed_members }`, recorded when a write targets
a Circle whose roster names a removed store member. Repair it by completing the
Circle's close (or re-adding the member to the store), then retry the blocked
write. Circle writes never claim global serializability: a concurrent
constraint conflict surfaces as a typed deterministic conflict, the same as a
Store write.

## Snapshots, restore, and reclamation

A new device [bootstraps](/docs/bootstrap) from a Store snapshot, then installs
one Circle image per Circle it has active access to. Circle images use the same
verified-image machinery as a Store snapshot: an active device authors a
standalone Circle snapshot for each Circle it holds, sealed to that epoch's key.
A Circle snapshot is acknowledgement-stable only once every device that holds
active access has acknowledged a frontier that dominates its cut; restore and
reclamation read the maximal stable snapshot.

Restore stages the Store image and every Circle image the restoring identity can
decrypt, verifies each, and installs them all in one transaction — a partially
installed image is never exposed as the current database.

Reclamation is audience-specific and evidence-based: it deletes a Circle
package, snapshot, or bootstrap only when verified acknowledgements and snapshot
coverage prove no retained history still needs it. A member-addition bootstrap
is pinned to its recipient's access activation and is not reclaimed until that
recipient has acknowledged a later sufficient Circle snapshot or lost authority
under signed evidence.

## Deletion

An Owner deletes a Circle with an Owner-signed terminal control transition.
Activation reduces the Circle's current state to `Deleted` and, in the same pull
transaction, prunes its materialized rows, private routes, blob bindings, and
access/roster/metadata caches, while retaining the control spine needed to
verify historical commits and reclamation. `list` reports the Circle as
`Deleted` rather than omitting it.

Deletion is terminal: a control descending from a deletion is invalid, and a
successor racing it from another Owner device surfaces as a `ControlConflict`
the Owner resolves. Deletion does not claim to erase copies members already
received.

## Errors

[`CircleError`](rustdoc:enum:coven::CircleError) maps each internal refusal to a
stable public variant carrying the ids a caller needs:

- `NotConfigured` / `LoopNotRunning` — no sync provider, or the sync loop is not
  running.
- `BrowsableStorage` — Circles require an opaque cloud home.
- `RotationRequired { circle_id, removed_members }` — the roster names removed
  store members; new content is refused until an Owner closes the epoch.
- `Conflicted { circle_id }` / `NotConflicted { circle_id }` /
  `ChosenBranchNotRetained { circle_id }` — the control-conflict refusals.
- `Deleted { circle_id }` — the Circle terminated in a deletion.
- `NoCloseToCancel` / `NoCloseToExclude` / `DeviceNotACloseParticipant` — the
  epoch-close refusals.
- `ResolveToClosingBranch { circle_id }` — conflict resolution selected a
  branch that starts an epoch close instead of an active branch.
- `ExcludedDeviceMustReset { circle_id, close_id }` — this device was excluded
  from the close and must reset from a successor bootstrap.
- `NotBlocked { operation_id }` / `Blocked { circle_id, block }` — the durable
  operation refusals.
- `DiscardRequiresNonactivation { operation_id }` — Coven cannot prove the
  candidate permanently unable to activate, so it remains durable.
- `Identity(..)` — the local signing identity is not established.
- `Protocol(..)` — an internal protocol or database failure with no distinct
  public category.

Write-path outcomes stay on [`WriteStatus`](/docs/sync-model#lifecycle) and
`WriteBlock`, not on `CircleError`.

## Security and privacy limits

Circles partition confidentiality *within* a store, but they run over the same
storage every store member and the provider can observe. These are protocol
properties, not gaps to close later:

- **Revocation prevents future access; it cannot un-send old data.** A removed
  member keeps every Circle plaintext, key, snapshot, and package they already
  pulled. Adding a member grants the retained history needed to materialize the
  Circle's current state.
- **Store members can see which identities administer which Circles.** A
  Circle's public control state is encrypted to the store, not to the Circle, so
  every store member can verify a Circle's control history — and that state names
  the Owner public keys and the roster/metadata head author coordinates. Store
  members therefore learn which identities own and administer which Circles, even
  Circles they are not in. This is deliberate and load-bearing: verifying a close
  outcome, a conflict resolution, or a deletion requires reading the Owner
  authority that signed it. Circle *contents*, rosters, and display names stay
  private to effective members; the *administrators* do not.
- **The Store-visible routing mirror leaks pseudonymous row counts, routing
  changes, and timing.** Every store member sees how many rows each Circle holds
  (under opaque routing ids), and every audience move and delete as it happens.
- **A store member with the routing key can test guesses for predictable
  table-and-primary-key combinations** against that mirror, since the routing id
  is a keyed hash of `(table, primary key)`.
- **Store commits reveal addressed Circle ids, object timing, sizes, and hashes**
  to store members and the storage provider.
- **Recipient access objects reveal their number, size, and update timing**, even
  though the recipient identity in each is pseudonymous.
- **User-supplied object storage can observe provider-level access and object
  metadata.**

When two sets of people must not learn *anything* about each other — not row
counts, not who administers what, not timing — that is two stores, not two
Circles in one store. The [Constraints](/docs/constraints) page covers the
store-level version of this boundary.
