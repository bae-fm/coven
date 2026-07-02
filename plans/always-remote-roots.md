# Always-Remote Blob Roots

## Context

`SyncedTable::new("table")` already declares whole-table row sync. Every row in
that table is eligible for outbound changesets without a gate column.

Blob locality is narrower today. `read_blob` and `open_blob_stream` resolve a
blob's source by finding the row that carries the blob, walking that row to a
gated root, and reading the gate truth. A plain synced table has no gated root,
so a blob on a plain row can be uploaded by the normal host-provided blob path
but later has no authoritative Local/Remote answer for the read path.

The missing shape is not another row-sync declaration. It is an explicit table
role for rows whose blobs are remote by construction.

## Design

Add a `SyncedTable::remote_root()` builder:

```rust
SyncedTable::new("torrents").remote_root()
```

The role means:

- Every row in the table syncs, exactly like `SyncedTable::new`.
- The row is a root for blob subtree walks.
- Blob locality for the root and descendants is always Remote.
- `make_remote`, `make_local`, and `cancel_make_remote` reject this root because
  there is no Local state to transition.
- Descendants inherit the always-remote root through the same foreign-key graph
  used by gated roots.

This is distinct from `gated_by`:

- `gated_by(column)` models mutable Local/Remote state and retains transition
  APIs.
- `remote_root()` models a table that has no local-only state in the sync model.

## Implementation

Extend `GateRole` with an always-remote root variant and add:

```rust
pub fn remote_root(self) -> Self
pub fn is_remote_root(&self) -> bool
```

Update `Gates`:

- Add a `TableGate::RemoteRoot`.
- Treat `RemoteRoot` as a gate terminus for plain descendants.
- Keep rows under a remote root unconditionally in outbound gating and snapshots.
- Make `root_kept_of` return `Some(true)` for a remote root and its descendants.
- Make `subtree_rows` work for remote roots so blob declarations can resolve
  root-scoped blob sets.
- Keep `resolve_root_of` returning the remote root identity for descendants.

Update blob transition code:

- Keep `make_remote`, `make_local`, and cancel paths requiring a gated root with
  a gate column. A remote root is already remote, so these APIs should fail loud
  with a specific error.
- Keep make-remote intent completion tied to gated roots only. Remote roots
  publish host-provided blobs through the normal outgoing changeset path.

Update blob read code:

- `resolve_source` should dispatch remote-root blobs to `BlobSource::Cache`.
- Plain tables that are neither under a gated root nor under a remote root should
  keep failing with `LocalityUnresolved` for blobs, because coven cannot know
  where their bytes live.

Update docs:

- Document `SyncedTable::new` as whole-table row sync.
- Document `remote_root()` as whole-table rows plus always-remote blob locality.
- Keep local-data docs clear that rows not passed to Coven remain local-only.

## Tests

Add coverage for:

- `SyncedTable::new("notes").remote_root()` syncs rows without a gate column.
- A host-provided blob row under a remote root uploads before its row is pushed,
  and a peer can read it through the cache path.
- A CacheLazy host-provided blob under a remote root is not downloaded on pull
  but reads on demand.
- A child table inheriting from a remote root resolves blob locality as Remote.
- `make_remote` rejects a remote root with a specific error.
- `make_local` rejects a remote root with a specific error.
- A plain blob-bearing table that is not under a gated root or remote root still
  fails locality resolution.

## Verification

Run:

```sh
cargo fmt
cargo test -p coven-core
cargo test
cargo clippy --all-targets -- -D warnings
```
