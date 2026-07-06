# Blob-Before-Publish Invariant

## Goal

No changeset or snapshot may be published if an inserted or updated row references a blob that cannot be fetched from remote storage.

## Existing Shape

- `SyncStorage` is the storage boundary for publishing changesets, heads, blobs, and snapshots.
- `CloudSyncStorage` owns the cloud object-key mapping through `CloudSyncStorage::blob_key`.
- `MockSyncStorage` stores blob objects in memory using the same plain-scheme key helper as existing tests.
- Changeset publishing is prepared in `crates/coven-core/src/sync/service.rs::sync`. The caller publishes the returned `OutgoingChangeset`, so the preflight must happen before `sync` returns `Some(outgoing)`.
- Snapshot publishing is committed in `crates/coven-core/src/sync/snapshot.rs::push_snapshot`. The pointer and head are the durable publish markers, so the preflight must happen before either is written.
- Host-provided changeset blobs are uploaded inline in `sync::sync`; host-provided snapshot blobs are uploaded by the cycle through `upload_snapshot_host_blobs` before `push_snapshot`.
- User-provided blobs are not uploaded inline. They become publishable only when the external ref is cleared and their cloud object exists.

## Storage API

Add to `SyncStorage`:

```rust
async fn blob_exists(
    &self,
    namespace: &str,
    id: &str,
    cloud_path: Option<&str>,
) -> Result<bool, StorageError>;
```

Implementation:

- `CloudSyncStorage::blob_exists` resolves the same key as `put_blob`/`get_blob` with `Self::blob_key(self.blob_paths, namespace, id, cloud_path)?`, then calls `self.home.exists(&key).await`.
- `MockSyncStorage::blob_exists` uses the existing mock `blob_key(namespace, id, cloud_path)` helper and checks `objects`.

Do not read or decrypt the blob for this preflight. Existence is the publish gate; content/auth failures remain read-path failures.

## Shared Preflight

Add one helper, reachable by both changeset and snapshot publish paths:

```rust
pub(crate) async fn ensure_publishable_blobs(
    db: &Database,
    storage: &dyn SyncStorage,
    blobs: &[BlobRef],
) -> Result<(), SyncCycleError>;
```

The helper enforces only user-provided refs:

- Ignore `Provenance::HostProvided`; those are uploaded before the preflight by the existing host upload paths.
- For `Provenance::UserProvided`, first check `db.external_blob(&blob.id).await`. If it returns `Some(_)`, fail with `SyncCycleError::BlobMissing` naming the local user-provided blob.
- Then call `storage.blob_exists(&blob.namespace, &blob.id, blob.cloud_path.as_deref()).await`. `Ok(false)` fails with `SyncCycleError::BlobMissing`; `Err(_)` fails loudly as `SyncCycleError::AssetUpload` or a more precise existing error variant.

The helper does not repair anything and does not clear external refs. It only proves that the already-recorded row points at a remote blob before publish proceeds.

## Changeset Path

After the existing host-provided inline uploads in `sync::sync`, run the preflight over the gated outgoing changeset's publishable blob refs before resolving membership and packing the outgoing envelope.

Blob refs come from `BlobDecls::ref_from_change` over `crate::changeset::walk(cs)`, filtered by operation:

- `Insert` and `Update`: require publishable user-provided blobs.
- `Delete`: no blob presence requirement.

Reuse the already-walked `changes` from the host upload block so host upload and the preflight see the same gated changeset.

## Snapshot Path

Extend `CreatedSnapshot` to carry the full publishable blob set, not only host blobs:

```rust
pub(crate) struct CreatedSnapshot {
    pub encrypted: Vec<u8>,
    pub host_blobs: Vec<BlobRef>,
    pub publish_blobs: Vec<BlobRef>,
}
```

`create_snapshot` keeps returning only `encrypted`.

`create_snapshot_with_host_blobs` derives all refs from the scoped snapshot copy once, deduped by `(namespace, id)`. `host_blobs` is the host-provided subset. `publish_blobs` is the full deduped set. Because the snapshot DB contains only current rows, every ref in `publish_blobs` is an insert/update-style live reference and requires remote availability unless host-provided upload handles it first.

Change `push_snapshot` to accept `publish_blobs: &[BlobRef]` and `db: &Database` or add a sibling wrapper that runs `ensure_publishable_blobs` immediately before writing the DB image. The preflight must happen after `upload_snapshot_host_blobs` has run and before `put_snapshot`, `put_snapshot_meta`, `put_snapshot_pointer`, or `put_head`.

Update all call sites and tests to pass the real database and blob set. Tests that do not carry blobs pass an empty slice.

## Tests

Write failing tests before implementation.

Changeset tests in the sync service/cycle test area:

- A user-provided insert/update with a remaining `local_blob_refs` row aborts before `put_changeset` or `put_head`.
- A user-provided insert/update with no external ref but no remote blob aborts before `put_changeset` or `put_head`.
- A user-provided insert/update with no external ref and an existing remote blob publishes.
- A delete of a user-provided blob row publishes even when the remote blob is absent.

Snapshot tests in `sync/snapshot.rs`:

- A snapshot containing a user-provided blob with a remaining external ref aborts before DB image, pointer, or head writes.
- A snapshot containing a user-provided blob with no external ref and no remote blob aborts before DB image, pointer, or head writes.
- A snapshot containing a valid user-provided remote blob and a host-provided blob publishes after the host blob upload path places the host object in storage.

Use the real units: `sync::sync` for changeset preflight and `push_snapshot`/cycle path for snapshot publish. Do not hand-rebuild publish ordering in a test-only copy.

## Verification

Run targeted tests covering `sync::service`, `sync::snapshot`, and blob upload/cache helpers, then:

```sh
cargo test -p coven-core
```

Also grep before commit:

```sh
rg "put_changeset|put_head|put_snapshot_pointer|put_snapshot\\(" crates/coven-core/src/sync
rg "blob_exists|ensure_publishable_blobs|publish_blobs" crates/coven-core/src
```

## Out of Scope

- No dependency pin changes in `coven-torrent` or `bae`.
- No repair of already-published broken remotes.
- No change to blob upload queue retry semantics.
