# Remote Root Host-Provided Staging Reads

## Context

`SyncedTable::remote_root()` makes a table's rows and descendants always shared, and makes their blob-bearing rows resolve as Remote. `CovenHandle::write` stages newly written host-provided blobs in the local store before the SQL transaction commits. The sync cycle later uploads those bytes and drops the local-store copy.

That leaves a real Remote host-provided state before upload: the row is Remote, but the only plaintext copy is the local-store staging file that the uploader will read. Current blob reads resolve Remote to cache/cloud only, so a host that writes a Remote-root blob and reads it before the sync cycle uploads it gets a cloud/cache miss even though Coven has the upload staging bytes.

## Design

Model the local-store copy of a Remote host-provided blob as upload staging, not as Local blob storage. The Remote read path stays:

1. read pinned/cache when present;
2. for host-provided blobs only, read the local-store staging copy when present;
3. fetch from cloud.

The local-store staging probe is not a generic search across stores. It is part of the Remote + HostProvided branch and only applies while the upload staging copy exists. User-provided Remote blobs still read cache/cloud only. Local host-provided blobs still require the local store as their only copy and fail loud if absent.

Range reads follow the same source order. A staged local-store hit serves the requested range from the whole local-store file. A miss continues to cloud range-read as today.

## Implementation

- Update `crates/coven-core/src/blob/cache.rs`.
- Split Remote reads by provenance:
  - `BlobSource::Cache` still identifies Remote locality.
  - `read_remote_whole` and `read_remote_range` receive the `BlobRef` and use `blob.provenance`.
  - For `Provenance::HostProvided`, after cache miss and before cloud fetch, check the local store once.
  - Treat local-store I/O errors as errors. Treat absence as "upload staging no longer exists" and continue to cloud.
- Update module docs/comments so they describe Remote host-provided staging as a third state inside the Remote branch.
- Do not change `CovenHandle::write`; it already creates the staging copy the uploader owns.
- Do not add app-specific behavior.

## Tests

Add Coven cache tests that prove the user-visible behavior:

- A host-provided blob under a `remote_root()` row can be read immediately after `CovenHandle::write`, before any sync upload.
- The same blob can serve a range read from the staging copy.
- A user-provided Remote blob with a stale local-store file does not read that file; it goes to cache/cloud and fails with no cloud home if neither exists.
- A Local host-provided blob still reads the local store through the existing Local branch.

Run:

- `cargo fmt`
- `cargo test -p coven-core`
- `cargo test`
- `CARGO_BUILD_RUSTC_WRAPPER= cargo clippy --all-targets -- -D warnings`
