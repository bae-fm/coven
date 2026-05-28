# Blobs

coven handles files referenced by synced rows as encrypted opaque blobs.

## Blob plan

The host supplies a [`blob::BlobPlan`](rustdoc:trait:coven::blob::BlobPlan) that
tells coven:

- Which row changes carry blob references.
- Where those blobs live locally.
- How each blob is scoped for encryption.

This keeps domain knowledge in the host while leaving encrypted movement to
coven.

## Outbox

Blob uploads move through the cloud outbox created by
[`db::MIGRATION_SQL`](rustdoc:const:coven::db::MIGRATION_SQL).

The host can provide a
[`blob::BlobUploadObserver`](rustdoc:trait:coven::blob::BlobUploadObserver) to
observe upload progress or surface errors in its UI.

## Storage

Blobs are uploaded as encrypted files. Cloud providers see object bytes and
paths, not plaintext application files.
