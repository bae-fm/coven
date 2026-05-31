# Overview

coven is a Rust sync layer for apps that store their domain data in SQLite.
The host app keeps ownership of its schema and database driver; coven captures
changes, encrypts them, signs them, moves them through storage, and applies
remote changes back into SQLite.

There is no coordination server. A library syncs through a
[`CloudHome`](rustdoc:trait:coven::storage::cloud::CloudHome) implementation
backed by storage the user or app already controls.

## What coven owns

- SQLite sync bookkeeping tables.
- Session-extension changeset capture.
- Hybrid logical clock timestamps.
- Per-author signing and encrypted envelopes.
- Membership chain verification.
- Library-key wrapping for members.
- Encrypted blob upload and download.
- Push, pull, restore, invite, and join operations.

## What the host owns

- The app schema and migrations.
- The SQLite driver.
- Which tables are synced.
- The local location and encryption scope of row-referenced blobs.
- Provider configuration and OAuth credentials.
- UI and product policy around sync status, invites, and restore codes.

## Flow

1. The host applies coven's bookkeeping migration.
2. The host declares synced tables.
3. coven captures local row changes through the SQLite session extension.
4. Changes are HLC-stamped, signed, encrypted, and pushed through storage.
5. Pull reads encrypted envelopes from storage and applies remote changes.
6. Blob references enqueue encrypted blob movement through the cloud outbox.

## Topics

- [Example](/docs/example) — a shared-todos host wired end-to-end.
- [Sync model](/docs/sync-model) — change capture, the cycle, schema
  versioning, backoff.
- [Storage](/docs/storage) — the `CloudHome` trait, errors, per-provider
  classification.
- [Blobs](/docs/blobs) — the blob plan, outbox, observer, layout.
- [Membership](/docs/membership) — signed chain, library-key wrapping,
  invite/join/restore.

## Status

coven is pre-1.0 and extracted from
[bae](https://github.com/bae-fm/bae).
