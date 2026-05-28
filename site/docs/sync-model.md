# Sync Model

coven syncs SQLite row changes as encrypted, signed changesets.

## Change capture

The SQLite session extension records changes for tables declared by the host.
coven converts those changes into sync envelopes, stamps them with a hybrid
logical clock, signs them as the local author, and encrypts them before upload.

## Ordering

Hybrid logical clocks provide sortable timestamps that include causality from the
local device and observed remote state.

Synced tables use `_updated_at` as the row conflict column.

## Conflict resolution

Conflicts resolve at row level. The row with the later `_updated_at` value wins.

This keeps conflict semantics visible to the host schema instead of hiding them
in a server-side merge layer.

## Push and pull

Push writes encrypted local changesets and encrypted blobs to storage. Pull reads
remote envelopes, verifies signatures and membership, decrypts payloads, and
applies accepted changes to SQLite.

The storage provider is only a transport and persistence layer; it does not
interpret the data.
