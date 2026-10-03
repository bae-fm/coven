# Research: Evolu and Actual Budget sync (2026-10-02)

Read from source (shallow clones of HEAD). A/ = actual/packages/,
E/ = evolu/packages/common/src/local-first/.

Current Evolu does not use a Merkle tree; it uses range-based set
reconciliation. Its blog says it dropped Actual's Merkle tree because the tree
"can't precisely identify what data changed—only when nodes last had the same
data".

## Actual Budget

### Change format and storage
- One message is one cell: dataset (table), row id, column, value, timestamp
  (A/crdt/src/proto/sync.proto:10-21).
- Values are tagged strings: `0:` null, `N:` number, `S:` string
  (A/loot-core/src/server/sync/serialization.ts:22-36).
- An insert or update sends one message per column, each with its own new
  timestamp (A/loot-core/src/server/db/index.ts:210-261).
- Every message ever received is kept in `messages_crdt (timestamp UNIQUE,
  dataset, row, column, value)`; current values live in ordinary tables; clock
  and Merkle tree are one JSON row (A/loot-core/src/server/sql/init.sql:73-85).
- Server: `messages_binary (timestamp PK, is_encrypted, content)` plus one JSON
  Merkle row.

### Clock
- Timestamp: 46-char sortable string `ISO-millis-counter(4 hex)-node(16 hex)`;
  order is millis, counter, node (A/crdt/src/crdt/timestamp.ts:21-23, 109-115).
- Send: `millis = max(old, now)`; counter +1 if millis didn't advance, else 0
  (timestamp.ts:195-230).
- Receive: `millis = max(old, now, remote)`; counter per the HLC paper
  (timestamp.ts:235-292).
- Drift over 5 minutes throws `ClockDriftError` and fails the whole sync
  (timestamp.ts:83-86, 251-253, 276-278).
- 16-bit counter; overflow throws (timestamp.ts:88, 217-219, 279-281).
- Node id: 16 random hex chars, regenerated when a device downloads a snapshot
  (A/loot-core/src/server/budgetfiles/app.ts:617-631). Duplicate-node check is
  commented out (timestamp.ts:247-250).
- Equal timestamps are treated as the same message (UNIQUE locally, INSERT OR
  IGNORE on the server).
- Merge: a message is "old" and not applied if the log has a later timestamp
  for that cell; the rest apply in timestamp order (sync/index.ts:286-364,
  452-461).

### Finding missing changes
- Merkle tree keyed by minutes since epoch in base 3, 16 digits; node hash is
  the XOR of the 32-bit murmurhash of every timestamp below
  (A/crdt/src/crdt/merkle.ts:36-54).
- Pruned to the 2 newest children per node after every batch
  (merkle.ts:141-164).
- A round: client posts all its messages newer than `since`; server stores
  them and returns all of its own newer than `since`, plus its tree. Client
  finds the first differing minute and repeats from there (merkle.ts:78-139;
  sync/index.ts:957-1035).
- Gives up `out-of-sync` after 10 rounds with the same difference or 100 total.
- Cost: each round resends everything after the divergence point both ways;
  lossy pruning can take several passes; the whole tree is JSON in every
  response; range limited to 1997–2051 (issue #1040).

### Deletes
- A delete is an ordinary newest-wins write `tombstone = 1`; reads filter
  `tombstone = 0` (db/index.ts:263-273).
- Re-add writes `tombstone = 0` with a newer timestamp (undo.ts:239-270,
  tags/app.ts:44-58).
- An edit to another column never touches `tombstone`, so a concurrent edit
  doesn't undelete.
- Rows are only physically removed by "reset sync".

### Foreign keys, unique constraints, triggers
- Foreign keys not enforced (`PRAGMA foreign_keys` never turned on).
- Dangling references handled in app code (category mapping for deleted
  categories; missing group treated as ungrouped).
- A received unique-constraint failure throws `invalid-schema` and rolls back
  the whole received batch (sync/index.ts:162-171): two devices creating the
  same tag offline would block sync. New UNIQUE constraints are now banned
  (migrations/README.md:27).
- No triggers; derived state recomputed in JS from before/after rows after
  apply commits (sync/index.ts:566-608).

### Atomicity
- Receiver applies a whole server response in one `immediate` transaction
  (sync/index.ts:430-558).
- No transaction id on the wire; grouping is per sync request, not per
  sender transaction.

### Encryption
- Optional AES-256-GCM per message; key from a password via PBKDF2-SHA512,
  10,000 iterations (encryption-internals.ts:17-94).
- Server sees plaintext timestamps, counts, sizes, file/group ids, key id;
  table/row/column are encrypted.
- Changing the password forces a sync reset.

### Several people
- Server accounts with per-file access lists; everyone shares one
  password-derived key; access enforced by the server, not cryptography.

### Schema changes
- Migrations after a cutoff must only add things (enforced by a test).
- Messages for unknown tables/columns/value formats are logged and synced but
  parked in `messages_pending`, replayed after migrations
  (sync/index.ts:88-145, 520-528; replay.ts:52-194).

### History growth
- Never compacted. Only remedy is "reset sync": delete the log and
  tombstoned rows, upload a snapshot, every device re-downloads.

### Known problems
- #8390 no-op rewrites grow the log ~16 MB/day; #4873 counter overflow on a
  178k-message full sync; #5619 large files slow; #8816 endless out-of-sync;
  #3786 one device's fast clock blocks every device; #473 the original author
  said a filesystem or Drive/Dropbox is a poor sync platform because it can't
  do atomic writes.

## Evolu

### Change format and storage
- One message: timestamp plus one row's changed columns (table, id,
  column→value, isInsert, isDelete) (E/Storage.ts:381-393, 466-479).
- App tables: every column `any` and nullable, keyed (ownerId, id)
  (E/Schema.ts:967-980).
- `evolu_history`: one row per (owner, table, id, column, timestamp) with its
  value, forever (E/Db.ts:647-675).

### Clock
- 48-bit millis, 16-bit counter, 64-bit random node id; 16-byte form sorts in
  timestamp order (E/Timestamp.ts:329, 480-483, 619-644).
- Order: millis, counter, node id.
- Counter overflow rolls into the next millisecond instead of throwing
  (Timestamp.ts:512-536).
- Drift limit 5 minutes; a change too far ahead is stored and synced but
  quarantined, not applied, and doesn't advance the clock; released at next
  startup (Timestamp.ts:63-104; Db.ts:1244, 1265-1293, 790-839).
- Identical timestamps mean the same message (Db.ts:1233-1241).
- Merge: a column is written only if history has no timestamp ≥ the incoming
  one, so arrival order doesn't matter (Db.ts:859-907).
- Documented gap: a cloned database shares a node id and can silently
  diverge (Timestamp.ts:192-242).

### Finding missing changes
- Range-based set reconciliation over a SQL skiplist of timestamps; each node
  stores a count and an XOR fingerprint (Storage.ts:250-266, 676-714).
- Ranges split by 16 until equal or under 32 items, then explicit lists
  (Protocol.ts:1515-1732). About log(n) rounds, bytes proportional to the
  difference. Needs a live peer computing fingerprints.

### Deletes
- `isDeleted` is an ordinary newest-wins soft-delete column; can be set back
  to false (Schema.ts:440-461).
- History kept forever; per-owner deletion "not implemented"
  (Evolu.ts:788-798, 1908-1911).

### Foreign keys, unique constraints, triggers
- No foreign keys; docs say an update can arrive before its insert, so
  queries should filter for complete rows.
- Unique indexes rejected by an assertion (Schema.ts:299-305); docs say
  resolve duplicates in the UI.
- Id derived from a string (`createIdFromString`) for things that must be
  one row.
- No triggers.

### Atomicity
- Local mutations in one batch share a SQLite transaction, but each row
  change gets its own timestamp.
- Receiver applies one protocol message per transaction; messages split by
  size, so a sender's batch can arrive split. A change that fails to decrypt
  is skipped alone.

### Encryption
- Owner secret → owner id, encryption key, write key via SLIP-21
  (E/Owner.ts:239-257).
- XChaCha20-Poly1305 per change, random nonce, padded to hide length
  (Protocol.ts:1852-1873).
- Timestamp also inside the ciphertext and checked against the outer one
  (Protocol.ts:1886-1888, 1943-1953).
- Relay sees owner id, all timestamps, padded sizes, write key.
- Local SQLite file encrypted too.

### Several people
- Sharing means handing over the secret or keys; no per-member keys; only the
  write key rotates, never the encryption key.

### Schema changes
- Append-only schema; unknown tables/columns quarantined and applied at the
  next startup with a matching schema (Db.ts:676-779).

### History growth
- History and relay log grow forever; intended answer is sharding data across
  owners and deleting a whole owner, not built.

### Known problems
- #712 no recovery from far-future timestamps; #709 drift limit trade-offs;
  #506 redacted deletion unchecked; #520 XOR fingerprint threat model.

## Takeaways for coven

Copy:
- Evolu's clock: 48/16/64 bits, counter rolls into the next millisecond,
  order millis→counter→node.
- Equal timestamps only arise from a shared node id: new node id for every
  install and every restored copy; detect "my node id on a timestamp I never
  wrote".
- Merge: per-cell timestamp; write a cell only if no stored timestamp is ≥
  the incoming one.
- Delete as a separate newest-wins tombstone column; edits never write it, so
  a concurrent edit doesn't undelete; re-add is an explicit newer undelete.
- No foreign-key enforcement, unique constraints or triggers on synced
  tables; ids derived from the unique key; derived data computed after apply.
- Additive-only schema, enforced by a test; park unknown changes, still
  synced onward, apply after upgrade.
- Quarantine far-future changes instead of failing sync.
- Timestamp inside the encrypted payload, checked; a change that won't
  decrypt is skipped and reported.

Different, because coven's transport is plain storage, not a relay:
- Finding missing changes: per-device logs with sequence numbers; list each
  device's folder, fetch higher numbers. Exact, no peer needed.
- Atomicity: one object per transaction, applied in one SQLite transaction;
  read position advances only past fully applied objects.
- History: snapshots with the per-device position they cover, devices
  publish how far they've applied, logs and tombstones below every member's
  position are deleted.
- Membership: a data key wrapped per member device, rotated on removal.
