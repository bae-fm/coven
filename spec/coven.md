# coven

## Contents

- [1. Overview](#1-overview)
- [2. Threat model](#2-threat-model)
- [3. Guarantees](#3-guarantees)
  - [3.1 IO bounds](#31-io-bounds)
- [4. Storage providers and access](#4-storage-providers-and-access)
  - [4.1 Open decisions for IO bounds](#41-open-decisions-for-io-bounds)
- [5. Local database](#5-local-database)
- [6. Syncing writes](#6-syncing-writes)
  - [One sync pass](sync-pass.md)
- [7. Order](#7-order)
  - [7.1 Causality](#71-causality)
  - [7.2 Timestamps](#72-timestamps)
  - [7.3 Example](#73-example)
- [8. Merge](#8-merge)
  - [8.1 Example](#81-example)
  - [8.2 Concurrent writes to one row](#82-concurrent-writes-to-one-row)
  - [8.3 Deletes](#83-deletes)
  - [8.4 Foreign keys](#84-foreign-keys)
  - [8.5 Keys and uniqueness](#85-keys-and-uniqueness)
  - [8.6 CHECK constraints](#86-check-constraints)
  - [8.7 Triggers](#87-triggers)
- [9. Members and roles](#9-members-and-roles)
- [10. Device identity](#10-device-identity)
- [11. Keys](#11-keys)
  - [11.1 Cryptography](#111-cryptography)
- [12. Joining and restore](#12-joining-and-restore)
  - [12.1 A person's new device](#121-a-persons-new-device)
  - [12.2 Adding a person](#122-adding-a-person)
  - [12.3 Losing everything](#123-losing-everything)
- [13. Removing members and devices](#13-removing-members-and-devices)
- [14. Audiences](#14-audiences)
  - [14.1 Roots and descendants](#141-roots-and-descendants)
  - [14.2 Moving rows](#142-moving-rows)
  - [14.3 Circles](#143-circles)
  - [14.4 Writes](#144-writes)
  - [14.5 References](#145-references)
  - [14.6 Leaving a circle](#146-leaving-a-circle)
  - [14.7 Deleting a circle](#147-deleting-a-circle)
- [15. Snapshots](#15-snapshots)
- [16. Files](#16-files)
  - [16.1 Kinds and where files are](#161-kinds-and-where-files-are)
  - [16.2 Storage and naming](#162-storage-and-naming)
  - [16.3 Reading ranges](#163-reading-ranges)
  - [16.4 Cache](#164-cache)
  - [16.5 Uploads and deletion](#165-uploads-and-deletion)
  - [16.6 What a device keeps about files](#166-what-a-device-keeps-about-files)
- [17. Schema changes](#17-schema-changes)
  - [17.1 Host application](#171-host-application)
  - [17.2 Coven's schema](#172-covens-schema)
- [18. Operations](#18-operations)
  - [18.1 Operations](#181-operations)
  - [18.2 Example](#182-example)
- [19. Recovery](#19-recovery)
  - [19.1 Noticing](#191-noticing)
  - [19.2 Recovering one device](#192-recovering-one-device)
  - [19.3 Resetting a store](#193-resetting-a-store)
- [20. Crates and conventions](#20-crates-and-conventions)
  - [20.1 Crates](#201-crates)
  - [20.2 Capabilities, owners and lifetimes](#202-capabilities-owners-and-lifetimes)
  - [20.3 Code conventions](#203-code-conventions)
  - [20.4 Checks](#204-checks)
- [Appendix A. SQLite behavior in synced tables](#appendix-a-sqlite-behavior-in-synced-tables)
  - [A.1 Integer primary keys](#a1-integer-primary-keys)
  - [A.2 Tables with no primary key](#a2-tables-with-no-primary-key)
  - [A.3 Unique constraints besides the primary key](#a3-unique-constraints-besides-the-primary-key)
  - [A.4 Restrict and no-action foreign keys](#a4-restrict-and-no-action-foreign-keys)
  - [A.5 CHECK constraints](#a5-check-constraints)
  - [A.6 Triggers that write synced tables](#a6-triggers-that-write-synced-tables)
  - [A.7 Schema changes](#a7-schema-changes)
- [Appendix B. Proof of convergence](proofs/merge.md), in its own file
- [Appendix C. Proof of the store log](proofs/storelog.md), in its own file
- [Store-log effects on data](proofs/storelog-data.md), coupling Appendices B and C
- [Appendix D. Storage format](format.md), in its own file
- [Appendix E. API](api.md), in its own file

## 1. Overview

- Sync for a small intimate group: a person's own devices, or a household.
- The app's data is SQLite. The app keeps its schema and writes ordinary SQL.
- Rows can carry files, like audio, images or documents, which sync with
  them.
- No server. It syncs through storage the members already have.
- Everything coven writes to the provider is encrypted: with the store key,
  a circle's key, or a member's public key.
- Only members' devices hold the store key.

## 2. Threat model

- The storage provider is assumed *honest but curious*, not *hostile*:
  - it may read what it stores;
  - it doesn't alter or withhold what it stores;
  - it sees object sizes, counts, timing, and which device writes.
- A thief with storage credentials but not the store key may be hostile.
- Deletion and withholding can't be prevented. No design can.
- Current members trust each other: each has access to the storage and
  isn't hostile on it.
- An ex-member may keep a copy of the store key they had.
- Each person keeps their restore code to themselves, on paper or shown on
  a screen ([§12](#12-joining-and-restore)); anyone who sees it can open
  the store.

## 3. Guarantees

- **Availability:** the app reads and writes with no network; writes never
  wait on sync.
- **Confidentiality:** members' data can't be read by those it isn't for:
  - the provider and outsiders can't read any of it;
  - nothing the provider sees is computed from content without a secret
    key, so it can't test whether you store a file or value it knows;
  - a circle's contents are readable only by its members
    ([§14](#14-audiences)).
- **Integrity:** members' data can't be tampered with:
  - members: no member can write as another member;
  - outsiders: nobody without the store key can alter or forge it.
- **Authorization:** only admins ([§9](#9-members-and-roles)) change
  membership, and every device enforces it.
- **Atomicity:** another device applies a write in one transaction, so a
  write is never half visible.
- **Convergence:** every device ends up with the same data:
  - a write made after seeing another wins over it on every device;
  - no device sees a write before the writes its author had seen;
  - when a concurrent write replaces or deletes a value, that value is
    recorded, not silently dropped. Values computed by a breaking migration
    that lost to a concurrent one are replaced by the winner's (§17.1).
- **Durability:** a crash loses nothing:
  - every committed write is still uploaded while this device's database
    and identity remain usable;
  - a damaged local database or restored stale copy starts again from
    storage. Edits it had not uploaded are lost, and the app is told
    (§10, §19.2);
  - every operation with several steps resumes and finishes, for example:
    - publishing sealed key copies, then the rotation entry naming their key;
    - writing a snapshot ([§15](#15-snapshots)), then deleting the logs it
      covers.
- **Revocation:** before sending a write for the first time, a device
  catches up on membership changes and lists sealed key copies at sync-pass
  start, then seals with the newest usable key. An ex-member cannot read
  other members' writes first sent by devices that already knew they had
  left, provided no copy of the sealing key is made for them after that
  pass's listing (§11).
  - A tried write retains its first attempt's key and bytes on every retry.
    Nothing is dropped or rewritten for revocation; storage access is cut
    off separately (§13).
  - The removed member's own pre-removal circle writes are outside this
    promise (§14.6). E.g. Ben queues a Gifts edit, then learns Ana removed
    him. He may still first-send that edit with the old key he already has;
    Ana's first attempts follow the revocation rule above.
- **Bounded storage:** device logs and superseded snapshots are deleted once
  snapshots cover them and the store-log decisions they depend on are final
  (§9, §15). An absent device does not hold finality back.
  - A store whose admins keep racing settles once more than 30 storage days
    have passed since its last late entry (§9).
  - The store log, sealed keys and still-required boundary snapshots remain.
- **Nothing waits silently:** whatever coven cannot apply or deliver is in
  the blocked list, with its subject and typed reason (§19.1).

### 3.1 IO bounds

The request order and cache lifetimes are in [One sync pass](sync-pass.md).
These are requirements on a device's sync, not estimates for a particular
store. Discovery charges every scoped listing page (§4.1); there is no
history-independent idle-request claim. All read, transfer, retry and local
work limits below apply without assuming a change feed or an unknown method.
The confidentiality, integrity, finality and atomicity rules still apply.

A *steady store* has a complete local catalog, unchanged storage and local
work, no evicted input needed by this pass, and no due retry, finality or
retention deadline. Its connection and credentials are usable. Starting with
an empty catalog and rebuilding an evicted cache are measured separately as
initialization or eviction work.
Repeatedly discarding a catalog at pass end is not eviction.

Count actual provider requests, including pagination, metadata lookups,
redirects, upload-session calls and SDK retries. A logical `list` call that
fetches three pages counts as three requests. Count received metadata as well
as object bodies in downloaded bytes. Also report body bytes separately, so
a zero-body pass cannot hide a listing of the whole history.

- **Request accounting:** separate discovery `D`, object reads `O`, transfer
  parts/ranges `F`, produced objects `P`, deletions `X`, and provider access
  work `A`. Also count `M`, the metadata lookups, redirects, session-control
  calls and confirmations not included in another term, and `E`, failed or
  repeated requests. No request belongs to two terms.
  - `D` is the actual complete listing-page and folder-traversal count.
    `O` counts distinct object versions whose headers or bodies are needed.
    At most two header ranges and one remaining-body stream are allowed
    per object without failures or eviction: at most `3 * O` requests.
  - `F` counts every file range, upload part and native asset-part read;
    these are excluded from the object-stream count. `P` counts each
    create or replacement publication request; session begin/status/finish
    requests belong to `M`, and their parts to `F`.
  - `X` counts deletion requests, including native deletion of asset parts.
    `A` counts permission reads, pages, grants, revokes and due job polls.
    `M` includes fresh single-object status and occupied-path comparison
    overhead. A required complete-byte comparison or snapshot verification
    body instead contributes an object to `O`.
  - Thus `R <= D + 3 * O + F + P + X + A + M + E`. Initialization and
    eviction are reported separately with the same terms. An arrival that
    causes a snapshot or historical-key sharing contributes to `P`, not an
    unspecified constant per arrival. A file's length contributes to `F`.
- **Idle requests:** `O = F = P = X = A = M = E = 0`, so `R = D`.
  Complete scans of store-log, devices, snapshots, positions and keys, plus
  each file prefix this device may delete from, determine `D`. History can
  increase their page count. More devices can increase folder traversal.
- **New objects:** checked immutable facts and retained bytes are reused.
  Charge only newly needed versions to `O`; changed positions count as new
  versions. Work induced by them stays in its own term above.
- **Downloads:** each immutable object is downloaded at most once per
  device while its retained bytes or sufficient checked facts remain.
  Reopening, another consumer, a missing prerequisite, or another pass does
  not justify another download. For replaceable positions this means once
  per observed version; for range-read files, once per retained chunk.
  A partial failed transfer is not a completed download: retry its missing
  bytes where supported and charge every retransmitted byte separately.
  Eviction permits another fetch, which is counted as eviction work.
- **Bytes:** report discovery metadata separately from new header, body and
  file-range bytes. Complete listings can repeat unchanged metadata; body
  reads cannot repeat unchanged retained bytes. A newly needed range is
  charged once. Prefix inspection uses at most two range reads. Loading fetches the
  uncached remainder in one body stream, reusing the retained prefix
  ([One sync pass](sync-pass.md#one-stream-checked-as-it-arrives)).
- **Waiting:** every automatic wait has a bounded request rate, including
  operations, invites, joining, missing keys and provider throttling.
  Delays are 1, 2, 4, …, 256, then 300 seconds between unsuccessful attempts,
  measured monotonically. Unsuccessful includes a successful request that
  finds its prerequisite still absent. A provider's `Retry-After` can only
  lengthen them.
  For `W` unchanged waits in one uninterrupted run, at most
  `W * (1 + floor(t / 1 second))` attempts start in any interval of length
  `t`; after reaching the cap it is `W * (1 + floor(t / 300 seconds))`.
  Multiply by each wait's declared request allowance, counting pages and
  retries within it. Neither pass completion nor a one-second worker tick
  resets a delay. Reopen uses the persisted T and W in §16.5. Pages are
  charged to discovery; the restart rate bound assumes no forward clock
  jump that makes a still-live wall-clock deadline appear expired.

Local work has bounds too:

- Opening unlocks store-key custody and member-key custody once each for
  the handle's session. A warm pass performs zero unlocks or passphrase
  derivations. Persist only changed keys through the same unlocked custody
  session; persistence does not derive the passphrase key again.
- Decode the saved store log at most once when the sync owner starts, on a
  read connection; keep the decoded value between passes. A warm idle pass
  performs zero full-log decodes and zero replays. Decode arriving entries
  once and replay each causally ready batch once; author-view checks remain
  distinct from the replay of the resulting received set (§9).
- An idle pass takes the database writer zero times. No writer connection
  is held across a storage request, custody call, timer wait, whole-object
  decoding, or store-log replay. Read-only decisions use read connections;
  a writer is held only to validate and commit actual state changes. An
  atomic apply can include its database work; it cannot include a download.
  Each transaction acquires the writer once; count failed transaction
  attempts separately rather than hiding repeated holds in a success count.
- An unchanged idle pass commits zero durable transactions.
  No unchanged operation rows, blocked records or last-checked timestamps
  are rewritten. Completion status is an in-memory notification (E5).
- Hot lookups use indexes: object identity; write audience and position;
  snapshot coverage; peer device and observed version; references by file
  path and retaining object; blocked subject/reporter and retry reason;
  store-log storage time; key introductions and observed copies; pending uploads and operations.
  Eager-file work selects changed references lacking complete cached bytes,
  rather than scanning every app row on every pass. Cache budget checks
  use a maintained namespace total, not a sum over every cached chunk.

Tests must count both logical operations and provider requests, transferred
bytes, custody calls, full-log decodes, writer holds and durable commits.
Run the same idle and `k`-arrival cases with more unchanged history, more
files, more devices and enough objects to cross listing pages. Assert hot
query plans with representative populated tables: a scan of unchanged app
or history rows is not an indexed lookup. Exercise warm passes, reopening,
cache eviction, interrupted streams, withheld prerequisites and rate limits
separately. Measure each term above; a logical call never hides native work.

**Ana's quiet library.** Adding 20,000 old files changes neither her idle
body downloads (zero) nor local durable work (zero). It can increase her
file-discovery pages. Ben's single new note can trigger a snapshot and an
eager file download: count the note under `O`, the snapshot under `P`, and
the file's ranges under `F`.

## 4. Storage providers and access

- A store lives on one provider: S3, Google Drive, Dropbox, OneDrive, or
  iCloud through CloudKit.
- Every member reaches the storage using their own provider account.
- For Google Drive, Dropbox and OneDrive, the app triggers `authenticate` on
  the builder or open handle. Coven builds the sign-in request, checks the
  redirect, exchanges the code and holds the tokens; none pass through the app.
  - The builder takes one presenter: open this authorization URL and return the
    provider's redirect or cancellation. Coven supplies the desktop browser and
    local listener; iOS and Android apps supply their platform sign-in sheet.
  - Before storage setup or bootstrap publishes a store, the sign-in lives in
    coven's session custody. Publication commits it to device custody. Coven
    uses a token until the provider rejects it with HTTP 401. It then
    refreshes once, commits the replacement and retries that request once.
  - Token expiry uses no device clock. Refresh failure or a second rejection
    reaches the caller; every storage consumer uses the same rule.
  - E.g. Ben sets his laptop clock back a year. Its token still works until
    the provider rejects it; one refresh lets the waiting request continue.
- On S3, each member has their own access key.
  - `RequestTimeTooSkewed` means the device clock is off, not that access
    was denied. Retry the request once with the server offset the SDK
    learned from the response.
  - If that retry still fails for clock skew, report `ClockSkew` with the
    provider's cause ([E5](api.md#e5-storage-and-sync)).
  - E.g. Ben's laptop signs with yesterday's date. The first reply supplies
    the offset; its one retry uses it without asking Ben to change his keys.

- What coven needs from a provider:
  - create an object, refusing an existing path without replacing its bytes,
    in one request or, past the provider's single request limit, through its
    resumable upload;
  - read it, whole and by range; complete-object reads stream with bounded
    buffers so the reader checks chunks without a request per chunk
    ([One sync pass](sync-pass.md#one-stream-checked-as-it-arrives));
  - list a prefix, with when storage stored each object; each query is scoped
    to that prefix, across all its pages, without a change feed;
  - get one object's status by its exact path: absent, or its complete
    encrypted size, first publication or replacement time, object id and
    revision. A revision changes on every replacement, even when size and
    timestamp match. An error is never absence;
  - delete;
  - grant and revoke a member's access, where the provider can: Google
    Drive, Dropbox, OneDrive and iCloud share with an account.
- On Drive, Dropbox and OneDrive, every directory component in D10 is a
  real provider folder. List that folder directly, never the configured
  root followed by client-side filtering. S3 uses the exact key prefix;
  CloudKit queries records under that exact path prefix in the selected zone.
  Folder ids and known path-to-object ids stay with the connection. An
  unknown Drive object uses an exact parent/name query; its id is then cached.
  Failed discovery never installs a partial catalog as complete.
  - Ana's `files/` folder can hold 20,000 photos. Reading `store-log/`
    never enumerates those photos or another store's objects.
- Status uses S3 HEAD, Drive file metadata (an exact name lookup when its id
  is unknown), Dropbox or OneDrive metadata, or a CloudKit record fetch.
  File status and upload confirmation use this call, not a folder listing.
  Count any name lookup separately. A revision proves freshness, not byte
  equality: occupied immutable paths still require §10's complete comparison.
  Snapshot confirmation still checks the complete-object checksum (§15).
  - Ben replaces his positions twice within one storage clock tick with
    equal-length bytes. Ana notices the new revision and reads the new post.
- Storage times come from the provider, on one clock for this store.
  An immutable object's time is its first complete publication; retrying
  an occupied path does not change it. Replacing posted positions or a clock object gets
  the replacement's storage time. Publication times do not go backwards.
- Device paths have one intended writer: the device they name. Retrying
  that writer uses fixed bytes. A copied identity can violate this;
  the checks in §10 detect it before sending or when a path is occupied.
  - Sealed key copies have one writer per path too. Several devices can
    supply the same key to the same member, but each uses its own
    `<store>/keys/<writer>/` folder in D10. Other writers never occupy that path.
    Duplicate valid copies have the same key meaning, not the same bytes.
  - On Google Drive, which allows two files with one name, a retry first
    looks for its own earlier copy.
- On the providers that share with an account, only the member whose
  account holds the store can share it and take it back, so:
  - that member's devices make invites ([§12.2](#122-adding-a-person));
  - any admin removes a member, and that member's access is taken back by
    the owner's device when it applies the removal
    ([§13](#13-removing-members-and-devices));
  - coven promises the access intended by the current replay. Provider
    requests take time, can fail, and need not match that intention at
    every instant;
  - each owner's device runs its grant and revoke requests one at a time.
    Before the next request it reads the current intended access. If a
    completed request has become obsolete, it records and performs the
    opposite request before reporting that account's work complete;
  - pending and failed requests remain in the existing operation journal
    and the blocked list (§18, §19.1). Security work starts immediately;
    it does not wait for entry finality.
  - Sharing is per account, not per invite: taking access back takes the
    account's access, whatever shared it.
  - So the owner's device leaves an account shared while a member in its
    store log reaches storage through it, or another of its own open
    invites names it.
  - An invite made at the same time on another of the owner's devices can
    still lose its access this way: the invited device's join fails with
    the provider's permission error, and an admin invites them again, as
    with access taken back in [§13](#13-removing-members-and-devices).
  - Taking access back removes only what grants that one account; a share
    that also grants others is left, and reported to the owner, who
    changes it in the provider.
- Every storage setup, including reconnecting an existing device, checks the
  candidate provider with the supplied credentials before reserving a store-log
  entry or committing credentials, keys, location or restore code. It creates a
  fresh sealed test object, requires a second create to be refused, verifies
  whole and ranged reads and listing, then deletes the object and checks it is
  absent. A failure names the failed check and preserves the previous connection
  ([E5](api.md#e5-storage-and-sync)).
- Every storage path starts with the store's id, including the provider
  check's temporary object ([D10](format.md#d10-paths)).
  - Several stores can share one bucket prefix, folder or zone.
  - Setup reads and writes only this store's prefix; other stores and
    unrelated objects outside it do not compete with this store.
  - E.g. Ana creates Household and Ben creates Garden in the same folder.
    Their store ids differ, so their first entries have different paths.
- Reconnecting uses the recorded provider location. Setup does not move an
  existing store to another location: its other devices still use the
  location in their restore codes.
- S3 has no standard way to make or delete access keys; each S3 provider
  has its own, so on S3 an admin makes and deletes members' keys in the
  provider's console, and coven says when.
- Posted positions live at `<store>/positions/<device>` ([§6](#6-syncing-writes)).
- Sealed circle keys live at `<store>/keys/<writer>/circles/<circle>/<key>/<member>`
  ([§14.3](#143-circles)).

### 4.1 Open decisions for IO bounds

The remaining discovery decision conflicts with retention. The complete
scoped listings in §4 remain the required mechanism; this section identifies
the missing guarantee that prevents replacing them with next-number reads.

#### Open decision: gap-free discovery after deletion

GETting the next number is sufficient only while the entire unpublished
suffix is gap-free. Retention can delete a number a reader has not seen.
Ana publishes snapshots 2 and 3, then deletes 2 because 3 covers it. Ben's
catalog ends at 1: GET 2 misses although 3 exists. Loading a snapshot cannot
resolve this without a way to discover that snapshot first.

Files also retain their attaching write's random id, support independent
uploads, and can be deleted. A missing numbered file cannot establish that
no later upload landed. A publication record written after a file would
leave a crash window; it is not an atomic solution.

Until a deletion-aware discovery rule is specified, complete scoped listings
remain required. Key copies are observed every pass. The idle request bound
must charge listing pages, including retained history; it cannot be stated
as one miss per writer. Exact-name reads also have provider-specific request
costs: [Drive downloads require a file id](https://developers.google.com/workspace/drive/api/reference/rest/v3/files/get),
so an unknown name first needs an exact parent/name query.

## 5. Local database

- The app declares which tables sync. The rest stay on the device.
- A *write* is one transaction that changes synced tables.
- A *row change* is one row's insert, update or delete within a write.
- SQLite's session extension records the row changes each write makes.
- Each write commits, together:
  - its rows, as they now stand;
  - its *write record*, waiting to be uploaded in coven's `_coven_uploads`
    table:
    - its row changes: which rows, which columns, old and new values;
    - which device wrote it, its number, its timestamp, and what it had
      read;
    - the schema version it was made with;
  - so every committed write gets uploaded, even after a crash.
- A write needs no key: the record waits unencrypted and unsigned, and its
  upload encrypts and signs it ([§6](#6-syncing-writes)).
  - So local writes need no store key, including before storage or identity
    has been initialized ([E1](api.md#e1-opening)).
- A write record, for a write that fixes a note's title and deletes a tag:

  ```
  ana-phone, write 3, 2026-10-02 12:00:00.000 #0
    had read: ben-phone 8, carol-tablet 1
    notes  row 42  update  title: "Grocry list" → "Grocery list"
    tags   "errands"  delete
  ```

- Database callbacks release their connections before propagating a panic.
  An uncommitted transaction rolls back, and later calls can reuse the writer
  and readers.
- Reads run on several read-only connections at once.
- Another process, such as a widget, can open the store for reading only
  while the app has it open.
- The app can subscribe to a query; it reruns only when rows it read change.
- Coven keeps its own internal tables in the same database. The app can't
  read or write them.
- In a table whose rowid isn't its primary key, the app can read the rowid
  but not change it once the row exists: it is SQLite's private address for
  the row, which coven doesn't sync or watch, and which VACUUM can renumber.

## 6. Syncing writes

A sync pass has three phases: catch up on membership and key copies; make
keys and provider access match that view; then sync data. The request order
and pending-work gates are in [One sync pass](sync-pass.md).

- Each device uploads its writes to its own log in storage, and the other
  devices download them.
- No two devices write the same object, so devices never have to coordinate
  their uploads.
  - Each object is one write from that device: its write record ([§5](#5-local-database)),
    encrypted.
  - A write is one object however big, encrypted in chunks like a file
    ([§16.2](#162-storage-and-naming)), so the format puts no limit on
    its size.
  - A device downloads and checks it a chunk at a time, and applies it in
    one transaction once every chunk and the signature check out.
  - Its plaintext record waits in `_coven_uploads` as one value. The record,
    sealing key ids and row overhead must fit SQLite's 1 GB value limit;
    a write exceeding it fails at commit with `DbError::TooLarge`.
  - E.g. Ana imports 50,000 notes in one transaction: one write, in many
    chunks.
  - It is named `<store>/devices/<device>/<n>`, created once and never changed.
  - `<n>` counts that device's own writes: 1, 2, 3, with no gaps.
  - Its name is part of its encryption, so the provider can't swap one
    object for another.
  - A retried upload writes the same name with the same bytes.
  - A retry finding an occupied path reads and compares the complete bytes.
    Equal bytes count as stored; different bytes reset this stale device
    (§10). A failed comparison read leaves the upload pending.
- A device uploads its writes in number order; a write never goes up
  before an earlier one.
- The first attempt records the header's and each part's sealing key ids
  in `_coven_uploads`, in the transaction marking the attempt, choosing the
  newest usable key under §11. Pre-removal circle writes follow §14.6.
  Every attempt re-seals the plaintext with those keys and signs with the
  device's member key. Each chunk's nonce is derived from its encryption
  key, path, cleartext prefix, section, index and the SHA-256 of that
  chunk's plaintext ([D11](format.md#d11-keys-contexts-and-fingerprints));
  Ed25519 signatures are deterministic, so retries produce identical bytes.
  The queue keeps no ciphertext.
  - The plaintext, format and key choices never change after the first
    attempt. Migrations convert only untried writes (§17.1).
  - E.g. Ana encrypts write 6, then restores a backup that forgot the
    attempt. A different edit at write 6 has different nonce inputs even
    if her custody id survives. The same holds for two live copies.
    Identical retries still reproduce the original object.
  - E.g. Ana's phone commits a Gifts pin offline, then reads her removal
    from Gifts before uploading it: the pin's part is sealed with the
    Gifts key she held, and counts like any write made before she read
    her removal ([§14.6](#146-leaving-a-circle)).
- Store log entries likewise keep only their plaintext and sealing key id,
  fixed before the first attempt, and re-seal identically on every retry.
  Sealed keys keep their fixed bytes because sealing uses a fresh ephemeral
  key pair; snapshots keep theirs because the database they describe changes
  ([§18](#18-operations)).
- A write record leaves `_coven_uploads` ([§5](#5-local-database)) once its upload succeeds.
- Each device remembers how far it has applied every device's log, in
  coven's `_coven_positions` table: one row per device, naming its last
  applied write's number.
- It posts positions to storage at `<store>/positions/<device>`, replacing
  its object when positions or blocked records change (D8).
  - In its own write log it posts only writes already uploaded.
  - The posted positions form a causally closed applied past: every cause
    of every included write is included, and no unfinished reload is passed.
  - Pending uploads can prevent advancing that past. Blocked records still
    travel: reuse the last publishable positions and omit fingerprints
    unless they describe exactly those positions.
  - Store-log positions count consumed entries, kept or dropped. They do
    not establish finality; that uses storage times and recorded reads (§9).
  - The object's member signature is checked against the member the received
    store log names for its device. An unknown device waits for registration;
    a wrong or missing signature is refused (§19.1).
- A missing prerequisite or failed read gets a blocked record. A complete
  immutable write or entry that fails a permanent check stops its log at
  that object. Independent logs continue (§19.1).
- A device finds devices it doesn't know yet, and their logs, by listing
  `<store>/devices/` and `<store>/store-log/` ([E5](api.md#e5-storage-and-sync)).

## 7. Order

Two mechanisms order writes:

- causality decides when a device may apply a write;
- timestamps decide which write a cell keeps.

### 7.1 Causality

- Every write records how far its device had read every other device's
  log.
  - Ana's write 3 had read Ben's log up to 8;
  - and Carol's tablet's log up to 1.
- A device applies a write only after it has applied everything that
  write's device had read.
- Every write also records how far its device had read the store log
  ([§9](#9-members-and-roles)), and is applied only after those entries.
  - So a device knows a write's circles, members and keys before it opens
    the write.
  - E.g. Ana's phone makes Gifts and pins note 4 in it. Her tablet holds
    the pin back until it has the entry making Gifts, then opens the pin
    with Gifts' key.
- A write counts only if its author was a member, and its device one of
  theirs, in the store log the write had read, judged like an entry's
  authority ([§9](#9-members-and-roles)). Otherwise refuse it as
  `NotAuthorized` and block its device's log at that number (§19.1).
  - It creates no lost values: the app must not be able to restore an
    unauthorized author's data.
  - An unknown device registration is a missing prerequisite, not proof
    of missing authority. Read the recorded store-log past before deciding.
  - E.g. Ben's write 9 names the entry removing his phone. Carol refuses
    write 9 and does not pass it or expose its values for restoration.
  - So a write made after its device read its own removal never counts,
    and a write from a device whose addition a later entry drops still
    counts if its author had read that addition.
- A device has always read its own earlier writes, except effects explicitly
  discarded by an adopted reset or breaking-change snapshot (§17.1, §19.3).
  Their numbers are passed; their discarded values are not new writes' inputs.
- So no device ever sees an effect before its cause:
  - cause: every write a write's device had read when making it;
  - effect: the write itself;
  - Ana creates note 43 in her write 5. Ben's phone reads it, and Ben adds
    attachment 9 to the note in his write 12.
  - No device ever has Ben's attachment without Ana's note.
- Two writes are concurrent when neither device had read the other's:
  - Carol's tablet goes offline before Ben's write 9. Its write 2, at
    14:00, had read Ben's log only up to 8.
  - Ben's write 9 had read Carol's tablet's log only up to 1.
  - So Carol's write 2 and Ben's write 9 are concurrent.
- A device can apply two concurrent writes in either order.
  - Ben's phone applies its own write 9 before Carol's write 2.
  - Carol's tablet applies its own write 2 before Ben's write 9.
  - Neither waits for the other.
  - Both orders converge on the same note.

### 7.2 Timestamps

- Every write carries a timestamp: the device's wall clock time, plus a
  counter.
  - Ana's write 3 is stamped 2026-10-02 12:00:00.000 #0;
  - 12:00:00.000 is the wall clock time, and #0 the counter.
- A timestamp is 48 bits of milliseconds, a 16-bit counter, and the
  device's 64-bit id.
- Timestamps sort by milliseconds, then counter, then device id.
- So no two devices' timestamps are ever equal.
- Every install, and every restored copy of a store, gets a new device id.
- The stamping rule:
  - each device keeps the latest timestamp it has seen, from its own writes
    and every write it applies, saved on disk;
  - a write waiting for a cause or a key is not seen until it applies;
  - to stamp a new write:
    - if its wall clock is past that, it uses the wall clock, counter 0;
    - otherwise it uses that latest time, counter raised by one;
    - a counter past its maximum moves to the next millisecond.
  - so a new write is always stamped later than everything its device had
    seen, whatever the devices' wall clocks say.
  - A wall clock before 1970 is never past the latest timestamp, so the
    write takes the latest time with its counter raised.
  - A wall clock past the last time 48 bits hold, in the year 10889, or a
    latest timestamp already at that time with its counter at the maximum,
    fails the write with `DbError::ClockOutOfRange`.
- A write applies once its causes and keys are available, whatever its
  timestamp says about the receiving device's clock.
- Store log entries use the same latest timestamp. Applying an entry
  advances it; an entry never waits for the wall clock either.
- Loading a snapshot adopts its applied writes' timestamps under the same
  rule. Snapshot loading and retention impose no wall-clock hold.
- A far-ahead clock can move other devices' timestamps forward.
  - E.g. Ana's phone jumps a year ahead and edits note 42. Ben's laptop
    applies that edit as soon as its causes arrive.
  - Ben's next edit is stamped after Ana's, even with his clock set right.
  - Carol's concurrent edit, made without reading Ana's and with the
    earlier stamp, loses; its value is recorded as usual (§8).
  - Until real time catches up, devices that have seen the future stamp
    keep using that time with increasing counters. Sync does not freeze.

### 7.3 Example

- Suppose Ana's phone clock runs a minute fast, and Ben's is right. In
  real time:
  - at 13:00:00, Ana edits a note:
    - her phone reads 13:01:00;
    - it stamps her write 13:01:00.000 #0;
    - it uploads the write record.
  - at 13:00:20, Ben's phone downloads Ana's write record:
    - its latest timestamp seen is now 13:01:00.000 #0;
    - it has now read Ana's log up to 4.
  - at 13:00:30, Ben edits the same note:
    - his phone reads 13:00:30, behind 13:01:00.000;
    - it stamps his write 13:01:00.000 #1;
    - it uploads the write record.
- If these were Ana's phone's 4th write and Ben's phone's 9th, storage
  now holds:
  - `<store>/devices/ana-phone/4`:

    ```
    ana-phone, write 4, 2026-10-02 13:01:00.000 #0
      had read: ben-phone 8, carol-tablet 1
      notes  row 42  update  title: "Grocery list" → "Groceries"
    signed with Ana's key
    ```

  - `<store>/devices/ben-phone/9`:

    ```
    ben-phone, write 9, 2026-10-02 13:01:00.000 #1
      had read: ana-phone 4, carol-tablet 1
      notes  row 42  update  title: "Groceries" → "Weekly groceries"
    signed with Ben's key
    ```

- Suppose Carol's tablet, coming back online, downloads Ben's write 9
  before Ana's write 4.
- Causality decides when it applies them:
  - write 9 had read Ana's log up to 4;
  - so Carol's tablet holds write 9 until it has applied write 4.
- Timestamps decide which title the note keeps:
  - same millisecond, but Ben's counter is 1 and Ana's is 0;
  - so Ben's write is later than Ana's.
- Whenever one write had read another, its timestamp is later, as here.
- Timestamps also order concurrent writes, which "had read" can't. The
  merge relies on that.

## 8. Merge

- Merging is how a device applies writes from every device, its own
  included, to build its local database.
- A *cell* is one column of one row: note 42's title is a cell.
- A device applies a write once it has every write that write had read
  ([§7.1](#71-causality)).
- Every device that applied the same writes ends with the same database,
  coven's merge tables included, whatever order the writes arrived in.
- Merging has two layers.
  - The *merged state* comes from the writes themselves: each row's
    generation ([§8.3](#83-deletes)), and each cell's winning value.
  - The *removal rules* then decide which rows the app sees, from the
    merged state and the store log ([§9](#9-members-and-roles)).
- The merged state follows two rules:
  - cells: of two values for one cell, the one with the larger timestamp
    stays;
  - deletes: a row change concurrent with a delete of its row loses
    ([§8.3](#83-deletes)).
- A value is *lost* when some write replaced it, and no write that replaced
  it had read it.
  - A lost value is recorded, so the app can show it and offer it back.
  - Lost values, including a removed row's cells, show what was written;
    deleting a reference's parent does not change them.
  - It stops being lost if a later write replaces it having read it.
- The removal rules take a row out of the app's table while a reason holds:
  - foreign keys: a row whose parent was deleted since the row pointed at
    it is taken out under cascade, restrict and no action, and a row whose
    parent is taken out is taken out with it, under every action
    ([§8.4](#84-foreign-keys));
  - CHECK constraints: a row whose merged values fail is taken out
    ([§8.6](#86-check-constraints));
  - deleted circles: a row in a circle the store log has deleted is taken
    out ([§14.7](#147-deleting-a-circle));
  - unique values: of two rows claiming one value, the row whose write has
    the larger timestamp is taken out, since the first claim keeps it
    ([§8.5](#85-keys-and-uniqueness));
  - keys in two audiences: of two present rows with one key, the store's
    row wins over a circle's ([§14.2](#142-moving-rows)).
- A removal is never stored as a delete.
  - A removed row is recorded as lost while it is out, and its `_coven_lost`
    row keeps its values, which later edits to it update.
  - Coven puts it back from there when the reason goes away, e.g. when the
    reference that made it a parent's child is pointed elsewhere.
  - The app dismissing it ([E4](api.md#e4-reading)) deletes it for good: the
    dismissal write records a delete of the row, so it never comes back,
    and its children follow its foreign keys' delete actions
    ([§8.4](#84-foreign-keys)). A dismissed lost value is dropped from
    `_coven_lost`.
- A removed row's `_coven_lost` row names every rule that holds for it once
  the rules have run, and it comes back only when none holds.
  - E.g. todos need `start <= end`, and todo 7 is in list 3.
  - Ana deletes list 3, while Ben moves todo 7's start past its end.
  - Todo 7 is taken out for both reasons, on every device, whichever rule
    a device ran first.
  - Unique and other-audience losers count their rule as holding, from the
    step that judged it.
- SQLite gives foreign keys and unique constraints no lasting name, so
  coven names them by what they are:
  - a foreign key by its columns, in order, and the table and columns it
    points at, so two keys on one column into different tables stay two;
  - a unique constraint by its terms, in order, each a column's name or an
    expression's text as written, and by its WHERE clause when it is
    partial, so `UNIQUE(title)` and `UNIQUE(lower(title))` stay two;
  - a CHECK by its name, or by its expression when it has none, as SQLite
    reports a failed one.
- The app can't see a removed row, so it can insert the same shared key
  again.
  - The write records that insert as an update of the removed row, setting
    every column.
  - The row comes back if the new values clear its reasons, and otherwise
    stays out with them, like any write to a removed row.
  - This is how an app puts a removed row somewhere else, in its audience or
    the store: its reason can still go away, and a copy under a new key
    would then show twice, while the same key shows once (in two audiences,
    the store's row wins).
  - E.g. tag "urgent" is taken out because it fails a CHECK; Ana adds
    "urgent" again with values that pass, and every device puts the row
    back with her values.
  - SQLite can't check against rows that are out, so a local insert can
    still lose.
  - E.g. tags have shared keys and unique labels, and tag b, a child of
    tag a, claimed the label "Plan" first: a loses it, and b goes with a.
  - Adding a again with "Plan" loses the same way, and both stay out.
- Every rule but unique values and keys in two audiences keeps firing when
  more rows are removed, so applying them in any order ends with the same
  rows removed.
- These two are judged once, between two passes of the others:
  1. apply the other rules until none fires;
  2. judge unique values, and keys present in two audiences, among the rows
     still present;
  3. apply the other rules again, to the losers' children.
- So every device that applied the same writes sees the same rows, whatever
  order the writes arrived in, and whatever order the rules ran in.
- The machine-checked merge model ([Appendix B](proofs/merge.md)) must
  establish this for the merged state and every removal rule, including
  rows hidden by a kept circle-deletion entry.
- When a write changes which rows are removed, coven makes the change in
  the app's table with ordinary SQL, which triggers see like any other.
- Merging uses these internal tables:
  - `_coven_writes`, one row per write the device has applied, naming:
    - the write's timestamp, which includes its device;
    - the write's number;
    - the writes it had read.
  - `_coven_columns`, one row per synced column, naming:
    - its table;
    - its column.
  - `_coven_rows`, one row per generation of each synced row, naming:
    - its table;
    - its primary key;
    - its audience;
    - the generation: how many times the row had been created, deleted or
      re-added;
    - the write that moved it there, or of several concurrent ones, the one
      with the smallest timestamp.
  - `_coven_cells`, one row per synced cell, naming:
    - its `_coven_columns` row;
    - its `_coven_rows` row;
    - the write that set it.
  - `_coven_references`, one row per reference a synced cell holds, naming:
    - the cell's `_coven_rows` and `_coven_columns` rows;
    - its `_coven_foreign_keys` row;
    - the parent's table, key, audience, and the generation it points at
      ([§8.4](#84-foreign-keys)).
  - `_coven_foreign_keys`, one row per foreign key of a synced table, naming
    it as below.
  - `_coven_claims`, one row per unique value a removed row claims, naming:
    - the row's `_coven_rows` row;
    - its `_coven_constraints` row, which names the unique constraint as
      below;
    - the row's audience and the value it claims
      ([§8.5](#85-keys-and-uniqueness)).
  - A present row's displayed values are in the app's table. When a
    reference reads differently from what was written (§8.4),
    `_coven_reference_values` keeps the written value by cell until the
    two agree again.
  - `_coven_lost`, one record for every kind of loss, naming:
    - the table, key and audience;
    - the column for a cell, or no column for a whole row;
    - the incarnation, or an excluded row change's generation;
    - the value that lost, or every value of the removed row;
    - the write that set each value;
    - what replaced it: a write that hadn't read it, the rules that removed
      the row, or a breaking change that excluded it;
    - whether its values are frozen: schema-excluded writes and rows
      deleted by a breaking migration retain old-shape values
      ([§17.1](#171-host-application));
    - for an excluded write, its identity even if a deletion has no old values.
    All losses use this record, one snapshot section and one fingerprint
    leaf shape; excluded writes need no duplicate header or row changes.
- Store-log publication uses `_coven_store_log_uploads`: the next local entry's
  number, canonical plaintext record and sealing key id.
  `_coven_store_log_key_uploads` holds its prerequisite sealed-key paths and fixed
  bytes. These contain no unsealed keys. Both commit before the first storage
  attempt; applying the published entry and its replay removes them atomically
  ([§9](#9-members-and-roles), [§18](#18-operations)).
- `_coven_key_uploads` holds the paths and fixed sealed bytes of copies shared
  for historical keys ([§11](#11-keys)). These copies have no pending
  local entry; their bytes commit before their first attempt and are removed
  after storage accepts a copy or the path is found occupied. No unsealed keys
  are kept here.
- The store log's effects that the database applies are kept with it:
  - `_coven_circles.deleted` records whether each circle is deleted; local
    writes, downloaded writes and row recomputation all read that same fact
    ([§9](#9-members-and-roles), [§14.7](#147-deleting-a-circle));
  - `_coven_applied_boundaries` names each breaking change and reset it has
    applied, with the writes its snapshot included, so a later write is
    judged against it ([§17.1](#171-host-application),
    [§19.3](#193-resetting-a-store)).
- Fingerprints ([§19.1](#191-noticing)) are kept incrementally:
  `_coven_fingerprint_leaves` holds one hash per row and per loss in
  each audience, and `_coven_fingerprint_sums` their sum per audience, so
  a write updates only the hashes of the rows it changed.
- Note 42 on Ben's phone, after Ana's write 4 and its own write 9:

  ```
  notes                            the app's own table
    id   title               body
    42   "Weekly groceries"  "milk, eggs"

  _coven_columns
    id   table   column
    1    notes   title
    2    notes   body

  _coven_rows
    id   table   key   audience   generation   write
    3    notes   42    store      1            1

  _coven_cells
    column   row   write
    1        3     7
    2        3     1

  _coven_writes
    id   timestamp                                number
    1    2026-10-01 09:12:40.511 #0  ana-phone    1
    7    2026-10-02 13:01:00.000 #1  ben-phone    9
  ```

- To find which write set note 42's title:
  - in `_coven_columns`, notes' title is column 1;
  - in `_coven_rows`, notes row 42 is row 3;
  - in `_coven_cells`, column 1 of row 3 points to write row 7;
  - in `_coven_writes`, write row 7 is Ben's phone's write 9, stamped
    13:01:00.000 #1.

### 8.1 Example

- Carol's tablet goes offline at 12:30, having read Ana's log up to 3 and
  Ben's up to 8.
- At 13:00 Ben sets the title to "Weekly groceries". His phone commits this
  as its write 9.
- At 14:00 Carol sets the title to "Shopping". Her tablet commits this as
  its write 2:

  ```
  carol-tablet, write 2, 2026-10-02 14:00:00.000 #0
    had read: ana-phone 3, ben-phone 8
    notes  row 42  update  title: "Grocery list" → "Shopping"
  signed with Carol's key
  ```

- At 14:30 the tablet comes back online and uploads the write record to
  `<store>/devices/carol-tablet/2`.
- Carol's tablet never read Ben's write 9, and Ben's phone never read
  Carol's write 2. The two writes are concurrent, so only their
  timestamps can order them.
- When a device applies one of these writes to the title, it:
  - adds the write's row to `_coven_writes`;
  - compares the write's stamp with the stamp of the title's current write;
  - if the new stamp is larger, sets the title in `notes` and points the
    title's `_coven_cells` row at the new write.
- Each device has its own `_coven_writes`, so one write gets a different row
  on each device.
- Ana's write 4, for example, is row 6 on Ben's phone and row 14 on Carol's
  tablet.
- Note 42's title on each device, in real time.

Ben's phone:

<table>
  <tr><th>Time</th><th>Applies → <code>_coven_writes</code> row</th><th>New stamp</th><th>Current stamp</th><th>Wins</th><th>Title</th><th>Title's <code>_coven_cells</code> row → <code>_coven_writes</code> row</th></tr>
  <tr><td>13:00</td><td>Ana's write 4 → 6</td><td>13:01:00 #0</td><td>12:00:00 #0</td><td>yes</td><td>"Groceries"</td><td>6</td></tr>
  <tr><td>13:00</td><td>its write 9 → 7</td><td>13:01:00 #1</td><td>13:01:00 #0</td><td>yes</td><td>"Weekly groceries"</td><td>7</td></tr>
  <tr><td>14:30</td><td>Carol's write 2 → 8</td><td>14:00:00 #0</td><td>13:01:00 #1</td><td>yes</td><td>"Shopping"</td><td>8</td></tr>
</table>

Carol's tablet:

<table>
  <tr><th>Time</th><th>Applies → <code>_coven_writes</code> row</th><th>New stamp</th><th>Current stamp</th><th>Wins</th><th>Title</th><th>Title's <code>_coven_cells</code> row → <code>_coven_writes</code> row</th></tr>
  <tr><td colspan="7"><em>12:30 · goes offline</em></td></tr>
  <tr><td>14:00</td><td>its write 2 → 13</td><td>14:00:00 #0</td><td>12:00:00 #0</td><td>yes</td><td>"Shopping"</td><td>13</td></tr>
  <tr><td colspan="7"><em>14:30 · comes back online and uploads its write 2</em></td></tr>
  <tr><td>14:30</td><td>Ana's write 4 → 14</td><td>13:01:00 #0</td><td>14:00:00 #0</td><td>no</td><td>"Shopping"</td><td>13, unchanged</td></tr>
  <tr><td>14:30</td><td>Ben's write 9 → 15</td><td>13:01:00 #1</td><td>14:00:00 #0</td><td>no</td><td>"Shopping"</td><td>13, unchanged</td></tr>
</table>

- Ben's phone and Carol's tablet applied the same three writes in opposite
  orders:
  - Ben's phone was online, so it applied Ana's write and its own at 13:00,
    and Carol's at 14:30;
  - Carol's tablet was offline, so it applied its own write first, and
    Ana's and Ben's at 14:30.
- Both devices end with "Shopping": each cell holds the largest-stamped
  write applied, whatever the order.

### 8.2 Concurrent writes to one row

- Two concurrent writes to one row can overlap or not:
  - a cell both set goes to the write with the larger timestamp ([§8.1](#81-example));
  - a cell only one sets keeps that write's value.
- All cells a write sets share its timestamp, so between any two writes,
  the one with the larger timestamp wins every cell they both set.
- At 15:00, offline, Ben sets note 42's title and Ana sets its body:

  ```
                    title                body
  Ana's write 6     ·                    "milk, eggs, bread"
  Ben's write 10    "Weekend shopping"   ·
  result            "Weekend shopping"   "milk, eggs, bread"
  ```

- No cell overlaps, so both writes keep their cells on every device.
- When concurrent writes set the same cell, the newer one wins.
- The value it replaced is lost when no write that replaced it had read it
  ([§8](#8-merge)).
- In [§8.1](#81-example), note 42's title went through four values:

  ```
  value               set by           replaced by             lost?
  "Grocery list"      Ana's write 3    Ana 4, Ben 9, Carol 2   no: all had read it
  "Groceries"         Ana's write 4    Ben 9, Carol 2          no: Ben 9 had read it
  "Weekly groceries"  Ben's write 9    Carol 2                 yes
  "Shopping"          Carol's write 2  nothing                 the current value
  ```

  - On Carol's tablet, Ana's write 4 arrives after Carol's write 2, so
    "Groceries" is lost at first; Ben's write 9, which had read it, then
    removes its `_coven_lost` row.
  - Every device ends with one row:

    ```
    _coven_lost
      cell            lost value          set by          replaced by
      note 42 title   "Weekly groceries"  Ben's write 9   Carol's write 2
    ```

- The app reads lost values through `lost_values`
  ([E4](api.md#e4-reading)) and can offer to restore one.
- Every device holds the same `_coven_lost` rows, because they follow from
  the writes alone.

### 8.3 Deletes

- A row's *generation* counts how many times it has been created, deleted
  or re-added.
  - It is odd while the row exists and even while it is deleted.
  - Each generation has its own `_coven_rows` row, naming the write that
    moved the row there.
- Each row change in a write record carries the generation the row had on
  the device that wrote it:

  ```
  ben-phone, write 11, 2026-10-02 16:00:00.000 #0
    had read: ana-phone 6, carol-tablet 2
    notes  row 43  generation 1  update  title: "Hardware store" → "Hardware store, Saturday"
  signed with Ben's key
  ```

- Edits to a row's columns change only its `_coven_cells` rows, never its
  generation.
- Deleting a row removes it from the app's table and moves its generation
  on to the next even number.
- Re-adding it moves its generation on to the next odd number.
- Its `_coven_cells` rows go with it, but its `_coven_rows` rows stay, so
  later writes to it still have a generation to lose to.
- A row change concurrent with a delete of its row loses, and its cells go
  to `_coven_lost`, replaced by the delete.
  - If the change arrives after the delete, coven sees it was made at an
    older generation.
  - If it arrives first, the delete records the cells set by writes it
    hadn't read, before the row goes.
  - So a concurrent edit never brings the row back, and never reaches it
    once it is re-added.
- When two deletes of one generation are concurrent, a value is lost only
  if neither had read it, by the rule of [§8](#8-merge).
  - The first delete to arrive records what it hadn't read.
  - If the second had read one of those values, it removes that value's
    `_coven_lost` row, which needs nothing from the deleted row.
  - The generation's `_coven_rows` row names the delete with the smaller
    timestamp, whichever arrived first.
  - E.g. Carol retitles note 43, and Ana and Ben, both offline, delete it;
    Ben had read Carol's edit and Ana hadn't.
  - A device that gets Ana's delete first records Carol's title as lost,
    and removes that `_coven_lost` row when Ben's delete arrives.
  - Every device ends with note 43 deleted and Carol's title not lost.
- E.g. at 16:00 Ana deletes note 43, "Hardware store", while Ben, offline,
  edits its title, and at 17:00 Ana re-adds it.
  - Note 43 on Carol's tablet:

    ```
    _coven_rows
      time    id   table   key   audience   generation   write
      14:45   4    notes   43    store      1            16      Ana's write 5 creates it
      16:00   5    notes   43    store      2            19      Ana's write 7 deletes it
      17:00   6    notes   43    store      3            20      Ana's write 8 re-adds it

    notes
      14:45   row 43 present
      16:00   row 43 gone
      17:00   row 43 present again
    ```

  - Ben's edit was made at generation 1, so it loses whenever it arrives,
    even if a fast clock stamps it after 17:00.
  - Generation 2's row names Ana's write 7, so even a device that gets
    Ben's edit after the re-add records it as replaced by write 7.
  - Every device records:

    ```
    _coven_lost
      cell           lost value                  set by          replaced by
      note 43 title  "Hardware store, Saturday"  Ben's write 11  Ana's write 7
    ```

- Concurrent deletes both move the generation from 1 to 2, so they count
  as one delete.
- If Ben had deleted note 43 instead of editing it, Ana's 17:00 re-add
  would still bring it back, since Ben deleted the same generation she did.
- Concurrent re-adds both move it from 2 to 3, and their cells merge
  ([§8.2](#82-concurrent-writes-to-one-row)).
- Between 14:45 and 16:00, Carol's tablet applies two writes that leave
  note 43's generation as it is:
  - Ana's write 6, her 15:00 edit to note 42's body
    ([§8.2](#82-concurrent-writes-to-one-row)), as row 17;
  - Carol's own write 3, at 15:30, which adds attachment 8 to note 43
    ([§8.4](#84-foreign-keys)), as row 18.

### 8.4 Foreign keys

- A foreign key makes one row point at another, its parent: e.g.
  attachment 9's `note_id` points at note 43, so note 43 is attachment 9's
  parent.
- Every device applies a parent's insert before the row pointing at it,
  through causality alone ([§7.1](#71-causality)):

  ```
  Ana's phone    write 5: insert note 43
                    │
                    │  Ben's phone applies write 5, then Ana's write 6
                    ▼
  Ben's phone    write 12: insert attachment 9, pointing at note 43
                 had read: ana-phone 6
                    │
                    │  another device downloads Ben's write 12 first
                    ▼
  that device    holds Ben's write 12
                    → applies Ana's write 5, inserting note 43
                    → applies Ana's write 6, and Ben's earlier writes
                    → applies Ben's write 12, inserting attachment 9
  ```

- A row change that points at a parent also carries the parent's
  generation ([§8.3](#83-deletes)).
- On the device that deletes a parent, SQLite runs each child's foreign key
  action, and the write records the results as ordinary row changes.
  - E.g. attachments point at notes with cascade, and links with set null;
    Carol added attachment 8 to note 43 in her write 3 at 15:30, and every
    device has it.
  - At 16:00 Ana deletes note 43, and her write records:

    ```
    ana-phone, write 7, 2026-10-02 16:00:00.000 #0
      had read: ben-phone 9, carol-tablet 3
      notes        row 43  generation 1  delete
      attachments  row 8   generation 1  delete
      links        row 5   generation 1  update  note_id: 43 → null
    signed with Ana's key
    ```

- A `RESTRICT` or `NO ACTION` key makes SQLite refuse that delete instead,
  if the deleting device has a child.
- A child the deleting device didn't have, because it was added or pointed
  at the parent concurrently, points at a generation that is deleted, on
  every device:
  - under cascade, restrict or no action, the removal rule of [§8](#8-merge)
    takes it out of the app's table and records it as lost;
  - under set null, it stays, and its reference is null, as SQLite would
    have made it.
  - The app's table reads the reference as null. Lost values always show
    what was written, without this substitution.
  - The cell still names the write whose reference won; it only reads as
    null, and later writes to it compete with that write's timestamp, as
    with any cell. Nothing is recorded as lost.
  - Coven's merge records keep the live reference as written, and its parent
    generation: the cell reads as null while that generation is deleted,
    and as written again if a reset brings the generation back
    ([§19.3](#193-resetting-a-store)). A later concurrent write that
    displaces it records the written value as lost, whichever write arrived
    first. Snapshots carry the reference as written.
  - E.g. links point at notes with set null, and three writes happen:

    ```
    16:00  Ben adds link 6 → note 43
    16:10  Dan, having read Ben's write, points link 6 at note 44
    16:20  Ana, having read neither, deletes note 43
    ```

  - A device that gets Ana's delete and Ben's insert first reads link 6
    as null, with the cell still naming Ben's write.
  - Dan's write then wins over Ben's, so every device ends with link 6 on
    note 44, whatever order the writes arrived in.
- Coven refuses `ON DELETE SET DEFAULT` and `ON UPDATE SET DEFAULT` on
  any foreign key of a synced table, and on a foreign key from a local
  table into a synced table, checked when the database opens and after
  migrating. The error names the table and foreign key
  ([Appendix A.8](#a8-set-default-foreign-keys)).
- Coven refuses set null on a `NOT NULL` column in any table, checked when
  the database opens and after migrating.
  - SQLite can never apply such an action: no device could delete the
    parent, and coven couldn't take it out.
- Where setting the reference to null would fail a CHECK, the child is
  taken out as under restrict.
- Coven takes rows out with SQLite's foreign keys enforced, children
  first, so SQLite's own actions reach only local children.
- Before an entry-dependent removal changes local children, coven retains
  their rows and reference values. Those actions are derived effects, not
  app edits.
  - If replay reverses the removal, restore the children and references.
    Apply any explicit local edits made since against the retained values;
    a later app delete or replacement is not undone.
  - Keep these inputs until the responsible entries are final (§9).
  - E.g. Carol's local pin points at note 7 in Gifts. A provisional circle
    deletion hides both. If the deletion drops, the note and pin return.
    If Carol explicitly deleted that pin meanwhile, it stays deleted.
- A synced row coven deletes or takes out can have children that only this
  device has, in local tables, so a local foreign key must never stop it:
  - coven refuses, checked when the database opens and after migrating, a
    foreign key from a local table into a synced table, or into a local
    table one of these reaches, unless its delete action is cascade, or
    set null on a column that allows NULL;
  - e.g. `pins(note_id REFERENCES notes ON DELETE RESTRICT)` is refused:
    Ben's phone, holding a pin, couldn't take out a note that Ana's phone
    could, and the two would differ.
- Anything else in the app's local schema that refuses such a delete, such
  as a trigger that raises an error, fails the write or the apply that
  needs it on that device, until the app changes its schema.
- A child whose parent is taken out by a rule, rather than deleted, is
  taken out with it under every action, and comes back with it.
- E.g. note 46 loses its title to note 45 and is taken out; link 7, which
  points at note 46 with set null, is taken out with it, not set to null.
- Coven refuses set null on a primary key column, checked when the database
  opens and after migrating, since nulling a key column changes the row's
  key: a delete plus an insert no write recorded.
- A taken-out child's own generation never moves, so it comes back if its
  reference is later pointed at a parent that is present.
- E.g. at 16:00, while Ana deletes note 43, Ben, offline, adds attachment 9
  to it, then moves attachment 9 to note 44:

  ```
  Ben's phone
    16:00  offline. Ben's write 12: insert attachment 9 → note 43
    16:05  Ben's write 13: attachment 9 → note 44
    16:30  online. Applies Ana's write 7: delete note 43
             attachment 9 points at note 44, so no rule applies

  Carol's tablet
    16:00  applies Ana's write 7: delete note 43
    16:30  Ben's write 12 arrives: attachment 9 → note 43
             note 43 is gone, so attachment 9 is taken out, and recorded as lost
           Ben's write 13 arrives: attachment 9 → note 44
             the reason is gone, so attachment 9 is put back
  ```

  - Both devices end with attachment 9 on note 44, and nothing lost.
  - Had Ben not moved it, both would end with attachment 9 taken out, and
    the same `_coven_lost` row, so the app can offer to put it on another
    note, by inserting it again with its key ([§8](#8-merge)).
- Re-adding a deleted parent doesn't bring back the rows taken out with it
  under cascade, restrict or no action, since they point at its old
  generation.
- Re-adding is a new insert. Any children the app wants back, it inserts in
  the same write.

### 8.5 Keys and uniqueness

#### Kinds of keys

- Each synced table declares one of two kinds of primary key:
  - independent: each new row gets a UUID, so rows made on different
    devices never share a key;
  - shared: the app derives the key from what makes the row unique, so
    equal values are one row on every device.
- Coven refuses, checked when the database opens and after migrating:
  - an independent key with no text column to hold its UUID;
  - a key SQLite picks itself, such as an integer rowid;
  - a synced table with no primary key;
  - a primary key column that allows NULL, since SQLite lets a non-integer
    key column hold NULL unless it is declared NOT NULL, and a key with a
    NULL in it names no row.
- Either kind can span several columns.
  - E.g. `note_tags(note_id, tag_id)` is a shared key.
  - Ana and Ben, both offline, each tag note 42 "urgent", and make one row.
  - An independent key over several columns needs a UUID in one of them.
  - A write that inserts a row whose independent key holds no UUID,
    version 4 or 7 in canonical lowercase form, is refused; opening never
    reads the rows to check.
- E.g. notes have independent keys, and tags have shared keys derived from
  the tag's name.
  - Ana and Ben, both offline, each add the tag "urgent".
  - Both derive the same key, so their inserts are one row, and merge
    ([§8.2](#82-concurrent-writes-to-one-row)).
- With shared keys, a device can insert a row another device is deleting.
  - E.g. Ana deletes the tag "urgent" while Ben, offline and never having
    received it, adds "urgent".
  - Ben's insert, made at generation 0, would start generation 1, which
    Ana's delete has ended, so it loses ([§8.3](#83-deletes)), and his
    cells go to `_coven_lost`.

#### Key changes

- A primary key change is a delete of the old row plus an insert of the
  new one, on every device.
- On the device changing the key, SQLite runs each child's `ON UPDATE`
  action, and the write records the results as ordinary row changes.
- A child that device didn't have sees its parent deleted, and follows its
  `ON DELETE` action by the removal rule of [§8.4](#84-foreign-keys).
- E.g. `note_tags(note_id, tag_id)` points at tags with `ON UPDATE
  CASCADE` and `ON DELETE CASCADE`, and at 16:00 Ana renames the tag
  "urgent" to "important", while Ben, offline, tags note 44 "urgent".

  ```
  Ana's phone
    16:00  renames "urgent" to "important"
             SQLite changes note_tags (42, "urgent") to (42, "important")
             her write: delete tag "urgent", insert tag "important",
                        delete (42, "urgent"), insert (42, "important")

  Ben's phone
    16:00  offline. Ben's write: insert note_tags (44, "urgent")
    16:30  online. Applies Ana's write
             "urgent" is deleted, so (44, "urgent") is taken out

  Carol's tablet
    16:00  applies Ana's write
    16:30  Ben's write arrives: insert note_tags (44, "urgent")
             "urgent" is deleted, so (44, "urgent") is taken out
  ```

- Every device ends with note 42 tagged "important", and note 44's
  "urgent" tag taken out and recorded in `_coven_lost`, so the app can offer
  to tag it again.
- Two devices changing one key concurrently are two concurrent deletes of
  its generation ([§8.3](#83-deletes)), and both new rows exist after the
  merge.

#### Unique constraints

- On one device, SQLite refuses a write that repeats a unique value, so two
  rows can claim one value only through concurrent writes.
- Of two rows claiming one value, the row whose write has the smaller
  timestamp keeps it, since the first claim to a value keeps it.
- The other row is taken out, whether its write inserted it or edited it
  to claim the value, and recorded as lost.
  - A change to its own value that ends the conflict clears that reason.
  - A loser whose value is unchanged comes back only when the winner is
    deleted, or taken out before unique values are judged.
  - Unique values are judged after the other removal rules, among the rows
    they leave ([§8](#8-merge)).
- E.g. note titles are unique, and Ana and Ben, both offline, each add a
  note titled "Groceries".
  - Ana's insert of note 45 is stamped 16:00, and Ben's of note 46 is
    stamped 16:05.
  - Every device keeps note 45, and records Ben's note 46 in `_coven_lost`.
- A row's claim dates from the latest write that set any of the
  constraint's columns in it, since that is when the row first held the
  whole value.
  - E.g. notes are unique by `(folder, title)`, and note 2 is "Draft" in
    Home.

    ```
    10:00  Ana adds note 1: "Plan" in Work
    11:00  Ben renames note 2 to "Plan"
    12:00  Carol, not having seen Ben's write, moves note 2 to Work
    ```

  - After the merge, note 2 is "Plan" in Work, like note 1.
  - Note 2's claim dates from 12:00, and note 1's from 10:00, so note 1
    keeps the value and note 2 is taken out.
- Of two claims with the same timestamp, the row with the smaller primary
  key keeps the value.
- A winner taken out after unique values are judged doesn't bring its
  loser back ([§8](#8-merge)).
  - E.g. notes are unique by title, and a sub-note points at its parent
    note with cascade.

    ```
    09:00  note 1 is "Ideas"
    10:00  Ana adds note 2, "Plan", as a sub-note of note 1
    11:00  Ben, not having seen note 2, renames note 1 to "Plan"
    ```

  - After the merge, note 2's claim from 10:00 keeps "Plan", and note 1 is
    taken out.
  - Note 2's parent is taken out, so note 2 goes too, by cascade.
  - Note 1 doesn't come back: if it did, note 2 would come back with it
    and take it out again.
  - Both are recorded in `_coven_lost`, so the app can offer them back.

### 8.6 CHECK constraints

- A CHECK on one column can't fail after a merge, since each value passed
  it on the device that wrote it.
- A CHECK on several columns can, when concurrent writes each set some of
  them.
- E.g. a row checks `start <= end`, Ana sets `start` and Ben, offline, sets
  `end`:

  ```
                start   end
  before         5      12
  Ana's write   10       ·
  Ben's write    ·       8
  merged        10       8     fails
  ```

- A row that fails after a merge is taken out, and recorded as lost.
- It comes back if a later write makes it pass, e.g. Ben setting `end` to
  20.
- Until the second write arrives, each device's row passes, since it has
  seen only one of them.
- Every device ends with the row taken out, and the same `_coven_lost` row.

### 8.7 Triggers

- The app declares each trigger on a synced table as local or shared;
  a trigger it doesn't declare shared is local ([E2](api.md#e2-declaring-synced-tables)).
- A local trigger runs on every device, for its own writes and applied ones
  alike, and writes only local tables.
  - Every change coven makes is ordinary SQL, including a late write
    taking a row back out ([§8](#8-merge)), so a count a local trigger keeps stays
    right.
  - E.g. a local trigger keeps a search index of note titles:

    ```sql
    CREATE TRIGGER notes_title_index AFTER UPDATE OF title ON notes
    BEGIN
      UPDATE title_index SET title = new.title WHERE note_id = new.id;
    END;
    ```

  - When Ben's phone applies Carol's write 2, the trigger updates Ben's
    index to "Shopping".
- A shared trigger runs only on the device making the write, writes only
  synced tables, and its writes become part of that write.
  - E.g. a shared trigger sets a note's `edited_at` when its body changes:

    ```sql
    CREATE TRIGGER notes_edited_at AFTER UPDATE OF body ON notes
    WHEN NOT coven_applying()
    BEGIN
      UPDATE notes SET edited_at = datetime('now') WHERE id = new.id;
    END;
    ```

  - Ana's write 6 records her edit and the trigger's:

    ```
    ana-phone, write 6, 2026-10-02 15:00:00.000 #0
      had read: ben-phone 9, carol-tablet 2
      notes  row 42  generation 1  update  body: "milk, eggs" → "milk, eggs, bread"
      notes  row 42  generation 1  update  edited_at: → 2026-10-02 15:00    shared trigger
    signed with Ana's key
    ```

  - Ben's phone applies both without running the trigger.
- Coven skips shared triggers when it applies another device's write,
  since the write already holds what they did.
  - SQLite can't turn off one trigger, so coven provides the SQL function
    `coven_applying()`, true while it applies another device's write.
  - A shared trigger declares `WHEN NOT coven_applying()`, as above, and
    coven refuses one that doesn't, checked when the database opens and
    after migrating.
- A trigger's write to the wrong kind of table fails.
  - SQLite's authorizer callback reports each table a statement would
    write, with the trigger doing the write, when the statement is
    prepared.
  - Coven refuses the statement when a local trigger writes a synced table,
    or a shared trigger a local one.
- A shared trigger's writes merge like any other write, so a value it
  derives can be wrong after concurrent writes.
  - E.g. a shared trigger counts a note's attachments, and Ana and Ben,
    both offline, each add an attachment to note 42:

    ```
                  attachments on note 42   attachment_count
    Ana's write   adds attachment 10       1 → 2
    Ben's write   adds attachment 11       1 → 2
    merged        3 attachments            2
    ```

  - A local trigger writing a local table counts 3 on every device.

## 9. Members and roles

- A member is a person in the store, using it from one or more devices.
  - Each member has their own keys, which identify them ([§11](#11-keys)).
  - An admin adds a member by writing their public key to the store log.
- The *store log* records changes to the store itself, separate from the
  app's writes.
  - Each change is one *entry*: add or remove a member, change a role, add
    or remove a device, make, rename or delete a circle
    ([§14](#14-audiences)) or change its members, rotate an audience's key,
    raise the store's or a circle's schema version, or reset the store or a circle to a
    snapshot ([§15](#15-snapshots)).
  - An entry names the store log entries its author had read, and is
    signed with its author's member key.
  - Entries live at `<store>/store-log/<device>/<n>`, numbered like a device's
    writes ([§6](#6-syncing-writes)).
  - The device in the path is only where the entry was written from.
  - Each device numbers its own entries, so no two entries ever get the
    same path. There is no shared sequence or cross-device slot to claim.
  - Whose entry it is comes from its signature: an entry signed with Ana's
    key is Ana's, whichever of her devices wrote it.

    ```
    <store>/store-log/ana-phone/1      create the store, Ana as admin       signed with Ana's key
    <store>/store-log/ana-phone/2      add member Ben, with his public key  signed with Ana's key
                               had read: ana-phone 1
    <store>/store-log/ben-laptop/1     add device ben-phone                 signed with Ben's key
                               had read: ana-phone 2
    <store>/store-log/ana-ipad/1       make Ben an admin                    signed with Ana's key
                               had read: ana-phone 2
    ```

  - The store's first entry creates it, names its first admin, and adds
    the device that wrote it, with its name.
- Administrative actions require storage to be reachable. Before starting
  membership, role, access, circle or reset work, catch up on the store log
  and run the candidate through the same authority and replay rules against
  that view. Return `SyncError::Rejected(DropReason)` if they reject it;
  validation has no separate membership-rejection vocabulary.
  - Without storage, or if the catch-up fails, return the typed failure
    without reserving an entry or starting an operation.
  - An already-started operation keeps its durable progress after a
    connection failure; its immutable attempted entries still retry (§18).
    An entry that lands too late is dropped under the rule below. Its
    publication is settled, but its requested change has not succeeded.
  - Two online devices can still act concurrently: each can finish reading
    before the other's entry is stored. Replay decides their result.
  - Local app writes and local schema migration remain available offline.
    Publishing a schema raise needs storage, like publishing any entry.
  - E.g. Ana's phone cannot queue “remove Ben” while offline. Once it
    catches up online, the call either starts with that membership view
    or returns the reason the change is no longer allowed. If Ana is the
    sole admin, demoting her returns `Rejected(NoAdminLeft)`—the same reason
    replay gives that candidate in that view.
- Roles:
  - several equal admins;
  - only admins add and remove members, and change roles;
  - each member adds and removes their own devices, with their own key, and
    admins can remove any device;
  - each member records their own new storage access, such as a replaced S3
    access key, so a later removal takes back the access they have now
    ([§13](#13-removing-members-and-devices));
  - a removal names a device its author had read the addition of, so it
    knows whose device it is;
  - the store always has at least one admin.
- Every device that has the same entries ends with the same member list,
  whatever order they arrived in.
  - The machine-checked store-log model ([Appendix C](proofs/storelog.md))
    must establish this for these conflict and replay rules, including
    concurrent changes to different members, circle deletion, and the
    storage-time drop and finality rules below.
- A device applies an entry once it has every entry that entry had read.
- Each device keeps, in coven's local tables:
  - `_coven_store_log`: every entry it has applied, as downloaded and
    checked, its storage time, its immutable author-view checks, whether
    it landed too late, whether replay kept or dropped it, and established
    finality;
  - `_coven_store_log_uploads`: locally authored entries with their numbers,
    timestamps, recorded past, plaintext and sealing key id fixed before upload;
    `_coven_store_log_key_uploads`: their sealed-key objects, uploaded first.
    An entry receives its number when these rows commit. A pending entry is
    published before another is made, so numbering remains contiguous. Once
    stored, it is applied through the same replay boundary as a download, and
    that transaction deletes its queue rows. If publication or its reply fails,
    the next store-log step re-seals that plaintext with the recorded key,
    deriving its nonce as in §6, and sends identical bytes;
  - `_coven_key_uploads`: historical sealed copies, fixed before their
    first attempt independently of the entry queue. Each attempt chooses
    recipients from the latest replay; a queued copy for a member outside that
    audience waits without being sent or resealed. A stored copy, including one
    another device stored first, retires its local queue row. Failure reaches
    the caller, and the next store-log call retries before publishing entries;
  - the replay's result: `_coven_members` (every member a kept entry
    added, their public keys and role, and whether they were removed),
    `_coven_devices` (every device a kept entry added, its member and name,
    and whether it is active, removed or replaced with closed log ends),
    `_coven_circles` (every circle a kept entry made, its name and whether
    it was deleted), `_coven_circle_members`, `_coven_store` (the store's id
    and name), `_coven_versions` (one row per audience:
    its schema version, snapshot, and raise entry), and
    `_coven_resets` (each audience's reset snapshot).
  - Keys themselves are only ever in key custody (§11). Key selection uses
    introductions in the received entries and the pass's sealed-copy listing
    (§11). There is no shared current-key field or local retired-key table;
    permanent sealed copies are the shared record of who can hold each key.
  - Removed members and removed or replaced devices stay. Their stored
    writes still count when authorized, checked with their keys
    ([§10](#10-device-identity)).
  - A causally ready batch of entries and its resulting replay commit in
    one transaction, so the tables always hold the replay of exactly the
    entries kept. Author-view checks still use each entry's own recorded
    past. The decoded store log stays with the sync owner between passes
    ([One sync pass](sync-pass.md#what-survives-a-pass)).
- Every store-log effect is computed from the entries received. An
  arriving entry can drop one kept before, or bring a dropped one back.
  - Keep the original inputs until every entry the effect depends on is
    final: rows, merge records, losses, file references and local sources,
    boundary snapshots and pre-migration data.
  - Applying a new replay replaces its derived database state atomically.
    If rebuilding fails, keep the previous committed state and report the
    failure; do not publish positions over the unfinished work.
  - Snapshot eligibility follows the received entries too. A snapshot's
    authorized key remains readable when membership replay changes (§11).
    Retention cannot erase inputs a later replay needs.
  - Local data changed by foreign-key actions follows the same rule (§8.4).
    Clearing a reset's error records is reversible until that reset is final.
  - App subscriptions report current results. Devices need not produce
    the same sequence of callbacks while they receive different entries.
  - E.g. Ben deletes Gifts, and Carol's device applies it. Then Ana's
    removal of Ben from the store arrives, made concurrently: a store
    removal beats a circle deletion, which is dropped. On Carol's device
    Gifts is back, with Ana in it, and its rows return, since the rule
    that took them out no longer holds ([§14.7](#147-deleting-a-circle)).
  - Entries waiting on ones they had read stay in storage until those
    arrive.
- An entry *lands* when storage publishes its complete object. Use that
  storage time, never its author's timestamp or a receiving device's clock.
  Thirty days means 30 × 24 hours.
  - An entry is *late* if it landed without having read every entry stored
    before it. Its recorded positions include its own earlier entries,
    and count both kept and dropped entries as read.
  - Drop an entry if it lands more than 30 days after the storage time of
    any entry it had not read. Exactly 30 days does not drop it.
  - Judge this against all stored entries, including dropped ones and
    entries from newly discovered devices, not only the author's past.
    Complete that check before replay or saving author-view checks.
  - This drop is permanent: keep the entry and pass its position, but
    exclude its change from every replay, including author views. It is
    not a damaged object and does not stop its device's log.
    Its keys and recorded storage access still follow §11 and §13.
  - Its author's device records `Dropped(LandedTooLate)` in the blocked
    list: “landed too late”. A new attempt at the action catches up online
    and uses a new entry; it cannot rewrite the stored one (§18).
  - Online calls read the log first, but an upload racing another device
    or retrying after a connection failure can still land late. The rule
    applies to every store-log entry, including device registrations.
    Ordinary app writes have no such age limit (§15).
- An entry is *final* once later arrivals cannot change whether replay
  keeps or drops it. At an observed storage time T:
  - entries stored strictly before T minus 30 days are final if no entry
    stored from T minus 30 days through T is late;
  - include both ends of that recent window, and include dropped entries
    when checking it. Entries with the same storage time stay on the
    same side of the window;
  - this test advances finality only after more than 30 days without a
    late entry. An entry once final stays final; later races cannot reopen it.
    The retained store log also lets a device establish finality from an
    earlier qualifying window.
- Establish T from storage before starting a complete store-log listing.
  Read and judge every entry through T, including unknown devices' logs,
  before advancing finality. A gap, unreadable entry or failed listing
  blocks that check and the cleanup that needs it (§19.1).
  - A provider-assigned stored time already observed is a lower bound on
    storage's current time. Reuse times from the pass's listings and
    successful publications, but only a time known before this store-log
    scan can be its T; a later observation serves a later scan.
  - Schedule a fresh time observation only when recorded storage times
    indicate that finality or retention could cross its next threshold.
    The device's timer schedules the check; it never proves storage age.
    An observation still short of the threshold backs off before retrying.
  - For a fresh time, replace `<store>/clock/<device>` with a sealed,
    signed clock object, then read its complete publication time with status.
    This device is its only writer. Keep one object, replacing it only when
    time-dependent work is due; positions are never reposted for time.
    The identity check precedes the replacement. The store-log listing that
    uses this T starts after the status response. A failed replacement or
    status call supplies no new T and remains visible as pending work.
  - Ana's covered log waits for its thirtieth storage day. Her timer wakes
    sync, which replaces her clock object and reads its status: two logical
    requests, plus any provider lookup or confirmation calls. If storage
    still says day 29, she keeps the log and schedules another check.
  - No device's acknowledgement is required. A sleeping device or a
    concurrently registered one has the same landing deadline as any other.
- Why this holds:
  - every entry in the recent window read all the older entries;
  - every future entry that escapes the time-based drop must also read
    all those older entries;
  - those reads force later timestamps (§7.2) and prevent concurrency with
    the older entries. Replay therefore keeps the same older prefix and
    cannot change its kept or dropped results, even through a chain.
- E.g. Ana and Carol are admins; Ana and Ben share Gifts. Ben's fast clock
  makes the timestamp order A, C, B:
  - Day 0: Ana's phone stores reset A. Ben's laptop has already attempted
    deletion B without reading A; its upload has not landed.
  - Day 28: Carol's tablet reads A and attempts removal C of Ben from the
    store. C has a later stamp than A but an earlier one than B.
  - Day 29: B lands. It missed A by 29 days, so it survives and defeats A.
    B is late, so A cannot become final on day 30.
  - Day 31: C lands. It read A and missed B by only two days, so it
    survives. C defeats B before B can defeat A; replay keeps A and C.
    C is late too, so the wait starts again.
  - After day 61, with no further late entry, all three results are final.
    Cleanup can release the inputs no longer needed by the winning reset.
    If C had instead landed after day 59, it would have missed B by more
    than 30 days and been dropped as “landed too late”.
- Only physical cleanup waits: logs covered by snapshots, a deleted
  circle's rows, files and objects, pre-breaking-change inputs, and local
  sources kept for reversible effects. Release them once every entry they
  depend on is final and their other retention checks pass (§15, §16.5).
  Key rotation and provider revocation act on the current replay immediately.
- The member list is what you get by replaying the applied entries in
  timestamp order, from the first.
  - For each causally ready arrival batch, the device replays the resulting
    received set once, from the first, so the result depends only on which
    entries it has. It does not replay the same growing set separately for
    every entry downloaded in that batch.
  - The member list an entry's author had read is the replay of just the
    entries that entry had read.
  - That past never changes once the entry is applied. The device keeps
    the checks derived from it with the entry: authority, a removed device's
    observed owner, and the circles a member removal deletes in that view. These commit in
    the same transaction as the entry and are reused on later replays.
    Replay marks start afresh for each ready batch; the permanent
    “landed too late” exclusions remain.
- At its place in the replay, an entry applies only if its author's role
  allowed it, in the member list the author had read.
- Then, an entry whose change is already in place applies and changes
  nothing.
  - E.g. Ana and Ben both add Dan: both apply, Dan is added once, and
    neither is dropped.
- Otherwise it applies only if:
  - what it needs exists: the member it changes, the device it removes,
    the member a new device belongs to, the circle it changes, the circle
    member it removes;
  - the store still has an admin after it;
  - it beats every concurrent entry already applied that it conflicts
    with.
- When an entry beats some already applied, they are all dropped, and the
  replay starts again without them.
  - A replay-dropped entry stays dropped until that replay ends; the next
    ready batch starts afresh, still excluding entries that landed too late.
- A dropped entry has a blocked record with its reason. Replay reasons
  can change until finality; “landed too late” cannot. Neither is a
  permanent refusal of the entry's bytes (§19.1).
- Whose entry it is comes from its signature, not from the device it was
  written from, so a new device adds itself, signed with its member's key
  ([§12.1](#121-a-persons-new-device)).
- Concurrent entries conflict only when they contradict:
  - different states for the same member or device, such as granting and
    removing the same membership, or assigning different roles;
  - one removes a member or device the other's change requires to exist;
  - one deletes a circle the other changes, adds to, removes from, resets
    or raises. A rotation changes no membership and conflicts with nothing;
  - two raises to the same audience and version with different snapshots,
    two resets of one audience with different snapshots, or a reset and
    raise of that same audience.
- Sharing a member id or changing keys is not itself a conflict.
  - Ben's two device additions, his new storage access and his circle
    addition can all apply.
  - Adding Carol and removing Dan both apply. Carol receives every key
    she needs; keys that reached Dan are retired for new sealing (§11).
  - Adding Carol to Gifts and removing Ben from Gifts both apply too.
  - Two removals of different members both apply if an admin remains.
    Two removals of the same member are the same membership result.
  - Renames use their existing latest-timestamp rule (§14.3), and raises
    to different versions keep the higher version (§17.1).
- For conflict comparison, an entry deletes a circle when it explicitly
  deletes it or removes its only member in the author's recorded view.
  A store removal also deletes the circles its author saw with only that
  member. This classification does not change on later replay.
- Deletion conflicts with changes requiring the circle because they cannot
  both take effect on an existing circle. The deletion wins by the tiers
  below; sharing a key or changing keys is not itself a conflict.
- Of two conflicting entries, the one that beats the other is:
  1. removing a member or device from the store;
  2. deleting a circle, including removing its last member in the author's view;
  3. removing someone from a circle;
  4. other changes, except making someone an admin;
  5. making someone an admin.
  - A lower-numbered tier wins; within a tier, the smaller timestamp wins.
  - These tiers compare conflicting entries only.
- Concurrent entries, and what applies:

  ```
  Ana adds Dan               Ben makes Carol an admin   both
  Ben adds his phone         Ben adds his laptop        both
  Ben adds his phone         Ben changes his S3 key     both
  Ben adds his phone         Ana removes Ben            removal
  Ana makes Ben an admin     Carol makes him a member   member
  Ana adds Carol             Ben removes Dan            both
  Ana removes Ben            Ben removes Ana            earlier, if only two admins
  ```

- E.g. Ana, Ben and Carol are the only admins. Each reads the store log,
  then removes another before receiving the others' entries:

  ```
  first stamp    Ana removes Ben
  second stamp   Ben removes Carol
  third stamp    Carol removes Ana
  ```

  - The first two apply. Ben's authority is checked in the view he read,
    not the member list left by Ana's earlier concurrent removal.
  - Carol's removal would leave no admin, so it drops. Ana remains admin.
  - Ben and Carol stop when their devices read their own removal; neither
    can start another removal after that.
- With only Ana and Ben as admins, their concurrent removals of each other
  leave the earlier remover as the remaining admin. The other removal drops
  because it would leave none.
  - A device that already observed its own removal stops for good (§10).
    It does not keep reading to discover a later reversal. In this case,
    another device of the remaining member may need to add it again.
- A replay-dropped entry stays dropped for that replay even if its defeater
  later drops. The next ready batch starts a fresh replay, with time-based drops
  still excluded.

## 10. Device identity

- A device is one install of the app, with its own device id, belonging to
  one member, who adds it to the store log ([§9](#9-members-and-roles)).
- Before any upload, check that this installation still owns its counters.
  - At sync start, list its write, store-log and snapshot paths completely.
    Check signed snapshot and posted positions too: covered writes may
    already have been deleted from the log.
  - A stored number beyond this database's last reserved number for that
    kind of object proves this is a stale copy. An outstanding local reservation is not stale
    merely because its upload succeeded before a crash.
  - The device id also lives in custody that backups do not copy. A missing
    or different custody id requires the same device reset.
  - Finish this check before sending writes, entries, snapshots, files,
    key copies, positions or clock observations, including sends outside
    the periodic sync loop.
    A failed check sends nothing and reports its blocker.
  - Check the store log for this id's replacement before sending too.
- A stale copy resets as a new device using storage and saved custody.
  - Stop its local writes and transfers. Discard its database and pending
    local work; do not salvage or renumber waiting writes.
  - Use the existing new-device installation path (§12.1), with a fresh id.
    Load the stored history, then allow app writes again.
  - Tell the app which old id was replaced and why, and that unsent edits
    were discarded. The reset notice is separate from ordinary sync status (E5).
  - Until replacement finishes, keep the installation unavailable. Retry
    through the existing bootstrap state, using the same new id and entry.
    Never reopen the discarded copy as a working store.
- The new device's add entry names the old id and its observed log ends:
  the last stored write and store-log entry; zero means an empty log (D6).
  - The old id is shown as replaced. This changes no membership or keys.
  - Only that member can replace the id. The new id differs from the old.
  - Concurrent replacements both apply. Their recorded ends combine by
    taking the greatest write and entry numbers, independently.
  - Readers consume the old logs through those ends. An object beyond a
    closed end is blocked pending a replacement entry that includes it;
    it is never silently accepted or discarded. Retain the relevant inputs
    until the replacement entries are final (§9).
  - A still-running old copy stops sending when it reads its replacement,
    and resets the same way. Its replacement records any additional stored
    objects, so another copy's completed upload is not lost.
  - Replacement does not exempt an entry from §9's landing rule. A stale
    registration or replacement retry that lands too late is consumed and
    reported; registering again uses a new entry after catching up online.
- E.g. Ana backs up her phone after write 5, then uploads writes 6 and 7.
  Her restored phone still has counter 5.
  - Its storage check finds 7 before it sends anything. It registers as
    `ana-phone-2`, replacing `ana-phone` through write 7, and loads 6 and 7.
  - Its first new write is `<store>/devices/ana-phone-2/1`.
    Edits made only in the restored copy are discarded, with an app notice.
- An occupied immutable path succeeds only if its complete stored bytes
  equal the bytes this attempt would send. Compare before retiring its queue
  row; a mismatch on this device's path triggers the same device reset.
  - This applies to writes, entries, snapshots, files and each writer's
    sealed key copies. Different writers use different key-copy paths.
  - A read failure keeps the attempt pending; occupation alone proves nothing.
  - Two live copies can pass the check together and send different bytes
    to one path. Content-bound nonces separate their encryption (§11.1);
    the byte check still resets the losing copy and discards its unsent edits.
- Every write record is signed with the key of the member whose device
  wrote it, so who wrote what is authentic.
- This is about authenticity, not trust.
- A write counts only if its author was a member, and its device one of
  theirs, in the store log the write had read ([§7.1](#71-causality)), so
  a write by Ana's phone counts as Ana's.
- A removed device's writes still count if they reached storage and were
  made before it read its removal.
- A device that reads its own removal, or its member's, stops syncing for
  good and tells the app ([E5](api.md#e5-storage-and-sync)).
- Removing a device takes away its storage access, so nothing it writes
  afterwards can reach other devices.

## 11. Keys

- Coven uses four kinds of key:
  - the store key, which encrypts what coven writes to storage, except a
    circle's rows and files and the sealed keys;
  - each circle's key, which encrypts that circle's rows and files
    ([§14.3](#143-circles));
  - each member's keys, which identify them ([§9](#9-members-and-roles)),
    sign their writes and store log entries, and open the store key;
  - storage credentials: each device's own sign-in to the provider, or on
    S3 its member's access key ([§4](#4-storage-providers-and-access)).
- Each key has a random id, picked when it is made, and the store log entry
  that brings it in names it ([§9](#9-members-and-roles)):
  - the store's first key, the entry creating the store; a circle's first
    key, the entry making the circle;
  - each later key, a rotation entry naming its audience. Removals carry
    no keys. After audience creation, rotation is the only way a new store
    or circle key is made.
- Every key-introducing entry—create store, create circle or rotate key—also
  carries `key_hash`, SHA-256 of the exact 32 key bytes, inside the encrypted
  entry. After opening a copy, check its audience, id and hash against that
  authorized introduction before accepting the key for ordinary reads or
  sending. A mismatch is an invalid copy and supplies neither the key nor
  evidence of its exposure.
  - A candidate needed to open its introducing entry is tentative until that
    entry's signature, authority, identity and hash checks succeed. It may
    open that entry for validation, but cannot enter the accepted key set
    or authorize other data before those checks.
  - A publisher validates its locally authored introduction against the
    caught-up view before reserving it. Its fresh key may seal that introduction
    itself; other first attempts wait until the entry is stored and kept.
    This applies to creation and rotation and does not authorize unrelated
    data with an unpublished key.
  - Dan plants chosen bytes under K2's copy path for Ana. Ana opens the box
    but rejects its hash; those bytes cannot become K2 in her custody.
- A listed box addressed to someone else cannot be opened by this device.
  Its plaintext hash therefore cannot be checked from the listing. Such a
  copy remains potential exposure under the conservative listing rule;
  ignoring forged third-party boxes requires evidence beyond D11's anonymous
  sealed box. Ana holding K2 cannot decrypt Dan's box merely by knowing K2.
- Each store key is sealed to every member's public key, and the sealed
  copies are kept in storage, at `<store>/keys/<writer>/store/<key>/<member>`.
- Sealed circle keys live at `<store>/keys/<writer>/circles/<circle>/<key>/<member>`
  ([§14.3](#143-circles)).
- Concurrent removals change membership independently. Once both arrive,
  keys known to have reached either excluded member are retired for first
  attempts. A remaining member uses an existing usable replacement or makes
  one by rotation. Removal never makes a key on behalf of a circle outsider.
- Historical keys remain readable when replay changes membership. Every
  device holding one shares it with current audience members lacking it.
  If no reachable device holds a needed key, its objects remain key waits.
  - Ana removes Ben; Carol rotates to K2. If a concurrent replay returns
    Ben, Carol supplies him K2 so he can read objects already sealed with it.
- Several keys for one audience may coexist. Each object names the one
  that sealed it; arrival of another key does not invalidate old objects.
- Ana's phone and Ben's laptop can both seal K to Carol. They publish
  `keys/ana-phone/store/K/carol` and `keys/ben-laptop/store/K/carol`;
  neither replaces the other's randomized sealed bytes.
- Each sync pass lists sealed store and circle key copies alongside the
  store log, at the paths above. Complete both reads before selecting keys
  for first attempts. A failed or incomplete listing blocks those attempts
  and records the failure (§19.1); an older listing cannot stand in for it.
  - A listed copy means its recipient may hold the key, whether or not the
    recipient has downloaded it. Copies remain in storage for good.
  - A key with a copy for someone the current replay excludes from its
    audience is retired for first attempts. Keep it for reading and for
    identical attempted retries. An addition or removal that drops does not
    erase its copies; an introducing entry returning does not erase them either.
  - Retirement is derived from this listing and replay, not remembered in
    a separate local table. A recipient returning to the audience is no
    longer excluded; other excluded recipients' copies still retire the key.
  - If no usable replacement is held, a current audience member's device
    acquires one or makes a fresh key, seals it to current members, then
    publishes a rotation entry. Old exposed copies remain listed forever;
    their presence does not cause another rotation once a usable replacement
    is held. Only a current circle member makes its replacement key (§13).
  - Several devices may rotate at once. Their keys have distinct ids;
    both remain readable. A rotation conflicts with no entry; its author
    must belong to its audience in its recorded past (D6).
- For a first attempt in an audience this member still belongs to, use
  the newest usable key this device holds, ordered by its introducing
  entry's timestamp, then key id.
  - Usable means introduced by a received authorized entry, available in
    custody, and with no copy in this pass's listing for an excluded member.
  - If no usable key exists, rotate or wait for its sealed copy, recording
    the first blocker. Never use a known exposed key as a fallback.
- Share every historical key with current members who lack it. A membership
  reversal does not revoke an authorized key introduction. Sharing adds a permanent sealed copy;
  every device's next listing can observe it.
- The guarantee uses the sending device's replay and listing from the start
  of this pass. There are no per-write or per-upload membership or key-copy
  checks. First attempts use that pass's selection; tried writes keep their bytes.
  - E.g. Ben's tablet sees Ana's removal drop and shares key K with her.
    Ben's phone sees the removal kept throughout. Its next sealed-copy
    listing still finds K's copy for Ana: it retires K and rotates before
    first sending another store write.
  - A copy made after the phone's listing is the residual window. The phone
    may first-send with K during that pass while Ana can obtain the new copy.
    Its next listing retires K if Ana is still excluded. This is accepted;
    no per-write check closes it.
  - Writes first sent before the sender learned of a removal also remain
    readable under their old keys while the recipient has storage access.
    Revocation ends that access; on S3 an admin deletes the access key in
    the console.
- Every object encrypted with a store or circle key names that key outside
  its encryption, so a reader knows which key opens it.
- A file's independent key is carried in its row's encrypted writes
  ([§16.1](#161-kinds-and-where-files-are)).
- A member's key opens every store key sealed to that member. A device
  reads the named copies from storage; key selection follows the rule above.
- Removal retires keys exposed to that member for first attempts. A remaining
  member's device publishes a rotation before using a replacement key.
  - First attempts made after learning the removal use a key eligible under
    the pass's listing, subject to the residual window above.
  - Devices keep the old keys, to read writes made before.
- Each device keeps its member's key in the OS keychain.
- Storage access, not keys, is what keeps a removed device out.
- The app can keep each of its own secrets, such as an API token, in a separate
  keychain entry, under the same access policy as coven's keys and with the
  platform's per-secret size limit. Names are arbitrary strings, encoded into
  native account names without colliding with coven's entries. Coven records
  each store's saved names in another keychain entry, outside the database.
  Saving adds the name before writing the secret; deleting removes the secret
  before the name. The list is always a superset of the secrets that exist:
  failure or a crash between steps can leave only a harmless extra name.
  Deleting the local store removes every listed secret, then the list and
  coven's other entries, even when the database is damaged, without the app
  supplying names. Removing an absent entry succeeds, so retrying after any
  failure is safe ([E1](api.md#e1-opening), [E11](api.md#e11-keys-and-secrets)).

### 11.1 Cryptography

- A member's keys are two key pairs, kept together:
  - an Ed25519 pair, which signs;
  - an X25519 pair, which opens keys sealed to the member.
- Everything coven writes to storage is encrypted with XChaCha20-Poly1305:
  write records, store log entries, snapshots, sealed keys and file
  chunks.
  - Each object's storage path is bound into its authentication, so the
    provider can't swap one object for another.
  - Each chunk of a file binds its index; each chunk of a write or a
    snapshot binds its section and its index: a write's header is
    section 0 and its part i is section i + 1, and a snapshot is one
    section.
- A write's signature covers its path and a SHA-256 hash of every byte
  before it, so a device checks it as the object streams in.
- Nonces:
  - for a file's chunks, the chunk's index, which never repeats under the
    file's own key, so retrying an upload sends the same bytes;
  - for writes and store log entries, the first 24 bytes of HMAC-SHA256
    under the encryption key, over a context binding the object's path,
    cleartext prefix, section, chunk index and SHA-256 of that chunk's
    plaintext ([D11](format.md#d11-keys-contexts-and-fingerprints)). Binding
    the prefix also separates changed authentication data. Rollback and
    copied identities need no unique-path assumption for this separation;
    it still relies on SHA-256 and the truncated HMAC resisting collisions;
  - for everything else, random.
- A key is sealed to a member with an anonymous sealed box: X25519 with
  XChaCha20-Poly1305.
- Keys for each purpose are derived from the store key, or a circle's key,
  with HKDF-SHA256 and a label per purpose: encryption and fingerprints
  ([§19.1](#191-noticing)).
- A join request's key is derived from its invite secret
  ([§12.2](#122-adding-a-person)) the same way, with its own label.
- An uploaded file's storage name is a random id, and it is encrypted with
  a random key of its own ([§16.2](#162-storage-and-naming)).

## 12. Joining and restore

- Restore and join take the app's configured, layout-scoped builder and return
  the open store ([E10](api.md#e10-joining-and-restore)). The app supplies its
  opening choices once; session-only key custody stays with that handle.
- An unfinished installation is hidden from listings and refused by ordinary
  opens. It stays at its permanent path while loading. Only after loading does
  coven commit final keys and credentials and publish it, without closing or
  moving the database. Cancellation closes it and removes its local work;
  dropping a waiting join preserves its encrypted request for an explicit retry.

### 12.1 A person's new device

- A person's new device needs their *restore code*, which holds:
  - their member key, which gets it the store key ([§11](#11-keys));
  - the store's id and name;
  - the storage location and, on S3, the member's access key.
- It gets the code in one of three ways:
  - by scanning it as a QR code on one of the person's devices that has
    the store open;
  - on Apple platforms, from iCloud Keychain, which holds the same
    contents, so a new device on the same Apple account opens the store
    with no step but a provider sign-in, where storage needs one;
  - by the person typing it in.
- The QR code is blurred until the person taps to show it.
- Where storage needs a sign-in, such as Google Drive, the new device
  asks coven to sign in to the person's own account first, through the builder's
  presenter. Restore and join use coven's held tokens. Restore codes never
  carry OAuth tokens.
- The new device then adds itself to the store log, signing with the
  member key ([§9](#9-members-and-roles)).
  It catches up before reserving its entry and follows the same landing
  rule as existing devices; finality does not depend on knowing it in advance.
- The person writes the code down when they create or join a store, as
  part of setup.
- On Apple platforms, coven writes the code to iCloud Keychain whenever it
  changes: when the person creates or joins a store, and when their S3
  key changes ([E9](api.md#e9-members-and-devices)).

### 12.2 Adding a person

- On a provider that shares with an account, the admin who invites is
  the member whose account holds the store
  ([§4](#4-storage-providers-and-access)).
- An admin adds a person with an *invite*, a code shown as a QR code,
  holding:
  - the store's id, name and location;
  - the invite's id, a one-time *invite secret*, the initial store key id
    from the creation entry, and the inviting writer's device id;
  - on S3, an access key the admin made for the new person in the
    provider's console and entered.
- E.g. Ana adds Carol, on Google Drive:
  1. Ana enters Carol's Google account email and picks her role; her phone
     shares the store's folder with that account and shows the invite.
  2. Carol's phone scans it, signs in to her own Google account, makes
     Carol's member keys, and writes a join request to storage, holding
     her public key and her device's name.
  3. Ana's phone shows the request, and Ana approves it.
  4. Ana's phone seals every store key to Carol's public key
     ([§11](#11-keys)), then writes "add member Carol" to the store log.
     - Every key, so Carol reads a late write made under an older one.
  5. Carol's phone opens the store key, adds itself to the store log, and
     Carol writes down her own restore code.
- The join request is stored at `<store>/join-requests/<invite id>`.
  - It is encrypted with a key derived from the invite secret, and signed
    with Carol's new member key.
  - Ana's device holds the secret too, so it opens and checks the request.
  - The provider sees only an encrypted object under the invite's id.
- The invite only lets a device ask; Ana's approval is what lets Carol in.
- Only the device that made the invite holds its secret, so only it shows
  and approves the invite's requests.
- Carol polls exactly
  `<store>/keys/<inviting writer>/store/<initial key>/<Carol's member id>`
  with the single-object status call, under the shared backoff. The approving
  device publishes that historical copy before the membership entry, even
  if the store has since rotated. Once present, Carol reads and retains it.
  No polling iteration scans every device's entry 1 while she waits.
  - Ana's invite names K1 and her phone. Ana rotates to K2 before approving
    Carol; approval still publishes K1 at the promised path, then supplies
    K2 and every other historical store key.
- Carol's phone learns the outcome from storage:
  - her store key sealed to her, then the store log entry adding her:
    she's in;
  - her request deleted with no key sealed to her: declined or expired.
  - It reads the store log again after observing a deleted request: approval
    may have happened after its preceding listing. Sealed keys without an
    effective membership entry keep it waiting; a dropped membership entry
    reaches the app with its replay reason.
- Before sending, Carol's phone keeps its new member keys and exact request
  bytes in its unpublished local directory, encrypted with the invite secret
  and bound to the store and invite. Restarting with the same invite and
  device name reuses them and the device id. These are not final key custody.
  - It records the publication attempt before issuing it. If the attempt's
    reply is lost and neither a request nor sealed keys exist on retry, it
    treats the invite as settled. Recreating that request could revive an
    invite the admin already declined. An admin can issue another invite.
  - Provider permission failures reach the app even when revocation followed
    decline or expiry; an unreadable request is not an absent request.
- The entry adding Carol records how she reaches storage, her provider
  account or the id of the S3 key made for her, so any admin can take it
  back later ([§13](#13-removing-members-and-devices)).
- Once the request is approved or declined, or the invite expires, Ana's
  device deletes the request object.
- Declining the request, or letting the invite expire after a day, takes
  back the storage access it granted; on S3, coven tells the admin to
  delete the key in the provider's console.
  - A day is by the inviting device's clock, and the invite expires when
    that device is next online after it.

### 12.3 Losing everything

- An admin re-invites a person who lost every device and their restore
  code.
  - They join with new member keys; their old identity stays a member
    until an admin removes it, like any member.
- Losing every member's devices and restore codes loses the store for good,
  since everything is encrypted.

## 13. Removing members and devices

- Removing a device, such as Ana's lost phone, is an entry in the
  store log ([§9](#9-members-and-roles)), and cuts the device off from storage.
- Providers can't cut off one device alone, so Ana cuts off all of hers,
  and signs in again on the ones she keeps:
  - Google Drive, Dropbox and OneDrive: she removes the app's access from
    her provider account;
  - iCloud: she removes the phone from her Apple account;
  - S3: she makes a new access key in the provider's console and enters it
    on one device, which changes her restore code; her other devices scan
    the new code, she writes it down, and she deletes the old key in the
    console.
- No keys change: the phone still holds Ana's key, but can't reach storage
  to read or write anything new.
- Removing a member is a store-log entry naming that member. It removes
  them and their devices; it contains no store key or circle-key list.
  Replay still derives deletion of circles whose sole member the author
  removed (§9). Key selection and rotation then follow §11.
  - Ana removes Carol, who shares Gifts with Ben. Ana is outside Gifts:
    she makes no Gifts key. Ben's device sees Carol's old copy and rotates
    Gifts before a first send. If Ben is offline, no remaining device sends
    new Gifts data until a member can supply a usable key.
- Applying a removal or access entry records the resulting provider work in
  the same transaction as replay. This is the only path that schedules a
  member's grant or revocation; the removal call does not do it again.
  The owner's device performs it in the pass's second phase. Elsewhere the
  pending list names the owner wait; on S3 it names the keys to delete.
- `remove_member` returns `()` once its entry is kept. Provider state is read
  only from the pending list, including retained grants and shared-account
  waits. Absence of an access record means no unresolved access work.
  - Ben removes Dan while Ana, the folder owner, is offline. Ben's call
    returns after replay keeps the entry. His list shows the owner wait;
    Ana's next pass records and performs the revocation.
- Taking back a removed member's access covers every access recorded for
  them in any entry the store log holds, kept or dropped: create-store and
  add-member entries naming them, and set-access entries signed by them.
  Dropping an entry does not undo the provider access it records.
  - Every distinct S3 key id gets a `DeleteAccessKey` blocked record until the
    admin confirms its deletion. A replacement concurrent with removal
    therefore lists both the old and the new key, even though removal
    defeats the set-access entry in replay.
  - Every provider account is revoked using the existing owner, shared-account
    and retained-grant rules ([E9](api.md#e9-members-and-devices)).
  - Applying a later access entry for a removed member records its revocation
    work in the same transaction, even when that entry is dropped and the
    removal's original operation has finished.
  - An invite's recorded access is treated the same way when the invite is
    cancelled, declined or expires; its S3 key remains listed until confirmed.
- What the entry names is fixed when the removal starts, from the member
  list the device has then; if the replay drops the entry, the removal
  starts over against the new list ([§18](#18-operations)).
- Provider access follows replay both ways. If a removal drops and the
  member is back, the owner's device grants the intended access again.
  - E.g. Ana owns the folder; Ben and Carol are its admins. Ana's phone
    applies Carol's removal of Ben and revokes Ben's share. Ben's earlier
    concurrent removal of Carol then wins. Ana's phone finishes its request,
    then grants Ben's share again; the app sees the pending work.
  - A grant that was already delivered cannot be undisclosed. Keys and
    storage cut-off bound what that access can reveal (§11, §13).
  - On S3, deleting a key in the provider's console is irreversible. If
    a returning member's recorded key was deleted, an admin supplies a
    replacement through the existing access-key calls (E9).
- The member whose provider account holds the store can't be removed:
  the store would go with their account. Removing them fails with
  `SyncError::StoreOwner`.
- On S3, `blocked()` lists keys the admin must delete until
  the admin confirms they are gone ([E9](api.md#e9-members-and-devices)), however
  the removal or expiry that needs it came about. The device retains the
  confirmation by key id: another entry, invite, retry or restart cannot
  bring that deletion notice back.
- A removed member's old keys cannot read other members' writes first sent by a device
  that already knew of the removal, subject to §11's window for copies made
  after its listing (§3). Earlier attempted writes keep their keys on retry;
  provider revocation cuts off access to those objects.
- A circle the removed member was alone in is deleted by the same entry:
  no one is left who could read it ([§14.7](#147-deleting-a-circle)).
- Adding a different member concurrently with a removal does not conflict.
  - E.g. Ana adds Carol while Ben removes Dan. Both entries apply.
  - Devices share the historical keys with Carol. Any key that reached
    Dan is retired for first attempts, and devices rotate as needed (§11).

## 14. Audiences

- A *circle* is a group of members inside a store who share rows the other
  members can't read ([§14.3](#143-circles)).
- Every synced row has an *audience*: the store, or one circle.
  - A row in the store reaches every member's devices.
  - A row in a circle reaches only that circle's members' devices.
  - E.g. a household's notes are the store's, and each person pins notes
    in a circle of their own.
- The app declares, for each synced table, how its rows get their audience:
  - a *root* table names a column holding each row's audience: `store` or
    a circle's id, and never NULL;
  - a *descendant* table names one foreign key, and each row takes the
    audience of the row it points at;
  - a table that declares neither is in the store, every row of it.
- Audience is defined once, from these declarations, and everything that
  decides where a row goes uses that one definition: writes, snapshots and
  files.
- Downloaded writes and snapshots use the same schema checks for row keys,
  columns and references. An independent key must hold a canonical UUID
  (§8.5), a root's audience cell must match its row, and a descendant's
  audience reference must name a parent in that same audience. Inserts and
  present snapshot rows supply their key, reference and audience cells;
  updates may omit unchanged cells.

### 14.1 Roots and descendants

- A descendant's declared foreign key is one column, into a synced table.
  - Its action can't be set null, since the row would lose its audience;
    coven refuses it, checked when the database opens and after migrating.
  - Following declared foreign keys from any descendant reaches a root,
    or a table in the store; a loop is refused, checked when the database
    opens and after migrating.
- E.g. todos are descendants of lists, labels are in the store, and the
  join table `todo_labels` declares `todo_id`:

  ```
  lists         root         audience column: store or a circle
  todos         descendant   through list_id
  todo_labels   descendant   through todo_id
  labels        store
  ```

- A unique constraint or shared key ([§8.5](#85-keys-and-uniqueness)) must not span audiences, since a
  device outside a circle can't see the circle's claim to a value.
  - On a root table, it must include the audience column, e.g.
    `UNIQUE(audience, title)`, so the same title in the store and in a
    circle are two separate claims.
  - On a descendant table, it must include the foreign key the table takes
    its audience from, since that parent fixes the audience.
  - E.g. `note_tags(note_id, tag_id)` is allowed: only someone who can see
    note 42 can tag it, so two rows `(42, "urgent")` are always in the same
    audience.
  - E.g. `UNIQUE(file_name)` on attachments is refused: Ana's `plan.pdf` on
    a circle note and Ben's on a store note would clash on Ana's device
    but not on Dan's, outside the circle; `UNIQUE(note_id, file_name)` is
    allowed.
  - Coven checks this when the database opens and after migrating,
    and refuses to open with an error naming the table and the constraint.

### 14.2 Moving rows

- A row moves when its root's audience column changes, or a descendant is
  pointed at a parent with another audience.
- A move is a delete of the root and its descendants in the old audience,
  and a full insert of them in the new one.
  - So devices in the new audience receive the whole subtree, not changes
    to rows they never had.
  - Devices that read only the old audience see a delete.
  - Which rows move is worked out against the database as it was before
    the write.
  - Moving them back later is a re-add ([§8.3](#83-deletes)).
- A move is an ordinary write, such as `UPDATE notes SET audience = …`.
  - It commits at once on the moving device.
  - Its moved rows' file references stay fixed: each file's key
    travels in its row ([§16.1](#161-kinds-and-where-files-are)).
- E.g. Ana moves note 42 and its attachments from the store into her
  circle: Ben's devices delete them, and Ana's insert them in the circle.
- A row's generations ([§8.3](#83-deletes)) are counted per audience, so
  `_coven_rows` has one row per table, key, audience and generation.
  - A move ends the row in one audience and starts it in the other.
  - E.g. Ana moves note 1 into her circle; Ben, outside it, sees it deleted
    and re-adds note 1 in the store.
  - The store's note 1 and the circle's note 1 are two rows, each with its
    own generations, and Carol's edit in the circle changes only the
    circle's.
- When one key is present in two audiences on a device, the app's table
  shows one of them, and the other is removed like a unique value's loser
  ([§8](#8-merge)):
  - the store's row wins over a circle's;
  - of two circles' rows, the one whose current generation started with
    the smaller timestamp wins, the write `_coven_rows` records for it.
  - A device in both circles can show a different row for that key than a
    device in one, since each reads different rows.
  - E.g. Ana moves note 1 into her circle; Ben, outside it, sees it deleted
    and re-adds note 1 in the store: Ana's devices show the store's note 1
    and take out the circle's.

### 14.3 Circles

- Circles are made, and members added to and removed from them, by entries
  in the store log, under its rules ([§9](#9-members-and-roles)).
- Any member of the store can make a circle, and is its first member.
- A circle's own members rename it, add and remove its members, and delete
  it ([§14.7](#147-deleting-a-circle)); an admin outside the circle can't.
- Concurrent renames don't conflict: both entries are kept, and the name is
  the latest kept rename's in timestamp order, as for edits to one cell
  ([§8.2](#82-concurrent-writes-to-one-row)). A rename that has read another
  has a later timestamp. Renames still conflict with deleting the circle
  ([§9](#9-members-and-roles)).
- Each circle has its own key, sealed to each of its members' public keys,
  like the store key ([§11](#11-keys)).
  - Its sealed copies live at `<store>/keys/<writer>/circles/<circle>/<key>/<member>`
    ([§11](#11-keys)).
  - It is replaced whenever someone leaves the circle.
  - Someone joining a circle gets its earlier keys too, so they can read its
    history.

### 14.4 Writes

- A circle's row changes go in the same device logs, encrypted with the
  circle's key.
  - A write that changes rows in the store and in a circle keeps each part
    encrypted with its own key, in one object.
  - A device applies the parts it can open, and skips the rest.
  - E.g. Ana adds a note and pins it, in one transaction:

    ```
    <store>/devices/ana-phone/12
      header, sealed with the store key:             ana-phone, write 12, timestamp, had read …
      part 1, chunk 0, sealed with the store key:    notes  row 47  insert "Paint colors"
      part 2, chunk 0, sealed with Ana's circle key: pins   row 4   insert note → 47
      signed with Ana's key, over all of the above
    ```

  - Ana's other devices apply both parts in one transaction; Ben's apply
    part 1, and can still check the signature, which covers part 2's
    encrypted bytes.
- A skipped part counts as applied, so a device never waits on a write it
  can't read ([§7.1](#71-causality)).
  - A device has applied every entry the write had read, so it knows each
    part's key; it skips a part only when it isn't in that key's audience.
- When replay makes a circle readable again, reload everything skipped
  while it was unreadable. A deleted circle returning follows the same rule.
  - Do not use the device's overall passed positions as proof that it read
    those parts. Load the circle's usable snapshot and remaining history.
  - Align it with the other audiences at the common point in §15. Commit
    the reload, merge records and positions together before allowing new
    app writes into the circle; until then they return
    `DbError::AudienceReloading`. Other audiences remain usable.
  - Missing keys, snapshots or history appear in the blocked list. The
    circle remains unavailable until the reload can commit.
  - E.g. Ben's laptop skips Ana's Gifts writes 9 and 10 after his removal.
    When Ben rejoins, it loads both even though its log position is 10.
    The same happens if Gifts was deleted and that deletion later drops.
- A part sealed with an authorized historical key counts like any other
  part. Current audience members wait for their copy before applying it.
  - Ana removes Ben, then Carol rotates to K2 and writes with it. Ben's
    concurrent removal of Ana can return Ben to the audience. Carol shares
    K2 with Ben; he applies those same parts. No applied value is taken back
    merely because membership changed around the key's introduction.
- All members can see that a circle exists, who writes to it, when, and how
  much.

### 14.5 References

- A row may only point at rows that everyone who can read it can also
  read.
  - A circle's row may point at rows in the same circle, or the store's.
  - A store row may point only at store rows.
- Coven refuses a write that breaks this, on the device making it.
  - That includes a move: moving note 42 into Gifts while store link 5
    still points at it is refused with `DbError::ReferenceAudience`, since
    link 5 would point at a row Ben can't read.
- Coven also refuses, with `DbError::NotInCircle`, a write that puts a row
  in a circle this member isn't in, or in no circle the store log has.

  ```
  store          notes  row 42   "Groceries"
  Ana's circle   pins   row 3    note → 42        allowed
  store          links  row 10   pin → 3          refused: Ben can't read pin 3
  Ben's circle   pins   row 2    pin → 3          refused: Ben can't read pin 3
  ```

- So every device that reads a row can check its foreign keys, and
  [§8.4](#84-foreign-keys) applies unchanged.
- E.g. when note 42 is deleted, the devices in Ana's circle apply its
  foreign key's action to pin 3.
- Which circle a row is in never depends on who is in the circle, so a
  change of members never breaks a reference.

### 14.6 Leaving a circle

- E.g. Ana and Ben share a circle, and Ana removes Ben from it.
  - The removal entry names Ben and the circle. Ana's device then rotates
    the exposed circle key, sealing its replacement to Ana alone.
  - Ben keeps the rows he already had. Remaining members' devices use a
    fresh key for first attempts once they know he has left. Earlier
    attempted writes keep their original keys (§11).
- Removing a circle's last member in the author's view deletes the circle
  ([§14.7](#147-deleting-a-circle)).
- Concurrent removals can also leave no members after the full replay,
  even though each author saw someone remaining.
  - Treat that circle as deleted while its resulting member list is empty.
    This is computed after replay, not an extra conflicting entry.
  - Its deleted-circle cause names the latest kept removal affecting that
    circle, by timestamp. Keep every contributing entry's inputs until final.
  - E.g. Ana's phone removes Ben from Gifts while her tablet removes Ana
    from the store; both had seen Ana and Ben in Gifts. Both removals apply
    if another store admin remains, and Gifts is empty and hidden.
  - Carol's concurrent addition to Gifts, if it arrives, can leave it with
    Carol instead. The rows return and Carol reloads their history (§14.4).
- A write Ben made to the circle before he had read his removal still
  counts, as with any concurrent entry ([§9](#9-members-and-roles)).
  - Ben is still in the store, so storage access doesn't stop him sending
    his own pre-removal edits with the old circle key (§6). He has no new
    circle data to reveal and cannot make new writes into it.
  - Only trust keeps him from claiming he hadn't read his removal, and
    members are trusted not to be hostile ([§2](#2-threat-model)).
- Once Ben's device reads his removal, it keeps the circle's rows it has,
  refuses new writes into the circle with `DbError::NotInCircle`, and no
  longer snapshots or fingerprints it.

### 14.7 Deleting a circle

- Any member of a circle can delete it.
- Deleting a circle publishes one store-log entry (§9). It makes no row
  write and advances no row generation.
- The deleted-circle rule hides every row in that circle while the replay
  keeps the deletion, including rows arriving later.
  - The loss record names `DeletedCircle` and the entry responsible.
    It describes a deleted circle, not a conflicting edit that lost.
  - Values and merge records remain available if the entry is dropped.
    Then the rule disappears and the rows return, unless another rule
    still hides them.
- A device that has applied the deletion refuses new writes into the circle.
- E.g. Ana and Ben share Gifts, holding notes 7 and 8. Ben deletes it while
  Ana's phone, which has not received the deletion, adds note 9.

  ```
  <store>/store-log/ben-phone/4   delete circle Gifts
  ```

  - All three notes are hidden under the same deleted-circle rule.
  - Ana's app can offer to put note 9 elsewhere by inserting it with the
    same key (§8), so a returning circle cannot create a second copy.
  - If Ana concurrently removed Ben from the store, her higher-tier entry
    wins. Ben's deletion drops, and all three notes return on replay.
- Devices outside the circle read only the entry, never its row values.
- A row hidden by circle deletion keeps its local file sources and its
  queued files keep uploading under the ordinary file rules. Hiding a row
  neither pauses its queue nor changes its fixed file reference.
  - Ana attaches a photo before Ben's Gifts deletion arrives. The photo
    still uploads while its row is hidden. If the deletion drops after Ana's
    phone goes offline, the returning row's photo is already in storage.
  - A real row delete releases sources only under the ordinary reference
    and finality checks; a circle deletion is not such a row delete.
- Physical cleanup waits for the deletion's finality (§9) and the snapshot
  and file-reference checks (§15, §16.5). Loss records that still name a
  file retain it.

## 15. Snapshots

- A snapshot is the synced tables and coven's merge tables
  ([§8](#8-merge)) as one device has them, encrypted, with how far into
  every log they reach.
  - A device's own `_coven_uploads`, `_coven_operations` and `_coven_blocked`
    aren't in it. Loading keeps that local state, except that committing a
    changed reset suppresses blocked records (§19.1). Their inputs
    remain until that reset is final (§9).
  - Snapshots live at `<store>/snapshots/<audience>/<device>/<n>`, where the
    audience is `store` or a circle's id.
  - Its prefix, outside its encryption, names its audience, its key and
    its positions, so any device can choose one and decide what it
    covers by verifying the prefix's own member signature, without opening
    its encrypted data. Unverified positions never establish coverage.
  - A snapshot is one object, encrypted in chunks like a write
    ([§6](#6-syncing-writes)), and written and loaded a chunk at a time.
    Its author's member key signs both its prefix and its complete sealed
    bytes. Every read verifies the prefix against the member the applied
    store log names for the device in its path; loading also verifies the
    complete object's signature before applying anything from it (D9).
  - A snapshot *covers* a write when the write is within its positions:
    `<store>/snapshots/store/ana-phone/3` covers ana-phone's writes 1 to 40.
- A device writes one for an audience once that audience's parts after
  its latest snapshot add up to more bytes than that snapshot, or than
  1 MiB while the audience has none.
  - Both sizes count encoded plaintext, before sealing adds chunk overhead.
    The latest snapshot's size follows from its listed object size and D9's
    fixed chunk layout. Choosing it uses its cached verified prefix, reading
    that prefix only when its checked local record is missing. Growth sums
    the part lengths recorded when writes were authored or applied; neither
    decision rereads unchanged headers or decrypts snapshot rows
    ([One sync pass](sync-pass.md#what-survives-a-pass)).
- Before uploading a snapshot, its writer opens the sealed temporary file
  through the snapshot reader: decrypt, verify both signatures, and parse
  and check every record.
  - Its fingerprint must equal the database state the writer captured,
    at those same positions and schema version. Writes committed since
    that capture do not change the comparison.
  - A failed check stops publication and reaches the caller.
- After upload, compare the provider's checksum of the complete stored
  bytes with the local checksum, using the provider's checksum algorithm.
  - For multipart uploads, compare the checksum of the complete object,
    not a part checksum or an identifier that is not a checksum.
  - A provider without a complete-object checksum must return the stored
    bytes for this check. A mismatch is an integrity failure, not success.
- Readers still choose by the authenticated prefix; they do not certify a
  snapshot for other devices by opening its body.
  - E.g. Ana's phone discovers its published snapshot was damaged. Ben's
    healthy laptop writes a newer snapshot; new devices load that one.
  - If Carol already loaded incorrect data, a person resets from Ben's
    trusted copy (§19.3). Keeping one older snapshot is not a recovery rule.
- The *latest* snapshot of an audience is the one covering the most writes,
  counted over every log; a tie goes to the smaller path.
- Until an audience has a snapshot, a new device reads every log from the
  start; no log object is deleted before a snapshot covers it.
- So loading a snapshot and the writes after it costs at most about twice
  the snapshot.
- Two devices can write one at the same time, and both are correct:

  ```
  <store>/snapshots/store/ana-phone/3     ana-phone up to 40, ben-laptop up to 22
  <store>/snapshots/store/ben-laptop/1    ana-phone up to 38, ben-laptop up to 25
  ```

- A new device loads either, then fetches every write after its positions,
  and ends up in the same place.
- A device snapshots only what it can read.
  - Any device snapshots the store's rows.
  - A device of one of a circle's members snapshots that circle's rows,
    separately, sealed with the circle's key ([§14.3](#143-circles)).
- A new device loads the store's latest snapshot and its member's circles',
  then the writes after them.
  - The snapshots can reach different points in each log, so the device
    starts each log at the lowest point any of them reaches, and a write's
    part that its own audience's snapshot already covers counts as applied
    and is skipped.
  - E.g. the store's snapshot reaches ana-phone 40 and Gifts' reaches 38:
    the device applies ana-phone 39 and 40, but only their Gifts parts.
  - It applies them in the same transaction that loads the snapshots,
    downloaded first, so no write ever sees one audience at 40 and another
    at 38: every audience reaches the highest point any snapshot reaches.
  - Reloading a device that already has writes works the same way, and the
    highest point also covers what its waiting writes had read. Eligible
    waiting parts apply on top in that transaction; the reset rule (§19.3)
    ignores pre-reset parts outside the chosen snapshot.
  - These writes are all still in storage: a log object is deleted only
    once snapshots cover every part of it.
  - During an ordinary snapshot reload, the app can write throughout; a
    write made while downloads run is one more waiting write.
  - When a kept reset or breaking-schema entry is received, writes into
    its audience wait for the atomic reload with `DbError::AudienceReloading`.
    Other audiences stay writable. A write must not claim to have adopted
    the boundary while still using the state it discards.
- A reload also covers the device's already uploaded own writes and their
  past: its next write implicitly reads every earlier own write ([§7.1](#71-causality)).
  An audience whose snapshot is not being replaced keeps its current positions
  when computing the common point, including an audience with no rows.
- These positions govern device-write logs. Before loading a snapshot, the
  device has applied at least the store-log positions it names; loading does
  not replace store-log entries or rewind their applied positions.
- A snapshot from an older schema can load across additions: SQLite supplies
  the new columns' defaults. It cannot cross a breaking change, and a newer
  snapshot waits for the app's schema to update.
- Loading an audience's snapshot replaces that audience's rows and merge
  records; other audiences keep theirs, and the removal rules run again on
  rows that point at changed ones, as after any write
  ([§8.4](#84-foreign-keys)).
- Every loss travels in the same snapshot section ([D7](format.md#d7-snapshots)),
  retaining the row, column when present, written values, setters and cause.
  Frozen losses do not require current schema columns. A migration-deleted
  row keeps its generation records; the frozen values need no live cells.
  Loading preserves them in `_coven_lost`, and every loss counts in its
  audience's fingerprint (§19.1).
- Each device advances its posted positions only over fully realized work.
  It can post new blocked records without advancing them (§6).
- Deleting a log object requires all of the following:
  - snapshots cover every part of it;
  - every store-log entry that makes this coverage usable, or makes the
    object unnecessary, is final (§9);
  - every device's posted write position has passed it, or storage has
    held it for 30 days.
  - Age compares storage times only: use the observed storage time T from
    §9 and the object's first stored time. A quiet store's due observation
    uses the per-device clock object in §9; unchanged positions are
    not refreshed. Device-made write and entry timestamps never establish
    storage age.
  - E.g. Ana's phone clock jumps ahead a year. A log stored yesterday is
    still only one storage day old, so that jump cannot release it.
  - Every device means every active device in the store log; removed and
    replaced devices are not waiting readers here. Finality still follows §9;
    one that has never posted counts as having read nothing.
  - Coverage needs only the write header's part audiences, authenticated by
    its store-key encryption and bound to its path and prefix. Retention
    uses the facts retained when that device authored, applied or checked
    the write. Only missing header facts require a read; keep its result
    across passes. A header-only read needs no parts or whole-object author
    signature. These fields describe
    the object being deleted; they grant no author authority and apply no
    rows. Loading a write still checks its complete signature (§6).
- A write waiting for entries or a key copy holds retention back, with a
  `Retention` blocked record naming that first prerequisite. It does not
  fail the pass. Arrival of the prerequisite permits another attempt.
- A log with a permanent refusal is not read at or past that object,
  including for retention and reload. Keep those objects and any files
  whose absence cannot be proved without them.
  - A reload requiring that gap fails atomically with `SyncError::Blocked`.
  - A snapshot covering it can load without reading it. The refusal remains
    recorded until the reset or update rules of §19.1 permit a new attempt.
- A device deletes a snapshot of its own once a newer one of the same
  audience covers everything it covers, and the entries that make that
  replacement usable are final (§9).
  - A snapshot named by a kept reset or version-raise entry that changes
    the replayed state stays: new devices need its authenticated coverage
    to judge late writes and select a snapshot following that boundary.
  - Each device deletes its own log objects.
  - A removed device's are deleted by another device of the same member,
    since they were uploaded with that member's account.
  - A removed member's are deleted by a device of the member whose provider
    account holds the store.
    On S3, where access-key ids do not identify their account, the member
    recorded by the kept creation entry performs this deletion.
  - On Google Drive only an uploader can delete their files, so a removed
    member's just leave the store's folder, and stay in their own account.
  - A device that never comes back keeps its covered log objects until it
    is removed.
- A device that needs writes already deleted loads the latest snapshot
  instead, like a new device.
  - A missing write that a snapshot covers counts as deleted, not late
    ([§19.1](#191-noticing)).
  - Its own writes still waiting in `_coven_uploads` keep their numbers, and
    it uploads them after.
  - Every device judges them under the schema and reset rules (§17.1,
    §19.3). Ordinary late writes apply once their prior reads are covered
    or applied; pre-reset writes outside the chosen snapshot are ignored.
  - E.g. Ana's old phone made writes 31 to 33 offline, then stayed offline
    for a year:

    ```
    1. it loads the latest snapshot, which reaches ana-old-phone up to 30
    2. it downloads the writes after the snapshot
    3. it uploads 31 to 33 from _coven_uploads
    4. every device judges 31 to 33 under §8, §17.1 and §19.3
    ```

  - Without an intervening breaking change or reset, an edit to a cell
    changed since loses on its stamp, and an edit to a deleted row loses
    on its generation.
- A deleted row's `_coven_rows` row stays for good, at one small row each,
  so a write made at its old generation loses however late it arrives.

## 16. Files

- Files are what the app attaches to rows: audio, images, documents.
- The app declares, for each synced table that carries files:
  - which column refers to the file;
  - which column holds its size in bytes, which a user-provided file is
    checked against;
  - whether a row, once it has a file, may be pointed at a different one;
  - which column holds the file's *content hash*, the SHA-256 of its
    bytes, which coven fills in when a write attaches the file;
  - which column holds the file's fixed storage reference, filled by coven
    ([§16.1](#161-kinds-and-where-files-are));
  - the file's kind, user-provided or app-provided;
  - whether devices download an uploaded file as soon as its row arrives,
    or on first read.

### 16.1 Kinds and where files are

- A *user-provided* file is the user's own file at a path on their device,
  such as a song in their music folder.
  - Coven records its path, size and modification time, and doesn't copy
    it.
  - Coven never changes or deletes it.
  - If it moves or changes, coven reports it as missing or changed, before
    reading any of it.
- An *app-provided* file is bytes the app hands to coven, which keeps and
  owns them.
- The first read records both the whole-file content hash and a SHA-256
  hash of each 64-KiB plaintext chunk (the last may be shorter), when
  preparing a user-provided original or staging an app-provided file.
  These hashes stay local; they are not part of the storage format.
- The app can hand them over as a stream, so a large file never has to fit
  in memory.
- The attaching write picks the file's random id and key, records its
  uploader, and fixes its path as `<store>/files/<device>/<file>`.
  - Its where-column carries that reference from the start (D12).
    It does not say whether an upload has finished.
  - The same transaction queues the file, even with no storage connected.
    No later write marks it uploaded or changes the row when transfer ends.
  - Pinning and caching change no file reference. A `FileRef` stays valid
    across upload completion.
- File status makes a fresh single-object status call (§4), then combines
  that observation with the current uploader and reports (E8), in this order:
  - **Available:** storage contains the complete file at its fixed path.
  - **Missing:** storage confirms absence, and its uploader was removed
    or replaced, or reports that it cannot upload this file (§19.1, D8).
  - **Uploading on D:** storage confirms absence and D is still active,
    with no report that it cannot supply the file. This includes a paused
    queue or an offline uploader; it does not promise current byte transfer.
  - A network or permission failure is an error, never proof of absence.
    With no storage configured, the call returns `NoStorage`.
  - Storage presence wins over an old missing-source report. A report's
    absence does not mean the object exists.
- A read can still use a checked local source or cached bytes. If it needs
  absent remote bytes, its typed error distinguishes uploading from missing
  and names the uploader. The app can ask for a missing file to be attached again.
- After a successful upload, reads on the attaching device use the cache
  or storage; coven never reads its user-provided original again.
  - The original stays where it is and is never changed or deleted.
- E.g. Ana attaches photo A to row 7, then Ben replaces it with photo B.
  Ana's upload of A finishes later. It changes no row and cannot restore A
  over B; each file keeps the reference chosen when it was attached.
- A row with no file has NULL in its hash and where-columns, so both
  must allow NULL; the app never writes them, and coven fills them in the
  write that attaches a file.
- A write that changes any of a row's file columns, its file, size, hash
  or where-column, writes all four, unchanged ones included, so a row's
  file always comes whole from one write.
  - E.g. Ana's phone and Ben's laptop each attach a different file to row
    7: the later write wins all four columns, never one file's size with
    the other's hash.
  - So coven refuses a foreign key with set null on any of the four, checked
    when the database opens and after migrating: its action would change
    one column alone ([§8.4](#84-foreign-keys)).
- An uploaded file is encrypted with a key of its own, which travels in
  its row's where-column, inside the row's encrypted writes, so only the
  row's readers can read it ([§16.2](#162-storage-and-naming)).
  - Moving a row between the store and a circle changes nothing about
    its file: the new audience's devices get the key with the row.

### 16.2 Storage and naming

- The content hash is a column of the row, and syncs inside encrypted
  writes like any other.
- Every device checks a downloaded file against it.
- Uploading stores the file encrypted at its already chosen path,
  `<store>/files/<device>/<file>`, using its attaching write's random key.
  - The provider sees only a random name, never a hash of the content.
  - Each upload is a copy of its own: identical files attached to two
    rows are stored twice, and deleting one never touches the other.
  - The key never reaches storage outside the row's encrypted writes, so
    a member who can't read the row can't read the file.
- A file is encrypted in 64-KiB chunks; the last may be shorter. Its
  header records the file size.
  - Each chunk is encrypted and authenticated on its own, with its index
    bound in, so a chunk can't be altered, swapped or reordered unnoticed.
  - So any chunk can be read and checked without the rest of the file.

### 16.3 Reading ranges

- The app reads a file through a *file reference*, taken from its row,
  naming the file the row had then.
  - A read checks the reference against the row first, so a row changed
    since can't redirect it to another file.
  - A write can check a reference the same way before changing the row.
- The app reads any byte range of a file, at any offset, as a stream.
- Playing a song and seeking in it are reads of different ranges.
- A file on this device, or in the cache, is read with positioned reads of
  the file on disk.
- An uploaded file not in the cache is read by fetching only the chunks that
  cover the range, with ranged requests to the provider.
  - Reuse its checked header across opens while it remains cached. If it
    is missing and the requested range starts at zero, fetch it with the
    first chunks; seeking elsewhere may need a separate header request.
  - Neighbouring chunks are fetched together, up to 1 MiB per request.
  - Each chunk is checked as it arrives; a failed check fails the read.
- Fetched chunks go into the cache, so reading a range again costs nothing.
- While a file is read in order, coven fetches the chunks ahead of the
  reader, so playback doesn't wait on the network.
- Seeking costs only the chunks under the new position.
- Reading a range whose chunks aren't cached, while offline, returns the
  storage network failure and its native cause for the app to show or log.
- Pinning a file ahead of time is how it becomes available offline.

### 16.4 Cache

- Each device caches uploaded files and chunks of them, within a size budget
  the app sets.
  - When the cache is over budget, the least recently used files and chunks
    go first.
  - Freeing space never fails a read.
- The budget is per namespace, the group a table's declared files belong
  to ([E2](api.md#e2-declaring-synced-tables)); each namespace evicts on its
  own ([E8](api.md#e8-files-and-the-cache)).
- The app can pin a file to keep it whole on the device regardless of the
  budget, and unpin it.
  - Pinning assembles a complete encrypted cache file from cached and fetched
    chunks, checks its content hash, and syncs it before publishing the pin.
    Publication replaces the separate cached chunks atomically. Cancellation
    or a crash leaves no partially downloaded file exempt from eviction.
- The app can remove an uploaded file from the cache, which never touches
  storage.

### 16.5 Uploads and deletion

- The attaching write queues each file atomically with its row. It waits
  there until storage is connected and the upload succeeds.
- The queue retains the attaching write's fixed id, key, captured reference
  and source location, with its size, content hash, original modification
  time when applicable, and queue-owned chunk hashes. Retargeting the row
  cannot replace the source facts of an already queued file. It keeps no
  encrypted copy.
- Every attempt, including retries and resumed provider sessions, checks
  the source's size and whole-file content hash, and a user-provided
  original's recorded modification time, before sending file bytes.
  As it streams, it checks each plaintext chunk against its recorded hash
  before encrypting or sending that chunk. A missing or changed source
  fails the upload with the existing file error and records
  `FileUnavailable` for this device and file (§19.1). Its positions publish
  that typed reason; a differing chunk is never encrypted or sent.
  - A retry must encrypt the same plaintext under the same key and chunk
    nonces ([§11.1](#111-cryptography)). The per-chunk check guarantees
    this even if the source changes after the whole-file check, without
    keeping a copy. Size and modification time alone cannot guarantee it.
  - A provider part may begin or end inside an encrypted chunk. Coven
    reads and verifies the whole plaintext chunk before encrypting it
    and selecting the requested bytes.
- Automatic retry delays start at 1 second and double to at most 5 minutes.
  Before waiting, persist the not-before wall-clock deadline T and the full
  wait W with the work. During a running session a monotonic timer enforces
  the delay; wall-clock changes do not move that timer.
  - On reopen arm `max(0, min(T - now, W))` on the monotonic timer. Keep the
    original T and W until an attempt supplies a new delay; reopening does
    not reset the backoff exponent or replace the persisted deadline.
  - Operations, invites, joining, missing keys and file retries share this
    rule. Persist provider/account cooldowns separately at their actual
    scope before any affected worker can retry. `Retry-After` may make W
    exceed five minutes; neither restart, sync_now nor app retry discards it.
  - Ana restarts with four seconds left in an eight-second delay. She waits
    four seconds. If her clock moved backward, the restart waits at most
    eight; moving it forward can shorten the wait to zero. This rule bounds
    restart delays, not real-time cooldown duration under arbitrary clock jumps.
  - Failure to persist a delay fails the initiating work and prevents automatic
    retry; it is never converted into permission to retry immediately.
- A large file goes up through the provider's resumable or multipart
  upload, in parts.
  - Providers require it above a size, such as Google Drive above 5 MB per
    request.
  - The upload session is recorded, so after a crash the upload continues
    from the last part stored, instead of starting over.
  - A session the provider has since expired starts over, reading and
    verifying the source again with the same id, key and chunk hashes.
  - Any object past the provider's single request limit goes up this way,
    a large write or snapshot included.
- Upload completion removes the queue row and its blocked record in one
  local transaction. It creates no app write, changes no row or file version,
  and does not delay the attaching write's upload.
  - A crash before that transaction retries the fixed upload. An occupied
    path counts only after its bytes compare equal (§10).
  - Rows may arrive before their files. No write waits for file bytes.
- Physical deletion of a circle's rows, files and objects, pre-migration
  inputs and local file sources also waits for every entry it depends on
  to be final (§9). Current replay alone cannot release them.
- A stored file is deleted only when no retained row, loss, snapshot or
  log write refers to its fixed path. Local rows, waiting writes, upload
  queues and inputs retained for non-final entries protect it too.
  - Check local protection first. If it protects every eligible listed
    file, no snapshot rows or log parts need reading for file retention.
  - Otherwise stream the necessary retained data into the database's
    reference checks, with complete object authentication before deletion.
    Keep checked references and metadata by immutable object identity across
    passes and reopening, with an explicit completeness record. Loading and
    retention share this index; an object already checked is not downloaded
    again. Keep inspected ranges and any subsequently loaded body under the
    retained-input budget; checked references have no pass-end expiry.
  - Reconsider deletion when references, protected inputs, finality,
    ownership or file presence change. Use the pass's history catalogs and
    observe every file prefix this device may delete from on every pass.
    A newly listed file triggers these checks even without a new row write.
    §4.1 explains why a missing next number cannot replace this observation.
  - Ana removes Ben while his last photo upload is still in flight. It lands
    after her previous scan. The next complete scan of Ben's assigned file
    prefix discovers it; retained references still decide whether it can go.
- Its storage path and fixed row reference carry the uploader's device
  id, so ownership remains known after the last reference disappears.
- If retained data belongs to an unreadable audience, lacks a key copy,
  or fails validation,
  a device cannot prove file absence and leaves uploaded files in storage.
  Keep that uncertainty by object identity; retry the local proof only when
  its prerequisite or applicable reader version changes, or the object is
  deleted. Another pass alone does not download the same retained data.
- Uploaded files are deleted by the same devices as logs
  ([§15](#15-snapshots)).
- Deleting a row deletes only coven's copies of its file, never a
  user-provided original.

### 16.6 What a device keeps about files

- Coven keeps, in its local tables, what only this device knows about
  files; none of it syncs:
  - `_coven_user_files`: each user-provided file's path, size and
    modification time, by its row and column;
  - `_coven_device_files`: each app-provided file waiting to upload, and
    where in coven's own folder;
  - `_coven_file_chunks`: one plaintext hash per 64-KiB chunk, keyed by the
    fixed `(store, device, file id, chunk index)` reference. The upload queue
    owns these rows; attachment inserts them with the queue, and confirmed
    upload completion deletes them with it. Hashes use separate rows so
    file length does not become a single SQLite-value limit;
  - `_coven_file_uploads`: the upload queue, each file's captured reference,
    source location and original-source metadata, independent id and key,
    and its provider session while one is in progress;
    failures and waits use `_coven_blocked`, not a second failure column;
    - Provider sessions belong to this queue; file upload operations do not
      keep a second recording of the same session.
    - Native error objects are available in the running process. Reopening
      exposes the persisted failure category rather than reconstructing an
      operating-system or provider error from text.
    - A stored file whose row changed is considered for storage deletion
      under the same retained-reference checks; no marking write is queued;
  - `_coven_cache`: each cached file or chunk, its namespace, size, when it
    was last read, and whether it is pinned;
  - `_coven_cache_budgets`: each namespace's budget and cached byte total,
    updated with its cache records so a budget check needs no full sum;
  - `_coven_file_removals`: unused local copies waiting to be deleted.
- Reading a not-yet-uploaded original finds its hashes through the row's
  fixed where-column reference. There is no row-and-column hash copy to keep
  in sync with the queue. After upload, reads use cache or storage (§16.1).
  - Ana attaches A, then the row is pointed at B before A finishes. A's
    queued reference still owns A's hashes; B's attachment records B's hashes
    under B's own id. Finishing A cannot delete B's facts.
- The bytes themselves are files in the store's directory: coven's own
  copies, and the cache.
- A file's bytes are written and synced to disk before the row that names
  them commits, and a row's removal commits before its bytes are deleted,
  so a crash never leaves a table naming bytes that aren't there.
- Nor bytes that no table names:
  - before writing new bytes, coven records their name in
    `_coven_file_removals`, and the write that attaches them takes it out;
  - the write that lets bytes go records them there in its transaction;
  - after a write commits or fails, and when the database opens, coven
    deletes the unused bytes `_coven_file_removals` names, then their records.
    Names still held by staging or cache reservations are retained. Cancelling
    either releases its active names synchronously, without requiring an async
    runtime. The durable removal records retain its abandoned bytes for the next
    write or open, which reports any deletion failure.
  - A deletion that fails stays recorded and is tried again then; the
    write that let the bytes go stays committed, and reports the failure.

## 17. Schema changes

### 17.1 Host application

- Every write records the schema version it was made with.
- Adding tables or columns is an *addition*.
  - Writes from devices on older versions still apply everywhere; they
    just don't set the new columns.
  - A device holds writes from a newer version until its app updates, then
    applies them.
  - E.g. Ana's app adds `color` to notes; Ben's older app keeps editing
    titles, and those edits reach Ana as usual.
- Any other change is a *breaking change*: renaming, retyping or dropping a
  column or table, or changing its constraints.
- Coven tells which a migration is by comparing the schema before and
  after it, as SQLite lists it.
  - Only new tables or new columns: an addition.
  - Anything else, such as a new index, constraint or foreign key: a
    breaking change.
  - Changes only to local tables that keep their names, their columns,
    indexes and triggers, and to views, are neither: they don't sync.
  - Dropping or renaming a table is a breaking change, since it may have
    synced.
  - A migration that inserts, updates or deletes rows of synced tables is a
    breaking change, even if it only adds tables or columns: each device
    would otherwise compute those changes from its own rows.
- For each breaking change, the app supplies a *migration* in two parts:
  - one changes the database, e.g. `ALTER TABLE notes RENAME COLUMN title
    TO name`;
  - optionally, one changes a write made in the old version, e.g. turns
    "note 43, title: X" into "note 43, name: X".
- An update runs every pending migration in one transaction, then checks
  the schema rules against the tables the app declares, which describe
  only the newest version; if a rule fails, nothing changes.
  - E.g. migration 1 makes `notes(id)` and migration 2 renames `id` to
    `note_id`: only the final schema has to match `key_columns(["note_id"])`.
- A breaking change raises the store's schema version, which every device
  shares.
  - Whichever device's app updates first makes it, whoever's device it is.
  - That device runs the migration's first part on its database, writes a
    snapshot in the new version ([§15](#15-snapshots)), and records the store's new version
    in the store log ([§9](#9-members-and-roles)).
  - An older app cannot apply that audience's newer schema. It records
    `UpdateRequired` and reloads from the snapshot after updating.
    Store-log reads and independent work continue (§19.1).
  - It snapshots every audience it can read, with one entry raising each
    to the new version; the store's own entry raises the store.
  - For a circle it can't read, the first device of one of that circle's
    members to update writes the circle's snapshot in the new version,
    with an entry raising the circle to it; the circle's other devices
    reload from that one.
  - A device that has migrated holds its new-version writes back until
    the store log has the entry raising the store to that version, its
    own or another device's.
  - A device whose app is older than the store stops uploading; its
    writes wait, and are converted when it updates.
  - Raises to different versions don't conflict: both apply, the store is
    at the higher one, and devices reload from its snapshot.
- What a breaking migration changes in the synced tables is one write by
  the device that runs it, in the new version: its *migration write*.
  - It holds what differs between the synced tables before and after the
    migration: each row inserted or deleted, and each cell whose value
    changed.
  - A cell the migration left as it was keeps the write that set it, so a
    late write competes with that write, not with the migration.
  - A cell whose references change, such as one a new foreign key now
    covers, counts as changed: the migration write sets it, naming the
    parent's generation after the migration
    ([§8.4](#84-foreign-keys)).
  - Only the tables before and after count, not the statements between:
    a table or column after the migration is the one renamed to it with
    `ALTER TABLE … RENAME` from one that existed before, or else the one
    with its name.
    - E.g. `DROP TABLE b; ALTER TABLE a RENAME TO b`: the new `b` is the
      old `a`, and the old `b` is gone.
  - So a renamed column keeps its cells' writes under its new name, and so
    does a table rebuilt by copying it into a new one and renaming that
    back; a table or column with no match after the migration loses them.
  - E.g. Ana's migration renames `title` to `name` and fills a new column
    `slug` from it: each note's `name` keeps the write that last set its
    title, and each `slug` is set by the migration write.
  - Its object in the device log names it and holds no row changes: its
    changes reach other devices only in the breaking change's snapshot.
  - So a device that runs the same migration later, then reloads from that
    snapshot, changes nothing anywhere with its own migration write; nor
    does one whose breaking change loses to a concurrent one.
- An update that runs several breaking migrations makes one migration
  write, and raises the store once, to the newest version.
- A breaking migration deletes rows already hidden by removal rules through
  the ordinary generation path (§8.3).
  - Its migration write moves each such row to the next even generation.
    Keep `_coven_rows`; late edits at the old generation lose to that delete.
  - Capture one frozen whole-row loss with the written values and their
    setters, caused by this breaking version. A plain delete would retain
    only unread values, but the migration had read them all.
  - Use the same schema-excluded loss shape as an excluded write.
    The generation delete removes its live removed-row record.
  - Frozen values retain their old names and scalar values. References have
    no live parent links, even if the migration drops their foreign key.
- A non-final circle deletion cannot decide that a migration loses a row.
  - Leave out rows whose only removal cause is that `DeletedCircle` rule.
    Keep their values and merge records for replay.
  - Judge independent causes with non-final deleted-circle causes absent;
    a child hidden solely through such a parent is left out too.
  - A row also failing an independent CHECK or other removal rule still
    follows the generation-delete and frozen-loss rule.
  - E.g. Gifts' deletion hides Ana's note 7. Ben's app migrates while that
    deletion is non-final. The migration retains note 7's inputs; if the
    deletion drops, note 7 returns through the winning migrated state.
  - If note 8 independently fails `start <= end`, the migration deletes
    its generation and keeps one frozen loss even if Gifts returns.
- Pre-migration inputs remain until the deciding entries are final (§9).
  If a deletion changes before then, recompute the migration's derived
  state from those inputs; do not revive erased cells by guessing values.
- Dropping a table or column does not dismiss its pending losses.
  - Freeze each affected loss as old-shape data before removing its schema
    records. A whole-row loss keeps the whole captured row.
  - Keep its table and column names, key, audience, scalar values, setters
    and original cause. Strip live parent links; the old schema need not exist.
  - Frozen records no longer take part in merge or current removal rules.
    They remain readable, dismissible, snapshotted and fingerprinted.
  - E.g. Ana's migration drops `notes.color` while Ben's losing value “blue”
    is still pending. Every device keeps that loss under `notes.color`;
    opening the new schema does not silently discard it.
- Waiting writes keep their device ids, numbers, timestamps and causal
  positions. A breaking change never renumbers or redoes them.
- Settle every attempted upload by resending its original bytes (§6).
  - Re-seal the retained plaintext with its first attempt's format and key
    ids. A positive upload result, or equal bytes already at its path,
    settles it as stored.
  - Snapshot coverage does not prove that the log object was stored.
    Even a write covered by the raise snapshot must finish its upload.
  - Until settlement, keep its queue record and record the blocker (§19.1).
- Let S be the snapshot of the kept breaking-change entry for an audience.
  A device judges waiting and incoming writes by the same rules:
  - S already determines the effects of writes it covers.
  - An old-version write outside S is excluded: keep its values as frozen
    losses, and pass its position without applying its changes.
  - A write that read an excluded write is excluded too. Its own earlier
    writes count as read, so every later queued write behind an uncovered
    attempted write is excluded with it.
  - An untried queued write is converted only if none of the inputs it read
    was excluded. Run the migration's second part in version order.
  - With no conversion, or with an excluded input, upload the untried write
    marked lost, naming the breaking version. Do not convert its values
    into a claim that can apply.
- “Read” here means input to the state on which the write was made.
  Positions passed as excluded after loading the kept boundary are not
  inputs to new writes. The write's `store_log_read` records whether its
  author had adopted that boundary.
  - After the atomic reload, new writes use S and the eligible writes after
    it. They do not inherit discarded queued effects merely because the
    device's log numbers passed them.
  - Keep exclusion verdicts separately from dismissible loss values while
    an entry can change. Loading reconstructs required verdicts from the
    retained boundary and log inputs (§9, §15); dismissing a loss cannot
    make its write eligible or release these inputs early.
  - Reset-ignored history follows §19.3 and does not create a schema loss.
- E.g. Ana raises the store from S, which covers Ben's phone through write 8.
  Ben's attempted write 9 is unsettled; queued write 10 read it.
  - Ben resends 9's original bytes until storage confirms them. S does not
    cover 9, so every device records it lost.
  - Write 10 is untried but cannot apply: it read 9. Ben uploads it marked
    lost, and every device reaches the same result.
  - Ben reloads S under those same rules. His next write, made after that
    reload and naming the kept raise in `store_log_read`, uses the new
    state and can apply.
- If Ben's untried write 9 instead read only inputs S retains, a supplied
  conversion can turn its old `title` edit into a `name` edit. It then
  applies on every device that reads that audience.
- Snapshots may include the author's unuploaded writes. Making the raise
  snapshot never waits for an empty upload queue; doing so could wait on
  the very raise needed to upload that queue.
- Loading the winning snapshot replaces values a losing migration computed.
  Only its device had those values; recording them as shared losses would
  make devices disagree. Ordinary excluded writes still keep their losses.
- If two devices make the same breaking change at once, the one with the
  smaller timestamp counts; the other's entry is dropped, with its snapshot
  ([§9](#9-members-and-roles)).

### 17.2 Coven's schema

- Coven's own tables, indexes, triggers and views, including temporary
  objects, use the `_coven_` prefix. SQLite-generated constraint indexes
  keep SQLite's `sqlite_autoindex_` prefix before the table name.
- App SQL objects and synced-table declarations may not use `_coven_` or
  `coven_`, compared without regard to ASCII case. Reserving both prefixes
  keeps app access restricted to app objects; coven creates its own objects
  only under `_coven_`. SQL functions such as `coven_applying()` keep their
  names.
- Coven's own tables in the local database, such as `_coven_rows`, are
  local only. Opening for writing always migrates them in place before
  running the app's migrations; each internal migration is atomic.
  A read-only open refuses tables that need migrating, and any open refuses
  an internal schema newer than this coven supports.
- What coven writes to storage has a *format*: write records, store log
  entries, snapshots, paths, every byte of it given in
  [Appendix D](format.md).
- Every object records the format version it was written in, outside its
  encryption, so an older coven tells a newer object from a damaged one,
  and asks for an update instead of reporting it.
- A newer coven reads every older format and writes the newest. It keeps
  every older reader, since a device can return with waiting writes from
  any of them.
- A write or store log entry already tried is retried in the format of
  its first attempt, so re-sealing it reproduces the same bytes under the
  same content-bound nonces ([§6](#6-syncing-writes)).
- An older coven that encounters an object in a newer format asks for an
  update and waits, as with schema additions. Once updated, it reads that
  object and continues.
- Format versions belong to objects. A format change needs no store-log
  entry, snapshot or reload.

## 18. Operations

- An *operation* is work that takes several steps, any of which a crash can
  interrupt, such as publishing a rotation.
- Coven keeps every unfinished operation in one local table:

  ```sql
  CREATE TABLE _coven_operations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,     -- 'rotate key', 'reload from snapshot', …
    last_step   INTEGER NOT NULL,  -- 0 before the first step completes
    data        BLOB NOT NULL,     -- what this kind's steps need, in its own shape
    started_by  TEXT NOT NULL      -- the app call that started it, or 'coven'
  );
  ```

- Operation ids are never reused, so a retained app reference cannot retry or
  discard another operation after its original row is deleted.
- Each kind defines its own steps and the shape of its `data`, so one table
  holds any operation:

  ```
  id   kind                   last_step   data                                started_by
  1    rotate key             2           audience: store, key: …             coven
  2    reload from snapshot   1           snapshot: <store>/snapshots/store/ana-phone/7,    coven
                                          temporary file: …
  ```

- A step that changes the local database updates `last_step` in the same
  transaction:

  ```
  BEGIN
    ALTER TABLE notes RENAME COLUMN title TO name
    UPDATE _coven_operations SET last_step = 1 WHERE id = 3
  COMMIT
  ```

  - A crash before the commit undoes both, so the step runs again.
  - The migration can never be done while the row says it isn't, which
    would run it twice.
- A step that changes storage is safe to run twice.
  - Writing an object writes the same path with the same bytes: writes and
    entries regenerate them from fixed plaintext and key ids; sealed keys
    and snapshots retain fixed bytes ([§6](#6-syncing-writes)).
  - Deleting an object that is already gone succeeds.
- A store log entry takes its number when its plaintext and key id commit.
  It is always uploaded, even if the operation is abandoned, and the replay
  judges it like any entry, since a device's later entries had read it.
- An operation that writes a store log entry finishes only once the replay
  keeps it; if the replay drops it, the operation starts over from its
  first step, against the member list it then has.
  - Unless the drop leaves nothing to do, such as removing a member a
    concurrent entry already removed.
  - Starting over repeats §9's online catch-up and validation. This also
    applies after “landed too late”: the old entry remains dropped and
    reported, and any replacement has a new number and recorded past.
    A failed validation reaches the caller and blocks the operation.
- A device runs one operation that writes store log entries at a time, and
  none while it reloads from a snapshot.
- An app call first satisfies the online checks of §9. Once its operation
  starts, the call returns when it finishes or cannot proceed without app
  action. Dropping the call does not stop the recorded operation.
- Work already recorded can wait for storage to return. This does not
  allow an offline call to start a new administrative action.
- Steps are ordered so other devices never see a half-done operation.
- Anything another device reads, such as a store log entry, is uploaded
  last, after everything it refers to.
- When the app starts, coven resumes every unfinished operation from the
  step after its last completed one.
  - Thereafter it wakes for commands, changed prerequisites or due retry
    timers, following [the pass's waiting rules](sync-pass.md#waiting-without-repeated-work).
    It does not scan and replay the store log every second while waiting.
  - Determine snapshot growth and retention eligibility from local indexed
    facts before creating maintenance operations. An idle check creates no
    operation row; an unchanged blocker causes no durable rewrite.
- A step that cannot advance records its first blocker in `_coven_blocked`
  in the same transaction that records what the step completed.
  - A waiting app call receives the typed error. The record retains its
    category across restart without converting an error into text.
  - Every operation is visible there, including snapshots, retention,
    internal reloads and pending provider access work.
  - The reason determines retry: automatic, after an update, app action,
    or never (§19.1, E5). Unrelated operations can still advance.
  - The app can retry or discard its own blocked operations with E6.
    It can also retry failed provider access work. It cannot discard that
    current intention or automatic maintenance through those calls.
  - A failed reload leaves the old database in place. No positions pass
    it until the replacement and its dependent work commit.
- An operation's row is deleted when its last step completes.

### 18.1 Operations

- Removing a member ([§13](#13-removing-members-and-devices)) publishes one
  entry and returns once it is kept. Applying replay records access work;
  the removal itself has no provider step or operation row.
- Removing someone from a circle ([§14.6](#146-leaving-a-circle)) publishes
  one entry naming the circle and member. It is not a multi-step operation.
- Rotating an audience key ([§11](#11-keys)):
  1. a current member makes the key and id, records its SHA-256 commitment,
     and persists the key in custody;
  2. upload fixed sealed copies to the current audience members;
  3. upload the rotation entry with the audience, key id and key hash.
- A breaking schema change ([§17](#17-schema-changes)):
  1. migrate the database, with its migration write, in one transaction;
  2. upload a snapshot in the new version;
  3. upload the store log entry raising the version.
- Reloading from a snapshot ([§15](#15-snapshots)):
  1. settle attempted uploads under §17.1, then download the snapshots and
     writes between the lowest and highest points they and the waiting
     writes reach, into temporary files, retaining the inputs needed to
     judge exclusions;
  2. replace the synced tables and coven's merge tables
     ([§8](#8-merge)) with the snapshots, apply those writes, and re-apply
     eligible waiting writes, in one transaction. Convert eligible untried
     writes and record exclusions under §17.1;
     reset-ignored writes follow §19.3. Positions advance only with this
     realized state.
- Writing a snapshot ([§15](#15-snapshots)):
  1. write it, sealed, to a temporary file, record it and check it locally
     against the captured database state;
  2. upload it and compare the stored bytes' checksum;
  3. delete the log objects, older snapshots and files it lets go
     ([§16.5](#165-uploads-and-deletion)).
- Making a circle ([§14.3](#143-circles)):
  1. make its first key and its id, and record them;
  2. upload the key sealed to this member;
  3. upload the entry making the circle.
- Adding someone to a circle: seal each of its keys to them, then upload
  the entry adding them.
- Deleting a circle ([§14.7](#147-deleting-a-circle)) publishes its entry.
  There is no preceding write or wait for row uploads.
- Inviting a person ([§12.2](#122-adding-a-person)):
  1. share the storage with their account, or record the S3 key the admin
     made in the provider's console, and record the invite;
  2. once an admin approves the request, seal the store key to them, then
     write the store log entry adding them;
  3. on decline or expiry, take back the access instead.
- Uploading a write or a file is not an operation: it waits in its queue
  until stored ([§6](#6-syncing-writes), [§16.5](#165-uploads-and-deletion)).
  - A large file's provider session is recorded in its queue row, with the
    last part stored, so after a crash it continues from there
    ([§16.6](#166-what-a-device-keeps-about-files)).

### 18.2 Example

- Carol rotates the store key after Ana removes Ben. Her phone crashes
  after uploading K2's sealed copies:

  ```
  _coven_operations
    kind         last step   data                          started by
    rotate key   2           store, K2, key hash            coven

  storage
    <store>/keys/carol-phone/store/K2/ana       uploaded
    <store>/keys/carol-phone/store/K2/carol     uploaded
    <store>/store-log/carol-phone/9             not yet: the rotation
  ```

- The copies cannot authorize K2 for sending until its introduction is read.
- On restart Carol resumes at step 3 with the same key, hash and sealed bytes.
  Generating another key would disagree with the copies already published.
## 19. Recovery

- Recovery is for any state coven's rules didn't produce: a write that
  never arrives, an object that is damaged, a database a disk or a bug
  damaged, or devices holding different data after the same writes.
- The last is the one nothing else would catch, since every rule would
  still run; it is what a bug in coven or the app looks like.

### 19.1 Noticing

- `_coven_blocked` holds one record per subject and reporting device.
  The app reads it with `blocked()` or `subscribe_blocked()` (E5).
  - The subject identifies the write, entry, key copy, snapshot, positions,
    file, operation, retention target, audience, join request or connection.
  - An agreement check names both its audience and the peer.
  - The reason is the first unmet condition in that subject's processing
    order. Do not duplicate one subject for all of its downstream symptoms.
  - Observe a block and save its record in one transaction. Replace it if
    the first condition changes; delete it when the subject advances or
    replay removes the need for that work.
  - No wall-clock time or attempt counter controls these records. They
    describe current work, not a history of attempts.
- Record every wait and failure, including:
  - a gap in either kind of log, a listed object that cannot yet be read,
    an unknown author's device registration, or missing causal history;
  - a missing or damaged sealed key copy;
  - an app schema or object format that needs an update;
  - an unpublished schema raise, pending own uploads or a required reload;
  - a snapshot that cannot be used, a positions object that cannot be
    trusted, or history that prevents proving a file safe to delete;
  - physical cleanup waiting for an entry to become final, with
    `Waits(EntryFinality(entry))` naming that entry (§9);
  - file sources that cannot be uploaded, paused transfers, and provider
    work waiting for the owner, a request, or an app action;
  - a dropped entry, including “landed too late” on its author's device,
    or a fingerprint disagreement.
- E.g. Carol's tablet has Ben's write 9, which read Ana's write 4. Storage
  currently lists only Ana's writes 1 to 3.
  - Ana's next log position reports the missing object.
  - Ben's write reports that same object as its first prerequisite.
  - Carol keeps applying independent writes. Both records disappear when
    Ana's write arrives and those subjects advance.
- The typed reason determines retry (E5):
  - missing objects, prerequisites, key copies and transient storage work
    retry automatically;
  - an immutable object's permanent refusal, or an unsupported schema or
    format, waits for the applicable update;
  - a missing file source, retained provider grant or disagreement needs
    an app action;
  - replay drops are recomputed on replay, not downloaded repeatedly;
    a “landed too late” drop never becomes eligible again. It passes the
    entry's position and remains reported even if a new entry succeeds.
    A reset can clear the report, but cannot undo that drop.
- A permanent refusal requires a complete download and a failed check:
  decryption, authentication, signature, parsing, authorization, identity,
  causality or merge validation. For example, a write cannot have a timestamp
  no later than one of its causes. Network failures are not permanent checks.
  - A refused write or entry stops its own log at that number. Later writes
    in that log and dependent work cannot pass it; independent logs continue.
  - Keep the coven package version of the local refusal internally.
    A different installed version permits one new attempt. Reopening the
    same version does not retry the same immutable bytes. A permitted
    check uses retained bytes first; only eviction requires another download.
  - A successful changed reset clears the current blocked list atomically
    with its reload. A failed reload changes nothing. Retain suppressed
    records until the reset is final; if the reset drops, recompute the list
    from those inputs (§9). Still-unmet conditions are recorded when
    observed again; clearing applies even when a refused object's audience
    is unknown, to both kinds of log.
  - Snapshot loading alone does not clear refusals or bypass required
    store-log history.
- Posted positions carry the subset peers can act on (D8): permanent
  refusals, missing objects and prerequisites, unavailable member key copies,
  update requirements, and this device's undeliverable files.
  - Publish only observations made here, never reports received from peers.
  - Authenticate each report. Retain it when it names this device's object,
    an object needed from this device, a key copy sealed to this member,
    or an undeliverable file referenced by this device's retained data.
  - Peer reports inform the author or file reader; they do not dictate its own download
    checks. One completed positions scan replaces the received reports
    atomically. A failed scan leaves the previous reports and records why.
    The pass reuses peer objects whose listed identity is unchanged (§3.1,
    §4); agreement and retention use that same decoded state. An equal
    received report set needs no replacement transaction.
  - A device can publish these records while its write positions wait (§6).
    Silence or different positions alone never proves an immutable refusal.
- A damaged snapshot is recorded and passed over for the next latest, or
  the logs. Its verified prefix still establishes required history; it
  never authorizes applying a damaged body.
  - Unverified prefixes establish no coverage.
  - Missing required logs block the reload without changing the database.
- A damaged positions object has reason `InvalidPositions` and counts as
  not posted. Its discovery retries automatically because its author can
  replace it; only a changed object identity or a permitted local recheck
  triggers another validation, using retained bytes when available.
- A damaged local database is detected by SQLite's integrity check or
  decoding its stored facts (§19.2).
- Devices that disagree:
  - each device keeps a *fingerprint* of the data in each audience it can
    read, updated as writes apply;
  - it is a hash of exactly what every device must agree on:
    - the rows the app sees;
    - each row's generations;
    - which write set each cell, named by device and number;
    - its `_coven_lost` rows, with their values as written;
  - all computed as if the rule for a key present in two audiences didn't
    exist, since which row it shows depends on which circles a device
    reads ([§14.2](#142-moving-rows));
  - and nothing else that differs between devices by design, such as
    `_coven_uploads`, `_coven_operations`, or local row ids;
  - it is keyed with a key derived from the store's or circle's key, so the
    provider learns nothing from it;
  - each device posts its fingerprints with its positions ([§6](#6-syncing-writes));
  - two devices that have applied exactly the same writes must have the
    same fingerprints;
  - so whenever two devices are at the same positions, in the device logs
    and the store log, and on the same schema version, as they usually are
    once a store is quiet, each compares, and a mismatch means a bug made
    one of them wrong;
  - on different versions they can't compare: an added column exists on
    one device only.
- A fingerprint mismatch is an `Agreement` blocked subject naming the
  audience and peer, with reason `Disagrees`.
  - Compare only fingerprints using the same key as well as the same
    write positions, store-log positions and schema version.
  - E.g. Ana and Ben reach the same positions in Gifts but their hashes
    differ. Each app sees the peer and Gifts in its blocked list.
  - Neither device can tell which copy is right. Recovery requires the
    person's choice of reload (§19.2) or reset (§19.3).

### 19.2 Recovering one device

- A usable database that disagrees with another device reloads in place
  from snapshots and logs (§15). It keeps its device id and waiting writes.
  - Settle attempted writes by sending their original bytes. Convert
    eligible untried writes or record their losses under §17.1.
  - Reset boundaries still decide which parts apply (§19.3).
- A damaged database fails to open with `DbError::DamagedDatabase`.
  The app calls `CovenBuilder::reset_device` (E1).
  - Use the same new-device path as a stale copy (§10), from saved settings
    and custody. Read no rows, counters, queues or operations from the
    damaged database for salvage.
  - Check storage and unlock the member and store keys before moving the
    damaged files. A missing setting, unavailable key or failed connection
    leaves them in place and returns its typed error.
  - Stop all local database use and hold the store's writer and reader locks.
    Move the old directory aside as a whole, including SQLite journals and
    local file sources. Keep it available for inspection, not as live state.
  - Use the existing bootstrap state (§12) to keep the new id, replacement
    entry and old directory identity before moving it. Resume that bootstrap
    after a failure; ordinary opens refuse the unfinished installation.
    There is no separate recovery journal or recovery marker.
  - Load storage under the fresh id, then publish the replacement handle.
    Never import unsent rows or files from the damaged directory.
  - Tell the app that unsent edits may have been lost. The returned handle's
    device-reset notice names the old and new ids and `DatabaseDamage` (E5).
- E.g. Ana's laptop has uploaded through write 12, but its damaged database
  may contain write 13. Resetting loads the stored history through 12 under
  a new device id. Coven does not guess which fragments of 13 are usable;
  the app receives the loss notice.

### 19.3 Resetting a store

- When the problem is shared, or nobody can tell which device is right,
  the person picks the device they trust, and the store is reset from it.
- E.g. "Ana's phone and Ben's laptop disagree about the store; reset it
  from this device?"
- An admin's device resets the store's rows; a member of a circle resets
  that circle's rows, one reset per audience.
- A reset is an operation ([§18](#18-operations)):
  1. write a snapshot of what this device has ([§15](#15-snapshots));
  2. record in the store log that the store, or the circle, is reset to
     that snapshot ([§9](#9-members-and-roles)).
- Every device reloads from that snapshot when it applies the entry, the
  resetting device too, so writes it applied after writing the snapshot
  are judged like everyone else's.
- A device reloads whenever the replay changes an audience's reset or
  schema version, from the snapshot the kept entry names, so a reset
  dropped by a later entry is followed by the winner's.
- A reset replaces that audience with the chosen snapshot. On every
  device, including its author, only these contents count:
  - the snapshot's rows and merge records;
  - later writes whose `store_log_read` includes the kept reset entry.
- A write outside the snapshot that had not read that entry is ignored
  for the reset audience, even if it read every write in the snapshot.
  - Do not apply its row changes or record them as lost.
  - Count the ignored part as passed, so log positions can advance.
  - Other audiences' parts follow their own reset and schema rules.
  - Reading an ignored pre-reset write does not invalidate a post-reset
    write: the reset deliberately discards that old history.
- A device still uploads its unsent pre-reset writes, keeping their
  identities and attempted bytes, so its log has no gaps. It ignores their
  effects in the reset audience itself, like every other device.
- E.g. Ana resets from her phone's snapshot, covering Ben's writes through 8.
  Ben's laptop made writes 9 and 10 without reading the reset; 10 read 9.
  - Ben uploads both. Neither changes the reset audience or creates a loss.
  - After reading the reset, Ben makes write 11. It applies on every device.
  - Ana's own writes made after capturing the snapshot but before reading
    her reset entry are ignored by the same rule.
- If two devices reset the same audience at once, the one with the smaller
  timestamp counts; the other's entry is dropped, with its snapshot
  ([§9](#9-members-and-roles)).

## 20. Crates and conventions

### 20.1 Crates

- Coven is a Cargo workspace of eight crates, each owning one part of
  this spec:

| Crate | Owns | Spec |
| --- | --- | --- |
| `coven-foundation` | The clock, the id source, atomic file writes, the store's directory and its lock | [§7.2](#72-timestamps), [§10](#10-device-identity), [E1](api.md#e1-opening) |
| `coven-crypto` | Ciphers, sealed boxes, derived keys, file keys, member keys and their custody | [§11.1](#111-cryptography) |
| `coven-merge` | Timestamps, the merged state, the removal rules and lost values, as functions with no I/O | [§7](#7-order), [§8](#8-merge), [§14](#14-audiences) |
| `coven-format` | The bytes in storage: write records, store log entries, snapshots, file headers and chunks, encoded, decoded and checked, using merge's and crypto's types | [Appendix D](format.md) |
| `coven-database` | The SQLite connection, coven's internal tables, applying the merge's results, triggers, live queries, migrations | [§5](#5-local-database) |
| `coven-storage` | Each provider, and the operations coven needs from it, including upload sessions | [§4](#4-storage-providers-and-access) |
| `coven-sync` | Device logs, the store log, members, circles, snapshots, files and the cache, operations, recovery | [§6](#6-syncing-writes), [§9](#9-members-and-roles), [§12](#12-joining-and-restore) to [§19](#19-recovery) |
| `coven` | The API, and nothing else | [Appendix E](api.md#appendix-e-api) |

- Each crate depends only on crates above it in the table, except that
  `coven-database` and `coven-storage` never depend on each other.
- So the database never reaches storage, and storage never reads the
  database; `coven-sync` is where the two meet.
- `coven-merge` and `coven-format` read no clock, file, database or
  network.
- So the merge is tested, and checked against the Lean model of
  [Appendix B](proofs/merge.md), without SQLite or storage.
- Whole-history construction belongs to test support; production merges
  arriving writes against the applied metadata without retaining write bodies.
- Ids shared by several crates (store, device and circle ids) live in
  `coven-foundation`; each concept has one type.
- Each external dependency's version is set once, in the workspace, and
  crates name only the features they need.

### 20.2 Capabilities, owners and lifetimes

#### Capabilities

- A *capability* is something outside the program's own memory: the
  network, cryptography, SQLite, the OS keychain, the current time, new
  ids, and files on disk.
- Each capability is used directly in one place only:

  ```
  network          coven-storage's providers
  cryptography     coven-crypto
  SQLite           coven-database
  OS keychain      coven-crypto's custody
  current time     coven-foundation's clock
  new ids          coven-foundation's id source
  files on disk    coven-foundation's file writes, and the file cache
                   in coven-sync
  ```

- Everything else reaches a capability through the object that owns it,
  given to it when it is built.
  - The clock, the id source, key custody, the CloudKit calls and the
    OAuth clients and presenter are all set on the builder ([E1](api.md#e1-opening)), so
    a test replaces each one.
  - E.g. the snapshot writer never calls the system clock; it asks the
    clock it was given, and a test gives it a fixed one.
- A method never takes a raw capability as a parameter, such as a store
  directory or a database connection; it calls the owner of it instead.

#### Owners and tasks

- An *owner* is an object that holds a capability, or holds another
  owner, and lives while the store is open.
- E.g. the database owner holds the SQLite connection, and the sync owner
  holds the database owner and the storage owner.
- A *task* is a value that lives for one piece of work and is then
  dropped, such as one sync pass, or one step of an operation
  ([§18](#18-operations)).
- A task borrows the owners it needs for that work, and holds nothing
  past it.
- Owners receive their collaborators when they are built.
- E.g. the sync owner takes the database owner and the storage owner as
  arguments, and doesn't open either itself.
- One authoritative list in the checker's policy names every place allowed
  to construct owners or start long-lived work. Each entry names an exact
  source file, type and method (or free function), not a whole file or a
  type-name suffix. These *composition roots* include opening and bootstrap,
  provider connection construction, owner constructors and task starts.
- The checker infers owners from retained capabilities and other owners.
  Every owner construction outside a listed root fails the check, including
  construction of an owner's private representation. Runtime acquisition
  and thread or task spawning use the same list. A legitimate new place
  must be added deliberately; returning an owner grants no construction
  permission. Test-only sources and items may assemble their own graphs.
- Each long-lived task has one owner responsible for starting and stopping
  it, including when that owner is dropped. Its start sites belong to the
  same list; there is no separate list of lifetime authorities.
- E.g. only the sync owner starts the sync loop, so closing the store stops
  it, and nothing else can leave one running.
- Opening leaves configured storage `Stopped`, or `Disconnected` when none is
  set up. `start_sync` asks the injected provider connector to build a client
  when absent, using custody credentials, then starts the loop. Tokens refresh
  on rejection (§4). Repeated starts keep the running loop and client.
- `stop_sync` finishes the active pass and file transfers before releasing
  every worker's provider reference. Session keys stay with the open handle;
  closing or explicit key forgetting erases them. It retains the location
  and custody credentials for the next start. `disconnect_storage` also forgets
  this device's storage credentials, leaving remote contents untouched (E5).
- An owner never hands out what it holds, by returning it or by a public
  field; callers ask it to do the work.
- E.g. nothing outside coven-database gets the SQLite connection; it asks
  the database owner to run a write.
- A closed custody owner is represented by the absence of its held custody,
  without a separate closed flag. Storage calls serialize with closing;
  calls already started finish even when their app future is dropped. Sign-in
  remains cancellable while presenting or exchanging credentials (E5).
- A struct built only to be taken apart again, with every field public
  and no methods, is not used to pass collaborators; they are passed by
  name.

### 20.3 Code conventions

- Visibility:
  - nothing is `pub` that can't be reached from outside its crate;
  - `pub(in path)` and `super::super::` are not used: an item needed
    elsewhere moves to where both callers can see it;
  - `coven` re-exports the API at its root and keeps every module
    private.
- Errors are typed enums per crate; an error is never turned into text
  to be passed on, and nothing returns `Result<_, String>`.
- Failed app calls and whole-pass sync failures retain typed causes.
  `Offline` means the last attempt could not reach storage; `Failed` means
  storage answered with an error preventing the whole pass (E5).
  Every object or operation that cannot advance has a typed blocked record,
  including maintenance, prerequisites and relevant signed peer reports
  (§19.1, E5). A persisted reason never substitutes text for its category.
- A source file holds at most 1,000 lines, and its tests live beside it
  in `<name>_tests.rs`.
- Crates offer shared fakes through `test-utils` where needed, such as an
  in-memory provider and a fixed clock. Test-only implementations stay behind
  injected dependencies; tests build the same object graph production does.

### 20.4 Checks

- One script runs every check. CI runs its code checks on every platform,
  and the Lean proofs once, on Linux, since a proof checks the same on any
  platform:
  - formatting, and clippy with warnings denied;
  - the dependency rules of [§20.1](#201-crates);
  - the rules of [§20.2](#202-capabilities-owners-and-lifetimes), by a
    checker that reads the syntax tree of every crate, including inside
    macro calls, against the policy file;
  - the checker's own guard tests: every file, owner and composition
    root the policy file names must exist, so the policy can't go stale;
  - the visibility and file-size rules of
    [§20.3](#203-code-conventions);
  - `cargo doc` with broken links denied;
  - every crate built without test code, so an item only tests use shows
    up as dead;
  - the tests, with all features and with none;
    - the guarantees in §3 are exercised through app calls where visible:
      committed writes survive dropping the handle and process exits at each
      post-commit boundary, remain queued and upload after reopening; app reads
      and writes finish while a sync pass is held;
    - removal's next write and snapshots open with the rotated keys and fail
      with the removed member's keys; identical file uploads store different
      ciphertext; snapshots delete covered logs and superseded own snapshots;
    - injected failures at each remote-write mutation roll back app rows,
      merge records, losses, fingerprints, positions and query notifications;
      these tests use fixed inputs, an injected clock and explicit barriers;
  - the Lean proofs, built from scratch, with no `sorry` and no axiom
    beyond Lean's own, and their differential tests against Rust.
- The checker is the first thing built, before any crate, so every rule
  holds from the first line of code.
- Tests control clocks, transfer limits and request completion. Upload boundary
  tests use byte-sized payloads; races wait for observed events, and interrupted
  operations are reopened until they report completion.
- The pre-commit hook runs the fast ones: formatting, clippy, and the
  rules of [§20.1](#201-crates) to [§20.3](#203-code-conventions).

## Appendix A. SQLite behavior in synced tables

Each SQLite feature whose meaning changes when devices write offline and
merge later.

### A.1 Integer primary keys

- Problem: two offline devices can pick the same id for different rows,
  and coven would treat them as one row.
- Status: refused; synced tables use UUIDs or keys derived from the
  content ([§8.5](#85-keys-and-uniqueness)).

### A.2 Tables with no primary key

- Problem: coven can't tell which row a change belongs to, and SQLite's
  hidden rowid collides across devices like an integer key.
- Status: refused ([§8.5](#85-keys-and-uniqueness)).

### A.3 Unique constraints besides the primary key

- Problem: two offline devices can each insert a row with the same value.
- Status: the row whose write has the smaller timestamp keeps the value;
  the other row is taken out while it conflicts, and recorded in
  `_coven_lost` ([§8.5](#85-keys-and-uniqueness)).

### A.4 Restrict and no-action foreign keys

- Problem: a device can delete a parent while another adds a child it
  hasn't seen.
- Status: the child is taken out while its parent is gone, and recorded in
  `_coven_lost` ([§8.4](#84-foreign-keys)).

### A.5 CHECK constraints

- Problem: two concurrent edits that each pass can merge into a row that
  fails, such as one device setting `start` and another `end`.
- Status: a row that fails after a merge is taken out until it passes, and
  recorded in `_coven_lost` ([§8.6](#86-check-constraints)).

### A.6 Triggers that write synced tables

- Problem: a trigger that runs again while coven applies a remote write
  would repeat what the original device already sent.
- Status: allowed as shared triggers, which run only on the device making
  the write ([§8.7](#87-triggers)).

### A.7 Schema changes

- Problem: devices running different app versions hold different schemas.
- Status: additions sync with no change of version; any other change
  raises the store's version, and every device updates and reloads ([§17](#17-schema-changes)).

### A.8 SET DEFAULT foreign keys

- Problem: the default parent is a second parent that merges like any row
  and can itself be gone. Supporting it requires restrict-like removal
  while that parent is gone, tracking its current generation, and handling
  rows that cannot be deleted when their defaults fail a CHECK.
- Status: refused on foreign keys of synced tables and foreign keys from
  local tables into synced tables, for both delete and update actions
  ([§8.4](#84-foreign-keys)). SET DEFAULT appears in so few SQL files that
  refusing it affects almost nobody. Counts of SQL files on GitHub using
  each action, from GitHub's REST code search (`GET /search/code`, query
  `"ON DELETE <action>" language:SQL`, its `total_count`), retrieved
  2026-10-07; approximate, across all SQL dialects:

  | Action | SQL files |
  | --- | --- |
  | `ON DELETE CASCADE` | ~1,714,000 |
  | `ON DELETE SET NULL` | ~646,000 |
  | `ON DELETE RESTRICT` | ~496,000 |
  | `ON DELETE NO ACTION` | ~115,000 |
  | `ON DELETE SET DEFAULT` | ~2,200 |
