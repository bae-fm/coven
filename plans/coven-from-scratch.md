# coven, from scratch

## Contents

- [1. What coven is](#1-what-coven-is)
- [2. Threat model](#2-threat-model)
- [3. Guarantees](#3-guarantees)
- [4. Storage providers and access](#4-storage-providers-and-access)
- [5. Local database](#5-local-database)
- [6. Syncing writes](#6-syncing-writes)
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
- [20. API](#20-api)
  - [20.1 Opening](#201-opening)
  - [20.2 Declaring synced tables](#202-declaring-synced-tables)
  - [20.3 Writing](#203-writing)
  - [20.4 Reading](#204-reading)
  - [20.5 Storage and sync](#205-storage-and-sync)
  - [20.6 Operations and recovery](#206-operations-and-recovery)
  - [20.7 Moves and uploads](#207-moves-and-uploads)
  - [20.8 Files and the cache](#208-files-and-the-cache)
  - [20.9 Members and devices](#209-members-and-devices)
  - [20.10 Joining and restore](#2010-joining-and-restore)
  - [20.11 Keys and secrets](#2011-keys-and-secrets)
  - [20.12 Circles](#2012-circles)
  - [20.13 Migrations](#2013-migrations)
- [21. Crates and conventions](#21-crates-and-conventions)
  - [21.1 Crates](#211-crates)
  - [21.2 Capabilities, owners and lifetimes](#212-capabilities-owners-and-lifetimes)
  - [21.3 Code conventions](#213-code-conventions)
  - [21.4 Checks](#214-checks)
- [Appendix A. SQLite features](#appendix-a-sqlite-features)
  - [A.1 Integer primary keys](#a1-integer-primary-keys)
  - [A.2 Tables with no primary key](#a2-tables-with-no-primary-key)
  - [A.3 Unique constraints besides the primary key](#a3-unique-constraints-besides-the-primary-key)
  - [A.4 Restrict and no-action foreign keys](#a4-restrict-and-no-action-foreign-keys)
  - [A.5 CHECK constraints](#a5-check-constraints)
  - [A.6 Triggers that write synced tables](#a6-triggers-that-write-synced-tables)
  - [A.7 Schema changes](#a7-schema-changes)
- [Appendix B. Proof of convergence](coven-merge-proof.md), in its own file
- [Appendix C. Proof of the store log](coven-storelog-proof.md), in its own file
- [Appendix D. Storage format](coven-format.md), in its own file

## 1. What coven is

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
    recorded, not silently dropped.
- **Durability:** a crash loses nothing:
  - every committed write is still uploaded;
  - every operation with several steps resumes and finishes, for example:
    - rotating the key, then removing the member;
    - writing a snapshot ([§15](#15-snapshots)), then deleting the logs it
      covers;
    - uploading a file, then marking it stored.
- **Revocation:** an ex-member can't read anything written after they left.
- **Bounded storage:** cloud history doesn't grow forever: device logs and
  old snapshots are deleted once newer snapshots cover them; only the
  store log and sealed keys stay, growing with membership changes alone.

## 4. Storage providers and access

- A store lives on one provider: S3, Google Drive, Dropbox, OneDrive, or
  iCloud through CloudKit.
- Every member reaches the storage using their own provider account.
- On S3, each member has their own access key.
- What coven needs from a provider:
  - create an object, in one request or, past the provider's single
    request limit, through its resumable upload;
  - read it, whole and by range;
  - list a prefix, with when storage stored each object;
  - delete;
  - grant and revoke a member's access, where the provider can: Google
    Drive, Dropbox, OneDrive and iCloud share with an account.
- Every path has one writer: the device it names, or the device that made
  the key, file or request it holds.
  - So creating an object only has to survive its own device retrying,
    never two devices racing for one path.
  - On Google Drive, which allows two files with one name, a retry first
    looks for its own earlier copy.
- On the providers that share with an account, only the member whose
  account holds the store can share it and take it back, so:
  - that member's devices make invites ([§12.2](#122-adding-a-person));
  - any admin removes a member, and that member's access is taken back by
    the owner's device when it applies the removal
    ([§13](#13-removing-members-and-devices)).
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
- Setting up a store refuses a location that already holds another store,
  or anything that isn't a coven store.
  - Two devices setting up different stores in one empty location at the
    same moment can both succeed; whichever store's first entry has the
    larger timestamp finds the other's when it next syncs, stops syncing,
    and reports the location taken.
  - Its data is all on its devices, so the app sets it up somewhere else,
    and it uploads everything again.
- S3 has no standard way to make or delete access keys; each S3 provider
  has its own, so on S3 an admin makes and deletes members' keys in the
  provider's console, and coven says when.
- Posted positions live at `positions/<device>` ([§6](#6-syncing-writes)).
- Sealed circle keys live at `keys/circles/<circle>/<key>/<member>`
  ([§14.3](#143-circles)).

## 5. Local database

- The app declares which tables sync. The rest stay on the device.
- A *write* is one transaction that changes synced tables.
- A *row change* is one row's insert, update or delete within a write.
- SQLite's session extension records the row changes each write makes.
- Each write commits, together:
  - its rows, as they now stand;
  - its *write record*, waiting to be uploaded in coven's `coven_uploads`
    table:
    - its row changes: which rows, which columns, old and new values;
    - which device wrote it, its number, its timestamp, and what it had
      read;
    - the schema version it was made with;
  - so every committed write gets uploaded, even after a crash.
- A write needs no key: the record waits unencrypted and unsigned, and its
  upload encrypts and signs it ([§6](#6-syncing-writes)).
  - So the app writes before any key is unlocked
    ([§20.1](#201-opening)).
- A write record, for a write that fixes a note's title and deletes a tag:

  ```
  ana-phone, write 3, 2026-10-02 12:00:00.000 #0
    had read: ben-phone 8, carol-tablet 1
    notes  row 42  update  title: "Grocry list" → "Grocery list"
    tags   "errands"  delete
  ```

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
  - Its record waits in `coven_uploads` as one value, and its sealed
    bytes, once fixed, as one value in a row of their own, so a write's
    limit is SQLite's largest value, 1 GB: a write whose sealed bytes
    would be bigger, which its plaintext's size decides, fails at commit
    with `DbError::TooLarge`.
  - E.g. Ana imports 50,000 notes in one transaction: one write, in many
    chunks.
  - It is named `devices/<device>/<n>`, created once and never changed.
  - `<n>` counts that device's own writes: 1, 2, 3, with no gaps.
  - Its name is part of its encryption, so the provider can't swap one
    object for another.
  - A retried upload writes the same name with the same bytes.
  - A retry that finds its path already holds an object counts it as
    stored: only this device writes that path, and every attempt sends the
    same kept bytes.
- A device uploads its writes in number order; a write never goes up
  before an earlier one.
- The first attempt to upload a write encrypts each part with the newest
  key of its audience this device holds, signs the object with the
  device's member key ([§14.4](#144-writes)), and keeps those bytes in
  `coven_uploads` before sending them; every retry sends the kept bytes.
  - E.g. Ana's phone commits a Gifts pin offline, then reads her removal
    from Gifts before uploading it: the pin's part is sealed with the
    Gifts key she held, and counts like any write made before she read
    her removal ([§14.6](#146-leaving-a-circle)).
- Every other object a device uploads has its bytes fixed the same way
  before its first attempt, and kept until it is stored: store log
  entries, sealed keys, snapshots and files
  ([§18](#18-operations)).
- A write record leaves `coven_uploads` ([§5](#5-local-database)) once its upload succeeds.
- Each device remembers how far it has applied every device's log, in
  coven's `coven_positions` table: one row per device, naming its last
  applied write's number.
- It also posts those positions to storage at `positions/<device>`,
  replacing its own object when the positions advance.
  - In its own log it posts the last write it has uploaded.
- A device finds devices it doesn't know yet, and their logs, by listing
  `devices/` and `store-log/` ([§20.5](#205-storage-and-sync)).

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
  authority ([§9](#9-members-and-roles)); otherwise every device records
  it lost.
  - So a write made after its device read its own removal never counts,
    and a write from a device whose addition a later entry drops still
    counts if its author had read that addition.
- A device has always read its own earlier writes.
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
  - a write that waits, such as one stamped too far ahead, isn't seen yet,
    so a device with a wrong clock can't drag the others' stamps forward;
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
- A write stamped more than five minutes ahead of the receiving device's
  clock waits until that clock catches up, instead of being applied.
- Store log entries are stamped the same way, from the same latest
  timestamp, and applying one advances it like applying a write; one
  stamped more than five minutes ahead waits too.

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
  - `devices/ana-phone/4`:

    ```
    ana-phone, write 4, 2026-10-02 13:01:00.000 #0
      had read: ben-phone 8, carol-tablet 1
      notes  row 42  update  title: "Grocery list" → "Groceries"
    signed with Ana's key
    ```

  - `devices/ben-phone/9`:

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
  - A removed row is recorded as lost while it is out, and its `coven_lost`
    row keeps its values, which later edits to it update.
  - Coven puts it back from there when the reason goes away, e.g. when the
    reference that made it a parent's child is pointed elsewhere.
- A removed row's `coven_lost` row names every rule that holds for it once
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
- The proof is [Appendix B](coven-merge-proof.md), in its own file, checked
  by machine for the merged state and the removal rules.
- When a write changes which rows are removed, coven makes the change in
  the app's table with ordinary SQL, which triggers see like any other.
- Merging uses these internal tables:
  - `coven_writes`, one row per write the device has applied, naming:
    - the write's timestamp, which includes its device;
    - the write's number;
    - the writes it had read.
  - `coven_columns`, one row per synced column, naming:
    - its table;
    - its column.
  - `coven_rows`, one row per generation of each synced row, naming:
    - its table;
    - its primary key;
    - its audience;
    - the generation: how many times the row had been created, deleted or
      re-added;
    - the write that moved it there, or of several concurrent ones, the one
      with the smallest timestamp.
  - `coven_cells`, one row per synced cell, naming:
    - its `coven_columns` row;
    - its `coven_rows` row;
    - the write that set it.
  - `coven_references`, one row per reference a synced cell holds, naming:
    - the cell's `coven_rows` and `coven_columns` rows;
    - its `coven_foreign_keys` row;
    - the parent's table, key, audience, and the generation it points at
      ([§8.4](#84-foreign-keys)).
  - `coven_foreign_keys`, one row per foreign key of a synced table, naming
    it as below.
  - `coven_claims`, one row per unique value a removed row claims, naming:
    - the row's `coven_rows` row;
    - its `coven_constraints` row, which names the unique constraint as
      below;
    - the row's audience and the value it claims
      ([§8.5](#85-keys-and-uniqueness)).
  - A present row's values are in the app's table, and only there.
  - `coven_lost`, one row per lost value or removed row, naming:
    - the cell, or the row;
    - the value that lost, or every value of the removed row;
    - the write that set each value;
    - what replaced it: a write that hadn't read it, the rules that removed
      the row, or a breaking change or reset its write hadn't read.
  - `coven_lost_references`, one row per reference a lost value holds,
    naming its `coven_lost` row, its `coven_foreign_keys` row and the
    parent, so a lost reference reads as null or the default when its
    parent goes ([§8.4](#84-foreign-keys)).
- The store log's effects that the database applies are kept with it:
  - `coven_deleted_circles` names each circle whose deletion it has applied
    ([§14.7](#147-deleting-a-circle));
  - `coven_applied_boundaries` names each breaking change and reset it has
    applied, with the writes its snapshot included, so a later write is
    judged against it ([§17.1](#171-host-application),
    [§19.3](#193-resetting-a-store)).
- Fingerprints ([§19.1](#191-noticing)) are kept incrementally:
  `coven_fingerprint_leaves` holds one hash per row and per lost write in
  each audience, and `coven_fingerprint_sums` their sum per audience, so
  a write updates only the hashes of the rows it changed.
- Note 42 on Ben's phone, after Ana's write 4 and its own write 9:

  ```
  notes                            the app's own table
    id   title               body
    42   "Weekly groceries"  "milk, eggs"

  coven_columns
    id   table   column
    1    notes   title
    2    notes   body

  coven_rows
    id   table   key   audience   generation   write
    3    notes   42    store      1            1

  coven_cells
    column   row   write
    1        3     7
    2        3     1

  coven_writes
    id   timestamp                                number
    1    2026-10-01 09:12:40.511 #0  ana-phone    1
    7    2026-10-02 13:01:00.000 #1  ben-phone    9
  ```

- To find which write set note 42's title:
  - in `coven_columns`, notes' title is column 1;
  - in `coven_rows`, notes row 42 is row 3;
  - in `coven_cells`, column 1 of row 3 points to write row 7;
  - in `coven_writes`, write row 7 is Ben's phone's write 9, stamped
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
  `devices/carol-tablet/2`.
- Carol's tablet never read Ben's write 9, and Ben's phone never read
  Carol's write 2. The two writes are concurrent, so only their
  timestamps can order them.
- When a device applies one of these writes to the title, it:
  - adds the write's row to `coven_writes`;
  - compares the write's stamp with the stamp of the title's current write;
  - if the new stamp is larger, sets the title in `notes` and points the
    title's `coven_cells` row at the new write.
- Each device has its own `coven_writes`, so one write gets a different row
  on each device.
- Ana's write 4, for example, is row 6 on Ben's phone and row 14 on Carol's
  tablet.
- Note 42's title on each device, in real time.

Ben's phone:

<table>
  <tr><th>Time</th><th>Applies → <code>coven_writes</code> row</th><th>New stamp</th><th>Current stamp</th><th>Wins</th><th>Title</th><th>Title's <code>coven_cells</code> row → <code>coven_writes</code> row</th></tr>
  <tr><td>13:00</td><td>Ana's write 4 → 6</td><td>13:01:00 #0</td><td>12:00:00 #0</td><td>yes</td><td>"Groceries"</td><td>6</td></tr>
  <tr><td>13:00</td><td>its write 9 → 7</td><td>13:01:00 #1</td><td>13:01:00 #0</td><td>yes</td><td>"Weekly groceries"</td><td>7</td></tr>
  <tr><td>14:30</td><td>Carol's write 2 → 8</td><td>14:00:00 #0</td><td>13:01:00 #1</td><td>yes</td><td>"Shopping"</td><td>8</td></tr>
</table>

Carol's tablet:

<table>
  <tr><th>Time</th><th>Applies → <code>coven_writes</code> row</th><th>New stamp</th><th>Current stamp</th><th>Wins</th><th>Title</th><th>Title's <code>coven_cells</code> row → <code>coven_writes</code> row</th></tr>
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
    removes its `coven_lost` row.
  - Every device ends with one row:

    ```
    coven_lost
      cell            lost value          set by          replaced by
      note 42 title   "Weekly groceries"  Ben's write 9   Carol's write 2
    ```

- The app reads lost values through `lost_values`
  ([§20.4](#204-reading)) and can offer to restore one.
- Every device holds the same `coven_lost` rows, because they follow from
  the writes alone.

### 8.3 Deletes

- A row's *generation* counts how many times it has been created, deleted
  or re-added.
  - It is odd while the row exists and even while it is deleted.
  - Each generation has its own `coven_rows` row, naming the write that
    moved the row there.
- Each row change in a write record carries the generation the row had on
  the device that wrote it:

  ```
  ben-phone, write 11, 2026-10-02 16:00:00.000 #0
    had read: ana-phone 6, carol-tablet 2
    notes  row 43  generation 1  update  title: "Hardware store" → "Hardware store, Saturday"
  signed with Ben's key
  ```

- Edits to a row's columns change only its `coven_cells` rows, never its
  generation.
- Deleting a row removes it from the app's table and moves its generation
  on to the next even number.
- Re-adding it moves its generation on to the next odd number.
- Its `coven_cells` rows go with it, but its `coven_rows` rows stay, so
  later writes to it still have a generation to lose to.
- A row change concurrent with a delete of its row loses, and its cells go
  to `coven_lost`, replaced by the delete.
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
    `coven_lost` row, which needs nothing from the deleted row.
  - The generation's `coven_rows` row names the delete with the smaller
    timestamp, whichever arrived first.
  - E.g. Carol retitles note 43, and Ana and Ben, both offline, delete it;
    Ben had read Carol's edit and Ana hadn't.
  - A device that gets Ana's delete first records Carol's title as lost,
    and removes that `coven_lost` row when Ben's delete arrives.
  - Every device ends with note 43 deleted and Carol's title not lost.
- E.g. at 16:00 Ana deletes note 43, "Hardware store", while Ben, offline,
  edits its title, and at 17:00 Ana re-adds it.
  - Note 43 on Carol's tablet:

    ```
    coven_rows
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
    coven_lost
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
  - under set null or set default, it stays, and its reference is null or
    the default, as SQLite would have made it.
  - The reference is null or the default wherever coven keeps it, in the
    app's table and in `coven_lost`, so every device stores the same value
    whichever write arrived first.
  - The cell still names the write whose reference won; it only reads as
    null or the default, and later writes to it compete with that write's
    timestamp, as with any cell. Nothing is recorded as lost.
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
- Coven refuses set null on a `NOT NULL` column, and set default where
  the column is `NOT NULL` and its default is NULL, in any table, checked
  when the database opens and after migrating.
  - SQLite can never apply such an action: no device could delete the
    parent, and coven couldn't take it out.
- Where SQLite would still refuse the default, such as one failing a
  CHECK, the child is taken out as under restrict.
- Coven takes rows out with SQLite's foreign keys enforced, children
  first, so SQLite's own actions reach only local children.
  - Rows the app's schema makes impossible to delete stay so: e.g. two
    rows pointing at each other with set default, whose default fails a
    CHECK, can't be deleted by the app, and can't be taken out by coven.
  - A write that would need it fails with SQLite's error.
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
- A reference set to its default points at whichever generation of the
  default parent is current, and is never stale.
  - If that parent is deleted or taken out, the child is taken out as
    under restrict, as SQLite would refuse it, and comes back once the
    parent is there again.
  - E.g. notes point at folders with set default, and the default is
    "Inbox".

    ```
    16:00  Ana deletes "Work"; Ben, offline, puts note 50 in "Work"
    16:10  Carol deletes "Inbox"
    16:30  every device: note 50's folder would be "Inbox", but there is
           none, so note 50 is taken out and recorded in coven_lost
    17:00  Carol re-adds "Inbox"; note 50 comes back, in Inbox
    ```

- A child whose parent is taken out by a rule, rather than deleted, is
  taken out with it under every action, and comes back with it.
- E.g. note 46 loses its title to note 45 and is taken out; link 7, which
  points at note 46 with set null, is taken out with it, not set to null.
- Coven refuses set null and set default on a primary key column, checked
  when the database opens and after migrating, since nulling a key
  column changes the row's key: a delete plus an insert no write recorded.
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
    the same `coven_lost` row, so the app can offer to put it on another
    note.
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
    cells go to `coven_lost`.

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
  "urgent" tag taken out and recorded in `coven_lost`, so the app can offer
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
  - Every device keeps note 45, and records Ben's note 46 in `coven_lost`.
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
  - Both are recorded in `coven_lost`, so the app can offer them back.

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
- Every device ends with the row taken out, and the same `coven_lost` row.

### 8.7 Triggers

- The app declares each trigger on a synced table as local or shared;
  a trigger it doesn't declare shared is local ([§20.2](#202-declaring-synced-tables)).
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
    ([§14](#14-audiences)) or change its members, raise the store's schema or format version, or
    reset the store or a circle to a snapshot ([§15](#15-snapshots)).
  - An entry names the store log entries its author had read, and is
    signed with its author's member key.
  - Entries live at `store-log/<device>/<n>`, numbered like a device's
    writes ([§6](#6-syncing-writes)).
  - The device in the path is only where the entry was written from.
  - Each device numbers its own entries, so no two entries ever get the
    same path.
  - Whose entry it is comes from its signature: an entry signed with Ana's
    key is Ana's, whichever of her devices wrote it.

    ```
    store-log/ana-phone/1      create the store, Ana as admin       signed with Ana's key
    store-log/ana-phone/2      add member Ben, with his public key  signed with Ana's key
                               had read: ana-phone 1
    store-log/ben-laptop/1     add device ben-phone                 signed with Ben's key
                               had read: ana-phone 2
    store-log/ana-ipad/1       make Ben an admin                    signed with Ana's key
                               had read: ana-phone 2
    ```

  - The store's first entry creates it, names its first admin, and adds
    the device that wrote it, with its name.
- Roles:
  - several equal admins;
  - only admins add and remove members, and change roles;
  - each member adds and removes their own devices, with their own key, and
    admins can remove any device;
  - a removal names a device its author had read the addition of, so it
    knows whose device it is;
  - the store always has at least one admin.
- Every device that has the same entries ends with the same member list,
  whatever order they arrived in.
  - The proof is [Appendix C](coven-storelog-proof.md), in its own file,
    checked by machine.
- A device applies an entry once it has every entry that entry had read.
- Each device keeps, in coven's local tables:
  - `coven_store_log`: every entry it has applied, as downloaded and
    checked, and whether the replay kept or dropped it;
  - the replay's result: `coven_members` (every member a kept entry
    added, their public keys and role, and whether they were removed),
    `coven_devices` (every device a kept entry added, its member and name,
    and whether it was removed), `coven_circles` (every circle a kept
    entry made, its name and current key's id, and whether it was deleted),
    `coven_circle_members`, and `coven_store_state` (the current store
    key's id, the schema and format versions, and each audience's reset).
  - Keys themselves are only ever in key custody
    ([§11](#11-keys)); these tables hold their ids.
  - Removed members and devices stay, since their writes that reached
    storage still count, checked with their keys
    ([§10](#10-device-identity)).
  - An entry and the replay it causes commit in one transaction, so the
    tables always hold the replay of exactly the entries kept.
- What depends on the replay follows its latest result, both ways: an
  arriving entry can drop one kept before.
  - E.g. Ben deletes Gifts, and Carol's device applies it. Then Ana's
    removal of Ben from Gifts arrives, made concurrently with an earlier
    stamp: it beats the deletion, which is dropped. On Carol's device
    Gifts is back, with Ana in it, and its rows return, since the rule
    that took them out no longer holds ([§14.7](#147-deleting-a-circle)).
  - Entries waiting on ones they had read stay in storage until those
    arrive.
- The member list is what you get by replaying the applied entries in
  timestamp order, from the first.
  - Each time an entry arrives, the device replays them all again, from
    the first, so the result depends only on which entries it has.
  - The member list an entry's author had read is the replay of just the
    entries that entry had read.
- At its place in the replay, an entry applies only if its author's role
  allowed it, in the member list the author had read.
- Then, an entry whose change is already in place applies and changes
  nothing.
  - E.g. Ana and Ben both add Dan: both apply, Dan is added once, and
    neither is reported.
- Otherwise it applies only if:
  - what it needs exists: the member it changes, the device it removes,
    the member a new device belongs to, the circle it changes, the circle
    member it removes;
  - the store still has an admin after it;
  - it beats every concurrent entry already applied that it conflicts
    with.
- When an entry beats some already applied, they are all dropped, and the
  replay starts again without them.
  - A dropped entry stays dropped until that replay ends; the next arrival
    starts a new replay with every entry.
- A dropped entry is shown to its author.
- Whose entry it is comes from its signature, not from the device it was
  written from, so a new device adds itself, signed with its member's key
  ([§12.1](#121-a-persons-new-device)).
- Two concurrent entries conflict when:
  - they're about the same member or device and say different things,
    where an entry about a device is also about the member it belongs to;
  - one adds a member and the other removes one, which replaces the store
    key ([§13](#13-removing-members-and-devices));
  - they give one circle different names;
  - one deletes a circle and the other changes it, its members, or resets
    it;
  - one adds someone to a circle and the other replaces that circle's key:
    by removing someone from it ([§14.6](#146-leaving-a-circle)), or by
    removing from the store a member the removal names as in it
    ([§13](#13-removing-members-and-devices));
    - e.g. Ana's tablet adds Ben, then Carol, to Gifts, while her phone,
      which hadn't seen either, removes Ben from the store: the removal
      doesn't name Gifts, so Carol's add applies; Ben's own add is about
      Ben, so the removal beats it;
  - they each replace the same key: two removals from the store, or two
    removals of someone from the same circle, since otherwise a key one of
    them made would be sealed to the member the other removed
    ([§13](#13-removing-members-and-devices));
  - they raise the schema or format to the same version with different
    snapshots, reset the same audience to different snapshots, or one
    resets an audience the other raises to a new version
    ([§17](#17-schema-changes), [§19.3](#193-resetting-a-store)).
- Whether an entry deletes a circle is judged in the member list its
  author had read: removing the circle's only member there deletes it.
  - E.g. Ana and Ben are admins and share Gifts. Concurrently, Ana's phone
    removes Ben from Gifts, and her tablet removes Ana from the store.
  - Each had read Gifts with two members, so neither deletes it, and they
    are about different members: both apply.
  - Ana leaves the store, Ben stays as its admin, and Gifts, left with no
    members, is deleted.
- Of two conflicting entries, the one that beats the other is:
  - removing a member, a device or someone from a circle, or deleting a
    circle, over anything else;
  - anything else over making someone an admin;
  - otherwise, the one with the smaller timestamp.
- Concurrent entries, and what applies:

  ```
  Ana adds Dan               Ben makes Carol an admin   both: no conflict
  Ben adds his new phone     Ana removes Ben            the removal
  Ana makes Ben an admin     Carol makes him a member   member: an admin
                                                        grant loses
  Ana removes Ben            Ben removes Ana            the earlier
  Ana adds Carol             Ben removes Dan            the removal
  ```

- E.g. Ana, Ben and Carol are admins, and each, offline, removes another:

  ```
  first stamp    Ana removes Ben
  second stamp   Ben removes Carol
  third stamp    Carol removes Ana
  ```

  - Each pair conflicts, since each removal replaces the store key: Ana's,
    the earliest, applies, and Ben's and Carol's are dropped.
  - Carol's device then redoes her removal against the new member list,
    as an operation does until its entry is kept
    ([§18](#18-operations)): Ana is removed, and Carol stays the admin.
    Ben's device, removed, can't redo his.
- E.g. Ana and Ben are admins. Concurrently, Ben removes Ana, Ana removes
  Ben a moment later, and Ben adds his new phone:
  - after Ben's removal, Ana's would leave no admin, so it is dropped;
  - Ana's removal would have beaten Ben's new phone, but it was dropped,
    so the phone is added.
- A dropped entry stays dropped for that replay even if what beat it is
  dropped later; this only ever drops a change, never grants one.
  - E.g. Ana and Ben are admins. Concurrently, Ana adds Carol as an admin,
    Ben is made a member, and Ben removes Ana.
  - Ben's removal beats Carol's add, which is dropped, and the replay
    starts again; now removing Ana would leave no admin, so Ben's removal
    is dropped too.
  - Carol isn't added. Ana is shown her dropped add and invites Carol
    again.

## 10. Device identity

- A device is one install of the app, with its own device id, belonging to
  one member, who adds it to the store log ([§9](#9-members-and-roles)).
- A device restored from a backup is a new device, with a new id.
  - It knows it was restored because its id is also kept where backups
    don't reach, such as a keychain item kept to this device only; a
    store whose kept id is missing or different takes a new id at open.
  - So it never reuses write numbers its backup's device already used.
  - E.g. Ana's phone is backed up after its write 5, writes 6 and 7, and is
    lost. Her new phone is restored from the backup:

    ```
    devices/ana-phone/
      5   in the backup
      6   written after the backup
      7   written after the backup

    devices/ana-phone-2/
      1   the restored phone's first write
    ```

  - The restored phone downloads Ana's old phone's writes 6 and 7 like any
    other device's.
  - With the old id, its next write would be another write 6.
- Every write record is signed with the key of the member whose device
  wrote it, so who wrote what is authentic.
- This is about authenticity, not trust.
- A write counts only if its author was a member, and its device one of
  theirs, in the store log the write had read ([§7.1](#71-causality)), so
  a write by Ana's phone counts as Ana's.
- A removed device's writes still count if they reached storage and were
  made before it read its removal.
- A device that reads its own removal, or its member's, stops syncing and
  tells the app ([§20.5](#205-storage-and-sync)).
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
  - each later key, the removal that replaced the one before.
- Each store key is sealed to every member's public key, and the sealed
  copies are kept in storage, at `keys/store/<key>/<member>`.
- Sealed circle keys live at `keys/circles/<circle>/<key>/<member>`
  ([§14.3](#143-circles)).
- So two concurrent removals never write their keys to the same paths.
  - E.g. Ana removes Dan while Ben, offline, removes Erin: each removal
    names its own new store key; the two conflict, since each key is
    sealed to the member the other removes, so the earlier applies and
    the other is redone against it ([§9](#9-members-and-roles)).
- The *current* store key is the one named by the latest entry the replay
  keeps, in its order, that brings one in, and likewise for each circle; new
  writes, entries, snapshots and files use it.
  - An entry already in place changes nothing, so it brings no key in,
    though what was sealed with the key it names still opens: that key
    is sealed to the same members.
  - E.g. Ana and Ben both remove Dan: Ana's earlier removal brings its
    key in, and Ben's applies without one.
- Every encrypted object names, outside its encryption, the key that seals
  each of its parts, so a reader knows which key opens it.
- So a member's key alone gets the current store key: a device holding it
  reads its member's sealed copy from storage and opens it.
- The store key is replaced whenever a member is removed.
  - Writes made after that use the new key.
  - Devices keep the old keys, to read writes made before.
- Each device keeps its member's key in the OS keychain.
- Storage access, not keys, is what keeps a removed device out.
- The app can use coven's keys for its own data on the device:
  - it encrypts a value with the current store key, bound to where it is
    kept, such as a row's key, since the local database isn't encrypted;
  - it keeps its own secrets, such as an API token, in coven's keychain
    entry, under the same access policy as coven's keys.

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
  - for a file's chunks, derived from the file's own key and the chunk's
    index, so retrying an upload sends the same bytes;
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

### 12.1 A person's new device

- A person's new device needs their *restore code*, which holds:
  - their member key, which gets it the store key ([§11](#11-keys));
  - the store's id and name;
  - storage credentials.
- It gets the code in one of three ways:
  - by scanning it as a QR code on one of the person's devices that has
    the store open;
  - on Apple platforms, from iCloud Keychain, which holds the same
    contents, so a new device on the same Apple account opens the store
    with no step but a provider sign-in, where storage needs one;
  - by the person typing it in.
- The QR code is blurred until the person taps to show it.
- Where storage needs a sign-in, such as Google Drive, the new device
  signs in to the person's own account first.
- The new device then adds itself to the store log, signing with the
  member key ([§9](#9-members-and-roles)).
- The person writes the code down when they create or join a store, as
  part of setup.
- On Apple platforms, coven writes the code to iCloud Keychain whenever it
  changes: when the person creates or joins a store, and when their S3
  key or credentials change ([§20.9](#209-members-and-devices)).

### 12.2 Adding a person

- On a provider that shares with an account, the admin who invites is
  the member whose account holds the store
  ([§4](#4-storage-providers-and-access)).
- An admin adds a person with an *invite*, a code shown as a QR code,
  holding:
  - the store's id, name and location;
  - the invite's id, and a one-time *invite secret*;
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
- The join request is stored at `join-requests/<invite id>`.
  - It is encrypted with a key derived from the invite secret, and signed
    with Carol's new member key.
  - Ana's device holds the secret too, so it opens and checks the request.
  - The provider sees only an encrypted object under the invite's id.
- The invite only lets a device ask; Ana's approval is what lets Carol in.
- Only the device that made the invite holds its secret, so only it shows
  and approves the invite's requests.
- Carol's phone learns the outcome from storage:
  - her store key sealed to her, then the store log entry adding her:
    she's in;
  - her request deleted with no key sealed to her: declined or expired.
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
- Removing a member removes them and all their devices, in this order
  ([§18.1](#181-operations)):
  - the store key is rotated: a new one, sealed to each remaining member's
    public key ([§11](#11-keys));
  - so is the key of each circle they were in: a new one, sealed to that
    circle's remaining members ([§14.6](#146-leaving-a-circle));
  - the store log entry removing them is written; it names the new keys,
    and the circles whose keys it replaced, which must be exactly the
    circles they shared with others in the author's view, or the replay
    drops the entry ([§9](#9-members-and-roles));
  - the storage access their invite granted is taken back: the store's
    folder is unshared from their account, by the store owner's device
    when it applies the removal, or on S3 coven tells the admin to delete
    their key in the provider's console ([§12.2](#122-adding-a-person)).
- What the entry names is fixed when the removal starts, from the member
  list the device has then; if the replay drops the entry, the removal
  starts over against the new list ([§18](#18-operations)).
- Access taken back stays taken back, even if a later entry drops the
  removal: the member is told, and an admin shares the storage again.
- The member whose provider account holds the store can't be removed:
  the store would go with their account. Removing them fails with
  `SyncError::StoreOwner`.
- On S3, a key the admin must delete is shown in the sync status until
  the admin confirms it's gone ([§20.5](#205-storage-and-sync)), however
  the removal or expiry that needs it came about.
- So a removed member's copies of the old store and circle keys read
  nothing written after the removal, even if they regain read access.
- The removing device makes a new key for a circle its member isn't in,
  seals it, and doesn't keep it.
  - E.g. Ana removes Carol, who shares "Gifts" with Ben; Ana isn't in
    Gifts. Ana's phone makes Gifts' next key, seals it to Ben alone, and
    forgets it. Ana still can't read Gifts.
  - The phone holds that key for a moment; members are trusted not to be
    hostile ([§2](#2-threat-model)).
- A circle the removed member was alone in is deleted by the same entry:
  no one is left who could read it ([§14.7](#147-deleting-a-circle)).
- Adding a member concurrently with a removal that rotates the key is a
  conflict, and the removal beats the add, which is dropped
  ([§9](#9-members-and-roles)).
  - Otherwise the new member would hold only the old key, and couldn't
    read anything written after the rotation.
  - E.g. Ana adds Carol while Ben, offline, removes Dan: on every device
    Carol's add is dropped, Ana is shown it, and she invites Carol again.
  - Carol's phone shows the join as dropped; Ana invites her again, or
    cancels the invite, which takes back the storage access it granted
    ([§12.2](#122-adding-a-person)).
  - Nothing is taken back on its own: if a later entry brings the add
    back, Carol is in, and her phone shows it.

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

### 14.1 Roots and descendants

- A descendant's declared foreign key is one column, into a synced table.
  - Its action can't be set null or set default, since the row would lose
    its audience; coven refuses it, checked when the database opens and
    after migrating.
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
  - Its moved rows' uploaded files stay where they are: each file's key
    travels in its row ([§16.1](#161-kinds-and-where-files-are)).
- E.g. Ana moves note 42 and its attachments from the store into her
  circle: Ben's devices delete them, and Ana's insert them in the circle.
- A row's generations ([§8.3](#83-deletes)) are counted per audience, so
  `coven_rows` has one row per table, key, audience and generation.
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
    the smaller timestamp wins, the write `coven_rows` records for it.
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
- Each circle has its own key, sealed to each of its members' public keys,
  like the store key ([§11](#11-keys)).
  - Its sealed copies live at `keys/circles/<circle>/<key>/<member>`
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
    devices/ana-phone/12
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
- A part sealed with a key whose entry the replay drops counts on no
  device: every device records it lost, whether or not it holds the key.
  - E.g. Ana removes Ben and makes key K2, and her devices write with it;
    then Ben's earlier, concurrent removal of Ana wins. Carol holds K2 but
    Ben doesn't; neither applies those parts, so they agree.
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
  - The circle key is replaced, sealed to Ana alone.
  - Ben keeps the circle's rows he already had, but can't read anything
    written to it afterwards.
- Removing a circle's last member deletes the circle
  ([§14.7](#147-deleting-a-circle)).
- A write Ben made to the circle before he had read his removal still
  counts, as with any concurrent entry ([§9](#9-members-and-roles)).
  - Ben is still in the store, so storage access doesn't stop him writing
    with the old circle key.
  - Only trust keeps him from claiming he hadn't read his removal, and
    members are trusted not to be hostile ([§2](#2-threat-model)).
- Once Ben's device reads his removal, it keeps the circle's rows it has,
  refuses new writes into the circle with `DbError::NotInCircle`, and no
  longer snapshots or fingerprints it.

### 14.7 Deleting a circle

- Any member of a circle can delete it.
- Deleting a circle is two things, made together, as an operation
  ([§18.1](#181-operations)):
  - a write deleting each of the circle's rows the device has, like any
    delete, encrypted with the circle's key;
  - a store log entry removing the circle ([§9](#9-members-and-roles)),
    uploaded after the write.
- A row added to the circle concurrently, which the write doesn't name, is
  taken out by a removal rule when it arrives, and recorded as lost.
- A device that has applied the deletion refuses a write into the circle,
  as SQLite refuses a broken foreign key.
- E.g. Ana and Ben share a circle "Gifts", holding notes 7 and 8; Ben
  deletes it while Ana, offline, adds note 9 to it.

  ```
  ben-phone, write 31
    notes  row 7  generation 1  delete
    notes  row 8  generation 1  delete
  store-log/ben-phone/4   delete circle Gifts
  ```

  - Notes 7 and 8 are deleted on both devices.
  - Note 9 arrives in a deleted circle, so it is taken out and recorded in
    `coven_lost`, and Ana's app can offer to put it somewhere else.
- Devices outside the circle see only the store log entry.
- The circle's files and log objects go like those of any deleted row
  ([§15](#15-snapshots), [§16.5](#165-uploads-and-deletion)).

## 15. Snapshots

- A snapshot is the synced tables and coven's merge tables
  ([§8](#8-merge)) as one device has them, encrypted, with how far into
  every log they reach.
  - A device's own `coven_uploads` and `coven_operations` aren't in it, so
    a device that loads one keeps its own.
  - Snapshots live at `snapshots/<audience>/<device>/<n>`, where the
    audience is `store` or a circle's id.
  - Its prefix, outside its encryption, names its audience, its key and
    its positions, so any device can choose one and decide what it
    covers without opening it.
  - A snapshot is one object, encrypted in chunks like a write
    ([§6](#6-syncing-writes)), and written and loaded a chunk at a time.
  - A snapshot *covers* a write when the write is within its positions:
    `snapshots/store/ana-phone/3` covers ana-phone's writes 1 to 40.
- A device writes one for an audience once that audience's parts after
  its latest snapshot add up to more bytes than that snapshot, or than
  1 MiB while the audience has none.
- The *latest* snapshot of an audience is the one covering the most writes,
  counted over every log; a tie goes to the smaller path.
- Until an audience has a snapshot, a new device reads every log from the
  start; no log object is deleted before a snapshot covers it.
- So loading a snapshot and the writes after it costs at most about twice
  the snapshot.
- Two devices can write one at the same time, and both are correct:

  ```
  snapshots/store/ana-phone/3     ana-phone up to 40, ben-laptop up to 22
  snapshots/store/ben-laptop/1    ana-phone up to 38, ben-laptop up to 25
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
- Loading an audience's snapshot replaces that audience's rows and merge
  records; other audiences keep theirs, and the removal rules run again on
  rows that point at changed ones, as after any write
  ([§8.4](#84-foreign-keys)).
- Each device posts its positions only after uploading its own earlier
  writes.
- A log object is deleted once snapshots cover every part of it, and either
  every device's posted position has passed it or storage has held it for
  30 days.
  - Every device means every device the store log has and hasn't removed;
    one that has never posted counts as having read nothing.
- A device deletes a snapshot of its own once a newer one of the same
  audience covers everything it covers.
  - Each device deletes its own log objects.
  - A removed device's are deleted by another device of the same member,
    since they were uploaded with that member's account.
  - A removed member's are deleted by a device of the member whose provider
    account holds the store.
  - On Google Drive only an uploader can delete their files, so a removed
    member's just leave the store's folder, and stay in their own account.
  - A device that never comes back keeps its covered log objects until it
    is removed.
- A device that needs writes already deleted loads the latest snapshot
  instead, like a new device.
  - A missing write that a snapshot covers counts as deleted, not late
    ([§19.1](#191-noticing)).
  - Its own writes still waiting in `coven_uploads` keep their numbers, and
    it uploads them after.
  - Every device then applies them like any late write: they had read only
    writes the snapshot covers, which count as applied.
  - E.g. Ana's old phone made writes 31 to 33 offline, then stayed offline
    for a year:

    ```
    1. it loads the latest snapshot, which reaches ana-old-phone up to 30
    2. it downloads the writes after the snapshot
    3. it uploads 31 to 33 from coven_uploads
    4. every device applies 31 to 33 under the rules of §8
    ```

  - An edit to a cell changed since loses on its stamp, and an edit to a
    row deleted since loses on its generation.
- A deleted row's `coven_rows` row stays for good, at one small row each,
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
  - which column holds where the file is, which coven fills in
    ([§16.1](#161-kinds-and-where-files-are));
  - the file's kind, user-provided or app-provided;
  - whether a file is uploaded when it is attached, or only when the app
    asks;
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
- The app can hand them over as a stream, so a large file never has to fit
  in memory.
- Every row of a synced table syncs, but each file is in one of two
  places:
  - *uploaded*: stored encrypted, and read the same way on every device;
  - *on one device*: only on the device that has it, as the user's
    original or coven's own copy, and never uploaded.
- The row's where-column says which: `uploaded`, with the file's id and
  key, or the id of the device that has it.
  - So every device knows where each file is, and can say so.
  - Reading a file that is on another device fails with an error of its
    own, naming that device.
- E.g. Ana imports 2 TB of music on her laptop, into a table whose files
  upload only when the app asks:
  - every album syncs to her phone, which shows them as on her laptop;
  - she uploads twenty albums, which then play on her phone too.
- Uploading a file stores it ([§16.5](#165-uploads-and-deletion)), then
  writes `uploaded` to its row.
  - A user-provided original stays where it is.
  - From then on every device reads the uploaded copy, the one it came
    from included; coven never reads the original again.
- Keeping an uploaded file on one device downloads it there first, then
  writes that device's id to its row.
  - A user-provided file is written to a path the user picks, which must
    not already exist; an app-provided one goes into coven's own folder.
  - The uploaded copy is then deleted like any unused file.
- A device copy that its row no longer names, because another device's
  later write moved the file, is deleted once that write applies here.
- A row with no file has NULL in its hash and where-columns, so both
  must allow NULL; the app never writes them, and coven fills them in the
  write that attaches a file.
- A write that changes any of a row's file columns, its file, size, hash
  or where-column, writes all four, unchanged ones included, so a row's
  file always comes whole from one write.
  - E.g. Ana's phone and Ben's laptop each attach a different file to row
    7: the later write wins all four columns, never one file's size with
    the other's hash.
- Concurrent changes to where a file is follow [§8.2](#82-concurrent-writes-to-one-row):
  the later write wins.
  - E.g. an album is uploaded; at 10:00 Ana's laptop keeps it on the
    laptop, while her phone, offline, keeps it on the phone at 10:05.
  - Both download it first; the phone's later write wins, so every device
    shows the album on the phone, and the uploaded copy goes once nothing
    refers to it as uploaded.
  - An uploaded copy stays while any write still refers to it as uploaded
    ([§16.5](#165-uploads-and-deletion)), so the winner always finds the
    file where its row says.
- An uploaded file is encrypted with a key of its own, which travels in
  its row's where-column, inside the row's encrypted writes, so only the
  row's readers can read it ([§16.2](#162-storage-and-naming)).
  - Moving a row between the store and a circle changes nothing about
    its file: the new audience's devices get the key with the row.

### 16.2 Storage and naming

- The content hash is a column of the row, and syncs inside encrypted
  writes like any other.
- Every device checks a downloaded file against it.
- Uploading a file picks a random id and a random key for it, stores it
  encrypted at `files/<id>`, and writes both into its row's where-column.
  - The provider sees only a random name, never a hash of the content.
  - Each upload is a copy of its own: identical files attached to two
    rows are stored twice, and deleting one never touches the other.
  - The key never reaches storage outside the row's encrypted writes, so
    a member who can't read the row can't read the file.
- A file is encrypted in chunks, 64 KiB by default, recorded in its
  header.
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
  - The header is fetched once per open file.
  - Neighbouring chunks are fetched together, up to 1 MiB per request.
  - Each chunk is checked as it arrives; a failed check fails the read.
- Fetched chunks go into the cache, so reading a range again costs nothing.
- While a file is read in order, coven fetches the chunks ahead of the
  reader, so playback doesn't wait on the network.
- Seeking costs only the chunks under the new position.
- Reading a range whose chunks aren't cached, while offline, fails with an
  error of its own, which the app can show.
- Pinning a file ahead of time is how it becomes available offline.

### 16.4 Cache

- Each device caches uploaded files and chunks of them, within a size budget
  the app sets.
  - When the cache is over budget, the least recently used files and chunks
    go first.
  - Freeing space never fails a read.
- The budget is per namespace, the group a table's declared files belong
  to ([§20.2](#202-declaring-synced-tables)); each namespace evicts on its
  own ([§20.8](#208-files-and-the-cache)).
- The app can pin a file to keep it whole on the device regardless of the
  budget, and unpin it.
- The app can also fetch an uploaded file into the cache ahead of reading
  it, or remove it from the cache, which never touches storage.

### 16.5 Uploads and deletion

- A file waits in a local upload queue until it is stored.
- A large file goes up through the provider's resumable or multipart
  upload, in parts.
  - Providers require it above a size, such as Google Drive above 5 MB per
    request.
  - The upload session is recorded, so after a crash the upload continues
    from the last part stored, instead of starting over.
  - A session the provider has since expired starts over, from the kept
    bytes.
  - Any object past the provider's single request limit goes up this way,
    a large write or snapshot included.
- The write that marks a file uploaded is made only once the file is
  stored, so no device ever sees a row whose uploaded file isn't there
  yet, and no write ever waits for a file.
- An uploaded file is deleted once nothing in the latest snapshot or the
  writes after it refers to it as uploaded.
- Uploaded files are deleted by the same devices as logs
  ([§15](#15-snapshots)).
- Deleting a row deletes only coven's copies of its file, never a
  user-provided original.

### 16.6 What a device keeps about files

- Coven keeps, in its local tables, what only this device knows about
  files; none of it syncs:
  - `coven_user_files`: each user-provided file's path, size and
    modification time, by its row and column;
  - `coven_device_files`: each app-provided file this device keeps, and
    where in coven's own folder;
  - `coven_file_uploads`: the upload queue, each file's attempts, last
    failure, and its provider upload session while one is in progress;
  - `coven_cache`: each cached file or chunk, its namespace, size, when it
    was last read, and whether it is pinned;
  - `coven_cache_budgets`: each namespace's budget.
- The bytes themselves are files in the store's directory: coven's own
  copies, and the cache.
- A file's bytes are written and synced to disk before the row that names
  them commits, and a row's removal commits before its bytes are deleted,
  so a crash never leaves a table naming bytes that aren't there.

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
  - A device whose app is older can't sync until it updates; it then
    reloads from that snapshot.
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
- Rows a removal rule had taken out before a breaking change stay out for
  good: they stay in `coven_lost`, and coven forgets their other merge
  records.
- A device that updates runs the migration's second part on its own
  writes still waiting in `coven_uploads`, then uploads them.
  - Without a second part, it uploads them marked lost, and every device
    records them in `coven_lost` without applying them.
  - A lost write names the breaking change by the schema version it raised
    the store to, which the device knows when it migrates, before any
    store log entry for it exists.
  - It converts or marks only writes no upload has tried yet; a tried
    write's bytes are fixed ([§6](#6-syncing-writes)).
  - E.g. Ana's app renames `title` to `name`, while Ben's phone, offline,
    edits a title; when Ben updates, his edit becomes a `name` edit, and
    reaches every device.
- Writes already uploaded in the old version that the breaking change
  hadn't read are lost: every device records them in `coven_lost`, and
  none applies them.
- This happens only when a device uploads just as another makes the
  breaking change.
- If two devices make the same breaking change at once, the one with the
  smaller timestamp counts; the other's entry is dropped, with its snapshot
  ([§9](#9-members-and-roles)).

### 17.2 Coven's schema

- Coven's own tables in the local database, such as `coven_rows`, are
  local only, and a newer coven migrates them in place when the app starts.
- What coven writes to storage has a *format*: write records, store log
  entries, snapshots, paths, every byte of it given in
  [Appendix D](coven-format.md).
- Every object records the format version it was written in, outside its
  encryption, so an older coven tells a newer object from a damaged one,
  and asks for an update instead of reporting it.
- A format change works like a breaking change of the app's schema, with
  coven supplying both parts of the migration.
  - The first device with the newer coven writes a snapshot in the new
    format, and records the store's new format version in the store
    log ([§9](#9-members-and-roles)).
  - A device with an older coven can't sync until its app ships the newer
    one; it then reloads from that snapshot.
  - Its writes still waiting to upload are migrated to the new format.
  - Writes uploaded in the old format that the change hadn't read are
    migrated too, so a format change never loses anything.
- Coven keeps the migrations for every older format, since a device can
  come back with waiting writes from any of them.

## 18. Operations

- An *operation* is work that takes several steps, any of which a crash can
  interrupt, such as removing a member.
- Coven keeps every unfinished operation in one local table:

  ```sql
  CREATE TABLE coven_operations (
    id          INTEGER PRIMARY KEY,
    kind        TEXT NOT NULL,     -- 'remove member', 'reload from snapshot', …
    last_step   INTEGER NOT NULL,  -- 0 before the first step completes
    data        BLOB NOT NULL,     -- what this kind's steps need, in its own shape
    started_by  TEXT NOT NULL,     -- the app call that started it, or 'coven'
    failure     TEXT               -- set when a step fails for good
  );
  ```

- Each kind defines its own steps and the shape of its `data`, so one table
  holds any operation:

  ```
  id   kind                   last_step   data                                started_by
  1    remove member          2           member: ben, new store key: …       remove_member call
  2    reload from snapshot   1           snapshot: snapshots/store/ana-phone/7,    coven
                                          temporary file: …
  ```

- A step that changes the local database updates `last_step` in the same
  transaction:

  ```
  BEGIN
    ALTER TABLE notes RENAME COLUMN title TO name
    UPDATE coven_operations SET last_step = 1 WHERE id = 3
  COMMIT
  ```

  - A crash before the commit undoes both, so the step runs again.
  - The migration can never be done while the row says it isn't, which
    would run it twice.
- A step that changes storage is safe to run twice.
  - Writing an object writes the same path with the same bytes: its bytes
    are fixed and kept before the first attempt ([§6](#6-syncing-writes)).
  - Deleting an object that is already gone succeeds.
- A store log entry takes its number when its bytes are fixed; from then on
  it is always uploaded, even if the operation is abandoned, and the replay
  judges it like any entry, since a device's later entries had read it.
- An operation that writes a store log entry finishes only once the replay
  keeps it; if the replay drops it, the operation starts over from its
  first step, against the member list it then has.
  - Unless the drop leaves nothing to do, such as removing a member a
    concurrent entry already removed.
- A device runs one operation that writes store log entries at a time, and
  none while it reloads from a snapshot.
- The app call that starts an operation returns once it finishes or fails
  for good; without storage it waits. Dropping the call doesn't stop the
  operation.
- Steps are ordered so other devices never see a half-done operation.
- Anything another device reads, such as a store log entry, is uploaded
  last, after everything it refers to.
- When the app starts, coven resumes every unfinished operation from the
  step after its last completed one.
- A step that fails for good, rather than for lack of network, stops its
  operation, and sets its `failure`.
  - The failure goes to the app call that started it while that call
    waits; otherwise it is reported in the sync status.
  - The app can retry or abandon it.
- An operation's row is deleted when its last step completes.

### 18.1 Operations

- Removing a member ([§13](#13-removing-members-and-devices)):
  1. make the new store key, and a new key for each circle the member
     shared with others, with their ids, and record them in the
     operation's row with the member list they were made from;
  2. upload each new key sealed to each remaining member of its audience;
  3. upload the store log entry removing the member;
  4. revoke the member's storage access, or on S3 tell the admin to delete
     their key in the provider's console.
- Removing someone from a circle ([§14.6](#146-leaving-a-circle)):
  1. make the circle's new key and its id, and record them in the
     operation's row;
  2. upload it sealed to each remaining circle member;
  3. upload the store log entry removing them from the circle, naming the
     new key.
- A breaking schema or format change ([§17](#17-schema-changes)):
  1. migrate the database, with its migration write, in one transaction;
  2. upload a snapshot in the new version;
  3. upload the store log entry raising the version.
- Reloading from a snapshot ([§15](#15-snapshots)):
  1. download the snapshot to a temporary file;
  2. replace the synced tables and coven's merge tables
     ([§8](#8-merge)) with it, in one transaction;
  3. migrate the writes waiting in `coven_uploads`, if the snapshot's
     version is newer ([§17](#17-schema-changes)).
- Writing a snapshot ([§15](#15-snapshots)):
  1. write it, sealed, to a temporary file, and record the file;
  2. upload it;
  3. delete the log objects, older snapshots and files it lets go
     ([§16.5](#165-uploads-and-deletion)).
- Making a circle ([§14.3](#143-circles)):
  1. make its first key and its id, and record them;
  2. upload the key sealed to this member;
  3. upload the entry making the circle.
- Adding someone to a circle: seal each of its keys to them, then upload
  the entry adding them.
- Deleting a circle ([§14.7](#147-deleting-a-circle)):
  1. commit the write deleting its rows, recording the operation in the
     same transaction;
  2. upload the entry deleting the circle, after the write.
- Changing where a file is ([§16.1](#161-kinds-and-where-files-are)):
  1. upload it, or download it to the device keeping it;
  2. write its row's file columns, checking the row still has that file;
     if it doesn't, the operation stops for good, and what step 1 made is
     deleted like any unused copy.
- Inviting a person ([§12.2](#122-adding-a-person)):
  1. share the storage with their account, or record the S3 key the admin
     made in the provider's console, and record the invite;
  2. once an admin approves the request, seal the store key to them, then
     write the store log entry adding them;
  3. on decline or expiry, take back the access instead.
- Uploading a large file in parts ([§16.5](#165-uploads-and-deletion)):
  1. start the provider's upload session, and record it in the
     operation's row;
  2. send each part, recording the last one stored;
  3. finish the session.
- Uploading a write, or a file small enough for one request, is not an
  operation: it waits in its queue until stored, and starts over if
  interrupted ([§6](#6-syncing-writes), [§16.5](#165-uploads-and-deletion)).

### 18.2 Example

- Ana removes Ben, and her phone crashes after uploading the sealed keys:

  ```
  coven_operations
    kind            last step   data              started by
    remove member   2           new store key     Ana's "remove Ben"

  storage
    keys/store/7f3a…/ana     uploaded
    keys/store/7f3a…/carol   uploaded
    store-log/ana-phone/9   not yet: the removal entry
  ```

- No other device sees anything yet: the sealed keys are unreferenced until
  the entry exists.
- When the phone restarts, coven resumes at step 3 with the recorded key.
- If it made a new key instead, the sealed copies already uploaded would
  hold a different one; recording it in step 1 is what prevents that.

## 19. Recovery

- Recovery is for any state coven's rules didn't produce: a write that
  never arrives, an object that is damaged, a database a disk or a bug
  damaged, or devices holding different data after the same writes.
- The last is the one nothing else would catch, since every rule would
  still run; it is what a bug in coven or the app looks like.

### 19.1 Noticing

- A write that never arrives:
  - a device waits for every write that a write it holds had read
    ([§7.1](#71-causality));
  - usually the missing write just isn't listed by storage yet, and
    arrives;
  - rarely it never does: someone with storage access deleted it ([§2](#2-threat-model)), or
    the provider lost it;
  - a device can't tell the two apart, so it waits, and the app sees which
    writes it is waiting for, and for how long.
  - E.g. Ben's write 9 had read Ana's log up to 4, but storage shows Carol's
    tablet only Ana's writes 1 to 3; Carol's tablet holds back Ben's write
    9, and its app shows it is waiting for Ana's write 4.
- An object that fails its check when read: it won't decrypt, its
  signature doesn't match, it doesn't parse, or its write breaks the
  merge's rules, such as a timestamp no later than a write it had read.
  - A damaged write or entry holds back what had read it, like a missing
    one; the device reads it again on every sync, in case the failure was
    passing.
  - A damaged snapshot is passed over for the next latest, or the logs.
  - A damaged positions object counts as not posted.
- A damaged local database, found by SQLite's integrity check when the
  database opens.
- Devices that disagree:
  - each device keeps a *fingerprint* of the data in each audience it can
    read, updated as writes apply;
  - it is a hash of exactly what every device must agree on:
    - the rows the app sees;
    - each row's generations;
    - which write set each cell, named by device and number;
    - its `coven_lost` rows;
  - all computed as if the rule for a key present in two audiences didn't
    exist, since which row it shows depends on which circles a device
    reads ([§14.2](#142-moving-rows));
  - and nothing else that differs between devices by design, such as
    `coven_uploads`, `coven_operations`, or local row ids;
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
- The app sees each of these, and which devices are involved; nothing
  reloads on its own after a mismatch, since neither device can tell which
  is wrong. The person picks, as in [§19.3](#193-resetting-a-store), or
  reloads one device ([§19.2](#192-recovering-one-device)).

### 19.2 Recovering one device

- When only one device is broken, it reloads from the latest snapshot
  ([§15](#15-snapshots)).
  - A damaged database fails to open with an error of its own; reloading
    then moves the damaged file aside and starts from the snapshot.
  - A device that opens but disagrees with the others reloads in place.
- Its own writes still waiting in `coven_uploads`, those it can still
  read, are uploaded after, and merge like any late write.

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
- A write that had read everything the snapshot includes came after the
  reset, and applies like any write.
- Any other write the reset snapshot doesn't cover is judged by what it had
  read:
  - if it had read a write the snapshot doesn't include, its cause is gone,
    so it is recorded as lost on every device, and never applied;
  - otherwise it merges like any late write.
- If two devices reset the same audience at once, the one with the smaller
  timestamp counts; the other's entry is dropped, with its snapshot
  ([§9](#9-members-and-roles)).

## 20. API

- The API is listed as Rust declarations with their doc comments.
- Long parameter lists are abbreviated: `/* … */` marks parameters left out,
  and the comment beside it names them.
- Calls on `handle` are on the `CovenHandle` that opening a store returns.
- Every call that reaches storage or the database is `async`.
- A row is named by its table and its *key*: the values of its primary key
  columns, in order ([§8.5](#85-keys-and-uniqueness)).

```rust
// External types used by the declarations below.
use std::{collections::HashMap, future::Future, num::{NonZeroU64, NonZeroUsize},
          path::{Path, PathBuf}, pin::Pin, sync::Arc, time::SystemTime};
use async_trait::async_trait;
use rusqlite::{Params, ToSql};
use tokio::{io::AsyncRead, sync::watch};
use url::Url;
use uuid::Uuid;

/// A row's primary key: one value per key column, in the order the table
/// declares them (§8.5).
pub struct RowKey(/* private */);

impl From<&str> for RowKey { /* a one-column text key */ }
impl<A: ToSql, B: ToSql> From<(A, B)> for RowKey { /* a two-column key */ }

/// One install of the app, by its 64-bit device id (§10).
pub struct DeviceId(/* private */);

/// A member, by the public half of their Ed25519 key pair (§11.1).
pub struct MemberId(/* private */);

/// One write: the device that made it and its number in that device's log (§6).
pub struct WriteId {
    pub device: DeviceId,
    pub number: u64,
}

/// One store log entry: the device that wrote it and its number in that device's store log (§9).
pub struct EntryId {
    /// The device that wrote the entry.
    pub device: DeviceId,
    /// Its entry number, starting at one.
    pub number: u64,
}

/// Where a row goes: the store, or one circle (§14).
pub enum Audience {
    Store,
    Circle(CircleId),
}
```

### 20.1 Opening

- A store lives in one directory on the device, its `StoreDir`, which holds
  the database, coven's copies of files, the cache, and the store's
  settings: its id and name, this device's id, and its storage settings.
- Creating, restoring or joining a store makes the directory and writes
  the settings, so the app never handles a device id ([§10](#10-device-identity)).
- A `StoreLayout` says where an app's stores live on disk.
- Opening a store needs its declared tables ([§20.2](#202-declaring-synced-tables))
  and its migrations ([§20.13](#2013-migrations)).
- *Key custody* is where this device keeps the store keys and circle keys
  it has opened ([§11](#11-keys)): every key it has used, so it reads
  writes made under older ones.
- *Identity custody* is where this device keeps its member's two key pairs
  ([§11.1](#111-cryptography)).
- The app's `CloudKitOps` maps paths to stable record names in the configured
  container, owner and zone; all bytes it receives are encrypted.
- The bridge creates objects once and replaces posted positions atomically.
  Large uploads use bounded CKAssets, keep their ids and parts across
  restarts, and publish a record only after all parts are stored.
- Bridge failures keep their native cause in `StorageError::Provider`,
  classified with `CloudProvider::CloudKit` and a `StorageFailure`.

```rust
/// A store's UUID, independent of its name and location (§20.1).
pub struct StoreId(pub Uuid);

/// A circle's UUID, independent of its name and key (§14.3).
pub struct CircleId(pub Uuid);

/// The wall clock used for timestamps (§7.2).
pub trait Clock: Send + Sync {
    /// The wall clock's current time.
    fn now(&self) -> SystemTime;
}

/// A shared clock supplied when opening a store (§21.2).
pub type ClockRef = Arc<dyn Clock>;

/// The system wall clock used unless the app supplies another (§20.1).
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> SystemTime;
}

/// The source of fresh ids, supplied when creating or opening a store (§21.2).
pub trait IdSource: Send + Sync {
    /// A fresh UUID; distinct calls must yield distinct ids.
    fn new_id(&self) -> Uuid;
    /// The default implementation derives a fresh 64-bit device id from this source (§10).
    fn new_device_id(&self) -> DeviceId;
}

/// A shared source of ids (§21.2).
pub type IdSourceRef = Arc<dyn IdSource>;

/// Random UUIDv4 ids, independent of the clock (§20.1).
pub struct UuidIds;

impl IdSource for UuidIds {
    fn new_id(&self) -> Uuid;
}

/// One store's directory; its path and store id are private (§20.1).
pub struct StoreDir { /* private fields */ }

/// The app directory under which each store has its own directory (§20.1).
pub struct StoreLayout { /* private fields */ }

/// A store on this device (§20.1).
pub struct StoreInfo {
    /// The store's identity.
    pub id: StoreId,
    /// The store's name.
    pub name: String,
}

/// The choices collected before opening a store (§20.1).
pub struct CovenBuilder { /* private fields */ }

/// A shared handle to the open store and its running work (§21.2).
pub struct CovenHandle { /* private fields */ }

/// A handle that only reads an already open store (§5, §20.1).
pub struct CovenReadHandle { /* private fields */ }

/// A table's name, key, audience, file and shared-trigger declarations (§20.2).
pub struct SyncedTable { /* private fields */ }

/// A numbered database migration and its optional waiting-write conversion (§17.1).
pub struct Migration { /* private fields */ }

/// A passphrase owned by custody and erased when dropped (§20.1).
pub struct Passphrase(/* private */);

impl Passphrase {
    /// Takes ownership of the passphrase without exposing it again.
    pub fn new(secret: String) -> Self;
}

/// The member's Ed25519 and X25519 pairs, erased when dropped (§11.1).
pub struct MemberKeys { /* private fields */ }

/// Every opened store and circle key, including older keys (§11, §14.3).
pub struct StoreKeyring { /* private fields */ }

/// The app's provider clients and shared clock, kept private (§20.10).
pub struct OAuthClients { /* private fields */ }

/// A validated encrypted-object path in the store (§4).
pub struct ObjectPath { /* private fields */ }

impl ObjectPath {
    /// A device's create-once write record (§6).
    pub fn device_log(device: DeviceId, number: NonZeroU64) -> Self;
    /// A device's create-once store log entry (§9).
    pub fn store_log(device: DeviceId, number: NonZeroU64) -> Self;
    /// A snapshot written by a device (§15).
    pub fn snapshot(audience: Audience, device: DeviceId, number: NonZeroU64) -> Self;
    /// A device's posted positions (§6).
    pub fn positions(device: DeviceId) -> Self;
    /// A sealed store key for a member (§11).
    pub fn store_key(key: KeyId, member: &MemberId) -> Self;
    /// A sealed circle key for a member (§14.3).
    pub fn circle_key(circle: CircleId, key: KeyId, member: &MemberId) -> Self;
    /// An uploaded file's encrypted bytes, under its random id (§16.2).
    pub fn file(id: FileId) -> Self;
    /// An encrypted join request under its invite id (§12.2).
    pub fn join_request(invite: InviteId) -> Self;
    /// Parses a listed or recorded path, refusing paths outside the store's layout.
    pub fn parse(value: &str) -> Result<Self, StorageError>;
    /// The path bound into the object's encryption.
    pub fn as_str(&self) -> &str;
    /// Whether this is a posted-positions path, the only kind that may be replaced.
    pub fn is_replaceable(&self) -> bool;
    /// The device named by a log, snapshot or positions path.
    pub fn device(&self) -> Option<DeviceId>;
}

/// A prefix of the validated object layout (§4).
pub struct ObjectPrefix { /* private fields */ }

impl ObjectPrefix {
    /// Every object in the store's location.
    pub fn all() -> Self;
    /// Every write of a device.
    pub fn device_log(device: DeviceId) -> Self;
    /// Every device's writes, to find devices not yet known (§6).
    pub fn device_logs() -> Self;
    /// Every store log entry of a device.
    pub fn store_log(device: DeviceId) -> Self;
    /// Every device's store log entries (§6, §9).
    pub fn store_logs() -> Self;
    /// Every snapshot.
    pub fn snapshots() -> Self;
    /// Every stored file.
    pub fn files() -> Self;
    /// Every waiting join request.
    pub fn join_requests() -> Self;
    /// Every sealed key.
    pub fn keys() -> Self;
    /// Every device's posted positions.
    pub fn positions() -> Self;
    /// The prefix supplied to the provider.
    pub fn as_str(&self) -> &str;
    /// Whether a validated path is under this prefix.
    pub fn contains(&self, path: &ObjectPath) -> bool;
}

/// A nonempty byte range, including its start and excluding its end (§16.3).
pub struct ByteRange { /* private fields */ }

impl ByteRange {
    /// Validates that the start is before the end.
    pub fn new(start: u64, end: u64) -> Result<Self, StorageError>;
    /// The first byte included.
    pub fn start(self) -> u64;
    /// The first byte excluded; a read refuses an end beyond the object.
    pub fn end(self) -> u64;
    /// The number of requested bytes.
    pub fn len(self) -> u64;
    /// Always false for a validated range.
    pub fn is_empty(self) -> bool;
}

/// A durable upload prepared by the app's CloudKit bridge (§16.5).
pub struct CloudKitUpload {
    /// The bridge's recorded session capability, erased on drop.
    pub id: SecretText,
    /// The maximum part size the bridge accepts.
    pub part_size: usize,
}

/// The bridge's confirmed upload state (§16.5).
pub enum CloudKitUploadStatus {
    /// Bytes stored before publishing the complete object.
    Uploading {
        /// The contiguous stored prefix, in bytes.
        confirmed: u64,
    },
    /// This session's object has been published in the zone.
    Complete,
}

/// Native CloudKit calls implemented by the app (§4, §20.1).
#[async_trait]
pub trait CloudKitOps: Send + Sync {
    /// Creates complete encrypted bytes using the server's create-only policy.
    async fn create(&self, location: &StorageConfig, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Replaces a complete posted-positions object atomically (§6).
    async fn replace(&self, location: &StorageConfig, path: &ObjectPath, bytes: &[u8]) -> Result<(), StorageError>;
    /// Reads the whole object or only the asset parts covering the range (§16.3).
    async fn read(&self, location: &StorageConfig, path: &ObjectPath, range: Option<ByteRange>) -> Result<Vec<u8>, StorageError>;
    /// Lists every object under the prefix, following every native query cursor.
    async fn list(&self, location: &StorageConfig, prefix: &ObjectPrefix) -> Result<Vec<ObjectPath>, StorageError>;
    /// Deletes an object and its parts; an already absent object succeeds (§18).
    async fn delete(&self, location: &StorageConfig, path: &ObjectPath) -> Result<(), StorageError>;
    /// Sets read/write sharing for the named Apple account (§12.2, §13).
    async fn set_access(&self, location: &StorageConfig, email: &str, granted: bool) -> Result<(), StorageError>;
    /// Prepares a durable upload without publishing its destination (§16.5).
    async fn begin_upload(&self, location: &StorageConfig, path: &ObjectPath, total: u64) -> Result<CloudKitUpload, StorageError>;
    /// Returns confirmed progress, including parts whose replies were lost.
    async fn upload_status(&self, location: &StorageConfig, id: &SecretText) -> Result<CloudKitUploadStatus, StorageError>;
    /// Stores a part at its byte offset; retrying identical bytes is idempotent.
    async fn upload_part(&self, location: &StorageConfig, id: &SecretText, offset: u64, bytes: &[u8]) -> Result<(), StorageError>;
    /// Publishes all parts atomically; a retry succeeds without replacing another object.
    async fn finish_upload(&self, location: &StorageConfig, id: &SecretText) -> Result<(), StorageError>;
    /// Discards pending parts without deleting a published object.
    async fn abort_upload(&self, location: &StorageConfig, id: &SecretText) -> Result<(), StorageError>;
}

/// A database or store call's result, retaining its typed cause (§21.3).
pub type CovenResult<T> = Result<T, CovenError>;

/// Failures of opening, reading and writing a store (§5, §20.1, §20.3).
pub enum CovenError {
    /// The database or a write's validation failed.
    Database(DbError),
    /// An app migration failed or cannot run on this schema.
    Migration(MigrationError),
    /// Coven's tables need a migration this open cannot run.
    CovenMigration(CovenMigrationError),
    /// The directory's settings could not be read.
    Settings(SettingsError),
    /// The writer's lock could not be acquired.
    Lock(StoreLockError),
    /// A required builder choice was not supplied (§20.1).
    MissingConfiguration { field: &'static str },
    /// Reloading from storage failed (§19.2).
    Sync(SyncError),
}

/// A failed database call or a write the database refuses (§5, §8, §14, §16).
pub enum DbError {
    /// This handle or a clone has closed the store (§20.1).
    StoreClosed,
    /// SQLite refused or failed a statement, preserving its error.
    Sqlite(rusqlite::Error),
    /// SQLite's opening integrity check found damage (§19.1).
    DamagedDatabase,
    /// A synced table declaration or schema is invalid.
    Schema(SchemaError),
    /// App SQL read, changed or defined one of coven's internal objects (§5).
    InternalTable { table: String },
    /// App SQL tried transaction control, a PRAGMA, ATTACH or loading an
    /// extension (§5, §20.13).
    StatementForbidden { operation: &'static str },
    /// SQLite rolled a migration's transaction back itself, so it can't go on (§20.13).
    TransactionEnded,
    /// SQLite kept another journal mode instead of WAL (§5).
    WalUnavailable { mode: String },
    /// The wall clock reads past the last time a timestamp holds, or no
    /// later timestamp is left (§7.2).
    ClockOutOfRange,
    /// A value or a row is larger than the format holds (§5), or a write's record
    /// is larger than SQLite's largest value (§6).
    TooLarge { field: &'static str, actual: u64, maximum: u64 },
    /// A local trigger wrote a synced table or a shared trigger a local one (§8.7).
    TriggerTarget { trigger: String, table: String },
    /// A reference points at a row outside the source row's audience (§14.5).
    ReferenceAudience { table: String, key: RowKey, column: String },
    /// A write targets a circle whose deletion has been applied (§14.7).
    DeletedCircle(CircleId),
    /// A write puts a row in a circle this member isn't in, or in no circle
    /// the store log has (§14.5, §14.6).
    NotInCircle(CircleId),
    /// An inserted row's independent key holds no UUID (§8.5).
    KeyNotUuid { table: String, key: RowKey },
    /// A downloaded write fails the merge's checks, such as a timestamp no
    /// later than a write it had read; it is never applied (§19.1).
    InvalidWrite { write: WriteId, error: MergeError },
    /// A write changes a file declared write-once (§20.2).
    FileWriteOnce { table: String, key: RowKey },
    /// A file reference no longer names the row's file (§16.3).
    FileRefChanged { table: String, key: RowKey },
    /// The row's declared size differs from the prepared file (§16).
    FileSizeMismatch { expected: u64, actual: u64 },
    /// The user's file is no longer at the recorded path (§16.1).
    UserFileMissing { path: PathBuf },
    /// The user's file changed during preparation or since it was prepared (§20.3).
    UserFileChanged { path: PathBuf },
    /// Reading or keeping file bytes failed (§20.3).
    Disk(DiskError),
    /// A transaction failed and rolling it back failed too.
    Rollback { operation: Box<DbError>, rollback: rusqlite::Error },
    /// Closing failed for these connections, after every one was tried (§20.1).
    Closing { failures: Vec<DbError> },
}

/// A schema rule checked on open and after migrating (§8, §14.1).
pub enum SchemaError {
    /// A declared synced table isn't in the database (§20.2).
    MissingTable { table: String },
    /// Two declarations name one table (§20.2).
    DuplicateTable { table: String },
    /// A table declares both `audience_column` and `audience_from` (§20.2).
    TwoAudiences { table: String },
    /// A declared file column isn't in the table (§20.2).
    FileColumn { table: String, column: String },
    /// A trigger declared shared isn't on the table (§8.7).
    MissingTrigger { table: String, trigger: String },
    /// A synced table has no primary key (§8.5).
    NoPrimaryKey { table: String },
    /// A primary key column allows NULL (§8.5).
    NullableKey { table: String, column: String },
    /// SET NULL or SET DEFAULT would put NULL in a NOT NULL column (§8.4).
    ImpossibleAction { table: String, column: String },
    /// A local table's foreign key could stop coven deleting a synced row (§8.4).
    LocalChildAction { table: String, column: String },
    /// SQLite chooses the primary key itself (§8.5).
    GeneratedPrimaryKey { table: String },
    /// An independent key has no text column to hold its UUID (§8.5).
    IndependentKeyNotUuid { table: String },
    /// Declared key columns do not match the table's primary key (§20.2).
    KeyColumns { table: String },
    /// SET NULL or SET DEFAULT acts on a primary-key column (§8.4).
    PrimaryKeyAction { table: String, column: String },
    /// The audience root column is nullable or is not text (§14, §20.2).
    AudienceColumn { table: String, column: String },
    /// The audience foreign key spans more than one column (§14.1).
    AudienceForeignKeyColumns { table: String },
    /// The audience foreign key does not point into a synced table (§14.1).
    AudienceForeignKeyTarget { table: String, column: String },
    /// The audience foreign key uses SET NULL or SET DEFAULT (§14.1).
    AudienceForeignKeyAction { table: String, column: String },
    /// Following audience foreign keys forms a loop (§14.1).
    AudienceCycle { table: String },
    /// A unique constraint or shared key spans audiences (§14.1).
    AudienceConstraint { table: String, constraint: String },
    /// A shared trigger lacks WHEN NOT coven_applying() (§8.7).
    SharedTriggerGuard { table: String, trigger: String },
}

/// Coven's local tables cannot be used at this version (§17.2, §20.1).
pub enum CovenMigrationError {
    /// Opening would need to migrate, but this open refuses it.
    Pending,
    /// A migration failed and its transaction rolled back.
    Failed { source: Box<DbError> },
}

/// A filesystem failure, including whether bytes already changed (§20.1, §21.3).
pub enum FileError {
    /// The operation failed without replacing its target.
    Io { operation: &'static str, path: PathBuf, source: Box<dyn std::error::Error + Send + Sync> },
    /// Replacement is visible, but syncing its directory failed.
    AfterReplace { path: PathBuf, source: Box<dyn std::error::Error + Send + Sync> },
    /// Removal is visible, but syncing its directory failed.
    AfterRemove { path: PathBuf, source: Box<dyn std::error::Error + Send + Sync> },
    /// Removing an unpublished temporary file failed too.
    Cleanup { operation: Box<FileError>, cleanup: Box<dyn std::error::Error + Send + Sync> },
}

/// File reads and custody report the same filesystem failures (§16, §20.1).
pub type DiskError = FileError;

/// The store's directory settings could not be read or written (§20.1).
pub enum SettingsError {
    /// No settings file exists.
    Missing(StoreId),
    /// The settings do not contain the declared data.
    Corrupt(Box<dyn std::error::Error + Send + Sync>),
    /// Settings name a different store from the directory.
    WrongStore { expected: StoreId, actual: StoreId },
    /// The settings path is not a regular file.
    NotRegularFile(StoreId),
    /// The filesystem operation failed.
    File(FileError),
}

/// The writer's lock could not be taken (§20.1).
pub enum StoreLockError {
    /// Another handle or process holds the lock.
    AlreadyOpen(StoreId),
    /// Opening or locking the lock file failed.
    File(FileError),
}

/// Listing the app's stores failed (§20.1).
pub enum StoreLayoutError {
    /// Listing directories or reading settings failed.
    File(FileError),
}

/// Creating a store failed, with publication and rollback made explicit (§20.1).
pub enum StoreCreationError {
    /// A directory or file already occupies this store id.
    AlreadyExists(StoreId),
    /// A file operation failed before publication.
    File(FileError),
    /// Writing the store's settings failed before publication.
    Settings(SettingsError),
    /// The store is visible, but syncing its parent directory failed.
    Published { id: StoreId, source: Box<dyn std::error::Error + Send + Sync> },
    /// Removing the unpublished directory failed too.
    Rollback { operation: Box<StoreCreationError>, cleanup: Box<dyn std::error::Error + Send + Sync> },
}

/// Deleting the local store stopped at a step the caller may retry (§20.1).
pub enum StoreDeletionError {
    /// The store is open or its lock could not be taken.
    Lock(StoreLockError),
    /// Removing a keychain entry failed.
    Key(KeyError),
    /// Removing the store directory failed.
    File(FileError),
}

/// Unlocking, keeping or forgetting keys or host secrets failed (§20.1, §20.11).
pub enum KeyError {
    /// The custody file operation failed.
    File(FileError),
    /// A cryptographic service was unavailable or stored bytes failed validation.
    Crypto(CryptoError),
    /// The opened key material was malformed.
    Material(MaterialError),
    /// The passphrase was wrong or the custody file was altered.
    PassphraseAuthentication,
    /// The passphrase file's header was malformed or unsupported.
    PassphraseHeader,
    /// The stored passphrase settings exceed accepted resource bounds.
    PassphraseParameters,
    /// The device lacks resources needed to unlock or keep keys.
    Unavailable(Box<dyn std::error::Error + Send + Sync>),
    /// The OS credential store refused the call.
    Keychain(KeychainError),
    /// The keyring service was not registered at startup.
    ServiceNotRegistered,
    /// A different service name was already registered.
    ServiceAlreadyRegistered,
    /// The service name is empty or contains NUL.
    InvalidServiceName,
    /// This platform has no native credential store.
    UnsupportedKeyringPlatform,
    /// The host secret name cannot name an app entry.
    SecretName(SecretNameError),
    /// A stored host secret is not UTF-8.
    HostSecretEncoding,
}

/// Secret bytes crossing custody or code boundaries, erased when dropped (§11, §12).
pub struct SecretBytes { /* private fields */ }

impl SecretBytes {
    /// Takes ownership of bytes, including their allocation capacity.
    pub fn new(bytes: Vec<u8>) -> Self;
    /// Borrows bytes for custody or a restore code.
    pub fn as_bytes(&self) -> &[u8];
}

/// Secret text, erased when dropped and redacted in diagnostics (§11, §20.10).
pub struct SecretText { /* private fields */ }

impl SecretText {
    /// Takes ownership of text, including its allocation capacity.
    pub fn new(text: String) -> Self;
    /// Borrows text for a provider request or key custody.
    pub fn as_str(&self) -> &str;
}

/// A native keychain cause whose diagnostics do not expose secret bytes (§11).
pub struct KeychainError(/* private */);

/// A host secret name is invalid (§20.11).
pub enum SecretNameError {
    /// The name is empty.
    Empty,
    /// The name contains a colon.
    Separator,
    /// The name is reserved for coven.
    Reserved,
    /// Native credential APIs cannot represent NUL in a name.
    Nul,
}

/// A cryptographic operation failed (§11.1).
pub enum CryptoError {
    /// The device cannot provide a cryptographic service needed by this call.
    Unavailable(Box<dyn std::error::Error + Send + Sync>),
    /// The ciphertext, key, path, index or associated data did not authenticate.
    Authentication,
    /// A sealed value was truncated or had invalid framing.
    Malformed,
    /// The X25519 public key has low order.
    WeakSealingKey,
    /// The Ed25519 public key is invalid or weak.
    InvalidMemberId,
    /// The member's signature did not verify.
    Signature,
    /// Authenticated key material had the wrong shape.
    Material(MaterialError),
}

/// A key or encoded secret has an invalid representation, or isn't held (§11).
pub enum MaterialError {
    /// The encoded secret or sealed value is malformed.
    Encoding,
    /// A different key is already held under this store key id.
    StoreKeyConflict(KeyId),
    /// A different key is already held under this circle and key id.
    CircleKeyConflict { circle: CircleId, key: KeyId },
    /// The keyring does not hold this store key.
    UnknownStoreKey(KeyId),
    /// The keyring does not hold this circle key.
    UnknownCircleKey { circle: CircleId, key: KeyId },
}

/// A store or circle key's random id, named by the store log entry that
/// brings the key in (§11).
pub struct KeyId(pub Uuid);

/// Registers the OS keychain service every key and secret is stored under.
/// Called once at startup, before any store opens.
pub fn set_keyring_service(name: impl Into<String>) -> Result<(), KeyError>;

impl StoreLayout {
    /// The stores under `app_dir`, one directory each.
    pub fn new(app_dir: PathBuf) -> Self;

    /// The stores on this device, by id and name.
    pub async fn stores(&self) -> Result<Vec<StoreInfo>, StoreLayoutError>;

    /// The directory of the store with `id`.
    pub fn store_dir(&self, id: &StoreId) -> StoreDir;
}

pub struct Coven;

impl Coven {
    /// Makes a new store on this device, named `name`: its directory, its id
    /// and this device's id, both from `ids`. Storage is set up after opening
    /// (§20.5).
    pub async fn create_store(
        layout: &StoreLayout,
        name: &str,
        ids: IdSourceRef,
    ) -> Result<StoreDir, StoreCreationError>;

    /// Starts opening the store in `store_dir`, with the settings coven keeps
    /// there.
    pub fn builder(store_dir: StoreDir) -> CovenBuilder;

    /// Deletes a closed store from this device: every keychain entry coven
    /// holds for it, including the named host secrets, then its directory.
    /// Refused while the store is open; storage is untouched, and running it
    /// again finishes a deletion that failed partway.
    pub async fn delete_store(
        store_dir: &StoreDir,
        host_secret_names: &[&str],
    ) -> Result<(), StoreDeletionError>;
}

impl CovenBuilder {
    /// The tables that sync (§20.2). Required.
    pub fn synced_tables(self, tables: Vec<SyncedTable>) -> Self;

    /// The app's schema migrations, numbered from 1 with no gaps (§20.13).
    /// Required.
    pub fn migrations(self, migrations: Vec<Migration>) -> Self;

    /// Whether opening may migrate coven's own tables to this version of
    /// coven (§17.2). Required by `open`.
    pub fn coven_migration_policy(self, policy: CovenMigrationPolicy) -> Self;

    /// The wall clock that timestamps use (§7.2). Defaults to the system clock.
    pub fn clock(self, clock: ClockRef) -> Self;

    /// The source of new ids (§21.2). Defaults to `UuidIds`, random UUIDs.
    pub fn id_source(self, ids: IdSourceRef) -> Self;

    /// The app's own OAuth clients for Google Drive, Dropbox and OneDrive.
    /// Coven ships none.
    pub fn oauth_clients(self, clients: OAuthClients) -> Self;

    /// The app's CloudKit calls, on Apple platforms. Every iCloud operation
    /// goes through them.
    pub fn apply_cloudkit_ops(self, ops: Option<Arc<dyn CloudKitOps>>) -> Self;

    /// How many file uploads run at once. Defaults to one.
    pub fn max_concurrent_uploads(self, n: NonZeroUsize) -> Self;

    /// How many file downloads a pin runs at once (§20.8). Defaults to one.
    pub fn max_concurrent_downloads(self, n: NonZeroUsize) -> Self;

    /// Where this device keeps the store keys: the OS keychain by default, a
    /// file sealed with a passphrase, memory for this session only, or the
    /// app's own `StoreKeyCustody`.
    pub fn key_custody(self, custody: KeyCustody) -> Self;

    /// Where this device keeps its member's keys, with the same four choices
    /// and the app's own `MemberKeyCustody` as the last.
    pub fn identity_custody(self, custody: IdentityCustody) -> Self;

    /// Opens the store for reading and writing, taking the store's lock.
    /// Opening runs migrations and reads no key, so a store opens and works
    /// on the device before any key is unlocked; the first call that needs a
    /// key reads it. Opening never starts syncing; `connect_sync` does.
    pub async fn open(self) -> CovenResult<CovenHandle>;

    /// Opens a store whose database is damaged (§19.2): moves the damaged
    /// file aside, loads the latest snapshot, and queues the waiting writes it
    /// can still read from the old file, then resumes unfinished operations.
    /// It needs storage and the store key; without either it fails, leaving
    /// the damaged file where it was.
    pub async fn open_reloading(self) -> CovenResult<CovenHandle>;

    /// Opens the store for reading only, alongside a handle that has it open,
    /// for example from another process. It takes no lock and runs no
    /// migration, and refuses a database whose schema is newer than its
    /// migrations or whose coven tables need migrating.
    pub async fn open_read_only(self) -> CovenResult<CovenReadHandle>;
}

pub enum KeyCustody {
    Keyring,
    Passphrase(Passphrase),
    InMemory(StoreKeyring),
    Custom(Arc<dyn StoreKeyCustody>),
}

pub enum IdentityCustody {
    Keyring,
    Passphrase(Passphrase),
    InMemory(MemberKeys),
    Custom(Arc<dyn MemberKeyCustody>),
}

pub enum CovenMigrationPolicy {
    /// Migrate coven's tables on open.
    ApplyPending,
    /// Fail to open with `CovenMigrationError::Pending` instead.
    RefusePending,
}

impl CovenHandle {
    /// Closes the store: stops syncing, closes every database connection and
    /// releases the lock. Any later call on this handle or a clone of it
    /// fails with `DbError::StoreClosed`.
    pub async fn close(&self);
}
```

Example:

```rust
coven::set_keyring_service("com.example.notes")?;

let layout = StoreLayout::new(app_dir);
let store_dir = Coven::create_store(&layout, "Household", Arc::new(UuidIds)).await?;

let handle = Coven::builder(store_dir)
    .synced_tables(tables())                        // §20.2
    .migrations(migrations())                       // §20.13
    .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
    .open()
    .await?;
```

### 20.2 Declaring synced tables

- Each synced table declares its kind of key ([§8.5](#85-keys-and-uniqueness)),
  how its rows get their audience ([§14](#14-audiences)), and whether its rows
  carry a file ([§16](#16-files)).
- A table declares at most one of `audience_column` and `audience_from`;
  opening refuses both with `SchemaError::TwoAudiences`. A table that
  declares neither is in the store.

```rust
pub enum RowIdentity {
    /// Each new row gets a UUID, version 4 or 7, in canonical lowercase form.
    IndependentUuid,
    /// The app derives the key from what makes the row unique, so equal keys
    /// are one row on every device.
    SharedKey,
}

impl SyncedTable {
    /// Declares a synced table and its kind of key.
    pub fn new(name: impl Into<String>, identity: RowIdentity) -> Self;

    /// The columns of its primary key, in order, matching the table's
    /// PRIMARY KEY. Without this call the key is the one column `id`.
    pub fn key_columns<I, S>(self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>;

    /// Makes the table a root: its text `column` holds each row's audience,
    /// `store` or a circle's id, and is never NULL (§14).
    pub fn audience_column(self, column: impl Into<String>) -> Self;

    /// A descendant: each row takes the audience of the row its
    /// `foreign_key` column points at (§14.1).
    pub fn audience_from(self, foreign_key: impl Into<String>) -> Self;

    /// The table's rows carry a file, declared by `declaration`.
    pub fn carries_files(self, declaration: FileDecl) -> Self;

    /// Declares the trigger `name` on this table as shared (§8.7); every
    /// trigger not declared shared is local. Opening refuses a shared trigger
    /// without `WHEN NOT coven_applying()`, naming it.
    pub fn shared_trigger(self, name: impl Into<String>) -> Self;
}

pub enum Provenance {
    /// The user's own file at a path on their device; coven records it and
    /// never copies, changes or deletes it (§16.1).
    UserProvided,
    /// Bytes the app hands to coven, which keeps and owns them.
    AppProvided,
}

pub enum CacheFill {
    /// Devices download an uploaded file as soon as its row arrives.
    CacheEager,
    /// Devices download it on first read.
    CacheLazy,
}

pub enum Uploads {
    /// A file is uploaded when a write attaches it, and the write uploads
    /// after it (§16.5).
    WhenAttached,
    /// A file stays on the device that attached it until the app uploads it
    /// (§16.1).
    WhenAsked,
}

/// One table's file columns, namespace, kind, upload and cache choices (§16, §20.2).
pub struct FileDecl { /* private fields */ }

impl FileDecl {
    /// Declares the file a table's rows carry: its namespace, which groups
    /// files in the cache, each with its own budget (§20.8), its kind, when
    /// it is uploaded, and when devices download it.
    pub fn new(
        namespace: impl Into<String>,
        provenance: Provenance,
        uploads: Uploads,
        fill: CacheFill,
    ) -> Self;

    /// The column naming the file. Defaults to `id`.
    pub fn with_id_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's size in bytes. Defaults to `size`.
    pub fn with_size_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's content hash, the SHA-256 of its bytes,
    /// which coven fills in (§16.2). Defaults to `hash`.
    pub fn with_hash_column(self, column: impl Into<String>) -> Self;

    /// The column holding where the file is, which coven fills in:
    /// `uploaded` with the file's id and key, or the id of the device that
    /// has it (§16.1, §16.2). Read it through `FileRef::location`.
    /// Defaults to `location`.
    pub fn with_location_column(self, column: impl Into<String>) -> Self;

    /// Refuses a write that points an existing row at a different file.
    pub fn write_once(self) -> Self;
}
```

Example:

```rust
fn tables() -> Vec<SyncedTable> {
    vec![
        // A root: each note is the store's or a circle's.
        SyncedTable::new("notes", RowIdentity::IndependentUuid).audience_column("audience"),
        // Descendants of notes. Each attachment carries the user's own file.
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("attachments", Provenance::UserProvided, Uploads::WhenAsked, CacheFill::CacheLazy)),
        // A thumbnail the app makes, in the note's audience.
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .audience_from("note_id")
            .carries_files(FileDecl::new("thumbnails", Provenance::AppProvided, Uploads::WhenAttached, CacheFill::CacheEager)),
        // In the store, with keys from the tag's name.
        SyncedTable::new("tags", RowIdentity::SharedKey),
        // A shared key over two columns, which includes note_id (§14.1).
        SyncedTable::new("note_tags", RowIdentity::SharedKey)
            .key_columns(["note_id", "tag_id"])
            .audience_from("note_id"),
    ]
}
```

### 20.3 Writing

- A write runs the app's SQL in one transaction ([§5](#5-local-database)).
- The closure returns the write's result; an error rolls the whole write
  back, files included.

```rust
/// App-provided files staged for one write; a failure discards them (§20.3).
pub struct WriteBatch { /* private fields */ }

/// A transaction's SQL access, borrowing its connection and write's file state (§5, §20.3).
pub struct SqlContext<'connection, 'write> { /* private fields */ }

/// A user's file checked before a write; callers cannot change its recorded facts (§20.3).
pub struct PreparedUserFile { /* private fields */ }

/// The facts recorded for a user's original file (§16.1).
pub struct UserFile {
    /// The user's path, which coven never changes or deletes.
    pub path: PathBuf,
    /// Its size in bytes.
    pub size: u64,
    /// Its recorded modification time.
    pub modified_at: SystemTime,
}

/// A row's file at the time it was read; its captured facts are private (§16.3).
pub struct FileRef { /* private fields */ }

impl CovenHandle {
    /// Runs one write.
    pub async fn write<F, R>(&self, sql: F) -> CovenResult<R>
    where
        F: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;

    /// Runs one write that also hands coven app-provided files. `build` adds
    /// the files, then `sql` runs the write that refers to them.
    pub async fn write_with_files<F, S, R>(&self, build: F, sql: S) -> CovenResult<R>
    where
        F: FnOnce(&mut WriteBatch) -> CovenResult<()> + Send + 'static,
        S: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;
}

impl WriteBatch {
    /// Hands coven an app-provided file's bytes, kept under `namespace` and
    /// `id`. `bytes` is a byte buffer, or a stream read once, so a large file
    /// never has to fit in memory. The write must give a row of a table
    /// declared under `namespace` this id in its id column; a staged file
    /// no row names, or a row naming an id neither staged nor kept, fails
    /// the write. Coven fills that row's size, hash and where-columns.
    pub fn put_file(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<FileSource>,
    );
}

pub enum FileSource {
    Bytes(Vec<u8>),
    Stream(Pin<Box<dyn AsyncRead + Send>>),
}

impl SqlContext<'_, '_> {
    /// Ordinary SQL inside the write.
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize>;
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()>;
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>;

    /// Runs the app's INSERT of a row that carries a user-provided file, and
    /// records the prepared file on it.
    pub fn insert_user_file(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        prepared: PreparedUserFile,
        insert_sql: &str,
        params: &[(&str, &dyn ToSql)],
    ) -> Result<(), DbError>;

    /// Records a prepared user-provided file on a row the write already has.
    /// Fails if the row's size column disagrees with the file, or the file
    /// changed since it was prepared.
    pub fn register_user_file(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        prepared: PreparedUserFile,
    ) -> Result<(), DbError>;

    /// Forgets the user-provided file recorded on a row. The file itself is
    /// untouched.
    pub fn clear_user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<(), DbError>;

    /// Checks that a file reference taken earlier still names the row's
    /// current file; the write fails if it doesn't. Called before changing
    /// or deleting the row.
    pub fn validate_file_ref(&self, reference: &FileRef) -> Result<(), DbError>;
}

/// Reads a user's file once, before the write, for its size and content.
/// `progress` receives the bytes read so far. Fails if the file changes
/// while it is read.
pub async fn prepare_user_file(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedUserFile, DbError>;
```

Example:

```rust
let note_id = Uuid::now_v7().to_string();
let attachment_id = Uuid::now_v7().to_string();
let size = std::fs::metadata(&path)?.len() as i64;
let prepared = prepare_user_file(&path, |read| show_progress(read)).await?;

handle
    .write(move |sql| {
        sql.execute(
            "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, 'store')",
            (&note_id, "Paint colors"),
        )?;
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, size) VALUES (?1, ?2, ?3, ?4)",
            (&attachment_id, &note_id, "Swatches", size),
        )?;
        sql.register_user_file("attachments", attachment_id.as_str(), prepared)?;
        Ok(())
    })
    .await?;
```

### 20.4 Reading

- Reads run on several read-only connections at once ([§5](#5-local-database)).
- A *live query* runs once, then again whenever a write commits that changes
  rows it read.

```rust
/// SQL access to one read-only database snapshot (§5, §20.4).
pub struct SqlReadContext<'connection> { /* private fields */ }

/// An awaitable read that keeps its store borrow until it finishes (§20.4).
pub struct Read<'a, F> { /* private fields */ }

impl<F, R> Future for Read<'_, F>
where
    F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
    R: Send + 'static,
{
    type Output = CovenResult<R>;
    fn poll(self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<Self::Output>;
}

/// A subscription's results and lifetime; dropping it ends the query (§20.4).
pub struct LiveQuery<T> { /* private fields */ }

/// A subscription whose request may be replaced while it runs (§20.4).
pub struct ReconfigurableLiveQuery<Q, T> { /* private fields */ }

/// Shared access to replacing a live query's request (§20.4).
pub struct LiveQueryRequests<Q> { /* private fields */ }

/// A request revision assigned by one live query (§20.4).
pub struct LiveQueryRevision(pub u64);

/// The query was dropped before its request could be replaced (§20.4).
pub struct LiveQueryClosed;

/// A result together with the request that produced it (§20.4).
pub struct ReconfigurableLiveQueryEvent<Q, T> {
    /// The request used by this run.
    pub request: Q,
    /// The revision assigned to that request.
    pub revision: LiveQueryRevision,
    /// A new request, changed rows, or both.
    pub cause: LiveQueryCause,
    /// The query's result, including failures that do not end the query.
    pub result: CovenResult<T>,
}

/// Why a reconfigurable live query ran; its first run answers the initial request (§20.4).
pub enum LiveQueryCause {
    /// A new request, including the initial one.
    Request,
    /// A write changed rows the query read.
    Write,
    /// A new request and a relevant write arrived before this run.
    RequestAndWrite,
}

/// An open file with checked identity, header and range-reading state (§16.3).
pub struct FileStream { /* private fields */ }

/// App data could not be sealed or opened with its store key (§11, §20.11).
pub enum SealError {
    /// The keyring lacks the named key or the encoded material is invalid.
    Key(MaterialError),
    /// The cipher refused the bytes or their associated data.
    Crypto(CryptoError),
    /// Lazy unlocking from custody failed (§20.1).
    Custody(KeyError),
}

impl CovenHandle {
    /// A read of one consistent snapshot, run when awaited. Attach `process`
    /// to work on the result after the connection is released.
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>
    where
        F: FnOnce(SqlReadContext<'_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;

    /// A live query. Coven records the tables, columns and keys the query
    /// reads, and reruns it only for writes that touch them.
    pub fn subscribe<F, R>(&self, query: F) -> LiveQuery<R>
    where
        F: Fn(SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static;

    /// A live query whose request, such as a page or a search term, can be
    /// replaced without starting a new subscription.
    pub fn subscribe_reconfigurable<Q, F, R>(
        &self,
        initial_request: Q,
        query: F,
    ) -> ReconfigurableLiveQuery<Q, R>
    where
        Q: Clone + PartialEq + Send + Sync + 'static,
        F: Fn(&Q, SqlReadContext<'_>) -> CovenResult<R> + Send + Sync + 'static,
        R: Send + 'static;

    /// Every lost value and removed row, as `coven_lost` holds them (§8).
    pub async fn lost_values(&self) -> CovenResult<Vec<LostValue>>;

    /// Dismisses lost values the app has dealt with, in a write, so every
    /// device drops them from `coven_lost`.
    pub async fn dismiss_lost_values(&self, values: &[LostValue]) -> CovenResult<()>;

    /// The same, as a live query.
    pub fn subscribe_lost_values(&self) -> LiveQuery<Vec<LostValue>>;
}

impl SqlReadContext<'_> {
    /// Ordinary SQL reads. A read context can't write.
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>;
}

impl<'a, F, R> Read<'a, F> {
    /// Runs `process` on the read's result on a separate worker, after the
    /// read has released its connection.
    pub async fn process<P, T>(self, process: P) -> CovenResult<T>
    where
        P: FnOnce(R) -> CovenResult<T> + Send + 'static;
}

impl<T: Clone + PartialEq> LiveQuery<T> {
    /// The first result at once; after that, waits for a write that changes
    /// the result and returns the new one. An error is a result too, and
    /// doesn't end the query.
    pub async fn next(&mut self) -> CovenResult<T>;

    /// Runs `process` on each result after the connection is released.
    pub fn process<P, U>(self, process: P) -> LiveQuery<U>
    where
        P: Fn(T) -> CovenResult<U> + Send + Sync + 'static;
}

impl<Q, T: Clone + PartialEq> ReconfigurableLiveQuery<Q, T> {
    /// A handle that replaces the request.
    pub fn requests(&self) -> LiveQueryRequests<Q>;

    /// The next result, with the request and revision that produced it, and
    /// whether a new request, a write, or both caused it.
    pub async fn next(&mut self) -> ReconfigurableLiveQueryEvent<Q, T>;
}

impl<Q: Clone + PartialEq> LiveQueryRequests<Q> {
    /// Replaces the request, returning the revision whose result will answer
    /// it. Fails once the query is dropped.
    pub fn set(&self, request: Q) -> Result<LiveQueryRevision, LiveQueryClosed>;
}

/// One `coven_lost` row (§8).
pub struct LostValue {
    pub table: String,
    pub key: RowKey,
    /// One cell's value, or a whole removed row's values.
    pub lost: Lost,
    /// What replaced it: a write that hadn't read it, the removal rules, or
    /// a breaking change or reset the write hadn't read.
    pub replaced_by: Replacement,
}

pub enum Lost {
    Cell(LostCell),
    /// Each of the removed row's columns, with the write that set it.
    Row(Vec<LostCell>),
}

pub struct LostCell {
    pub column: String,
    pub value: rusqlite::types::Value,
    /// The write that set the value.
    pub set_by: WriteId,
}

pub enum Replacement {
    Write(WriteId),
    /// Removal rules took the row out: every one that holds (§8.4, §8.5,
    /// §8.6, §14).
    Rules(Vec<RemovalRule>),
    /// A breaking schema change the write hadn't read, named by the
    /// schema version it raised the store to (§17.1).
    SchemaChange { version: u32 },
    /// A reset the write hadn't read (§19.3).
    Reset(EntryId),
}

pub enum RemovalRule {
    ForeignKey { columns: Vec<String>, parent: String, parent_columns: Vec<String> },
    Check { constraint: String },
    /// The row is in a deleted circle (§14.7).
    DeletedCircle,
    /// The same key is present in another audience, whose row is shown
    /// (§14.2).
    OtherAudience,
    Unique { terms: Vec<String>, partial: Option<String> },
}

/// A handle that only reads, opened with `open_read_only`.
impl CovenReadHandle {
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>;
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError>;
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;
    pub async fn is_pinned(&self, files: &[FileRef]) -> Result<bool, FileReadError>;
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError>;
}
```

- A read handle's file reads fill the cache like any other, which changes no
  synced state.

Example:

```rust
let titles: Vec<String> = handle
    .read(|sql| Ok(sql.query("SELECT title FROM notes", [], |row| row.get(0))?))
    .process(|mut titles| {
        titles.sort();
        Ok(titles)
    })
    .await?;

let mut notes = handle.subscribe(|sql| {
    Ok(sql.query(
        "SELECT id, title, audience FROM notes ORDER BY title",
        [],
        |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?)),
    )?)
});
while let Ok(rows) = notes.next().await {
    show_notes(rows);
}

let mut lost = handle.subscribe_lost_values();
while let Ok(values) = lost.next().await {
    offer_to_restore(values);
}
```

### 20.5 Storage and sync

- *Setting up storage* creates the store at a location on a provider, the
  first time it connects, and connects this device to it.
  - At a location that already holds this store, such as after
    `disconnect_storage`, setup reconnects to it; one that holds another
    store, or anything that isn't a coven store, fails with
    `LocationOccupied`.
  - Creating uploads the store's first entry and its key sealed to this
    member; waiting writes then go up through sync like any others.
- Setup commits the storage credentials and keys only once the connection
  is ready; a failed setup leaves the device as it was.
- A device that isn't connected still reads and writes
  ([§3](#3-guarantees)); its writes wait in `coven_uploads`.

```rust
/// The provider holding a store (§4).
pub enum CloudProvider {
    /// S3, including compatible providers.
    S3,
    /// Google Drive.
    GoogleDrive,
    /// Dropbox.
    Dropbox,
    /// OneDrive.
    OneDrive,
    /// iCloud through the app's CloudKit calls.
    CloudKit,
}

/// A store's location, with credentials kept separately (§4, §20.5).
pub enum StorageConfig {
    /// An existing S3 bucket and the prefix reserved for this store.
    S3 {
        /// The bucket's name.
        bucket: String,
        /// The signing region.
        region: String,
        /// A compatible provider's endpoint, or None for AWS's regional endpoint.
        endpoint: Option<Url>,
        /// The store's prefix, without leading or trailing slashes.
        prefix: String,
    },
    /// A store folder in Google Drive.
    GoogleDrive { folder_id: String },
    /// A Dropbox shared folder namespace, independent of each member's mount path.
    Dropbox { namespace_id: String },
    /// A store folder in a OneDrive drive.
    OneDrive { drive_id: String, folder_id: String },
    /// The CloudKit zone reached by the app's bridge.
    CloudKit {
        /// The app's CloudKit container.
        container: String,
        /// The owner's CloudKit record name.
        owner: String,
        /// The custom zone containing the store.
        zone: String,
    },
}

impl StorageConfig {
    /// The provider of this location.
    pub fn provider(&self) -> CloudProvider;
    /// Refuses missing or invalid location information before making a request.
    pub fn validate(&self) -> Result<(), StorageError>;
}

/// Limits for concurrent file transfers (§20.1, §20.5).
pub struct TransferLimits {
    /// Maximum file uploads running at once.
    pub uploads: NonZeroUsize,
    /// Maximum file downloads a pin runs at once.
    pub downloads: NonZeroUsize,
}

/// A storage failure the app can act on, classified by `failure()` (§21.3).
pub enum StorageFailure {
    /// No route to the provider, a timeout, or an interrupted response.
    Network,
    /// Credentials were refused or must be refreshed.
    Authentication,
    /// The account is signed in but cannot perform this operation.
    PermissionDenied,
    /// The requested object is absent.
    NotFound,
    /// A create-once path is occupied.
    AlreadyExists,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 endpoint or signing region is wrong.
    RegionMismatch,
    /// The provider has no space left.
    QuotaExceeded,
    /// The provider requests a later retry.
    RateLimited,
    /// The request's configuration is invalid.
    InvalidConfiguration,
    /// The provider refused the request for another reason.
    Refused,
    /// The response or recorded session is malformed.
    Protocol,
}

/// A storage error that preserves its typed cause (§4, §20.5).
pub enum StorageError {
    /// A classified provider failure with its original cause.
    Provider {
        /// The provider that failed.
        provider: CloudProvider,
        /// The failure the app can act on.
        failure: StorageFailure,
        /// The original transport, SDK or bridge error.
        source: Box<dyn std::error::Error + Send + Sync>,
    },
    /// The location settings are invalid.
    InvalidConfiguration(&'static str),
    /// An object path is outside the store's layout.
    InvalidPath,
    /// A range is empty, reversed or beyond the object's end.
    InvalidRange,
    /// An object does not exist.
    NotFound,
    /// The path already holds an object; creation never replaces it.
    AlreadyExists,
    /// The upload session belongs to another provider or location.
    SessionMismatch,
    /// The provider no longer retains the recorded upload.
    SessionExpired,
    /// A part disagrees with the session's offset, size or alignment.
    InvalidPart,
    /// A response violates the provider's protocol.
    Protocol(&'static str),
    /// Parsing recorded data failed.
    Encoding(Box<dyn std::error::Error + Send + Sync>),
    /// Persisting provider settings failed.
    File(FileError),
    /// Cleanup failed too; both causes are retained.
    Cleanup { operation: Box<StorageError>, cleanup: Box<StorageError> },
}

impl StorageError {
    /// The failure the app can act on.
    pub fn failure(&self) -> StorageFailure;
    /// True only for network interruptions and provider throttling.
    pub fn retryable(&self) -> bool;
}

/// Storage setup failed before committing credentials and keys (§20.5).
pub enum StorageSetupError {
    /// Another store already occupies the location.
    LocationOccupied,
    /// The provider refused or failed setup.
    Storage(StorageError),
    /// Sign-in failed or was cancelled.
    OAuth(OAuthError),
    /// This device does not hold its member's keys.
    MemberKeysMissing,
    /// Keeping credentials or keys failed.
    SecureStorage(KeyError),
    /// Local setup failed, with its cause.
    Internal(Box<dyn std::error::Error + Send + Sync>),
}

/// The setup failure the app presents, classified by `failure()` (§20.5).
pub enum StorageSetupFailure {
    /// Sign-in did not complete or the credentials were rejected.
    Authentication,
    /// The account lacks access.
    PermissionDenied,
    /// The bucket, folder or zone is absent.
    ContainerNotFound,
    /// The S3 bucket is in a different region.
    RegionMismatch,
    /// The provider's quota is exhausted.
    QuotaExceeded,
    /// A provider or sign-in setting is invalid.
    InvalidConfiguration,
    /// Another store already occupies this location.
    LocationOccupied,
    /// Storage cannot be reached.
    Network,
    /// This device lacks its member's keys.
    MemberKeysMissing,
    /// Key or credential custody failed.
    SecureStorage,
    /// A setup step failed internally.
    Internal,
}

/// Opening the member's sealed store key failed (§11, §20.5).
pub enum StoreKeyUnlockError {
    /// No storage is connected.
    NoStorage,
    /// Identity custody has no member keys.
    MemberKeysMissing,
    /// Reading the sealed key failed.
    Storage(StorageError),
    /// The sealed key could not be opened or checked.
    Crypto(CryptoError),
    /// Unlocking or keeping keys in custody failed.
    SecureStorage(KeyError),
}

/// Sync or a store-log change failed (§9, §13, §17, §20.5).
pub enum SyncError {
    /// No storage is connected for a call that requires it.
    NoStorage,
    /// The provider refused or failed the call.
    Storage(StorageError),
    /// The local database failed.
    Database(DbError),
    /// Reading or removing credentials or keys failed.
    SecureStorage(KeyError),
    /// Opening the store key failed.
    Unlock(StoreKeyUnlockError),
    /// Making or checking signed or encrypted bytes failed.
    Crypto(CryptoError),
    /// The member's role does not permit this entry (§9).
    PermissionDenied,
    /// Removing or demoting the member would leave no admin (§9).
    LastAdmin,
    /// The member's provider account holds the store, so they can't be
    /// removed (§4, §13).
    StoreOwner,
    /// The store needs a newer schema or format (§17).
    UpdateRequired,
    /// A restore code could not be decoded (§20.9).
    Code(CodeError),
    /// A credential update's code names another store (§20.9).
    WrongStore { expected: StoreId, actual: StoreId },
    /// A credential update's code names another member (§20.9).
    WrongMember { expected: MemberId, actual: MemberId },
    /// A multi-step operation stopped (§18).
    Operation(Box<OperationError>),
}

/// How far this device has applied one device's writes (§6, §20.5).
pub struct DeviceActivity {
    /// The authoring device.
    pub device: DeviceId,
    /// The last applied write number; zero means none.
    pub applied_through: u64,
}

/// An object that failed a check when read (§19.1).
pub struct DamagedObject {
    /// The object's path in storage.
    pub path: String,
    /// Which check failed, retaining its cause.
    pub failure: ObjectCheckFailure,
}

/// The checks whose failure makes a stored object damaged (§19.1).
pub enum ObjectCheckFailure {
    /// The object would not decrypt or authenticate.
    Decryption(CryptoError),
    /// Its member signature did not verify.
    Signature(CryptoError),
    /// Its bytes could not be parsed.
    Parse(Arc<dyn std::error::Error + Send + Sync>),
}

/// Another device differs at the same applied write positions (§19.1).
pub struct Disagreement {
    /// The device whose fingerprint differs from this device's.
    pub device: DeviceId,
    /// The audience compared.
    pub audience: Audience,
    /// The last applied write of each log included in the comparison.
    pub positions: Vec<WriteId>,
}

/// This member's store log entry that was dropped during replay (§9).
pub struct DroppedEntry {
    /// The dropped entry.
    pub entry: EntryId,
    /// What it would have done.
    pub change: StoreLogChange,
    /// Why the replay dropped it.
    pub reason: DropReason,
}

/// Why a store log entry was dropped (§9).
pub enum DropReason {
    /// A conflicting concurrent entry beat it.
    BeatenBy(EntryId),
    /// At its place in the replay, the member, device or circle it changes
    /// no longer existed.
    TargetGone,
    /// Applying it would have left the store without an admin.
    NoAdminLeft,
    /// Its author's role didn't allow it, in the member list they had read.
    NotAllowed,
    /// A removal's replaced circle keys didn't name exactly the circles the
    /// removed member shared with others, in its author's view (§13).
    WrongCircleKeys,
}

/// What a dropped store log entry would have changed, for its author to see (§9).
pub enum StoreLogChange {
    /// Creates the store and names its first admin.
    CreateStore { store: StoreId, name: String, admin: MemberId },
    /// Adds a member with this role.
    AddMember { member: MemberId, role: MemberRole },
    /// Removes the member and their devices (§13).
    RemoveMember { member: MemberId },
    /// Sets the member's role.
    SetMemberRole { member: MemberId, role: MemberRole },
    /// Adds a device belonging to this member (§10).
    AddDevice { member: MemberId, device: DeviceId },
    /// Removes a device.
    RemoveDevice { device: DeviceId },
    /// Makes a named circle with its creator as a member (§20.12).
    CreateCircle { circle: CircleId, name: String, member: MemberId },
    /// Renames a circle (§20.12).
    RenameCircle { circle: CircleId, name: String },
    /// Deletes a circle (§14.7).
    DeleteCircle { circle: CircleId },
    /// Adds a store member to a circle (§14.3).
    AddCircleMember { circle: CircleId, member: MemberId },
    /// Removes a circle member and replaces its key (§14.6).
    RemoveCircleMember { circle: CircleId, member: MemberId },
    /// Raises the schema version and names its snapshot path (§17.1).
    SchemaChange { version: u32, snapshot: String },
    /// Raises the format version and names its snapshot path (§17.2).
    FormatChange { version: u32, snapshot: String },
    /// Resets the audience to the named snapshot path (§19.3).
    Reset { audience: Audience, snapshot: String },
}

impl CovenHandle {
    /// Sets up storage on S3 with this member's access key (§4).
    pub async fn setup_s3_storage(
        &self,
        storage: StorageConfig,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Sets up storage on Google Drive, Dropbox or OneDrive, running the
    /// provider's sign-in with the builder's OAuth clients. `cancel` stops
    /// the sign-in.
    pub async fn setup_oauth_storage(
        &self,
        storage: StorageConfig,
        cancel: watch::Receiver<bool>,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Sets up storage on iCloud, through the builder's CloudKit calls.
    pub async fn setup_cloudkit_storage(
        &self,
        storage: StorageConfig,
    ) -> Result<ConnectedStorage, StorageSetupError>;

    /// Checks that the storage `storage` describes can be reached and used,
    /// without connecting to it.
    pub async fn probe_storage(&self, storage: &StorageConfig) -> Result<(), SyncError>;

    /// Opens the current store key from its copy sealed to this member in
    /// storage (§11), keeps it in key custody, and connects, without
    /// starting to sync.
    pub async fn unlock_store_key(&self) -> Result<ConnectedStorage, StoreKeyUnlockError>;

    /// Whether key custody holds the store key: `Available` or `Locked`.
    pub fn store_key_state(&self) -> Result<StoreKeyState, KeyError>;

    /// Disconnects and removes this device's storage credentials. If removing
    /// them fails, the connection stays.
    pub async fn disconnect_storage(&self) -> Result<(), SyncError>;

    /// Connects to the configured storage with the credentials and keys this
    /// device holds, and starts syncing.
    pub async fn connect_sync(&self) -> Result<(), SyncError>;

    /// Starts syncing again after `stop_sync`. Does nothing with no storage
    /// connected.
    pub async fn start_sync(&self) -> Result<(), SyncError>;

    /// Stops syncing after the sync in progress, keeping the connection.
    /// Keys the sync had unlocked are dropped from memory, and read again
    /// from custody on the next start.
    pub fn stop_sync(&self);

    /// Stops syncing and drops the connection.
    pub fn disconnect_sync(&self);

    /// Syncs now instead of at the next idle tick. While idle, coven syncs
    /// every 30 seconds, and at once after a local write.
    pub fn sync_now(&self);

    /// The sync status, live. The first value is the current status.
    pub fn subscribe_sync_status(&self) -> watch::Receiver<SyncStatus>;

    /// How many uploads and downloads run at once.
    pub fn transfer_limits(&self) -> TransferLimits;

    /// Changes them while the store is open. Transfers already running keep
    /// the limit they started under.
    pub fn set_transfer_limits(&self, limits: TransferLimits);
}

pub enum SyncStatus {
    /// No storage is connected.
    Disconnected,
    /// Storage is connected and syncing is stopped.
    Stopped,
    /// Storage hasn't been reached since connecting.
    Offline,
    /// A sync is running.
    Syncing,
    /// The last sync finished.
    Synced(SyncReport),
    /// The last sync failed as a whole.
    Failed { error: SyncFailure },
}

pub struct SyncReport {
    pub finished_at: SystemTime,
    /// How far this device has applied each other device's log.
    pub devices: Vec<DeviceActivity>,
    /// Writes this device holds back, the writes they wait for, and since
    /// when (§19.1).
    pub waiting: Vec<WaitingWrite>,
    /// Objects that failed their check when read (§19.1).
    pub damaged_objects: Vec<DamagedObject>,
    /// Devices whose fingerprints differ from this device's at the same
    /// positions (§19.1).
    pub disagreements: Vec<Disagreement>,
    /// Operations that failed for good (§18).
    pub blocked_operations: Vec<BlockedOperation>,
    /// This member's store log entries dropped during replay, with their reasons (§9).
    pub dropped_entries: Vec<DroppedEntry>,
    /// S3 keys an admin must delete in the provider's console, until the
    /// admin confirms each is gone (§13).
    pub access_keys_to_delete: Vec<AccessKeyToDelete>,
    /// The rows the sync's writes changed, as a hint for refreshing views
    /// that aren't live queries. Not a complete list.
    pub row_changes: Option<Vec<RowChange>>,
}

/// An S3 key whose member no longer has access, or whose invite ended (§13).
pub struct AccessKeyToDelete {
    pub access_key_id: String,
    pub member: Option<MemberId>,
}

pub struct WaitingWrite {
    pub write: WriteId,
    pub waiting_for: Vec<WriteId>,
    pub since: SystemTime,
}

/// Storage this device has set up, with whether it holds the store key.
pub struct ConnectedStorage {
    pub storage: StorageConfig,
    pub key_state: StoreKeyState,
}

pub enum StoreKeyState {
    Available,
    Locked,
}

impl StorageSetupError {
    /// The setup failure the app presents, with its cause retained in this error.
    pub fn failure(&self) -> StorageSetupFailure;
}

pub enum SyncFailure {
    /// The store's schema or format version is newer than this app (§17).
    UpdateRequired,
    /// This device, or its member, was removed from the store (§10).
    Removed,
    /// Another store was set up in this location at the same moment, first;
    /// set this one up somewhere else (§4).
    LocationTaken,
    /// Storage refused or failed a request.
    Storage(Arc<StorageError>),
    /// Anything else, with its cause.
    Other(Arc<dyn std::error::Error + Send + Sync>),
}
```

Example:

```rust
match handle.setup_s3_storage(storage, access_key_id, SecretText::new(secret_access_key)).await {
    Ok(connected) => remember(connected.storage),
    Err(error) => return show_setup_failure(error.failure()),
}

let mut status = handle.subscribe_sync_status();
loop {
    match &*status.borrow_and_update() {
        SyncStatus::Synced(report) if !report.waiting.is_empty() => show_waiting(&report.waiting),
        SyncStatus::Failed { error } => show_sync_error(error),
        other => show_status(other),
    }
    if status.changed().await.is_err() {
        break;
    }
}
```

### 20.6 Operations and recovery

- Every unfinished operation is a row in `coven_operations`
  ([§18](#18-operations)).
- A failed step goes to the app call that started its operation while
  that call waits; otherwise it is reported in the sync status.

```rust
/// The local integer primary key of one unfinished operation (§18).
pub struct OperationId(pub i64);

/// The work represented by an unfinished operation (§18.1, §19.3).
pub enum OperationKind {
    /// Remove a member and replace the store key.
    RemoveMember,
    /// Remove a circle member and replace its key.
    RemoveCircleMember,
    /// Migrate the schema, snapshot it and raise the version.
    SchemaChange,
    /// Migrate the format, snapshot it and raise the version.
    FormatChange,
    /// Replace this device's synced data from a snapshot.
    ReloadFromSnapshot,
    /// Write a snapshot and delete covered logs and unused files.
    Snapshot,
    /// Upload a file or keep it on this device.
    ChangeFileLocation,
    /// Grant access, approve or decline a join, and settle the invite.
    Invite,
    /// Upload a file in parts using a recorded provider session.
    MultipartUpload,
    /// Snapshot and reset an audience (§19.3).
    Reset,
}

/// Who initiated an operation (§18).
pub enum StartedBy {
    /// The app call, named as in coven_operations.started_by.
    AppCall(String),
    /// Coven's own running work.
    Coven,
}

/// A multi-step operation stopped with a failure the app can act on (§18).
pub enum OperationError {
    /// Reading or committing the operation's state failed.
    Database(DbError),
    /// A storage step failed.
    Storage(StorageError),
    /// Reading or keeping keys failed.
    SecureStorage(KeyError),
    /// Making or checking encrypted or signed bytes failed.
    Crypto(CryptoError),
    /// Reading a file needed by the operation failed (§16.1).
    File(FileReadError),
    /// A user-provided download destination already exists (§16.1).
    DestinationExists { path: PathBuf },
    /// The member lacks authority for the operation (§9, §14.3, §19.3).
    PermissionDenied,
}

impl CovenHandle {
    /// Runs a failed operation again from the step after its last completed
    /// one. An operation whose cause still stands fails again.
    pub async fn retry_blocked_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Abandons a failed operation and deletes its row. Steps already done
    /// stay done; each kind's steps are ordered so other devices never see a
    /// half-done operation (§18).
    pub async fn discard_blocked_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Reloads this device from the latest snapshot, keeping its waiting
    /// writes, as an operation (§19.2).
    pub async fn reload_from_snapshot(&self) -> Result<(), OperationError>;

    /// Resets the store from this device's copy, as an admin (§19.3): writes
    /// a snapshot, then records the reset in the store log. Every other
    /// device reloads from that snapshot.
    pub async fn reset_store(&self) -> Result<(), SyncError>;
}

/// One `coven_operations` row whose step failed for good.
pub struct BlockedOperation {
    pub id: OperationId,
    /// Such as removing a member, or reloading from a snapshot.
    pub kind: OperationKind,
    pub last_step: u32,
    /// The app call that started it, or coven.
    pub started_by: StartedBy,
    pub failure: String,
}
```

### 20.7 Moves and uploads

- A row moves to another audience by an ordinary write that changes its
  root's audience column, or points a descendant at a parent in another
  audience ([§14.2](#142-moving-rows)).
  - The write commits at once on this device; its moved rows' uploaded
    files stay where they are ([§16.1](#161-kinds-and-where-files-are)).
- Uploading a file, and keeping an uploaded file on one device, change
  where it is ([§16.1](#161-kinds-and-where-files-are)); each is an
  operation ([§18.1](#181-operations)), so the call records it and
  returns, and it finishes whenever storage can be reached.

```rust
/// Live upload-queue results, ending when the store closes (§20.7).
pub struct UploadsLiveQuery { /* private fields */ }

/// A file upload attempt failed (§16.5, §20.7).
pub enum UploadFailure {
    /// Reading or checking the source file failed.
    File(FileReadError),
    /// The provider refused or failed the upload.
    Storage(StorageError),
    /// Encrypting the file failed.
    Crypto(CryptoError),
    /// Unlocking the file's audience key failed.
    SecureStorage(KeyError),
}

/// The failed files and their causes from one upload drain (§20.7).
pub type UploadFailures = Vec<(FileRef, UploadFailure)>;

impl CovenHandle {
    /// Uploads files that are on this device, then marks them uploaded.
    pub async fn upload_files(&self, files: &[FileRef]) -> Result<(), OperationError>;

    /// Downloads uploaded files to this device and marks them as on this
    /// device; the uploaded copies are deleted once unused. `destinations`
    /// maps each user-provided file's id to the path it is written to, which
    /// must not already exist.
    pub async fn keep_files_on_this_device(
        &self,
        files: &[FileRef],
        destinations: &HashMap<String, PathBuf>,
    ) -> Result<(), OperationError>;

    /// A live query over the upload queue: every file waiting to upload, with
    /// its progress. The first result is the current state.
    pub fn subscribe_uploads(&self) -> UploadsLiveQuery;

    /// Retries every waiting upload now, instead of after its retry delay,
    /// which starts at 1 second and doubles to at most 5 minutes.
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError>;

    /// Pauses uploads, or resumes them. A paused upload keeps its place,
    /// including a provider upload session in progress.
    pub fn set_uploads_paused(&self, paused: bool);
}

impl UploadsLiveQuery {
    /// The current state at once, then the next state each time it changes.
    pub async fn next(&mut self) -> Result<UploadQueue, DbError>;
}

pub struct UploadQueue {
    pub paused: bool,
    /// Oldest first.
    pub files: Vec<QueuedUpload>,
}

pub struct QueuedUpload {
    pub file: FileRef,
    pub phase: UploadPhase,
    /// Failed attempts so far.
    pub attempts: u64,
    pub last_failure: Option<UploadFailure>,
    pub queued_at: SystemTime,
    pub last_attempt_at: Option<SystemTime>,
}

pub enum UploadPhase {
    /// Not started, or waiting for its retry delay.
    Waiting,
    /// Reading and encrypting the file: bytes read of its size.
    Preparing { bytes_read: u64, bytes_total: u64 },
    /// Sending it: encrypted bytes the provider has received of the total.
    Uploading { bytes_sent: u64, bytes_total: u64 },
    /// Stored; the write that refers to it can upload (§16.5).
    Stored,
}

pub enum DrainOutcome {
    Drained { uploaded: usize, failures: UploadFailures },
    QueueEmpty,
    AllInBackoff,
    Paused,
}
```

Example:

```rust
// Upload a note's attachment, which is on this device.
let attachment = handle.file_ref("attachments", attachment_id.as_str()).await?;
handle.upload_files(&[attachment]).await?;

let mut uploads = handle.subscribe_uploads();
loop {
    let state = uploads.next().await?;
    for upload in &state.files {
        if let UploadPhase::Uploading { bytes_sent, bytes_total } = upload.phase {
            show_progress(upload.file.key(), bytes_sent, bytes_total);
        }
    }
    if state.files.is_empty() {
        break; // every queued file is stored
    }
}
```

### 20.8 Files and the cache

- A *file reference*, `FileRef`, names one row's file as of that row's
  current version, so a later change to the row can't redirect a read.
- Coven reads a file from wherever it is: the user's original, coven's own
  copy, the cache, or storage ([§16](#16-files)).

```rust
/// Progress while keeping files whole on this device (§16.4, §20.8).
pub struct PinProgress {
    /// Files whose bytes have all been kept.
    pub files_completed: u64,
    /// Files requested, including those already present.
    pub files_total: u64,
    /// Bytes downloaded by this call so far.
    pub bytes_downloaded: u64,
    /// Bytes this call needs to download, excluding bytes already cached.
    pub bytes_total: u64,
}

/// A live query of whether each requested row's file is pinned (§20.8).
pub struct RowsPinnedLiveQuery { /* private fields */ }

impl RowsPinnedLiveQuery {
    /// Replaces the table and keys whose pin state is watched.
    pub fn set_rows(&self, table: &str, keys: Vec<RowKey>) -> Result<(), LiveQueryClosed>;
    /// The current answers, in key order, then each change (§20.8).
    pub async fn next(&mut self) -> Result<Vec<Option<bool>>, FileReadError>;
}

/// Downloads of files declared CacheEager (§16.4, §20.8).
pub enum EagerCacheFillStatus {
    /// No files are waiting to download.
    Idle,
    /// Files are being fetched into the cache.
    Downloading(PinProgress),
    /// The app stopped these downloads.
    Cancelled(PinProgress),
    /// A download failed, with the progress reached before it failed.
    Failed { progress: PinProgress, error: Arc<FileReadError> },
}

impl CovenHandle {
    /// The file a row carries, as of the row's current version.
    pub async fn file_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<FileRef, DbError>;

    /// Reads a whole file, checking it against its row.
    pub async fn read_file(&self, file: &FileRef) -> Result<Vec<u8>, FileReadError>;

    /// Opens a file for reading ranges (§16.3). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    pub async fn open_file_stream(&self, file: &FileRef) -> Result<FileStream, FileReadError>;

    /// Makes sure a file's bytes are on this device: an uploaded file is
    /// downloaded into the cache, and one kept on this device is checked.
    pub async fn ensure_file_on_device(&self, file: &FileRef) -> Result<(), FileReadError>;

    /// The path, size and modification time coven recorded for a row's
    /// user-provided file, or `None` when the row has none.
    pub async fn user_file(&self, table: &str, key: impl Into<RowKey>) -> Result<Option<UserFile>, DbError>;

    /// Keeps uploaded files whole on this device regardless of the cache
    /// budget, downloading what is missing. `on_progress` is called before
    /// the first download, as bytes arrive, and as each file is kept.
    pub async fn pin(
        &self,
        files: &[FileRef],
        on_progress: &(dyn Fn(PinProgress) + Send + Sync),
    ) -> Result<(), FileReadError>;

    /// Stops keeping files; they stay in the cache until the budget evicts them.
    pub async fn unpin(&self, files: &[FileRef]) -> Result<(), FileReadError>;

    /// Whether every file in `files` is pinned. An empty set is pinned.
    pub async fn is_pinned(&self, files: &[FileRef]) -> Result<bool, FileReadError>;

    /// Whether each row's file is pinned, one answer per key in order, or
    /// `None` for a key with no row carrying a file. A file not yet uploaded
    /// reads as not pinned.
    pub async fn rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> Result<Vec<Option<bool>>, FileReadError>;

    /// The same answers, live. `set_rows` changes which rows it watches.
    pub fn subscribe_rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> RowsPinnedLiveQuery;

    /// Removes an uploaded file's copies from the cache, pinned or not. Never
    /// touches a file kept on this device, or storage; a later read
    /// downloads it again.
    pub async fn evict_file(&self, file: &FileRef) -> Result<(), FileReadError>;

    /// The cache budget for one namespace, in bytes; each namespace evicts
    /// on its own. A namespace with no budget set evicts nothing.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError>;
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError>;

    /// Progress of downloading every file declared to download as soon as
    /// its row arrives that this device doesn't have yet, such as after
    /// loading a snapshot to join or recover (§16, §19.2).
    pub fn subscribe_eager_cache_fill_status(&self) -> watch::Receiver<EagerCacheFillStatus>;

    /// Stops those downloads without stopping sync.
    pub fn cancel_eager_cache_fill(&self);
}

impl FileRef {
    pub fn table(&self) -> &str;
    pub fn key(&self) -> &RowKey;
    /// The column naming the file.
    pub fn column(&self) -> &str;
    /// The file's size in bytes.
    pub fn plaintext_size(&self) -> u64;
    /// The row's audience, whose key encrypts the file once uploaded (§16.1).
    pub fn audience(&self) -> Audience;
    /// Where the file is (§16.1).
    pub fn location(&self) -> FileLocation;
}

pub enum FileLocation {
    Uploaded,
    /// Only on the named device.
    OnDevice(DeviceId),
}

impl FileStream {
    /// The file's whole size in bytes.
    pub fn plaintext_size(&self) -> u64;

    /// Reads `len` bytes at `offset`. A range past the end is an error, never
    /// a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, FileReadError>;
}

pub enum FileReadError {
    /// The range needs chunks that aren't cached, and storage can't be reached.
    Offline { id: String },
    /// An uploaded file was read with no storage connected.
    NoStorage,
    /// The file is only on another device, which the app can name.
    OnOtherDevice { id: String, device: DeviceId },
    /// A user-provided file is gone from its recorded path.
    UserFileMissing { id: String, path: PathBuf },
    /// A user-provided file's size or modification time no longer matches
    /// what coven recorded.
    UserFileChanged { id: String, path: PathBuf },
    /// A chunk, or a copy on this device, failed its check.
    Integrity { id: String },
    /// The range lies outside the file.
    RangeOutOfBounds { id: String, offset: u64, end: u64, size: u64 },
    /// Storage refused or failed the request.
    Storage(StorageError),
    /// The database failed, with its cause.
    Database(DbError),
    /// The disk failed, with its cause.
    Disk(DiskError),
}
```

Example:

```rust
let recording = handle.file_ref("attachments", attachment_id.as_str()).await?;
let stream = handle.open_file_stream(&recording).await?;

// A voice memo: read its header, then seek to where listening resumes.
let header = stream.read_at(0, 64 * 1024).await?;
let resume_at = position_for(&header, saved_seconds);
match stream.read_at(resume_at, 256 * 1024).await {
    Ok(bytes) => play(bytes),
    Err(FileReadError::Offline { .. }) => show_not_downloaded(),
    Err(error) => return Err(error.into()),
}
```

### 20.9 Members and devices

- Only admins add and remove members, and change roles; each member removes
  their own devices, and admins any device ([§9](#9-members-and-roles)).
- Removing a member is an operation ([§18.1](#181-operations)).

```rust
impl CovenHandle {
    /// The members and their devices, as this device's store log has them.
    pub async fn get_members(&self) -> Result<Vec<MemberInfo>, SyncError>;

    /// On S3: switches this member to the access key they made in the
    /// provider's console, and returns their new restore code, for their
    /// other devices to scan and for them to write down. They delete the old
    /// key in the console (§13).
    pub async fn replace_access_key(
        &self,
        access_key_id: String,
        secret_access_key: SecretText,
    ) -> Result<String, SyncError>;

    /// On a device that already has the store open: takes the storage
    /// credentials from a new restore code of this member's, and keeps
    /// everything else.
    pub async fn update_credentials(&self, code: &str) -> Result<(), SyncError>;

    /// Changes a member's role, as an admin (§9).
    pub async fn set_member_role(&self, member: &MemberId, role: MemberRole) -> Result<(), SyncError>;

    /// Records that the admin deleted an S3 key in the provider's console,
    /// so the sync status stops asking for it (§13).
    pub async fn confirm_access_key_deleted(&self, access_key_id: &str) -> Result<(), SyncError>;

    /// Removes a member and all their devices, rotates the store key, and
    /// revokes their storage access; on S3 the result says to delete their
    /// key in the provider's console (§13).
    pub async fn remove_member(&self, member: &MemberId) -> Result<MemberRemoval, SyncError>;

    /// Removes a device. The provider can't cut off one device alone, so the
    /// result says how its member signs out of the provider and signs in
    /// again on the devices they keep (§13).
    pub async fn remove_device(&self, device: DeviceId) -> Result<ProviderSignOut, SyncError>;
}

/// The member's provider account or the public id of their S3 key (§4).
pub enum MemberAccess {
    /// The member's provider account email.
    ProviderAccount(String),
    /// An S3 key the admin made in the provider's console.
    S3AccessKey { access_key_id: String },
}

/// Revoked sharing or the instruction to delete an S3 key (§13).
pub enum MemberRemoval {
    /// The provider no longer shares with the account.
    Revoked,
    /// The admin deletes the key with this public id in the provider's console.
    DeleteAccessKey { access_key_id: String },
}

pub struct MemberInfo {
    pub id: MemberId,
    pub role: MemberRole,
    pub devices: Vec<DeviceId>,
    /// Whether this is the member using this device.
    pub is_self: bool,
}

pub enum MemberRole {
    Admin,
    Member,
}

/// How a removed device's member cuts off its provider access (§13).
pub enum ProviderSignOut {
    /// Remove the app's access from Google Drive, Dropbox or OneDrive, then sign in again.
    RemoveAppAccess { provider: CloudProvider },
    /// Remove the device from the Apple account.
    RemoveFromAppleAccount,
    /// Enter a new S3 key, update retained devices and the written restore code, then delete the old key.
    ReplaceAccessKey,
}
```

### 20.10 Joining and restore

- A person's new device opens the store from their restore code: scanned
  as a QR code, typed in, or read from iCloud Keychain
  ([§12.1](#121-a-persons-new-device)).
- An admin adds a person with an invite, and approves their join request
  ([§12.2](#122-adding-a-person)).
- Each call that opens the store on a new device makes the store
  directory, puts the keys in custody, loads the store, and returns the
  store's directory, which the app then opens.
- Each takes the same tables, migrations and custody choices as the
  builder, and `cancel`, which stops it.

```rust
/// A one-time invite's UUID, also naming its join-request object (§12.2).
pub struct InviteId(pub Uuid);

/// An uploaded file's random id, its name in storage (§16.2).
pub struct FileId(pub Uuid);

/// What the app may show before using a scanned or typed code (§20.10).
pub struct CodeInfo {
    /// Whether this is the person's restore code or an invite.
    pub kind: CodeKind,
    /// The store named by the code.
    pub store_id: StoreId,
    /// The store's name.
    pub store_name: String,
    /// The provider holding the store.
    pub cloud_provider: CloudProvider,
    /// Whether this provider requires sign-in before using the code.
    pub needs_oauth: bool,
}

/// The two codes used to open a store on a new device (§12).
pub enum CodeKind {
    /// Holds the person's member keys and storage credentials.
    Restore,
    /// Holds an invite id and secret; approval is still required.
    Invite,
}

/// A scanned or typed code cannot be used for this call (§12, §20.10).
pub enum CodeError {
    /// The code does not contain valid restore or invite data.
    Invalid,
    /// The call needs the other kind of code.
    WrongKind { expected: CodeKind, actual: CodeKind },
}

/// Opening a store on a new device failed (§12, §20.10).
pub enum BootstrapError {
    /// The person cancelled the call.
    Cancelled,
    /// The code cannot be used.
    Code(CodeError),
    /// Creating the local store directory failed.
    CreateStore(StoreCreationError),
    /// Loading or opening the store failed.
    Store(CovenError),
    /// Provider sign-in failed.
    OAuth(OAuthError),
    /// Reading or keeping credentials or identity keys failed.
    SecureStorage(KeyError),
    /// Opening the member's sealed store key failed.
    Unlock(StoreKeyUnlockError),
}

/// Provider sign-in tokens, held as secrets rather than printed (§20.10).
pub struct OAuthTokens {
    /// The token authorizing provider requests.
    pub access_token: SecretText,
    /// A renewal token, if the provider supplied one.
    pub refresh_token: Option<SecretText>,
    /// Expiry calculated with the injected clock, or None for a non-expiring token.
    pub expires_at: Option<SystemTime>,
}

/// A browser request plus private state retained to check its redirect (§20.10).
pub struct AuthorizeRequest {
    /// The URL the app opens for sign-in.
    pub auth_url: String,
    /* private fields */
}

/// Provider sign-in could not finish (§20.5, §20.10).
pub enum OAuthError {
    /// This provider is not an OAuth provider or the app supplied no client id.
    Unavailable(CloudProvider),
    /// The request's provider, redirect or client id does not match the exchange.
    RequestMismatch,
    /// The redirect state is missing or different.
    StateMismatch,
    /// The provider declined sign-in.
    Denied,
    /// The callback omitted its authorization code.
    MissingCode,
    /// The person cancelled sign-in.
    Cancelled,
    /// No callback arrived before the deadline.
    Timeout,
    /// The current tokens have expired; refresh and commit them before reuse.
    Expired,
    /// A new provider sign-in is required.
    Reauthorize,
    /// The redirect URI or callback request is malformed.
    InvalidRedirect,
    /// The provider's expiry cannot be represented.
    InvalidExpiry,
    /// The browser or local redirect listener failed.
    Io(std::io::Error),
    /// The provider refused sign-in or token exchange, or could not be reached.
    Storage(StorageError),
}

impl CovenHandle {
    /// This member's restore code: their member key, the store's id and
    /// name, and storage credentials. The app shows it as a QR code, blurred
    /// until the person taps it, and asks them to write it down at setup.
    pub async fn restore_code(&self) -> Result<String, SyncError>;

    /// Starts adding a person, as an admin, and returns the invite to show as
    /// a QR code. Expires after a day.
    pub async fn create_invite(&self, role: MemberRole, access: InviteAccess) -> Result<Invite, SyncError>;

    /// Join requests waiting for approval, live. The first value is the
    /// current list.
    pub fn subscribe_join_requests(&self) -> watch::Receiver<Vec<JoinRequest>>;

    /// Adds the person who sent `request` as a member with the invite's role,
    /// and seals the store key to them.
    pub async fn approve_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Declines the request, and takes back the storage access its invite
    /// granted; on S3 the admin deletes the key in the provider's console.
    pub async fn decline_join_request(&self, request: &JoinRequest) -> Result<(), SyncError>;

    /// Cancels an invite before anyone joins with it, taking back the
    /// storage access it granted; on S3 the admin deletes the key in the
    /// provider's console.
    pub async fn cancel_invite(&self, invite: &InviteId) -> Result<(), SyncError>;
}

/// How the new person reaches storage (§12.2).
pub enum InviteAccess {
    /// Google Drive, Dropbox, OneDrive or iCloud: the store is shared with
    /// this account.
    ProviderAccount { email: String },
    /// S3: an access key the admin made for them in the provider's console.
    S3AccessKey { access_key_id: String, secret_access_key: SecretText },
}

pub struct Invite {
    pub id: InviteId,
    /// The code to show as a QR code.
    pub code: String,
    pub role: MemberRole,
    pub expires_at: SystemTime,
}

pub struct JoinRequest {
    pub invite: InviteId,
    /// The new member's public key.
    pub member: MemberId,
    /// The name the new device gave itself, for the admin to check.
    pub device_name: String,
    pub provider_account_email: Option<String>,
}

/// What a restore code or invite is for, before using it: the store's id and
/// name, its provider, and whether the provider needs a sign-in first.
pub fn decode_code_info(code: &str) -> Result<CodeInfo, CodeError>;

/// Opens the store on a new device from the person's restore code, scanned
/// or typed. `oauth_tokens` is the provider sign-in, when `needs_oauth`.
pub async fn restore_from_code(
    code: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    /* transfer limits, OAuth clients, CloudKit calls, clock, id source */
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<StoreDir, BootstrapError>;

/// Opens the store on a new Apple device from the iCloud Keychain item,
/// which holds what a restore code holds (§12.1). `None` when the keychain
/// holds no store.
pub async fn restore_from_keychain(
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    /* transfer limits, OAuth clients, CloudKit calls, clock, id source */
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<StoreDir>, BootstrapError>;

/// On the new person's device: makes their member keys, writes a join
/// request to storage, and waits for the admin to approve it, then loads
/// the store. Picks up where it left off after a restart. Returns `None`
/// when the request is declined or the invite expires.
pub async fn join_with_invite(
    code: &str,
    device_name: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    /* transfer limits, OAuth clients, CloudKit calls, clock, id source */
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Option<StoreDir>, BootstrapError>;

impl OAuthClients {
    /// Sets client ids (None for providers the app does not offer) and the sign-in clock.
    pub fn new(
        google_drive_client_id: Option<String>,
        dropbox_client_id: Option<String>,
        onedrive_client_id: Option<String>,
        clock: ClockRef,
    ) -> Self;

    /// Runs browser sign-in with a local redirect; cancellation or dropping it closes the listener.
    pub async fn authorize(
        &self,
        provider: CloudProvider,
        cancel: watch::Receiver<bool>,
    ) -> Result<OAuthTokens, OAuthError>;

    /// Builds the URL and private proof for an app that handles its own redirect.
    pub fn build_authorize_request(&self, provider: CloudProvider, redirect_uri: &str) -> Result<AuthorizeRequest, OAuthError>;
    /// Checks the redirect's state and exchanges its code using this client's clock.
    pub async fn exchange_code(
        &self,
        provider: CloudProvider,
        code: &str,
        callback_state: Option<&str>,
        request: &AuthorizeRequest,
        redirect_uri: &str,
    ) -> Result<OAuthTokens, OAuthError>;

    /// Gets replacement tokens, retaining the old refresh token when the provider omits it.
    pub async fn refresh(&self, provider: CloudProvider, tokens: &OAuthTokens) -> Result<OAuthTokens, OAuthError>;
}
```

- Refreshed tokens are committed to key custody before the provider session
  uses them.

Example, when the app handles the sign-in redirect:

```rust
let request = oauth_clients.build_authorize_request(provider, redirect_uri)?;
open_sign_in(&request.auth_url);
let (code, callback_state) = receive_sign_in_redirect().await?;
let tokens = oauth_clients
    .exchange_code(provider, &code, callback_state.as_deref(), &request, redirect_uri)
    .await?;
```

Example, adding Ana's laptop. On her phone:

```rust
let code = handle.restore_code().await?;
show_blurred_qr_code(&code);
```

On the laptop:

```rust
let info = decode_code_info(&scanned)?;
let tokens = if info.needs_oauth {
    Some(oauth_clients.authorize(info.cloud_provider, cancel_rx.clone()).await?)
} else {
    None
};
let store_dir = restore_from_code(
    &scanned,
    &tables(),
    &migrations(),
    CovenMigrationPolicy::ApplyPending,
    KeyCustody::Keyring,
    IdentityCustody::Keyring,
    tokens,
    &layout,
    transfer_limits,
    oauth_clients.clone(),
    cloudkit_ops.clone(),
    clock.clone(),
    ids.clone(),
    |step| show_step(step),
    &cancel_rx,
)
.await?;
let handle = Coven::builder(store_dir)
    .synced_tables(tables())
    .migrations(migrations())
    .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
    .open()
    .await?;
```

Example, Ana adding Carol. On Ana's phone:

```rust
let invite = handle
    .create_invite(
        MemberRole::Member,
        InviteAccess::ProviderAccount { email: "carol@example.com".into() },
    )
    .await?;
show_qr_code(&invite.code);

let mut requests = handle.subscribe_join_requests();
loop {
    for request in requests.borrow_and_update().clone() {
        if request.invite == invite.id {
            if confirm(&request.device_name).await {
                handle.approve_join_request(&request).await?;
            } else {
                handle.decline_join_request(&request).await?;
            }
        }
    }
    requests.changed().await?;
}
```

On Carol's phone:

```rust
let tokens = oauth_clients.authorize(CloudProvider::GoogleDrive, cancel_rx.clone()).await?;
match join_with_invite(
    &scanned,
    "Carol's phone",
    &tables(),
    &migrations(),
    CovenMigrationPolicy::ApplyPending,
    KeyCustody::Keyring,
    IdentityCustody::Keyring,
    Some(tokens),
    &layout,
    transfer_limits,
    oauth_clients.clone(),
    cloudkit_ops.clone(),
    clock.clone(),
    ids.clone(),
    |step| show_step(step),
    &cancel_rx,
)
.await?
{
    Some(store_dir) => open_store(store_dir),
    None => show_declined(),
}
```

### 20.11 Keys and secrets

```rust
/// Initializing this device's member identity failed (§20.11).
pub enum IdentityError {
    /// Custody already holds member keys.
    AlreadyInitialized,
    /// Making the two key pairs failed.
    Crypto(CryptoError),
    /// Reading or persisting identity custody failed.
    Custody(KeyError),
}

impl MemberKeys {
    /// Encodes both private seeds for custody or a restore code (§12.1).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Restores both pairs from custody or a restore code.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl StoreKeyring {
    /// Encodes every opened store and circle key for custody (§11).
    pub fn to_secret_bytes(&self) -> SecretBytes;
    /// Reads custody bytes, rejecting malformed, duplicate or empty keyrings.
    pub fn from_secret_bytes(bytes: &[u8]) -> Result<Self, MaterialError>;
}

impl CovenHandle {
    /// Makes this member's two key pairs and puts them in identity custody,
    /// for the person creating a store. Fails if custody already holds keys.
    /// Joining and restoring put the keys there themselves.
    pub fn initialize_identity(&self) -> Result<MemberId, IdentityError>;

    /// Removes the store keys from key custody and drops any connection that
    /// holds them unlocked. If custody can't remove them, the connection
    /// stays.
    pub async fn forget_store_keys(&self) -> Result<(), SyncError>;

    /// Keeps an app secret, such as an API token, in the same keychain and
    /// under the same access policy as coven's keys. Names can't be empty,
    /// contain `:`, or match one of coven's own entries.
    pub fn set_host_secret(&self, name: &str, value: &str) -> Result<(), KeyError>;

    /// The secret, or `None` if it was never set.
    pub fn host_secret(&self, name: &str) -> Result<Option<String>, KeyError>;

    /// Deletes the secret; succeeds if it was never set.
    pub fn delete_host_secret(&self, name: &str) -> Result<(), KeyError>;

    /// Encrypts the app's own data with the current store key, for the app to
    /// keep in its rows, since the local database is not encrypted. `aad`
    /// binds it to its place, such as the row's key.
    pub fn seal_app_data(&self, plaintext: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError>;

    /// Decrypts what `seal_app_data` made, with the store key it names, so it
    /// still opens after the key is replaced. Fails with a different `aad`.
    pub fn open_app_data(&self, sealed: &[u8], aad: &[u8]) -> Result<Vec<u8>, SealError>;
}

/// The app's own store for the store keys and circle keys this device holds.
pub trait StoreKeyCustody: Send + Sync {
    /// The keys, or `None` when this device has never held any.
    fn unlock(&self) -> Result<Option<StoreKeyring>, KeyError>;
    /// Keeps `keyring`, replacing what was kept.
    fn persist(&self, keyring: &StoreKeyring) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
}

/// The app's own store for this member's keys.
pub trait MemberKeyCustody: Send + Sync {
    fn unlock(&self) -> Result<Option<MemberKeys>, KeyError>;
    fn persist(&self, keys: &MemberKeys) -> Result<(), KeyError>;
    fn forget(&self) -> Result<(), KeyError>;
}
```

### 20.12 Circles

- A circle's members add and remove its members ([§14.3](#143-circles)).
- Removing someone from a circle is an operation that replaces the circle's
  key ([§18.1](#181-operations)); its failure is retried or abandoned with
  the calls of [§20.6](#206-operations-and-recovery).

```rust
/// Circle calls borrowing the open store that owns their work (§14, §20.12).
pub struct Circles<'a> { /* private fields */ }

/// A circle call failed (§14, §20.12).
pub enum CircleError {
    /// The caller is not a member of the circle (§14.3).
    NotMember(CircleId),
    /// The circle has been deleted (§14.7).
    Deleted(CircleId),
    /// Only a member of the store may be added (§20.12).
    NotStoreMember(MemberId),
    /// Sync, keys or a multi-step operation failed.
    Sync(SyncError),
}

impl CovenHandle {
    pub fn circles(&self) -> Circles<'_>;
}

impl Circles<'_> {
    /// Makes a circle with this member in it.
    pub async fn create(&self, name: &str) -> Result<CircleId, CircleError>;

    /// Renames a circle. Its key, members and rows don't change.
    pub async fn rename(&self, circle: CircleId, name: &str) -> Result<(), CircleError>;

    /// Deletes a circle: deletes each of its rows this device has, and
    /// removes the circle in the store log (§14.7).
    pub async fn delete(&self, circle: CircleId) -> Result<(), CircleError>;

    /// Adds a store member to the circle. They get its current and earlier
    /// keys, so they read its history.
    pub async fn add_member(&self, circle: CircleId, member: &MemberId) -> Result<(), CircleError>;

    /// Removes someone from the circle and replaces its key (§14.6).
    pub async fn remove_member(&self, circle: CircleId, member: &MemberId) -> Result<(), CircleError>;

    /// The circles this member is in.
    pub async fn list(&self) -> Result<Vec<Circle>, CircleError>;

    /// A circle's members who are still in the store.
    pub async fn members(&self, circle: CircleId) -> Result<Vec<CircleMemberInfo>, CircleError>;

    /// Resets a circle's rows from this device's copy (§19.3).
    pub async fn reset(&self, circle: CircleId) -> Result<(), CircleError>;
}

pub struct Circle {
    pub id: CircleId,
    pub name: String,
}

pub struct CircleMemberInfo {
    pub member: MemberId,
    pub is_self: bool,
}
```

### 20.13 Migrations

- The app's schema is a list of migrations numbered from 1 with no gaps.
- Opening runs every migration above the database's version in one
  transaction, so a failure leaves the schema as it was.
- A migration has two parts ([§17.1](#171-host-application)): the first
  changes the database, and the optional second changes writes made in the
  older version that still wait in `coven_uploads`.
- Coven decides whether a migration is an addition or a breaking change by
  comparing the schema before and after it.
- A converted change keeps the references its columns carried, renamed
  with them. Changing a reference's value, or adding a column that is a
  reference, fails the migration: the device can't know which generation
  of the parent the old write meant.

```rust
impl Migration {
    /// A migration whose first part is SQL.
    pub fn sql(version: u32, name: &'static str, sql: &'static str) -> Self;

    /// A migration whose first part is code, for rebuilds and backfills SQL
    /// alone can't express.
    pub fn run<F>(version: u32, name: &'static str, f: F) -> Self
    where
        F: Fn(&MigrationContext<'_>) -> Result<(), DbError> + Send + Sync + 'static;

    /// Its second part: changes each row change of a waiting write to fit the
    /// new schema. Without it, a breaking change's waiting writes upload
    /// marked lost.
    pub fn writes<F>(self, f: F) -> Self
    where
        F: Fn(&mut RowChange) -> Result<(), DbError> + Send + Sync + 'static;
}

/// SQL access inside the transaction applying a migration (§17.1, §20.13).
pub struct MigrationContext<'connection> { /* private fields */ }

/// What one row change does (§5).
pub enum ChangeOp {
    /// Inserts the row.
    Insert,
    /// Changes columns of the row.
    Update,
    /// Deletes the row.
    Delete,
}

impl MigrationContext<'_> {
    pub fn execute<P: Params>(&self, sql: &str, params: P) -> rusqlite::Result<usize>;
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()>;
    pub fn query_row<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<T>;
    pub fn query<T, P, F>(&self, sql: &str, params: P, map: F) -> rusqlite::Result<Vec<T>>;
}

/// One row change of a write record (§5).
pub struct RowChange {
    pub table: String,
    pub key: RowKey,
    pub op: ChangeOp,
    pub columns: Vec<ColumnChange>,
}

impl RowChange {
    /// Renames one column of the change.
    pub fn rename_column(&mut self, from: &str, to: &str);
}

/// One column of a row change. A column that holds a reference also keeps,
/// privately, which generation of its parent the write named (§8.4).
pub struct ColumnChange {
    pub name: String,
    pub old: Option<rusqlite::types::Value>,
    pub new: Option<rusqlite::types::Value>,
    /* private: the references the new value carries */
}

impl ColumnChange {
    /// A column the conversion adds; it carries no references.
    pub fn new(name: impl Into<String>, old: Option<rusqlite::types::Value>, new: Option<rusqlite::types::Value>) -> Self;
}

pub enum MigrationError {
    /// The versions aren't 1 to N in order.
    NotContiguous { position: usize, found: u32, expected: u32 },
    /// The database's schema is newer than this app; the app must update.
    SchemaTooNew { current: u32, supported: u32 },
    /// A migration failed, and the transaction rolled back.
    Failed { version: u32, name: &'static str, source: Box<DbError> },
}
```

Example:

```rust
fn migrations() -> Vec<Migration> {
    vec![
        Migration::sql(1, "initial", include_str!("migrations/001_initial.sql")),
        Migration::sql(2, "rename_attachment_title", "ALTER TABLE attachments RENAME COLUMN title TO name")
            .writes(|change| {
                if change.table == "attachments" {
                    change.rename_column("title", "name");
                }
                Ok(())
            }),
    ]
}
```

## 21. Crates and conventions

### 21.1 Crates

- Coven is a Cargo workspace of eight crates, each owning one part of
  this spec:

| Crate | Owns | Spec |
| --- | --- | --- |
| `coven-foundation` | The clock, the id source, atomic file writes, the store's directory and its lock | [§7.2](#72-timestamps), [§10](#10-device-identity), [§20.1](#201-opening) |
| `coven-crypto` | Ciphers, sealed boxes, derived keys, file naming, member keys and their custody | [§11.1](#111-cryptography) |
| `coven-merge` | Timestamps, the merged state, the removal rules and lost values, as functions with no I/O | [§7](#7-order), [§8](#8-merge), [§14](#14-audiences) |
| `coven-format` | The bytes in storage: write records, store log entries, snapshots, file headers and chunks, encoded, decoded and checked, using merge's and crypto's types | [Appendix D](coven-format.md) |
| `coven-database` | The SQLite connection, coven's internal tables, applying the merge's results, triggers, live queries, migrations | [§5](#5-local-database) |
| `coven-storage` | Each provider, and the operations coven needs from it, including upload sessions | [§4](#4-storage-providers-and-access) |
| `coven-sync` | Device logs, the store log, members, circles, snapshots, files and the cache, operations, recovery | [§6](#6-syncing-writes), [§9](#9-members-and-roles), [§12](#12-joining-and-restore) to [§19](#19-recovery) |
| `coven` | The API, and nothing else | [§20](#20-api) |

- Each crate depends only on crates above it in the table, except that
  `coven-database` and `coven-storage` never depend on each other.
- So the database never reaches storage, and storage never reads the
  database; `coven-sync` is where the two meet.
- `coven-merge` and `coven-format` read no clock, file, database or
  network.
- So the merge is tested, and checked against the Lean model of
  [Appendix B](coven-merge-proof.md), without SQLite or storage.
- Ids shared by several crates (store, device and circle ids) live in
  `coven-foundation`; each concept has one type.
- Each external dependency's version is set once, in the workspace, and
  crates name only the features they need.

### 21.2 Capabilities, owners and lifetimes

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
    OAuth clients are all set on the builder ([§20.1](#201-opening)), so
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
- An owner never builds another owner; it is given its collaborators.
- E.g. the sync owner takes the database owner and the storage owner as
  arguments, and doesn't open either itself.
- Owners are built only at *composition roots*, listed in one policy
  file:
  - the builder's `open`;
  - the calls that open a store on a new device: `restore_from_code`,
    `restore_from_keychain` and `join_with_invite`;
  - the test fixtures that build the same graph.
- Each long-lived task has one *lifetime authority*, the only owner that
  may start it, and that stops it when it is dropped.
- E.g. only the sync owner starts the sync loop, so closing the store stops
  it, and nothing else can leave one running.
- An owner never hands out what it holds, by returning it or by a public
  field; callers ask it to do the work.
- E.g. nothing outside coven-database gets the SQLite connection; it asks
  the database owner to run a write.
- A struct built only to be taken apart again, with every field public
  and no methods, is not used to pass collaborators; they are passed by
  name.

### 21.3 Code conventions

- Visibility:
  - nothing is `pub` that can't be reached from outside its crate;
  - `pub(in path)` and `super::super::` are not used: an item needed
    elsewhere moves to where both callers can see it;
  - `coven` re-exports the API at its root and keeps every module
    private.
- Errors are typed enums per crate; an error is never turned into text
  to be passed on, and nothing returns `Result<_, String>`.
- Every failure reaches the app as an error it can tell apart and act on,
  such as a damaged database, a file on another device, or storage that
  can't be reached; none is dropped, and a retry shows in the upload queue
  or the sync status.
- A source file holds at most 1,000 lines, and its tests live beside it
  in `<name>_tests.rs`.
- Each crate offers a `test-utils` feature with its fakes, such as an
  in-memory provider and a fixed clock; tests build the same object graph
  production does.

### 21.4 Checks

- One script runs every check, and CI runs that same script on every
  platform:
  - formatting, and clippy with warnings denied;
  - the dependency rules of [§21.1](#211-crates);
  - the rules of [§21.2](#212-capabilities-owners-and-lifetimes), by a
    checker that reads the syntax tree of every crate, including inside
    macro calls, against the policy file;
  - the checker's own guard tests: every file, owner and composition
    root the policy file names must exist, so the policy can't go stale;
  - the visibility and file-size rules of
    [§21.3](#213-code-conventions);
  - `cargo doc` with broken links denied;
  - every crate built without test code, so an item only tests use shows
    up as dead;
  - the tests, with all features and with none;
  - the Lean proof, built from scratch, with no `sorry` and no axiom
    beyond Lean's own.
- The checker is the first thing built, before any crate, so every rule
  holds from the first line of code.
- The pre-commit hook runs the fast ones: formatting, clippy, and the
  rules of [§21.1](#211-crates) to [§21.3](#213-code-conventions).

## Appendix A. SQLite features

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
  `coven_lost` ([§8.5](#85-keys-and-uniqueness)).

### A.4 Restrict and no-action foreign keys

- Problem: a device can delete a parent while another adds a child it
  hasn't seen.
- Status: the child is taken out while its parent is gone, and recorded in
  `coven_lost` ([§8.4](#84-foreign-keys)).

### A.5 CHECK constraints

- Problem: two concurrent edits that each pass can merge into a row that
  fails, such as one device setting `start` and another `end`.
- Status: a row that fails after a merge is taken out until it passes, and
  recorded in `coven_lost` ([§8.6](#86-check-constraints)).

### A.6 Triggers that write synced tables

- Problem: a trigger that runs again while coven applies a remote write
  would repeat what the original device already sent.
- Status: allowed as shared triggers, which run only on the device making
  the write ([§8.7](#87-triggers)).

### A.7 Schema changes

- Problem: devices running different app versions hold different schemas.
- Status: additions sync with no change of version; any other change
  raises the store's version, and every device updates and reloads ([§17](#17-schema-changes)).
