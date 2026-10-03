# coven, from scratch

## 1. What coven is

- Sync for a small intimate group: a person's own devices, or a household.
- The app's data is SQLite. The app keeps its schema and writes ordinary SQL.
- Rows can carry files, like audio, images or documents, which sync with
  them.
- No server. It syncs through storage the members already have.
- Everything coven writes to the provider is encrypted with the store key.
- Only members' devices hold the store key.

## 2. Threat model

- The storage provider is assumed *honest but curious*, not *hostile*:
  - it may read what it stores;
  - it doesn't alter, withhold, fork<sup>1</sup> or roll back<sup>2</sup> what it
    stores;
  - it sees object sizes, counts, timing, and which device writes.
- A thief with storage credentials but not the store key may be hostile.
- Deletion and withholding can't be prevented. No design can.
- Current members trust each other: each has access to the storage and
  isn't hostile on it.
- An ex-member may keep a copy of the store key they had.

## 3. Guarantees

- **Availability:** the app reads and writes with no network; writes never
  wait on sync.
- **Confidentiality:** members' data can't be read by those it isn't for:
  - the provider and outsiders can't read any of it;
  - a circle's contents are readable only by its members.
- **Integrity:** members' data can't be tampered with:
  - members: no member can write as another member;
  - outsiders: nobody without the store key can alter or forge it;
  - rollback and forks are detected.
- **Authorization:** only admins change membership, and every device
  enforces it.
- **Atomicity:** another device applies a write all together or not at
  all.
- **Convergence:** every device ends up with the same data:
  - a write made after seeing another wins over it on every device;
  - no device sees a write before the writes its author had seen;
  - when concurrent writes set the same cell, the losing value is recorded,
    not silently dropped.
- **Durability:** a crash loses nothing:
  - every committed write is still uploaded;
  - every operation with several steps resumes and finishes, for example:
    - removing a member, then rotating the key;
    - writing a snapshot, then deleting the logs it covers;
    - uploading a file, then marking it stored.
- **Revocation:** an ex-member can't read anything written after they left.
- **Bounded storage:** cloud history doesn't grow forever.

## 4. Storage providers and access

- A store lives on one provider:
  - S3;
  - Google Drive;
  - Dropbox;
  - OneDrive;
  - iCloud, through CloudKit.
- Every member reaches the storage using their own provider account.
- On S3, each member has their own access key.
- What coven needs from a provider:
  - create an object;
  - read it, whole and by range;
  - list a prefix;
  - delete;
  - grant and revoke a member's access.

## 5. Local database

- The app declares which tables sync. The rest stay on the device.
- A *write* is one transaction that changes synced tables.
- A *row change* is one row's insert, update or delete within a write.
- SQLite's session extension records the row changes each write makes.
- Each write commits, together:
  - its rows, as they now stand;
  - its *write record*, waiting to be uploaded:
    - its row changes: which rows, which columns, old and new values;
    - which device wrote it, its number, its timestamp, and what it had
      read;
    - the schema version it was made with;
    - the device's signature;
  - so every committed write gets uploaded, even after a crash.
- A write record, for a write that fixes a note's title and deletes a tag:

  ```
  ana-phone, write 3, 2026-10-02 12:00:00.000 #0
    had read: ben-phone 8, carol-tablet 1
    notes  row 42  update  title: "Grocry list" → "Grocery list"
    tags   row 7   delete
  signed by ana-phone
  ```

- Reads run on several read-only connections at once.
- The app can subscribe to a query; it reruns only when rows it read change.
- Coven keeps its own internal tables in the same database. The app can't
  read or write them.

## 6. Syncing writes

- Each device uploads its writes to its own log in storage, and the other
  devices download them.
- No two devices write the same object, so devices never have to coordinate
  their uploads.
  - Each object is one write from that device: its write record (§5),
    encrypted.
  - It is named `devices/<device>/<n>`, created once and never changed.
  - `<n>` counts that device's own writes: 1, 2, 3, with no gaps.
  - Its name is part of its encryption, so the provider can't swap one
    object for another.
  - A retried upload writes the same name with the same bytes.

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
- So no device ever sees an effect before its cause:
  - cause: every write a write's device had read when making it;
  - effect: the write itself;
  - Ana creates note 50 in her write 5. Ben's phone reads it, and Ben tags
    the note in his write 10.
  - No device ever has Ben's tag without Ana's note.
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
    and every write it downloads, saved on disk;
  - to stamp a new write:
    - if its wall clock is past that, it uses the wall clock, counter 0;
    - otherwise it uses that latest time, counter raised by one;
    - a counter past its maximum moves to the next millisecond.
  - so a new write is always stamped later than everything its device had
    seen, whatever the devices' wall clocks say.
- A write stamped more than five minutes ahead of the receiving device's
  clock waits until that clock catches up, instead of being applied.

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
    signed by ana-phone
    ```

  - `devices/ben-phone/9`:

    ```
    ben-phone, write 9, 2026-10-02 13:01:00.000 #1
      had read: ana-phone 4, carol-tablet 1
      notes  row 42  update  title: "Groceries" → "Weekly groceries"
    signed by ben-phone
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
- Coven keeps three internal tables for this:
  - `coven_writes`: one row per write the device has applied, with the
    write's timestamp, which includes its device, and its number;
  - `coven_columns`: one row per synced column, naming its table and
    column;
  - `coven_cells`: one row per synced cell, naming the column, the row, and
    the write that set it.
- Note 42 on Ben's phone, after Ana's write 4 and its own write 9:

  ```
  notes                            the app's own table
    id   title               body
    42   "Weekly groceries"  "milk, eggs"

  coven_columns
    id   table   column
    1    notes   title
    2    notes   body

  coven_cells
    column   row   write
    1        42    7
    2        42    1

  coven_writes
    id   timestamp                                number
    1    2026-10-01 09:12:40.511 #0  ana-phone    1
    7    2026-10-02 13:01:00.000 #1  ben-phone    9
  ```

- To find which write set note 42's title:
  - in `coven_columns`, notes' title is column 1;
  - in `coven_cells`, column 1 of row 42 points to write row 7;
  - in `coven_writes`, write row 7 is Ben's phone's write 9, stamped
    13:01:00.000 #1.
- Applying a row change to a cell keeps whichever value has the larger
  timestamp.

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
  signed by carol-tablet
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
  - a cell both set goes to the write with the larger timestamp (§8.1);
  - a cell only one sets keeps that write's value.
- All cells a write sets share its timestamp, so between any two writes,
  the one with the larger timestamp wins every cell they both set.
- At 15:00, offline, Ben sets note 42's title and Ana sets its body:

  ```
                 title                body
  Ana's write    ·                    "milk, eggs, bread"
  Ben's write    "Weekend shopping"   ·
  result         "Weekend shopping"   "milk, eggs, bread"
  ```

- No cell overlaps, so both writes keep their cells on every device.
- When concurrent writes set the same cell, the newer one wins, and coven
  records the value that lost.
  - Carol's "Shopping" replaced Ben's "Weekly groceries" without her write
    having read it, so coven records "Weekly groceries".
  - The writes were concurrent when the winning write had not read the
    write that set the losing value.
  - The app can read these records and offer to restore the lost value.
  - Every device holds the same records, because they follow from the
    writes alone.

### 8.3 Deletes

- Deleting a row sets its deleted mark.
- The mark is one more cell, stamped with the delete's timestamp.
- Editing other columns never touches the mark, so a concurrent edit
  doesn't bring a deleted row back.
  - At 16:00 Ana deletes note 43, "Hardware store".
  - At the same time Ben, offline, edits its title.
  - Note 43 stays deleted on every device.
- Re-adding the row clears the mark with a newer timestamp.
  - At 17:00 Ana undoes the delete, and note 43 is back on every device.

### 8.4 Foreign keys

- A row's parent is always applied before it (§7.1).
- A row pointing at a deleted row follows its foreign key's declared action,
  whichever was applied first.
  - Cascade deletes it.
  - Set null clears the reference.
- At 16:00, while Ana deletes note 43, Ben, offline, adds tag 9 to it.
  - With cascade, tag 9 is deleted on every device.
- If a deleted parent is re-added, do the rows deleted along with it come
  back?
  - When Ana undoes her delete at 17:00, does tag 9 come back?
- What does a restrict foreign key do when the delete and the new child
  were concurrent?

### 8.5 Keys and uniqueness

- A primary key change is recorded as a delete of the old row plus an
  insert of the new one.
- Where a value must be unique, the app makes it the primary key, or
  derives the primary key from it.
- Then two inserts of the same value are one row, and merge.
  - Ana and Ben, both offline, each add the tag "urgent".
  - The tag's key is derived from "urgent", so both inserts are one row.
- Coven refuses to sync a table with any other unique constraint, checked
  whenever the schema changes.

### 8.6 Triggers

- Triggers on synced tables may write only local tables.
- They run on every device, for its own writes and applied ones alike.
- So a local table a trigger maintains stays current everywhere.
  - A trigger keeps a local search index of note titles.
  - When Ben's phone applies Carol's write 2, the trigger updates Ben's
    index to "Shopping".
- Should triggers that write synced tables be allowed?

## 9. Rollback and fork detection

- Each device remembers how far it has applied every device's log.
- Each device also posts how far it has applied every log, as one object
  of its own.
- Something is being withheld when a device can't find a write that
  another device's position, or a write's "had read", says exists.
  - Carol's tablet sees Ben's write 9 had read Ana's log up to 4.
  - If storage shows Carol's tablet only Ana's writes 1 to 3, write 4 is
    being withheld from it.
- What does the app see when one is detected?

## 10. Signatures

- Every device has its own key pair: a private key it never shares, and a
  public key the other members know.
- Every write record is signed with the private key of the device that
  wrote it, so who wrote what is authentic.
- This is about authenticity, not trust.

## 11. Membership and roles

- Membership is a synced table.
- Several equal admins.
- Only admins change membership.
- Removing the last admin isn't allowed.
- Every device works out the member list from all writes to the membership
  table, in timestamp order.
- Devices check each write record's signature against the member list at
  the write's timestamp (§10), which is what makes these rules hold.

## 12. Removing a member

- Revoke their storage access.
- Rotate the store key: make a new one and encrypt it to each remaining
  device's public key (§10).
- So an ex-member's copy of the old store key reads nothing written after
  they left, even if they regain read access.

## 13. Joining, restore, and not losing the store key

- An existing device pairs a new one over the local network.
- Pairing hands over the store key and storage access.
- An admin re-invites a person who lost every device.
- A one-person store has a restore code, written down by the
  person, holding the store key and storage credentials.
- Losing every device and the restore code loses the store for good, since
  everything is encrypted.
- Each device keeps the store key in the OS keychain.
- Where the keychain syncs, as iCloud Keychain does, a new device of the
  same person can open the store without pairing.
- Setup makes saving the restore code part of creating a store.
- What stands in for a synced keychain on Android, Windows and Linux?
- How does a device that got the store key from the keychain add its own
  public key to the member list, since write records are checked against
  it?

## 14. Circles

- A circle is a group of members inside a store who share rows the other
  members can't read.
- Each circle has its own key, encrypted to each circle member's devices'
  public keys.
- All members can see that a circle exists, who writes to it, when, and how
  much.
- A circle's writes go in the same device logs, encrypted
  with its key.
- Devices outside a circle skip what they can't open.
- Leaving a circle rotates the circle key.
- What happens when a row references a row in another circle?

## 15. Snapshots and bounded history

- Any member writes a snapshot: the synced tables, encrypted, and
  how far into every log they reach.
- A new device loads the latest snapshot, then the logs after it.
- A log object is deleted once a snapshot covers it and every member's
  posted position has passed it.
- Deleted rows' marks are cleared the same way.
- Who writes snapshots, and when?
- How long do covered logs stay?

## 16. Files

- Files are what the app attaches to rows: audio, images, documents.
- Each file is stored encrypted, named by a hash of its content.
- Encrypted in fixed-size chunks, so any range can be read and
  checked on its own.
- A local cache with a size budget; the app can pin a file to keep
  it on the device regardless of the budget.
- A file waits in a local upload queue until it is stored.
- A file is deleted once nothing in the latest snapshot or the
  logs after it refers to it.
- How do files inside a circle work?
- Which device deletes?

## 17. Operations with several steps

- Upload, snapshot, deleting covered logs, and pairing each take several
  steps that a crash can interrupt.
- One shared mechanism records each operation's progress on disk
  and resumes it after a crash.
- Every step is safe to run twice.
- A failure goes to whoever started the operation.

## 18. Schema changes

- Synced schema changes only add tables and columns.
- Every write records the schema version it was made with.
- A device keeps writes from a newer schema version but holds them until
  its app upgrades.
- How do non-additive schema changes work, if at all?

## 19. The API apps use

- Open, write, read, subscribed queries, files, membership, circles,
  pairing, sync status.

## Notes

1. Fork: show different devices different histories.
2. Roll back: serve an older version of what it stores.
