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
  - On S3, that means their own access key, not a shared one.
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
  ana-phone, write 3, 2026-10-02 13:04:12.003 #0
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

- timestamps decide which write a cell keeps;
- causality decides when a device may apply a write.

### 7.1 Timestamps

- Every write carries a timestamp: the device's wall clock time, plus a
  counter.
  - Ana's write 3 (§5) is stamped 2026-10-02 13:04:12.003 #0.
- A timestamp is 48 bits of milliseconds, a 16-bit counter, and the
  device's 64-bit id.
- Timestamps sort by milliseconds, then counter, then device id.
  - So no two devices' timestamps are ever equal.
- Every install, and every restored copy of a store, gets a new device id.
- Each device keeps the latest timestamp it has seen, from its own writes
  and every write it downloads, saved on disk.
  - After write 3, Ana's phone's latest is 13:04:12.003 #0.
- To stamp a new write:
  - if its wall clock is past that, it uses the wall clock, counter 0;
  - otherwise it uses that latest time, counter raised by one;
  - a counter past its maximum moves to the next millisecond.
- So a new write is always stamped later than everything its device had
  seen, whatever the devices' wall clocks say.
- A write stamped more than five minutes ahead of the receiving device's
  clock waits until that clock catches up, instead of being applied.

### 7.2 Causality

- Every write records how far its device had read every other device's
  log.
  - Ana's write 3 had read Ben's log up to 8.
- A device applies a write only after it has applied everything that
  write's device had read.
- So no device ever sees an effect before its cause:
  - cause: every write a write's device had read when making it;
  - effect: the write itself;
  - Ana creates note 50 in her write 5. Ben's phone reads it, and Ben tags
    the note in his write 10.
  - No device ever has Ben's tag without Ana's note.
- Two writes are concurrent when neither device had read the other's:
  - Carol's tablet is offline all day. Its write 2, at 18:00, had read
    Ben's log only up to 8.
  - Ben's write 9 had read Carol's tablet's log only up to 1.
  - So Carol's write 2 and Ben's write 9 are concurrent.
- A device can apply two concurrent writes in either order.
  - Dana's laptop may apply Carol's write 2 before or after Ben's write 9.
  - Neither waits for the other.
  - Both orders converge on the same note (§8).

### 7.3 Example

- Suppose Ana's phone clock runs a minute fast, and Ben's is right. In
  real time:
  - at 13:05:00, Ana edits a note:
    - her phone reads 13:06:00;
    - it stamps her write 13:06:00.000 #0;
    - it uploads the write record.
  - at 13:05:20, Ben's phone downloads Ana's write record:
    - its latest timestamp seen is now 13:06:00.000 #0;
    - it has now read Ana's log up to 4.
  - at 13:05:30, Ben edits the same note:
    - his phone reads 13:05:30, behind 13:06:00.000;
    - it stamps his write 13:06:00.000 #1;
    - it uploads the write record.
- If these were Ana's phone's 4th write and Ben's phone's 9th, storage
  now holds:
  - `devices/ana-phone/4`:

    ```
    ana-phone, write 4, 2026-10-02 13:06:00.000 #0
      had read: ben-phone 8, carol-tablet 1
      notes  row 42  update  title: "Grocery list" → "Groceries"
    signed by ana-phone
    ```

  - `devices/ben-phone/9`:

    ```
    ben-phone, write 9, 2026-10-02 13:06:00.000 #1
      had read: ana-phone 4, carol-tablet 1
      notes  row 42  update  title: "Groceries" → "Weekly groceries"
    signed by ben-phone
    ```

- Both mechanisms put Ben's write after Ana's:
  - timestamp: same millisecond, but Ben's counter is 1 and Ana's is 0;
  - had read: Ben's write had read Ana's log up to 4.
- Suppose Dana's laptop downloads Ben's write 9 before Ana's write 4.
  - Write 9 had read Ana's log up to 4.
  - Dana's laptop holds write 9 until it has applied Ana's write 4.

## 8. Merge

- Each synced cell stores its value and the timestamp of the write that
  set it, in coven's internal tables.
- Applying a row change to a cell keeps whichever timestamp is larger.
  - So each cell ends with the largest-stamped write it has received,
    whatever the arrival order and however often a write repeats.
- Every cell a write touches gets that write's timestamp.
  - So a write wins or loses whole against a concurrent write to the same
    cells.
- Different columns of a row merge independently.
  - Both phones are offline. Ana changes note 42's body; Ben changes its
    title.
  - Different columns, so both edits stay.
- Deleting a row sets its deleted mark: one more cell, stamped with the
  delete's timestamp.
  - Editing other columns never touches the mark, so a concurrent edit
    doesn't bring a deleted row back.
    - Ana deletes note 42 while Ben, offline, edits its title.
    - The note stays deleted on every device.
  - Re-adding the row clears the mark with a newer timestamp.
- When concurrent writes set the same cell, the newer one wins, and coven
  records the value that lost (§8.1).
  - The app can read these records and offer to restore the lost value.
  - Every device holds the same records, because they follow from the
    writes alone.
- A row's parent always arrives before it (§7.2).
- A row pointing at a deleted row is deleted too, whichever arrived first.
  - Ana deletes note 42 while Ben, offline, adds tag 9 to it.
  - Tag 9 is deleted on every device.
- Primary keys never change. Changing one is a delete plus an insert.
- A unique value that names a thing is the row's identity, so two inserts
  of it are one row and merge.
- Synced tables have no other unique constraints.
- A trigger runs only on the device where its write happens.
  - What it writes to synced tables is part of that write and syncs with
    it.
  - Applying remote writes doesn't run triggers.
- If a deleted parent is re-added, do the rows deleted along with it come
  back?
- How does coven keep the app's triggers from running while it applies
  remote writes?

### 8.1 Example

- This follows note 42's title through the writes in §7.3.
- Every device applies Ana's write 4, then Ben's write 9:
  - write 4 sets the title to "Groceries", stamped 13:06:00.000 #0;
  - write 9 is stamped 13:06:00.000 #1, larger, so the title becomes
    "Weekly groceries".
- With wall-clock stamps alone, Ben's write would be stamped 13:05:30:
  - the title would keep Ana's "Groceries", although Ben's write had read
    hers;
  - the clock rule (§7.1) prevents this by keeping timestamps in agreement
    with "had read".
- Carol's tablet has been offline all day. At 18:00 Carol edits the same
  note:
  - her tablet reads 18:00, past anything it has seen;
  - it stamps her write 18:00:00.000 #0;
  - it uploads the write record when it comes back online.
- Storage now also holds `devices/carol-tablet/2`:

  ```
  carol-tablet, write 2, 2026-10-02 18:00:00.000 #0
    had read: ana-phone 3, ben-phone 8
    notes  row 42  update  title: "Grocery list" → "Shopping"
  signed by carol-tablet
  ```

- Carol's write 2 and Ben's write 9 are concurrent (§7.2), so "had read"
  can't order them.
- Their timestamps can: 18:00 is later than 13:06.
  - The title becomes "Shopping" on every device.
  - Coven records that "Shopping" replaced "Weekly groceries" unseen.
- Dana's laptop may apply Carol's write 2 before Ben's write 9:
  - the title becomes "Shopping", stamped 18:00:00.000 #0;
  - Ben's write 9 arrives with a smaller stamp, so the title stays;
  - coven records the same lost value, "Weekly groceries".
- Without timestamps, only an arbitrary rule could decide, such as the
  larger device id winning, and Ben's earlier edit could beat Carol's
  later one.

## 9. Rollback and fork detection

- Each device remembers how far it has applied every device's log.
- Each device also posts how far it has applied every log, as one object
  of its own.
- Something is being withheld when a device can't find a write that
  another device's position, or a write's "had read", says exists.
  - Dana sees Ben's write 9 had read Ana's log up to 4.
  - If storage shows Dana's laptop only Ana's writes 1 to 3, write 4 is
    being withheld from her.
- What does the app see when one is detected?

## 10. Membership and roles

- Membership is a synced table.
- Several equal admins.
- Only admins change membership.
- Removing the last admin isn't allowed.
- Every device works out the member list from all writes to the membership
  table, in timestamp order.

## 11. Removing a member

- Revoke their storage access.
- Rotate the store key: make a new one and encrypt it to each remaining
  device's public key (§12).
- So an ex-member's copy of the old store key reads nothing written after
  they left, even if they regain read access.

## 12. Signatures

- Every device has its own key pair: a private key it never shares, and a
  public key the other members know.
- Every write record is signed with the private key of the device that
  wrote it, so who wrote what is authentic.
- Devices check each write record's signature against the member list at
  the write's timestamp, which is what makes the roles hold (§10).
- This is about authenticity, not trust.

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
