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
  - nothing the provider sees is computed from content without a secret
    key, so it can't test whether you store a file or value it knows;
  - a circle's contents are readable only by its members.
- **Integrity:** members' data can't be tampered with:
  - members: no member can write as another member;
  - outsiders: nobody without the store key can alter or forge it.
- **Authorization:** only admins change membership, and every device
  enforces it.
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
  - its *write record*, waiting to be uploaded in coven's `coven_uploads`
    table:
    - its row changes: which rows, which columns, old and new values;
    - which device wrote it, its number, its timestamp, and what it had
      read;
    - the schema version it was made with;
    - a signature over the record, made with the key of the member whose
      device wrote it;
  - so every committed write gets uploaded, even after a crash.
- A write record, for a write that fixes a note's title and deletes a tag:

  ```
  ana-phone, write 3, 2026-10-02 12:00:00.000 #0
    had read: ben-phone 8, carol-tablet 1
    notes  row 42  update  title: "Grocry list" → "Grocery list"
    tags   row 7   delete
  signed with Ana's key
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
- A write record leaves `coven_uploads` (§5) once its upload succeeds.
- Each device remembers how far it has applied every device's log.
- It also posts those positions to storage, as one object of its own.

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
  - A device has always read its own earlier writes.
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
  (§7.1).
- Every device that applied the same writes ends with the same database,
  coven's own tables included, whatever order the writes arrived in.
- Merging has two layers.
  - The *merged state* comes from the writes themselves: each row's
    generation, and each cell's winning value.
  - The *removal rules* then decide which rows the app sees, from the
    merged state alone.
- The merged state follows two rules:
  - cells: of two values for one cell, the one with the larger timestamp
    stays;
  - deletes: a row change concurrent with a delete of its row loses
    (§8.3).
- A value is *lost* when some write replaced it, and no write that replaced
  it had read it.
  - A lost value is recorded, so the app can show it and offer it back.
  - It stops being lost if a later write replaces it having read it.
- The removal rules take a row out of the app's table while a reason holds:
  - foreign keys: a row whose parent is gone, or was deleted and re-added
    since the row last pointed at it, follows its key's action: cascade and
    restrict take it out, set null and set default make its reference read
    null or the default (§8.4);
  - CHECK constraints: a row whose merged values fail is taken out (§8.6);
  - ancestors: an ancestor that no shared row points at is taken out
    (§14);
  - unique values: of two rows claiming one value, the row whose write has
    the larger timestamp is taken out, since the first claim keeps it
    (§8.5).
- A removal is never stored as a delete.
  - Coven keeps a removed row's values, and puts it back when the reason
    goes away, e.g. when the reference that made it a parent's child is
    pointed elsewhere.
  - A removed row is recorded as lost while it is out.
- Every rule but the unique one keeps firing when more rows are removed,
  so applying them in any order ends with the same rows removed.
- The unique rule is judged once, between two passes of the others:
  1. apply the other rules until none fires;
  2. judge unique values among the rows still present;
  3. apply the other rules again, to the losers' children and ancestors.
- So every device that applied the same writes sees the same rows, whatever
  order the writes arrived in, and whatever order the rules ran in.
  - The proof is in `coven-merge-proof.md`, checked by machine for the
    merged state and the removal rules.
- When a write changes which rows are removed, coven makes the change in
  the app's table with ordinary SQL, which triggers see like any other.
- Coven keeps six internal tables for this:
  - `coven_writes`, one row per write the device has applied, naming:
    - the write's timestamp, which includes its device;
    - the write's number.
  - `coven_columns`, one row per synced column, naming:
    - its table;
    - its column.
  - `coven_rows`, one row per generation of each synced row, naming:
    - its table;
    - its primary key;
    - the generation: how many times the row had been created, deleted or
      re-added;
    - the write that moved it there, or of several concurrent ones, the one
      with the smallest timestamp.
  - `coven_cells`, one row per synced cell, naming:
    - its `coven_columns` row;
    - its `coven_rows` row;
    - the write that set it.
  - `coven_lost`, one row per lost value or removed row, naming:
    - the cell, or the row;
    - the value that lost;
    - the write that set it;
    - the write that replaced it without having read it, or the rule that
      removed the row.
  - `coven_held`, one row per row a removal rule has taken out of the app's
    table, holding its values, and one per reference a rule makes read
    null or a default, holding the real one.
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
    id   table   key   generation   write
    3    notes   42    1            1

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
- When concurrent writes set the same cell, the newer one wins.
- The value it replaced is lost when no write that replaced it had read it
  (§8).
- In §8.1, note 42's title went through four values:

  ```
  value               set by          replaced by                     lost?
  "Grocery list"      Ana's write 1   Ana 4, Ben 9, Carol 2           no: all had read it
  "Groceries"         Ana's write 4   Ben 9, Carol 2                  no: Ben 9 had read it
  "Weekly groceries"  Ben's write 9   Carol 2                         yes
  "Shopping"          Carol's write 2  nothing                        the current value
  ```

  - On Carol's tablet, Ana's write 4 arrives after Carol's write 2, so
    "Groceries" is lost at first; Ben's write 9, which had read it, then
    takes it back out.
  - Every device ends with one row:

    ```
    coven_lost
      cell            lost value          set by          replaced by
      note 42 title   "Weekly groceries"  Ben's write 9   Carol's write 2
    ```

- The app can read `coven_lost` and offer to restore a lost value.
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
  ben-phone, write 10, 2026-10-02 16:00:00.000 #0
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
  if neither had read it, by the rule of §8.
  - The first delete to arrive records what it hadn't read.
  - If the second had read one of those values, it removes that value's
    `coven_lost` row, which needs nothing from the deleted row.
  - The generation's `coven_rows` row names the delete with the smaller
    timestamp, whichever arrived first.
- E.g. at 16:00 Ana deletes note 43, "Hardware store", while Ben, offline,
  edits its title, and at 17:00 Ana re-adds it.
  - Note 43 on Carol's tablet:

    ```
    coven_rows
      time    id   table   key   generation   write
      14:45   4    notes   43    1            16      Ana's write 5 creates it
      16:00   5    notes   43    2            18      Ana's write 7 deletes it
      17:00   6    notes   43    3            19      Ana's write 8 re-adds it

    notes
      14:45   row 43 present
      16:00   row 43 gone
      17:00   row 43 present again
    ```

  - Ben's edit was made at generation 1, so it loses whenever it arrives,
    even if a fast clock stamps it after 17:00.
  - Generation 2's row names write 7, so even a device that gets Ben's edit
    after the re-add records it as replaced by write 7.
  - Every device records:

    ```
    coven_lost
      cell           lost value                  set by          replaced by
      note 43 title  "Hardware store, Saturday"  Ben's write 10  Ana's write 7
    ```

- Concurrent deletes both move the generation from 1 to 2, so they count
  as one delete.
  - If Ben had deleted note 43 instead of editing it, Ana's 17:00 re-add
    would still bring it back, since Ben deleted the same generation she
    did.
- Concurrent re-adds both move it from 2 to 3, and their cells merge
  (§8.2).
- Ana's write 6, between 14:45 and 16:00, is her 15:00 edit to note 42's
  body (§8.2), applied as row 17.

### 8.4 Foreign keys

- A foreign key makes one row point at another, its parent.
  - E.g. tag 9's `note_id` points at note 43, so note 43 is tag 9's parent.
- Every device applies a parent's insert before the row pointing at it,
  through causality alone (§7.1):

  ```
  Ana's phone    write 5: insert note 43
                    │
                    │  Ben's phone applies write 5
                    ▼
  Ben's phone    write: insert tag 9, pointing at note 43
                 had read: ana-phone 5
                    │
                    │  another device downloads Ben's write first
                    ▼
  that device    holds Ben's write
                    → applies Ana's write 5, inserting note 43
                    → applies Ben's write, inserting tag 9
  ```

- A row change that points at a parent also carries the parent's
  generation (§8.3).
- On the device that deletes a parent, SQLite runs each child's foreign key
  action, and the write records the results as ordinary row changes.
  - E.g. tags point at notes with cascade, and links with set null; Carol
    tagged note 43 "hardware" as tag 8 at 15:30, and every device has it.
  - At 16:00 Ana deletes note 43, and her write records:

    ```
    ana-phone, write 7, 2026-10-02 16:00:00.000 #0
      had read: ben-phone 9, carol-tablet 3
      notes  row 43  generation 1  delete
      tags   row 8   generation 1  delete
      links  row 5   generation 1  update  note_id: 43 → null
    signed with Ana's key
    ```

- A `RESTRICT` or `NO ACTION` key makes SQLite refuse that delete instead,
  if the deleting device has a child.
- A child the deleting device didn't have, because it was added or pointed
  at the parent concurrently, is handled by the removal rule of §8.
  - The rule applies while its parent is gone, or at a newer generation
    than the one the child points at.
  - Under cascade, restrict or no action, the child is taken out of the
    app's table, held in `coven_held`, and recorded as lost.
  - Under set null or set default, the child stays, and its reference reads
    null or the default, with the real one held.
- The child's own generation never moves, so it comes back if its
  reference is later pointed at a parent that is present.
- E.g. at 16:00, while Ana deletes note 43, Ben, offline, adds tag 9 to it,
  then moves tag 9 to note 44:

  ```
  Ben's phone
    16:00  offline. Ben's write 10: insert tag 9 → note 43
    16:05  Ben's write 11: tag 9 → note 44
    16:30  online. Applies Ana's write 7: delete note 43
             tag 9 points at note 44, so no rule applies

  Carol's tablet
    16:00  applies Ana's write 7: delete note 43
    16:30  Ben's write 10 arrives: tag 9 → note 43
             note 43 is gone, so tag 9 is held, and recorded as lost
           Ben's write 11 arrives: tag 9 → note 44
             the reason is gone, so tag 9 is put back
  ```

  - Both devices end with tag 9 on note 44, and nothing lost.
  - Had Ben not moved it, both would end with tag 9 held, and the same
    `coven_lost` row, so the app can offer to put it on another note.
- Re-adding a deleted parent doesn't bring back the rows held with it,
  since they point at its old generation.
  - Re-adding is a new insert. Any children the app wants back, it inserts
    in the same write.

### 8.5 Keys and uniqueness

#### Kinds of keys

- Each synced table declares one of two kinds of primary key:
  - independent: each new row gets a UUID, so rows made on different
    devices never share a key;
  - shared: the app derives the key from what makes the row unique, so
    equal values are one row on every device.
- Coven refuses, checked whenever the schema changes:
  - an independent key that isn't a UUID;
  - a key SQLite picks itself, such as an integer rowid;
  - a synced table with no primary key.
- Either kind can span several columns.
  - E.g. `note_tags(note_id, tag_id)` is a shared key.
  - Ana and Ben, both offline, each tag note 42 "urgent", and make one row.
  - An independent key over several columns needs a UUID in one of them.
- E.g. notes have independent keys, and tags have shared keys derived from
  the tag's name.
  - Ana and Ben, both offline, each add the tag "urgent".
  - Both derive the same key, so their inserts are one row, and merge
    (§8.2).
- With shared keys, a device can insert a row another device is deleting.
  - E.g. Ana deletes the tag "urgent" while Ben, offline, adds "urgent"
    again.
  - Ben's insert was made at the generation before Ana's delete, so it
    loses (§8.3), and his cells go to `coven_lost`.

#### Key changes

- A primary key change is recorded as a delete of the old row plus an
  insert of the new one.
- The old key's `coven_rows` row also records which key replaced it.
- Rows pointing at the old key follow their foreign key's `ON UPDATE`
  action on every device.
- If the device changing the key had the child, SQLite there runs the
  action, and the write records it.
- If it didn't have the child, the action is a removal rule (§8), and the
  child's real reference is held in `coven_held`:
  - under cascade, the reference reads the new key;
  - under set null and set default, it reads null or the default, as in
    §8.4;
  - under restrict and no action, the child is taken out and held, as in
    §8.4.
- E.g. notes point at tags with `ON UPDATE CASCADE`, and at 16:00 Ana
  renames the tag "urgent" to "important", while Ben, offline, tags note
  44 "urgent".

  ```
  Ana's phone
    16:00  renames "urgent" to "important"
             SQLite re-points note 42's tag to "important"
             coven_rows  tags  "urgent"  generation 2  replaced by: "important"

  Ben's phone
    16:00  offline. Ben's write: note 44 tag → "urgent"
    16:30  online. Applies Ana's write
             note 44's tag reads "important"

  Carol's tablet
    16:00  applies Ana's write
    16:30  Ben's write arrives: note 44 tag → "urgent"
             "urgent" was replaced, so note 44's tag reads "important"
  ```

- Every device ends with notes 42 and 44 tagged "important".
- Two devices can change one key to different new keys concurrently.
  - E.g. Ana renames "urgent" to "important" while Ben, offline, renames
    it to "critical".
  - Both new tags exist after the merge.
  - A child still pointing at "urgent" follows the change with the larger
    timestamp.

#### Unique constraints

- On one device, SQLite refuses a write that repeats a unique value, so two
  rows can claim one value only through concurrent writes.
- Of two rows claiming one value, the row whose write has the smaller
  timestamp keeps it, since the first claim to a value keeps it.
- The other row is taken out and held, whether its write inserted it or
  edited it to claim the value, and recorded as lost.
  - It comes back if the reason goes away, e.g. its own value is changed,
    or the winning row is removed.
  - Unique values are judged after the other removal rules, among the rows
    they leave (§8).
- E.g. note titles are unique, and Ana and Ben, both offline, each add a
  note titled "Groceries".
  - Ana's insert of note 45 is stamped 16:00, and Ben's of note 46 is
    stamped 16:05.
  - Every device keeps note 45, and records Ben's note 46 in `coven_lost`.

### 8.6 CHECK constraints

- A CHECK on one column can't fail after a merge, since each value passed
  it on the device that wrote it.
- A CHECK on several columns can, when concurrent writes each set some of
  them.
  - E.g. a row checks `start <= end`, Ana sets `start` and Ben, offline,
    sets `end`:

    ```
                  start   end
    before         5      12
    Ana's write   10       ·
    Ben's write    ·       8
    merged        10       8     fails
    ```

- A row that fails after a merge is taken out and held, and recorded as
  lost.
  - It comes back if a later write makes it pass, e.g. Ben setting `end`
    to 20.
- Until the second write arrives, each device's row passes, since it has
  seen only one of them.
- Every device ends with the row held, and the same `coven_lost` row.

### 8.7 Triggers

- The app declares each trigger on a synced table as local or shared.
- A local trigger runs on every device, for its own writes and applied ones
  alike, and writes only local tables.
  - Every change coven makes is ordinary SQL, including a late write
    taking a row back out (§8), so a count a local trigger keeps stays
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
    coven refuses one that doesn't, checked whenever the schema changes.
- A trigger's write to the wrong kind of table fails.
  - SQLite's authorizer callback reports each table a statement would
    write, with the trigger doing the write, when the statement is
    prepared.
  - Coven refuses the statement when a local trigger writes a synced table,
    or a shared trigger a local one.
- A shared trigger's writes merge like any other write, so a value it
  derives can be wrong after concurrent writes.
  - E.g. a shared trigger counts a note's tags, and Ana and Ben, both
    offline, each add a tag to note 42:

    ```
                  tags on note 42      tag_count
    Ana's write   adds tag 10          1 → 2
    Ben's write   adds tag 11          1 → 2
    merged        3 tags               2
    ```

  - A local trigger writing a local table counts 3 on every device.

## 9. Members and roles

- A member is a person in the store, using it from one or more devices.
  - Each member has their own keys, which identify them (§11).
  - An admin adds a member by writing their public key to the store log.
- The *store log* records changes to the store itself, separate from the
  app's writes.
  - Each change is one *entry*: add or remove a member, change a role, add
    or remove a device, or raise the store's schema or format version.
  - An entry names the store log entries its author had read, and is
    signed with its author's member key.
  - Entries live at `store-log/<device>/<n>`, numbered like a device's
    writes (§6).
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

  - The store's first entry creates it, and names its first admin.
- Roles:
  - several equal admins;
  - only admins add and remove members, and change roles;
  - each member adds and removes their own devices, with their own key, and
    admins can remove any device;
  - the store always has at least one admin.
- Every device that applied the same entries ends with the same member
  list, whatever order they arrived in.
- Applying an entry follows these rules:
  - order: a device applies an entry once it has every entry that entry had
    read, never by timestamps;
  - authority: an entry applies only if its author's role allowed it, in
    the member list the author had read;
  - agreement: concurrent entries that don't conflict both apply, and two
    saying the same thing combine;
  - less access: of two concurrent entries that conflict, the one giving
    less access applies;
  - ties: when neither gives less access, the one with the smaller
    timestamp applies;
  - a dropped entry is shown to its author.
- Two concurrent entries conflict when:
  - they're about the same member or device and say different things;
  - or applying both would break a role rule.
- E.g. Ana adds Dan while Ben adds Eve: both apply.
- E.g. Ana adds Ben's new phone while Ben removes it: the removal applies.
- E.g. Ana makes Ben an admin while Carol makes him a member: he stays a
  member.
- E.g. Ana and Ben, both admins, remove each other while offline.
  - Removing both would leave no admin, so they conflict.
  - Neither gives less access than the other, so the earlier removal
    applies.

## 10. Device identity

- A device is one install of the app, with its own device id, belonging to
  one member.
  - Its member adds it to the store log (§9).
- A device restored from a backup is a new device, with a new id.
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
- A device applies a write only if it is signed with the key of a member
  the store log has added, and comes from one of that member's
  devices (§9).
  - So a write by Ana's phone counts as Ana's.
- A removed device's writes still count if they reached storage.
  - Removing a device takes away its storage access, so nothing it writes
    afterwards can reach other devices.

## 11. Keys

- Coven uses three kinds of key:
  - the store key, which encrypts everything coven writes to storage;
  - each member's keys, which identify them (§9), sign their writes and
    store log entries, and open the store key;
  - storage credentials: each device's own sign-in to the provider, or on
    S3 its member's access key (§4).
- Each store key is sealed to every member's public key, and the sealed
  copies are kept in storage, at `keys/store/<n>/<member>` for the store's
  `n`th key.
  - So a member's key alone gets the current store key: a device holding
    it reads its member's sealed copy from storage and opens it.
- The store key is replaced whenever a member is removed (§12).
  - Writes made after that use the new key.
  - Devices keep the old keys, to read writes made before.
- Each device keeps its member's key in the OS keychain.
- Storage access, not keys, is what keeps a removed device out.

### 11.1 Cryptography

- A member's keys are two key pairs, kept together:
  - an Ed25519 pair, which signs;
  - an X25519 pair, which opens keys sealed to the member.
- Everything coven writes to storage is encrypted with XChaCha20-Poly1305:
  write records, store log entries, snapshots, sealed keys and file
  chunks.
  - Each object's storage path is bound into its authentication, so the
    provider can't swap one object for another.
  - Each file chunk also binds its index.
- Nonces:
  - for a file's chunks, derived from the key and the file's name, combined
    with the chunk's index, so the same file under the same key encrypts to
    the same bytes;
  - for everything else, random.
- A key is sealed to a member with an anonymous sealed box: X25519 with
  XChaCha20-Poly1305.
- Keys for each purpose are derived from the store key, or a circle's key,
  with HKDF-SHA256 and a label per purpose: encryption, naming, file
  nonces.
- A file's storage name is HMAC-SHA256 of its content, with the naming key.

## 12. Removing members and devices

- Removing a device, such as Ana's lost phone, is an entry in the
  store log (§9), and cuts the device off from storage.
- Providers can't cut off one device alone, so Ana cuts off all of hers,
  and signs in again on the ones she keeps:
  - Google Drive, Dropbox and OneDrive: she removes the app's access from
    her provider account;
  - iCloud: she removes the phone from her Apple account;
  - S3: one of her devices replaces her access key, her other devices get
    the new one by pairing, and she writes down a new restore code.
- No keys change: the phone still holds Ana's key, but can't reach storage
  to read or write anything new.
- Removing a member removes them and all their devices, and then:
  - their storage access is revoked (§4);
  - the store key is rotated: a new one, sealed to each remaining member's
    public key (§11).
- So a removed member's copy of the old store key reads nothing written
  after the removal, even if they regain read access.
- A member added concurrently with a rotation doesn't get the new key.
  - Their devices can't read anything written after the rotation.
  - Removing them and adding them again gives them the current key.

## 13. Joining and restore

- A person's new device needs their member key, which gets it the store key
  (§11), in one of three ways:
  - pairing: one of their existing devices hands it over, with storage
    access, over the local network;
  - on Apple platforms, iCloud Keychain syncs it, and the device opens the
    store without pairing;
  - on other platforms, the person enters their restore code.
- The new device then adds itself to the store log, signing with the
  member key (§9).
- A restore code holds the person's member key and storage credentials.
  - The person writes it down when they create or join a store, as part of
    setup.
- An admin re-invites a person who lost every device and their restore
  code.
- Losing every member's devices and restore codes loses the store for good,
  since everything is encrypted.

## 14. Audiences

- Every synced row has an *audience*: the store, one circle, or this device
  only.
  - A row in the store reaches every member's devices.
  - A row in a circle reaches only that circle's members' devices.
  - A row on this device only never leaves it, though its table syncs.
  - E.g. a household's notes are the store's, each person pins notes in a
    circle of their own, and a draft stays on the device that wrote it.
- The app declares, for each synced table, how its rows get their audience:
  - a *root* table names a column holding each row's audience: `store`, a
    circle's id, or `device`, and never NULL;
  - a *descendant* table names one foreign key, and each row takes the
    audience of the row it points at;
  - an *ancestor* table has no audience of its own: a row is in the store
    exactly while a present row in the store or a circle points at it;
  - a table that declares none of these is in the store.
- Audience is defined once, from these declarations, and everything that
  decides where a row goes uses that one definition: writes, snapshots and
  files.

### 14.1 Roots, descendants and ancestors

- A descendant's declared foreign key is one column, into a synced table.
  - Following declared foreign keys from any descendant reaches a root or
    an ancestor; a loop is refused when the database opens.
- The rows that keep an ancestor are those of every other synced table
  with a foreign key into it, except:
  - a table whose declared foreign key points at the ancestor, since it
    takes its audience from the ancestor instead;
  - an *asset* table, a descendant declared never to keep anything alive,
    such as cover images;
  - the ancestor's own table.
  - Only a table's first foreign key into the ancestor counts.
- E.g. todos are descendants of lists, labels are an ancestor, and the
  join table `todo_labels` declares `todo_id`:

  ```
  lists         root         audience column: store or device
  todos         descendant   through list_id
  todo_labels   descendant   through todo_id, so a label is kept by it
  labels        ancestor     kept while a shared todo wears it
  ```

- An ancestor that no table could keep is refused when the database opens.
- A row is *shared* when its audience is the store or a circle.
- A unique constraint or shared key (§8.5) must not span audiences, since a
  device outside a circle can't see the circle's claim to a value.
  - On a root table, it must include the audience column, e.g.
    `UNIQUE(audience, title)`, so the same title in the store and in a
    circle are two separate claims.
  - On a descendant or ancestor table whose rows can be in different
    audiences, it is refused, since there is no audience column to
    include.
  - Coven checks this when the database opens, and after every migration,
    and refuses to open with an error naming the table and the constraint.
- When a write shares a row, every row it points at that isn't shared yet
  goes with it in the same write.
  - A root row is never shared this way; a write that would need it is
    refused.
  - So no device ever receives a row whose parent never arrives.

### 14.2 Moving rows

- A row moves when:
  - its root's audience column changes, or a root row is inserted with
    an audience other than `device`;
  - a descendant is pointed at a parent with another audience;
  - an ancestor gains its first present shared child, or loses its last.
- Moving rows into the store or a circle sends them there as full inserts:
  the root, its descendants, and the ancestors they keep.
  - So other devices receive the whole subtree, not changes to rows they
    never had.
- Moving rows out of an audience is, for the devices that lose them, a
  delete.
  - The moving device keeps its rows, now on this device only.
  - A row still kept by another root stays.
  - Which rows leave is worked out against the database as it was before
    the write.
  - Moving them back later is a re-add (§8.3).
- Moving a row from one circle to another is a delete in the first and an
  insert in the second.
- An ancestor leaving other devices is never a delete: each device works
  out from the children it has whether the ancestor is present, by the
  ancestor rule of §8.
  - E.g. Ana unshares the last shared todo wearing the label "urgent",
    while Ben gives "urgent" to a new shared todo.
  - Ben's write carries the label too, so every device that has his todo
    has the label.
  - Every device ends with Ben's todo present, so every device has the
    label, whatever order the writes arrived in.

### 14.3 Circles

- A circle is a group of members inside a store who share rows the other
  members can't read.
- Circles are made, and members added to and removed from them, by entries
  in the store log, under its rules (§9).
  - A circle's own members add and remove its members.
- Each circle has its own key, sealed to each of its members' public keys,
  like the store key (§11).
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
      header, sealed with the store key:    ana-phone, write 12, timestamp, had read …
      part 1, sealed with the store key:    notes  row 47  insert "Paint colors"
      part 2, sealed with Ana's circle key: pins   row 4   insert note → 47
      signed with Ana's key, over all of the above
    ```

  - Ana's other devices apply both parts in one transaction; Ben's apply
    part 1, and can still check the signature, which covers part 2's
    encrypted bytes.
- A skipped part counts as applied, so a device never waits on a write it
  can't read (§7.1).
- Changes to rows on this device only are left out of the write record
  altogether, before it is signed and uploaded.
- All members can see that a circle exists, who writes to it, when, and how
  much.

### 14.5 References

- A row may only point at rows that everyone who can read it can also
  read.
  - A row on this device only may point at anything on the device.
  - A circle's row may point at rows in the same circle, or the store's.
  - A store row may point only at store rows.
- Coven refuses a write that breaks this, on the device making it.

  ```
  store          notes  row 42   "Groceries"
  Ana's circle   pins   row 3    note → 42        allowed
  store          tags   row 10   pin → 3          refused: Ben can't read pin 3
  Ben's circle   pins   row 2    pin → 3          refused: Ben can't read pin 3
  ```

- So every device that reads a row can check its foreign keys, and §8.4
  applies unchanged.
  - E.g. when note 42 is deleted, the devices in Ana's circle apply its
    foreign key's action to pin 3.
- Which circle a row is in never depends on who is in the circle, so a
  change of members never breaks a reference.

### 14.6 Leaving a circle

- E.g. Ana and Ben share a circle, and Ana removes Ben from it.
  - The circle key is replaced, sealed to Ana alone.
  - Ben keeps the circle's rows he already had, but can't read anything
    written to it afterwards.
- A write Ben made to the circle before he had read his removal still
  counts, as with any concurrent entry (§9).
  - Ben is still in the store, so storage access doesn't stop him writing
    with the old circle key.
  - Only trust keeps him from claiming he hadn't read his removal, and
    members are trusted not to be hostile (§2).

## 15. Snapshots

- A snapshot is the synced tables and coven's own tables as one device
  has them, encrypted, with how far into every log they reach.
  - Snapshots live at `snapshots/<device>/<n>`.
  - A snapshot *covers* a write when the write is within its positions:
    `snapshots/ana-phone/3` covers ana-phone's writes 1 to 40.
- A device writes one once the writes after the latest snapshot it knows of
  add up to more bytes than that snapshot.
  - So loading a snapshot and the writes after it costs at most about
    twice the snapshot.
- Two devices can write one at the same time, and both are correct:

  ```
  snapshots/ana-phone/3     ana-phone up to 40, ben-laptop up to 22
  snapshots/ben-laptop/1    ana-phone up to 38, ben-laptop up to 25
  ```

  - A new device loads either, then fetches every write after its
    positions, and ends up in the same place.
- A device snapshots only what it can read, and never rows on a device
  only.
  - Any device snapshots the store's rows.
  - A device of one of a circle's members snapshots that circle's rows,
    separately, sealed with the circle's key (§14).
- A new device loads the store's latest snapshot and its member's circles',
  then the writes after them.
- Each device posts its positions only after uploading its own earlier
  writes.
- A log object is deleted once snapshots cover every part of it, and either
  every device's posted position has passed it or storage has held it for
  30 days.
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
  - Its own writes still waiting in `coven_uploads` keep their numbers, and
    it uploads them after.
  - Every device then applies them like any late write: they had read only
    writes the snapshot covers, which count as applied.
  - E.g. Ana's old phone made writes 31 to 33 offline, then sat in a drawer
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
  - the file's kind, user-provided or app-provided;
  - whether devices download it as soon as its row arrives, or on first
    read.

### 16.1 Kinds and locality

- A *user-provided* file is the user's own file at a path on their device,
  such as a song in their music folder.
  - Coven records its path, size and modification time, and doesn't copy
    it.
  - Coven never changes or deletes it.
  - If it moves or changes, coven reports it as missing or changed, before
    reading any of it.
- An *app-provided* file is bytes the app hands to coven, which keeps and
  owns them.
  - The app can hand them over as a stream, so a large file never has to
    fit in memory.
- A file follows its row's audience (§14).
  - A row on this device only has a *local* file: the user's original, or
    coven's own copy of app-provided bytes.
  - A shared row has a *remote* file: uploaded, encrypted, and read the
    same way on every device.
- So user-provided or app-provided says where a local file's bytes are,
  and downloading as soon as the row arrives, or on first read, says how a
  remote file reaches each device.
- Moving a row into the store or a circle uploads its file first, then
  writes the move.
  - A user-provided original stays where it is.
  - From then on the file is remote, and every device reads it as remote,
    the one it came from included; coven never reads the original again.
- Moving a row back to this device only downloads its file first, then
  writes the move.
  - A user-provided file is written to a path the user picks, which must
    not already exist; an app-provided one goes back into coven's own
    folder.
  - The uploaded copy is then deleted like any unused file.

### 16.2 Storage and naming

- Each remote file is stored encrypted at `files/<name>`, where the name is
  a keyed hash of its content.
  - A keyed hash, such as HMAC-SHA256, can't be computed without a secret
    key, so the provider can't hash a file it knows and look for its name.
  - The key is a *naming key* derived from the store key, separate from the
    key that encrypts.
  - So identical files in the store share one copy.
- A plain hash of content is never part of anything the provider can see:
  not a path, a name, or metadata.
- A file attached to a circle's row is encrypted with the circle's key, and
  named with a naming key derived from it.
  - Only the circle's members can read it.
  - The same file in the store and in a circle gets two different names.
- After a key is replaced, a file added again gets a new name, and is
  stored again.
- A file is encrypted in chunks, 64 KiB by default, recorded in its
  header.
  - Each chunk is encrypted and authenticated on its own, with its index
    bound in, so a chunk can't be altered, swapped or reordered unnoticed.
  - So any chunk can be read and checked without the rest of the file.

### 16.3 Reading ranges

- The app reads any byte range of a file, at any offset, as a stream.
  - Playing a song and seeking in it are just reads of different ranges.
- A local file, or one in the cache, is read with positioned reads of the
  file on disk.
- A remote file not in the cache is read by fetching only the chunks that
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

- Each device caches remote files and chunks of them, within a size budget
  the app sets.
  - When the cache is over budget, the least recently used files and chunks
    go first.
  - Freeing space never fails a read.
- The app can pin a file to keep it whole on the device regardless of the
  budget, and unpin it.

### 16.5 Uploads and deletion

- A file waits in a local upload queue until it is stored.
- A large file goes up through the provider's resumable or multipart
  upload, in parts.
  - Providers require it above a size, such as Google Drive above 5 MB per
    request.
  - The upload session is recorded, so after a crash the upload continues
    from the last part stored, instead of starting over.
- A write that refers to a remote file is uploaded only after the file is
  stored.
  - So no device ever sees a row whose file isn't there yet.
- A file is deleted once nothing in the latest snapshot or the writes after
  it refers to it.
  - They are deleted by the same devices as logs (§15).
- Deleting a row deletes only coven's copies of its file, never a
  user-provided original.

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
- For each breaking change, the app supplies a *migration* in two parts:
  - one changes the database, e.g. `ALTER TABLE notes RENAME COLUMN title
    TO name`;
  - optionally, one changes a write made in the old version, e.g. turns
    "note 43, title: X" into "note 43, name: X".
- A breaking change raises the store's schema version, which every device
  shares.
  - Whichever device's app updates first makes it, whoever's device it is.
  - That device runs the migration's first part on its database, writes a
    snapshot in the new version (§15), and records the store's new version
    in the store log (§9).
  - A device whose app is older can't sync until it updates; it then
    reloads from that snapshot.
- A device that updates runs the migration's second part on its own
  writes still waiting in `coven_uploads`, then uploads them.
  - Without a second part, it uploads them marked lost, and every device
    records them in `coven_lost` without applying them.
  - E.g. Ana's app renames `title` to `name`, while Ben's phone, offline,
    edits a title; when Ben updates, his edit becomes a `name` edit, and
    reaches every device.
- Writes already uploaded in the old version that the breaking change
  hadn't read are lost: every device records them in `coven_lost`, and
  none applies them.
  - This happens only when a device uploads just as another makes the
    breaking change.
- If two devices make the same breaking change at once, the one with the
  smaller timestamp counts, and the other's snapshot is ignored.

### 17.2 Coven's schema

- Coven's own tables in the local database, such as `coven_rows`, are
  local only.
  - A newer coven migrates them in place when the app starts.
- What coven writes to storage has a *format*: write records, store log
  entries, snapshots, paths.
  - Every object records the format version it was written in.
- A format change works like a breaking change of the app's schema, with
  coven supplying both parts of the migration.
  - The first device with the newer coven writes a snapshot in the new
    format, and records the store's new format version in the store
    log (§9).
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
  2    reload from snapshot   1           snapshot: snapshots/ana-phone/7,    coven
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
  - Writing an object writes the same path with the same bytes.
  - Deleting an object that is already gone succeeds.
- Steps are ordered so other devices never see a half-done operation.
  - Anything another device reads, such as a store log entry, is uploaded
    last, after everything it refers to.
- When the app starts, coven resumes every unfinished operation from the
  step after its last completed one.
- A step that fails for good, rather than for lack of network, stops its
  operation, and sets its `failure`.
  - The failure goes to the app call that started it, or as an event to
    the app if coven started it.
  - The app can retry or abandon it.
- An operation's row is deleted when its last step completes.

### 18.1 Operations

- Removing a member (§12):
  1. make the new store key, and record it in the operation's row;
  2. upload the new key sealed to each remaining member;
  3. upload the store log entry removing the member;
  4. revoke the member's storage access.
- Removing someone from a circle (§14): the same, with the circle's key.
- A breaking schema or format change (§17):
  1. migrate the database, in one transaction;
  2. upload a snapshot in the new version;
  3. upload the store log entry raising the version.
- Reloading from a snapshot (§15):
  1. download the snapshot to a temporary file;
  2. replace the synced tables and coven's tables with it, in one
     transaction;
  3. migrate the writes waiting in `coven_uploads`, if the snapshot's
     version is newer (§17).
- Writing a snapshot, then deleting the log objects and files it lets go
  (§15, §16).
- Moving rows that carry files (§14, §16):
  1. upload each file, or download it, for a move back to this device;
  2. write the move.
- Pairing a new device (§13).
- Uploading a large file in parts (§16):
  1. start the provider's upload session, and record it in the
     operation's row;
  2. send each part, recording the last one stored;
  3. finish the session.
- Uploading a write, or a file small enough for one request, is not an
  operation: it waits in its queue until stored, and starts over if
  interrupted (§6, §16).

### 18.2 Example

- Ana removes Ben, and her phone crashes after uploading the sealed keys:

  ```
  coven_operations
    kind            last step   data              started by
    remove member   2           new store key     Ana's "remove Ben"

  storage
    keys/store/4/ana     uploaded
    keys/store/4/carol   uploaded
    store-log/ana-phone/9   not yet: the removal entry
  ```

- No other device sees anything yet: the sealed keys are unreferenced until
  the entry exists.
- When the phone restarts, coven resumes at step 3 with the recorded key.
- If it made a new key instead, the sealed copies already uploaded would
  hold a different one; recording it in step 1 is what prevents that.

## 19. Missing writes

- A device knows a write exists when any of these shows it:
  - a later write in the same log, since each log counts with no gaps;
  - another device's write that had read it;
  - another device's posted position reaching it;
  - for its own writes, its own count.
- A write is missing when one of these shows it, storage doesn't have it,
  and no snapshot covers it (§15).
  - E.g. Ben's write 9 had read Ana's log up to 4.
  - Storage shows Carol's tablet only Ana's writes 1 to 3.
  - Ana's write 4 is missing.
- A device holds back the missing write and every write that had read it.
  - Carol's tablet holds back Ben's write 9.
  - The app sees which writes it is waiting for.
- Coven doesn't re-upload or patch a missing write.
- How does a store recover from a missing write, or any other broken
  state?

## 20. API

- Signatures are abbreviated: parameters by name, and `async` left out.

### 20.1 Opening

```rust
Coven::builder(store_dir, config)          // config: a Config, or a closure re-read on each use
  .synced_tables(tables)
  .migrations(migrations)
  .coven_migration_policy(ApplyPending | RefusePending)
  .clock(clock)
  .oauth_clients(clients)
  .apply_cloudkit_ops(ops)
  .max_concurrent_uploads(n)
  .max_concurrent_downloads(n)
  .key_custody(custody)
  .identity_custody(custody)                // the member's keys (§11)
  .open() -> CovenHandle
  .open_read_only() -> CovenReadHandle
Coven::delete_store(store_dir)
handle.close()
```

### 20.2 Declaring synced tables

```rust
SyncedTable::new(name, RowIdentity::IndependentUuid | RowIdentity::SharedKey)
  .key_columns(columns)                     // keys may span columns (§8.5)
  .gated_by(column)                         // audience root: boolean, never NULL (§14)
  .scoped_by(column)                        // audience root: store, a circle, or device (§14)
  .remote_root()                            // audience root: always the store (§14)
  .gated_through(foreign_key)               // descendant (§14.1)
  .gated_by_descendants()                   // ancestor (§14.1)
  .asset()                                  // never keeps an ancestor (§14.1)
  .carries_blob(BlobDecl)

BlobDecl::new(namespace, Provenance::UserProvided | HostProvided,
              CacheFill::CacheEager | CacheLazy)
  .with_id_column(column)
  .with_size_column(column)
  .with_scope(scope)
  .write_once()
```

### 20.3 Writing and reading

```rust
handle.write(|sql| …) -> WriteReceipt<R>
handle.write_with_blobs(|batch| …, |sql| …)
  batch.put_blob(namespace, id, bytes | stream)
  batch.delete_blob(namespace, id)
sql.execute(…) / sql.query(…) / sql.query_row(…)
sql.insert_external_blob(…)
sql.register_external_blob(…)
sql.clear_external_blob(…)
sql.validate_row_blob_ref(…)
sql.set_audience(table, id, audience)     // inside the write that creates the rows
prepare_external_blob(path, on_consumed)

handle.read(|sql| …) -> Read
handle.subscribe(query) -> LiveQuery
handle.subscribe_reconfigurable(query) -> ReconfigurableLiveQuery
handle.lost_values() -> Vec<LostValue>                 // coven_lost (§8)
handle.subscribe_lost_values() -> LostValuesLiveQuery
```

### 20.4 Sync

```rust
handle.setup_s3_cloud_home(…) / setup_oauth_cloud_home(…) / setup_cloudkit_cloud_home()
handle.probe_cloud_home(…)
handle.unlock_cloud_home(…)
handle.cloud_home_key_state()
handle.disconnect_cloud_home()
handle.forget_master_key()
handle.connect_sync(…)                        // CloudKit operations come only from the builder
handle.start_sync() / stop_sync() / disconnect_sync()
handle.sync_now()
handle.subscribe_sync_status() -> SyncStatusLiveQuery  // its first result is the current status
handle.retry_blocked_operation(operation)     // coven_operations (§18)
handle.discard_blocked_operation(operation)
handle.subscribe_eager_cache_fill_status()
handle.cancel_eager_cache_fill()
handle.transfer_limits() / set_transfer_limits(limits)
handle.set_host_secret(…) / host_secret() / delete_host_secret()
handle.initialize_identity(…)
handle.seal_app_data(…) / open_app_data(…)
```

- The sync status says whether the device is connected and syncing, and
  names the writes it is waiting for (§19).

### 20.5 Audiences and uploads

```rust
handle.set_audience(table, id, audience, destination)  // store, a circle, or device (§14.2)
handle.set_audiences(moves)                              // several roots at once
handle.cancel_set_audience(table, id)

handle.subscribe_uploads() -> UploadsLiveQuery
  // each result: every queued upload with its phase, attempts, last
  // failure and live byte progress, and every audience move in progress;
  // the first result is the current state
handle.retry_uploads_now() -> RetryOutcome   // Drained | QueueEmpty | AllInBackoff | Paused
handle.set_uploads_paused(paused)
```

- `destination` is the path a user-provided file is written to when its
  row moves back to this device (§16.1); it is unused otherwise.
- Upload progress comes in the same results as the queue, so an app never
  joins two sources.

### 20.6 Files and the cache

```rust
handle.row_blob_ref(table, id) -> RowBlobRef
handle.read_blob(ref)
handle.open_blob_stream(ref) -> BlobStream     // read_at(offset, len), plaintext_size()
handle.materialize_row_blob(…)
handle.external_blob(…)
handle.pin(refs, on_progress) / unpin(refs) / is_pinned(ref)
handle.rows_pinned() / subscribe_rows_pinned()
handle.evict_blob(ref)
handle.set_cache_budget(…) / get_cache_budget()
```

- Reading an uncached range of a remote file while offline fails with
  `BlobCacheError::Offline` (§16).

### 20.7 Members, devices and restore

```rust
handle.get_members()
handle.remove_member(member)
handle.remove_device(device)                  // and how to sign it out (§12)
handle.start_device_pairing() / approve_device_pairing(…) / cancel_device_pairing()
join_with_device_pairing(…) -> PreparedDevicePairing
handle.generate_restore_code()
restore_from_code(code) / decode_restore_code_info(code)
restore_from_cloud(…)
```

- Roles are admin and member (§9).

### 20.8 Circles

```rust
handle.circles()
  .create(…) / rename(…) / delete(…)
  .add_member(…) / remove_member(…)
  .list() / members(…)
```

- A circle change with several steps, such as removing a member, is an
  operation like any other, retried or discarded with the calls of §20.4.

### 20.9 Migrations

```rust
Migration::sql(…) / Migration::run(…)
  .writes(…)        // the optional second part, for waiting writes (§17.1)
CovenMigrationPolicy::ApplyPending | RefusePending
```

## Appendix A. SQLite features

Each SQLite feature whose meaning changes when devices write offline and
merge later.

### A.1 Integer primary keys

- Problem: two offline devices can pick the same id for different rows,
  and coven would treat them as one row.
- Status: refused; synced tables use UUIDs or keys derived from the
  content (§8.5).

### A.2 Tables with no primary key

- Problem: coven can't tell which row a change belongs to, and SQLite's
  hidden rowid collides across devices like an integer key.
- Status: refused (§8.5).

### A.3 Unique constraints besides the primary key

- Problem: two offline devices can each insert a row with the same value.
- Status: the row whose write has the smaller timestamp keeps the value;
  the other row is taken out and held while it conflicts, and recorded in
  `coven_lost` (§8.5).

### A.4 Restrict and no-action foreign keys

- Problem: a device can delete a parent while another adds a child it
  hasn't seen.
- Status: the child is taken out and held while its parent is gone, and
  recorded in `coven_lost` (§8.4).

### A.5 CHECK constraints

- Problem: two concurrent edits that each pass can merge into a row that
  fails, such as one device setting `start` and another `end`.
- Status: a row that fails after a merge is taken out and held until it
  passes, and recorded in `coven_lost` (§8.6).

### A.6 Triggers that write synced tables

- Problem: a trigger that runs again while coven applies a remote write
  would repeat what the original device already sent.
- Status: allowed as shared triggers, which run only on the device making
  the write (§8.7).

### A.7 Schema changes

- Problem: devices running different app versions hold different schemas.
- Status: additions sync with no change of version; any other change
  raises the store's version, and every device updates and reloads (§17).

## Notes

1. Fork: show different devices different histories.
2. Roll back: serve an older version of what it stores.
