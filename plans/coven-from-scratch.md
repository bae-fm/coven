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
- Applying a write follows these rules, each of which gives the same
  result in any arrival order:
  - cells: of two values for one cell, the one with the larger timestamp
    stays;
  - deletes: a row change concurrent with a delete of its row loses;
  - foreign keys: a row pointing at a deleted row follows its key's action,
    even if the deleting device never saw it;
    - under restrict, the row is lost;
  - key changes: a row pointing at a changed key follows its key's
    `ON UPDATE` action;
    - of two changes to one key, the one with the larger timestamp decides
      where those rows go;
  - unique values: of two rows claiming one value, the row whose write has
    the smaller timestamp keeps it;
    - the first claim to a value keeps it, and the other row is lost;
  - CHECK constraints: a row that fails after a merge is lost.
- A lost row is deleted, like any delete, and whatever loses is recorded,
  so the app can show it and offer it back.
- The result is the one applying every write in timestamp order would
  give.
- One rule's result can break another, so coven applies them until no row
  breaks any.
  - E.g. a note that loses a unique title is deleted, and its tags then
    follow their foreign key's action.
  - This always stops: each step only deletes rows.
- When a write arrives after another and changes the result for a row
  already applied, coven makes the change with ordinary SQL, which
  triggers see like any other.
  - E.g. a write that wins a unique value from a row already applied makes
    coven delete that row.
- Coven keeps five internal tables for this:
  - `coven_writes`, one row per write the device has applied, naming:
    - the write's timestamp, which includes its device;
    - the write's number.
  - `coven_columns`, one row per synced column, naming:
    - its table;
    - its column.
  - `coven_rows`, one row per synced row, naming:
    - its table;
    - its primary key;
    - its generation: how many times it has been created, deleted or
      re-added;
    - the write that last changed its generation.
  - `coven_cells`, one row per synced cell, naming:
    - its `coven_columns` row;
    - its `coven_rows` row;
    - the write that set it.
  - `coven_lost`, one row per value a concurrent write replaced or
    deleted, naming:
    - the cell;
    - the value that lost;
    - the write that set it;
    - the write that replaced it without having read it.
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
- When concurrent writes set the same cell, the newer one wins, and coven
  records the value that lost in `coven_lost`.
  - `coven_lost` keeps each value that lost, with the write that set it and
    the write that replaced it.
  - The writes were concurrent when the winning write had not read the
    write that set the losing value.
- Carol's "Shopping" replaced Ben's "Weekly groceries":

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
  - It lives on the row's `coven_rows` row, with the write that last
    changed it.
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
- Its `coven_rows` row and `coven_cells` rows stay while it is deleted, so
  later writes to it still have something to compare against.
- A row change concurrent with a delete of its row loses, and its cells go
  to `coven_lost`, replaced by the delete.
  - If the change arrives after the delete, coven sees it was made at an
    older generation.
  - If it arrives first, the delete finds the cells set by writes it
    hadn't read.
  - So a concurrent edit never brings the row back, and never reaches it
    once it is re-added.
- E.g. at 16:00 Ana deletes note 43, "Hardware store", while Ben, offline,
  edits its title, and at 17:00 Ana re-adds it.
  - Note 43 on Carol's tablet:

    ```
    coven_rows
      time    id   table   key   generation   write
      14:45   4    notes   43    1            16      Ana's write 5 creates it
      16:00   4    notes   43    2            18      Ana's write 7 deletes it
      17:00   4    notes   43    3            19      Ana's write 8 re-adds it

    notes
      14:45   row 43 present
      16:00   row 43 gone
      17:00   row 43 present again
    ```

  - Ben's edit was made at generation 1, so it loses whenever it arrives,
    even if a fast clock stamps it after 17:00.
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

- A row pointing at a deleted row follows its foreign key's declared action
  on every device.
- A row change that points at a parent also carries the parent's
  generation (§8.3).
  - If the parent has moved to a newer generation, the child is handled as
    pointing at a deleted parent, even if the parent was re-added since.
  - E.g. Ben tags note 43 at its generation 1, while Ana deletes it and
    then re-adds it: Ben's tag still follows the delete.
- On the device that deletes the parent, SQLite runs the action, and the
  write records it.
  - E.g. if tags point at notes with cascade and links with set null,
    deleting note 43 records:

    ```
    notes  row 43  delete
    tags   row 8   delete                         cascade
    links  row 5   update  note_id: 43 → null     set null
    ```

- When another device applies the parent's delete, each child is in one of
  three cases:
  - Case 1: the deleting device had the child;
  - Case 2: the child's own device has it when the delete arrives;
  - Case 3: the delete arrives before the child.

#### Cascade

- Case 1: Ana's phone, which deletes note 43, had the child, tag 8.
  - At 15:30 Carol tags note 43 "hardware", as tag 8. Every device, Ana's
    phone included, applies it.
  - At 16:00 Ana's phone deletes note 43, SQLite cascades tag 8, and her
    write record carries both:

    ```
    ana-phone, write 7, 2026-10-02 16:00:00.000 #0
      had read: ben-phone 9, carol-tablet 3
      notes  row 43  generation 1  delete
      tags   row 8   generation 1  delete
      links  row 5   generation 1  update  note_id: 43 → null
    signed with Ana's key
    ```

  - Carol's tablet applies write 7 like any other write: note 43 and tag 8
    go, and link 5 loses its note.
- Case 2: Ana's phone never saw the child, tag 9, but Ben's phone, which
  created it, has it when Ana's delete arrives.

  ```
  Ben's phone
    16:00  offline. Ben's write: insert tag 9 → note 43
    16:30  online. Applies Ana's write 7: delete note 43
             SQLite cascades: tag 9 deleted
             coven_rows  tags  9  generation 2  write: Ana's write 7
  ```

  - Ben's phone applies write 7 like any other write, and SQLite's cascade
    removes tag 9 with note 43. Coven records tag 9's delete as part of
    write 7.
- Case 3: Carol's tablet applies Ana's delete before tag 9 arrives.

  ```
  Carol's tablet
    16:00  applies Ana's write 7: delete note 43
             coven_rows  notes  43  generation 2  write: Ana's write 7
    16:30  Ben's write arrives: insert tag 9 → note 43
             note 43 is deleted in coven_rows, so cascade applies
             tag 9 is never inserted into tags
             coven_rows  tags  9  generation 2  write: Ana's write 7
  ```

  - SQLite would reject inserting a tag that points at a missing note.
  - So coven never runs Ben's insert into `tags`.
  - It only adds tag 9's `coven_rows` row, deleted, set by Ana's write 7.
- Every device ends with note 43, tag 8 and tag 9 deleted.
- Ana's write 7 hadn't read Ben's insert, so tag 9's cells are recorded in
  `coven_lost`, replaced by write 7 (§8.3).
- Re-adding a deleted parent doesn't bring back the rows deleted with it.
  - Re-adding is a new insert. Any children the app wants back, it inserts
    in the same write.
  - At 17:00 Ana's write 8 re-inserts note 43 and tag 8.
  - Ana's phone never had tag 9, so it can't re-insert it.

#### Set null and set default

- Set default works like set null, with the column's default in place of
  null.
- If the deleting device had the child, SQLite there sets the child's
  reference to null, and the write records it.
- If the deleting device didn't have the child, coven records the null as
  set by the parent's delete, and the child's old reference in
  `coven_lost`.
  - If another device has the child when the delete arrives, SQLite there
    sets its reference to null.
  - If the delete arrives before the child, coven inserts the child with
    its reference null.
- E.g. at 16:00 Ana deletes note 43, which link 5 points at, while Ben,
  offline, adds link 6 pointing at it.

  ```
  Ana's phone
    16:00  deletes note 43
             SQLite sets link 5's note_id to null
             write 7 records: links row 5 update note_id: 43 → null

  Ben's phone
    16:00  offline. Ben's write: insert link 6 → note 43
    16:30  online. Applies Ana's write 7: delete note 43
             SQLite sets link 6's note_id to null
             coven_cells  link 6 note_id  write: Ana's write 7

  Carol's tablet
    16:00  applies Ana's write 7: delete note 43
    16:30  Ben's write arrives: insert link 6 → note 43
             note 43 is deleted, so coven inserts link 6 with note_id null
             coven_cells  link 6 note_id  write: Ana's write 7
  ```

- Every device ends with links 5 and 6 present and pointing at nothing,
  and with the same `coven_lost` row:

  ```
  coven_lost
    cell           lost value   set by        replaced by
    link 6 note_id 43           Ben's write   Ana's write 7
  ```

#### Restrict and no action

- `RESTRICT` and `NO ACTION` both refuse to delete a parent that has
  children.
- If the deleting device had the child, SQLite refuses the delete there.
- If the deleting device didn't have the child, the child is lost, as if
  it had never been added, and coven records its insert in `coven_lost`,
  replaced by the parent's delete.
  - If another device has the child when the delete arrives, SQLite there
    would refuse the delete, so coven removes the child first.
  - If the delete arrives before the child, coven never inserts the child.
- E.g. at 16:00 Ana deletes note 43, while Ben, offline, adds tag 9
  "receipts" to it.

  ```
  Ben's phone
    16:00  offline. Ben's write: insert tag 9 → note 43
    16:30  online. Applies Ana's write 7: delete note 43
             tag 9 would block the delete, so coven removes it first
             coven_rows  tags  9  generation 2  write: Ana's write 7

  Carol's tablet
    16:00  applies Ana's write 7: delete note 43
    16:30  Ben's write arrives: insert tag 9 → note 43
             note 43 is deleted, so tag 9 is never inserted into tags
             coven_rows  tags  9  generation 2  write: Ana's write 7
  ```

- Both devices end without tag 9, and with the same `coven_lost` rows:

  ```
  coven_lost
    cell           lost value   set by        replaced by
    tag 9 name     "receipts"   Ben's write   Ana's write 7
    tag 9 note_id  43           Ben's write   Ana's write 7
  ```

- The app can show Ben that his tag was lost, and offer to add it to
  another note.

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
- If it didn't have the child, coven runs the action itself, and records
  the child's old reference in `coven_lost`.
  - Cascade re-points the child to the new key.
  - Set null and set default clear the reference, as in §8.4.
  - Restrict and no action lose the child, as in §8.4.
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
             coven changes the key in place, so SQLite re-points note 44

  Carol's tablet
    16:00  applies Ana's write
    16:30  Ben's write arrives: note 44 tag → "urgent"
             "urgent" was replaced, so coven sets note 44's tag to "important"
  ```

- Every device ends with notes 42 and 44 tagged "important".
- Two devices can change one key to different new keys concurrently.
  - E.g. Ana renames "urgent" to "important" while Ben, offline, renames
    it to "critical".
  - Both new tags exist after the merge.
  - A child still pointing at "urgent" follows the change with the larger
    timestamp.
  - As with any child the changing device didn't have, coven records its
    old reference, "urgent", in `coven_lost`, replaced by that change.

#### Unique constraints

- On one device, SQLite refuses a write that repeats a unique value, so two
  rows can claim one value only through concurrent writes.
- Of two rows claiming one value, the row whose write has the smaller
  timestamp keeps it, since the first claim to a value keeps it.
- The other row is lost, whether its write inserted it or edited it to
  claim the value.
  - Coven deletes it, whichever row arrived first.
  - Coven records its cells in `coven_lost`, replaced by the winning
    write.
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

- A row that fails after a merge is lost.
  - Coven deletes it, whichever write arrived first.
  - Coven records its cells in `coven_lost`, each replaced by the other
    conflicting write.
- Until the second write arrives, each device's row passes, since it has
  seen only one of them.
- Every device ends without the row, and records Ana's 10 and Ben's 8 in
  `coven_lost`.

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
  - Each member has their own key pair, which identifies them.
  - An admin adds a member by writing their public key to the membership
    log.
- Membership is its own log, separate from the app's writes.
  - Each change is one *entry*: add or remove a member, change a role, add
    or remove a device.
  - An entry names the membership entries its author had read, and is
    signed with its author's member key.
  - Entries live at `membership/<device>/<n>`, numbered like a device's
    writes (§6).
  - The device in the path is only where the entry was written from.
  - Each device numbers its own entries, so no two entries ever get the
    same path.
  - Whose entry it is comes from its signature: an entry signed with Ana's
    key is Ana's, whichever of her devices wrote it.

    ```
    membership/ana-phone/1     create the store, Ana as admin       signed with Ana's key
    membership/ana-phone/2     add member Ben, with his public key  signed with Ana's key
                               had read: ana-phone 1
    membership/ben-laptop/1    add device ben-phone                 signed with Ben's key
                               had read: ana-phone 2
    membership/ana-ipad/1      make Ben an admin                    signed with Ana's key
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
  - Its member adds it to the membership log (§9).
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
  the membership log has added, and comes from one of that member's
  devices (§9).
  - So a write by Ana's phone counts as Ana's.
- A removed device's writes still count if they reached storage.
  - Removing a device takes away its storage access, so nothing it writes
    afterwards can reach other devices.

## 11. Keys

- Coven uses three kinds of key:
  - the store key, which encrypts everything coven writes to storage;
  - each member's key pair, which identifies them (§9), signs their writes
    and membership entries, and opens the store key;
  - storage credentials: each device's own sign-in to the provider, or on
    S3 its member's access key (§4).
- Each store key is sealed to every member's public key, and the sealed
  copies are kept in storage.
  - So a member's key alone gets the current store key: a device holding
    it reads its member's sealed copy from storage and opens it.
- The store key is replaced whenever a member is removed (§12).
  - Writes made after that use the new key.
  - Devices keep the old keys, to read writes made before.
- Each device keeps its member's key in the OS keychain.
- Storage access, not keys, is what keeps a removed device out.

## 12. Removing members and devices

- Removing a device, such as Ana's lost phone, is an entry in the
  membership log (§9), and cuts the device off from storage.
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

## 13. Joining, restore, and not losing the store key

- A person's new device needs their member key, which gets it the store key
  (§11), in one of three ways:
  - pairing: one of their existing devices hands it over, with storage
    access, over the local network;
  - on Apple platforms, iCloud Keychain syncs it, and the device opens the
    store without pairing;
  - on other platforms, the person enters their restore code.
- The new device then adds itself to the membership log, signing with the
  member key (§9).
- A restore code holds the person's member key and storage credentials.
  - The person writes it down when they create or join a store, as part of
    setup.
- An admin re-invites a person who lost every device and their restore
  code.
- Losing every member's devices and restore codes loses the store for good,
  since everything is encrypted.

## 14. Circles

- A circle is a group of members inside a store who share rows the other
  members can't read.
- Each circle has its own key, sealed to each circle member's public key,
  like the store key (§11).
- All members can see that a circle exists, who writes to it, when, and how
  much.
- A circle's writes go in the same device logs, encrypted
  with its key.
- Devices outside a circle skip what they can't open.
- Leaving a circle rotates the circle key.
- What happens when a row references a row in another circle?

## 15. Snapshots and bounded history

- Any device writes a snapshot: the synced tables and coven's own tables,
  encrypted, and how far into every log they reach.
- A new device loads the latest snapshot, then the logs after it.
- Each device posts its positions only after uploading its own earlier
  writes.
- A log object is deleted once a snapshot covers it and every device's
  posted position has passed it.
- A deleted row's `coven_rows` and `coven_cells` rows are cleared once
  every device's posted position has passed the delete, and this device
  has applied every log up to those positions.
  - Until then, a write made at the row's old generation could still
    arrive, and needs the generation to lose to.
  - Any such write was made before its device applied the delete, so it is
    within that device's posted position, and already applied.
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

## 19. Missing writes

- A device knows a write exists when any of these shows it:
  - a later write in the same log, since each log counts with no gaps;
  - another device's write whose "had read" covers it;
  - another device's posted position covering it;
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

## 20. The API apps use

- Open, write, read, subscribed queries, files, membership, circles,
  pairing, sync status.

## Appendix A. SQLite features across devices

Each SQLite feature whose meaning changes when devices write offline and
merge later.

### Integer primary keys

- Problem: two offline devices can pick the same id for different rows,
  and coven would treat them as one row.
- Status: refused; synced tables use UUIDs or keys derived from the
  content (§8.5).

### Tables with no primary key

- Problem: coven can't tell which row a change belongs to, and SQLite's
  hidden rowid collides across devices like an integer key.
- Status: refused (§8.5).

### Unique constraints besides the primary key

- Problem: two offline devices can each insert a row with the same value.
- Status: the row whose write has the smaller timestamp keeps the value;
  the other row is lost, and recorded in `coven_lost` (§8.5).

### Restrict and no-action foreign keys

- Problem: a device can delete a parent while another adds a child it
  hasn't seen.
- Status: the child is lost, recorded in `coven_lost` as replaced by the
  parent's delete (§8.4).

### CHECK constraints

- Problem: two concurrent edits that each pass can merge into a row that
  fails, such as one device setting `start` and another `end`.
- Status: a row that fails after a merge is lost, and recorded in
  `coven_lost` (§8.6).

### Triggers that write synced tables

- Problem: a trigger that runs again while coven applies a remote write
  would repeat what the original device already sent.
- Status: allowed as shared triggers, which run only on the device making
  the write (§8.7).

### Schema changes

- Problem: devices running different app versions hold different schemas.
- Status: synced schema changes only add tables and columns (§18).
- Could support: dropping, renaming and retyping with a conversion per
  schema version.

## Notes

1. Fork: show different devices different histories.
2. Roll back: serve an older version of what it stores.
