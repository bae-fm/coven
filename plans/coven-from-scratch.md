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
  - it doesn't alter or withhold what it stores;
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

- A store lives on one provider: S3, Google Drive, Dropbox, OneDrive, or
  iCloud through CloudKit.
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
  - Each object is one write from that device: its write record ([§5](#5-local-database)),
    encrypted.
  - It is named `devices/<device>/<n>`, created once and never changed.
  - `<n>` counts that device's own writes: 1, 2, 3, with no gaps.
  - Its name is part of its encryption, so the provider can't swap one
    object for another.
  - A retried upload writes the same name with the same bytes.
- A write record leaves `coven_uploads` ([§5](#5-local-database)) once its upload succeeds.
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
  ([§7.1](#71-causality)).
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
  - CHECK constraints: a row whose merged values fail is taken out ([§8.6](#86-check-constraints));
  - ancestors: an ancestor that no present shared row the device can read
    points at is taken out ([§14](#14-audiences));
  - unique values: of two rows claiming one value, the row whose write has
    the larger timestamp is taken out, since the first claim keeps it
    ([§8.5](#85-keys-and-uniqueness)).
- A removal is never stored as a delete.
  - A removed row is recorded as lost while it is out, and its `coven_lost`
    row keeps its values, which later edits to it update.
  - Coven puts it back from there when the reason goes away, e.g. when the
    reference that made it a parent's child is pointed elsewhere.
- A removed row's `coven_lost` row names every rule that holds for it once
  the rules have run, and it comes back only when none holds.
  - E.g. todos need `start <= end`, and todo 7 is in list 3.
  - Ana deletes list 3, while Ben moves todo 7's start past its end.
  - Todo 7 is held for both reasons, on every device, whichever rule a
    device ran first.
- The app can't see a removed row, so it can insert the same shared key
  again; the write records that insert as an update of the removed row,
  setting every column, and the row comes back if the new values clear its
  reasons, as a local insert's always do.
- Every rule but the unique one keeps firing when more rows are removed,
  so applying them in any order ends with the same rows removed.
- The unique rule is judged once, between two passes of the others:
  1. apply the other rules until none fires;
  2. judge unique values among the rows still present;
  3. apply the other rules again, to the losers' children and ancestors.
- So every device that applied the same writes sees the same rows, whatever
  order the writes arrived in, and whatever order the rules ran in.
  - The proof is in [Appendix B](#appendix-b-proof-of-convergence),
    checked by machine for the merged state and the removal rules.
- When a write changes which rows are removed, coven makes the change in
  the app's table with ordinary SQL, which triggers see like any other.
- Coven keeps five internal tables for this:
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
    - the value that lost, or every value of the removed row;
    - the write that set it;
    - the write that replaced it without having read it, or the rules that
      removed the row.
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
  - a cell both set goes to the write with the larger timestamp ([§8.1](#81-example));
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
  ([§8](#8-merge)).
- In [§8.1](#81-example), note 42's title went through four values:

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
  if neither had read it, by the rule of [§8](#8-merge).
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
  ([§8.2](#82-concurrent-writes-to-one-row)).
- Ana's write 6, between 14:45 and 16:00, is her 15:00 edit to note 42's
  body ([§8.2](#82-concurrent-writes-to-one-row)), applied as row 17.

### 8.4 Foreign keys

- A foreign key makes one row point at another, its parent.
  - E.g. tag 9's `note_id` points at note 43, so note 43 is tag 9's parent.
- Every device applies a parent's insert before the row pointing at it,
  through causality alone ([§7.1](#71-causality)):

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
  generation ([§8.3](#83-deletes)).
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
  at the parent concurrently, points at a generation that is deleted, on
  every device:
  - under cascade, restrict or no action, the removal rule of [§8](#8-merge)
    takes it out of the app's table and records it as lost;
  - under set null or set default, it stays, and its reference is null or
    the default, as SQLite would have made it.
  - The reference is null or the default wherever coven keeps it, in the
    app's table and in `coven_lost`, so every device stores the same value
    whichever write arrived first.
- Where SQLite would refuse the null or the default, such as on a `NOT NULL`
  column, the child is taken out as under restrict.
- A child whose parent is taken out by a rule, rather than deleted, is
  taken out with it under every action, and comes back with it.
- Coven refuses set null and set default on a primary key column, checked
  whenever the schema changes, since the change would be a key change no
  write made.
- A taken-out child's own generation never moves, so it comes back if its
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
    ([§8.2](#82-concurrent-writes-to-one-row)).
- With shared keys, a device can insert a row another device is deleting.
  - E.g. Ana deletes the tag "urgent" while Ben, offline, adds "urgent"
    again.
  - Ben's insert was made at the generation before Ana's delete, so it
    loses ([§8.3](#83-deletes)), and his cells go to `coven_lost`.

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
             "urgent" is deleted, so (44, "urgent") is held

  Carol's tablet
    16:00  applies Ana's write
    16:30  Ben's write arrives: insert note_tags (44, "urgent")
             "urgent" is deleted, so (44, "urgent") is held
  ```

  - Every device ends with note 42 tagged "important", and note 44's
    "urgent" tag held and recorded in `coven_lost`, so the app can offer
    to tag it again.
- Two devices changing one key concurrently are two concurrent deletes of
  its generation ([§8.3](#83-deletes)), and both new rows exist after the
  merge.

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
- A loser comes back when the winner is deleted, or taken out for a reason
  other than the loser itself.
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
  - Each member has their own keys, which identify them ([§11](#11-keys)).
  - An admin adds a member by writing their public key to the store log.
- The *store log* records changes to the store itself, separate from the
  app's writes.
  - Each change is one *entry*: add or remove a member, change a role, add
    or remove a device, raise the store's schema or format version, or
    reset the store or a circle to a snapshot.
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
- Concurrent entries, and what applies:

  ```
  Ana adds Dan               Ben adds Eve               both: no conflict
  Ana adds Ben's new phone   Ben removes it             the removal: less access
  Ana makes Ben an admin     Carol makes him a member   member: less access
  Ana removes Ben            Ben removes Ana            the earlier: a tie
  ```

  - In the last, both admins removing each other would leave no admin, so
    the entries conflict, and neither gives less access than the other.

## 10. Device identity

- A device is one install of the app, with its own device id, belonging to
  one member.
  - Its member adds it to the store log ([§9](#9-members-and-roles)).
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
  devices ([§9](#9-members-and-roles)).
  - So a write by Ana's phone counts as Ana's.
- A removed device's writes still count if they reached storage.
  - Removing a device takes away its storage access, so nothing it writes
    afterwards can reach other devices.

## 11. Keys

- Coven uses three kinds of key:
  - the store key, which encrypts everything coven writes to storage;
  - each member's keys, which identify them ([§9](#9-members-and-roles)), sign their writes and
    store log entries, and open the store key;
  - storage credentials: each device's own sign-in to the provider, or on
    S3 its member's access key ([§4](#4-storage-providers-and-access)).
- Each store key is sealed to every member's public key, and the sealed
  copies are kept in storage, at `keys/store/<n>/<member>` for the store's
  `n`th key.
  - So a member's key alone gets the current store key: a device holding
    it reads its member's sealed copy from storage and opens it.
- The store key is replaced whenever a member is removed.
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
  nonces, fingerprints.
- A file's storage name is HMAC-SHA256 of its content, with the naming key.

## 12. Joining and restore

- A person's new device needs their member key, which gets it the store key
  ([§11](#11-keys)), in one of three ways:
  - pairing: one of their existing devices hands it over, with storage
    access, over the local network;
  - on Apple platforms, iCloud Keychain syncs it, and the device opens the
    store without pairing;
  - on other platforms, the person enters their restore code.
- A restore code holds the person's member key, the store's id and name,
  and storage credentials.
  - The person writes it down when they create or join a store, as part of
    setup.
  - Where storage needs a sign-in, such as Google Drive, the device signs
    in first.
- On Apple platforms, the iCloud Keychain item holds exactly what a restore
  code holds, so a new device on the same Apple account finds the store
  and opens it the same way.
- The new device then adds itself to the store log, signing with the
  member key ([§9](#9-members-and-roles)).
- An admin re-invites a person who lost every device and their restore
  code.
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
  - S3: one of her devices replaces her access key, her other devices get
    the new one by pairing, and she writes down a new restore code.
- No keys change: the phone still holds Ana's key, but can't reach storage
  to read or write anything new.
- Removing a member removes them and all their devices, and then:
  - their storage access is revoked ([§4](#4-storage-providers-and-access));
  - the store key is rotated: a new one, sealed to each remaining member's
    public key ([§11](#11-keys)).
- So a removed member's copy of the old store key reads nothing written
  after the removal, even if they regain read access.
- A member added concurrently with a rotation doesn't get the new key.
  - Their devices can't read anything written after the rotation.
  - Removing them and adding them again gives them the current key.

## 14. Audiences

- Every synced row has an *audience*: the store, one circle, or this device
  only.
  - A row in the store reaches every member's devices.
  - A row in a circle reaches only that circle's members' devices.
  - A row on this device only never leaves it, though its table syncs.
  - E.g. a household's notes are the store's, each person pins notes in a
    circle of their own, and a draft stays on the device that wrote it.
- A row is *shared* when its audience is the store or a circle.
- The app declares, for each synced table, how its rows get their audience:
  - a *root* table names a column holding each row's audience: `store`, a
    circle's id, or `device`, and never NULL;
  - a *descendant* table names one foreign key, and each row takes the
    audience of the row it points at;
  - an *ancestor* table has no audience of its own: a row is in the store
    while a present shared row points at it, and a device shows it only
    while a present shared row that device can read points at it;
  - a table that declares none of these is in the store.
- Audience is defined once, from these declarations, and everything that
  decides where a row goes uses that one definition: writes, snapshots and
  files.

### 14.1 Roots, descendants and ancestors

- A descendant's declared foreign key is one column, into a synced table.
  - Its action can't be set null or set default, since the row would lose
    its audience; coven refuses it when the database opens.
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
- Devices that read different circles can differ on whether they show an
  ancestor.
  - E.g. a label worn only by a todo in Ana's circle is in the store.
  - Ana's device shows it.
  - Dan, outside the circle, can't see that todo, so his device holds the
    label.
- A unique constraint or shared key ([§8.5](#85-keys-and-uniqueness)) must not span audiences, since a
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
  - Moving them back later is a re-add ([§8.3](#83-deletes)).
- Moving a row from one circle to another is a delete in the first and an
  insert in the second.
- A row's generations ([§8.3](#83-deletes)) are counted per audience, so
  `coven_rows` has one row per table, key, audience and generation.
  - A move ends the row in one audience and starts it in the other.
  - E.g. Ana moves note 1 into her circle, while Ben, outside it, re-adds
    note 1 in the store.
  - The store's note 1 and the circle's note 1 are two rows, each with its
    own generations, and Carol's edit in the circle changes only the
    circle's.
- When one key is present in two audiences on a device, the app's table
  shows one of them, and the other is removed like a unique value's loser
  ([§8](#8-merge)):
  - the store's row wins over a circle's;
  - of two circles' rows, the one whose insert has the smaller timestamp
    wins.
  - A device in both circles can show a different row for that key than a
    device in one, since each reads different rows.
- An ancestor leaving other devices is never a delete: each device works
  out from the children it has whether the ancestor is present, by the
  ancestor rule of [§8](#8-merge).
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
  in the store log, under its rules ([§9](#9-members-and-roles)).
  - A circle's own members add and remove its members.
- Each circle has its own key, sealed to each of its members' public keys,
  like the store key ([§11](#11-keys)).
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
  can't read ([§7.1](#71-causality)).
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

- So every device that reads a row can check its foreign keys, and [§8.4](#84-foreign-keys)
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
  counts, as with any concurrent entry ([§9](#9-members-and-roles)).
  - Ben is still in the store, so storage access doesn't stop him writing
    with the old circle key.
  - Only trust keeps him from claiming he hadn't read his removal, and
    members are trusted not to be hostile ([§2](#2-threat-model)).

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
    separately, sealed with the circle's key ([§14](#14-audiences)).
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
  - which column holds the file's *content hash*, the SHA-256 of its
    bytes, which coven fills in when a write attaches the file;
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
- A file follows its row's audience ([§14](#14-audiences)).
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

- The content hash travels in the row, inside encrypted writes, like any
  other column.
  - Every device checks a downloaded file against it.
- Each remote file is stored encrypted at `files/<name>`, where the name is
  a keyed hash of its content hash.
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
  - They are deleted by the same devices as logs ([§15](#15-snapshots)).
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
- Coven tells which a migration is by comparing the schema before and
  after it, as SQLite lists it.
  - Only new tables or new columns: an addition.
  - Anything else, such as a new index, constraint or foreign key: a
    breaking change.
- For each breaking change, the app supplies a *migration* in two parts:
  - one changes the database, e.g. `ALTER TABLE notes RENAME COLUMN title
    TO name`;
  - optionally, one changes a write made in the old version, e.g. turns
    "note 43, title: X" into "note 43, name: X".
- A breaking change raises the store's schema version, which every device
  shares.
  - Whichever device's app updates first makes it, whoever's device it is.
  - That device runs the migration's first part on its database, writes a
    snapshot in the new version ([§15](#15-snapshots)), and records the store's new version
    in the store log ([§9](#9-members-and-roles)).
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

- Removing a member ([§13](#13-removing-members-and-devices)):
  1. make the new store key, and record it in the operation's row;
  2. upload the new key sealed to each remaining member;
  3. upload the store log entry removing the member;
  4. revoke the member's storage access.
- Removing someone from a circle ([§14](#14-audiences)): the same, with the circle's key.
- A breaking schema or format change ([§17](#17-schema-changes)):
  1. migrate the database, in one transaction;
  2. upload a snapshot in the new version;
  3. upload the store log entry raising the version.
- Reloading from a snapshot ([§15](#15-snapshots)):
  1. download the snapshot to a temporary file;
  2. replace the synced tables and coven's tables with it, in one
     transaction;
  3. migrate the writes waiting in `coven_uploads`, if the snapshot's
     version is newer ([§17](#17-schema-changes)).
- Writing a snapshot, then deleting the log objects and files it lets go
  ([§15](#15-snapshots), [§16](#16-files)).
- Moving rows that carry files ([§14](#14-audiences), [§16](#16-files)):
  1. upload each file, or download it, for a move back to this device;
  2. write the move.
- Pairing a new device ([§12](#12-joining-and-restore)).
- Uploading a large file in parts ([§16](#16-files)):
  1. start the provider's upload session, and record it in the
     operation's row;
  2. send each part, recording the last one stored;
  3. finish the session.
- Uploading a write, or a file small enough for one request, is not an
  operation: it waits in its queue until stored, and starts over if
  interrupted ([§6](#6-syncing-writes), [§16](#16-files)).

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

## 19. Recovery

- Recovery is for any state coven's rules didn't produce: a write that
  never arrives, an object that is damaged, a database a disk or a bug
  damaged, or devices holding different data after the same writes.
- The last is the one nothing else would catch, since every rule would
  still run; it is what a bug in coven or the app looks like.

### 19.1 Noticing

- A write that never arrives:
  - a device waits for every write that a write it has had read ([§7.1](#71-causality));
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
  signature doesn't match, or it doesn't parse.
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
  - and nothing that differs between devices by design, such as
    `coven_uploads`, `coven_operations`, local row ids, or whether an
    ancestor or a key present in two audiences is shown or recorded as lost,
    which depends on
    the circles a device reads ([§14](#14-audiences));
  - it is keyed with a key derived from the store's or circle's key, so the
    provider learns nothing from it;
  - each device posts its fingerprints with its positions ([§6](#6-syncing-writes));
  - two devices that have applied exactly the same writes must have the
    same fingerprints;
  - so whenever two devices are at the same positions, as they usually are
    once a store is quiet, each compares, and a mismatch means a bug made
    one of them wrong.
- The app sees each of these, and which devices are involved.

### 19.2 Recovering one device

- When only one device is broken, such as a damaged database, it reloads
  from the latest snapshot ([§15](#15-snapshots)).
- Its own writes still waiting in `coven_uploads` are uploaded after, and
  merge like any late write.

### 19.3 Resetting a store

- When the problem is shared, or nobody can tell which device is right,
  the person picks the device they trust, and the store is reset from it.
  - E.g. "Ana's phone and Ben's laptop disagree about the store; reset it
    from this device?"
- An admin's device resets the store's rows; a member of a circle resets
  that circle's rows, one reset per key.
- A reset is an operation ([§18](#18-operations)):
  1. write a snapshot of what this device has ([§15](#15-snapshots));
  2. record in the store log that the store, or the circle, is reset to
     that snapshot ([§9](#9-members-and-roles)).
- Every other device reloads from that snapshot when it sees the entry.
- A write the reset snapshot doesn't cover is then judged by what it had
  read:
  - if it had read a write the snapshot doesn't include, its cause is gone,
    so it is recorded as lost on every device, and never applied;
  - otherwise it merges like any late write.
- If two devices reset at once, the one with the smaller timestamp counts,
  and the other's snapshot is ignored.

## 20. API

- The API is listed as Rust declarations with their doc comments.
- Long parameter lists are abbreviated: `/* … */` marks parameters left out,
  and the comment beside it names them.
- Calls on `handle` are on the `CovenHandle` that opening a store returns.
- Every call that reaches storage or the database is `async`.
- A row is named by its table and its *key*: the values of its primary key
  columns, in order ([§8.5](#85-keys-and-uniqueness)).

```rust
/// A row's primary key: one value per key column, in the order the table
/// declares them (§8.5).
pub struct RowKey(Vec<rusqlite::types::Value>);

impl From<&str> for RowKey { /* a one-column text key */ }
impl<A: ToSql, B: ToSql> From<(A, B)> for RowKey { /* a two-column key */ }

/// One install of the app, by its 64-bit device id (§10).
pub struct DeviceId(u64);

/// A member, by the public half of their Ed25519 key pair (§11.1).
pub struct MemberId(String);

/// One write: the device that made it and its number in that device's log (§6).
pub struct WriteId {
    pub device: DeviceId,
    pub number: u64,
}

/// Where a row goes: the store, one circle, or this device only (§14).
pub enum Audience {
    Store,
    Circle(CircleId),
    Device,
}
```

### 20.1 Opening

- A store lives in one directory on the device, its `StoreDir`, which holds
  the database, coven's copies of files, and the cache.
- Opening a store needs its declared tables ([§20.2](#202-declaring-synced-tables))
  and its migrations ([§20.13](#2013-migrations)).
- *Key custody* is where this device keeps the store keys it has opened
  ([§11](#11-keys)): every key it has used, so it reads writes made under
  older ones.
- *Identity custody* is where this device keeps its member's two key pairs
  ([§11.1](#111-cryptography)).

```rust
/// Registers the OS keychain service every key and secret is stored under.
/// Called once at startup, before any store opens.
pub fn set_keyring_service(name: impl Into<String>) -> Result<(), KeyError>;

pub struct Coven;

impl Coven {
    /// Starts opening the store in `store_dir`. `config` is a `Config`, or a
    /// closure that returns the current `Config` and is called on each use, so
    /// the app can change storage settings without reopening.
    pub fn builder(store_dir: StoreDir, config: impl Into<CovenConfig>) -> CovenBuilder;

    /// Deletes a closed store from this device: every keychain entry coven
    /// holds for it, including the named host secrets, then its directory.
    /// Refused while the store is open; storage is untouched, and running it
    /// again finishes a deletion that failed partway.
    pub fn delete_store(
        store_dir: &StoreDir,
        store_id: &str,
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
    /// key reads it.
    pub fn open(self) -> CovenResult<CovenHandle>;

    /// Opens the store for reading only, alongside a handle that has it open,
    /// for example from another process. It takes no lock and runs no
    /// migration, and refuses a database whose schema is newer than its
    /// migrations or whose coven tables need migrating.
    pub fn open_read_only(self) -> CovenResult<CovenReadHandle>;
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
let config = Config::with_defaults(store_id.clone(), device_id, "Household".into());

let handle = Coven::builder(layout.store_dir(&store_id), config)
    .synced_tables(tables())                        // §20.2
    .migrations(migrations())                       // §20.13
    .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
    .open()?;
```

### 20.2 Declaring synced tables

- Each synced table declares its kind of key ([§8.5](#85-keys-and-uniqueness)),
  how its rows get their audience ([§14](#14-audiences)), and whether its rows
  carry a file ([§16](#16-files)).
- A table declares at most one of `gated_by`, `scoped_by`, `remote_root`,
  `gated_through` and `gated_by_descendants`; a table that declares none is
  in the store.

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

    /// A root whose boolean `column` holds each row's audience: true for the
    /// store, false for this device. Never NULL.
    pub fn gated_by(self, column: impl Into<String>) -> Self;

    /// A root whose text `column` holds each row's audience: `store`, a
    /// circle's id, or `device`. Never NULL.
    pub fn scoped_by(self, column: impl Into<String>) -> Self;

    /// A root whose rows are always the store's.
    pub fn remote_root(self) -> Self;

    /// A descendant: each row takes the audience of the row its
    /// `foreign_key` column points at (§14.1).
    pub fn gated_through(self, foreign_key: impl Into<String>) -> Self;

    /// An ancestor: a row is in the store while a present shared row points
    /// at it, and a device shows it only while a shared row that device can
    /// read does (§14).
    pub fn gated_by_descendants(self) -> Self;

    /// Marks a descendant as an asset, which never keeps an ancestor present.
    pub fn asset(self) -> Self;

    /// The table's rows carry a file, declared by `declaration`.
    pub fn carries_blob(self, declaration: BlobDecl) -> Self;
}

pub enum Provenance {
    /// The user's own file at a path on their device; coven records it and
    /// never copies, changes or deletes it (§16.1).
    UserProvided,
    /// Bytes the app hands to coven, which keeps and owns them.
    HostProvided,
}

pub enum CacheFill {
    /// Devices download the file as soon as its row arrives.
    CacheEager,
    /// Devices download it on first read.
    CacheLazy,
}

impl BlobDecl {
    /// Declares the file a table's rows carry: its namespace, which groups
    /// files in the cache and in storage, its kind, and when devices download
    /// it.
    pub fn new(
        namespace: impl Into<String>,
        provenance: Provenance,
        fill: CacheFill,
    ) -> Self;

    /// The column naming the file. Defaults to `id`.
    pub fn with_id_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's size in bytes. Defaults to `size`.
    pub fn with_size_column(self, column: impl Into<String>) -> Self;

    /// The column holding the file's content hash, the SHA-256 of its bytes,
    /// which coven fills in (§16.2). Defaults to `hash`.
    pub fn with_hash_column(self, column: impl Into<String>) -> Self;

    /// Encrypts the file with a key derived for `scope`, instead of the
    /// audience's own key.
    pub fn with_scope(self, scope: BlobScope) -> Self;

    /// Refuses a write that points an existing row at a different file.
    pub fn write_once(self) -> Self;
}
```

Example:

```rust
fn tables() -> Vec<SyncedTable> {
    vec![
        // A root: each note is the store's, a circle's, or this device's.
        SyncedTable::new("notes", RowIdentity::IndependentUuid).scoped_by("audience"),
        // Descendants of notes. Each attachment carries the user's own file.
        SyncedTable::new("attachments", RowIdentity::IndependentUuid)
            .gated_through("note_id")
            .carries_blob(BlobDecl::new("attachments", Provenance::UserProvided, CacheFill::CacheLazy)),
        // An asset: a thumbnail the app makes, which never keeps an ancestor present.
        SyncedTable::new("thumbnails", RowIdentity::IndependentUuid)
            .gated_through("note_id")
            .asset()
            .carries_blob(BlobDecl::new("thumbnails", Provenance::HostProvided, CacheFill::CacheEager)),
        // An ancestor, with keys from the tag's name, kept while a shared note wears it.
        SyncedTable::new("tags", RowIdentity::SharedKey).gated_by_descendants(),
        // A key over two columns; note_id holds a UUID.
        SyncedTable::new("note_tags", RowIdentity::IndependentUuid)
            .key_columns(["note_id", "tag_id"])
            .gated_through("note_id"),
    ]
}
```

### 20.3 Writing

- A write runs the app's SQL in one transaction ([§5](#5-local-database)).
- The closure returns the write's result; an error rolls the whole write
  back, files included.

```rust
impl CovenHandle {
    /// Runs one write.
    pub async fn write<F, R>(&self, sql: F) -> CovenResult<R>
    where
        F: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;

    /// Runs one write that also hands coven app-provided files. `build` adds
    /// the files, then `sql` runs the write that refers to them.
    pub async fn write_with_blobs<F, S, R>(&self, build: F, sql: S) -> CovenResult<R>
    where
        F: FnOnce(&mut WriteBatch) -> CovenResult<()> + Send + 'static,
        S: FnOnce(SqlContext<'_, '_>) -> CovenResult<R> + Send + 'static,
        R: Send + 'static;
}

impl WriteBatch {
    /// Hands coven an app-provided file's bytes, kept under `namespace` and
    /// `id`. `bytes` is a byte buffer, or a stream read once, so a large file
    /// never has to fit in memory.
    pub fn put_blob(
        &mut self,
        namespace: impl Into<String>,
        id: impl Into<String>,
        bytes: impl Into<BlobSource>,
    );

    /// Deletes coven's copy of an app-provided file. The write fails if a row
    /// still refers to the file after it.
    pub fn delete_blob(&mut self, namespace: impl Into<String>, id: impl Into<String>);
}

pub enum BlobSource {
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
    pub fn insert_external_blob(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        prepared: PreparedExternalBlob,
        insert_sql: &str,
        params: &[(&str, &dyn ToSql)],
    ) -> Result<(), DbError>;

    /// Records a prepared user-provided file on a row the write already has.
    /// Fails if the row's size column disagrees with the file, or the file
    /// changed since it was prepared.
    pub fn register_external_blob(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        prepared: PreparedExternalBlob,
    ) -> Result<(), DbError>;

    /// Forgets the user-provided file recorded on a row. The file itself is
    /// untouched.
    pub fn clear_external_blob(&self, table: &str, key: impl Into<RowKey>) -> Result<(), DbError>;

    /// Checks that a file reference taken earlier still names the row's
    /// current file; the write fails if it doesn't. Called before changing
    /// or deleting the row.
    pub fn validate_row_blob_ref(&self, reference: &RowBlobRef) -> Result<(), DbError>;

    /// Sets the audience of a root row this write creates. A shared audience
    /// uploads the row's files before the write is uploaded (§16.5).
    pub fn set_audience(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        audience: Audience,
    ) -> Result<(), DbError>;
}

/// Reads a user's file once, before the write, for its size and content.
/// `progress` receives the bytes read so far. Fails if the file changes
/// while it is read.
pub async fn prepare_external_blob(
    path: &Path,
    progress: impl Fn(u64) + Send + Sync,
) -> Result<PreparedExternalBlob, DbError>;
```

Example:

```rust
let note_id = Uuid::now_v7().to_string();
let attachment_id = Uuid::now_v7().to_string();
let size = std::fs::metadata(&path)?.len() as i64;
let prepared = prepare_external_blob(&path, |read| show_progress(read)).await?;

handle
    .write(move |sql| {
        sql.execute(
            "INSERT INTO notes (id, title, audience) VALUES (?1, ?2, 'device')",
            (&note_id, "Paint colors"),
        )?;
        sql.execute(
            "INSERT INTO attachments (id, note_id, title, size) VALUES (?1, ?2, ?3, ?4)",
            (&attachment_id, &note_id, "Swatches", size),
        )?;
        sql.register_external_blob("attachments", attachment_id.as_str(), prepared)?;
        Ok(())
    })
    .await?;
```

### 20.4 Reading

- Reads run on several read-only connections at once ([§5](#5-local-database)).
- A *live query* runs once, then again whenever a write commits that changes
  rows it read.

```rust
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
    /// The write that set the lost value.
    pub set_by: WriteId,
    /// What replaced it: a write that hadn't read it, or a removal rule.
    pub replaced_by: Replacement,
}

pub enum Lost {
    Cell { column: String, value: rusqlite::types::Value },
    Row { values: Vec<(String, rusqlite::types::Value)> },
}

pub enum Replacement {
    Write(WriteId),
    /// Removal rules took the row out: every one that holds (§8.4, §8.5,
    /// §8.6, §14).
    Rules(Vec<RemovalRule>),
    /// A breaking schema change the write hadn't read (§17.1).
    SchemaChange,
    /// A reset the write hadn't read (§19.3).
    Reset,
}

pub enum RemovalRule {
    ForeignKey { column: String },
    Check { constraint: String },
    Unique { columns: Vec<String> },
    Ancestor,
}

/// A handle that only reads, opened with `open_read_only`.
impl CovenReadHandle {
    pub fn read<F, R>(&self, read: F) -> Read<'_, F>;
    pub async fn row_blob_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<RowBlobRef, DbError>;
    pub async fn read_blob(&self, file: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError>;
    pub async fn open_blob_stream(&self, file: &RowBlobRef) -> Result<BlobStream, BlobCacheError>;
    pub async fn is_pinned(&self, files: &[RowBlobRef]) -> Result<bool, BlobCacheError>;
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
- Setup commits the storage credentials and keys only once the connection
  is ready; a failed setup leaves the device as it was.
- A device that isn't connected still reads and writes
  ([§3](#3-guarantees)); its writes wait in `coven_uploads`.

```rust
impl CovenHandle {
    /// Sets up storage on S3 with this member's access key (§4).
    pub async fn setup_s3_cloud_home(
        &self,
        cloud_home: CloudHomeConfig,
        access_key: String,
        secret_key: String,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError>;

    /// Sets up storage on Google Drive, Dropbox or OneDrive, running the
    /// provider's sign-in with the builder's OAuth clients. `cancel` stops
    /// the sign-in.
    pub async fn setup_oauth_cloud_home(
        &self,
        cloud_home: CloudHomeConfig,
        cancel: watch::Receiver<bool>,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError>;

    /// Sets up storage on iCloud, through the builder's CloudKit calls.
    pub async fn setup_cloudkit_cloud_home(
        &self,
        cloud_home: CloudHomeConfig,
    ) -> Result<ConnectedCloudHome, CloudHomeSetupError>;

    /// Checks that the storage `config` describes can be reached and used,
    /// without connecting to it.
    pub async fn probe_cloud_home(&self, config: &Config) -> Result<(), SyncError>;

    /// Opens the current store key from its copy sealed to this member in
    /// storage (§11), keeps it in key custody, and connects.
    pub async fn unlock_cloud_home(&self) -> Result<ConnectedCloudHome, CloudHomeUnlockError>;

    /// Whether key custody holds the store key: `Available` or `Locked`.
    pub fn cloud_home_key_state(&self) -> Result<CloudHomeKeyState, KeyError>;

    /// Disconnects and removes this device's storage credentials. If removing
    /// them fails, the connection stays.
    pub async fn disconnect_cloud_home(&self) -> Result<(), SyncError>;

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

    /// Syncs now instead of at the next idle tick.
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
    /// Operations coven started that failed for good (§18).
    pub blocked_operations: Vec<BlockedOperation>,
    /// This member's store log entries that another entry won over (§9).
    pub dropped_entries: Vec<DroppedEntry>,
    /// The rows the sync's writes changed, as a hint for refreshing views
    /// that aren't live queries. Not a complete list.
    pub row_changes: Option<Vec<RowChange>>,
}

pub struct WaitingWrite {
    pub write: WriteId,
    pub waiting_for: Vec<WriteId>,
    pub since: SystemTime,
}

/// Storage this device has set up, with whether it holds the store key.
pub struct ConnectedCloudHome {
    pub cloud_home: CloudHomeConfig,
    pub key_state: CloudHomeKeyState,
}

pub enum CloudHomeKeyState {
    Available,
    Locked,
}

impl CloudHomeSetupError {
    /// What went wrong, for the app to show: `Authentication`,
    /// `PermissionDenied`, `ContainerNotFound`, `RegionMismatch`,
    /// `QuotaExceeded`, `InvalidConfiguration`, `LocationOccupied` when the
    /// location holds another store, `Network`, `MemberKeysMissing`,
    /// `SecureStorage` or `Internal`.
    pub fn failure(&self) -> CloudHomeSetupFailure;
}

pub enum SyncFailure {
    /// The store's schema or format version is newer than this app (§17).
    UpdateRequired,
    /// Storage refused or failed a request.
    Storage(Arc<StorageError>),
    /// Anything else, with its cause.
    Other(Arc<dyn std::error::Error + Send + Sync>),
}
```

Example:

```rust
match handle.setup_s3_cloud_home(cloud_home, access_key, secret_key).await {
    Ok(connected) => remember(connected.cloud_home),
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
- A failed step of an operation the app started is that call's error; one
  coven started is reported in the sync status.

```rust
impl CovenHandle {
    /// Runs a failed operation again from the step after its last completed
    /// one. An operation whose cause still stands fails again.
    pub async fn retry_blocked_operation(&self, operation: OperationId) -> Result<(), OperationError>;

    /// Abandons a failed operation and deletes its row. Steps already done
    /// stay done; each kind's steps are ordered so other devices never see a
    /// half-done operation (§18).
    pub async fn discard_blocked_operation(&self, operation: OperationId) -> Result<(), OperationError>;

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

### 20.7 Audiences and uploads

- *Moving* a root row sends it, its descendants and the ancestors they keep
  to another audience ([§14.2](#142-moving-rows)).
- A move into the store or a circle uploads the rows' files first, then
  writes the move; a move back to this device downloads them first
  ([§16.1](#161-kinds-and-locality)).
- A move is an operation ([§18.1](#181-operations)): the call records it and
  returns, and the move finishes whenever storage can be reached.

```rust
impl CovenHandle {
    /// Moves a root row to `audience`. For a move back to this device,
    /// `destinations` maps each user-provided file's id to the path it is
    /// written to, which must not already exist; other moves pass an empty
    /// map.
    pub async fn set_audience(
        &self,
        table: &str,
        key: impl Into<RowKey>,
        audience: Audience,
        destinations: &HashMap<String, PathBuf>,
    ) -> Result<(), AudienceMoveError>;

    /// Records several moves at once, in one transaction.
    pub async fn set_audiences(&self, moves: Vec<AudienceChange>) -> Result<(), AudienceMoveError>;

    /// Cancels a move whose files are still uploading. Files already
    /// uploaded for it are deleted, and the rows keep their audience.
    pub async fn cancel_set_audience(
        &self,
        table: &str,
        key: impl Into<RowKey>,
    ) -> Result<(), AudienceMoveError>;

    /// A live query over the upload queue: every file waiting to upload, with
    /// its progress, and every move in progress. The first result is the
    /// current state.
    pub fn subscribe_uploads(&self) -> UploadsLiveQuery;

    /// Retries every waiting upload now, instead of after its retry delay.
    pub async fn retry_uploads_now(&self) -> Result<DrainOutcome, SyncError>;

    /// Pauses uploads, or resumes them. A paused upload keeps its place,
    /// including a provider upload session in progress.
    pub fn set_uploads_paused(&self, paused: bool);
}

pub struct AudienceChange {
    pub table: String,
    pub key: RowKey,
    pub audience: Audience,
    pub destinations: HashMap<String, PathBuf>,
}

impl UploadsLiveQuery {
    /// The current state at once, then the next state each time it changes.
    pub async fn next(&mut self) -> Result<Uploads, DbError>;
}

pub struct Uploads {
    pub paused: bool,
    /// Oldest first.
    pub files: Vec<QueuedUpload>,
    pub moves: Vec<AudienceMove>,
}

pub struct QueuedUpload {
    pub file: RowBlobRef,
    /// The move this upload belongs to, if any.
    pub moving: Option<(String, RowKey)>,
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

pub struct AudienceMove {
    pub table: String,
    pub key: RowKey,
    pub audience: Audience,
    pub phase: MovePhase,
}

pub enum MovePhase {
    Uploading,
    Downloading,
    /// Every file is in place; the move's write is next.
    Writing,
    /// Cancelled; its uploaded files are being deleted.
    Cancelling,
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
// Share a note: its attachments and thumbnails upload, then the move is written.
handle
    .set_audience("notes", note_id.as_str(), Audience::Store, &HashMap::new())
    .await?;

let note_key = RowKey::from(note_id.as_str());
let mut uploads = handle.subscribe_uploads();
loop {
    let state = uploads.next().await?;
    for upload in &state.files {
        if let UploadPhase::Uploading { bytes_sent, bytes_total } = upload.phase {
            show_progress(upload.file.key(), bytes_sent, bytes_total);
        }
    }
    let moving = state.moves.iter().any(|m| m.table == "notes" && m.key == note_key);
    if !moving {
        break; // the note is now in the store
    }
}
```

### 20.8 Files and the cache

- A *file reference*, `RowBlobRef`, names one row's file as of that row's
  current version, so a later change to the row can't redirect a read.
- Coven reads a file from wherever it is: the user's original, coven's own
  copy, the cache, or storage ([§16](#16-files)).

```rust
impl CovenHandle {
    /// The file a row carries, as of the row's current version.
    pub async fn row_blob_ref(&self, table: &str, key: impl Into<RowKey>) -> Result<RowBlobRef, DbError>;

    /// Reads a whole file, checking it against its row.
    pub async fn read_blob(&self, file: &RowBlobRef) -> Result<Vec<u8>, BlobCacheError>;

    /// Opens a file for reading ranges (§16.3). Opening checks the file
    /// against its row once; keep the stream for as long as the file is read.
    pub async fn open_blob_stream(&self, file: &RowBlobRef) -> Result<BlobStream, BlobCacheError>;

    /// Makes sure a file's bytes are on this device: a remote file is
    /// downloaded into the cache, and a local one is checked.
    pub async fn materialize_row_blob(&self, file: &RowBlobRef) -> Result<(), BlobCacheError>;

    /// The path, size and modification time coven recorded for a row's
    /// user-provided file, or `None` when the row has none.
    pub async fn external_blob(&self, table: &str, key: impl Into<RowKey>) -> Result<Option<ExternalBlob>, DbError>;

    /// Keeps remote files whole on this device regardless of the cache
    /// budget, downloading what is missing. `on_progress` is called before
    /// the first download, as bytes arrive, and as each file is kept.
    pub async fn pin(
        &self,
        files: &[RowBlobRef],
        on_progress: &(dyn Fn(PinProgress) + Send + Sync),
    ) -> Result<(), BlobCacheError>;

    /// Stops keeping files; they stay in the cache until the budget evicts them.
    pub async fn unpin(&self, files: &[RowBlobRef]) -> Result<(), BlobCacheError>;

    /// Whether every file in `files` is pinned. An empty set is pinned.
    pub async fn is_pinned(&self, files: &[RowBlobRef]) -> Result<bool, BlobCacheError>;

    /// Whether each row's file is pinned, one answer per key in order, or
    /// `None` for a key with no row carrying a file. A file not yet uploaded
    /// reads as not pinned.
    pub async fn rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> Result<Vec<Option<bool>>, BlobCacheError>;

    /// The same answers, live. `set_rows` changes which rows it watches.
    pub fn subscribe_rows_pinned(&self, table: &str, keys: Vec<RowKey>) -> RowsPinnedLiveQuery;

    /// Removes a remote file's copies from the cache, pinned or not. Never
    /// touches a local file or storage; a later read downloads it again.
    pub async fn evict_blob(&self, file: &RowBlobRef) -> Result<(), BlobCacheError>;

    /// The cache budget for one namespace, in bytes; each namespace evicts
    /// on its own.
    pub async fn set_cache_budget(&self, namespace: &str, max_bytes: u64) -> Result<(), DbError>;
    pub async fn get_cache_budget(&self, namespace: &str) -> Result<Option<u64>, DbError>;

    /// Progress of downloading every file declared to download as soon as
    /// its row arrives that this device doesn't have yet, such as after
    /// loading a snapshot to join or recover (§16, §19.2).
    pub fn subscribe_eager_cache_fill_status(&self) -> watch::Receiver<EagerCacheFillStatus>;

    /// Stops those downloads without stopping sync.
    pub fn cancel_eager_cache_fill(&self);
}

impl RowBlobRef {
    pub fn table(&self) -> &str;
    pub fn key(&self) -> &RowKey;
    /// The column naming the file.
    pub fn column(&self) -> &str;
    /// The file's size in bytes.
    pub fn plaintext_size(&self) -> u64;
    /// The row's audience, which the file follows (§16.1).
    pub fn audience(&self) -> Audience;
}

impl BlobStream {
    /// The file's whole size in bytes.
    pub fn plaintext_size(&self) -> u64;

    /// Reads `len` bytes at `offset`. A range past the end is an error, never
    /// a short read.
    pub async fn read_at(&self, offset: u64, len: u64) -> Result<Vec<u8>, BlobCacheError>;
}

pub enum BlobCacheError {
    /// The range needs chunks that aren't cached, and storage can't be reached.
    Offline { id: String },
    /// A remote file was read with no storage connected.
    NoCloudHome,
    /// A user-provided file is gone from its recorded path.
    ExternalMissing { id: String, path: PathBuf },
    /// A user-provided file's size or modification time no longer matches
    /// what coven recorded.
    ExternalChanged { id: String, path: PathBuf },
    /// A chunk, or a local copy, failed its check.
    Integrity { id: String },
    /// The range lies outside the file.
    RangeOutOfBounds { id: String, offset: u64, end: u64, size: u64 },
    /// Storage refused or failed the request.
    Storage(StorageError),
    /// A database or disk failure, with its cause.
    Metadata(DbError),
    File(FileError),
}
```

Example:

```rust
let recording = handle.row_blob_ref("attachments", attachment_id.as_str()).await?;
let stream = handle.open_blob_stream(&recording).await?;

// A voice memo: read its header, then seek to where listening resumes.
let header = stream.read_at(0, 64 * 1024).await?;
let resume_at = position_for(&header, saved_seconds);
match stream.read_at(resume_at, 256 * 1024).await {
    Ok(bytes) => play(bytes),
    Err(BlobCacheError::Offline { .. }) => show_not_downloaded(),
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

    /// Removes a member and all their devices, revokes their storage access,
    /// and rotates the store key (§13).
    pub async fn remove_member(&self, member: &MemberId) -> Result<(), SyncError>;

    /// Removes a device. The provider can't cut off one device alone, so the
    /// result says how its member signs out of the provider and signs in
    /// again on the devices they keep (§13).
    pub async fn remove_device(&self, device: DeviceId) -> Result<ProviderSignOut, SyncError>;
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

pub enum ProviderSignOut {
    /// Remove the app's access from the provider account, then sign in again.
    RemoveAppAccess { provider: CloudProvider },
    /// Remove the device from the Apple account.
    RemoveFromAppleAccount,
    /// Replace the S3 access key on one device, pair the others, and write
    /// down a new restore code.
    ReplaceAccessKey,
}
```

### 20.10 Joining and restore

- A new device of a person already in the store gets their member keys, which
  get it the store key, by pairing, from a synced keychain, or from their
  restore code ([§12](#12-joining-and-restore)).
- Each of these makes the store directory, puts the keys in custody, loads
  the store, and returns the `Config` the app then opens the store with.
- Each takes the same tables, migrations and custody choices as the builder,
  and `cancel`, which stops it.

```rust
impl CovenHandle {
    /// On an existing device: starts a pairing session on the local network.
    /// The session shows a code for the new device to scan, and lasts 15
    /// minutes.
    pub async fn start_device_pairing(&self) -> Result<DevicePairingHost, StartDevicePairingError>;

    /// Hands the new device this member's keys and storage access, once the
    /// person has confirmed its request, and waits until it has added itself
    /// to the store log.
    pub async fn approve_device_pairing(
        &self,
        host: &DevicePairingHost,
        request: &DevicePairingRequest,
        on_progress: &(dyn Fn(AdmittingDeviceJoinProgress) + Send + Sync),
        cancel: watch::Receiver<bool>,
    ) -> Result<(), ApproveDevicePairingError>;

    /// Cancels a pairing session, and waits for an approval in progress to
    /// stop.
    pub async fn cancel_device_pairing(&self, host: &DevicePairingHost) -> Result<(), ApproveDevicePairingError>;

    /// Makes the person's restore code: their member keys and storage
    /// credentials. They write it down as part of setup.
    pub async fn generate_restore_code(&self) -> Result<String, SyncError>;
}

impl DevicePairingHost {
    /// The code the new device scans.
    pub fn pairing_code(&self) -> String;

    /// Waits for the new device's signed request.
    pub async fn wait_for_request(&self) -> Result<DevicePairingRequest, DevicePairingTransportError>;
}

impl DevicePairingRequest {
    /// The provider account the new device signed in with, for the person to
    /// confirm.
    pub fn provider_account_email(&self) -> Option<&str>;
}

impl PreparedDevicePairing {
    /// On the new device: starts pairing from a scanned code, or picks up the
    /// pairing already started from it. Kept on disk, so pairing resumes after
    /// a restart.
    pub fn open_or_create(
        pairing_code: &str,
        provider_account_email: Option<String>,
        layout: &StoreLayout,
    ) -> Result<Self, DevicePairingError>;

    /// The pairings this app started and hasn't finished.
    pub fn pending(layout: &StoreLayout) -> Result<Vec<Self>, DevicePairingError>;
}

/// On the new device: receives the member's keys, adds this device to the
/// store log, and loads the store.
pub async fn join_with_device_pairing(
    pairing: &PreparedDevicePairing,
    layout: StoreLayout,
    synced_tables: Vec<SyncedTable>,
    migrations: Vec<Migration>,
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    /* transfer limits, OAuth clients, CloudKit calls, clock */
    on_progress: JoiningDeviceJoinProgressObserver,
    cancel: &watch::Receiver<bool>,
) -> Result<DeviceJoinTransportOutcome, BootstrapError>;

pub enum DeviceJoinTransportOutcome {
    Joined(Config),
    /// The existing device cancelled the pairing.
    Abandoned(DeviceJoinAbandonment),
}

/// What a restore code is for, before using it: the store's id and name, its
/// provider, and whether the provider needs a sign-in first.
pub fn decode_restore_code_info(code: &str) -> Result<RestoreCodeInfo, RestoreCodeError>;

/// Opens the store on a new device from the person's restore code.
/// `oauth_tokens` is the provider sign-in, when `needs_oauth`.
pub async fn restore_from_code(
    code: &str,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    oauth_tokens: Option<OAuthTokens>,
    layout: &StoreLayout,
    /* transfer limits, OAuth clients, CloudKit calls, clock, ids */
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError>;

/// Opens the store on a new device from the iCloud Keychain item, which holds
/// what a restore code holds (§12): `store_id`, `store_name` and `source`
/// come from it.
pub async fn restore_from_cloud(
    store_id: &str,
    store_name: &str,
    source: RestoreSource,
    synced_tables: &[SyncedTable],
    migrations: &[Migration],
    coven_migration_policy: CovenMigrationPolicy,
    key_custody: KeyCustody,
    identity_custody: IdentityCustody,
    layout: &StoreLayout,
    /* transfer limits, clock, ids */
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<Config, BootstrapError>;

impl OAuthClients {
    /// Runs the provider's sign-in in the browser, with a redirect to a local
    /// port, and returns its tokens.
    pub async fn authorize(
        &self,
        provider: CloudProvider,
        cancel: watch::Receiver<bool>,
        clock: &dyn Clock,
    ) -> Result<OAuthTokens, OAuthError>;

    /// For an app that handles the redirect itself: the request to open, and
    /// the call that turns the redirect's code into tokens.
    pub fn build_authorize_request(&self, provider: CloudProvider, redirect_uri: &str) -> Result<AuthorizeRequest, OAuthError>;
    pub async fn exchange_code(
        &self,
        provider: CloudProvider,
        code: &str,
        callback_state: Option<&str>,
        request: &AuthorizeRequest,
        redirect_uri: &str,
        clock: &dyn Clock,
    ) -> Result<OAuthTokens, OAuthError>;
}
```

Example, on the existing device:

```rust
let host = handle.start_device_pairing().await?;
show_qr_code(&host.pairing_code());

let request = host.wait_for_request().await?;
if confirm_new_device(request.provider_account_email()).await {
    handle
        .approve_device_pairing(&host, &request, &|step| show_step(step), cancel_rx)
        .await?;
} else {
    handle.cancel_device_pairing(&host).await?;
}
```

On the new device:

```rust
let pairing = PreparedDevicePairing::open_or_create(&scanned_code, account_email, &layout)?;
let outcome = join_with_device_pairing(
    &pairing,
    layout.clone(),
    tables(),
    migrations(),
    CovenMigrationPolicy::ApplyPending,
    KeyCustody::Keyring,
    IdentityCustody::Keyring,
    oauth_tokens,
    /* … */
    Arc::new(|step| show_step(step)),
    &cancel_rx,
)
.await?;
if let DeviceJoinTransportOutcome::Joined(config) = outcome {
    let handle = Coven::builder(layout.store_dir(&config.store_id), config)
        .synced_tables(tables())
        .migrations(migrations())
        .coven_migration_policy(CovenMigrationPolicy::ApplyPending)
        .open()?;
}
```

From a restore code:

```rust
let info = decode_restore_code_info(&code)?;
let tokens = if info.needs_oauth {
    Some(oauth_clients.authorize(info.cloud_provider, cancel_rx.clone(), &SystemClock).await?)
} else {
    None
};
let config = restore_from_code(
    &code,
    &tables(),
    &migrations(),
    CovenMigrationPolicy::ApplyPending,
    KeyCustody::Keyring,
    IdentityCustody::Keyring,
    tokens,
    &layout,
    /* … */
    |step| show_step(step),
    &cancel_rx,
)
.await?;
```

### 20.11 Keys and secrets

```rust
impl CovenHandle {
    /// Makes this member's two key pairs and puts them in identity custody,
    /// for the person creating a store. Fails if custody already holds keys.
    /// Joining and restoring put the keys there themselves.
    pub fn initialize_identity(&self) -> Result<MemberId, IdentityError>;

    /// Removes the store keys from key custody and drops any connection that
    /// holds them unlocked. If custody can't remove them, the connection
    /// stays.
    pub async fn forget_master_key(&self) -> Result<(), SyncError>;

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

/// The app's own store for the store keys this device holds.
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
impl CovenHandle {
    pub fn circles(&self) -> Circles<'_>;
}

impl Circles<'_> {
    /// Makes a circle with this member in it.
    pub async fn create(&self, name: &str) -> Result<CircleId, CircleError>;

    /// Renames a circle. Its key, members and rows don't change.
    pub async fn rename(&self, circle: CircleId, name: &str) -> Result<(), CircleError>;

    /// Deletes a circle.
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

pub struct ColumnChange {
    pub name: String,
    pub old: Option<rusqlite::types::Value>,
    pub new: Option<rusqlite::types::Value>,
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
  the other row is taken out and held while it conflicts, and recorded in
  `coven_lost` ([§8.5](#85-keys-and-uniqueness)).

### A.4 Restrict and no-action foreign keys

- Problem: a device can delete a parent while another adds a child it
  hasn't seen.
- Status: the child is taken out and held while its parent is gone, and
  recorded in `coven_lost` ([§8.4](#84-foreign-keys)).

### A.5 CHECK constraints

- Problem: two concurrent edits that each pass can merge into a row that
  fails, such as one device setting `start` and another `end`.
- Status: a row that fails after a merge is taken out and held until it
  passes, and recorded in `coven_lost` ([§8.6](#86-check-constraints)).

### A.6 Triggers that write synced tables

- Problem: a trigger that runs again while coven applies a remote write
  would repeat what the original device already sent.
- Status: allowed as shared triggers, which run only on the device making
  the write ([§8.7](#87-triggers)).

### A.7 Schema changes

- Problem: devices running different app versions hold different schemas.
- Status: additions sync with no change of version; any other change
  raises the store's version, and every device updates and reloads ([§17](#17-schema-changes)).

## Appendix B. Proof of convergence

- This appendix proves the convergence guarantee of [§3](#3-guarantees) for the merge of [§8](#8-merge)
  and the audiences of [§14](#14-audiences).
- The proof is checked by machine: a Lean 4 development in
  `plans/proofs/merge/`, which has no unproven step and uses no axiom beyond
  Lean's own.
- Each claim names the Lean theorem that checks it; [B11](#b11-what-lean-checks-and-what-is-prose) lists what is argued
  only here.

### B1 The claim

- Two devices that read the same audiences and applied the same writes, each
  in an order that respects causality, hold the same database:
  - the same `coven_rows` and `coven_cells`;
  - the same rows in the app's tables, with the same values;
  - the same `coven_lost` rows, for lost values and removed rows alike.
- This holds whatever order each device ran the removal rules in.
- Two devices that read different audiences agree on every row both read,
  except what depends on rows only one of them reads: whether an ancestor is
  shown, which row shows for a key present in two audiences, and the rows
  taken out with those ([B9](#b9-audiences)).
- `coven_writes` holds the same writes on both, under row ids of each
  device's own ([§8.1](#81-example)).

### B2 Terms

- *Write*, *row change*, *cell*, *generation*, *audience*: as in [§5](#5-local-database), [§8](#8-merge),
  [§8.3](#83-deletes) and [§14](#14-audiences).
- *Row*: one table, key and audience. The store's note 1 and a circle's note
  1 are two rows, each with its own generations ([§14.2](#142-moving-rows)).
- *Had read*: write `w` had read write `a` when `w`'s device had applied `a`
  before making `w`; a device has always read its own earlier writes ([§7.1](#71-causality)).
- *Causal order*: an order of applying writes in which each write comes after
  every write it had read. Every device applies writes in a causal order.
- *Closed set*: a set of writes that holds every write any of them had read.
  The writes a device has applied form a closed set.
- *Incarnation*: one lifetime of a row. An insert made at generation `g`
  starts incarnation `g + 1`; an update or delete made at generation `g` acts
  on incarnation `g`.
  - E.g. Ana creates note 43 at generation 0, so it lives in incarnation 1;
    she deletes it, and re-adds it at generation 2, starting incarnation 3.
- *Setter*: a write that sets a cell. Its value is the cell's value in that
  write.
- *Replacer* of a value: a write that sets the same cell of the same
  incarnation with a larger timestamp, or deletes that incarnation.
- *Merged state*: each row's generation, the write `coven_rows` names for
  each generation, each cell's value and the write that set it, and the lost
  values ([§8](#8-merge)).
- *Removed row*: a row present in the merged state that a removal rule takes
  out of the app's table.

### B3 Model and assumptions

- A write is an identifier with:
  - its timestamp, a number;
  - which writes it had read;
  - for each row it changes, one row change: insert, update or delete, the
    generation it was made at, and which columns it sets.
- A cell's value is identified with the write that set it, since a write
  sets one value per cell.
- The writes satisfy, by [§7.1](#71-causality), [§7.2](#72-timestamps) and [§8.3](#83-deletes):
  - **assumption 1**: timestamps are unique;
  - **assumption 2**: a write's timestamp is larger than that of every write
    it had read;
  - **assumption 3**: a change's generation was reached on the authoring
    device: it is 0, or some write the author had read moved the row to it;
  - **assumption 4**: an insert is made at an even generation, an update or
    delete at an odd one.
- What a device stores for the merged state:
  - each row's current generation;
  - per generation, the write `coven_rows` names: of the writes that moved
    the row there, the one with the smallest timestamp;
  - per cell, the write that set its value;
  - per value, its `coven_lost` row: the incarnation it was set in, and the
    write recorded as replacing it.
- In Lean: `Writes`, `Valid`, `St`, in `Model.lean`.

### B4 The merged state as a function of the writes

For a closed set `S` of writes, each part of the merged state is defined from
`S` alone, with no order of application:

- **Generation** of a row: the largest generation any write in `S` moved it
  to by an insert or delete; 0 if none.
- **Write per generation**: of the writes in `S` that moved the row to that
  generation, the one with the smallest timestamp ([§8.3](#83-deletes)).
- **Cell**: of the writes in `S` that set the cell in the current
  incarnation, the one with the largest timestamp; nothing while the row is
  deleted ([§8.1](#81-example), [§8.2](#82-concurrent-writes-to-one-row)).
- **Lost**: a value is lost when some write in `S` replaced it and no write
  in `S` that replaced it had read it ([§8](#8-merge)).
- **Replaced by**, for a lost value of incarnation `k`:
  - if `k` was deleted: its earliest delete, which is the write `coven_rows`
    names for generation `k + 1`;
  - otherwise: the cell's current winning write.
- E.g. the writes of [§8.1](#81-example) on note 42's title:

  ```
  write             value               stamp          had read
  Ana's write 1     "Grocery list"      12:00:00 #0
  Ana's write 4     "Groceries"         13:01:00 #0    Ana 1
  Ben's write 9     "Weekly groceries"  13:01:00 #1    Ana 1, Ana 4
  Carol's write 2   "Shopping"          14:00:00 #0    Ana 1
  ```

  - the title: Carol's write 2, the largest stamp;
  - "Grocery list": replaced by Ana 4, Ben 9 and Carol 2, which all had read
    it; not lost;
  - "Groceries": replaced by Ben 9 and Carol 2, and Ben 9 had read it; not
    lost;
  - "Weekly groceries": replaced by Carol 2 only, which hadn't read it; lost,
    replaced by Carol's write 2.
- In Lean: `IsSpec` in `Model.lean`.

### B5 Applying one write

A device applies an arriving write `w` to each row `r` it changes. Let `G` be
`r`'s generation on the device, and `g` the generation `w`'s change was made
at.

- **Generation**: an insert or delete with `g = G` moves the row to `G + 1`.
  Any other change leaves it.
- **Write per generation**: an insert or delete made at `g` competes for
  generation `g + 1`'s `coven_rows` row: the smaller timestamp stays.
- **Cells**:
  - a delete with `g = G` empties the row's cells;
  - an insert with `g = G` starts a new incarnation with its values;
  - an update at `G`, or an insert at `G - 1` concurrent with the one that
    started the current incarnation, competes per cell: the larger timestamp
    stays ([§8.2](#82-concurrent-writes-to-one-row), [§8.3](#83-deletes));
  - a change made at an older incarnation changes no cell.
- **`coven_lost`**, for each value `a` of each cell of `r`:
  - `w`'s own value:
    - made at an incarnation already deleted: lost, replaced by the write
      `coven_rows` names for that incarnation's delete ([§8.3](#83-deletes));
    - made at the current incarnation with a smaller stamp than the cell's
      value: lost, replaced by the cell's winning write;
    - otherwise not lost;
  - a value already lost that `w` replaces:
    - if `w` had read it: no longer lost ([§8.2](#82-concurrent-writes-to-one-row));
    - otherwise still lost, and "replaced by" becomes the earlier delete or
      the later setter, by [B4](#b4-the-merged-state-as-a-function-of-the-writes);
  - the cell's current value, which `w` replaces without having read it:
    lost, replaced by `w`;
  - anything else: unchanged.
- The step reads only the device's state, the timestamps of writes it has
  applied, and the arriving write's record.
- In Lean: `step` in `Model.lean`.

### B6 Proof for the merged state

- **Step lemma**: let `S` be closed, the state be [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result for `S`, and
  `w` a write not in `S` whose had-read writes are all in `S`. Then applying
  `w` gives [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result for `S` plus `w`. Lean: `step_spec`.
- The step lemma rests on these facts:
  - no write in `S` had read `w`, since `S` is closed and `w` is not in it;
    so any replacer already applied hadn't read `w`;
  - `g ≤ G`, by assumption 3;
  - every generation from 1 to `G` was reached by a write in `S`, so with
    assumption 4 an incarnation was deleted exactly when it is older than
    `G`;
  - a value nobody replaced is the cell's current value;
  - the largest or smallest of a set, by timestamp, changes as the step
    does when `w` joins the set.
- **Induction**: applying a causal order write by write keeps [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result,
  from the empty database or from a snapshot. Lean: `foldl_isSpec`,
  `run_isSpec`.
- **Uniqueness**: two states that are both [B4](#b4-the-merged-state-as-a-function-of-the-writes)'s result for one set are equal.
  Lean: `isSpec_unique`.
- **Convergence**: two causal orders of the same writes give the same merged
  state. Lean: `merge_converges`.
- **Snapshots** ([§15](#15-snapshots)): loading a snapshot and applying the writes after it,
  with the covered writes counting as applied, gives the same merged state
  as applying every write from the start. Lean: `snapshot_converges`.
- **Timestamp order** is one causal order, by assumption 2, so every causal
  order gives the state applying the writes in timestamp order gives. Lean:
  `causalOrder_of_ts_sorted`.
- E.g. Lean runs the step on the writes of [§8.1](#81-example), [§8.2](#82-concurrent-writes-to-one-row) and [§8.3](#83-deletes), in the
  arrival orders they describe:
  - Ben's phone and Carol's tablet both end with "Shopping" and one
    `coven_lost` row, "Weekly groceries" replaced by Carol's write 2. Lean:
    `example_8_1`.
  - On Carol's tablet, "Groceries" is lost after Ana's write 4 arrives, and
    no longer lost after Ben's write 9. Lean: `example_8_2`.
  - Ben's edit of note 43 is lost, replaced by Ana's write 7, whether it
    arrives before Ana's delete or after her re-add, and generation 2's
    `coven_rows` row names write 7. Lean: `example_8_3`.

### B7 The removal rules

- The removal rules read only the merged state, so what they read is a
  function of the set of writes:
  - which rows are present: an odd generation;
  - each row's references: the parent each points at, and whether it is
    *stale*: the parent's generation it carries has been deleted since
    ([§8.4](#84-foreign-keys));
  - whether the row's merged values fail a CHECK ([§8.6](#86-check-constraints));
  - which rows are ancestors, and through which reference a row keeps one
    ([§14.1](#141-roots-descendants-and-ancestors));
  - which rows are shared;
  - each row's unique claims, each with a stamp: the timestamp of the latest
    write that set any of the constraint's columns in it ([§8.5](#85-keys-and-uniqueness)).
- A reference under set null or set default whose parent's generation was
  deleted holds null or the default in the merged state itself ([§8.4](#84-foreign-keys)). It is
  a value like any other, which CHECK and unique constraints see, not a
  reference the rules follow.
  - E.g. at 16:00 Ana deletes note 43 while Ben, offline, adds link 6
    pointing at it, under set null. Every device stores link 6 with
    `note_id` null, whether Ben's write or Ana's arrived first. Lean:
    `example_8_4`.
  - Where SQLite would refuse that value, such as on a `NOT NULL` column,
    the reference stays and counts as stale.
- The rules other than the unique one take a present row out when:
  - **foreign keys**: one of its references is stale, or its parent is
    absent or taken out;
  - **CHECK**: its merged values fail;
  - **ancestors**: it is an ancestor and no row keeps it: a present row,
    not taken out, shared, whose keeping reference points at it. A row whose
    audience comes from an ancestor is shared while that ancestor is present
    and not taken out.
- **Unique values**: a row loses when a row still present claims the same
  value of the same constraint with a smaller stamp, or an equal stamp and a
  smaller primary key.
  - A key present in two audiences on one device is a claim too, the store's
    first ([B9](#b9-audiences)).
- [§8](#8-merge)'s three steps:
  1. apply the rules other than the unique one until none fires;
  2. judge unique values among the rows still present;
  3. apply the other rules again until none fires.
- A removed row's `coven_lost` row names every rule that holds for it once
  the steps end, and the unique rule if it lost in step 2.
- In Lean: `Inputs`, `fires`, `rivalBefore`, `removal`, `view`, in
  `Removal.lean`.

### B8 Proof for the removal rules

- **Monotone**: a rule is monotone when it keeps firing for a row as more
  rows are removed.
- Every rule other than the unique one is monotone:
  - a stale reference stays stale, and a parent taken out stays out when
    more rows are removed;
  - a CHECK result doesn't depend on removals;
  - a row stops keeping an ancestor only when it, or the ancestor its own
    audience comes from, is taken out; so an ancestor with no keeper still
    has none when more rows are removed.
  - Lean: `fires_monotone`.
- For monotone rules over a finite set of rows:
  - **termination**: each step removes a row, so applying rules stops. Lean:
    `killStep_terminating`;
  - **local confluence**: two steps from one state, removing `x` and `y`,
    meet again: after removing `x`, the rule for `y` still fires, by
    monotonicity, and the reverse. Lean: `killStep_locallyConfluent`;
  - **one result**: by Newman's lemma, a terminating, locally confluent
    system ends in one state from each start, whatever order the steps take.
    Lean: `newman`, `kill_unique_normal`;
  - **least fixpoint**: that state is the smallest set of removed rows that
    contains the start and leaves no rule firing. Lean: `normal_least`.
- No pair of rules needs checking on its own: monotonicity covers every
  pair, and any rule added later that is monotone.
- **Why unique values are judged once**: the unique rule isn't monotone,
  since taking the winner out stops it firing for the loser. Applied as one
  more rule, it can end two ways:
  - Ana deletes folder 0; note 1 is in it, under cascade; note 2 is not; both
    are titled "Plan", note 1's claim first;
  - cascade first takes note 1 out, and note 2 then has no rival: note 2
    stays;
  - unique first takes note 2 out, then cascade takes note 1 out: neither
    stays.
  - Lean: `unique_with_others`.
- **[§8](#8-merge)'s three steps have one result**: steps 1 and 3 have one result each
  by monotonicity, and step 2 is a function of step 1's result. In the
  example above, step 1 takes note 1 out, so note 2 keeps "Plan" on every
  device. Lean: `stratified_unique`, `judged_once`.
- **Any order**: coven's run of the rules is one of the orders the three
  steps allow, so every order gives the same removed rows. Lean:
  `removal_stratified`, `any_order_removal`.
- **When a unique loser comes back**: when its winner is deleted, or taken
  out in step 1. A winner taken out in step 3 doesn't bring it back.
  - E.g. notes are unique by title, and a sub-note points at its parent
    under cascade. Note 1 is "Ideas"; at 10:00 Ana adds note 2, "Plan", as a
    sub-note of note 1; at 11:00 Ben, not having seen it, renames note 1 to
    "Plan".
    - Step 2: note 2's claim from 10:00 keeps "Plan"; note 1 is taken out.
    - Step 3: note 2 goes with its parent, by cascade.
    - Note 1 stays out: back, it would bring note 2 back, and lose to it
      again. Lean: `example_8_5_subnote`.
  - E.g. the same, but note 2 is a sub-note of note 3, "Ideas" since 12:00,
    and note 4 has been "Ideas" since 09:00.
    - Step 2: note 1 loses "Plan" to note 2, and note 3 loses "Ideas" to note
      4.
    - Step 3: note 2 goes with note 3.
    - Note 1 stays out: it lost in step 2, when note 2 was present. Lean:
      `example_8_5_step3`.
- **Every removed row names a rule**: a row removed by a run of monotone
  rules still meets a rule once the run ends. Lean: `star_fires`,
  `removed_has_rule`.
  - E.g. todos need `start <= end`, and todo 7 is in list 3. Ana deletes
    list 3 while Ben moves todo 7's start past its end. Todo 7's `coven_lost`
    row names the foreign key and the CHECK, on every device, whichever rule
    a device ran first. Lean: `example_8`.
- **End to end**: the merged state converges ([B6](#b6-proof-for-the-merged-state)), and the rules, and
  therefore the app's tables and the removed rows' `coven_lost` rows, are
  functions of it. Lean: `device_converges`, `rule_order_converges`.
- E.g. Lean runs the writes of [§8.4](#84-foreign-keys), [§8.5](#85-keys-and-uniqueness) and [§8.6](#86-check-constraints), with the rules reading
  the merged state it computes, in both arrival orders:
  - [§8.4](#84-foreign-keys): Carol's tablet applies Ana's delete of note 43, then Ben's tag 9
    on it: tag 9 is taken out, and its `coven_lost` row names the foreign
    key. Ben's move of tag 9 to note 44 arrives: tag 9 is back. Ben's phone,
    which gets Ana's delete last, never takes it out. Lean: `example_8_4`.
  - [§8.5](#85-keys-and-uniqueness): Ana renames the tag "urgent" to "important" while Ben tags note 44
    "urgent". Note 42 ends tagged "important", and Ben's `(44, "urgent")` is
    taken out, naming the foreign key, in either order. Lean:
    `example_8_5_key`.
  - [§8.5](#85-keys-and-uniqueness): notes are unique by folder and title. Ana adds note 1, "Plan" in
    Work, at 10:00; Ben renames note 2 "Plan" at 11:00; Carol moves note 2 to
    Work at 12:00. Note 2's claim dates from 12:00, so note 1 keeps the
    value. Lean: `example_8_5_stamp`.
  - [§8.6](#86-check-constraints): Ana sets start 10 while Ben sets end 8: the row is taken out,
    naming the CHECK. Ben's later end of 20 brings it back. Lean:
    `example_8_6`.

### B9 Audiences

- A row is one table, key and audience, with its own generations ([§14.2](#142-moving-rows)).
  Each row change sits in the part of its row's audience; a device applies
  the parts it can read and counts the rest as applied ([§14.4](#144-writes)).
- E.g. Ana moves note 1 into her circle, while Ben, outside it, re-adds note
  1 in the store, and then Carol, in the circle, edits the circle's note 1.
  - The store's note 1 is deleted by Ana's move and re-added by Ben: it ends
    at generation 3 with Ben's values, on Carol's device and on Dan's, who
    is outside the circle.
  - The circle's note 1 is a row of its own, at generation 1 with Carol's
    edit; only circle members have it.
  - Lean: `Moved.agree`.
- **Same audiences**: the writes as a device sees them still meet [B3](#b3-model-and-assumptions)'s
  assumptions, since a change's generation was reached by a change to the
  same row, in the same audience. So two devices that read the same
  audiences hold the same merged state. Lean: `valid_project`,
  `audience_converges`.
- **Different audiences**: a row's merged state depends only on the changes
  to that row, so devices agree on the merged state of every row both read.
  Lean: `fold_atRow`, `audiences_agree`.
- **Removals on devices with different rows**: two devices take out the same
  rows among any set of rows both have that holds, for each of its rows:
  - every parent its references point at;
  - every row whose unique claim rivals its own;
  - if it is an ancestor, every row that keeps it, and the ancestor those
    rows take their audience from.
  - Lean: `removal_local`.
- [§14.5](#145-references) gives the parents: a row points only at rows every reader of it can
  read.
- [§14.1](#141-roots-descendants-and-ancestors) gives the rivals: a unique constraint on a root table includes the
  audience column, so rivals share an audience; constraints that could span
  audiences are refused. Lean: `rivals_closed`.
- Two rules read rows outside that set, so devices that read different
  circles can differ on them:
  - **ancestors**: a device shows an ancestor only while a present shared row
    that device can read points at it ([§14](#14-audiences)).
    - E.g. a label worn only by a todo in Ana's circle is a store row. Ana's
      device shows it; Dan's, outside the circle, takes it out, and with it
      the label's cover image, a store row in an asset table whose audience
      comes from the label. Lean: `AncestorAcross.differs`.
  - **a key present in two audiences**: the store's row wins over a
    circle's; of two circles' rows, the one whose insert has the smaller
    timestamp wins; the other is taken out like a unique loser, in step 2
    ([§14.2](#142-moving-rows)).
    - E.g. in the move above, Carol's device has note 1 in the store and in
      the circle. The store's shows; the circle's is taken out, naming the
      unique rule. Dan's device has only the store's, which shows. Lean:
      `Moved.store_wins`.
    - The store's row never loses, so devices agree on every store row.
    - A device in two circles can show a different circle row for one key
      than a device in only one of them.
- **Fingerprints** ([§19.1](#191-noticing)): with the ancestor rule and the rule for a key in
  two audiences left out, two devices take out the same rows in every
  audience both read. So a fingerprint that leaves out what those two rules
  decide, including the rows taken out with the rows they take out, is the
  same on every device that applied the same writes. Lean:
  `fingerprint_local`, `AncestorAcross.fingerprint_agrees`.
  - E.g. without the ancestor rule, Ana's device and Dan's both show the
    label and its cover image.

### B10 The spec's examples, checked

- [§8](#8-merge): todo 7 names both rules. Lean: `example_8`.
- [§8.1](#81-example): both orders end with "Shopping" and one lost value. Lean:
  `example_8_1`.
- [§8.2](#82-concurrent-writes-to-one-row): "Groceries" is lost on Carol's tablet until Ben's write 9 arrives.
  Lean: `example_8_2`.
- [§8.3](#83-deletes): Ben's edit is replaced by Ana's write 7 in every order. Lean:
  `example_8_3`.
- [§8.4](#84-foreign-keys): tag 9 is taken out, then back when Ben's move arrives; link 6 holds
  null. Lean: `example_8_4`.
- [§8.5](#85-keys-and-uniqueness): a key change leaves Ben's `(44, "urgent")` taken out; note 2's claim
  dates from 12:00; a loser whose winner leaves in step 3 stays out. Lean:
  `example_8_5_key`, `example_8_5_stamp`, `example_8_5_subnote`,
  `example_8_5_step3`.
- [§8.6](#86-check-constraints): the row is taken out, then back when Ben sets end 20. Lean:
  `example_8_6`.
- [§14.2](#142-moving-rows): a label whose last shared todo leaves stays while Ben's new shared
  todo wears it, on every device; the store's note 1 wins over the circle's.
  Lean: `example_14_2`, `Moved.store_wins`.

### B11 What Lean checks, and what is prose

- Lean checks every theorem named above, with no unproven step. The axioms
  they use are only Lean's own: `propext`, `Classical.choice`, `Quot.sound`.
  `CovenMerge/Axioms.lean` prints them.
- What Lean models abstractly:
  - a write's values: a cell's value is the write that set it;
  - the removal rules' inputs: any function of the merged state, in the
    shape of [B7](#b7-the-removal-rules). The examples compute them from the merged state Lean
    builds.
- Argued here only:
  - that coven computes the rules' inputs from the merged state as [B7](#b7-the-removal-rules) says:
    presence from generations, stale references by comparing generations,
    null or the default for a set null or set default reference whose
    parent's generation was deleted, CHECK on merged values, claim stamps
    from the cells' writes;
  - that two devices compute the same inputs for a row when the merged
    states of the rows those inputs read agree, which [B9](#b9-audiences)'s locality theorem
    then uses;
  - local triggers: they converge when they compute a function of the
    current rows, since every change coven makes is ordinary SQL ([§8.7](#87-triggers));
  - schema changes ([§17](#17-schema-changes)), files ([§16](#16-files)) and resets ([§19.3](#193-resetting-a-store)), which the model
    doesn't include.
