# Merge

Two devices edit the same store while apart. Once both have applied the same
writes, they must hold the same rows, whatever order the writes arrived in.
This page is how that works: the timestamps that decide which value a cell
keeps, what happens to the value that loses, deletes, and the rules the app's
SQL schema brings.

How writes travel and in what order devices apply them is the
[Sync](/docs/sync-model) page; this one starts where a write is being applied.
Every device merges every write the same way, its own included, and applies a
write only after everything that write had read
([Order](/docs/sync-model#order)).

<svg width="0" height="0" style="position:absolute" aria-hidden="true"><defs><marker id="fa" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="amf"/></marker><marker id="fam" markerWidth="8" markerHeight="8" refX="7" refY="4" orient="auto" markerUnits="userSpaceOnUse"><path d="M0,0L8,4L0,8Z" class="ammf"/></marker></defs></svg>

## Timestamps

Every write carries a timestamp: the wall-clock time in milliseconds, a
counter, and the device's id, compared in that order. Ana's write 3 is stamped
`2026-10-02 12:00:00.000 #0`: 12:00:00.000 is the time, `#0` the counter. No two
devices produce equal timestamps, because their ids differ.

A device never stamps a write earlier than anything it has already seen. Each
device keeps, on disk, the latest timestamp it has seen, from its own writes
and every write it has applied. To stamp a new write, it uses the wall clock if
the clock is past that latest time, with counter 0; otherwise it uses the
latest time with the counter raised by one. So Ben's edit made after reading
Ana's always beats Ana's, even if Ben's clock is slow.

At 13:00, Ana's phone, whose clock is fast, stamps a title change at
13:01:00.000 #0 and claims slot 21. Ben's phone reads slot 21 at 13:00:20 and,
at 13:00:30, changes the title again. Its clock reads 13:00:30, behind Ana's
stamp, so it stamps 13:01:00.000 #1. Ben's value wins over Ana's because of
the counter.

A device whose clock jumps a year ahead drags everyone's timestamps forward
until real time catches up, but nothing freezes and nothing is refused: a
write applies as soon as what it read has applied, whatever its timestamp says.
A clock past the year 10889, the last time 48 bits of milliseconds hold, fails
the write with `DbError::ClockOutOfRange`.

The app has no timestamp column to keep. Coven stamps the write as it commits.

## Cells

A *cell* is one column of one row: note 42's title. Of two concurrent values
for a cell, the one with the larger timestamp stays. A cell only one write
touched keeps that write's value. So if at 15:00 Ben retitles note 42 while
Ana, offline, edits its body, both edits survive on every device:

|                | title              | body                |
|----------------|--------------------|---------------------|
| Ana's write 6  | ·                  | "milk, eggs, bread" |
| Ben's write 10 | "Weekend shopping" | ·                   |
| result         | "Weekend shopping" | "milk, eggs, bread" |

<svg class="flow" viewBox="0 0 660 190" role="img" aria-label="Base row; Ana edits the body, Ben edits the title; the merged row holds both edits">
<text class="sub" x="330" y="20" text-anchor="middle">note 42 before</text>
<rect class="chip" x="205" y="28" width="125" height="28" rx="7"/>
<text class="lbl s11" x="267" y="46" text-anchor="middle">title: “Groceries”</text>
<rect class="chip" x="330" y="28" width="125" height="28" rx="7"/>
<text class="lbl s11" x="392" y="46" text-anchor="middle">body: “milk, eggs”</text>
<line class="arrd" x1="240" y1="62" x2="140" y2="92" marker-end="url(#fam)"/>
<line class="arrd" x1="420" y1="62" x2="520" y2="92" marker-end="url(#fam)"/>
<text class="sub" x="120" y="84" text-anchor="middle">Ben edits the title</text>
<text class="sub" x="540" y="84" text-anchor="middle">Ana, offline, edits the body</text>
<rect class="chipa" x="35" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="97" y="116" text-anchor="middle">“Weekend shopping”</text>
<rect class="chip" x="160" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="222" y="116" text-anchor="middle">“milk, eggs”</text>
<rect class="chip" x="375" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="437" y="116" text-anchor="middle">“Groceries”</text>
<rect class="chipa" x="500" y="98" width="125" height="28" rx="7"/>
<text class="lbl s11" x="562" y="116" text-anchor="middle">“milk, eggs, bread”</text>
<line class="arr" x1="160" y1="132" x2="270" y2="158" marker-end="url(#fa)"/>
<line class="arr" x1="500" y1="132" x2="390" y2="158" marker-end="url(#fa)"/>
<text class="sub" x="330" y="148" text-anchor="middle">merge</text>
<rect class="chipa" x="205" y="160" width="125" height="28" rx="7"/>
<text class="lbl s11" x="267" y="178" text-anchor="middle">“Weekend shopping”</text>
<rect class="chipa" x="330" y="160" width="125" height="28" rx="7"/>
<text class="lbl s11" x="392" y="178" text-anchor="middle">“milk, eggs, bread”</text>
</svg>

All the cells one write sets share its timestamp, so between two writes, the one
with the larger timestamp wins every cell they both set.

The order of arrival doesn't matter. Note 42 is titled "Grocery list". Ana's
write 4 sets "Groceries" (13:01:00.000 #0), Ben's write 9 sets "Weekly
groceries" (13:01:00.000 #1, having read Ana's), and Carol, offline since
12:30, sets "Shopping" at 14:00 (14:00:00.000 #0) and uploads it at 14:30. Ben's
phone applies them in the order Ana, Ben, Carol; Carol's tablet applies its own
write first, then Ana's and Ben's. Both end with "Shopping": each cell holds the
value of the largest-stamped write applied.

## Lost values

"Weekly groceries" didn't survive, and Carol, who replaced it, had never seen
it. Coven records it as a *lost value*: which cell, the value, which write set
it, and which write replaced it. The app can show it ("Ben's title was
replaced") and offer to restore it.

The rule: **a value is lost when some write replaced it, and no write that
replaced it had read it.** A value replaced by someone who had seen it is not
lost: that was an edit, not a conflict.

| Value              | Set by          | Replaced by            | Lost?                 |
|--------------------|-----------------|------------------------|-----------------------|
| "Grocery list"     | Ana's write 3   | Ana 4, Ben 9, Carol 2  | no: all had read it   |
| "Groceries"        | Ana's write 4   | Ben 9, Carol 2         | no: Ben 9 had read it |
| "Weekly groceries" | Ben's write 9   | Carol 2                | yes                   |
| "Shopping"         | Carol's write 2 | nothing                | the current value     |

Every device ends with the same lost values, because they follow from the
writes alone. The app reads them with `handle.lost_values()` or
`subscribe_lost_values()`. Each [`LostValue`](rustdoc:struct:coven::LostValue)
names the table and key, the lost cell or whole row with the write that set
each value, and what replaced it:

```rust
pub enum Replacement {
    Write(WriteId),                                          // a write that hadn't read it
    Rules(Vec<RemovalRule>),                                 // the row was taken out (below)
    SchemaChange { version: u32 },                           // a breaking schema change
    DataDependency { write: WriteId, sources: Vec<ChangeSource> }, // it built on dropped data
    NotAuthorized { write: WriteId, reason: RowAuthorizationFailure }, // owner rules
}
```

`handle.dismiss_lost_values(&values)` drops values the app has dealt with, on
every device: dismissal is itself a write. To put a value back, the app writes
it again like any edit.

A value stops being lost only when a later write that had read it replaces it
in place: it sets the same cell, or deletes that row. Changing another cell of
the row leaves it lost.

## Deletes

Each row has a *generation*: 1 when created, 2 when deleted, 3 if added again,
and so on. Each row change records the generation the row had on the device
that made it. Editing a row's columns never changes its generation.

**An edit made at a generation that has since been deleted loses, whenever it
arrives.** Ana deletes note 43 at 16:00. Ben, offline, edits its title. Ana
re-adds note 43 at 17:00. Ben's edit was made at generation 1, so it doesn't
bring the note back, and it doesn't reach generation 3, even if his clock
stamped it later than 17:00. It becomes a lost value replaced by Ana's delete.

Either arrival order gives the same result. If the edit arrives after the
delete, coven sees it was made at an older generation. If it arrives before,
the delete records as lost the cells set by writes it hadn't read, before the
row goes.

Two concurrent deletes of the same generation count as one. Concurrent re-adds
both start the same next generation, and their cells merge as above.

## Rows taken out, and rows that come back

Some conflicts can't be settled cell by cell. Coven settles them by *taking a
row out*: removing it from the app's table while a reason holds, and recording
it as a lost value with all its columns. It is never stored as a delete, so it
comes back when the reason goes away. The reasons are:

- its parent was deleted after it pointed at it ([foreign keys](#foreign-keys));
- its merged values fail a CHECK ([CHECK constraints](#check-constraints));
- it lost a unique value to an earlier claim ([unique values](#unique-values));
- its circle was deleted ([Circles](/docs/circles#deleting-a-circle));
- the same key is present in two audiences, and the store's row wins over a
  circle's ([Circles](/docs/circles#moving-rows)).

Todos need `start <= end`, and todo 7 is in list 3. Ana deletes list 3 while
Ben moves todo 7's start past its end. Todo 7 is out for both reasons, on every
device, and its lost-value record names both
([`RemovalRule`](rustdoc:enum:coven::RemovalRule)). It comes back only when
neither holds.

While a row is out, later edits to it update its record. When a write changes
which rows are out, coven changes the app's table with ordinary SQL, so the
app's triggers and live queries see it like any other change.

The app can offer a taken-out row back in two ways:

- **Insert the same key again** with values that clear its reasons. Coven
  records the insert as an update of the row that is out; if the new values
  pass, every device puts the row back. A copy under a new key would show twice
  if the original's reason later went away.
- **Dismiss it** with `dismiss_lost_values`, which records a real delete, so it
  never comes back.

## The rules SQL brings with it

The app's schema has foreign keys, unique constraints, CHECKs and triggers.
Each device's SQLite enforces them on its own writes. Merging writes made on
different devices can break them, so coven decides each case the same way on
every device. Some schemas can't be decided the same way everywhere; coven
refuses those when the database opens and after every migration, with an error
naming the table and constraint.

### Foreign keys

A foreign key makes one row point at another, its *parent*. Attachments point
at notes with cascade; links point at notes with set null.

**On the device that deletes the parent,** SQLite runs each child's
foreign-key action, and the write records the results. Carol added attachment 8
to note 43 at 15:30, and every device has it. At 16:00 Ana deletes note 43; her
write deletes attachment 8 and sets link 5's `note_id` to null, and those
changes merge like any others.

**A child the deleting device didn't have,** because it was added
concurrently, points at a deleted parent on every device. Ben, offline, adds
attachment 9 to note 43. When it arrives where note 43 is gone, coven follows
the foreign key's declared action:

- **cascade, restrict or no action:** attachment 9 is taken out and recorded as
  lost, as SQLite would have done or refused;
- **set null:** it stays, and its reference reads as null, as SQLite would have
  made it.

A taken-out child comes back if its reference is later pointed at a parent
that exists. If Ben then moves attachment 9 to note 44, every device ends with
it on note 44 and nothing lost.

Re-adding a deleted parent doesn't bring back the children taken out with it:
they point at its old generation. Re-adding is a new insert; children the app
wants back, it inserts in the same write.

A reference of several columns merges as one value: the write with the larger
timestamp wins the whole tuple, so `FOREIGN KEY(a, b)` never ends up with one
write's `a` and another's `b`.

Coven refuses, naming the table and foreign key:

| Refused | Why |
| --- | --- |
| `SET DEFAULT` on a foreign key of a synced table, or into one (`SchemaError::SetDefault`) | the default parent merges like any row and can itself be gone |
| set null on a `NOT NULL` column (`SchemaError::ImpossibleAction`) or a key column (`SchemaError::PrimaryKeyAction`) | SQLite could never apply it, or it would change the row's key |
| a foreign key of a synced table that doesn't reference the parent's whole primary key (`SchemaError::ForeignKeyNotPrimaryKey`) | a row's identity can't follow a value that changes |
| a foreign key that is one generated column (`SchemaError::GeneratedForeignKey`) | merging its inputs cell by cell could name a parent no write named |
| a foreign key from a local table into a synced table whose delete action isn't cascade, or set null on a nullable column (`SchemaError::LocalChildAction`) | a local row must never stop coven deleting a synced one |
| a foreign key from a local table into a synced non-key column whose update action isn't cascade or set null (`SchemaError::LocalChildUpdate`) | coven applies other devices' changes as `UPDATE`s, which the local key must follow |

`pins(note_id REFERENCES notes ON DELETE RESTRICT)` in a local table is
refused: Ben's phone, holding a pin, couldn't take out a note that Ana's phone
could, and the two would differ.

### Key changes

Changing a row's primary key is a delete of the old row plus an insert of the
new one, on every device. On the device changing the key, SQLite runs each
child's `ON UPDATE` action. A child that device didn't have sees its parent
deleted and follows its `ON DELETE` action. Ana renames the tag "urgent" to
"important" while Ben, offline, tags note 44 "urgent": every device ends with
note 42 tagged "important", and note 44's "urgent" tag taken out and recorded
as lost, so the app can offer to tag it again.

### Unique values

On one device, SQLite refuses a write that repeats a unique value, so two rows
can claim one value only through concurrent writes. Ana and Ben both create a
note titled "Groceries". When both arrive, **the first slot wins**: Ana's note
claimed slot 40 and Ben's slot 41, so Ana keeps the title and Ben's note is
taken out and recorded as lost. That holds even if Ben wrote his note earlier,
offline. If Ana later renames hers, the title goes to the earliest remaining
claimant, and Ben's note comes back.

Slots decide this, not timestamps, so a device with a wrong clock can't steal a
value by backdating. A write still waiting in a device's queue has no slot yet
and ranks after everything published, so a row created locally can lose its
unique value before it is sent.

A unique constraint or shared key must not span [audiences](/docs/circles#unique-values-don-t-cross-audiences):
a device outside a circle can't see the circle's claims.

### CHECK constraints

A CHECK that reads one column can't fail after a merge, since each value passed
it on the device that wrote it. A CHECK that reads several can. Todos need
`start <= end`. Ana moves the start to 10 while Ben, offline, moves the end
to 8:

|             | start | end |       |
|-------------|-------|-----|-------|
| before      | 5     | 12  |       |
| Ana's write | 10    | ·   |       |
| Ben's write | ·     | 8   |       |
| merged      | 10    | 8   | fails |

Once both have arrived, the row is taken out and recorded as lost on every
device. It comes back when a later write makes it pass, such as Ben setting
`end` to 20.

### Triggers

The app declares each trigger on a synced table as **local** or **shared**.
A trigger not declared shared is local.

A **local** trigger runs on every device, for its own writes and applied ones
alike, and writes only local tables. A search index is a local trigger:

```sql
CREATE TRIGGER notes_title_index AFTER UPDATE OF title ON notes
BEGIN
  UPDATE title_index SET title = new.title WHERE note_id = new.id;
END;
```

When Ben's phone applies Carol's title change, the trigger updates Ben's index.

A **shared** trigger runs only on the device making the write, writes only
synced tables, and its changes become part of that write. Setting `edited_at`
when a body changes is a shared trigger:

```sql
CREATE TRIGGER notes_edited_at AFTER UPDATE OF body ON notes
WHEN NOT coven_applying()
BEGIN
  UPDATE notes SET edited_at = datetime('now') WHERE id = new.id;
END;
```

```rust
SyncedTable::new("notes", RowIdentity::IndependentUuid).shared_trigger("notes_edited_at")
```

Other devices apply the trigger's changes without running it: the write already
holds what it did. SQLite can't turn one trigger off, so coven provides the SQL
function `coven_applying()`, true while coven itself puts rows into their
merged state: applying another device's write, loading a snapshot, and taking
rows out or putting them back as this device's own write merges, which every
device does alike from the write's record. It is false during the app's own
SQL and the triggers that SQL fires. A
shared trigger must say `WHEN NOT coven_applying()`; opening refuses one that
doesn't (`SchemaError::SharedTriggerGuard`). A trigger that writes the wrong
kind of table fails its statement with `DbError::TriggerTarget`.

A value a shared trigger derives can be wrong after a merge, because its writes
merge like any other. If a shared trigger counts a note's attachments, and Ana
and Ben each add one offline, both writes set the count from 1 to 2 while the
note has 3. A local trigger writing a local table counts 3 on every device.

### Dependent changes

Some changes only make sense on top of data that turned out not to count. Ben,
offline, creates note 50 in write 20, then retitles it in write 21. If write 20
is dropped when it arrives, for example because a schema change excluded it,
write 21's retitle has nothing to stand on. Coven drops that change too and
records it as a lost value naming write 20 as its cause
(`Replacement::DataDependency`).

Dependence is decided by data: the write that supplied the row's generation
and the writes that set each cell the change replaced. Having merely read a
dropped write creates no dependence, and the write's other changes apply by the
ordinary rules.

### Owner tables

A table declared `owner_only()` keeps each row's changes to the member who
created it. Only that member may update or delete it directly. This is for
authenticity, not secrecy: "Ana rated this" really was Ana.

```rust
SyncedTable::new("ratings", RowIdentity::IndependentUuid).owner_only()
```

The owner comes from coven's own record of the authenticated member whose write
created the row, never from an app column. When several members create the same
shared key, the lowest slot wins, and later claims never change an established
owner.

On the author's own device, a write that would change someone else's owned row
fails with `DbError::NotOwner` and commits nothing. A write that arrives from
another device with such a change applies its other changes; the unauthorized
one becomes a lost value naming the owner (`Replacement::NotAuthorized`).

Foreign-key actions caused by an authorized change still apply. Ben deletes a
track that Ana's review points at: with cascade, Ben's write deletes the review
too, and every device checks that this deletion is exactly what the schema says
Ben's delete causes. Ben can never edit Ana's review directly.

[Members and profiles](/docs/profiles) uses an owner table to keep each
member's profile in that member's hands.
