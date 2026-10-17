# Circles

Some rows are for only some members. Ana and Ben's Gifts list shouldn't be
readable by Carol, though all three share Household. A *circle* is a group of
members inside a store who share rows the others can't read.

The hard part is keeping SQL's rules working when some devices can't see some
rows: Carol's device must still check foreign keys and unique values, and
decide the same things, without reading Gifts. This page covers how rows get
their audience, the rules that keep that working, and how circles are run.

## Audiences

Every synced row belongs to one *audience*: the store, or one circle. A row in
the store reaches every member's devices; a row in a circle reaches only its
members' devices. Each audience has its own key, so the other members' devices
hold only encrypted bytes they can't open.

```rust
pub enum Audience {
    Store,
    Circle(CircleId),
}
```

A [`CircleId`](rustdoc:struct:coven::CircleId) is a random, permanent UUID;
renaming a circle never changes it.

## Declaring audiences

The app says, for each synced table, how its rows get their audience:

- a **root** table names an audience column, never NULL, holding `store` or a
  circle's id: `audience_column("audience")`;
- a **descendant** table names one foreign key, and each row takes the audience
  of the row it points at: `audience_from("list_id")`. A todo is in its list's
  circle;
- any other table is in the store, every row of it.

```rust
vec![
    SyncedTable::new("lists", RowIdentity::IndependentUuid).audience_column("audience"),
    SyncedTable::new("todos", RowIdentity::IndependentUuid).audience_from("list_id"),
    SyncedTable::new("todo_labels", RowIdentity::SharedKey)
        .key_columns(["todo_id", "label_id"])
        .audience_from("todo_id"),
    SyncedTable::new("labels", RowIdentity::SharedKey),
]
```

Bind audiences as [`Audience`](rustdoc:enum:coven::Audience) or `CircleId`
values; coven writes them as canonical text, `store` or the circle's UUID in
lowercase with hyphens. Any other spelling, uppercase included, fails with
`DbError::InvalidAudience`, because two spellings of one circle would otherwise
look like two audiences.

A descendant's foreign key must be one column, into a synced table, not set
null (the row would lose its audience), and part of a chain that ends at a root
or a store table. A table can't declare both forms. Coven checks these when the
database opens and after each migration (`SchemaError::AudienceForeignKeyColumns`,
`AudienceForeignKeyTarget`, `AudienceForeignKeyAction`, `AudienceCycle`,
`TwoAudiences`).

## Unique values don't cross audiences

A device outside a circle can't see the circle's claim to a unique value, so it
would decide differently. So a unique constraint, or a shared key, must stay
inside one audience:

- **on a root table** it must include the audience column, as in
  `UNIQUE(audience, title)`: the same title in the store and in a circle are two
  separate claims;
- **on a descendant table** it must include the foreign key the table takes its
  audience from.

`note_tags(note_id, tag_id)` is allowed: only someone who can see note 42 can
tag it. `UNIQUE(file_name)` on attachments is refused: Ana's `plan.pdf` on a
Gifts note and Ben's on a store note would clash on Ana's device but not on
Carol's. `UNIQUE(note_id, file_name)` is allowed. The error is
`SchemaError::AudienceConstraint`, naming the table and constraint.

## References stay readable

A row may point only at rows everyone who can read it can also read:

- a circle's row may point at rows in the same circle, or the store's;
- a store row may point only at store rows.

So every device that reads a row can check its foreign keys. Carol also has a
circle of her own, Recipes:

```
store     notes  row 42   "Groceries"
Gifts     pins   row 3    note → 42        allowed
store     links  row 10   pin → 3          refused: Carol can't read pin 3
Recipes   pins   row 2    pin → 3          refused: Carol, in Recipes, can't read pin 3
```

A write that breaks this fails on the device making it with
`DbError::ReferenceAudience`. That includes a move: moving note 42 into Gifts
while store link 5 still points at it fails, since link 5 would point at a row
Carol can't read. Which circle a row is in never depends on who is in the
circle, so a change of members never breaks a reference.

## Moving rows

A row moves when its root's audience column changes, or when a descendant is
pointed at a parent in another audience. A move is an ordinary write, and it
commits at once:

```rust
handle.write(move |sql| {
    sql.execute("UPDATE notes SET audience = ?1 WHERE id = ?2", (gifts, note))?;
    Ok(())
}).await?;
```

A move is a delete of the root and its descendants in the old audience and a
full insert of them in the new one. Carol's devices see note 42 and its
attachments deleted; Ana's and Ben's insert them in Gifts. Files don't move:
each file's key travels in its row.

**One key in two audiences.** Ana moves note 1 into Gifts; Carol, outside it,
sees it deleted and adds note 1 to the store again. The store's note 1 and
Gifts' note 1 are now two rows. A device that reads both shows one of them in
the app's table and takes the other out, like a unique value's loser: the
store's row wins over a circle's. Of two circles' rows, the one whose current
generation started with the smaller timestamp wins.

## Running a circle

Circle calls live on `handle.circles()`:

```rust
let circles = handle.circles();
let gifts = circles.create("Gifts").await?;          // this member is its first member
circles.add_member(gifts, &ben).await?;
circles.rename(gifts, "Birthday gifts").await?;
let mine = circles.list().await?;                    // the circles this member is in
let in_gifts = circles.members(gifts).await?;        // CircleMemberInfo { member, is_self }
circles.remove_member(gifts, &ben).await?;
circles.delete(gifts).await?;
```

Any member of the store can make a circle and is its first member. Only the
circle's own members rename it, add and remove its members, rotate its key and
delete it; an admin outside it can't. Each call is an admin operation
([Sharing](/docs/sharing#how-admin-changes-are-made)), so it needs storage, and
a change no longer allowed fails with `SyncError::Rejected`: `NotAllowed` for a
caller without authority, `TargetGone(..)` for a member, device or circle that
no longer exists. `subscribe_list` and `subscribe_members` are the live forms.

`circles.members(circle)` lists only members who are still in the store.

## Writes into circles

A write can touch several audiences. Ana's write adds a store note and a Gifts
pin: its slot carries one *part* per audience, each encrypted with its
audience's key, under one signature.

Every device opens the slot's header with the store key and checks the whole
signature, including parts it can't read. Parts for audiences this member isn't
in are passed over without being opened: not being in Gifts is not a missing
key. A missing key for a part the device *should* read stops it at that slot
([Sync](/docs/sync-model#a-stuck-device-stops)).

A write into a circle the member isn't in, or one that doesn't exist, fails on
the writing device with `DbError::NotInCircle`.

## Joining a circle

Ana adds Dan to Gifts. Dan needs two things: Gifts' older keys, which Ana's
device sends him before the entry adding him, and the Gifts history his devices
passed over unopened while he was outside. So Dan's laptop loads Gifts'
snapshot and the slots after it, then commits Gifts' data. Until that commits,
writes into Gifts fail with `DbError::AudienceReloading`; other audiences stay
writable.

A newly made circle has no history, so its first members load nothing.

## Leaving a circle

Ana removes Ben from Gifts. Ben keeps the rows and keys he already had. The
remaining members' devices replace every Gifts key Ben was sent before writing
new Gifts data, so he can't read anything written after.

Edits Ben made before he read his removal still count: he may send Gifts parts
he queued earlier, sealed with the old key he already held, which shows him no
new data. Once his device reads the removal, new writes into Gifts fail with
`NotInCircle`. Removing Ben from the store, rather than from the circle, applies
the full [cut-off](/docs/sharing#the-cut-off).

A circle left empty is deleted at that slot, whether its last member left it or
was removed from the store.

## Deleting a circle

Any member of a circle can delete it with one admin entry. From that slot on,
its rows are taken out on every device that read them, including late edits
made before the deletion that arrive afterwards. Their lost values name the
deletion entry (`RemovalRule::DeletedCircle`), so the app can offer their values
in another audience, keeping their keys. A new local write into the circle
fails with `DbError::DeletedCircle`. A deletion is never reversed; making
another circle gives it a fresh id.

Deleting a circle doesn't delete its files from storage by itself: lost values,
snapshots and queued writes that still name them keep protecting them.

## What other members can see

Circles hide contents, not structure. Everyone in the store can see that a
circle exists, its name, who is in it, who writes to it, when, and how much:
admin entries and each slot's header, which names its parts' audiences, are
sealed with the store key, which every member holds. Key copies are routed in
the clear, so the provider sees circle ids too
([Encryption](/docs/encryption#what-the-provider-sees)). Only a circle's members
read its rows and its files.

When two groups of people must not learn anything about each other, not even
that the other exists, that is two stores, not two circles in one store.
