# Schema evolution

The app owns its schema and changes it with migrations. But Ana's phone may run
the new version for a week before Ben's laptop updates, and Ben keeps editing
offline in the meantime. Every device has to agree on what the data looks like
after the change, and Ben's edits made in the old shape have to land somewhere.

## Migrations

The app's schema is a list of migrations numbered from 1 with no gaps:

```rust
fn migrations() -> Vec<Migration> {
    vec![
        Migration::addition(1, "initial", include_str!("migrations/001_initial.sql")),
        Migration::breaking(2, "rename_attachment_title", "ALTER TABLE attachments RENAME COLUMN title TO name",
            OldWrites::Convert(std::sync::Arc::new(|change| {
                if change.table == "attachments" {
                    change.rename_column("title", "name");
                }
                Ok(())
            }))),
    ]
}
```

`Migration::addition` and `Migration::breaking` run SQL; their `addition_with`
and `breaking_with` forms run code with a
[`MigrationContext`](rustdoc:struct:coven::MigrationContext). Breaking migrations
require an [`OldWrites`](rustdoc:enum:coven::OldWrites) choice. Opening runs every
migration above the database's version in one transaction, then checks the schema rules against the
declared tables, which describe only the newest version. If anything fails,
nothing changes. A database newer than the app's migrations fails with
`MigrationError::SchemaTooNew`; numbering that isn't 1 to N fails with
`MigrationError::NotContiguous`.

Coven's own tables migrate first, automatically, every time a device opens for
writing ([below](#coven-s-own-schema-and-format)).

## Additions and breaking changes

Every write records the schema version it was made with. Coven sorts each
migration into one of two kinds by comparing the schema before and after it,
together with the app's table declarations:

| The migration | Kind |
| --- | --- |
| adds only new tables or new columns | addition |
| adds an index, a constraint or a foreign key, or anything else | breaking |
| changes a table's `owner_only` declaration | breaking |
| drops or renames a table or column | breaking |
| inserts, updates or deletes rows of synced tables | breaking, even if it only adds tables or columns |
| changes only local tables, or views | neither: they don't sync |

A migration that changes rows is breaking because each device would otherwise
compute those changes from its own rows, and the devices would disagree. Its
statements decide its kind, never the rows a device happens to hold.
Coven checks the constructor's declaration against this classification. Either
mismatch returns `DbError::MigrationKindMismatch { version }` and rolls back
all pending migrations. Use `addition` for local-only changes too.

## Additions

Ana's app adds `color` to notes. Devices on the old version keep working, and
their writes apply everywhere; they just don't set `color`. Ben, on the old
version, can still edit titles locally. Once his laptop reaches a slot written
at the newer version, it stops there with `UpdateRequired`
([Sync](/docs/sync-model#a-stuck-device-stops)) and waits for his app update
before his queued titles can reach Ana.

## Breaking changes and the raise

A breaking change raises the store's schema version, which every device shares.
Whichever device updates first makes the raise:

1. it runs the migration;
2. it takes a snapshot, at the new version, of every audience it can read;
3. it announces those snapshots;
4. it claims the raise: one admin entry per audience.

Every other device reloads from the raise's snapshot when it applies the raise,
once its own app has updated. While that reload runs, writes into the audience
fail with `DbError::AudienceReloading`. A device on an older app stops at the
raise with `UpdateRequired`, which also stops it uploading; after updating it
reloads and sends its waiting old-version writes, unchanged.

For a circle the raising device can't read, the first device of one of that
circle's members to update writes the circle's snapshot and raises it.

If Ana and Ben update on the same afternoon and both prepare the raise, the
first claim establishes it. The other device adopts that snapshot and undoes
its own unused work.

### The migration write

Everything a breaking migration changes in the synced tables is one write by the
device that runs it: its *migration write*. It holds what differs between the
synced tables before and after: rows inserted or deleted, and cells whose value
changed. A cell the migration left as it was keeps the write that set it, so a
late write competes with that write, not with the migration. A renamed column
keeps its cells' writes under its new name.

Ana's migration renames `title` to `name` and fills a new column `slug` from it.
Each note's `name` keeps the write that last set its title, and each `slug` is
set by the migration write. The migration write reaches other devices only
inside the raise's snapshot.

## Late writes in the old shape

Writes made on the old version still arrive afterwards: Ben edited offline
before updating. A breaking migration must choose what happens to those writes.
`OldWrites::Convert` turns an eligible old-shape row change into the new shape:

```rust
Migration::breaking(2, "rename_attachment_title", "ALTER TABLE attachments RENAME COLUMN title TO name",
    OldWrites::Convert(std::sync::Arc::new(|change| {
        if change.table == "attachments" {
            change.rename_column("title", "name");
        }
        Ok(())
    })))
```

The callback gets a [`RowChange`](rustdoc:struct:coven::RowChange): its table,
key, operation and changed columns, each with old and new values. A write from
version 1 arriving at a version-3 device goes through the converters of
migrations 2 and 3, in order.

With a converter, Ben's late edit converts and applies on every device,
including his own once he adopts the raise. With an explicit `OldWrites::Drop`,
it is kept as a lost value in its old shape, naming the breaking version as its
cause (`Replacement::SchemaChange`), so nothing disappears silently. Such a lost value
is *frozen*: it keeps its old column names and values, takes no part in later
merges, and survives later raises. A converter added later can't revive it.

Coven never calls a converter on a change that couldn't count anyway: one the
owner rules refuse, or one built on data that was dropped
([dependent changes](/docs/merge#dependent-changes)). Those stay frozen in their
original shape, while the write's other changes convert and apply. A converter
may rename columns and add new ones; changing a reference's value, or adding a
column that is a reference, fails, because the device can't know which version
of the parent the old write meant. A failing converter aborts the receiving
transaction and leaves the previous state.

Nothing in an upload queue is ever rewritten. Ben's waiting old-version writes
go out exactly as he made them; receivers convert them.

## The migrating device's own queue

When Ana's phone migrates, its queue may already hold writes made at the old
version, and she keeps writing at the new one before the raise is claimed.
Migrating appends an empty record, the *migration marker*, behind the writes
already queued. The old-version writes and the marker go out first, unchanged
and in order; writes made after the marker wait until the raise applies. An
update that only adds columns, run while a raise still waits to publish, moves
that raise to the newer version.

## Lost values across a migration

A breaking migration deletes rows that are [taken out](/docs/merge#rows-taken-out-and-rows-that-come-back),
and records each as one frozen lost value, so its values survive. Dropping a
table or column doesn't dismiss its lost values either: Ana's migration drops
`notes.color` while Ben's lost value "blue" is still pending, and every device
keeps that lost value under `notes.color`, frozen. The app can still read and
dismiss it.

## Coven's own schema and format

Coven changes too. Its own tables in the app's database migrate in place each
time a device opens for writing, before the app's migrations; each internal
migration is atomic. A read-only open refuses tables that need migrating, and
any open refuses an internal schema newer than this coven supports
(`CovenMigrationError::SchemaTooNew`).

Everything coven writes to storage records the format version it was written in,
outside its encryption. An older coven that meets a newer object asks for an
update (`UpdateRequired`) instead of reporting damage, and carries on once
updated. A format change needs no admin entry, snapshot or reload. A write
already tried is retried in the format of its first attempt, so its bytes don't
change.
