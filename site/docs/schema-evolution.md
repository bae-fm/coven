# Schema evolution

A database image and a retained write have separate schema versions. Updating an
app migrates its local database, but does not rewrite the packages other devices
signed or the writes this device already captured. Coven interprets each retained
write under its authoring schema, then applies the host's registered changeset
transformations before merging it into the current database.

## The migration ladder

The host passes contiguous versions `1..=N` to the builder. Coven applies pending
steps over SQLite's `PRAGMA user_version` inside the database-open transaction.
A failure rolls back the pending migrations and their version updates together.
A database newer than the registered ladder is refused with
[`MigrationError::SchemaTooNew`](rustdoc:enum:coven::MigrationError).

```rust
let handle = Coven::builder(store_dir, config)
    .synced_tables(synced_tables)
    .coven_migration_policy(coven::CovenMigrationPolicy::ApplyPending)
    .migrations(vec![
        Migration::sql(1, "initial", include_str!("migrations/0001_initial.sql")),
        Migration::sql(2, "remove_origin", include_str!("migrations/0002_remove_origin.sql"))
            .changesets(vec![coven::TableChangesetMigration::new(
                "todos",
                &[],
                |row, _identity| {
                    row.columns.retain(|column| column.name != "origin");
                    Ok(())
                },
            )]),
    ])
    .open()?;
```

`Migration::sql` runs a SQL batch; `Migration::run` accepts a callback for data
migrations. The same registered ladder builds historical column layouts on a
separate in-memory database. Migration callbacks must therefore operate on the
provided SQL context; they cannot depend on an external cache or current wall
clock to determine canonical synced values.

Coven's own bookkeeping tables have a separate ordered ladder and exact schema
manifest. `ApplyPending` authorizes its pending steps; `RefusePending` refuses
an existing database that needs them. Read-only opens never migrate. On writer
open, Coven's migrations precede the host migrations in the same transaction.
Final validation must succeed before any of those changes commit.

## Transform historical writes explicitly

SQLite session changesets address columns by position. Renaming a column without
changing its width does not make an older changeset mean the new thing; appending
a required column does not supply the value an old INSERT needs. Register a
`TableChangesetMigration` on the migration that changes the table's row meaning.
Coven does not infer that meaning from column counts or names.

An adapter receives a `ChangesetRow` with named, typed `ChangesetColumn` cells.
Each column has `old` and `new` values. `None` is an undefined cell in a sparse
UPDATE; `Some(Value::Null)` is SQL NULL. Keep that distinction when translating
values. An INSERT states new values, a DELETE states old values, and an UPDATE
states its primary key and changed cells. A newly required column needs values
on INSERT and DELETE, while an unchanged UPDATE column remains undefined.

Adapters compose from the write's recorded version through the current version.
After each step the row must match that registered schema. They preserve the
operation, table, primary key, indirect flag, and `_updated_at` cells. Changing
row identities or the immutable sync-routing contract is not a column migration.
The database's synced-table declarations still require `STRICT` tables and valid
row identities, as described in [Local data](/docs/local-data).

A sparse UPDATE may omit an identity field needed by a transformation. Declare
such fields explicitly in `TableChangesetMigration::new`'s second argument.
Coven uses a stated old/new value when present; otherwise it reads that declared
immutable field from the target row by primary key. An UPDATE that changes a
declared immutable field is rejected. A missing target retains delete-wins
behavior. Mutable current-row values are not exposed as historical facts.

## Original bytes and current application

Pull verifies the signed package and validates its original layout against the
recorded authoring version. Conversion happens during ordered transactional
application, before current-schema readers, audience checks, blob checks, and
conflict resolution. An earlier INSERT in that transaction can therefore supply
immutable context for a later sparse UPDATE.

The original authenticated bytes and their version remain retained unchanged.
Converted bytes are an application value, not a replacement signed package.
Retained history replay, local journal replay, and Circle bootstrap installation
use the same registered transformations. Conversion or application failure rolls
back the transaction; it cannot leave an earlier row committed while its
materialized position reports otherwise.

## Captured writes keep their version

A local write records its authoring schema in the same transaction as its
changeset and durable write receipt. Publication uses that captured version,
even if the app upgraded before preparing or retrying the package. The currently
open database's `user_version` is not a replacement version for older bytes.

When upgrading an existing unversioned journal, Coven uses an exact prepared
package version only when the retained bytes agree. Otherwise it compares the
registered historical layouts and transformations. Recovery succeeds only when
the interpretation is unambiguous. A same-width rename with different meanings
is an upgrade error, not a reason to label the write with the current version.
The complete database-open transaction rolls back on that error.

## Newer packages wait for an app update

A package whose `schema_version` exceeds the receiver's supported version is
held with `HeldStorePositionReason::NewerSchema`. Its materialized position does
not advance, and dependent commits wait. After upgrading, the receiver can
validate and apply those original packages.

An older package is interpreted through its registered migration path. A newer
version number alone does not prove that every older row shape is understood:
a missing transformation is an explicit failure. There is no storage-level
`min_schema_version` enforcement; the package hold and registered schema
interpretation are the implemented version boundaries.

For example, a device at version 2 can receive a version-1 UPDATE after its
`origin` column was removed when migration 2 declares the transformation above.
A device still at version 1 holds version-2 packages until it upgrades. Neither
device re-labels a write to make it fit.

## Snapshots and Circle images

A Store snapshot is a SQLite database image containing its own `user_version`.
Its signed metadata states the same schema version. A newer receiver runs the
pending image migrations before applying the snapshot's retained tails. An
older receiver refuses a snapshot newer than its supported schema.

A Circle bootstrap is a changeset of INSERTs. Coven verifies the original hash,
row identities, declared tables, and absence of repeated rows before converting
those INSERTs. It then stages the converted rows on the receiver's schema and
checks the routing contract and exact blob closure. Installation and its
bookkeeping share the caller's transaction. The signed original image is never
rewritten to impersonate a current-schema image.
