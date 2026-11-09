//! Carry merge identities in place; row values remain in SQLite throughout.

use crate::migration_names::MigrationMatch;
use crate::schema::Schema;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{decoded, encoded};
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::merge_fields;
use rusqlite::params;
use std::collections::BTreeSet;

pub(crate) fn carry(
    db: &DatabaseConnection,
    before: &Schema,
    names: &MigrationMatch,
    after: &WriteSchema,
) -> Result<BTreeSet<String>, DbError> {
    let mut refresh = BTreeSet::new();
    let definitions = before.changed_tables(&after.schema);
    for table in before.tables.values() {
        if let Some(new) = names.tables.get(&table.name) {
            if definitions.contains(&table.name.to_ascii_lowercase())
                || definitions.contains(&new.to_ascii_lowercase())
            {
                refresh.insert(new.clone());
            }
            if *new == table.name {
                continue;
            }
        }
        if !names.tables.contains_key(&table.name) {
            crate::write_commit::forget(
                db,
                &crate::write_commit::StoredRows::InTable(&table.name),
            )?;
            crate::ownership::forget_table(db, &table.name)?;
            crate::forget_records::look_again(db)?;
            // A dropped table's losses stay, frozen under its old names (§18.7).
            let losses = db.query(
                "SELECT id FROM _coven_lost WHERE table_name=?1 AND frozen=0",
                [&table.name],
                |r| r.get::<_, i64>(0),
            )?;
            freeze(db, losses)?;
        }
    }
    columns(db, names)?;
    references(db, names, after, &mut refresh)?;
    constraints(db, before, names, after)?;
    // Stage names so swaps and a rename onto a dropped table cannot collide.
    for (index, (old, new)) in names.tables.iter().filter(|(a, b)| a != b).enumerate() {
        let staged = format!("coven_migration_table_{index}");
        rename_table(db, old, &staged)?;
        refresh.insert(new.clone());
    }
    for (index, (_, new)) in names.tables.iter().filter(|(a, b)| a != b).enumerate() {
        rename_table(db, &format!("coven_migration_table_{index}"), new)?;
    }
    Ok(refresh)
}

/// Lean `Migration.freezeDropped`: a loss whose table or column goes keeps its
/// written values, setters, old names and original cause, without live
/// parent links; it no longer follows merge or removal rules (§18.7, D7).
pub(crate) fn freeze(db: &DatabaseConnection, ids: Vec<i64>) -> Result<(), DbError> {
    for id in ids {
        let loss = db.query_row(
            &format!("{} WHERE l.id=?1", crate::loss_record::SELECT),
            [id],
            crate::loss_record::read,
        )?;
        let loss = loss.freeze();
        db.internal_execute(
            "INSERT INTO temp._coven_migration_frozen(audience,record) VALUES(?1,?2)",
            params![&loss.row.audience, encoded(merge_fields::encode_loss(&loss))?],
        )?;
        crate::loss_record::put(db, &loss, Some(id))?;
    }
    Ok(())
}

fn rename_table(db: &DatabaseConnection, old: &str, new: &str) -> Result<(), DbError> {
    crate::write_commit::rename_table(db, old, new)?;
    // A renamed table's rows keep their original creations (§9.6).
    crate::ownership::rename(db, old, new)?;
    crate::loss_record::rename_table(db, old, new)
}

fn columns(db: &DatabaseConnection, names: &MigrationMatch) -> Result<(), DbError> {
    let columns =
        db.query("SELECT id,table_name,column_name FROM _coven_columns WHERE synced=1", [], |r| {
            Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?))
        })?;
    let mut renamed = Vec::new();
    let mut removed_values = std::collections::BTreeMap::<
        String,
        std::collections::BTreeMap<String, Option<String>>,
    >::new();
    for (id, table, column) in columns {
        let target =
            names.tables.get(&table).zip(names.columns.get(&(table.clone(), column.clone())));
        if target == Some((&table, &column)) {
            continue;
        }
        removed_values
            .entry(table.clone())
            .or_default()
            .insert(column.clone(), target.map(|(_, column)| column.clone()));
        let Some((new_table, new_column)) = target else {
            crate::write_commit::forget_column(db, id)?;
            // Its cell losses stay, frozen under the old name (§18.7).
            let losses = db.query(
                "SELECT id FROM _coven_lost WHERE column_id=?1 AND frozen=0",
                [id],
                |r| r.get::<_, i64>(0),
            )?;
            freeze(db, losses)?;
            crate::row_queries::drop_column(db, id)?;
            continue;
        };
        let retained: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?1 AND frozen=1)",
            [id],
            |r| r.get(0),
        )?;
        let active = if retained {
            let active = crate::row_queries::stage_successor(db, id, new_column)?;
            move_column(db, id, active)?;
            crate::row_queries::retire_column(db, id)?;
            active
        } else {
            crate::row_queries::stage_column(db, id, new_column)?;
            id
        };
        renamed.push((active, new_table, new_column));
    }
    for (table, columns) in &removed_values {
        crate::loss_record::rename_removed_values(db, table, columns)?;
    }
    for (id, table, column) in renamed {
        let existing: Option<i64> = db.query_row(
            "SELECT (SELECT id FROM _coven_columns WHERE table_name=?1 AND column_name=?2)",
            params![table, column],
            |r| r.get(0),
        )?;
        if let Some(existing) = existing {
            move_column(db, id, existing)?;
            crate::row_queries::merge_column(db, id, existing)?;
        } else {
            crate::row_queries::rename_column(db, id, table, column)?;
        }
    }
    Ok(())
}

fn move_column(db: &DatabaseConnection, old: i64, new: i64) -> Result<(), DbError> {
    crate::write_commit::move_column(db, old, new)?;
    crate::loss_record::move_column(db, old, new)
}

fn references(
    db: &DatabaseConnection,
    names: &MigrationMatch,
    after: &WriteSchema,
    refresh: &mut BTreeSet<String>,
) -> Result<(), DbError> {
    let keys = db.query("SELECT id,table_name,identity FROM _coven_foreign_keys", [], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            decoded(merge_fields::decode_foreign_key(&r.get::<_, Vec<u8>>(2)?))?,
        ))
    })?;
    let mut renamed = Vec::new();
    let mut losses = BTreeSet::new();
    for (id, table, old) in keys {
        let target = names.tables.get(&table).and_then(|new_table| {
            let key = names.foreign_key(&table, &old, None)?;
            let target = after.table(new_table);
            target
                .foreign_keys
                .iter()
                .any(|fk| after.foreign_key(target, fk) == key)
                .then_some((new_table, key))
        });
        if target.as_ref().is_some_and(|(t, key)| **t == table && *key == old) {
            continue;
        }
        losses.insert(table.clone());
        if let Some((table, key)) = target {
            refresh.insert(table.clone());
            crate::row_queries::stage_definition(
                db,
                crate::row_queries::Definition::ForeignKey,
                id,
            )?;
            renamed.push((id, table, key));
        } else {
            crate::write_commit::forget_foreign_key(db, id)?;
            crate::row_queries::forget_definition(
                db,
                crate::row_queries::Definition::ForeignKey,
                id,
            )?;
        }
    }
    for (id, table, key) in renamed {
        crate::row_queries::rename_definition(
            db,
            crate::row_queries::Definition::ForeignKey,
            id,
            table,
            &encoded(merge_fields::encode_foreign_key(&key))?,
        )?;
        crate::write_commit::repoint_references(db, id, &key.parent)?;
    }
    for table in losses {
        // Values of concurrent cell losses embed reference identities as well.
        crate::loss_record::carry_cell_values(db, &table, |mut value| {
            value.parents = names.carried_parents(after, &table, value.parents);
            value
        })?;
    }
    Ok(())
}

fn constraints(
    db: &DatabaseConnection,
    before: &Schema,
    names: &MigrationMatch,
    after: &WriteSchema,
) -> Result<(), DbError> {
    let constraints =
        db.query("SELECT id,table_name,identity FROM _coven_constraints", [], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                decoded(merge_fields::decode_unique_constraint(&r.get::<_, Vec<u8>>(2)?))?,
            ))
        })?;
    let mut renamed = Vec::new();
    for (id, table, old) in constraints {
        let target = names.tables.get(&table).and_then(|new| {
            let mut key = old.clone();
            for term in &mut key.terms {
                if before.tables[&table.to_ascii_lowercase()]
                    .columns
                    .iter()
                    .any(|column| column.name == *term)
                {
                    *term = names.columns.get(&(table.clone(), term.clone()))?.clone();
                }
            }
            after.rules[new].unique.iter().any(|u| u.identity == key).then_some((new, key))
        });
        if target.as_ref().is_some_and(|(t, key)| **t == table && *key == old) {
            continue;
        }
        if let Some((table, key)) = target {
            crate::row_queries::stage_definition(
                db,
                crate::row_queries::Definition::Constraint,
                id,
            )?;
            renamed.push((id, table, key));
        } else {
            // A dropped constraint's claims go with it. Definitions that no
            // longer exist must not be reused by a later incarnation.
            crate::write_commit::forget_constraint(db, id)?;
            crate::row_queries::forget_definition(
                db,
                crate::row_queries::Definition::Constraint,
                id,
            )?;
        }
    }
    for (id, table, key) in renamed {
        crate::row_queries::rename_definition(
            db,
            crate::row_queries::Definition::Constraint,
            id,
            table,
            &encoded(merge_fields::encode_unique_constraint(&key))?,
        )?;
    }
    Ok(())
}

pub(crate) fn refresh(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    tables: &BTreeSet<String>,
) -> Result<(), DbError> {
    let deleted = crate::store_state_tables::deleted_circles(db)?;
    for table in tables {
        db.for_each::<_, crate::DbError>(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1",
            [table],
            |r| {
                let row = crate::row_queries::read_identity(r)?;
                // Each row gets its own cache. Renaming a table must not retain all
                // of that table's merge state until the migration finishes.
                let visible = crate::write_rows::AppView::after(db, schema);
                let store = crate::merge_store::MergeStore::new(db, &visible);
                crate::write_apply::WriteApply::new(
                    db, schema, &store, &visible, &visible, &deleted,
                )
                .keeping_recomputed()
                .apply(None, [row].into())?;
                Ok(())
            },
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "migration_state_tests.rs"]
pub(crate) mod tests;
