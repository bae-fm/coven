//! Carry merge identities in place; row values remain in SQLite throughout.

use crate::migration_names::MigrationMatch;
use crate::schema::Schema;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
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
    // The cursor owns identities only, and never changes the table being visited.
    db.batch("CREATE TEMP TABLE _coven_migration_retired AS SELECT DISTINCT table_name,key,audience FROM _coven_lost WHERE retired=0 AND replacement_kind='rules'")?;
    db.for_each::<_, crate::DbError>("SELECT table_name,key,audience FROM temp._coven_migration_retired", [], |r| {
        let id = crate::row_queries::read_identity(r)?;
        freeze_losses(db, &id)?;
        crate::fingerprint::forget_rows(db, std::iter::once(&id))?;
        let audience = audience_text(&id.audience);
        for table in ["_coven_references", "_coven_cells", "_coven_claims"] {
            db.internal_execute(&format!("DELETE FROM {table} WHERE row_id IN (SELECT id FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3)"),params![id.table,id.key,audience])?;
        }
        db.internal_execute("DELETE FROM _coven_rows WHERE table_name=?1 AND key=?2 AND audience=?3",params![id.table,id.key,audience])?;
        Ok(())
    })?;
    db.batch("DROP TABLE temp._coven_migration_retired")?;

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
        db.for_each::<_, crate::DbError>(
            "SELECT DISTINCT table_name,key,audience FROM _coven_rows WHERE table_name=?1",
            [&table.name],
            |r| {
                crate::fingerprint::forget_rows(
                    db,
                    std::iter::once(&crate::row_queries::read_identity(r)?),
                )
            },
        )?;
        if !names.tables.contains_key(&table.name) {
            for child in ["_coven_references", "_coven_cells", "_coven_claims"] {
                db.internal_execute(&format!("DELETE FROM {child} WHERE row_id IN (SELECT id FROM _coven_rows WHERE table_name=?1)"), [&table.name])?;
            }
            db.internal_execute("DELETE FROM _coven_rows WHERE table_name=?1", [&table.name])?;
            db.internal_execute(
                "DELETE FROM _coven_lost WHERE table_name=?1 AND retired=0",
                [&table.name],
            )?;
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

/// Retired losses keep their written values without active reference metadata.
fn freeze_losses(db: &DatabaseConnection, row: &coven_merge::RowId) -> Result<(), DbError> {
    let ids = db.query(
        "SELECT id FROM _coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3 AND retired=0",
        params![row.table, row.key, audience_text(&row.audience)],
        |r| r.get::<_, i64>(0),
    )?;
    for id in ids {
        let mut loss = db.query_row(
            &format!("{} WHERE l.id=?1", crate::loss_record::SELECT),
            [id],
            crate::loss_record::read,
        )?;
        loss.retired = true;
        match &mut loss.values {
            coven_format::loss::LossValues::Cell { cell, .. } => cell.value.parents.clear(),
            coven_format::loss::LossValues::Row(cells) => {
                for cell in cells.values_mut() {
                    cell.value.parents.clear();
                }
            }
        }
        crate::loss_record::put(db, &loss, Some(id))?;
    }
    Ok(())
}

fn rename_table(db: &DatabaseConnection, old: &str, new: &str) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_rows SET table_name=?2 WHERE table_name=?1",
        params![old, new],
    )?;
    db.internal_execute("UPDATE _coven_lost SET table_name=?2 WHERE table_name=?1 AND retired=0 AND replacement_kind='write'",params![old,new])?;
    Ok(())
}

fn columns(db: &DatabaseConnection, names: &MigrationMatch) -> Result<(), DbError> {
    let columns = db.query(
        "SELECT id,table_name,column_name FROM _coven_columns",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
            ))
        },
    )?;
    let mut renamed = Vec::new();
    for (id, table, column) in columns {
        let target = names
            .tables
            .get(&table)
            .zip(names.columns.get(&(table.clone(), column.clone())));
        if target == Some((&table, &column)) {
            continue;
        }
        let Some((new_table, new_column)) = target else {
            db.internal_execute("DELETE FROM _coven_references WHERE column_id=?1", [id])?;
            db.internal_execute("DELETE FROM _coven_cells WHERE column_id=?1", [id])?;
            db.internal_execute("DELETE FROM _coven_lost WHERE column_id=?1 AND retired=0 AND replacement_kind='write'",[id])?;
            db.internal_execute("DELETE FROM _coven_columns WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?1)",[id])?;
            continue;
        };
        let retained: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_lost WHERE column_id=?1 AND retired=1)",
            [id],
            |r| r.get(0),
        )?;
        let stage = format!("coven_migration_column_{id}");
        let active = if retained {
            db.internal_execute(
                "INSERT INTO _coven_columns(table_name,column_name) VALUES(?1,?2)",
                params![stage, new_column],
            )?;
            let active = db.query_row(
                "SELECT id FROM _coven_columns WHERE table_name=?1 AND column_name=?2",
                params![stage, new_column],
                |r| r.get(0),
            )?;
            move_column(db, id, active)?;
            active
        } else {
            db.internal_execute(
                "UPDATE _coven_columns SET table_name=?2,column_name=?3 WHERE id=?1",
                params![id, stage, new_column],
            )?;
            id
        };
        renamed.push((active, new_table, new_column));
    }
    for (id, table, column) in renamed {
        let existing: Option<i64> = db.query_row(
            "SELECT (SELECT id FROM _coven_columns WHERE table_name=?1 AND column_name=?2)",
            params![table, column],
            |r| r.get(0),
        )?;
        if let Some(existing) = existing {
            move_column(db, id, existing)?;
            db.internal_execute("DELETE FROM _coven_columns WHERE id=?1", [id])?;
        } else {
            db.internal_execute(
                "UPDATE _coven_columns SET table_name=?2,column_name=?3 WHERE id=?1",
                params![id, table, column],
            )?;
        }
    }
    Ok(())
}

fn move_column(db: &DatabaseConnection, old: i64, new: i64) -> Result<(), DbError> {
    for table in ["_coven_cells", "_coven_references"] {
        db.internal_execute(
            &format!("UPDATE {table} SET column_id=?2 WHERE column_id=?1"),
            params![old, new],
        )?;
    }
    db.internal_execute("UPDATE _coven_lost SET column_id=?2 WHERE column_id=?1 AND retired=0 AND replacement_kind='write'",params![old,new])?;
    Ok(())
}

fn references(
    db: &DatabaseConnection,
    names: &MigrationMatch,
    after: &WriteSchema,
    refresh: &mut BTreeSet<String>,
) -> Result<(), DbError> {
    let keys = db.query(
        "SELECT id,table_name,identity FROM _coven_foreign_keys",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                decoded(merge_fields::decode_foreign_key(&r.get::<_, Vec<u8>>(2)?))?,
            ))
        },
    )?;
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
        if target
            .as_ref()
            .is_some_and(|(t, key)| **t == table && *key == old)
        {
            continue;
        }
        losses.insert(table.clone());
        if let Some((table, key)) = target {
            refresh.insert(table.clone());
            db.internal_execute(
                "UPDATE _coven_foreign_keys SET table_name=?2 WHERE id=?1",
                params![id, format!("coven_migration_fk_{id}")],
            )?;
            renamed.push((id, table, key));
        } else {
            db.internal_execute(
                "DELETE FROM _coven_references WHERE foreign_key_id=?1",
                [id],
            )?;
            db.internal_execute("DELETE FROM _coven_foreign_keys WHERE id=?1", [id])?;
        }
    }
    for (id, table, key) in renamed {
        db.internal_execute(
            "UPDATE _coven_foreign_keys SET table_name=?2,identity=?3 WHERE id=?1",
            params![id, table, encoded(merge_fields::encode_foreign_key(&key))?],
        )?;
        db.internal_execute("UPDATE _coven_references SET parent_table=?2 WHERE foreign_key_id=?1 AND parent_table<>?2",params![id,key.parent])?;
    }
    for table in losses {
        // Values of concurrent cell losses embed reference identities as well.
        db.for_each::<_, crate::DbError>("SELECT id,value FROM _coven_lost WHERE table_name=?1 AND retired=0 AND replacement_kind='write'",[&table],|r| {
            let id: i64 = r.get(0)?;
            let mut value = decoded(merge_fields::decode_column_value(&r.get::<_,Vec<u8>>(1)?))?;
            let old = value.clone();
            value.parents = value.parents.into_iter().filter_map(|(fk,mut parent)| {
                let fk = names.foreign_key(&table,&fk, None)?;
                let target = after.table(&names.tables[&table]);
                if !target.foreign_keys.iter().any(|f|after.foreign_key(target,f)==fk) { return None; }
                parent.row.table = names.tables.get(&parent.row.table)?.clone();
                Some((fk,parent))
            }).collect();
            if value != old {
                db.internal_execute("UPDATE _coven_lost SET value=?2 WHERE id=?1",params![id,encoded(merge_fields::encode_column_value(&value))?])?;
            }
            Ok(())
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
    let constraints = db.query(
        "SELECT id,table_name,identity FROM _coven_constraints",
        [],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                decoded(merge_fields::decode_unique_constraint(
                    &r.get::<_, Vec<u8>>(2)?,
                ))?,
            ))
        },
    )?;
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
            after.rules[new]
                .unique
                .iter()
                .any(|u| u.identity == key)
                .then_some((new, key))
        });
        if target
            .as_ref()
            .is_some_and(|(t, key)| **t == table && *key == old)
        {
            continue;
        }
        if let Some((table, key)) = target {
            db.internal_execute(
                "UPDATE _coven_constraints SET table_name=?2 WHERE id=?1",
                params![id, format!("coven_migration_constraint_{id}")],
            )?;
            renamed.push((id, table, key));
        } else {
            // Claims belong to removed rows, which were retired above. Definitions
            // that no longer exist must not be reused by a later incarnation.
            db.internal_execute("DELETE FROM _coven_claims WHERE constraint_id=?1", [id])?;
            db.internal_execute("DELETE FROM _coven_constraints WHERE id=?1", [id])?;
        }
    }
    for (id, table, key) in renamed {
        db.internal_execute(
            "UPDATE _coven_constraints SET table_name=?2,identity=?3 WHERE id=?1",
            params![
                id,
                table,
                encoded(merge_fields::encode_unique_constraint(&key))?
            ],
        )?;
    }
    Ok(())
}

pub(crate) fn refresh(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    tables: &BTreeSet<String>,
) -> Result<(), DbError> {
    let deleted = crate::store_log_tables::deleted_circles(db)?;
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
                .apply(None, [row].into())?;
                Ok(())
            },
        )?;
    }
    Ok(())
}
