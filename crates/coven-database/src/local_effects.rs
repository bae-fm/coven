//! Local rows that SQLite's foreign-key actions change when coven takes out a
//! synced parent whose removal still depends on a non-final store-log entry
//! (§8.4, §9). Coven keeps those rows and reference values before the delete
//! and puts them back when the parent returns. A later explicit local edit
//! wins: a row inserted again under the same identity, or a reference set
//! since, is left as the app made it. The records are released once every
//! entry the removal depended on is final.
use crate::entry_inputs::FINAL;
use crate::schema::TableSchema;
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded, sql_value};
use crate::write_rows::{column_list, key_columns, target_columns, AppValues};
use crate::write_schema::WriteSchema;
use crate::{DbError, EntryId};
use coven_format::{merge_fields, value::Value};
use coven_merge::{ColumnValue, RowId};
use rusqlite::{params, params_from_iter};
use std::collections::{BTreeMap, BTreeSet, VecDeque};

/// The entries of `entries` that are not final under the persisted horizon.
pub(crate) fn provisional(
    db: &DatabaseConnection,
    entries: &BTreeSet<EntryId>,
) -> Result<BTreeSet<EntryId>, DbError> {
    let mut open = BTreeSet::new();
    for entry in entries {
        let finished: bool = db.query_row(
            &format!(
                "SELECT coalesce((SELECT {FINAL} FROM _coven_store_log l WHERE l.device=?1 AND l.number=?2),0)"
            ),
            params![
                entry.device.0.to_be_bytes().as_slice(),
                entry.number.to_be_bytes().as_slice()
            ],
            |r| r.get(0),
        )?;
        if !finished {
            open.insert(*entry);
        }
    }
    Ok(open)
}

fn local_tables(schema: &WriteSchema) -> impl Iterator<Item = &TableSchema> {
    schema.schema.tables.values().filter(|table| {
        !schema
            .declarations
            .iter()
            .any(|declaration| declaration.name.eq_ignore_ascii_case(&table.name))
    })
}

fn columns(values: &AppValues) -> Result<Vec<u8>, DbError> {
    encoded(merge_fields::encode_columns(
        &values
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    ColumnValue {
                        value: value.clone(),
                        parents: BTreeMap::new(),
                    },
                )
            })
            .collect(),
    ))
}

fn values(bytes: &[u8]) -> rusqlite::Result<AppValues> {
    Ok(decoded(merge_fields::decode_columns(bytes))?
        .into_iter()
        .map(|(name, column)| (name, column.value))
        .collect())
}

/// A local row is identified by its declared primary key, which an
/// `INTEGER PRIMARY KEY` rowid alias is too; only a table without one is
/// identified by its hidden rowid.
fn keyed_by_rowid(table: &TableSchema) -> bool {
    key_columns(table).is_empty()
}

fn identity_sql(table: &TableSchema) -> String {
    if keyed_by_rowid(table) {
        "rowid=?".to_owned()
    } else {
        key_columns(table)
            .iter()
            .map(|c| format!("{}=?", identifier(&c.name)))
            .collect::<Vec<_>>()
            .join(" AND ")
    }
}

fn identity_params(
    table: &TableSchema,
    rowid: Option<i64>,
    values: &AppValues,
) -> Vec<rusqlite::types::Value> {
    match rowid {
        Some(rowid) => vec![rusqlite::types::Value::Integer(rowid)],
        None => key_columns(table)
            .iter()
            .map(|c| sql_value(&values[&c.name]))
            .collect(),
    }
}

/// Before coven deletes a visible synced row, keep every local row SQLite's
/// cascade deletes and every reference it sets to null, transitively through
/// local tables. Only removals depending on non-final entries are kept.
pub(crate) fn capture(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    parent: &RowId,
    parent_values: &AppValues,
    causes: &BTreeSet<EntryId>,
) -> Result<(), DbError> {
    if causes.is_empty() {
        return Ok(());
    }
    let mut records = Vec::new();
    let mut deleted = BTreeSet::new();
    let mut queue = VecDeque::from([(parent.table.clone(), parent_values.clone())]);
    while let Some((target, target_values)) = queue.pop_front() {
        let target_table = schema.table(&target);
        for local in local_tables(schema) {
            for key in &local.foreign_keys {
                if !key.target.eq_ignore_ascii_case(&target_table.name) {
                    continue;
                }
                let bound: Vec<_> = target_columns(target_table, key)
                    .iter()
                    .map(|column| target_values[column].clone())
                    .collect();
                if bound.contains(&Value::Null) {
                    continue;
                }
                let rowid = if keyed_by_rowid(local) { "rowid," } else { "" };
                let offset = usize::from(keyed_by_rowid(local));
                let rows = db.query(
                    &format!(
                        "SELECT {rowid}{} FROM main.{} WHERE {}",
                        column_list(local),
                        identifier(&local.name),
                        key.columns
                            .iter()
                            .map(|c| format!("{}=?", identifier(c)))
                            .collect::<Vec<_>>()
                            .join(" AND ")
                    ),
                    params_from_iter(bound.iter().map(sql_value)),
                    |r| {
                        let rowid = keyed_by_rowid(local)
                            .then(|| r.get::<_, i64>(0))
                            .transpose()?;
                        let values = local
                            .columns
                            .iter()
                            .enumerate()
                            .map(|(i, c)| {
                                Ok((
                                    c.name.clone(),
                                    crate::write_encoding::value(r.get_ref(i + offset)?)?,
                                ))
                            })
                            .collect::<rusqlite::Result<AppValues>>()?;
                        Ok((rowid, values))
                    },
                )?;
                for (rowid, values) in rows {
                    // A local row: its rowid in a keyless table, else its key.
                    let local_row = (
                        local.name.clone(),
                        rowid,
                        if rowid.is_some() {
                            Vec::new()
                        } else {
                            encoded(coven_format::key::encode_key(
                                &key_columns(local)
                                    .iter()
                                    .map(|c| values[&c.name].clone())
                                    .collect::<Vec<_>>(),
                            ))?
                        },
                    );
                    if key.on_delete == "CASCADE" {
                        if deleted.insert(local_row) {
                            records.push(("row", local.name.clone(), rowid, values.clone()));
                            queue.push_back((local.name.clone(), values));
                        }
                    } else {
                        let nulled: AppValues = key
                            .columns
                            .iter()
                            .map(|c| {
                                let name = crate::write_rows::column_name(local, c);
                                (name.to_owned(), values[name].clone())
                            })
                            .chain(
                                key_columns(local)
                                    .iter()
                                    .map(|c| (c.name.clone(), values[&c.name].clone())),
                            )
                            .collect();
                        records.push(("null", local.name.clone(), rowid, nulled));
                    }
                }
            }
        }
    }
    if records.is_empty() {
        return Ok(());
    }
    let effect: i64 = db.query_row(
        "INSERT INTO _coven_local_effects(table_name,key,audience) VALUES(?1,?2,?3) RETURNING id",
        params![parent.table, parent.key, audience_text(&parent.audience)],
        |r| r.get(0),
    )?;
    for entry in causes {
        db.internal_execute(
            "INSERT INTO _coven_local_effect_causes(effect,entry_device,entry_number) VALUES(?1,?2,?3)",
            params![
                effect,
                entry.device.0.to_be_bytes().as_slice(),
                entry.number.to_be_bytes().as_slice()
            ],
        )?;
    }
    for (ordinal, (kind, table, rowid, values)) in records.into_iter().enumerate() {
        db.internal_execute(
            "INSERT INTO _coven_local_effect_rows(effect,ordinal,table_name,kind,rowid,columns) VALUES(?1,?2,?3,?4,?5,?6)",
            params![effect, ordinal as i64, table, kind, rowid, columns(&values)?],
        )?;
    }
    Ok(())
}

/// Whether any local rows are kept, so inserts need no lookups otherwise.
pub(crate) fn any(db: &DatabaseConnection) -> Result<bool, DbError> {
    db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_local_effects)",
        [],
        |r| r.get(0),
    )
}

/// After coven inserts a synced row again, put back the local rows kept for
/// its removal, and retry kept rows that waited for this row as a parent.
pub(crate) fn restore(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    parent: &RowId,
    parent_values: &AppValues,
) -> Result<(), DbError> {
    let mut ready = db.query(
        "SELECT r.effect,r.ordinal FROM _coven_local_effects e JOIN _coven_local_effect_rows r ON r.effect=e.id
         WHERE e.table_name=?1 AND e.key=?2 AND e.audience=?3 AND r.wait_table IS NULL ORDER BY r.effect,r.ordinal",
        params![parent.table, parent.key, audience_text(&parent.audience)],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)),
    )?;
    ready.extend(waiting(db, &parent.table, parent_values)?);
    let mut queue = VecDeque::from(ready);
    while let Some((effect, ordinal)) = queue.pop_front() {
        if let Some((table, values)) = restore_row(db, schema, effect, ordinal)? {
            queue.extend(waiting(db, &table, &values)?);
        }
    }
    Ok(())
}

/// Kept rows waiting for a parent in `table` with these values.
fn waiting(
    db: &DatabaseConnection,
    table: &str,
    values: &AppValues,
) -> Result<Vec<(i64, i64)>, DbError> {
    Ok(db
        .query(
            "SELECT effect,ordinal,wait_values FROM _coven_local_effect_rows WHERE wait_table=?1 ORDER BY effect,ordinal",
            [table],
            |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?, self::values(&r.get::<_, Vec<u8>>(2)?)?)),
        )?
        .into_iter()
        .filter(|(_, _, wanted)| {
            wanted
                .iter()
                .all(|(column, value)| values.get(column) == Some(value))
        })
        .map(|(effect, ordinal, _)| (effect, ordinal))
        .collect())
}

/// Some referenced row is absent: the parent this row waits for, as column values.
fn missing_parent(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    table: &TableSchema,
    values: &AppValues,
) -> Result<Option<(String, AppValues)>, DbError> {
    for key in &table.foreign_keys {
        let child: Vec<_> = key
            .columns
            .iter()
            .map(|c| values[crate::write_rows::column_name(table, c)].clone())
            .collect();
        if child.contains(&Value::Null) {
            continue;
        }
        let target = schema.table(&key.target);
        let columns = target_columns(target, key);
        let present: bool = db.query_row(
            &format!(
                "SELECT EXISTS(SELECT 1 FROM main.{} WHERE {})",
                identifier(&target.name),
                columns
                    .iter()
                    .map(|c| format!("{}=?", identifier(c)))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            params_from_iter(child.iter().map(sql_value)),
            |r| r.get(0),
        )?;
        if !present {
            return Ok(Some((
                target.name.clone(),
                columns.into_iter().zip(child).collect(),
            )));
        }
    }
    Ok(None)
}

/// Put back one kept row, or record the absent parent it waits for. Returns
/// the local row put back, whose own waiting children can follow it.
fn restore_row(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    effect: i64,
    ordinal: i64,
) -> Result<Option<(String, AppValues)>, DbError> {
    let Some((table, kind, rowid, values)) = db
        .query(
            "SELECT table_name,kind,rowid,columns FROM _coven_local_effect_rows WHERE effect=?1 AND ordinal=?2",
            params![effect, ordinal],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, Option<i64>>(2)?,
                    self::values(&r.get::<_, Vec<u8>>(3)?)?,
                ))
            },
        )?
        .into_iter()
        .next()
    else {
        return Ok(None);
    };
    let table = schema.table(&table);
    let mut restored = None;
    if kind == "row" {
        if let Some((wait, wanted)) = missing_parent(db, schema, table, &values)? {
            db.internal_execute(
                "UPDATE _coven_local_effect_rows SET wait_table=?3,wait_values=?4 WHERE effect=?1 AND ordinal=?2",
                params![effect, ordinal, wait, columns(&wanted)?],
            )?;
            return Ok(None);
        }
        // A row the app inserted since under the same key or unique value is
        // its replacement and stays; OR IGNORE keeps it.
        let inserted: Vec<_> = table.columns.iter().filter(|c| !c.generated).collect();
        let rowid_column = if rowid.is_some() { "rowid," } else { "" };
        let rowid_value = if rowid.is_some() { "?," } else { "" };
        db.app_execute(
            &format!(
                "INSERT OR IGNORE INTO main.{} ({rowid_column}{}) VALUES ({rowid_value}{})",
                identifier(&table.name),
                inserted
                    .iter()
                    .map(|c| identifier(&c.name))
                    .collect::<Vec<_>>()
                    .join(","),
                vec!["?"; inserted.len()].join(",")
            ),
            params_from_iter(
                rowid
                    .map(rusqlite::types::Value::Integer)
                    .into_iter()
                    .chain(inserted.iter().map(|c| sql_value(&values[&c.name]))),
            ),
        )?;
        restored = Some((table.name.clone(), values));
    } else {
        // Only a reference still null is put back; one the app set since stays.
        let references: Vec<_> = values
            .keys()
            .filter(|name| !key_columns(table).iter().any(|c| &c.name == *name))
            .collect();
        db.app_execute(
            &format!(
                "UPDATE main.{} SET {} WHERE {} AND {}",
                identifier(&table.name),
                references
                    .iter()
                    .map(|c| format!("{}=?", identifier(c)))
                    .collect::<Vec<_>>()
                    .join(","),
                identity_sql(table),
                references
                    .iter()
                    .map(|c| format!("{} IS NULL", identifier(c)))
                    .collect::<Vec<_>>()
                    .join(" AND ")
            ),
            params_from_iter(
                references
                    .iter()
                    .map(|c| sql_value(&values[*c]))
                    .chain(identity_params(table, rowid, &values)),
            ),
        )?;
    }
    db.internal_execute(
        "DELETE FROM _coven_local_effect_rows WHERE effect=?1 AND ordinal=?2",
        params![effect, ordinal],
    )?;
    db.internal_execute(
        "DELETE FROM _coven_local_effects WHERE id=?1 AND NOT EXISTS(SELECT 1 FROM _coven_local_effect_rows WHERE effect=?1)",
        [effect],
    )?;
    Ok(restored)
}

/// Forget kept local rows once every entry their removal depended on is final:
/// the removal can no longer be reversed by replay (§9).
pub(crate) fn release(db: &DatabaseConnection) -> Result<(), DbError> {
    db.internal_execute(
        &format!(
            "DELETE FROM _coven_local_effects WHERE NOT EXISTS(
                SELECT 1 FROM _coven_local_effect_causes c
                JOIN _coven_store_log l ON l.device=c.entry_device AND l.number=c.entry_number
                WHERE c.effect=_coven_local_effects.id AND NOT {FINAL})"
        ),
        [],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "local_effects_tests.rs"]
mod tests;
