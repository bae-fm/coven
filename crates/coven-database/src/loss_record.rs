//! The common durable loss record and its snapshot projection.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{counter, damaged, decoded, encoded};
use crate::DbError;
use coven_format::value::Value;
use coven_format::{
    loss::{Loss, LossCause, LossValues},
    merge_fields,
};
use coven_merge::{Audience, Cell, ColumnValue, LostKey, LostValue, RowId, Rule, WriteId};
use rusqlite::params;
use std::collections::{BTreeMap, BTreeSet};

pub(crate) const SELECT: &str = "SELECT l.table_name,l.key,l.audience,l.generation,c.column_name,l.value,l.set_by,l.replacement_kind,l.replaced_by,l.frozen,l.column_id,c.table_name FROM _coven_lost l LEFT JOIN _coven_columns c ON c.id=l.column_id";

/// A table holding loss records: this device's, or the losses a snapshot
/// view gathers for the counted state it publishes (`snapshot_view::losses`).
#[derive(Clone, Copy)]
pub(crate) enum LossTable {
    Stored,
    SnapshotView,
}

/// A loss table's statements: its name, the lookup of a frozen loss equal to
/// the one being stored, and the store of one record by id.
struct LossStatements {
    name: &'static str,
    frozen_id: &'static str,
    put: &'static str,
}

const STORED: LossStatements = LossStatements {
    name: "_coven_lost",
    frozen_id: "SELECT (SELECT id FROM _coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4 AND column_id IS ?5 AND set_by=?6 AND replacement_kind=?7 AND replaced_by=?8 AND frozen=1)",
    put: "INSERT INTO _coven_lost(id,table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by,frozen) VALUES(?11,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO UPDATE SET table_name=excluded.table_name,key=excluded.key,audience=excluded.audience,generation=excluded.generation,column_id=excluded.column_id,value=excluded.value,set_by=excluded.set_by,replacement_kind=excluded.replacement_kind,replaced_by=excluded.replaced_by,frozen=excluded.frozen",
};

const SNAPSHOT_VIEW: LossStatements = LossStatements {
    name: "temp._coven_snapshot_losses",
    frozen_id: "SELECT (SELECT id FROM temp._coven_snapshot_losses WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4 AND column_id IS ?5 AND set_by=?6 AND replacement_kind=?7 AND replaced_by=?8 AND frozen=1)",
    put: "INSERT INTO temp._coven_snapshot_losses(id,table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by,frozen) VALUES(?11,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO UPDATE SET table_name=excluded.table_name,key=excluded.key,audience=excluded.audience,generation=excluded.generation,column_id=excluded.column_id,value=excluded.value,set_by=excluded.set_by,replacement_kind=excluded.replacement_kind,replaced_by=excluded.replaced_by,frozen=excluded.frozen",
};

impl LossTable {
    fn statements(self) -> LossStatements {
        match self {
            LossTable::Stored => STORED,
            LossTable::SnapshotView => SNAPSHOT_VIEW,
        }
    }
}

pub(crate) fn visit<E: From<DbError>>(
    database: &DatabaseConnection,
    audience: &Audience,
    visit: impl FnMut(Loss) -> Result<(), E>,
) -> Result<(), E> {
    visit_table(database, audience, LossTable::Stored, visit)
}

pub(crate) fn visit_table<E: From<DbError>>(
    database: &DatabaseConnection,
    audience: &Audience,
    table: LossTable,
    mut visit: impl FnMut(Loss) -> Result<(), E>,
) -> Result<(), E> {
    let select = SELECT.replace("_coven_lost", table.statements().name);
    database.for_each(
        &format!("{select} WHERE l.audience=?1 ORDER BY l.table_name,l.key,l.generation,_coven_loss_order(c.column_name,l.set_by),l.frozen,CASE l.replacement_kind WHEN 'write' THEN 0 WHEN 'rules' THEN 1 WHEN 'excluded' THEN 2 WHEN 'dependency' THEN 3 ELSE 4 END,CASE WHEN l.replacement_kind IN ('excluded','dependency','unauthorized') THEN substr(l.replaced_by,1,16) END"),
        [audience],
        |r| visit(read(r).map_err(DbError::from)?),
    )
}

pub(crate) fn read(r: &rusqlite::Row<'_>) -> rusqlite::Result<Loss> {
    let row = crate::row_queries::read_identity(r)?;
    let generation = counter(r.get(3)?)?;
    let column: Option<String> = r.get(4)?;
    let column_id: Option<i64> = r.get(10)?;
    let column_table: Option<String> = r.get(11)?;
    if column_id.is_some() && (column.is_none() || column_table.as_ref() != Some(&row.table)) {
        return Err(damaged(0, rusqlite::types::Type::Blob));
    }
    let value: Vec<u8> = r.get(5)?;
    let setter: Vec<u8> = r.get(6)?;
    let kind: String = r.get(7)?;
    let replacement: Vec<u8> = r.get(8)?;
    let values = match column {
        Some(column) => LossValues::Cell {
            column,
            cell: Cell {
                write: decoded(merge_fields::decode_write_id(&setter))?,
                value: decoded(merge_fields::decode_column_value(&value))?,
            },
        },
        None => {
            let columns = decoded(merge_fields::decode_columns(&value))?;
            let mut setters = decoded(merge_fields::decode_setters(&setter))?;
            let cells = columns
                .into_iter()
                .map(|(name, value)| {
                    let write = setters
                        .remove(&name)
                        .ok_or_else(|| damaged(0, rusqlite::types::Type::Blob))?;
                    Ok((name, Cell { write, value }))
                })
                .collect::<rusqlite::Result<_>>()?;
            if !setters.is_empty() {
                return Err(damaged(0, rusqlite::types::Type::Blob));
            }
            LossValues::Row(cells)
        }
    };
    let cause = match kind.as_str() {
        "write" => LossCause::Write(decoded(merge_fields::decode_write_id(&replacement))?),
        "rules" => LossCause::Rules(decoded(merge_fields::decode_rules(&replacement))?),
        "excluded" => {
            let (write, version) = decoded(merge_fields::decode_exclusion(&replacement))?;
            LossCause::Excluded { write, version }
        }
        "dependency" => {
            let (write, sources) = decoded(merge_fields::decode_dependency(&replacement))?;
            LossCause::Dependency { write, sources }
        }
        "unauthorized" => {
            let (write, reason) = decoded(merge_fields::decode_unauthorized(&replacement))?;
            LossCause::NotAuthorized { write, reason }
        }
        _ => return Err(damaged(0, rusqlite::types::Type::Blob)),
    };
    if !matches!(
        (&values, &cause),
        (LossValues::Cell { .. }, LossCause::Write(_))
            | (
                LossValues::Row(_),
                LossCause::Rules(_)
                    | LossCause::Excluded { .. }
                    | LossCause::Dependency { .. }
                    | LossCause::NotAuthorized { .. }
            )
    ) {
        return Err(damaged(0, rusqlite::types::Type::Blob));
    }
    Ok(Loss { row, generation, frozen: r.get(9)?, values, cause })
}

/// Store a new loss that arrived without a displayed row change, and keep
/// its paths and file references at once (`file_protection::retain_loss`).
pub(crate) fn keep(
    database: &DatabaseConnection,
    schema: &crate::write_schema::WriteSchema,
    loss: &Loss,
) -> Result<(), DbError> {
    put(database, loss, None)?;
    crate::file_protection::retain_loss(database, schema, loss)
}

/// Keep an existing local identity when freezing or dismissing part of a loss.
pub(crate) fn put(
    database: &DatabaseConnection,
    loss: &Loss,
    id: Option<i64>,
) -> Result<(), DbError> {
    put_table(database, loss, id, LossTable::Stored)
}

pub(crate) fn put_table(
    database: &DatabaseConnection,
    loss: &Loss,
    id: Option<i64>,
    table: LossTable,
) -> Result<(), DbError> {
    let statements = table.statements();
    let (column, value, setter) = match &loss.values {
        LossValues::Cell { column, cell } => (
            Some(if loss.frozen {
                crate::row_queries::frozen_column(database, &loss.row.table, column)?
            } else {
                database.column_id(&loss.row.table, column)?
            }),
            encoded(merge_fields::encode_column_value(&cell.value))?,
            encoded(merge_fields::encode_write_id(&cell.write))?,
        ),
        LossValues::Row(cells) => (
            None,
            encoded(merge_fields::encode_columns(
                &cells.iter().map(|(n, c)| (n.clone(), c.value.clone())).collect(),
            ))?,
            encoded(merge_fields::encode_setters(
                &cells.iter().map(|(n, c)| (n.clone(), c.write)).collect(),
            ))?,
        ),
    };
    let (kind, cause) = match &loss.cause {
        LossCause::Write(write) => ("write", encoded(merge_fields::encode_write_id(write))?),
        LossCause::Rules(rules) => ("rules", encoded(merge_fields::encode_rules(rules))?),
        LossCause::Excluded { write, version } => {
            ("excluded", encoded(merge_fields::encode_exclusion(*write, *version))?)
        }
        LossCause::Dependency { write, sources } => {
            ("dependency", encoded(merge_fields::encode_dependency(*write, sources))?)
        }
        LossCause::NotAuthorized { write, reason } => {
            ("unauthorized", encoded(merge_fields::encode_unauthorized(*write, reason))?)
        }
    };
    // A captured migration may name a frozen loss already present in the counted
    // prefix. Its immutable identity keeps one record when the queue is folded.
    let id = if id.is_none() && loss.frozen {
        database.query_row(
            statements.frozen_id,
            params![
                loss.row.table,
                loss.row.key,
                &loss.row.audience,
                loss.generation.to_be_bytes(),
                column,
                setter,
                kind,
                cause
            ],
            |r| r.get(0),
        )?
    } else {
        id
    };
    database.internal_execute(
        statements.put,
        params![
            loss.row.table,
            loss.row.key,
            &loss.row.audience,
            loss.generation.to_be_bytes().as_slice(),
            column,
            value,
            setter,
            kind,
            cause,
            loss.frozen,
            id
        ],
    )?;
    Ok(())
}

/// Delete the stored loss `id`.
pub(crate) fn delete(database: &DatabaseConnection, id: i64) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM _coven_lost WHERE id=?1", [id])?;
    Ok(())
}

/// Store `row`'s lost cell `key` as `value` names it: over its loss `id`
/// when the row holds one, as a new loss otherwise.
pub(crate) fn put_write_loss(
    database: &DatabaseConnection,
    row: &RowId,
    key: &LostKey,
    value: &LostValue<Value>,
    id: Option<i64>,
) -> Result<(), DbError> {
    let bytes = encoded(merge_fields::encode_column_value(&value.value))?;
    let setter = encoded(merge_fields::encode_write_id(&key.write))?;
    let replaced_by = encoded(merge_fields::encode_write_id(&value.replaced_by))?;
    if let Some(id) = id {
        database.internal_execute(
            "UPDATE _coven_lost SET value=?1,set_by=?2,replaced_by=?3 WHERE id=?4",
            params![bytes, setter, replaced_by, id],
        )?;
    } else {
        let column = database.column_id(&row.table, &key.column)?;
        database.internal_execute("INSERT INTO _coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES(?1,?2,?3,?4,?5,?6,?7,'write',?8)",params![row.table,row.key,&row.audience,value.incarnation.to_be_bytes().as_slice(),column,bytes,setter,replaced_by])?;
    }
    Ok(())
}

/// Store that `rules` took `row` out at `generation`, holding `columns` as
/// `setters` set them: over its active rules loss `id` when it has one, as a
/// new loss otherwise. An unchanged loss is left alone, so unchanged region
/// neighbors make no metadata writes or local trigger effects.
pub(crate) fn put_rules_loss(
    database: &DatabaseConnection,
    row: &RowId,
    generation: u64,
    columns: &BTreeMap<String, ColumnValue<Value>>,
    setters: &BTreeMap<String, WriteId>,
    rules: &BTreeSet<Rule>,
    id: Option<i64>,
) -> Result<(), DbError> {
    let values = encoded(merge_fields::encode_columns(columns))?;
    let setters = encoded(merge_fields::encode_setters(setters))?;
    let rules = encoded(merge_fields::encode_rules(rules))?;
    match id {
        Some(id) => database.internal_execute("UPDATE _coven_lost SET value=?1,set_by=?2,replaced_by=?3 WHERE id=?4 AND (value<>?1 OR set_by<>?2 OR replaced_by<>?3)",params![values,setters,rules,id])?,
        None => database.internal_execute("INSERT INTO _coven_lost(table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by) VALUES(?1,?2,?3,?4,NULL,?5,?6,'rules',?7)",params![row.table,row.key,&row.audience,generation.to_be_bytes().as_slice(),values,setters,rules])?,
    };
    Ok(())
}

/// Delete every loss of `row`, frozen ones too.
pub(crate) fn forget_row(database: &DatabaseConnection, row: &RowId) -> Result<(), DbError> {
    database.internal_execute(
        "DELETE FROM _coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3",
        params![row.table, row.key, &row.audience],
    )?;
    Ok(())
}

/// Delete every loss of `audience`.
pub(crate) fn forget_audience(
    database: &DatabaseConnection,
    audience: &Audience,
) -> Result<(), DbError> {
    database.internal_execute("DELETE FROM _coven_lost WHERE audience=?1", [audience])?;
    Ok(())
}

/// The active losses of table `old` follow its rows to `new`: a hidden row
/// keeps its values in its removal record (§18.7). Frozen ones keep the
/// names they were frozen under.
pub(crate) fn rename_table(
    database: &DatabaseConnection,
    old: &str,
    new: &str,
) -> Result<(), DbError> {
    database.internal_execute(
        "UPDATE _coven_lost SET table_name=?2 WHERE table_name=?1 AND frozen=0",
        params![old, new],
    )?;
    Ok(())
}

/// A hidden row's removal record holds its values by column name: renamed
/// columns move with the row's cells, and dropped ones go with them.
/// `columns` maps each of `table`'s changed columns to its new name, or to
/// none when it is dropped.
pub(crate) fn rename_removed_values(
    database: &DatabaseConnection,
    table: &str,
    columns: &BTreeMap<String, Option<String>>,
) -> Result<(), DbError> {
    for (id, bytes) in database.query(
        "SELECT id,value FROM _coven_lost WHERE table_name=?1 AND frozen=0 AND replacement_kind='rules' AND column_id IS NULL",
        [table],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
    )? {
        let values = decoded(merge_fields::decode_columns(&bytes))?;
        let renamed: BTreeMap<_, _> = values
            .into_iter()
            .filter_map(|(column, value)| match columns.get(&column) {
                Some(Some(new)) => Some((new.clone(), value)),
                Some(None) => None,
                None => Some((column, value)),
            })
            .collect();
        database.internal_execute(
            "UPDATE _coven_lost SET value=?2 WHERE id=?1",
            params![id, encoded(merge_fields::encode_columns(&renamed))?],
        )?;
    }
    Ok(())
}

/// The active cell losses of column `old` follow its cells to column `new`.
pub(crate) fn move_column(
    database: &DatabaseConnection,
    old: i64,
    new: i64,
) -> Result<(), DbError> {
    database.internal_execute("UPDATE _coven_lost SET column_id=?2 WHERE column_id=?1 AND frozen=0 AND replacement_kind='write'",params![old,new])?;
    Ok(())
}

/// Give each active cell loss of `table` the value `carry` makes of it: a
/// lost value embeds the identities of the references it held.
pub(crate) fn carry_cell_values(
    database: &DatabaseConnection,
    table: &str,
    carry: impl Fn(ColumnValue<Value>) -> ColumnValue<Value>,
) -> Result<(), DbError> {
    database.for_each::<_, DbError>("SELECT id,value FROM _coven_lost WHERE table_name=?1 AND frozen=0 AND replacement_kind='write'",[table],|r| {
        let id: i64 = r.get(0)?;
        let old = decoded(merge_fields::decode_column_value(&r.get::<_,Vec<u8>>(1)?))?;
        let value = carry(old.clone());
        if value != old {
            database.internal_execute("UPDATE _coven_lost SET value=?2 WHERE id=?1",params![id,encoded(merge_fields::encode_column_value(&value))?])?;
        }
        Ok(())
    })
}

/// Delete the stored loss equal to `loss`, a frozen one; whether there was one.
pub(crate) fn remove(database: &DatabaseConnection, loss: &Loss) -> Result<bool, DbError> {
    let ids = database.query(
        "SELECT id FROM _coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3 AND frozen=1",
        params![loss.row.table, loss.row.key, &loss.row.audience],
        |r| r.get::<_, i64>(0),
    )?;
    for id in ids {
        let stored = database.query_row(&format!("{SELECT} WHERE l.id=?1"), [id], read)?;
        if stored == *loss {
            delete(database, id)?;
            return Ok(true);
        }
    }
    Ok(false)
}

/// SQLite sorts without collecting a row's potentially unbounded loss history.
/// Escape names so their byte order matches the format's logical key order.
pub(crate) fn sort_key(column: Option<String>, setter: &[u8]) -> rusqlite::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    match column {
        Some(column) => {
            bytes.push(0);
            sort_name(&mut bytes, &column);
            bytes.extend_from_slice(setter);
        }
        None => {
            let setters = decoded(merge_fields::decode_setters(setter))?;
            bytes.push(1);
            for (name, write) in setters {
                bytes.push(1);
                sort_name(&mut bytes, &name);
                bytes.extend_from_slice(&write.device.0.to_be_bytes());
                bytes.extend_from_slice(&write.number.to_be_bytes());
            }
            bytes.push(0);
        }
    }
    Ok(bytes)
}
fn sort_name(bytes: &mut Vec<u8>, name: &str) {
    for byte in name.bytes() {
        bytes.extend_from_slice(&[1, byte]);
    }
    bytes.push(0);
}

/// A row removal rules took out, as its active loss holds it.
pub(crate) struct RemovedRow {
    /// The loss's id.
    pub(crate) loss: i64,
    /// The row's values when the rules took it out.
    pub(crate) columns: BTreeMap<String, ColumnValue<Value>>,
}

/// The row removal rules took out of `row` at `generation`: none while the
/// rules leave it shown. A row has at most one (format.md D7.2); a second is
/// `DamagedDatabase` (§19.3).
pub(crate) fn removed(
    database: &DatabaseConnection,
    row: &coven_merge::RowId,
    generation: u64,
) -> Result<Option<RemovedRow>, DbError> {
    let mut removed = database.query(
        "SELECT id,value FROM _coven_lost WHERE table_name=?1 AND key=?2 AND audience=?3 AND generation=?4 AND column_id IS NULL AND replacement_kind='rules' AND frozen=0",
        params![row.table, row.key, &row.audience, generation.to_be_bytes().as_slice()],
        |r| {
            Ok(RemovedRow {
                loss: r.get(0)?,
                columns: decoded(merge_fields::decode_columns(
                    &r.get::<_, Vec<u8>>(1)?,
                ))?,
            })
        },
    )?;
    if removed.len() > 1 {
        return Err(DbError::DamagedDatabase);
    }
    Ok(removed.pop())
}

#[cfg(test)]
#[path = "loss_record_tests.rs"]
pub(crate) mod tests;
