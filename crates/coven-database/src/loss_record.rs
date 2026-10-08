//! The common durable loss record, shared by snapshots and fingerprints.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, counter, encoded};
use crate::DbError;
use coven_format::{
    loss::{Loss, LossCause, LossValues},
    merge_fields,
};
use coven_merge::{Audience, Cell};
use rusqlite::params;

pub(crate) const SELECT: &str = "SELECT l.table_name,l.key,l.audience,l.generation,c.column_name,l.value,l.set_by,l.replacement_kind,l.replaced_by,l.retired,l.column_id,c.table_name FROM _coven_lost l LEFT JOIN _coven_columns c ON c.id=l.column_id";

pub(crate) fn visit<E: From<DbError>>(
    database: &DatabaseConnection,
    audience: &Audience,
    mut visit: impl FnMut(Loss) -> Result<(), E>,
) -> Result<(), E> {
    database.for_each(
        &format!("{SELECT} WHERE l.audience=?1 ORDER BY l.table_name,l.key,l.generation,_coven_loss_order(c.column_name,l.set_by),l.retired,CASE l.replacement_kind WHEN 'write' THEN 0 WHEN 'rules' THEN 1 ELSE 2 END,CASE l.replacement_kind WHEN 'excluded' THEN substr(l.replaced_by,1,16) END"),
        [audience_text(audience)],
        |r| visit(read(r).map_err(DbError::from)?),
    )
}

pub(crate) fn read(r: &rusqlite::Row<'_>) -> rusqlite::Result<Loss> {
    let row = crate::row_queries::read_identity(r)?;
    let generation = counter(r.get(3)?);
    let column: Option<String> = r.get(4)?;
    let column_id: Option<i64> = r.get(10)?;
    let column_table: Option<String> = r.get(11)?;
    if column_id.is_some() && (column.is_none() || column_table.as_ref() != Some(&row.table)) {
        return Err(damaged());
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
                    let write = setters.remove(&name).ok_or_else(damaged)?;
                    Ok((name, Cell { write, value }))
                })
                .collect::<rusqlite::Result<_>>()?;
            if !setters.is_empty() {
                return Err(damaged());
            }
            LossValues::Row(cells)
        }
    };
    let cause = match kind.as_str() {
        "write" => LossCause::Write(decoded(merge_fields::decode_write_id(&replacement))?),
        "rules" => LossCause::Rules(decoded(merge_fields::decode_rules(&replacement))?),
        "excluded" => {
            let (write, cause) = decoded(merge_fields::decode_exclusion(&replacement))?;
            LossCause::Excluded { write, cause }
        }
        _ => return Err(damaged()),
    };
    if !matches!(
        (&values, &cause),
        (LossValues::Cell { .. }, LossCause::Write(_))
            | (
                LossValues::Row(_),
                LossCause::Rules(_) | LossCause::Excluded { .. }
            )
    ) {
        return Err(damaged());
    }
    Ok(Loss {
        row,
        generation,
        retired: r.get(9)?,
        values,
        cause,
    })
}

/// Keep an existing local identity when freezing or dismissing part of a loss.
pub(crate) fn put(
    database: &DatabaseConnection,
    loss: &Loss,
    id: Option<i64>,
) -> Result<(), DbError> {
    let (column, value, setter) = match &loss.values {
        LossValues::Cell { column, cell } => (
            Some(crate::write_commit::column(
                database,
                &loss.row.table,
                column,
            )?),
            encoded(merge_fields::encode_column_value(&cell.value))?,
            encoded(merge_fields::encode_write_id(&cell.write))?,
        ),
        LossValues::Row(cells) => (
            None,
            encoded(merge_fields::encode_columns(
                &cells
                    .iter()
                    .map(|(n, c)| (n.clone(), c.value.clone()))
                    .collect(),
            ))?,
            encoded(merge_fields::encode_setters(
                &cells.iter().map(|(n, c)| (n.clone(), c.write)).collect(),
            ))?,
        ),
    };
    let (kind, cause) = match &loss.cause {
        LossCause::Write(write) => ("write", encoded(merge_fields::encode_write_id(write))?),
        LossCause::Rules(rules) => ("rules", encoded(merge_fields::encode_rules(rules))?),
        LossCause::Excluded { write, cause } => (
            "excluded",
            encoded(merge_fields::encode_exclusion(*write, *cause))?,
        ),
    };
    database.internal_execute("INSERT INTO _coven_lost(id,table_name,key,audience,generation,column_id,value,set_by,replacement_kind,replaced_by,retired) VALUES(?11,?1,?2,?3,?4,?5,?6,?7,?8,?9,?10) ON CONFLICT(id) DO UPDATE SET table_name=excluded.table_name,key=excluded.key,audience=excluded.audience,generation=excluded.generation,column_id=excluded.column_id,value=excluded.value,set_by=excluded.set_by,replacement_kind=excluded.replacement_kind,replaced_by=excluded.replaced_by,retired=excluded.retired",params![loss.row.table,loss.row.key,audience_text(&loss.row.audience),loss.generation.to_be_bytes().as_slice(),column,value,setter,kind,cause,loss.retired,id])?;
    if loss.retired {
        crate::fingerprint::loss(database, loss)?;
    }
    Ok(())
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

fn damaged() -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(DbError::DamagedDatabase))
}
fn decoded<T>(result: Result<T, coven_format::Error>) -> rusqlite::Result<T> {
    result.map_err(|_| damaged())
}
