//! Retained losses have values and identities independent of live merge rows.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, counter, decoded};
use crate::DbError;
use coven_format::merge_fields;
use coven_format::retained_loss::{RetainedLoss, RetainedValues};
use coven_merge::{Audience, Cell, LostKey, LostValue};

const RETAINED: &str = "l.retired=1";

pub(crate) fn count(database: &DatabaseConnection, audience: &Audience) -> Result<u64, DbError> {
    database.query_row(
        &format!("SELECT count(*) FROM _coven_lost l WHERE l.audience=?1 AND {RETAINED}"),
        [audience_text(audience)],
        |r| r.get::<_, i64>(0).map(|n| n as u64),
    )
}

pub(crate) fn visit<E: From<DbError>>(
    database: &DatabaseConnection,
    audience: &Audience,
    mut visit: impl FnMut(RetainedLoss) -> Result<(), E>,
) -> Result<(), E> {
    database.for_each(
        &format!("SELECT l.table_name,l.key,l.audience,l.generation,c.column_name,l.value,l.set_by,l.replaced_by
         FROM _coven_lost l LEFT JOIN _coven_columns c ON c.id=l.column_id
         WHERE l.audience=?1 AND {RETAINED}
         ORDER BY l.table_name,l.key,l.generation,_coven_loss_order(c.column_name,l.set_by)"),
        [audience_text(audience)],
        |r| visit(read(r).map_err(DbError::from)?),
    )
}

fn read(r: &rusqlite::Row<'_>) -> rusqlite::Result<RetainedLoss> {
    let row = crate::row_queries::read_identity(r)?;
    let generation = counter(r.get(3)?);
    let column: Option<String> = r.get(4)?;
    let value: Vec<u8> = r.get(5)?;
    let setter: Vec<u8> = r.get(6)?;
    let replacement: Vec<u8> = r.get(7)?;
    let values = match column {
        Some(column) => RetainedValues::Cell {
            key: LostKey {
                column,
                write: decoded(merge_fields::decode_write_id(&setter))?,
            },
            value: LostValue {
                incarnation: generation,
                value: decoded(merge_fields::decode_column_value(&value))?,
                replaced_by: decoded(merge_fields::decode_write_id(&replacement))?,
            },
        },
        None => {
            let columns = decoded(merge_fields::decode_columns(&value))?;
            let mut setters = decoded(merge_fields::decode_setters(&setter))?;
            let cells = columns
                .into_iter()
                .map(|(name, value)| {
                    let write = setters.remove(&name).ok_or(rusqlite::Error::InvalidQuery)?;
                    Ok((name, Cell { write, value }))
                })
                .collect::<rusqlite::Result<_>>()?;
            if !setters.is_empty() {
                return Err(rusqlite::Error::InvalidQuery);
            }
            RetainedValues::Row {
                generation,
                cells,
                replaced_by: decoded(merge_fields::decode_rules(&replacement))?,
            }
        }
    };
    Ok(RetainedLoss { row, values })
}

/// SQLite sorts without collecting a row's potentially unbounded retired history.
/// Escape variable strings so their byte order is the format's logical key order.
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
