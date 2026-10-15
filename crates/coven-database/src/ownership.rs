//! Each row's selected original creation, and the owner it names (§8.8).
//!
//! A row's owner is the authenticated member whose write originally created
//! it. Of several generation-zero inserts of one row, the one with the
//! smallest timestamp is its creation on every device, whatever arrived first
//! and whatever later checks decide about each insert. Updates, deletes,
//! reinserts and audience copies keep that creation; a copy names the creation
//! it copied. The record lives apart from merge generations and dismissible
//! losses, so none of them can change it.

use crate::internal_schema::WRITE_DEVICE_SQL;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, decoded, encoded};
use crate::DbError;
use coven_format::merge_fields;
use coven_format::write_sources::ChangeId;
use coven_foundation::id_source::{DeviceId, MemberId};
use coven_merge::{Audience, RowId, Timestamp, WriteId};
use rusqlite::params;

/// A row's selected generation-zero insert and the creation it names.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Creation {
    /// The selected insert's write.
    pub(crate) write: WriteId,
    /// The original creation: the insert itself, or the creation it copied.
    pub(crate) creator: ChangeId,
}

/// The selected creation of a row, if any write has created it.
pub(crate) fn creation(db: &DatabaseConnection, row: &RowId) -> Result<Option<Creation>, DbError> {
    db.query(
        "SELECT write_id,creator FROM _coven_creations WHERE table_name=?1 AND key=?2 AND audience=?3",
        params![row.table, row.key, audience_text(&row.audience)],
        |r| {
            Ok(Creation {
                write: decoded(merge_fields::decode_write_id(&r.get::<_, Vec<u8>>(0)?))?,
                creator: decoded(merge_fields::decode_change_id(&r.get::<_, Vec<u8>>(1)?))?,
            })
        },
    )
    .map(|mut rows| rows.pop())
}

fn timestamp(db: &DatabaseConnection, write: WriteId) -> Result<Option<Timestamp>, DbError> {
    let stamp: Option<Vec<u8>> = db.query_row(
        &format!("SELECT (SELECT timestamp FROM _coven_writes WHERE {WRITE_DEVICE_SQL}=?1 AND number=?2)"),
        params![
            write.device.0.to_be_bytes().as_slice(),
            write.number.to_be_bytes().as_slice()
        ],
        |r| r.get(0),
    )?;
    stamp
        .map(|stamp| Ok(decoded(merge_fields::decode_timestamp(&stamp))?))
        .transpose()
}

/// Offer a generation-zero insert as a row's creation. It is selected when no
/// earlier insert is, and returns the creation it replaced: owner checks
/// already made against that creation must be made again.
pub(crate) fn offer(
    db: &DatabaseConnection,
    row: &RowId,
    write: WriteId,
    stamp: Timestamp,
    creator: ChangeId,
) -> Result<Offer, DbError> {
    let previous = creation(db, row)?;
    if let Some(previous) = &previous {
        if previous.write == write {
            return Ok(Offer::Kept);
        }
        let earlier = timestamp(db, previous.write)?.ok_or(DbError::DamagedDatabase)?;
        if earlier < stamp {
            return Ok(Offer::Kept);
        }
    }
    put(
        db,
        row,
        &Creation {
            write,
            creator: creator.clone(),
        },
    )?;
    Ok(Offer::Selected { previous })
}

/// Offer a part's generation-zero inserts as creations, before any check
/// decides about them (§8.8). Returns each row whose selected creation this
/// replaced, with the replaced one.
pub(crate) fn offer_part(
    db: &DatabaseConnection,
    header: &coven_format::write::WriteHeader,
    part: &coven_format::write::WritePart,
) -> Result<Vec<(RowId, Option<Creation>)>, DbError> {
    let mut replaced = Vec::new();
    for change in &part.rows {
        replaced.extend(offer_change(db, header, change)?);
    }
    Ok(replaced)
}

/// Offer one original change as its row's creation if it is a
/// generation-zero insert. Rows are keyed by their current table names; the
/// creation keeps the original change's own names.
pub(crate) fn offer_change(
    db: &DatabaseConnection,
    header: &coven_format::write::WriteHeader,
    change: &coven_format::write::RowChange,
) -> Result<Option<(RowId, Option<Creation>)>, DbError> {
    if change.change.generation != 0
        || !matches!(change.change.operation, coven_merge::Operation::Insert(_))
    {
        return Ok(None);
    }
    let Some(table) = crate::source_names::table(
        db,
        header.schema_version,
        &change.row.table,
        db.schema_version()?,
    )?
    else {
        return Ok(None);
    };
    let row = RowId {
        table,
        ..change.row.clone()
    };
    let creator = change.creator.clone().unwrap_or(ChangeId {
        write: header.position,
        row: change.row.clone(),
    });
    Ok(
        match offer(db, &row, header.position, header.timestamp, creator)? {
            Offer::Selected { previous } => Some((row, previous)),
            Offer::Kept => None,
        },
    )
}

/// Whether an offered insert became the row's creation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Offer {
    /// An earlier insert, or this one, was already selected.
    Kept,
    /// This insert is now the creation, replacing this one if any.
    Selected { previous: Option<Creation> },
}

/// Record a creation as loaded or carried over unchanged.
pub(crate) fn put(
    db: &DatabaseConnection,
    row: &RowId,
    creation: &Creation,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_creations(table_name,key,audience,write_id,creator) VALUES(?1,?2,?3,?4,?5)
         ON CONFLICT(table_name,key,audience) DO UPDATE SET write_id=excluded.write_id,creator=excluded.creator",
        params![
            row.table,
            row.key,
            audience_text(&row.audience),
            encoded(merge_fields::encode_write_id(&creation.write))?,
            encoded(merge_fields::encode_change_id(&creation.creator))?
        ],
    )?;
    Ok(())
}

/// Forget an audience's creations before a loaded snapshot supplies them.
pub(crate) fn clear_audience(db: &DatabaseConnection, audience: &Audience) -> Result<(), DbError> {
    db.internal_execute(
        "DELETE FROM _coven_creations WHERE audience=?1",
        [audience_text(audience)],
    )?;
    Ok(())
}

/// Visit an audience's creations in row order.
pub(crate) fn visit<E: From<DbError>>(
    db: &DatabaseConnection,
    audience: &Audience,
    mut visit: impl FnMut(RowId, Creation) -> Result<(), E>,
) -> Result<(), E> {
    db.for_each(
        "SELECT table_name,key,write_id,creator FROM _coven_creations WHERE audience=?1 ORDER BY table_name,key",
        [audience_text(audience)],
        |r| {
            let row = RowId {
                table: r.get(0).map_err(DbError::from)?,
                key: r.get(1).map_err(DbError::from)?,
                audience: audience.clone(),
            };
            let creation = Creation {
                write: decoded(merge_fields::decode_write_id(
                    &r.get::<_, Vec<u8>>(2).map_err(DbError::from)?,
                ))
                .map_err(DbError::from)?,
                creator: decoded(merge_fields::decode_change_id(
                    &r.get::<_, Vec<u8>>(3).map_err(DbError::from)?,
                ))
                .map_err(DbError::from)?,
            };
            visit(row, creation)
        },
    )
}

/// A device's member, as the store log recorded it.
pub(crate) fn member(
    db: &DatabaseConnection,
    device: DeviceId,
) -> Result<Option<MemberId>, DbError> {
    let member: Option<Vec<u8>> = db.query_row(
        "SELECT (SELECT member FROM _coven_devices WHERE device=?1)",
        [device.0.to_be_bytes().as_slice()],
        |r| r.get(0),
    )?;
    member
        .map(|bytes| {
            let bytes: [u8; 16] = bytes.try_into().map_err(|_| DbError::DamagedDatabase)?;
            Ok(MemberId(uuid::Uuid::from_bytes(bytes)))
        })
        .transpose()
}

/// Whether a synced table restricts direct writes to each row's owner.
pub(crate) fn owner_table(schema: &crate::write_schema::WriteSchema, table: &str) -> bool {
    schema
        .declarations
        .iter()
        .any(|declaration| declaration.owner_only && declaration.name.eq_ignore_ascii_case(table))
}

/// Carry an audience's creations across a table rename (§17.1).
pub(crate) fn rename(db: &DatabaseConnection, from: &str, to: &str) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_creations SET table_name=?2 WHERE table_name=?1",
        params![from, to],
    )?;
    Ok(())
}

/// Forget a dropped table's creations.
pub(crate) fn forget_table(db: &DatabaseConnection, table: &str) -> Result<(), DbError> {
    db.internal_execute("DELETE FROM _coven_creations WHERE table_name=?1", [table])?;
    Ok(())
}

#[cfg(test)]
#[path = "ownership_tests.rs"]
mod tests;
