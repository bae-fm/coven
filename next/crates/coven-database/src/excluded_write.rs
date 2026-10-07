//! Excluded audience parts retain their undismissed changes for future snapshots.

use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, encoded};
use crate::DbError;
use coven_format::merge_fields;
use coven_format::snapshot_rows::LostWriteCause;
use coven_format::write::WriteRecord;
use coven_format::write_stream::WriteEncoder;
use rusqlite::params;

pub(crate) fn retain(
    database: &DatabaseConnection,
    record: &WriteRecord,
    cause: LostWriteCause,
) -> Result<(), DbError> {
    assert_eq!(record.parts.len(), 1, "one excluded audience per record");
    let part = &record.parts[0];
    let header = &record.header;
    let encoder = encoded(WriteEncoder::new(record))?;
    let mut frame = encoder.header().clone();
    for bytes in database.query(
        "SELECT header FROM coven_excluded_writes WHERE audience=?1 AND device=?2 AND number=?3",
        params![
            audience_text(&part.audience),
            header.position.device.0.to_be_bytes().as_slice(),
            header.position.number.to_be_bytes().as_slice()
        ],
        |row| row.get::<_, Vec<u8>>(0),
    )? {
        let previous = coven_format::write_stream::WriteHeaderFrame::decode(&bytes)?;
        if previous.header != frame.header {
            return Err(DbError::DamagedDatabase);
        }
        frame.parts[0].record_count += previous.parts[0].record_count;
        frame.parts[0].plaintext_length += previous.parts[0].plaintext_length;
    }
    let ordinal: i64 = database.query_row(
        "INSERT INTO coven_excluded_writes(audience,device,number,header,cause)
         VALUES(?1,?2,?3,?4,?5) ON CONFLICT(audience,device,number) DO UPDATE SET header=excluded.header RETURNING id",
        params![
            audience_text(&part.audience),
            header.position.device.0.to_be_bytes().as_slice(),
            header.position.number.to_be_bytes().as_slice(),
            encoded(frame.encode())?,
            encoded(merge_fields::encode_lost_write_cause(&cause))?,
        ],
        |r| r.get(0),
    )?;
    for row in &part.rows {
        database.internal_execute(
            "INSERT INTO coven_excluded_rows(write_id,table_name,key,record) VALUES(?1,?2,?3,?4)",
            params![ordinal, row.row.table, row.row.key, encoded(row.encode())?],
        )?;
    }
    Ok(())
}

/// Keep the snapshot's excluded changes equal to the remaining undismissed losses.
pub(crate) fn dismiss(
    database: &DatabaseConnection,
    dismissal: &coven_format::dismissal::Dismissal,
) -> Result<(), DbError> {
    use crate::write_encoding::decoded;
    use coven_format::write::RowChange;
    use coven_format::write_stream::WriteHeaderFrame;
    use coven_merge::Operation;

    let row = &dismissal.row;
    let (ordinal, bytes, mut header) = database.query_row(
        "SELECT w.id,r.record,w.header FROM coven_excluded_writes w
         JOIN coven_excluded_rows r ON r.write_id=w.id
         WHERE w.audience=?1 AND w.device=?2 AND w.number=?3
         AND r.table_name=?4 AND r.key=?5",
        params![
            audience_text(&row.audience),
            dismissal.write.device.0.to_be_bytes().as_slice(),
            dismissal.write.number.to_be_bytes().as_slice(),
            row.table,
            row.key
        ],
        |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Vec<u8>>(1)?,
                decoded(WriteHeaderFrame::decode(&r.get::<_, Vec<u8>>(2)?))?,
            ))
        },
    )?;
    let mut change = decoded(RowChange::decode(&bytes))?;
    let before_length = bytes.len() as u64;
    let (removed, remaining) = match &mut change.change.operation {
        Operation::Insert(columns) | Operation::Update(columns) => {
            let removed = columns.remove(&dismissal.column).is_some();
            change.old.remove(&dismissal.column);
            (removed, !columns.is_empty())
        }
        Operation::Delete => {
            let removed = change.old.remove(&dismissal.column).is_some();
            (removed, !change.old.is_empty())
        }
    };
    if !removed {
        return Err(DbError::DamagedDatabase);
    }
    let after_length = if remaining {
        let bytes = encoded(change.encode())?;
        database.internal_execute(
            "UPDATE coven_excluded_rows SET record=?1 WHERE write_id=?2 AND table_name=?3 AND key=?4",
            params![bytes, ordinal, row.table, row.key],
        )?;
        bytes.len() as u64
    } else {
        database.internal_execute(
            "DELETE FROM coven_excluded_rows WHERE write_id=?1 AND table_name=?2 AND key=?3",
            params![ordinal, row.table, row.key],
        )?;
        0
    };
    let part = &mut header.parts[0];
    part.plaintext_length = part
        .plaintext_length
        .checked_sub(before_length)
        .and_then(|length| length.checked_add(after_length))
        .ok_or(DbError::DamagedDatabase)?;
    if !remaining {
        part.record_count = part
            .record_count
            .checked_sub(1)
            .ok_or(DbError::DamagedDatabase)?;
    }
    if part.record_count == 0 {
        database.internal_execute("DELETE FROM coven_excluded_writes WHERE id=?1", [ordinal])?;
    } else {
        database.internal_execute(
            "UPDATE coven_excluded_writes SET header=?1 WHERE id=?2",
            params![encoded(header.encode())?, ordinal],
        )?;
    }
    Ok(())
}
