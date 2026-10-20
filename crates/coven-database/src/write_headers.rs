//! Checked plaintext headers remain available after queued or received bodies leave.

use crate::{sqlite::DatabaseConnection, DbError};
use coven_format::write_stream::WriteHeaderFrame;
use coven_merge::WriteId;
use rusqlite::params;

pub(crate) fn retain(db: &DatabaseConnection, header: &WriteHeaderFrame) -> Result<(), DbError> {
    let bytes = header.encode()?;
    let id = header.header.position;
    let previous = read(db, id)?;
    if let Some(previous) = previous {
        if previous != *header {
            return Err(DbError::InvalidWrite {
                write: id,
                error: coven_merge::MergeError::DuplicateWrite(id),
            });
        }
        return Ok(());
    }
    db.internal_execute(
        "INSERT INTO _coven_write_headers(device,number,header) VALUES(?1,?2,?3)",
        params![id.device.0.to_be_bytes(), id.number.to_be_bytes(), bytes],
    )?;
    Ok(())
}

#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
pub(crate) fn read(
    db: &DatabaseConnection,
    id: WriteId,
) -> Result<Option<WriteHeaderFrame>, DbError> {
    let bytes: Option<Vec<u8>> = db.query_row(
        "SELECT (SELECT header FROM _coven_write_headers WHERE device=?1 AND number=?2)",
        params![id.device.0.to_be_bytes(), id.number.to_be_bytes()],
        |row| row.get(0),
    )?;
    bytes
        .map(|bytes| WriteHeaderFrame::decode(&bytes).map_err(|_| DbError::DamagedDatabase))
        .transpose()
}
