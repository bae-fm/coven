//! Convert waiting records without changing their already-applied merge effects.

use crate::migration::WriteConversion;
use crate::migration_names::MigrationMatch;
use crate::schema::Schema;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::counter;
use crate::{DbError, RowChange};
use coven_format::{
    write::WriteDisposition,
    write_stream::{decode_plaintext, WriteEncoder},
};
use coven_foundation::id_source::DeviceId;
use coven_merge::WriteId;
use rusqlite::params;

pub(crate) fn convert(
    database: &DatabaseConnection,
    before: &Schema,
    after: &Schema,
    names: &MigrationMatch,
    version: u32,
    conversion: Option<&WriteConversion>,
) -> Result<(), DbError> {
    let waiting = database.query(
        "SELECT device,number,record FROM coven_uploads WHERE sealed_bytes IS NULL ORDER BY device,number", [],
        |row| Ok((WriteId { device: DeviceId(counter(row.get(0)?)), number: counter(row.get(1)?) }, row.get::<_,Vec<u8>>(2)?)),
    )?;
    for (id, bytes) in waiting {
        let mut record = decode_plaintext(&bytes)?;
        if record.header.position != id {
            return Err(DbError::DamagedDatabase);
        }
        if record.header.disposition != WriteDisposition::Apply {
            continue;
        }
        if record.header.schema_version >= version {
            return Err(DbError::MigrationWriteVersion {
                write: id,
                schema_version: record.header.schema_version,
                migration_version: version,
            });
        }
        if let Some(conversion) = conversion {
            for part in &mut record.parts {
                for row in &mut part.rows {
                    let mut change = RowChange::read(row, before)?;
                    conversion(&mut change)?;
                    *row = change.write(row, after, names)?;
                }
                part.rows.sort_by(|a, b| a.row.cmp(&b.row));
            }
            record.header.schema_version = version;
        } else {
            record.header.disposition = WriteDisposition::Lost(version);
        }
        let bytes = crate::write_commit::plaintext(database, WriteEncoder::new(&record)?)?;
        let updated = database.internal_execute(
            "UPDATE coven_uploads SET record=?1 WHERE device=?2 AND number=?3 AND sealed_bytes IS NULL",
            params![bytes,id.device.0.to_be_bytes().as_slice(),id.number.to_be_bytes().as_slice()],
        )?;
        if updated != 1 {
            return Err(DbError::MigrationUploadMissing { write: id });
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "migration_writes_tests.rs"]
pub(crate) mod tests;
