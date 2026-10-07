//! Salvage local work from an archived damaged database. Synced state is always
//! rebuilt by the authenticated snapshot loader, never copied from this source.

use crate::sqlite::DatabaseConnection;
use crate::DbError;
use coven_format::snapshot_rows::AppliedWrite;

pub(crate) fn salvage(
    source: &DatabaseConnection,
    destination: &DatabaseConnection,
) -> Result<(), DbError> {
    // Restore the queue before reconstructing the app schema. The migration
    // runner converts unattempted records across the source's pending versions;
    // sealing keys and upload sessions remain fixed just as on an ordinary update.
    let result = source.for_each("SELECT record,sealing_keys FROM _coven_uploads ORDER BY rowid", [], |row| {
        let bytes: Vec<u8> = row.get(0)?;
        let record = match coven_format::write_stream::decode_plaintext(&bytes) {
            Ok(record) => record,
            Err(error) => {
                tracing::warn!(%error, "unreadable queued write remains in damaged database archive");
                return Ok::<_, DbError>(());
            }
        };
        destination.stream_transaction(|db| {
            crate::write_commit::retain_metadata(db, &AppliedWrite {
                id: record.header.position,
                timestamp: record.header.timestamp,
                had_read: record.header.had_read.clone(),
            })?;
            crate::write_commit::queue(db, &record)?;
            let write = record.header.position;
            let args = (write.device.0.to_be_bytes(), write.number.to_be_bytes());
            if let Some(keys) = row.get::<_, Option<Vec<u8>>>(1)? {
                db.internal_execute("UPDATE _coven_uploads SET sealing_keys=?1 WHERE device=?2 AND number=?3", (&keys, &args.0[..], &args.1[..]))?;
                for session in source.query("SELECT session FROM _coven_write_upload_sessions WHERE device=?1 AND number=?2", (&args.0[..], &args.1[..]), |r| r.get::<_,Vec<u8>>(0))? {
                    db.internal_execute("INSERT INTO _coven_write_upload_sessions(device,number,session) VALUES(?1,?2,?3)", (&args.0[..], &args.1[..], session))?;
                }
            }
            Ok(())
        })
    });
    tolerate_damage(result, "waiting writes")?;
    // The operation journal and fixed entry queue form one unit. Copying one
    // without the other could repeat an already published side effect.
    tolerate_damage(
        destination.stream_transaction(|db| {
            for table in [
                "_coven_operations",
                "_coven_store_log_uploads",
                "_coven_store_log_key_uploads",
                "_coven_key_uploads",
                "_coven_access_keys_to_delete",
                "_coven_file_uploads",
                "_coven_file_upload_chunks",
                "_coven_file_removals",
                "_coven_user_files",
                "_coven_device_files",
                "_coven_file_chunks",
            ] {
                copy_table(source, db, table)?;
            }
            Ok(())
        }),
        "operation and file journal",
    )?;
    Ok(())
}

fn copy_table(
    source: &DatabaseConnection,
    destination: &DatabaseConnection,
    table: &str,
) -> Result<(), DbError> {
    let columns: Vec<String> =
        source.query(&format!("PRAGMA table_info({table})"), [], |r| r.get(1))?;
    if columns.is_empty() {
        return Err(DbError::DamagedDatabase);
    }
    let placeholders = vec!["?"; columns.len()].join(",");
    let insert = format!("INSERT INTO {table} VALUES({placeholders})");
    source.for_each(&format!("SELECT * FROM {table}"), [], |row| {
        let values: Vec<rusqlite::types::Value> = (0..columns.len())
            .map(|i| row.get(i))
            .collect::<Result<_, _>>()?;
        destination.internal_execute(&insert, rusqlite::params_from_iter(values))?;
        Ok::<_, DbError>(())
    })
}

fn tolerate_damage(result: Result<(), DbError>, work: &str) -> Result<(), DbError> {
    match result {
        Err(error) if damaged(&error) => {
            tracing::warn!(%error, work, "unreadable local work remains in damaged database archive");
            Ok(())
        }
        result => result,
    }
}

pub(crate) fn damaged(error: &DbError) -> bool {
    matches!(error, DbError::DamagedDatabase)
        || matches!(error,
        DbError::Sqlite(rusqlite::Error::SqliteFailure(e, _)) if matches!(e.code, rusqlite::ErrorCode::DatabaseCorrupt | rusqlite::ErrorCode::NotADatabase))
}

#[cfg(test)]
#[path = "database_recovery_tests.rs"]
mod tests;
