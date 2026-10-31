//! What a reload's installed rows protect (§17.2, §17.10): each file an app
//! row references, and the stored bytes each file record names, brought up
//! to date for every row the install showed differently and every key whose
//! lost values it replaced. A row the app shows as before, with no lost
//! value before or after, holds the same cells and protects the same files.
//!
//! A changed row whose key only the replaced audiences hold, with no lost
//! value and none of its rows among those the removal rules reached,
//! references exactly what its installed merge cells hold. Its protection is
//! written a table at a time from the install tables. Any other changed row
//! is brought up to date one at a time from its whole merge state and its
//! losses, and the rules' own rows are after materializing.

use crate::file_write::FileWrite;
use crate::snapshot_layout::{SnapshotLayout, TableLayout};
use crate::sql::identifier;
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_foundation::id_source::FileId;
use rusqlite::params;

pub(crate) fn protect(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    layout: &SnapshotLayout,
    files: &FileWrite<'_>,
) -> Result<(), DbError> {
    for table in &layout.tables {
        let record = crate::file_table::is_files(&table.table);
        let columns = schema.file_columns(&table.table);
        if record || !columns.is_empty() {
            db.internal_execute(
                "INSERT INTO temp._coven_install_protected(table_name,key)
                 SELECT c.table_name,c.key FROM (
                   SELECT table_name,key FROM temp._coven_install_changed WHERE table_name=?1
                   UNION SELECT table_name,key FROM temp._coven_install_lost WHERE table_name=?1) AS c
                 WHERE NOT EXISTS(SELECT 1 FROM temp._coven_install_region g WHERE g.table_name=?1 AND g.key=c.key)
                 AND NOT EXISTS(SELECT 1 FROM _coven_lost l WHERE l.table_name=?1 AND l.key=c.key)
                 AND NOT EXISTS(SELECT 1 FROM _coven_rows r WHERE r.table_name=?1 AND r.key=c.key
                   AND r.audience NOT IN (SELECT audience FROM temp._coven_download_replaced))",
                [&table.table],
            )?;
            db.for_each(
                "SELECT c.key FROM (
                   SELECT key FROM temp._coven_install_changed WHERE table_name=?1
                   UNION SELECT key FROM temp._coven_install_lost WHERE table_name=?1) AS c
                 WHERE NOT EXISTS(SELECT 1 FROM temp._coven_install_protected p WHERE p.table_name=?1 AND p.key=c.key)
                 ORDER BY c.key",
                [&table.table],
                |r| files.retain_received(&(table.table.clone(), r.get(0)?)),
            )?;
        } else {
            continue;
        }
        if record {
            records(db, table)?;
        } else {
            for column in columns {
                references(db, table, column)?;
            }
        }
    }
    // Any other table's rows reference files only through the lost values
    // of a column once declared one.
    db.for_each(
        "SELECT table_name,key FROM temp._coven_install_lost ORDER BY table_name,key",
        [],
        |r| {
            let key: (String, Vec<u8>) = (r.get(0)?, r.get(1)?);
            if crate::file_table::is_files(&key.0) || !schema.file_columns(&key.0).is_empty() {
                return Ok(());
            }
            files.retain_received(&key)
        },
    )?;
    released(db)?;
    files.settle_received()
}

/// The files the protected keys' rows reference through `column`, as
/// `_coven_file_references` keeps them.
fn references(db: &DatabaseConnection, table: &TableLayout, column: &str) -> Result<(), DbError> {
    // A cell on a column the table no longer has references nothing.
    let Some(j) = table.columns.iter().position(|name| name == column) else {
        return Ok(());
    };
    db.internal_execute(
        &format!(
            "INSERT INTO temp._coven_install_references(file,table_name,key,column_name)
             SELECT _coven_file_id(i.{column}),?1,i.{key},?2
             FROM temp._coven_install_protected p CROSS JOIN {installed} AS i ON i.{key}=p.key
             WHERE p.table_name=?1 AND i.{present}=1 AND i.{setter_device} IS NOT NULL
             AND _coven_file_id(i.{column}) IS NOT NULL
             ON CONFLICT DO NOTHING",
            column = identifier(&table.columns[j]),
            key = table.column("key"),
            installed = table.installed(),
            present = table.column("present"),
            setter_device = table.setter_device(j),
        ),
        params![table.table, column],
    )?;
    Ok(())
}

/// Each protected file record's stored bytes, from its installed merge cell
/// of coordinates; its stored file, from the record the app's table shows;
/// and, for a record with any of its own here, what this device keeps for
/// it, as one row at a time would settle it.
fn records(db: &DatabaseConnection, table: &TableLayout) -> Result<(), DbError> {
    let Some(j) = table
        .columns
        .iter()
        .position(|name| Some(name.as_str()) == crate::file_protection::location(&table.table))
    else {
        return Err(DbError::Invariant { what: "coven's file records have coordinates" });
    };
    let key = table.column("key");
    let installed = table.installed();
    let present = table.column("present");
    db.internal_execute(
        &format!(
            "INSERT INTO temp._coven_install_stored(device,file,table_name,key)
             SELECT _coven_stored_device(i.{column}),_coven_stored_object(i.{column}),?1,i.{key}
             FROM temp._coven_install_protected p CROSS JOIN {installed} AS i ON i.{key}=p.key
             WHERE p.table_name=?1 AND i.{present}=1 AND i.{setter_device} IS NOT NULL
             AND _coven_stored_device(i.{column}) IS NOT NULL
             ON CONFLICT DO NOTHING",
            column = identifier(&table.columns[j]),
            setter_device = table.setter_device(j),
        ),
        [&table.table],
    )?;
    db.internal_execute(
        "DELETE FROM _coven_row_file_refs WHERE (device,file,table_name,key) IN (
           SELECT r.device,r.file,r.table_name,r.key FROM temp._coven_install_protected p
           CROSS JOIN _coven_row_file_refs r ON r.table_name=p.table_name AND r.key=p.key
           WHERE p.table_name=?1 AND NOT EXISTS(SELECT 1 FROM temp._coven_install_stored s
             WHERE s.device=r.device AND s.file=r.file AND s.table_name=r.table_name AND s.key=r.key))",
        [&table.table],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_row_file_refs(device,file,table_name,key)
         SELECT device,file,table_name,key FROM temp._coven_install_stored WHERE table_name=?1
         ON CONFLICT DO NOTHING",
        [&table.table],
    )?;
    // The records with something of their own here to settle once their
    // stored files are up to date, with the stored bytes each named before.
    db.internal_execute(
        "INSERT INTO temp._coven_install_records(file,namespace,cache_id)
         SELECT _coven_file_of_key(p.key),s.namespace,s.cache_id FROM temp._coven_install_protected p
         LEFT JOIN _coven_stored_files s ON s.file=_coven_file_of_key(p.key)
         WHERE p.table_name=?1 AND (s.file IS NOT NULL
           OR EXISTS(SELECT 1 FROM _coven_user_files u WHERE u.file=_coven_file_of_key(p.key))
           OR EXISTS(SELECT 1 FROM _coven_upload_files u WHERE u.file=_coven_file_of_key(p.key))
           OR EXISTS(SELECT 1 FROM _coven_pin_requests r WHERE r.file=_coven_file_of_key(p.key))
           OR EXISTS(SELECT 1 FROM _coven_device_files d WHERE d.file=_coven_file_of_key(p.key)))",
        [&table.table],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_stored_files(file,namespace,device,object,cache_id,hash)
         SELECT f.id,f.namespace,_coven_stored_device(f.coordinates),_coven_stored_object(f.coordinates),
           _coven_stored_cache_id(f.coordinates),f.hash
         FROM temp._coven_install_protected p CROSS JOIN main.coven_files f ON f.id=_coven_file_of_key(p.key)
         WHERE p.table_name=?1 AND _coven_stored_device(f.coordinates) IS NOT NULL
         ON CONFLICT(file) DO UPDATE SET namespace=excluded.namespace,device=excluded.device,object=excluded.object,cache_id=excluded.cache_id,hash=excluded.hash
         WHERE namespace!=excluded.namespace OR device!=excluded.device OR object!=excluded.object OR hash!=excluded.hash",
        [&table.table],
    )?;
    db.internal_execute(
        "DELETE FROM _coven_stored_files WHERE file IN (
           SELECT _coven_file_of_key(p.key) FROM temp._coven_install_protected p WHERE p.table_name=?1
           AND NOT EXISTS(SELECT 1 FROM main.coven_files f WHERE f.id=_coven_file_of_key(p.key)
             AND _coven_stored_device(f.coordinates) IS NOT NULL))",
        [&table.table],
    )?;
    db.for_each(
        "SELECT file,namespace,cache_id FROM temp._coven_install_records ORDER BY file",
        [],
        |r| {
            let file: FileId = r.get(0)?;
            let cached = match (r.get::<_, Option<String>>(1)?, r.get(2)?) {
                (Some(namespace), Some(cache)) => Some((namespace, cache)),
                _ => None,
            };
            let kept = db.query_row(
                &format!(
                    "SELECT EXISTS(SELECT 1 FROM {installed} AS i WHERE i.{key}=?1 AND i.{present}=1)"
                ),
                [crate::file_table::key(file)?],
                |r| r.get::<_, bool>(0),
            )?;
            let shown = crate::file_table::Record::read(db, file)?;
            crate::file_protection::settle_record(db, file, cached, shown.as_ref(), kept)
        },
    )?;
    db.internal_execute("DELETE FROM temp._coven_install_records", [])?;
    Ok(())
}

/// Replace the protected keys' file references with the ones their rows
/// hold now. A file whose last reference went while this device still has
/// its record waits for this device's own write to delete it (§17.2).
fn released(db: &DatabaseConnection) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO temp._coven_install_released(file)
         SELECT r.file FROM temp._coven_install_protected p
         CROSS JOIN _coven_file_references r ON r.table_name=p.table_name AND r.key=p.key
         WHERE NOT EXISTS(SELECT 1 FROM temp._coven_install_references d
           WHERE d.file=r.file AND d.table_name=r.table_name AND d.key=r.key AND d.column_name=r.column_name)
         ON CONFLICT DO NOTHING",
        [],
    )?;
    db.internal_execute(
        "DELETE FROM _coven_file_references WHERE (file,table_name,key,column_name) IN (
           SELECT r.file,r.table_name,r.key,r.column_name FROM temp._coven_install_protected p
           CROSS JOIN _coven_file_references r ON r.table_name=p.table_name AND r.key=p.key
           WHERE NOT EXISTS(SELECT 1 FROM temp._coven_install_references d
             WHERE d.file=r.file AND d.table_name=r.table_name AND d.key=r.key AND d.column_name=r.column_name))",
        [],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_file_references(file,table_name,key,column_name)
         SELECT file,table_name,key,column_name FROM temp._coven_install_references WHERE true
         ON CONFLICT DO NOTHING",
        [],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_unreferenced_files(file)
         SELECT d.file FROM temp._coven_install_released d
         WHERE NOT EXISTS(SELECT 1 FROM _coven_file_references r WHERE r.file=d.file)
         AND EXISTS(SELECT 1 FROM main.coven_files f WHERE f.id=d.file)
         ON CONFLICT DO NOTHING",
        [],
    )?;
    Ok(())
}

#[cfg(test)]
#[path = "snapshot_files_tests.rs"]
mod tests;
