//! Making cloud files device-only on this device (§17.3): who may, and the
//! write that switches every file of one call to this device, its copy
//! becoming the original this device records.

use super::FileWrite;
use crate::{
    file_location::{FileLocation, Place},
    file_table::Record,
    sqlite::DatabaseConnection,
    write_schema::WriteSchema,
    DbError, Provenance,
};
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::RowId;
use rusqlite::params;
use std::path::PathBuf;

/// A cloud file to make device-only here: its id, the stored bytes its copy
/// was downloaded from, and the copy's path, which becomes the original
/// this device records (§17.3).
#[derive(Clone, Debug)]
pub struct DeviceOnlyCopy {
    /// The file.
    pub file: FileId,
    /// The stored bytes the copy holds.
    pub location: FileLocation,
    /// The copy, placed in its final path.
    pub path: PathBuf,
}

/// The record of `file` once this device may make it device-only (§17.3):
/// its record is here and in the cloud, of a user namespace; this device's
/// member is in its audience; and it may change every row referencing it,
/// which an `owner_only` table's rows another member created refuse. An
/// admin gets no exception, as with any row (§9.6).
pub(crate) fn permitted(
    db: &DatabaseConnection,
    schema: &WriteSchema,
    device: DeviceId,
    file: FileId,
) -> Result<Record, DbError> {
    let record = Record::read(db, file)?.ok_or(DbError::FileAbsent { file })?;
    let kind = schema
        .namespace(&record.namespace)
        .ok_or_else(|| DbError::UndeclaredNamespace {
            namespace: record.namespace.clone(),
        })?
        .kind
        .clone();
    if kind != Provenance::UserProvided {
        return Err(DbError::NamespaceKind {
            namespace: record.namespace,
            kind,
        });
    }
    if !matches!(record.place, Place::Cloud(_)) {
        return Err(DbError::NotInCloud { file });
    }
    crate::write_commit::check_membership(db, device, &record.audience)?;
    let mut owners = crate::judge::Owners::new(db, schema, device, db.schema_version()?)?;
    for (table, key) in db.query(
        "SELECT DISTINCT table_name,key FROM _coven_file_references WHERE file=?1
         ORDER BY table_name,key",
        [file],
        |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
    )? {
        let row = RowId {
            table,
            key,
            audience: record.audience.clone(),
        };
        if let Some(owner) = owners.foreign_owner(db, &row)? {
            return Err(DbError::NotOwner {
                key: crate::RowKey::decode(&row.key)?,
                table: row.table,
                owner,
            });
        }
    }
    Ok(record)
}

impl FileWrite<'_> {
    /// Switch each file to this device only, off the stored bytes its copy
    /// was downloaded from, in this write (§17.3). Each must still be
    /// permitted, and still stored there: a file another device's switch
    /// took off them meanwhile is `NotInCloud`. No app row changes.
    pub(crate) fn make_device_only(&self, copies: Vec<DeviceOnlyCopy>) -> Result<(), DbError> {
        let db = self.database;
        for copy in copies {
            let record = permitted(db, self.schema, self.device, copy.file)?;
            if record.place != Place::Cloud(copy.location.clone()) {
                return Err(DbError::NotInCloud { file: copy.file });
            }
            Record::device_only(db, copy.file, self.device)?;
            self.local.borrow_mut().push(copy);
        }
        Ok(())
    }

    /// Once the write applied, record each copy it switched as its file's
    /// original here, with the size and modification time the copy had when
    /// it was placed, and keep the copy (§17.3). An original recorded for the
    /// file before is forgotten, never touched.
    pub(super) fn keep_device_only(&self) -> Result<(), DbError> {
        let db = self.database;
        for copy in self.local.take() {
            let path = copy.path.to_str().ok_or(DbError::Invariant {
                what: "a copy's path is valid Unicode",
            })?;
            db.internal_execute("DELETE FROM _coven_user_files WHERE file=?1", [copy.file])?;
            let recorded = db.internal_execute(
                "INSERT INTO _coven_user_files(file,path,size,modified_at)
                 SELECT ?1,path,size,modified_at FROM _coven_copies WHERE path=?2 AND state='placed'",
                params![copy.file, path],
            )?;
            if recorded != 1 {
                return Err(DbError::Invariant {
                    what: "a copy is placed before its file switches to it",
                });
            }
            db.internal_execute(
                "UPDATE _coven_copies SET state='kept' WHERE path=?1",
                [path],
            )?;
        }
        Ok(())
    }
}
