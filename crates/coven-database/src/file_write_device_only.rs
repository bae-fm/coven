//! Making cloud files device-only on this device (§17.3): who may, and the
//! write that switches every file of one call to this device, its copy
//! becoming the original this device records.

use super::FileWrite;
use crate::{
    file_hashes::FileHashes,
    file_location::{FileLocation, Place},
    file_table::Record,
    sqlite::DatabaseConnection,
    write_schema::WriteSchema,
    DbError, Provenance,
};
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::RowId;
use std::path::PathBuf;

/// A cloud file to make device-only here: its id, the stored bytes its copy
/// was downloaded from, the copy's path, which becomes the original this
/// device records, and the hashes of the bytes written to it, which a later
/// move to the cloud checks the copy against (§17.3).
#[derive(Debug)]
pub struct DeviceOnlyCopy {
    /// The file.
    pub file: FileId,
    /// The stored bytes the copy holds.
    pub location: FileLocation,
    /// The copy, placed in its final path.
    pub path: PathBuf,
    /// The content hash and chunk hashes of the bytes written to the copy.
    pub hashes: FileHashes,
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
    schema.namespace_of(&record.namespace, Provenance::UserProvided)?;
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
        let row = RowId { table, key, audience: record.audience.clone() };
        if let Some(owner) = owners.foreign_owner(&row)? {
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
    /// Switch a prepared call's files to their copies in this write. Every
    /// copy must be reserved in this store (`PreparedElsewhere`); the
    /// reservation is let go once the write has decided.
    pub(crate) fn make_prepared_device_only(
        &self,
        prepared: crate::PreparedDeviceOnly,
    ) -> Result<(), DbError> {
        super::held_here(self.files.copying, prepared.paths())?;
        let (copies, paths) = prepared.take();
        self.files.held_copies.borrow_mut().extend(paths);
        self.make_device_only(copies)
    }

    /// Switch each file to this device only, off the stored bytes its copy
    /// was downloaded from, in this write (§17.3). Each must still be
    /// permitted, and still stored there: a file another device's switch
    /// took off them meanwhile is `NotInCloud`.
    fn make_device_only(&self, copies: Vec<DeviceOnlyCopy>) -> Result<(), DbError> {
        let db = self.database;
        for copy in copies {
            let record = permitted(db, self.files.schema, self.files.device, copy.file)?;
            if record.place != Place::Cloud(copy.location.clone()) {
                return Err(DbError::NotInCloud { file: copy.file });
            }
            crate::file_table::device_only(db, copy.file, self.files.device)?;
            self.files.local.borrow_mut().push(copy);
        }
        Ok(())
    }

    /// Once the write applied, record each copy it switched as its file's
    /// original here, with the size and modification time the copy had when
    /// it was placed and the hashes of the bytes written to it, and keep the
    /// copy (§17.3). An original recorded for the file before is forgotten,
    /// never touched.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    pub(super) fn keep_device_only(&self) -> Result<(), DbError> {
        let db = self.database;
        for copy in self.files.local.take() {
            let path = crate::database::file_database::copies::text(&copy.path)?;
            let (size, modified_at) = db
                .query(
                    "SELECT size,modified_at FROM _coven_copies WHERE path=?1 AND state='placed'",
                    [path],
                    |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Vec<u8>>(1)?)),
                )?
                .pop()
                .ok_or(DbError::Invariant {
                    what: "a copy is placed before its file switches to it",
                })?;
            let original = crate::UserFile {
                path: copy.path.clone(),
                size: u64::try_from(size).map_err(|_| DbError::DamagedDatabase)?,
                modified_at: crate::user_file::decode_time(&modified_at)?,
            };
            crate::user_file::forget(db, copy.file)?;
            crate::user_file::record(db, copy.file, &original, &copy.hashes)?;
            crate::database::file_database::copies::keep(db, path)?;
        }
        Ok(())
    }
}
