//! The pins coven keeps for files kept here (§17.7), as the file owner
//! reads and settles them.

use super::{file_ref, read_only, FileDatabase, FileDatabaseAccess};
use crate::{DbError, FileRef};
use coven_foundation::{files::FileName, id_source::FileId};

impl FileDatabase {
    /// The files a switch to the cloud asked this device to keep whole, each
    /// with the copy its cache copy is made from and its wait (§17.7).
    /// Read-only handles keep none.
    pub async fn pin_requests(&self) -> Result<Vec<crate::PinRequest>, DbError> {
        let FileDatabaseAccess::Writer(_) = &self.access else {
            return Ok(Vec::new());
        };
        self.read(crate::file_pins::read).await
    }
    /// The pin `file`'s request asked for is made, or its file is gone: the
    /// request, its record and wait end, and the copy it kept goes once
    /// nothing else names it.
    pub async fn end_pin_request(&self, file: FileId) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database
            .call(move |inner| {
                // The released copy goes once the writer is free (§21.2).
                inner.with_files(Vec::new(), |writer, _| {
                    writer.pending_transaction(|db| crate::file_pins::end(db, file))
                })
            })
            .await
    }
    /// The cache copy couldn't be made from coven's copy: the pin downloads
    /// from storage instead.
    pub async fn drop_pin_copy(&self, file: FileId) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database
            .call(move |inner| {
                inner.with_files(Vec::new(), |writer, _| {
                    writer.transaction(|db| crate::file_pins::drop_copy(db, file))
                })
            })
            .await
    }
    /// Record a failed attempt at a pin coven keeps as its record, with its
    /// next wait or none, in one transaction. A request already ended
    /// records nothing.
    pub async fn fail_pin<P: crate::PendingValue>(
        &self,
        file: FileId,
        record: P,
        retry: Option<crate::RetryWait>,
    ) -> Result<(), DbError> {
        self.require_writer()?;
        self.run(move |db, _, _, _| {
            db.pending_transaction(|db| {
                if record.pin_of() != Some(file) {
                    return Err(DbError::Invariant {
                        what: "a kept pin's failure is its own record",
                    });
                }
                if !db.query_row(
                    "SELECT EXISTS(SELECT 1 FROM _coven_pin_requests WHERE file=?1)",
                    [file],
                    |r| r.get::<_, bool>(0),
                )? {
                    return Ok(());
                }
                crate::pending::save(db, &record)?;
                let subject = crate::RetrySubject::Pin(file);
                match &retry {
                    Some(wait) => crate::retry::save(db, subject, wait),
                    None => crate::retry::clear(db, subject),
                }
            })
        })
        .await
    }
    /// The first-read plaintext hash of one chunk of coven's copy, which the
    /// cache copy made from it checks.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert is damage, a computed one a broken invariant"
    )]
    pub async fn copy_chunk_hash(
        &self,
        copy: &FileName,
        index: u64,
    ) -> Result<coven_crypto::ContentHash, DbError> {
        let copy = copy.as_str().to_owned();
        self.read(move |db| {
            let index = i64::try_from(index).map_err(|_| DbError::Invariant {
                what: "a chunk index fits SQLite",
            })?;
            let hash = db
                .query(
                    "SELECT hash FROM _coven_copy_chunks WHERE path=?1 AND chunk=?2",
                    rusqlite::params![copy, index],
                    |r| r.get::<_, Vec<u8>>(0),
                )?
                .pop()
                .ok_or(DbError::DamagedDatabase)?;
            Ok(coven_crypto::ContentHash::from_bytes(
                hash.try_into().map_err(|_| DbError::DamagedDatabase)?,
            ))
        })
        .await
    }
    /// Open coven's copy a pin request keeps, which must still have the
    /// file's size; it touches only the disk.
    pub async fn open_pin_copy(
        &self,
        file: &FileRef,
        copy: &FileName,
    ) -> Result<crate::LocalFileStream, crate::LocalFileError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(DbError::StatementForbidden {
                operation: "keeping a pin through a read-only handle",
            }
            .into());
        };
        let (id, size, copy) = (file.id().to_string(), file.size(), copy.clone());
        database
            .call(move |inner| {
                file_ref::open_source(
                    &inner.access.directory,
                    &id,
                    size,
                    file_ref::LocalSource::Copy(copy),
                )
            })
            .await
    }
}
