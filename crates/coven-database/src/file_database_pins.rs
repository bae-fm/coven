//! The pins coven keeps for files kept here (§17.8), as the file owner
//! reads and settles them.

use super::{file_ref, FileDatabase};
use crate::{DbError, FileRef};
use coven_foundation::{files::FileName, id_source::FileId};

impl FileDatabase {
    /// The files a switch to the cloud asked this device to keep whole, each
    /// with the copy its cache copy is made from and its wait (§17.8).
    pub async fn pin_requests(&self) -> Result<Vec<crate::PinRequest>, DbError> {
        self.read(crate::file_pins::read).await
    }
    /// `file` left the stored bytes its request asked to keep: the request,
    /// its record and wait end, and the copy it kept goes once nothing else
    /// names it.
    pub async fn end_pin_request(&self, file: FileId) -> Result<(), DbError> {
        self.database
            .call(move |inner| {
                // The released copy goes once the writer is free (§21.2).
                inner.with_files("end a pin request", |writer, _| {
                    writer.state_transaction(|db| crate::file_pins::end(db, file).map(drop))
                })
            })
            .await
    }
    /// Make the pin coven keeps for `file` from `prepared`, ending its
    /// request in the same write, and return true (§17.8). A request that
    /// ended since it was read, by the app's unpin or the file leaving,
    /// makes nothing: false.
    pub async fn keep(&self, prepared: crate::PreparedPin, file: FileId) -> Result<bool, DbError> {
        self.database
            .write(move |sql| {
                if !sql.end_pin_request(file)? {
                    return Ok(false);
                }
                sql.pin(prepared)?;
                Ok(true)
            })
            .await
    }
    /// The cache copy couldn't be made from coven's copy: the pin downloads
    /// from storage instead.
    pub async fn drop_pin_copy(&self, file: FileId) -> Result<(), DbError> {
        self.database
            .call(move |inner| {
                inner.with_files("drop a pin's copy", |writer, _| {
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
        self.run(move |db, _, _, _| {
            db.state_transaction(|db| {
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
                crate::retry::save_failure(
                    db,
                    &record,
                    crate::RetrySubject::Pin(file),
                    retry.as_ref(),
                )
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
            let index = i64::try_from(index)
                .map_err(|_| DbError::Invariant { what: "a chunk index fits SQLite" })?;
            let hash = db
                .query(
                    "SELECT hash FROM _coven_copy_chunks WHERE path=?1 AND chunk=?2",
                    rusqlite::params![copy, index],
                    |r| r.get::<_, Vec<u8>>(0),
                )?
                .pop()
                .ok_or(DbError::DamagedDatabase)?;
            crate::file_hashes::stored_hash(hash)
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
        let (id, size, copy) = (file.id().to_string(), file.size(), copy.clone());
        self.database
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
