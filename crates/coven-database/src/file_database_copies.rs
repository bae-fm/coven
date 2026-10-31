//! The records behind making cloud files device-only (§17.3): who may, the
//! copies a call writes into the person's folder, recorded before any of
//! their bytes exist, the write switching the call's files to them, and the
//! removal of what a failing call made.

use super::{read_only, FileDatabase, FileDatabaseAccess};
use crate::{DbError, DeviceOnlyCopy, FileRef};
use coven_foundation::{files::ObservedFile, id_source::FileId};
use rusqlite::params;
use std::path::{Path, PathBuf};

/// One copy a call to make files device-only writes (§17.3): its path, the
/// temporary name in the same folder its bytes are written under, and the
/// folders the call makes on its path.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FolderCopy {
    /// Where the copy goes.
    pub path: PathBuf,
    /// The name its bytes are written under until they are whole and synced.
    pub temporary: PathBuf,
    /// The folders the call makes that hold it, outermost first.
    pub folders: Vec<PathBuf>,
}

impl FileDatabase {
    /// Each file as its record stands, once this device may make it
    /// device-only (§17.3): in the cloud, of a user namespace, in an
    /// audience this device's member is in, and referenced by no row of an
    /// `owner_only` table another member created. Read in one committed
    /// state; the write switching the files checks again.
    pub async fn device_only_files(&self, files: Vec<FileId>) -> Result<Vec<FileRef>, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database
            .call(move |inner| {
                inner.access.with_committed(|db| {
                    files
                        .iter()
                        .map(|file| {
                            crate::file_write::permitted(
                                db,
                                &inner.access.write_schema,
                                inner.access.device,
                                *file,
                            )
                            .and_then(FileRef::of)
                        })
                        .collect()
                })
            })
            .await
    }

    /// Record the copies one call writes, before any of their folders or
    /// bytes exist, so a failure or a crash leaves nothing unrecorded. A path
    /// another call is writing a copy at is `CopyPathTaken`. While the
    /// returned reservation is held, no other write's removal takes the
    /// copies; once it drops without the call's switch, as when the call's
    /// future is dropped, the next write removes them (Appendix C.3).
    pub async fn record_copies(&self, copies: Vec<FolderCopy>) -> Result<CopyReservation, DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        let paths = copies
            .iter()
            .map(|copy| text(&copy.path).map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()?;
        let resources = database.inner.clone();
        database
            .call(move |inner| {
                inner.access.with_writer(|writer| {
                    writer.transaction(|db| {
                        for copy in &copies {
                            let path = text(&copy.path)?;
                            if db.internal_execute(
                                "INSERT INTO _coven_copies(path,temporary,state) VALUES(?1,?2,'writing')
                                 ON CONFLICT(path) DO NOTHING",
                                params![path, text(&copy.temporary)?],
                            )? != 1
                            {
                                return Err(DbError::CopyPathTaken {
                                    path: copy.path.clone(),
                                });
                            }
                            for folder in &copy.folders {
                                db.internal_execute(
                                    "INSERT INTO _coven_copy_folders(folder,copy) VALUES(?1,?2)",
                                    params![text(folder)?, path],
                                )?;
                            }
                        }
                        Ok(())
                    })?;
                    // Registered under the writer, so no write's removal
                    // runs between the records and the registration.
                    inner
                        .copying
                        .lock()
                        .expect("copying lock poisoned")
                        .extend(paths.iter().cloned());
                    Ok::<_, DbError>(())
                })?;
                Ok(CopyReservation {
                    database: resources,
                    paths,
                })
            })
            .await
    }

    /// The call's own mkdir made `folder`: it is the call's to remove, once
    /// empty. A folder that was there already is never marked.
    pub async fn made_folder(&self, folder: &Path) -> Result<(), DbError> {
        self.require_writer()?;
        let folder = text(folder)?.to_owned();
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                db.internal_execute(
                    "UPDATE _coven_copy_folders SET made=1 WHERE folder=?1",
                    [&folder],
                )?;
                Ok(())
            })
        })
        .await
    }

    /// A copy is whole and synced at its path, renamed there from its
    /// temporary name, with the size and modification time `observed` read.
    #[expect(
        clippy::map_err_ignore,
        reason = "a size past SQLite's integer is TooLarge, which names the field and size"
    )]
    pub async fn place_copy(&self, observed: &ObservedFile) -> Result<(), DbError> {
        self.require_writer()?;
        let path = text(observed.path())?.to_owned();
        let size = i64::try_from(observed.size()).map_err(|_| DbError::TooLarge {
            field: "file size",
            actual: observed.size(),
            maximum: i64::MAX as u64,
        })?;
        let modified_at = crate::user_file::encode_time(observed.modified_at());
        self.run(move |db, _, _, _| {
            db.transaction(|db| {
                if db.internal_execute(
                    "UPDATE _coven_copies SET state='placed',size=?2,modified_at=?3
                     WHERE path=?1 AND state='writing'",
                    params![path, size, modified_at],
                )? != 1
                {
                    return Err(DbError::Invariant {
                        what: "a copy placed is one its call is writing",
                    });
                }
                Ok(())
            })
        })
        .await
    }

    /// The one write switching every file of a call to this device only,
    /// each to its placed copy, which becomes the original this device
    /// records (§17.3). Each file must still be permitted and still stored
    /// where its copy came from; otherwise nothing switches.
    pub async fn make_device_only(&self, copies: Vec<DeviceOnlyCopy>) -> Result<(), DbError> {
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database
            .call(move |inner| {
                inner.with_files(Vec::new(), Vec::new(), |writer, files| {
                    writer.local_write(
                        &inner.access.write_schema,
                        inner.access.device,
                        inner.clock.now(),
                        files,
                        |_sql| files.make_device_only(copies),
                    )
                })
            })
            .await
    }

    /// Remove what a failing call made: the copies at `paths` it left
    /// unswitched, if they are as it left them, and the folders it made for
    /// them, if they are empty. A failed removal keeps its record, and the
    /// next open removes it.
    pub async fn abandon_copies(&self, paths: Vec<PathBuf>) -> Result<(), DbError> {
        let paths = paths
            .iter()
            .map(|path| text(path).map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()?;
        let FileDatabaseAccess::Writer(database) = &self.access else {
            return Err(read_only());
        };
        database
            .call(move |inner| {
                let chosen = inner.access.with_writer(|writer| {
                    Ok::<_, DbError>(crate::file_removals::choose_copies(writer, &paths))
                })?;
                inner.finish_removals(Ok::<_, DbError>(()), chosen)
            })
            .await
    }
}

/// The copies one call to make files device-only is writing, which no other
/// write's removal takes while it is held. Dropping it, whatever became of
/// the call, lets the next write remove those its switch didn't keep.
pub struct CopyReservation {
    database: std::sync::Arc<std::sync::RwLock<Option<super::DatabaseInner>>>,
    paths: Vec<String>,
}

impl Drop for CopyReservation {
    fn drop(&mut self) {
        let slot = self.database.read().expect("database lock poisoned");
        // A closed store keeps the records for its next open.
        if let Some(inner) = slot.as_ref() {
            let mut copying = inner.copying.lock().expect("copying lock poisoned");
            for path in &self.paths {
                copying.remove(path);
            }
        }
    }
}

/// A path as coven records it: valid Unicode, as planning a copy checks.
fn text(path: &Path) -> Result<&str, DbError> {
    path.to_str().ok_or(DbError::Invariant {
        what: "a copy's path is valid Unicode",
    })
}

#[cfg(test)]
#[path = "file_database_copies_tests.rs"]
mod tests;
