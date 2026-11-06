//! The records behind making cloud files device-only (§17.3): who may, the
//! copies a call writes into the person's folder, recorded before any of
//! their bytes exist, the write switching the call's files to them, and the
//! removal of what a failing call made.

use super::{BytesWriting, Database, FileDatabase};
use crate::{sqlite::DatabaseConnection, DbError, DeviceOnlyCopy, FileRef};
use coven_foundation::locks::lock_unpoisoned;
use coven_foundation::task::{BugSinkRef, CarriesBug, TaskFailed};
use coven_foundation::{
    files::{Flushed, ObservedFile},
    id_source::FileId,
};
use rusqlite::params;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

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
        self.database
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
    /// bytes exist (`Database::record_copies`). A path another call is
    /// writing a copy at is `CopyPathTaken`; the returned reservation keeps
    /// the copies from other writes' removals while it is held.
    pub async fn record_copies(&self, copies: Vec<FolderCopy>) -> Result<CopyReservation, DbError> {
        self.database.record_copies(copies).await
    }

    /// The call's own mkdir made `folder`: it is the call's to remove, once
    /// empty. A folder that was there already is never marked.
    pub async fn made_folder(&self, folder: &Path) -> Result<(), DbError> {
        self.database.made_folder(folder).await
    }

    /// A copy is whole and synced at its path, renamed there from its
    /// temporary name, with the size and modification time `observed` read.
    pub async fn place_copy(&self, observed: &ObservedFile) -> Result<(), DbError> {
        self.database.place_copy(observed).await
    }

    /// The copies a call placed, ready for the write that switches their
    /// files to them (§17.3). It takes the flush of the batch the copies
    /// were written in: they are on the drive before the commit that records
    /// them (Appendix C.3).
    pub fn prepared_device_only(
        &self,
        mut reservation: CopyReservation,
        copies: Vec<DeviceOnlyCopy>,
        _flushed: Flushed,
    ) -> Result<PreparedDeviceOnly, DbError> {
        // The copies stay reserved, now for the prepared call, which holds
        // nothing close or a reopen waits for.
        let _writing = reservation.writing.take();
        let database = &self.database;
        Ok(PreparedDeviceOnly { database: database.clone(), reservation, copies })
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
        self.database.abandon_copies(paths).await
    }
}

impl Database {
    /// Record the copies one call writes, before any of their folders or
    /// bytes exist, so a failure or a crash leaves nothing unrecorded. A path
    /// another call is writing a copy at is `CopyPathTaken`. While the
    /// returned reservation is held, no other write's removal takes the
    /// copies; once it drops without the call's switch, as when the call's
    /// future is dropped, the next write removes them (Appendix C.3). Until
    /// the copies are prepared or it drops, closing the store waits for it,
    /// and so does a reopen once the last handle drops.
    pub(crate) async fn record_copies(
        &self,
        copies: Vec<FolderCopy>,
    ) -> Result<CopyReservation, DbError> {
        let paths = copies
            .iter()
            .map(|copy| text(&copy.path).map(str::to_owned))
            .collect::<Result<Vec<_>, _>>()?;
        let writing = BytesWriting::of(self).await;
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db: &DatabaseConnection| {
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
                lock_unpoisoned(&inner.copying).extend(paths.iter().cloned());
                Ok::<_, DbError>(())
            })?;
            Ok(CopyReservation {
                names: Arc::new(CopyNames {
                    copying: inner.copying.clone(),
                    paths: Mutex::new(paths),
                }),
                writing: Some(Arc::new(writing)),
            })
        })
        .await
    }

    /// The call's own mkdir made `folder`: it is the call's to remove, once
    /// empty. A folder that was there already is never marked.
    pub(crate) async fn made_folder(&self, folder: &Path) -> Result<(), DbError> {
        let folder = text(folder)?.to_owned();
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db: &DatabaseConnection| {
                    db.internal_execute(
                        "UPDATE _coven_copy_folders SET made=1 WHERE folder=?1",
                        [&folder],
                    )?;
                    Ok(())
                })
            })
        })
        .await
    }

    /// A copy is whole and synced at its path, renamed there from its
    /// temporary name, with the size and modification time `observed` read.
    pub(crate) async fn place_copy(&self, observed: &ObservedFile) -> Result<(), DbError> {
        let path = text(observed.path())?.to_owned();
        let size = crate::sqlite::integer("file size", observed.size())?;
        let modified_at = crate::user_file::encode_time(observed.modified_at());
        self.call(move |inner| {
            inner.access.with_writer(|writer| {
                writer.transaction(|db: &DatabaseConnection| {
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
        })
        .await
    }

    /// Remove what a failing call made: the copies at `paths` it left
    /// unswitched, if they are as it left them, and the folders it made for
    /// them, if they are empty, once the writer is free. A failed removal
    /// keeps its record, and the next open removes it.
    pub(crate) async fn abandon_copies(&self, paths: Vec<String>) -> Result<(), DbError> {
        self.call(move |inner| {
            let chosen = inner.access.with_writer(|writer| {
                Ok::<_, DbError>(crate::file_removals::choose_copies(writer, &paths))
            })?;
            inner.finish_removals(Ok::<_, DbError>(()), chosen)
        })
        .await
    }
}

/// Cloud files downloaded to copies in the person's folder, ready for one
/// write to make them device-only here (§17.3, E8). It holds nothing close
/// waits for. Committed by no write, its copies stay reserved while it is
/// held, and the next write or open removes them once it drops; `abandon`
/// removes them at once.
pub struct PreparedDeviceOnly {
    database: Database,
    reservation: CopyReservation,
    copies: Vec<DeviceOnlyCopy>,
}

impl PreparedDeviceOnly {
    /// The copies' paths, as the store reserving them records them.
    pub(crate) fn paths(&self) -> Vec<String> {
        lock_unpoisoned(&self.reservation.names.paths).clone()
    }

    /// The switches, and the copies' paths, for the write making them;
    /// the reservation's release is the write's from here.
    pub(crate) fn take(mut self) -> (Vec<DeviceOnlyCopy>, Vec<String>) {
        let paths = std::mem::take(&mut *lock_unpoisoned(&self.reservation.names.paths));
        (std::mem::take(&mut self.copies), paths)
    }

    /// Remove the copies, and the folders made for them, now, instead of
    /// at the next write or open (§17.3). A failed removal keeps its record,
    /// and the next open removes it.
    pub async fn abandon(self) -> Result<(), DbError> {
        let paths = self.paths();
        let database = self.database.clone();
        drop(self);
        database.abandon_copies(paths).await
    }
}

impl std::fmt::Debug for PreparedDeviceOnly {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedDeviceOnly")
            .field("copies", &self.copies.len())
            .finish_non_exhaustive()
    }
}

/// The copies one call to make files device-only is writing, which no other
/// write's removal takes while it is held or a step it runs is under way.
/// Once both end, whatever became of the call, the next write removes those
/// its switch didn't keep.
pub struct CopyReservation {
    // The names go before the store the writing holds can close.
    names: Arc<CopyNames>,
    /// Held while the copies are written, until they are prepared.
    writing: Option<Arc<BytesWriting>>,
}

impl CopyReservation {
    /// Run `work`, a step of the call named `operation` that makes a name in
    /// the person's folder, as its own task, holding the call's copies, so
    /// it runs to its end with its record though the call is dropped
    /// meanwhile: until it ends, no write's removal takes the call's copies,
    /// and closing the store waits for it (§17.3). Its panic comes back to a
    /// caller still waiting, and goes to `bugs` once none is (E15).
    pub async fn run_step<T: Send + 'static, E: CarriesBug + Send + 'static>(
        &self,
        operation: &'static str,
        bugs: BugSinkRef,
        work: impl std::future::Future<Output = Result<T, E>> + Send + 'static,
    ) -> Result<Result<T, E>, TaskFailed> {
        // The names go before the store the writing holds can close.
        let held = (self.names.clone(), self.writing.clone());
        coven_foundation::task::spawn_completing(operation, bugs, async move {
            let _held = held;
            work.await
        })
        .await
    }
}

/// The copies one call is writing, kept out of removals until the last of
/// its reservation and the steps it runs drops.
struct CopyNames {
    /// The copies the store that recorded them keeps out of removals; a
    /// closed store's records stay for its next open.
    copying: Arc<Mutex<std::collections::BTreeSet<String>>>,
    /// Until a write takes them to release once it has decided.
    paths: Mutex<Vec<String>>,
}

impl Drop for CopyNames {
    fn drop(&mut self) {
        crate::database::release_copies(&self.copying, &lock_unpoisoned(&self.paths));
    }
}

/// The call's copy at `path` is placed and its file switched to it: the
/// copy is an original this device records (§17.3).
pub(crate) fn keep(db: &DatabaseConnection, path: &str) -> Result<(), DbError> {
    db.internal_execute("UPDATE _coven_copies SET state='kept' WHERE path=?1", [path])?;
    Ok(())
}

/// The kept copy at the original's `path` lost its switch to another
/// device's, unless another of this device's files is recorded there: it is
/// recorded for removal (Appendix C.3).
pub(crate) fn lose(db: &DatabaseConnection, path: &[u8], file: FileId) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_copies SET state='lost' WHERE path=CAST(?1 AS TEXT) AND state='kept'
         AND NOT EXISTS(SELECT 1 FROM _coven_user_files WHERE path=?1 AND file!=?2)",
        params![path, file],
    )?;
    Ok(())
}

/// The copy at `path`, recorded as `state`, is removed; its folder links go
/// with it.
pub(crate) fn forget(db: &DatabaseConnection, path: &str, state: &str) -> Result<(), DbError> {
    db.internal_execute("DELETE FROM _coven_copies WHERE path=?1 AND state=?2", (path, state))?;
    Ok(())
}

/// A path as coven records it: valid Unicode, as planning a copy checks.
pub(crate) fn text(path: &Path) -> Result<&str, DbError> {
    path.to_str().ok_or(DbError::Invariant { what: "a copy's path is valid Unicode" })
}

#[cfg(test)]
#[path = "file_database_copies_tests.rs"]
pub(crate) mod tests;
