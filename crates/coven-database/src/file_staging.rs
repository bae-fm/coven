//! Staging app bytes before the write that puts them in rows (§17.2): async
//! sources are consumed without holding SQLite or a blocking worker.

use super::DatabaseInner;
use crate::file_hashes::FileHasher;
use crate::file_write::{StagedFile, StagedSource};
use crate::write_batch::BatchFile;
use crate::{DbError, FileSource, Place, Provenance, WriteBatch};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{FileArea, FileName, FlushBatch, Flushed};
use coven_foundation::locks::{lock_unpoisoned, read_unpoisoned};
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use tokio::sync::OwnedRwLockReadGuard;

/// App bytes still to write to coven's copy of them, and where their file is
/// to be.
struct StagingSource {
    namespace: String,
    id: coven_foundation::id_source::FileId,
    name: FileName,
    source: FileSource,
    place: Place,
}

pub(super) struct FileStaging {
    database: Arc<RwLock<Option<DatabaseInner>>>,
    // Close waits until publication or cancellation releases the reservation.
    lease: Option<OwnedRwLockReadGuard<()>>,
    names: Vec<FileName>,
    sources: VecDeque<StagingSource>,
    staged: Vec<StagedFile>,
}

impl FileStaging {
    pub(super) fn new<T, E: crate::WriteFailure>(
        database: Arc<RwLock<Option<DatabaseInner>>>,
        lease: OwnedRwLockReadGuard<()>,
        build: impl FnOnce(&mut WriteBatch<'_>) -> Result<T, E>,
    ) -> Result<(Self, T), E> {
        let (sources, value) = {
            let slot = read_unpoisoned(&database);
            let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
            if let Some(error) = inner.access.observer.error() {
                return Err(error.into());
            }
            // A panic in the batch fails the staging with `DbError::Bug`, as
            // a refused batch fails it.
            let built = coven_foundation::task::caught_sync("stage files", || {
                let mut batch = WriteBatch::new(inner.ids.as_ref());
                let value = build(&mut batch)?;
                Ok::<_, E>((batch.into_files(), value))
            })
            .unwrap_or_else(|bug| Err(E::from(DbError::Bug(bug))));
            let prepared = inner.access.with_writer(|writer| {
                let prepared = built.and_then(|(files, value)| {
                    if files.is_empty() {
                        return Ok((VecDeque::new(), value));
                    }
                    writer
                        .transaction(|db| {
                            let mut sources = VecDeque::new();
                            for BatchFile { id, namespace, source, place } in files {
                                inner
                                    .access
                                    .write_schema
                                    .namespace_of(&namespace, Provenance::AppProvided)?;
                                let name = FileName::new(inner.ids.new_id().to_string())
                                    .expect("UUID is a portable filename");
                                if !crate::file_removals::record_unnamed(db, name.as_str())? {
                                    return Err(DbError::FileNameReused { name });
                                }
                                sources.push_back(StagingSource {
                                    namespace,
                                    id,
                                    name,
                                    source,
                                    place,
                                });
                            }
                            Ok((sources, value))
                        })
                        .map_err(E::from)
                });
                match prepared {
                    Ok((sources, value)) => {
                        lock_unpoisoned(&inner.staging)
                            .extend(sources.iter().map(|s| s.name.clone()));
                        Ok::<_, E>(Ok((sources, value)))
                    }
                    Err(error) => Ok(Err((error, super::released(writer, inner)))),
                }
            })?;
            // A refused batch's recorded names go once the writer is free.
            match prepared {
                Ok(prepared) => prepared,
                Err((error, chosen)) => return inner.finish_removals(Err(error), chosen),
            }
        };
        Ok((
            Self {
                database,
                lease: Some(lease),
                names: sources.iter().map(|s| s.name.clone()).collect(),
                sources,
                staged: Vec::new(),
            },
            value,
        ))
    }

    /// Write each app source to coven's copy of it, each synced with a plain
    /// sync, then flush the batch: the folder and the drive's cache, once,
    /// so every copy is on the drive before the commit that records it
    /// (Appendix C.3). A failed flush fails the write. Only the creation of
    /// a copy failing on the blocking pool ends the staging here: a panic in
    /// it, or the pool dropping it unstarted as its runtime shuts down. The
    /// staging went with it, and its drop released its names; their recorded
    /// removals take any bytes written before (E15).
    pub(super) async fn write(
        mut self,
    ) -> Result<(Self, Result<Flushed, DbError>), coven_foundation::task::TaskFailed> {
        #[cfg(not(test))]
        let mut batch = FlushBatch::new();
        #[cfg(test)]
        let mut batch = {
            let slot = read_unpoisoned(&self.database);
            let inner = slot.as_ref().expect("staging holds close guard");
            let hook = lock_unpoisoned(&inner.flush_hook).clone();
            FlushBatch::new().with_hook(hook)
        };
        while let Some(source) = self.sources.pop_front() {
            let StagingSource { namespace, id, name, source, place } = source;
            // Creation owns the staging guard until it finishes. Cancellation
            // cannot remove a name before this blocking call creates its bytes.
            let file_name = name.clone();
            let (next, writer) = coven_foundation::task::blocking(
                "create a staged copy",
                crate::commit_scope::bind(move || {
                    let writer = (|| -> Result<_, DbError> {
                        let slot = read_unpoisoned(&self.database);
                        let inner = slot.as_ref().expect("staging holds close guard");
                        Ok(inner
                            .access
                            .directory
                            .create_writer(FileArea::AppProvided, &file_name)?)
                    })();
                    (self, writer)
                }),
            )
            .await?;
            self = next;
            let writer = match writer {
                Ok(writer) => writer,
                Err(error) => return Ok((self, Err(error))),
            };
            let mut hash = FileHasher::new();
            let mut size = 0;
            let written = |bytes: &[u8]| {
                hash.update(bytes);
                size += bytes.len() as u64;
            };
            let result = match source {
                FileSource::Bytes(bytes) => {
                    writer.write_from(&mut bytes.as_slice(), written, &mut batch).await
                }
                FileSource::Stream(mut reader) => {
                    writer.write_from(&mut reader, written, &mut batch).await
                }
            };
            if let Err(error) = result {
                return Ok((self, Err(error.into())));
            }
            self.staged.push(StagedFile {
                id,
                namespace,
                size,
                source: StagedSource::Copy(name),
                hashes: hash.finish(),
                place,
            });
        }
        let flushed = batch.flush().await.map_err(DbError::from);
        Ok((self, flushed))
    }

    /// Fail the staging with `error`: its names go free and the bytes it
    /// wrote are removed now, with any other released bytes, as a failed
    /// write's are; a removal that fails joins the error and keeps its
    /// record for the next write or open (Appendix C.3).
    pub(super) fn discard<E: crate::WriteFailure>(mut self, error: DbError) -> E {
        let database = self.database.clone();
        let slot = read_unpoisoned(&database);
        let inner = slot.as_ref().expect("staging holds close guard");
        inner.release_file_reservations(&self.names);
        self.names.clear();
        let chosen =
            inner.access.with_writer(|writer| Ok::<_, DbError>(super::released(writer, inner)));
        let failures = match chosen {
            Ok(chosen) => inner.remove_chosen(chosen),
            Err(cleanup) => vec![cleanup],
        };
        let _lease = self.lease.take();
        if failures.is_empty() {
            E::from(error)
        } else {
            E::with_cleanup(E::from(error), failures)
        }
    }

    /// The staged files, with what the batch returned, for the write that
    /// puts them in rows. The names stay reserved until that write has
    /// decided or the files are dropped; the lease close waits for ends here.
    pub(super) fn into_staged<T>(mut self, value: T) -> StagedFiles<T> {
        let reserved = {
            let slot = read_unpoisoned(&self.database);
            slot.as_ref().expect("staging holds close guard").staging.clone()
        };
        let _lease = self.lease.take();
        StagedFiles {
            reserved,
            names: std::mem::take(&mut self.names),
            files: std::mem::take(&mut self.staged),
            value: Some(value),
        }
    }
}

impl Drop for FileStaging {
    fn drop(&mut self) {
        let Some(_lease) = self.lease.take() else {
            return;
        };
        let slot = read_unpoisoned(&self.database);
        slot.as_ref()
            .expect("reservation holds close guard")
            .release_file_reservations(&self.names);
    }
}

/// App bytes `stage_files` wrote to coven's copies of them, under names
/// reserved in this store, with what its batch returned, until a write puts
/// them in rows (§17.2). It holds nothing close waits for. Dropped
/// uncommitted, or after the store closes, its names go free, and the next
/// write or open removes the bytes.
pub struct StagedFiles<T> {
    /// The names the store that staged these files reserves.
    reserved: Arc<std::sync::Mutex<std::collections::BTreeSet<FileName>>>,
    names: Vec<FileName>,
    files: Vec<StagedFile>,
    value: Option<T>,
}

impl<T> StagedFiles<T> {
    /// The names reserved for these files' copies.
    pub(crate) fn names(&self) -> &[FileName] {
        &self.names
    }

    /// The files, their reserved names and the batch's value, for the write
    /// taking them; dropping what is left releases nothing.
    pub(crate) fn take(mut self) -> (Vec<StagedFile>, Vec<FileName>, T) {
        let value = self.value.take().expect("staged files are taken once");
        (std::mem::take(&mut self.files), std::mem::take(&mut self.names), value)
    }
}

impl<T> std::fmt::Debug for StagedFiles<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StagedFiles").field("files", &self.files.len()).finish_non_exhaustive()
    }
}

impl<T> Drop for StagedFiles<T> {
    fn drop(&mut self) {
        super::release_reserved(&self.reserved, &self.names);
    }
}

#[cfg(test)]
#[path = "file_staging_tests.rs"]
mod tests;
