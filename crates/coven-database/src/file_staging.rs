//! Staging app bytes before the write that puts them in rows (§17.2): async
//! sources are consumed without holding SQLite or a blocking worker.

use super::DatabaseInner;
use crate::file_hashes::FileHasher;
use crate::file_removals::OwnedFile;
use crate::file_write::{StagedFile, StagedSource};
use crate::write_batch::BatchFile;
use crate::{DbError, FileSource, Place, Provenance, WriteBatch};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{FileName, FlushBatch, Flushed, StagedName, WriterFolder};
use coven_foundation::locks::read_unpoisoned;
use std::collections::VecDeque;
use std::sync::{Arc, RwLock};
use tokio::sync::OwnedRwLockReadGuard;

/// App bytes still to write to coven's copy of them, and where their file is
/// to be.
struct StagingSource {
    copy: StagingCopy,
    source: FileSource,
}

/// Where a staged copy is to be and what it is for, apart from its bytes.
struct StagingCopy {
    namespace: String,
    id: coven_foundation::id_source::FileId,
    name: StagedName,
    place: Place,
}

type StagingReader = std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send>>;

/// Sources in the order the batch put them: the copies of bytes the app
/// holds, then the stream that ends them, if the batch has one.
struct Run {
    bytes: Vec<(StagingCopy, Vec<u8>)>,
    stream: Option<(StagingCopy, StagingReader)>,
}

impl StagingCopy {
    /// The copy as staged, once its `size` bytes were written and hashed.
    fn staged(self, size: u64, hash: FileHasher) -> StagedFile {
        StagedFile {
            id: self.id,
            namespace: self.namespace,
            size,
            source: StagedSource::Copy(self.name),
            hashes: hash.finish(),
            place: self.place,
        }
    }
}

pub(super) struct FileStaging {
    database: Arc<RwLock<Option<DatabaseInner>>>,
    /// The names the store reserves, these among them until they go.
    reserved: Arc<crate::reserved_names::ReservedNames>,
    // Close waits until publication or cancellation releases the reservation.
    lease: Option<OwnedRwLockReadGuard<()>>,
    names: Vec<OwnedFile>,
    sources: VecDeque<StagingSource>,
    staged: Vec<StagedFile>,
}

impl FileStaging {
    pub(super) fn new<T, E: crate::WriteFailure>(
        database: Arc<RwLock<Option<DatabaseInner>>>,
        lease: OwnedRwLockReadGuard<()>,
        build: impl FnOnce(&mut WriteBatch<'_>) -> Result<T, E>,
    ) -> Result<(Self, T), E> {
        let (sources, value, reserved) = {
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
                            // The staging's own folder, which its copies'
                            // names name (Appendix C.3).
                            let folder = FileName::new(inner.ids.new_id().to_string())
                                .expect("UUID is a portable filename");
                            for BatchFile { id, namespace, source, place } in files {
                                inner
                                    .access
                                    .write_schema
                                    .namespace_of(&namespace, Provenance::AppProvided)?;
                                let copy = FileName::new(inner.ids.new_id().to_string())
                                    .expect("UUID is a portable filename");
                                let name = StagedName::new(&folder, &copy);
                                if !crate::file_removals::record_unnamed(db, name.as_str())? {
                                    return Err(DbError::FileNameReused {
                                        name: name.as_str().to_owned(),
                                    });
                                }
                                sources.push_back(StagingSource {
                                    copy: StagingCopy { namespace, id, name, place },
                                    source,
                                });
                            }
                            Ok((sources, value))
                        })
                        .map_err(E::from)
                });
                match prepared {
                    Ok((sources, value)) => {
                        inner.staging.reserve(
                            sources.iter().map(|s| OwnedFile::Staged(s.copy.name.clone())),
                        );
                        Ok::<_, E>(Ok((sources, value)))
                    }
                    Err(error) => Ok(Err((error, super::released(writer, inner)))),
                }
            })?;
            // A refused batch's recorded names go once the writer is free.
            match prepared {
                Ok((sources, value)) => (sources, value, inner.staging.clone()),
                Err((error, chosen)) => return inner.finish_removals(Err(error), chosen),
            }
        };
        Ok((
            Self {
                reserved,
                database,
                lease: Some(lease),
                names: sources.iter().map(|s| OwnedFile::Staged(s.copy.name.clone())).collect(),
                sources,
                staged: Vec::new(),
            },
            value,
        ))
    }

    /// Write each app source to coven's copy of it in the staging's own
    /// folder, made with the first copy, each synced with a plain sync, then
    /// flush the batch: the folders and the drive's cache, once, so every
    /// copy is on the drive before the commit that records it
    /// (Appendix C.3). A failed flush fails the write. The bytes the app
    /// already holds are written in one blocking task per run of them; a
    /// stream's copy is created on the blocking pool and then read and
    /// written as the stream yields. Only the creation of a copy failing on
    /// the blocking pool ends the staging here: a panic in it, or the pool
    /// dropping it unstarted as its runtime shuts down. The staging went
    /// with it, and its drop released its names; their recorded removals
    /// take any bytes written before (E15).
    pub(super) async fn write(
        mut self,
    ) -> Result<(Self, Result<Flushed, DbError>), coven_foundation::task::TaskFailed> {
        let (bugs, mut batch) = {
            let slot = read_unpoisoned(&self.database);
            let inner = slot.as_ref().expect("staging holds close guard");
            let bugs = inner.access.directory.bugs().clone();
            let batch = FlushBatch::new(bugs.clone());
            #[cfg(test)]
            let batch = batch
                .with_hook(coven_foundation::locks::lock_unpoisoned(&inner.flush_hook).clone());
            (bugs, batch)
        };
        let mut folder = None;
        loop {
            let Run { bytes: run, stream } = self.next_run();
            if !run.is_empty() {
                // The staging guard goes with the task, so cancellation
                // cannot remove a name before this call creates its bytes.
                let held = folder.take();
                let (next, written) = coven_foundation::task::blocking_completing(
                    "stage copies",
                    bugs.clone(),
                    crate::commit_scope::bind(move || {
                        let written = (|| -> Result<_, DbError> {
                            let held =
                                held_folder(&self.database, held, &run[0].0.name, &mut batch)?;
                            let mut staged = Vec::with_capacity(run.len());
                            for (copy, bytes) in run {
                                held.write_whole(&copy.name.copy(), &bytes, &mut batch)?;
                                let mut hash = FileHasher::new();
                                hash.update(&bytes);
                                staged.push(copy.staged(bytes.len() as u64, hash));
                            }
                            Ok((held, batch, staged))
                        })();
                        Ok::<_, std::convert::Infallible>((self, written))
                    }),
                )
                .await?
                .unwrap_or_else(|never| match never {});
                self = next;
                match written {
                    Ok((held, returned, staged)) => {
                        folder = Some(held);
                        batch = returned;
                        self.staged.extend(staged);
                    }
                    Err(error) => return Ok((self, Err(error))),
                }
            }
            let Some((copy, mut reader)) = stream else { break };
            // Creation owns the staging guard until it finishes. Cancellation
            // cannot remove a name before this blocking call creates its bytes.
            let file_name = copy.name.clone();
            let held = folder.take();
            let mut creating = batch;
            // The staging comes back with the writer, or with why it wasn't
            // made, so no error reaches the sink unread: a dropped caller's
            // staging releases its names as it drops.
            let (next, writer, returned) = coven_foundation::task::blocking_completing(
                "create a staged copy",
                bugs.clone(),
                crate::commit_scope::bind(move || {
                    let writer = (|| -> Result<_, DbError> {
                        let held = held_folder(&self.database, held, &file_name, &mut creating)?;
                        let writer = held.create(&file_name.copy())?;
                        Ok((held, writer))
                    })();
                    Ok::<_, std::convert::Infallible>((self, writer, creating))
                }),
            )
            .await?
            .unwrap_or_else(|never| match never {});
            self = next;
            batch = returned;
            let writer = match writer {
                Ok((held, writer)) => {
                    folder = Some(held);
                    writer
                }
                Err(error) => return Ok((self, Err(error))),
            };
            let mut hash = FileHasher::new();
            let mut size = 0;
            let written = |bytes: &[u8]| {
                hash.update(bytes);
                size += bytes.len() as u64;
            };
            if let Err(error) = writer.write_from(&mut reader, written, &mut batch).await {
                return Ok((self, Err(error.into())));
            }
            self.staged.push(copy.staged(size, hash));
        }
        let flushed = batch.flush().await.map_err(DbError::from);
        Ok((self, flushed))
    }

    /// The sources up to the next stream, in order, as the bytes the app
    /// already holds, and that stream with its reader.
    fn next_run(&mut self) -> Run {
        let mut bytes = Vec::new();
        while let Some(StagingSource { copy, source }) = self.sources.pop_front() {
            match source {
                FileSource::Bytes(held) => bytes.push((copy, held)),
                FileSource::Stream(reader) => return Run { bytes, stream: Some((copy, reader)) },
            }
        }
        Run { bytes, stream: None }
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
        let _lease = self.lease.take();
        StagedFiles {
            reserved: self.reserved.clone(),
            names: std::mem::take(&mut self.names),
            files: std::mem::take(&mut self.staged),
            value: Some(value),
        }
    }
}

/// The staging's folder, which its copies are made in: `held` once the
/// staging has it, else made for `first`, its first copy, in the store
/// `database` holds open, for `batch` to flush with the copies.
fn held_folder(
    database: &RwLock<Option<DatabaseInner>>,
    held: Option<WriterFolder>,
    first: &StagedName,
    batch: &mut FlushBatch,
) -> Result<WriterFolder, DbError> {
    match held {
        Some(held) => Ok(held),
        None => {
            let slot = read_unpoisoned(database);
            let inner = slot.as_ref().expect("staging holds close guard");
            Ok(inner.access.directory.staging_folder(&first.folder(), batch)?)
        }
    }
}

impl Drop for FileStaging {
    fn drop(&mut self) {
        let Some(_lease) = self.lease.take() else {
            return;
        };
        self.reserved.release(&self.names);
    }
}

/// App bytes `stage_files` wrote to coven's copies of them, under names
/// reserved in this store, with what its batch returned, until a write puts
/// them in rows (§17.2). It holds nothing close waits for. Dropped
/// uncommitted, or after the store closes, its names go free, and the next
/// write or open removes the bytes.
pub struct StagedFiles<T> {
    /// The names the store that staged these files reserves.
    reserved: Arc<crate::reserved_names::ReservedNames>,
    names: Vec<OwnedFile>,
    files: Vec<StagedFile>,
    value: Option<T>,
}

impl<T> StagedFiles<T> {
    /// The names reserved for these files' copies.
    pub(crate) fn names(&self) -> &[OwnedFile] {
        &self.names
    }

    /// The files, their reserved names and the batch's value, for the write
    /// taking them; dropping what is left releases nothing.
    pub(crate) fn take(mut self) -> (Vec<StagedFile>, Vec<OwnedFile>, T) {
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
        self.reserved.release(&self.names);
    }
}

#[cfg(test)]
#[path = "file_staging_tests.rs"]
mod tests;
