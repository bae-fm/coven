//! Async sources are consumed without holding SQLite or a blocking worker.

use super::{finish_blocking, DatabaseInner};
use crate::file_hashes::FileHasher;
use crate::file_write::{StagedFile, StagedSource};
use crate::write_batch::{BatchFile, BatchSource};
use crate::{DbError, FileSource, Place, Provenance, UserSource, WriteBatch};
use coven_foundation::files::StoreDirAccess;
use coven_foundation::files::{FileArea, FileName};
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
        let (sources, originals, value) = {
            let slot = database.read().expect("database lock poisoned");
            let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
            if let Some(error) = inner.access.observer.error() {
                return Err(error.into());
            }
            let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut batch = WriteBatch::new(inner.ids.as_ref());
                let value = build(&mut batch)?;
                Ok::<_, E>((batch, value))
            }));
            let built = match built {
                Ok(built) => built,
                Err(panic) => {
                    let chosen = inner.access.with_writer(|writer| {
                        Ok::<_, DbError>(super::released(writer, &inner.staging))
                    });
                    let failures = match chosen {
                        Ok(chosen) => inner.remove_chosen(chosen),
                        Err(error) => vec![error],
                    };
                    if !failures.is_empty() {
                        panic!("file cleanup after batch panic failed: {failures:?}");
                    }
                    std::panic::resume_unwind(panic)
                }
            };
            let prepared = inner.access.with_writer(|writer| {
                let prepared = built.and_then(|(batch, value)| {
                    if batch.files.is_empty() {
                        return Ok((VecDeque::new(), Vec::new(), value));
                    }
                    writer
                        .transaction(|db| {
                            let mut sources = VecDeque::new();
                            let mut originals = Vec::new();
                            for BatchFile {
                                id,
                                namespace,
                                source,
                            } in batch.files
                            {
                                let kind = match &source {
                                    BatchSource::App(..) => Provenance::AppProvided,
                                    BatchSource::User(_) => Provenance::UserProvided,
                                };
                                let declared = inner
                                    .access
                                    .write_schema
                                    .namespace(&namespace)
                                    .ok_or_else(|| DbError::UndeclaredNamespace {
                                        namespace: namespace.clone(),
                                    })?;
                                if declared.kind != kind {
                                    return Err(DbError::NamespaceKind {
                                        kind: declared.kind.clone(),
                                        namespace,
                                    });
                                }
                                match source {
                                    BatchSource::User(UserSource::DeviceOnly(observed)) => {
                                        originals.push(StagedFile {
                                            id,
                                            namespace,
                                            size: observed.observed.size(),
                                            source: StagedSource::Original(observed.observed),
                                            hashes: None,
                                            place: Place::DeviceOnly,
                                        })
                                    }
                                    BatchSource::User(UserSource::Cloud(prepared)) => {
                                        originals.push(staged_original(
                                            id,
                                            namespace,
                                            prepared,
                                            Place::Cloud,
                                        ))
                                    }
                                    BatchSource::User(UserSource::CloudKeptHere(prepared)) => {
                                        originals.push(staged_original(
                                            id,
                                            namespace,
                                            prepared,
                                            Place::CloudKeptHere,
                                        ))
                                    }
                                    BatchSource::App(source, place) => {
                                        let name = FileName::new(inner.ids.new_id().to_string())
                                            .expect("UUID is a portable filename");
                                        let recorded = db.internal_execute(
                                            "INSERT INTO _coven_file_removals(path) SELECT ?1
                                             WHERE NOT EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1)
                                                 AND NOT EXISTS(SELECT 1 FROM _coven_file_uploads WHERE copy=?1)
                                                 AND NOT EXISTS(SELECT 1 FROM _coven_pin_requests WHERE copy=?1)",
                                            [name.as_str()],
                                        )?;
                                        if recorded != 1 {
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
                                }
                            }
                            Ok((sources, originals, value))
                        })
                        .map_err(E::from)
                });
                match prepared {
                    Ok((sources, originals, value)) => {
                        inner
                            .staging
                            .lock()
                            .expect("file staging lock poisoned")
                            .extend(sources.iter().map(|s| s.name.clone()));
                        Ok::<_, E>(Ok((sources, originals, value)))
                    }
                    Err(error) => Ok(Err((error, super::released(writer, &inner.staging)))),
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
                staged: originals,
            },
            value,
        ))
    }

    pub(super) async fn write(mut self) -> (Self, Result<(), DbError>) {
        while let Some(source) = self.sources.pop_front() {
            let StagingSource {
                namespace,
                id,
                name,
                source,
                place,
            } = source;
            // Creation owns the staging guard until it finishes. Cancellation
            // cannot remove a name before this blocking call creates its bytes.
            let file_name = name.clone();
            let (next, writer) = finish_blocking(
                tokio::task::spawn_blocking(crate::commit_scope::bind(move || {
                    let writer = (|| -> Result<_, DbError> {
                        let slot = self.database.read().expect("database lock poisoned");
                        let inner = slot.as_ref().expect("staging holds close guard");
                        let lock = inner.access.directory.lock_read_only()?;
                        Ok(inner
                            .access
                            .directory
                            .file(FileArea::AppProvided, &file_name)
                            .create_writer(lock)?)
                    })();
                    (self, writer)
                }))
                .await,
            );
            self = next;
            let writer = match writer {
                Ok(writer) => writer,
                Err(error) => return (self, Err(error)),
            };
            let mut hash = FileHasher::new();
            let mut size = 0;
            let written = |bytes: &[u8]| {
                hash.update(bytes);
                size += bytes.len() as u64;
            };
            let result = match source {
                FileSource::Bytes(bytes) => writer.write_from(&mut bytes.as_slice(), written).await,
                FileSource::Stream(mut reader) => writer.write_from(&mut reader, written).await,
            };
            if let Err(error) = result {
                return (self, Err(error.into()));
            }
            self.staged.push(StagedFile {
                id,
                namespace,
                size,
                source: StagedSource::Copy(name),
                hashes: Some(hash.finish()),
                place,
            });
        }
        (self, Ok(()))
    }

    pub(super) fn finish<S, R, E: crate::WriteFailure>(
        mut self,
        prepared: Result<(), DbError>,
        sql: S,
    ) -> Result<R, E>
    where
        S: FnOnce(crate::SqlContext<'_, '_>) -> Result<R, E>,
    {
        let database = self.database.clone();
        let slot = database.read().expect("database lock poisoned");
        let inner = slot.as_ref().expect("staging holds close guard");
        inner.with_files(std::mem::take(&mut self.staged), |writer, files| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                prepared?;
                writer.local_write(
                    &inner.access.write_schema,
                    inner.access.device,
                    inner.clock.now(),
                    files,
                    sql,
                )
            }));
            inner.release_file_reservations(&self.names);
            let _lease = self.lease.take();
            #[cfg(any(test, feature = "test-utils"))]
            writer.checkpoint(crate::test_utils::WriteCheckpoint::StagingReleased);
            match result {
                Ok(result) => result,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        })
    }
}

/// A user's original read whole once, staged for the cloud.
fn staged_original(
    id: coven_foundation::id_source::FileId,
    namespace: String,
    prepared: crate::PreparedUserFile,
    place: Place,
) -> StagedFile {
    StagedFile {
        id,
        namespace,
        size: prepared.observed.size(),
        source: StagedSource::Original(prepared.observed),
        hashes: Some(prepared.hashes),
        place,
    }
}

impl Drop for FileStaging {
    fn drop(&mut self) {
        let Some(_lease) = self.lease.take() else {
            return;
        };
        let slot = self.database.read().expect("database lock poisoned");
        slot.as_ref()
            .expect("reservation holds close guard")
            .release_file_reservations(&self.names);
    }
}

#[cfg(test)]
#[path = "file_staging_tests.rs"]
mod tests;
