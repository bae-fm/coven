//! Async sources are consumed without holding SQLite or a blocking worker.

use super::{finish_blocking, Database};
use crate::file_hashes::FileHasher;
use crate::{file_write::StagedFile, DbError, FileSource, Provenance, WriteBatch};
use coven_foundation::files::{FileArea, FileName};
use std::collections::{BTreeSet, VecDeque};
use tokio::sync::OwnedRwLockReadGuard;

struct StagingSource {
    namespace: String,
    id: String,
    name: FileName,
    source: FileSource,
}

pub(super) struct FileStaging {
    database: Database,
    // Close waits until publication or cancellation releases the reservation.
    lease: Option<OwnedRwLockReadGuard<()>>,
    names: Vec<FileName>,
    sources: VecDeque<StagingSource>,
    staged: Vec<StagedFile>,
}

impl FileStaging {
    pub(super) fn new<E: crate::WriteFailure>(
        database: Database,
        lease: OwnedRwLockReadGuard<()>,
        build: impl FnOnce(&mut WriteBatch) -> Result<(), E>,
    ) -> Result<Self, E> {
        let sources = {
            let slot = database.inner.read().expect("database lock poisoned");
            let inner = slot.as_ref().ok_or(DbError::StoreClosed)?;
            let built = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let mut batch = WriteBatch::new();
                build(&mut batch)?;
                Ok::<_, E>(batch)
            }));
            inner.with_writer(|writer| {
                let built = match built {
                    Ok(built) => built,
                    Err(panic) => {
                        let active = inner.staging.lock().expect("file staging lock poisoned");
                        let failures = crate::file_removals::FileRemovals::new(
                            writer,
                            &inner.directory,
                            &active,
                        )
                        .remove_unused();
                        drop(active);
                        if !failures.is_empty() {
                            panic!("file cleanup after batch panic failed: {failures:?}");
                        }
                        std::panic::resume_unwind(panic)
                    }
                };
                let prepared = built.and_then(|batch| {
                    if batch.files.is_empty() {
                        return Ok(VecDeque::new());
                    }
                    writer
                        .transaction(|db| {
                            let mut identities = BTreeSet::new();
                            let mut sources = VecDeque::new();
                            for (namespace, id, source) in batch.files {
                                if !identities.insert((namespace.clone(), id.clone())) {
                                    return Err(DbError::FileBatchDuplicate { namespace, id });
                                }
                                if !inner.write_schema.declarations.iter().any(|d| {
                                    d.files.as_ref().is_some_and(|f| {
                                        f.namespace == namespace
                                            && f.provenance == Provenance::AppProvided
                                    })
                                }) {
                                    return Err(DbError::FileNamespaceNotAppProvided { namespace });
                                }
                                let name = FileName::new(inner.ids.new_id().to_string())
                                    .expect("UUID is a portable filename");
                                let recorded = db.internal_execute(
                                    "INSERT INTO _coven_file_removals(path) SELECT ?1
                         WHERE NOT EXISTS(SELECT 1 FROM _coven_device_files WHERE path=?1)",
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
                                });
                            }
                            Ok(sources)
                        })
                        .map_err(E::from)
                });
                let sources = match prepared {
                    Ok(sources) => sources,
                    Err(error) => {
                        let active = inner.staging.lock().expect("file staging lock poisoned");
                        return crate::file_removals::FileRemovals::new(
                            writer,
                            &inner.directory,
                            &active,
                        )
                        .finish(Err(error));
                    }
                };
                inner
                    .staging
                    .lock()
                    .expect("file staging lock poisoned")
                    .extend(sources.iter().map(|s| s.name.clone()));
                Ok::<_, E>(sources)
            })?
        };
        Ok(Self {
            database,
            lease: Some(lease),
            names: sources.iter().map(|s| s.name.clone()).collect(),
            sources,
            staged: Vec::new(),
        })
    }

    pub(super) async fn write(mut self) -> (Self, Result<(), DbError>) {
        while let Some(source) = self.sources.pop_front() {
            let StagingSource {
                namespace,
                id,
                name,
                source,
            } = source;
            // Creation owns the staging guard until it finishes. Cancellation
            // cannot remove a name before this blocking call creates its bytes.
            let file_name = name.clone();
            let (next, writer) = finish_blocking(
                tokio::task::spawn_blocking(move || {
                    let writer = (|| -> Result<_, DbError> {
                        let slot = self.database.inner.read().expect("database lock poisoned");
                        let inner = slot.as_ref().expect("staging holds close guard");
                        let lock = inner.directory.lock_read_only()?;
                        Ok(inner
                            .directory
                            .file(FileArea::AppProvided, &file_name)
                            .create_writer(lock)?)
                    })();
                    (self, writer)
                })
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
                namespace,
                id,
                name,
                size,
                hashes: hash.finish(),
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
        let slot = database.inner.read().expect("database lock poisoned");
        let inner = slot.as_ref().expect("staging holds close guard");
        inner.with_files(std::mem::take(&mut self.staged), |writer, files| {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                prepared?;
                writer.local_write(
                    &inner.write_schema,
                    inner.device,
                    inner.clock.now(),
                    files,
                    sql,
                )
            }));
            {
                let mut active = inner.staging.lock().expect("file staging lock poisoned");
                for name in &self.names {
                    assert!(active.remove(name), "staging name is registered");
                }
            }
            let _lease = self.lease.take();
            match result {
                Ok(result) => result,
                Err(panic) => std::panic::resume_unwind(panic),
            }
        })
    }
}

impl Drop for FileStaging {
    fn drop(&mut self) {
        let Some(_lease) = self.lease.take() else {
            return;
        };
        self.database.release_file_reservations(&self.names);
    }
}

#[cfg(test)]
#[path = "file_staging_tests.rs"]
mod tests;
