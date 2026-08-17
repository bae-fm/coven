use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::StreamExt;
use tokio::sync::watch;
use tracing::{info, warn};

use super::{BlobCacheError, RemoteStoreBlobAccess};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EagerCacheFillProgress {
    pub files_done: u64,
    pub files_total: u64,
    pub bytes_done: u64,
    pub bytes_total: u64,
}

impl EagerCacheFillProgress {
    pub(crate) fn empty() -> Self {
        Self {
            files_done: 0,
            files_total: 0,
            bytes_done: 0,
            bytes_total: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub enum EagerCacheFillStatus {
    NotRunning,
    Scanning,
    Downloading(EagerCacheFillProgress),
    Complete {
        files_total: u64,
        bytes_total: u64,
    },
    Cancelled(EagerCacheFillProgress),
    Failed {
        progress: EagerCacheFillProgress,
        error: Arc<EagerCacheFillError>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum EagerCacheFillError {
    #[error("read eager cache bindings: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("inspect eager cache row {table}/{row_id}: {source}")]
    Inspect {
        table: String,
        row_id: String,
        #[source]
        source: BlobCacheError,
    },
    #[error("eager cache byte total exceeds the supported byte count")]
    ByteCountOverflow,
    #[error("remote eager cache row {table}/{row_id} has no exact stored reference")]
    MissingStoredReference { table: String, row_id: String },
    #[error("eager cache downloads ended before every admitted byte completed")]
    IncompleteProgress,
    #[error("download eager cache row {table}/{row_id}: {source}")]
    Download {
        table: String,
        row_id: String,
        #[source]
        source: BlobCacheError,
    },
}

struct EagerDownload {
    reference: coven_protocol::blob::RowBlobRef,
    stored_size: u64,
}

struct FileProgress {
    bytes: AtomicU64,
    total: u64,
}

impl FileProgress {
    fn new(total: u64) -> Self {
        Self {
            bytes: AtomicU64::new(0),
            total,
        }
    }
}

fn aggregate_progress(
    files: &[Arc<FileProgress>],
    completed_files: &AtomicU64,
    files_total: u64,
    bytes_total: u64,
) -> Result<EagerCacheFillProgress, EagerCacheFillError> {
    let mut bytes_done = 0_u64;
    for file in files {
        let current = file.bytes.load(Ordering::Relaxed);
        if current > file.total {
            return Err(EagerCacheFillError::ByteCountOverflow);
        }
        bytes_done = bytes_done
            .checked_add(current)
            .ok_or(EagerCacheFillError::ByteCountOverflow)?;
    }
    Ok(EagerCacheFillProgress {
        files_done: completed_files.load(Ordering::Relaxed),
        files_total,
        bytes_done,
        bytes_total,
    })
}

fn fail(
    status: &watch::Sender<EagerCacheFillStatus>,
    progress: EagerCacheFillProgress,
    error: EagerCacheFillError,
) -> Arc<EagerCacheFillError> {
    let error = Arc::new(error);
    status.send_replace(EagerCacheFillStatus::Failed {
        progress,
        error: Arc::clone(&error),
    });
    error
}

pub(crate) async fn run(
    database: &coven_database::StoreDatabase,
    access: &RemoteStoreBlobAccess,
    mut cancel: watch::Receiver<bool>,
    status: &watch::Sender<EagerCacheFillStatus>,
) -> Result<(), Arc<EagerCacheFillError>> {
    status.send_replace(EagerCacheFillStatus::Scanning);
    if *cancel.borrow() {
        status.send_replace(EagerCacheFillStatus::Cancelled(
            EagerCacheFillProgress::empty(),
        ));
        return Ok(());
    }

    let references = match database.eager_row_blob_refs().await {
        Ok(references) => references,
        Err(error) => return Err(fail(status, EagerCacheFillProgress::empty(), error.into())),
    };
    let mut downloads = Vec::new();
    for reference in references {
        if !matches!(
            reference.authority(),
            coven_protocol::blob::RowBlobAuthority::Remote(_)
        ) {
            continue;
        }
        let materialized = match access.is_materialized(&reference).await {
            Ok(materialized) => materialized,
            Err(source) => {
                let error = EagerCacheFillError::Inspect {
                    table: reference.table().to_string(),
                    row_id: reference.row_id().to_string(),
                    source,
                };
                return Err(fail(status, EagerCacheFillProgress::empty(), error));
            }
        };
        if materialized {
            continue;
        }
        let Some(stored) = reference.stored() else {
            let error = EagerCacheFillError::MissingStoredReference {
                table: reference.table().to_string(),
                row_id: reference.row_id().to_string(),
            };
            return Err(fail(status, EagerCacheFillProgress::empty(), error));
        };
        let stored_size = stored.object().stored_size();
        downloads.push(EagerDownload {
            reference,
            stored_size,
        });
    }

    let files_total = downloads.len() as u64;
    let bytes_total = match downloads.iter().try_fold(0_u64, |total, download| {
        total.checked_add(download.stored_size)
    }) {
        Some(total) => total,
        None => {
            return Err(fail(
                status,
                EagerCacheFillProgress::empty(),
                EagerCacheFillError::ByteCountOverflow,
            ))
        }
    };
    if downloads.is_empty() {
        status.send_replace(EagerCacheFillStatus::Complete {
            files_total,
            bytes_total,
        });
        return Ok(());
    }

    let file_progress = downloads
        .iter()
        .map(|download| Arc::new(FileProgress::new(download.stored_size)))
        .collect::<Vec<_>>();
    let completed_files = Arc::new(AtomicU64::new(0));
    let initial = EagerCacheFillProgress {
        files_done: 0,
        files_total,
        bytes_done: 0,
        bytes_total,
    };
    status.send_replace(EagerCacheFillStatus::Downloading(initial));

    let limit = database.transfer_limits().downloads.get();
    let stream = futures_util::stream::iter(downloads.into_iter().enumerate())
        .map(|(index, download)| {
            let progress = Arc::clone(&file_progress[index]);
            let completed_files = Arc::clone(&completed_files);
            async move {
                let callback_progress = Arc::clone(&progress);
                let callback: coven_storage::cloud::DownloadProgress =
                    Arc::new(move |bytes_done| {
                        callback_progress.bytes.store(bytes_done, Ordering::Relaxed);
                    });
                let table = download.reference.table().to_string();
                let row_id = download.reference.row_id().to_string();
                match access
                    .materialize_with_progress(&download.reference, callback)
                    .await
                {
                    Ok(()) => {
                        progress.bytes.store(progress.total, Ordering::Relaxed);
                        completed_files.fetch_add(1, Ordering::Relaxed);
                        Ok(())
                    }
                    Err(source) => Err(EagerCacheFillError::Download {
                        table,
                        row_id,
                        source,
                    }),
                }
            }
        })
        .buffer_unordered(limit);
    tokio::pin!(stream);
    let mut ticker = tokio::time::interval(crate::blob::progress::TRANSFER_PROGRESS_TICK);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    ticker.tick().await;
    let mut last_reported = initial;

    loop {
        tokio::select! {
            next = stream.next() => {
                match next {
                    Some(Ok(())) => {}
                    Some(Err(error)) => {
                        let progress = aggregate_progress(
                            &file_progress,
                            &completed_files,
                            files_total,
                            bytes_total,
                        ).map_err(|error| fail(status, last_reported, error))?;
                        return Err(fail(status, progress, error));
                    }
                    None => break,
                }
            }
            _ = ticker.tick() => {
                let current = aggregate_progress(
                    &file_progress,
                    &completed_files,
                    files_total,
                    bytes_total,
                ).map_err(|error| fail(status, last_reported, error))?;
                if current != last_reported {
                    last_reported = current;
                    status.send_replace(EagerCacheFillStatus::Downloading(current));
                }
            }
            changed = cancel.changed() => {
                if changed.is_err() || *cancel.borrow() {
                    let current = aggregate_progress(
                        &file_progress,
                        &completed_files,
                        files_total,
                        bytes_total,
                    ).map_err(|error| fail(status, last_reported, error))?;
                    status.send_replace(EagerCacheFillStatus::Cancelled(current));
                    info!(files_done = current.files_done, files_total, "eager cache fill cancelled");
                    return Ok(());
                }
            }
        }
    }

    let completed = aggregate_progress(&file_progress, &completed_files, files_total, bytes_total)
        .map_err(|error| fail(status, last_reported, error))?;
    if completed.files_done != files_total || completed.bytes_done != bytes_total {
        let error = EagerCacheFillError::IncompleteProgress;
        warn!(
            ?completed,
            "eager cache fill finished with incomplete counters"
        );
        return Err(fail(status, completed, error));
    }
    status.send_replace(EagerCacheFillStatus::Complete {
        files_total,
        bytes_total,
    });
    info!(files_total, bytes_total, "eager cache fill complete");
    Ok(())
}
