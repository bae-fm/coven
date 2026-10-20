//! The upload queue as the app observes it (E7): the committed queue, with
//! the uploads this process is sending now.

use coven_database::{CovenResult, DbError, FileRef, UploadQueueState};
use coven_storage::UploadSession;
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::SystemTime,
};
use tokio::sync::watch;

/// Observable upload queue state (E7): one committed snapshot of the queue,
/// with the uploads this process is sending as it is read.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadQueue {
    /// Whether transfer work is paused; persisted across reopening.
    pub paused: bool,
    /// Waiting files, oldest first.
    pub files: Vec<QueuedUpload>,
}
/// One queued upload: whether it is being sent, and its committed progress.
#[derive(Clone, Debug, PartialEq)]
pub struct QueuedUpload {
    /// Captured row version.
    pub file: FileRef,
    /// Whether this device is sending the file now.
    pub phase: UploadPhase,
    /// Progress the provider confirmed and this device persisted in the
    /// file's resumable session; `None` while it has no session, as a file
    /// sent in one request never does.
    pub progress: Option<UploadProgress>,
    /// Enqueue time.
    pub queued_at: SystemTime,
}
/// Whether a queued file is being sent. Nothing about it is persisted: a
/// reopened store starts with every file waiting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UploadPhase {
    /// Not being sent: not started yet, paused, waiting for a free upload
    /// slot or for its retry.
    Waiting,
    /// An attempt is sending it now, in one request or in parts.
    Uploading,
}
/// Bytes the provider confirmed in a recorded upload session. Sending bytes
/// alone does not advance it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct UploadProgress {
    /// Provider-confirmed ciphertext bytes.
    pub bytes_confirmed: u64,
    /// Whole ciphertext size.
    pub bytes_total: u64,
}

/// The uploads this process is sending now, by queue id. The upload worker
/// adds each attempt as it starts and removes it as it ends, cancelled
/// included. One shared owner per open store: clones share it.
#[derive(Clone)]
pub struct UploadsInFlight {
    shared: Arc<InFlight>,
}
struct InFlight {
    ids: Mutex<BTreeSet<i64>>,
    changed: watch::Sender<()>,
}
impl Default for UploadsInFlight {
    fn default() -> Self {
        Self {
            shared: Arc::new(InFlight {
                ids: Mutex::new(BTreeSet::new()),
                changed: watch::channel(()).0,
            }),
        }
    }
}
impl UploadsInFlight {
    fn ids(&self) -> std::sync::MutexGuard<'_, BTreeSet<i64>> {
        self.shared.ids.lock().expect("uploads in flight poisoned")
    }
    /// Changes whenever an upload starts or ends, so a live query of the
    /// committed queue reruns and projects it again.
    pub fn changes(&self) -> watch::Receiver<()> {
        self.shared.changed.subscribe()
    }
    /// Project a committed queue: each file this process is sending is
    /// uploading, and a recorded session's confirmed bytes are its progress.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    pub fn project(&self, state: UploadQueueState) -> CovenResult<UploadQueue> {
        let sending = self.ids().clone();
        let files = state
            .uploads
            .into_iter()
            .map(|upload| {
                let progress = upload
                    .session
                    .as_ref()
                    .map(|bytes| {
                        UploadSession::decode(bytes.as_bytes())
                            .map(|session| UploadProgress {
                                bytes_confirmed: session.confirmed_bytes(),
                                bytes_total: session.total_bytes(),
                            })
                            .map_err(|_| DbError::DamagedDatabase)
                    })
                    .transpose()?;
                Ok(QueuedUpload {
                    file: upload.file,
                    phase: if sending.contains(&upload.id) {
                        UploadPhase::Uploading
                    } else {
                        UploadPhase::Waiting
                    },
                    progress,
                    queued_at: upload.queued_at,
                })
            })
            .collect::<CovenResult<_>>()?;
        Ok(UploadQueue {
            paused: state.paused,
            files,
        })
    }
    /// Mark the upload as being sent until the returned guard drops.
    pub(crate) fn start(&self, id: i64) -> Sending {
        self.ids().insert(id);
        self.shared.changed.send_replace(());
        Sending {
            owner: self.clone(),
            id,
        }
    }
}

/// One upload being sent; dropping it, also when its attempt is cancelled,
/// marks the upload as no longer sent.
pub(crate) struct Sending {
    owner: UploadsInFlight,
    id: i64,
}
impl Drop for Sending {
    fn drop(&mut self) {
        self.owner.ids().remove(&self.id);
        self.owner.shared.changed.send_replace(());
    }
}
