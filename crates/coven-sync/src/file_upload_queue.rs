//! The upload queue as the app observes it (E7): the committed queue, with
//! the uploads this process is sending now.

pub use coven_database::UploadProgress;
use coven_database::UploadQueueState;
use coven_foundation::id_source::FileId;
use std::time::SystemTime;

/// Observable upload queue state (E7): one committed snapshot of the queue,
/// with the uploads this process is sending as it is read.
#[derive(Clone, Debug, PartialEq)]
pub struct UploadQueue {
    /// Whether transfer work is paused; persisted across reopening.
    pub paused: bool,
    /// Waiting files, oldest first. Files sharing one upload share its
    /// place and progress.
    pub files: Vec<QueuedUpload>,
}
/// One queued upload: whether it is being sent, and its committed progress.
#[derive(Clone, Debug, PartialEq)]
pub struct QueuedUpload {
    /// The file its upload switches to the cloud.
    pub file: FileId,
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
    /// slot or for its retry, or stored, waiting for the rest of its move
    /// (§17.3).
    Waiting,
    /// An attempt is sending it now, in one request or in parts.
    Uploading,
}

impl From<UploadQueueState> for UploadQueue {
    /// Each file this process was sending as the queue was read is
    /// uploading, beside the confirmed bytes its recorded session holds.
    fn from(state: UploadQueueState) -> Self {
        let files = state
            .uploads
            .into_iter()
            .flat_map(|upload| {
                let sending = &state.sending;
                upload
                    .files
                    .iter()
                    .map(move |file| QueuedUpload {
                        phase: if sending.contains(file) {
                            UploadPhase::Uploading
                        } else {
                            UploadPhase::Waiting
                        },
                        file: *file,
                        progress: upload.progress,
                        queued_at: upload.queued_at,
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        Self {
            paused: state.paused,
            files,
        }
    }
}
