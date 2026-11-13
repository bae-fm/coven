//! Ending the provider session of an upload no file waits for any more,
//! before it retires (§17.9). Retiring the queue row alone would drop the
//! only record of the session, leaving the parts or temporary it holds at
//! the provider for good.

use crate::{files::file_failure::upload_subject, files::FilesInner, PendingRecord, SyncError};
use coven_storage::UploadSession;

impl FilesInner {
    /// Abort the recorded session of each upload no file waits for, not in
    /// flight and due, then forget it, so the upload retires with the rest.
    /// A session already complete needs no abort. A session that does
    /// not read, or a failed abort, is the
    /// upload's failure, recorded and retried as an attempt's is. Without
    /// storage, or while sync is stopped, nothing is aborted, and those
    /// uploads stay queued.
    pub(super) async fn end_abandoned_sessions(&self) -> Result<(), SyncError> {
        if !self.waits.started() {
            return Ok(());
        }
        let Ok(storage) = super::file_storage::connected(self.storage_state()) else {
            return Ok(());
        };
        let in_flight = self.database.uploads_in_flight().current();
        for (item, record) in
            self.database.abandoned_uploads::<PendingRecord>(upload_subject).await?
        {
            if in_flight.contains(&item.location.object())
                || !self.retry_due(&item, record.as_ref())
            {
                continue;
            }
            let session = match &item.session {
                Some(bytes) => match UploadSession::decode(bytes.as_bytes()) {
                    Ok(session) => session,
                    // A session that does not read is this upload's failure,
                    // not the run's: the other uploads go on.
                    Err(error) => {
                        self.record_failure(&item, error.into()).await?;
                        continue;
                    }
                },
                None => continue,
            };
            let ended =
                if session.is_complete() { Ok(()) } else { storage.abort_upload(&session).await };
            match ended {
                Ok(()) => self.database.forget_session(item.id).await?,
                Err(error) => self.record_failure(&item, error.into()).await?,
            }
        }
        Ok(())
    }
}
