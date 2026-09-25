use std::sync::Arc;

use coven_database::CommittedChanges;
use coven_replication::sync::BlobCacheError;

/// Whether each of a set of rows is kept offline, delivered again whenever the
/// answer changes.
///
/// Built by [`CovenHandle::subscribe_rows_pinned`](crate::CovenHandle::subscribe_rows_pinned).
/// The first [`next`](Self::next) answers immediately, one entry per row id in
/// order, as [`CovenHandle::rows_pinned`](crate::CovenHandle::rows_pinned)
/// does. Later calls wait until either a committed change (a row made Remote
/// or Local, a new blob version) or a change to the store's kept and cached
/// copies (a pin, an unpin, an upload keeping its copy, an eviction) could
/// change the answer, and deliver it only when it did change. Answers read
/// file presence and size, not file contents.
///
/// [`next`](Self::next) is cancel-safe: an answer stays owed until a read of
/// it completes. [`set_rows`](Self::set_rows) replaces the rows being watched,
/// so a host watching a scrolling page keeps one subscription.
pub struct RowsPinnedLiveQuery {
    blobs: crate::store_blobs::StoreBlobs,
    table: String,
    row_ids: Vec<String>,
    changes: tokio::sync::broadcast::Receiver<Arc<CommittedChanges>>,
    blob_copies: tokio::sync::watch::Receiver<u64>,
    answer_owed: bool,
    delivered: Option<Vec<Option<bool>>>,
}

impl RowsPinnedLiveQuery {
    pub(crate) fn new(
        blobs: crate::store_blobs::StoreBlobs,
        table: String,
        row_ids: Vec<String>,
    ) -> Self {
        let changes = blobs.subscribe_committed_changes();
        let blob_copies = blobs.subscribe_blob_copies();
        Self {
            blobs,
            table,
            row_ids,
            changes,
            blob_copies,
            answer_owed: true,
            delivered: None,
        }
    }

    /// Watch `row_ids` instead. The next call answers for them at once.
    pub fn set_rows(&mut self, row_ids: Vec<String>) {
        if row_ids != self.row_ids {
            self.row_ids = row_ids;
            self.answer_owed = true;
            self.delivered = None;
        }
    }

    /// Return the current answer on the first call and after
    /// [`set_rows`](Self::set_rows), otherwise wait for the answer to change.
    /// An error is delivered in place of an answer and does not end the
    /// subscription.
    pub async fn next(&mut self) -> Result<Vec<Option<bool>>, BlobCacheError> {
        loop {
            while !self.answer_owed {
                tokio::select! {
                    changes = self.changes.recv() => match changes {
                        Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                            self.answer_owed = true;
                        }
                        Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                            panic!("the pin-state live query retains its database")
                        }
                    },
                    changed = self.blob_copies.changed() => {
                        changed.expect("the pin-state live query retains its store directory");
                        self.answer_owed = true;
                    }
                }
            }
            let answer = self
                .blobs
                .rows_pinned(&self.table, self.row_ids.clone())
                .await;
            self.answer_owed = false;
            match answer {
                Ok(answer) if self.delivered.as_ref() == Some(&answer) => {}
                Ok(answer) => {
                    self.delivered = Some(answer.clone());
                    return Ok(answer);
                }
                Err(error) => {
                    self.delivered = None;
                    return Err(error);
                }
            }
        }
    }
}
