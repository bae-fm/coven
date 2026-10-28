//! How far each file in flight one way has got, for a reader that draws each
//! one: all of them first, then each file whose progress moved, started or
//! ended (E7, E8). Progress lives in this process's memory and changes with
//! each step of bytes, so no table holds it and no live query reads it.

use crate::observation::CommitSubscription;
use crate::TransferProgress;
use coven_foundation::id_source::FileId;
use std::collections::BTreeMap;
use tokio::sync::watch;

/// A change to one file's progress, or every file's.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransferChange {
    /// Every file in flight, as a reader first hears them; a file not
    /// listed has nothing in flight.
    Reset(BTreeMap<FileId, TransferProgress>),
    /// The file started, or its progress moved.
    Updated {
        /// The file, as `coven_sending` or `coven_downloading` names it.
        file: FileId,
        /// How far it has got now.
        progress: TransferProgress,
    },
    /// The file's transfer ended, whether it finished or not.
    Removed {
        /// The file whose transfer ended.
        file: FileId,
    },
}

/// Hears how far each file in flight one way has got. Changes that arrive
/// faster than the reader reads are heard together: the next read names
/// each file that differs from what the reader last heard.
pub struct TransferWatch {
    progress: watch::Receiver<BTreeMap<FileId, TransferProgress>>,
    /// Ends the watch once the store closes.
    store: CommitSubscription,
    /// What the reader last heard; `None` before the first read.
    told: Option<BTreeMap<FileId, TransferProgress>>,
}

impl TransferWatch {
    pub(crate) fn new(
        progress: watch::Receiver<BTreeMap<FileId, TransferProgress>>,
        store: CommitSubscription,
    ) -> Self {
        Self {
            progress,
            store,
            told: None,
        }
    }

    /// Every file in flight as one `Reset` on the first read; after that,
    /// wait for a file's progress to change and name each file it changed
    /// for. `None` once the store has closed.
    pub async fn next(&mut self) -> Option<Vec<TransferChange>> {
        loop {
            if self.store.state().closed() {
                return None;
            }
            let Some(told) = &self.told else {
                let current = self.progress.borrow_and_update().clone();
                self.told = Some(current.clone());
                return Some(vec![TransferChange::Reset(current)]);
            };
            tokio::select! {
                changed = self.progress.changed() => changed.ok()?,
                () = self.store.changed() => continue,
            }
            let next = self.progress.borrow_and_update().clone();
            let changes = changes_between(told, &next);
            self.told = Some(next);
            if !changes.is_empty() {
                return Some(changes);
            }
        }
    }
}

/// What a reader told `told` must hear to hold `next`: the files gone, then
/// each file new or moved.
fn changes_between(
    told: &BTreeMap<FileId, TransferProgress>,
    next: &BTreeMap<FileId, TransferProgress>,
) -> Vec<TransferChange> {
    let mut changes: Vec<TransferChange> = told
        .keys()
        .filter(|file| !next.contains_key(*file))
        .map(|file| TransferChange::Removed { file: *file })
        .collect();
    changes.extend(
        next.iter()
            .filter(|(file, progress)| told.get(*file) != Some(*progress))
            .map(|(file, progress)| TransferChange::Updated {
                file: *file,
                progress: *progress,
            }),
    );
    changes
}

#[cfg(test)]
#[path = "transfer_watch_tests.rs"]
mod tests;
