//! How far each file in flight one way has got, for a reader that draws each
//! one: all of them first, then each file whose progress moved, started or
//! ended, and how it ended (E7, E8). Progress lives in this process's memory
//! and changes with each step of bytes, so no table holds it and no live
//! query reads it.

use crate::observation::CommitSubscription;
use crate::transfers_in_flight::InFlight;
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
    /// The file's transfer ended.
    Removed {
        /// The file whose transfer ended.
        file: FileId,
        /// How it ended.
        end: TransferEnd,
    },
}

/// How a file's transfer ended, decided in the step that takes it out of
/// the files in flight. When two attempts move one file at once, the last
/// to end says it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TransferEnd {
    /// Its bytes all moved: an upload stored them, or a download assembled
    /// and checked the file's whole copy, which the write committing its
    /// pin keeps.
    Finished,
    /// It failed, and its failure went where the transfer's failures go:
    /// an upload's pending record, a pin's call.
    Failed,
    /// A hold stopped it before it finished, and nothing failed: a metered
    /// network, and for an upload also the pause, or a self-replacement
    /// pausing writes. It starts again once the hold ends (§12.3, §17.8,
    /// §17.9).
    Held,
    /// Whatever moved its bytes dropped it before it ended, as a pin's
    /// caller does that drops the call.
    Dropped,
}

/// What a reader last heard of one file: how far it had got, or, once it
/// ended since, how.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Heard {
    Moved(TransferProgress),
    Ended(TransferEnd),
}

/// What a reader that heard `heard` must hear to hold `now`, the files in
/// flight: each file that ended, then each file new, moved, or started
/// again since it ended. `heard` becomes `now`.
pub(crate) fn changes_since(
    heard: &mut BTreeMap<FileId, Heard>,
    now: BTreeMap<FileId, TransferProgress>,
) -> Vec<TransferChange> {
    let mut changes: Vec<TransferChange> = heard
        .iter()
        .filter_map(|(file, heard)| match heard {
            Heard::Ended(end) => Some(TransferChange::Removed { file: *file, end: *end }),
            Heard::Moved(_) => None,
        })
        .collect();
    changes.extend(
        now.iter()
            .filter(|(file, progress)| heard.get(*file) != Some(&Heard::Moved(**progress)))
            .map(|(file, progress)| TransferChange::Updated { file: *file, progress: *progress }),
    );
    *heard = now.into_iter().map(|(file, progress)| (file, Heard::Moved(progress))).collect();
    changes
}

/// Hears how far each file in flight one way has got. Changes that arrive
/// faster than the reader reads are heard together: the next read names
/// each file that differs from what the reader last heard, and how each
/// one it heard in flight ended.
pub struct TransferWatch {
    owner: InFlight,
    /// This watch's number among its owner's.
    number: u64,
    /// Told each time a file starts, moves or ends.
    changed: watch::Receiver<()>,
    /// Ends the watch once the store closes.
    store: CommitSubscription,
}

impl TransferWatch {
    pub(crate) fn new(
        owner: InFlight,
        number: u64,
        changed: watch::Receiver<()>,
        store: CommitSubscription,
    ) -> Self {
        Self { owner, number, changed, store }
    }

    /// Every file in flight as one `Reset` on the first read; after that,
    /// wait for a file's progress to change and name each file it changed
    /// for. `None` once the store has closed.
    pub async fn next(&mut self) -> Option<Vec<TransferChange>> {
        loop {
            if self.store.state().closed() {
                return None;
            }
            // Seen before the read, so a change after the read wakes the
            // wait below.
            self.changed.borrow_and_update();
            let changes = self.owner.read(self.number);
            if !changes.is_empty() {
                return Some(changes);
            }
            tokio::select! {
                changed = self.changed.changed() => {
                    changed.expect("the owner this watch holds keeps the sender");
                }
                () = self.store.changed() => {}
            }
        }
    }
}

impl Drop for TransferWatch {
    fn drop(&mut self) {
        self.owner.forget(self.number);
    }
}

#[cfg(test)]
#[path = "transfer_watch_tests.rs"]
mod tests;
