//! Inputs and journal progress committed by one snapshot reload.

use crate::{BoundaryCoverage, DownloadedWriteStream, OperationUpdate};
use coven_format::{sealed_snapshot::SnapshotObjectPrefix, store_log::SnapshotId};
use coven_foundation::id_source::{DeviceId, FileId};
use coven_merge::Audience;
use std::collections::BTreeSet;

/// One audience's starting state. An audience without a stored snapshot starts
/// empty and consumes its complete device-log history in the reload transaction.
pub enum SnapshotSource<R> {
    /// Authenticated plaintext supplied by sync.
    Stored {
        /// Expected object identity.
        id: SnapshotId,
        /// Authenticated sealed prefix carrying the snapshot positions.
        prefix: SnapshotObjectPrefix,
        /// Plaintext snapshot stream.
        input: R,
    },
    /// No stored snapshot exists for this readable audience.
    Empty(Audience),
}

/// A stored snapshot a committed reload read, with the uploaded files its
/// rows and losses name: file retention reuses them rather than reading the
/// body again (§16.5).
pub struct LoadedSnapshot {
    /// The snapshot's identity.
    pub id: SnapshotId,
    /// Uploaded files named by its rows and losses.
    pub files: BTreeSet<(DeviceId, FileId)>,
}

/// Checked inputs and metadata that must advance together with the loaded rows.
pub struct SnapshotReload<R, W> {
    /// One starting state for each audience being replaced.
    pub snapshots: Vec<SnapshotSource<R>>,
    /// Authenticated writes needed to reach a common causal state.
    pub writes: Vec<DownloadedWriteStream<W>>,
    /// Deleted writes some loaded audience lacks, established by counting
    /// coverage in a catalog listed after their absence (§15). One a loaded
    /// snapshot covers advances the common position by its authenticated
    /// metadata; one no loaded snapshot covers has no part in any loaded
    /// audience, and its number passes after its device's earlier writes.
    pub absent: Vec<coven_merge::WriteId>,
    /// Authenticated coverage for the effective readable boundary entries.
    /// A reload changes their application receipts without deleting retained facts.
    /// Ordinary recovery retains the preceding application receipts.
    pub boundaries: Option<Vec<BoundaryCoverage>>,
    /// Exact applied entries used to choose the inputs, sorted by identity.
    /// A changed store log rejects the transaction before any row is replaced.
    pub expected_entries: Option<Vec<coven_format::value::EntryId>>,
    /// Advance the initiating operation in the same commit as the loaded rows.
    pub operation: Option<OperationUpdate>,
}

impl<R, W> SnapshotReload<R, W> {
    /// Load stored snapshots and their gap writes, retaining existing boundaries
    /// and without an operation journal transition.
    pub fn new(
        snapshots: Vec<(SnapshotId, SnapshotObjectPrefix, R)>,
        writes: Vec<DownloadedWriteStream<W>>,
    ) -> Self {
        Self {
            snapshots: snapshots
                .into_iter()
                .map(|(id, prefix, input)| SnapshotSource::Stored { id, prefix, input })
                .collect(),
            writes,
            absent: Vec::new(),
            boundaries: None,
            expected_entries: None,
            operation: None,
        }
    }
}
