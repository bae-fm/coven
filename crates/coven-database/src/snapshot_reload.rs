//! Inputs and journal progress committed by one snapshot reload.

use crate::{DownloadedWriteStream, OperationUpdate, WriteBoundary};
use coven_format::{sealed_snapshot::SnapshotObjectPrefix, store_log::SnapshotId};
use coven_merge::Audience;

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

/// Checked inputs and metadata that must advance together with the loaded rows.
pub struct SnapshotReload<R, W> {
    /// One starting state for each audience being replaced.
    pub snapshots: Vec<SnapshotSource<R>>,
    /// Authenticated writes needed to reach a common causal state.
    pub writes: Vec<DownloadedWriteStream<W>>,
    /// Absent storage writes covered by at least one selected snapshot. Their
    /// authenticated snapshot metadata advances the common log position.
    pub absent: Vec<coven_merge::WriteId>,
    /// Replace the reset/schema boundaries when store-log replay has changed.
    /// Ordinary recovery retains the existing boundary facts.
    pub boundaries: Option<Vec<WriteBoundary>>,
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

/// A validated snapshot's header and declared file references.
pub struct SnapshotInspection {
    /// Header metadata, including positions from the authenticated sealed prefix.
    pub header: coven_format::snapshot::SnapshotHeader,
    /// Uploaded files named by its synced rows.
    pub files: std::collections::BTreeSet<(
        coven_foundation::id_source::DeviceId,
        coven_foundation::id_source::FileId,
    )>,
}
