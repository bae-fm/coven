//! Authenticated inputs installed at one fixed store slot.
use crate::{BoundaryCoverage, IncomingSlot, OperationUpdate, StoreState};
use coven_format::admin::{SnapshotId, StoreChange};
use coven_foundation::id_source::{DeviceId, FileId, MemberId};
use coven_merge::{Audience, Slot};
use std::collections::BTreeSet;

/// One readable audience's starting prefix.
pub enum SnapshotSource<R> {
    /// A stored snapshot selected from authenticated history.
    Stored {
        /// Expected object identity.
        id: SnapshotId,
        /// Its staged records, from a body that authenticated (§16.2).
        input: R,
        /// Member whose key signed the body.
        author: MemberId,
    },
    /// History starts empty at slot zero.
    Empty(Audience),
    /// Preserve this audience's committed prefix and apply only subsequent slots.
    Current(Audience),
}
/// Files retained by a loaded snapshot.
pub struct LoadedSnapshot {
    /// Snapshot identity.
    pub id: SnapshotId,
    /// Fixed file paths in its rows and losses.
    pub files: BTreeSet<(DeviceId, FileId)>,
}
/// Complete checked input for an atomic reload. Sync derives the administrative
/// projection from the original signed history carried by the store snapshot
/// and retained slots. Each readable audience reaches `target` before commit.
pub struct SnapshotReload<R, W> {
    /// Starting snapshot or empty prefix for every readable audience.
    pub snapshots: Vec<SnapshotSource<R>>,
    /// Consecutive slots after the earliest snapshot through the fixed target.
    pub slots: Vec<IncomingSlot<W>>,
    /// Greatest selected coverage or required applied slot when loading began.
    pub target: u64,
    /// Authenticated membership and keys at the target.
    pub state: StoreState,
    /// Provider work required by the authenticated state at the fixed target.
    pub provider: crate::ProviderChanges,
    /// Counted admin changes introducing cut-offs, including compacted history.
    pub cutoffs: Vec<(Slot, StoreChange)>,
    /// Activated schema and reset snapshot coverage at the target.
    pub boundaries: Vec<BoundaryCoverage>,
    /// Initiating operation's transition committed with the rows.
    pub operation: Option<OperationUpdate>,
    /// Authentication of other members' write evidence.
    pub authorship: std::sync::Arc<dyn crate::Authorship>,
}
