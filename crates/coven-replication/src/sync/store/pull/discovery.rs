use std::time::Duration;

use super::*;

pub(crate) struct MergeStreamDiscovery {
    pub(crate) latest_head: Option<StoreDeviceHead>,
    pub(crate) commits: Vec<(
        super::store_commit::StoreDeviceHeadRef,
        StoreDeviceHead,
        StoreBatchCommitRef,
        StoreBatchCommit,
    )>,
    pub(crate) block: Option<MergeStreamBlock>,
    pub(crate) reads: MergeStreamReadTiming,
}

/// How long the walk waited on the provider, split by what it read.
///
/// The walk probes one head slot per sequence number and stops on the first
/// miss, so even a stream with nothing new costs a remote read per device — and
/// each head it does find costs a second read for the commit behind it. Which
/// of the two dominates is the difference between "the store has new work" and
/// "probing for work nobody published is the whole cost", so the walk measures
/// them apart and hands both to whoever asked. Only the sync cycle turns them
/// into a log line; every other caller ignores them.
///
/// The read counts travel with the times because they answer different halves
/// of the same question: the time says how long the wait was, the count says
/// whether it was one slow read or a hundred fast ones. The walk is the only
/// place that can tell a head read from a commit read, so it is the only place
/// that can split the count the way it splits the time.
#[derive(Debug, Default, Clone, Copy)]
pub(crate) struct MergeStreamReadTiming {
    pub(crate) heads: Duration,
    pub(crate) head_reads: u64,
    pub(crate) commits: Duration,
    pub(crate) commit_reads: u64,
}

pub(crate) enum MergeStreamBlock {
    Unauthenticated(HeldStorePosition),
    Authenticated(HeldStorePosition),
}

impl MergeStreamBlock {
    pub(crate) fn into_position(self) -> HeldStorePosition {
        match self {
            Self::Unauthenticated(position) | Self::Authenticated(position) => position,
        }
    }
}
