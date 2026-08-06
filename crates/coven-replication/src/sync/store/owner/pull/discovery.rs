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
