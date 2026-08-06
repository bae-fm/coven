use super::*;
#[cfg(test)]
use crate::database::StoreDatabase;
use crate::sync::store::StoreError;
use coven_protocol::membership::MembershipChain;
use coven_protocol::objects::ExactObjectRef;
use coven_protocol::store_commit::{
    ActivatedStoreDeviceRegistration, DeviceJoinAttemptRef, DeviceJoinOutcomeRef, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreCommitOrder, StoreHistoryCut,
    StoreOperationMembershipAuthority, StoreRootRef,
};
use coven_protocol::{circle_control, membership, provider, remote_object, store_commit};
mod candidate;
mod plan;
mod publication;

#[cfg(test)]
mod tests;

pub(crate) use candidate::*;
pub(crate) use coven_protocol::prepared_commit::*;
pub(crate) use plan::*;
pub(crate) use publication::*;

pub(crate) const STORE_ROOT_AUTHORITY: &str = "store_root_authority";

pub(crate) fn successor_store_sequence(current: u64) -> Result<u64, StoreError> {
    current
        .checked_add(1)
        .ok_or(StoreError::SequenceExhausted { current })
}

pub(crate) fn next_store_sequence(
    previous: Option<&StoreBatchCommitRef>,
) -> Result<u64, StoreError> {
    previous.map_or(Ok(1), |reference| {
        successor_store_sequence(reference.coord.sequence())
    })
}
