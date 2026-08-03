use super::reclaim as store_reclaim;
use super::*;
use super::{PreparedMembershipPublication, PreparedMembershipTransition};
#[cfg(test)]
use crate::database::StoreDatabase;
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{
    head_slot_prefix, ActivatedStoreDeviceRegistration, DeviceJoinAttemptRef, DeviceJoinOutcomeRef,
    ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreCommitOperationsInput, StoreCommitOrder, StoreControl, StoreDeviceHead,
    StoreDeviceHeadRef, StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreHistoryCut,
    StoreOperationMembershipAuthority, StoreRootRef,
};
use crate::protocol::{
    circle_control, membership, provider, remote_object, store_commit, wrapped_store_key,
};
use crate::storage::StoreObjectError;
use crate::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain,
};
use crate::sync::store::owner::{device_join, owner_promotion};
use crate::sync::store::StoreError;
mod candidate;
mod plan;
mod prepared;
mod publication;

#[cfg(test)]
mod tests;

pub(crate) use candidate::*;
pub(crate) use plan::*;
pub(crate) use prepared::*;
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
