use super::membership_mutation as invite;
use super::reclaim as store_reclaim;
use super::*;
use crate::database::{MergeMaterializationTransaction, StoreDatabase};
use crate::protocol::membership::MembershipChain;
use crate::protocol::store_commit::{
    circle_package_semantic_prefix, commit_semantic_prefix, head_slot_prefix,
    package_semantic_prefix, ActivatedStoreDeviceRegistrationRef, DeviceJoinAttemptRef,
    DeviceJoinOutcomeRef, ObjectHash, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreCommitOperationsInput, StoreCommitOrder, StoreControl, StoreDeviceHead,
    StoreDeviceHeadRef, StoreDeviceRegistration, StoreDeviceRegistrationRef, StoreHistoryCut,
    StoreOperationMembershipAuthority, StoreRootRef,
};
use crate::protocol::{
    audience_package, circle_control, membership, provider, remote_object, store_commit,
    wrapped_store_key,
};
use crate::storage::StoreObjectError;
use crate::storage::{
    BlobWriteAuthority, ExactObjectRef, PreparedExactObject, ProtocolObjectContext,
    ProtocolObjectDomain,
};
use crate::sync::store::owner::{device_join, owner_promotion};
use crate::sync::store::{StoreError, StorePreparationError};
mod candidate;
mod plan;
mod prepared;
mod publication;
mod support;

#[cfg(test)]
mod tests;

pub(crate) use candidate::*;
pub(crate) use plan::*;
pub(crate) use prepared::*;
pub(crate) use publication::*;
pub(crate) use support::*;

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

pub(crate) async fn upload_commit(
    storage: &dyn SyncStorage,
    candidate: &PreparedStoreOperationCommit,
) -> Result<(), StoreError> {
    let stream_id = candidate.reference.coord.stream_id;
    let context = ProtocolObjectContext::signed_plaintext(
        candidate.commit.store_root_hash,
        ProtocolObjectDomain::StoreCommit,
    );
    let prefix = commit_semantic_prefix(
        candidate.commit.candidate_family(),
        &stream_id.to_string(),
        candidate.commit.seq(),
        candidate.commit.commit_hash(),
    );
    storage
        .create_protocol_object(&candidate.prepared)
        .await
        .map_err(StoreObjectError::from)?;
    let opened = storage
        .read_protocol_object(&context, &candidate.reference.object, &prefix)
        .await
        .map_err(StoreObjectError::from)?;
    if opened != candidate.commit.to_bytes() {
        return Err(StoreError::InvalidOutbound(
            "Store operation commit exact readback differs from its signed bytes".to_string(),
        ));
    }
    Ok(())
}
