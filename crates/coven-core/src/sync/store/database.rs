mod acknowledgements;
mod device_exclusion;
mod owner_promotion;
mod reclaim;

use crate::database::{
    begin_remote_candidate_nonactivation_on, finish_outbound_store_ack_on,
    load_activated_registration_on, load_outbound_store_ack_on, load_protocol_inert_object_on,
    load_remote_object_on, persist_exact_remote_object_on, replace_prepared_merge_head_remote_on,
    required_store_root_authority_on, CandidateCleanupObject, Database, DbError,
    OutboundStoreAckActivation,
};
use crate::sync::remote_object::{
    remote_object_id, CandidateNonactivationProof, VerifiedCandidateNonactivation,
};
use crate::sync::storage::PreparedExactObject;
use crate::sync::store::operations::PreparedStoreOperationCommit;
use crate::sync::store_commit::{StoreAckRef, StoreDeviceHead, StoreDeviceHeadRef};

#[derive(Clone, Copy)]
pub(super) struct StoreDatabase<'a> {
    database: &'a Database,
}

impl<'a> StoreDatabase<'a> {
    pub(super) fn new(database: &'a Database) -> Self {
        Self { database }
    }
}

#[cfg(test)]
pub(in crate::sync) async fn store_package_is_retained_for_replay_for_test(
    database: &Database,
    package: crate::sync::store_commit::StorePackageRef,
    activation: crate::sync::store_commit::StoreBatchCommitRef,
) -> Result<bool, DbError> {
    StoreDatabase::new(database)
        .store_package_is_retained_for_replay(package, activation)
        .await
}
