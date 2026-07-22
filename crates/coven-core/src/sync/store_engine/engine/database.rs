mod acknowledgements;

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
use crate::sync::store_commit::{StoreAckRef, StoreDeviceHead, StoreDeviceHeadRef};
use crate::sync::store_outbound::PreparedStoreOperationCommit;

#[derive(Clone, Copy)]
pub(super) struct StoreEngineDatabase<'a> {
    database: &'a Database,
}

impl<'a> StoreEngineDatabase<'a> {
    pub(super) fn new(database: &'a Database) -> Self {
        Self { database }
    }
}
