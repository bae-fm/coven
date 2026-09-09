use crate::{
    DurablePreparedProtocolObject, PreparedAudienceObjects, StoreBatchCompletion,
    StoreBatchLocalCleanup,
};
use coven_protocol::objects::PreparedProtocolObject;
use coven_protocol::store_commit::{StoreRootRef, VerifiedStoreBatchCommit};
use coven_protocol::write::WriteId;

pub struct StoreWritePreparation {
    pub root: StoreRootRef,
    pub write_id: WriteId,
    pub remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    pub audiences: PreparedAudienceObjects,
    pub commit: PreparedProtocolObject<VerifiedStoreBatchCommit>,
    pub publication: coven_protocol::prepared_commit::PreparedStorePublication,
    pub history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}

pub type StorePublicationPreparation = coven_protocol::prepared_commit::PreparedStorePublication;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct PreparedStoreWriteState {
    pub commit: DurablePreparedProtocolObject,
    pub history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}
