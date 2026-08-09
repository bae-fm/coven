use crate::{
    DurablePreparedProtocolObject, PreparedAudienceObjects, StoreBatchCompletion,
    StoreBatchLocalCleanup,
};
use coven_protocol::objects::PreparedProtocolObject;
use coven_protocol::store_commit::{
    StoreBatchCommitRef, StoreDeviceHead, StoreRootRef, VerifiedStoreBatchCommit,
};
use coven_protocol::write::WriteId;

pub struct StoreWritePreparation {
    pub root: StoreRootRef,
    pub write_id: WriteId,
    pub remote_objects: Vec<coven_protocol::remote_object::ClosedRemoteObject>,
    pub audiences: PreparedAudienceObjects,
    pub commit: PreparedProtocolObject<VerifiedStoreBatchCommit>,
    pub head: PreparedProtocolObject<StoreDeviceHead>,
    pub history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}

pub struct MergeCandidateAbandonmentPreparation {
    pub write_id: WriteId,
    pub commit: PreparedProtocolObject<VerifiedStoreBatchCommit>,
    pub head: PreparedProtocolObject<StoreDeviceHead>,
    pub history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum PreparedStoreWriteState {
    Publication {
        commit: DurablePreparedProtocolObject,
        head: DurablePreparedProtocolObject,
        history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
    MergeAbandonment {
        candidate_commit: DurablePreparedProtocolObject,
        candidate_head: DurablePreparedProtocolObject,
        candidate_history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        authority_commit: DurablePreparedProtocolObject,
        authority_head: DurablePreparedProtocolObject,
        authority_history_evidence: coven_protocol::store_commit::RetainedMergeCommitEvidence,
        outcome: MergeAbandonmentOutcome,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub enum MergeAbandonmentOutcome {
    Prepared,
    Accepted {
        authority: StoreBatchCommitRef,
    },
    Lost {
        winner_commit: StoreBatchCommitRef,
        winner_head: coven_protocol::store_commit::StoreDeviceHeadRef,
    },
    AuthorExcluded,
}
