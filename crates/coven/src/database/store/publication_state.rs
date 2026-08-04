use crate::database::{
    DurablePreparedProtocolObject, PreparedAudienceObjects, StoreBatchCompletion,
    StoreBatchLocalCleanup,
};
use crate::protocol::objects::PreparedProtocolObject;
use crate::protocol::remote_object::RemoteObjectRecord;
use crate::protocol::store_commit::{
    StoreBatchCommitRef, StoreDeviceHead, StoreRootRef, VerifiedStoreBatchCommit,
};
use crate::write::WriteId;

pub(crate) struct StoreWritePreparation {
    pub root: StoreRootRef,
    pub write_id: WriteId,
    pub remote_objects: Vec<RemoteObjectRecord>,
    pub audiences: PreparedAudienceObjects,
    pub commit: PreparedProtocolObject<VerifiedStoreBatchCommit>,
    pub head: PreparedProtocolObject<StoreDeviceHead>,
    pub history_summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
    pub local_cleanup: StoreBatchLocalCleanup,
    pub completion: StoreBatchCompletion,
}

pub(crate) struct MergeCandidateAbandonmentPreparation {
    pub write_id: WriteId,
    pub commit: PreparedProtocolObject<VerifiedStoreBatchCommit>,
    pub head: PreparedProtocolObject<StoreDeviceHead>,
    pub history_summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum PreparedStoreWriteState {
    Publication {
        commit: DurablePreparedProtocolObject,
        head: DurablePreparedProtocolObject,
        history_summary: crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
    MergeAbandonment {
        candidate_commit: DurablePreparedProtocolObject,
        candidate_head: DurablePreparedProtocolObject,
        candidate_history_summary:
            crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        authority_commit: DurablePreparedProtocolObject,
        authority_head: DurablePreparedProtocolObject,
        authority_history_summary:
            crate::protocol::store_commit::RetainedVerifiedMergeHistorySummary,
        outcome: MergeAbandonmentOutcome,
        local_cleanup: StoreBatchLocalCleanup,
        completion: StoreBatchCompletion,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum MergeAbandonmentOutcome {
    Prepared,
    Accepted {
        authority: StoreBatchCommitRef,
    },
    Lost {
        winner_commit: StoreBatchCommitRef,
        winner_head: crate::protocol::store_commit::StoreDeviceHeadRef,
    },
    AuthorExcluded,
}
