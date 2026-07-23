use super::*;

pub(crate) struct PreparedStoreWrite {
    pub write_id: WriteId,
    pub changeset: Vec<u8>,
    pub partitions: PreparedStoreWritePartitions,
    pub inverse_changeset: Vec<u8>,
    pub base: StoreWriteBase,
    pub blob_facts: StoreWriteBlobFacts,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PreparedStoreWritePartitions {
    pub store: Option<gate::AudiencePartition>,
    pub circles: Vec<gate::AudiencePartition>,
    pub local: Option<gate::AudiencePartition>,
}

pub(crate) struct MergeReplayWriteOverlay {
    pub(crate) write_id: WriteId,
    pub(crate) partitions: PreparedStoreWritePartitions,
}

#[derive(Clone, Copy)]
pub(crate) enum StoreWriteRouting<'a> {
    Unscoped,
    MergeScoped(&'a EncryptionService),
}

impl PreparedStoreWritePartitions {
    #[cfg(test)]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &gate::AudiencePartition> {
        self.store
            .iter()
            .chain(self.circles.iter())
            .chain(self.local.iter())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreWriteBase {
    pub dependencies: BTreeMap<String, StoreBatchCommitRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreWriteBlobFacts {
    pub blobs: Vec<StoreWriteBlobFact>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreWriteBlobFact {
    pub table: String,
    pub row_id: String,
    pub row_stamp: String,
    pub column: String,
    pub blob: BlobRef,
    pub plaintext_size: u64,
    pub plaintext_hash: ObjectHash,
    pub external_path: Option<PathBuf>,
    pub previous: Option<StoreWriteRemoteBlob>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreWriteRemoteBlob {
    pub authority: crate::sync::audience_package::PackageAudience,
    pub stored: StoredBlobRef,
}

impl StoreWriteBlobFact {
    pub(crate) fn identity_key(&self) -> (String, String, String, String) {
        (
            self.table.clone(),
            self.row_id.clone(),
            self.column.clone(),
            self.row_stamp.clone(),
        )
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ExactProtocolObject<T> {
    pub value: T,
    pub bytes: Vec<u8>,
    pub object: ExactObjectRef,
    pub prepared: PreparedExactObject,
}

pub(crate) struct PreparedProtocolObject<T> {
    pub value: T,
    pub prepared: PreparedExactObject,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedStoreWriteCommit {
    pub audiences: PreparedAudienceObjects,
    pub commit: ExactProtocolObject<StoreBatchCommit>,
    pub head: ExactProtocolObject<StoreDeviceHead>,
}

#[derive(Debug, Clone)]
pub(crate) struct BlockedMergeCandidate {
    pub commit: ExactProtocolObject<StoreBatchCommit>,
    pub head: ExactProtocolObject<StoreDeviceHead>,
}

#[derive(Debug, Clone)]
pub(crate) struct PreparedMergeAbandonmentCandidates {
    pub(crate) candidate: BlockedMergeCandidate,
    pub(crate) authority: BlockedMergeCandidate,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CompletePreparedStoreWriteOutcome {
    Published,
    AuthorExcluded {
        device_id: crate::sync::store_commit::StoreDeviceId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct AuthorExclusionActivationLocator {
    exclusion: crate::sync::store_commit::StoreDeviceExclusionRef,
    accepted_cut: BTreeMap<crate::sync::causal_grants::AuthorStreamId, StoreBatchCommitRef>,
    activation_commit: StoreBatchCommitRef,
    activation_head: crate::sync::store_commit::StoreDeviceHeadRef,
}

impl AuthorExclusionActivationLocator {
    pub(crate) fn verified(
        exclusion: crate::sync::store_commit::StoreDeviceExclusionRef,
        accepted_cut: BTreeMap<crate::sync::causal_grants::AuthorStreamId, StoreBatchCommitRef>,
        activation_commit: StoreBatchCommitRef,
        activation_head: crate::sync::store_commit::StoreDeviceHeadRef,
    ) -> Self {
        Self {
            exclusion,
            accepted_cut,
            activation_commit,
            activation_head,
        }
    }

    pub(crate) fn exclusion(&self) -> &crate::sync::store_commit::StoreDeviceExclusionRef {
        &self.exclusion
    }

    pub(crate) fn accepted_cut(
        &self,
    ) -> &BTreeMap<crate::sync::causal_grants::AuthorStreamId, StoreBatchCommitRef> {
        &self.accepted_cut
    }

    pub(crate) fn activation_head(&self) -> &crate::sync::store_commit::StoreDeviceHeadRef {
        &self.activation_head
    }

    pub(crate) fn activation_commit(&self) -> &StoreBatchCommitRef {
        &self.activation_commit
    }
}

#[derive(Debug, Clone)]
pub(crate) enum TerminalCandidateAuthority {
    AuthorExclusion(AuthorExclusionActivationLocator),
    MembershipGrantRevocation {
        grant_id: crate::sync::membership::MembershipGrantId,
        membership: crate::sync::circle_control::StoreMembershipStateRef,
        activation_commit: StoreBatchCommitRef,
        activation_head: crate::sync::store_commit::StoreDeviceHeadRef,
    },
    DependencyRetraction(crate::sync::remote_object::VerifiedDependencyRetractionAuthority),
}

#[derive(Debug, Clone)]
pub(crate) struct TerminalCandidateCleanupVerification {
    pub(crate) authority: TerminalCandidateAuthority,
    pub(crate) candidate: BlockedMergeCandidate,
}

pub(crate) struct InitialStoreMembershipAuthority {
    pub head_refs: Vec<crate::sync::membership::MembershipHeadRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MergeAbandonmentState {
    None,
    Prepared,
    Accepted,
    CandidateWon,
    OtherWon,
    AuthorExcluded,
}

#[derive(Debug, Clone)]
pub(crate) struct OutboundStoreAck {
    pub reference: StoreAckRef,
    pub ack: ExactProtocolObject<StoreAck>,
    pub activation: OutboundStoreAckActivation,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum OutboundStoreAckActivation {
    AwaitingCandidate,
    Prepared(crate::sync::store::operations::PreparedStoreOperationCommit),
    Nonactivating(crate::sync::store::operations::PreparedStoreOperationCommit),
}

#[derive(Debug, Clone)]
pub(crate) struct PublishedStoreAck {
    pub reference: StoreAckRef,
    pub successor_slot: crate::storage::cloud::ObjectSlot,
}
