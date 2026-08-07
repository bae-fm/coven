use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StoreOperationPublicationOutcome {
    Activated(StoreBatchCommitRef),
    Nonactivated(StoreBatchCommitRef),
    Reprepared,
    RepreparedCandidate(Box<PreparedStoreOperationCommit>),
    NonactivatedCandidate {
        candidate: Box<PreparedStoreOperationCommit>,
        nonactivation: Box<super::remote_object::VerifiedCandidateNonactivation>,
    },
}
