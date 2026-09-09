use super::*;

pub(crate) struct UploadedStoreOperationActivation {
    pub(crate) candidate: Box<PreparedStoreOperationCommit>,
    pub(crate) verified_commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
    pub(crate) circle_activations: coven_protocol::circle_activation::VerifiedCircleActivations,
}
