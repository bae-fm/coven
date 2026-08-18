use super::*;

pub(crate) struct PreparedStoreOperationActivation {
    pub(crate) candidate: Box<PreparedStoreOperationCommit>,
    pub(crate) retained_operation_objects: Vec<ExactObjectRef>,
}

pub(crate) struct UploadedStoreOperationActivation {
    pub(crate) activation: PreparedStoreOperationActivation,
    pub(crate) verified_commit: coven_protocol::store_commit::VerifiedStoreBatchCommit,
    pub(crate) circle_activations: coven_protocol::circle_activation::VerifiedCircleActivations,
}
