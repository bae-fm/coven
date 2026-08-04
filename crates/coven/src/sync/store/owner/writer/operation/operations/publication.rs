use super::*;

pub(crate) struct PreparedStoreOperationActivation {
    pub(crate) candidate: Box<PreparedStoreOperationCommit>,
    pub(crate) retained_operation_objects: Vec<ExactObjectRef>,
}
