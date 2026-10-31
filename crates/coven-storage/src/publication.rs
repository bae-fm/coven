//! Per-request publication authority supplied by the synchronization owner.
use crate::{ObjectPath, StorageError};
use std::sync::Arc;

/// Re-evaluates the installation's authority immediately before a native request.
/// This capability carries no provider IO and must include elapsed sleep time.
pub trait PublicationGate: Send + Sync {
    /// Refuse without sending when this path cannot currently be published.
    fn check(&self, path: &ObjectPath) -> Result<(), StorageError>;
}

/// One destination and its shared gate, retained through native waits and retries.
#[derive(Clone)]
pub struct PublicationRequest {
    gate: Arc<dyn PublicationGate>,
    path: ObjectPath,
}
impl PublicationRequest {
    pub(crate) fn new(gate: Arc<dyn PublicationGate>, path: ObjectPath) -> Self {
        Self { gate, path }
    }
    /// Check again after waiting for credentials or a transfer permit, directly
    /// before starting the provider request. Success grants no reusable permit.
    pub fn check(&self) -> Result<(), StorageError> {
        self.gate.check(&self.path)
    }
}
impl std::fmt::Debug for PublicationRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PublicationRequest").field("path", &self.path).finish_non_exhaustive()
    }
}
