//! Audience-package and blob preparation for Store commits.

use crate::sync::storage::PreparedExactObject;
use crate::sync::{circle, gate};

pub(crate) struct PreparedPartitionPackage {
    pub(crate) audience: circle::Audience,
    pub(crate) control: Option<gate::CirclePartitionControl>,
    pub(crate) key_fingerprint: Option<crate::KeyFingerprint>,
    pub(crate) semantic_bytes: Vec<u8>,
    pub(crate) prepared: PreparedExactObject,
    pub(crate) blobs: Vec<PreparedPartitionBlob>,
}

pub(crate) struct PreparedPartitionBlob {
    pub(crate) audience: crate::blob::locator::RemoteAudience,
    pub(crate) stored: crate::blob::locator::StoredBlobRef,
    pub(crate) spool_path: Option<std::path::PathBuf>,
    pub(crate) uploaded_verified: bool,
}
