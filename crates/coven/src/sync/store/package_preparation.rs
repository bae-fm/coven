//! Audience-package and blob preparation for Store commits.

use crate::database::CirclePartitionControl;
use coven_protocol::circle;
use coven_protocol::objects::PreparedExactObject;

pub(crate) struct PreparedPartitionPackage {
    pub(crate) audience: circle::Audience,
    pub(crate) control: Option<CirclePartitionControl>,
    pub(crate) key_fingerprint: Option<crate::KeyFingerprint>,
    pub(crate) semantic_bytes: Vec<u8>,
    pub(crate) prepared: PreparedExactObject,
    pub(crate) blobs: Vec<PreparedPartitionBlob>,
}

pub(crate) struct PreparedPartitionBlob {
    pub(crate) audience: coven_protocol::blob::locator::RemoteAudience,
    pub(crate) stored: coven_protocol::blob::locator::StoredBlobRef,
    pub(crate) spool_path: Option<std::path::PathBuf>,
    pub(crate) uploaded_verified: bool,
}

impl PreparedPartitionBlob {
    pub(crate) fn merge_exact_duplicate(
        &mut self,
        duplicate: Self,
    ) -> Result<(), super::StoreError> {
        if self.audience != duplicate.audience || self.stored != duplicate.stored {
            return Err(super::StoreError::InvalidOutbound(format!(
                "prepared blob object {} has conflicting exact references",
                coven_protocol::remote_object::remote_object_id(self.stored.object())
            )));
        }
        self.spool_path = match (&self.spool_path, duplicate.spool_path) {
            (Some(left), Some(right)) if left != &right => {
                return Err(super::StoreError::InvalidOutbound(format!(
                    "prepared blob object {} has conflicting spool paths",
                    coven_protocol::remote_object::remote_object_id(self.stored.object())
                )));
            }
            (Some(left), _) => Some(left.clone()),
            (None, right) => right,
        };
        self.uploaded_verified |= duplicate.uploaded_verified;
        if !self.uploaded_verified && self.spool_path.is_none() {
            return Err(super::StoreError::InvalidOutbound(format!(
                "prepared blob object {} awaiting upload has no local spool",
                coven_protocol::remote_object::remote_object_id(self.stored.object())
            )));
        }
        Ok(())
    }
}
