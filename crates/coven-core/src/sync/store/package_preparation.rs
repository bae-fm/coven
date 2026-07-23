//! Audience-package and blob preparation for Store commits.

use crate::sync::storage::{BlobWriteAuthority, SyncStorage};
use crate::sync::storage::{PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain};
use crate::sync::store_commit::{
    circle_package_semantic_prefix, package_semantic_prefix, CandidateFamilyId, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
};
use crate::sync::store_objects::StoreObjectError;
use crate::sync::{audience_package, circle, gate, remote_object, storage};

use crate::database::{
    PreparedAudienceBlob, PreparedAudienceObjects, PreparedAudiencePackage, StoreWriteBlobFact,
    StoreWriteBlobFacts,
};
use crate::store_dir::StoreDir;

mod audience_preparation;

use super::{StoreDatabase, StoreError};
pub(crate) use audience_preparation::*;

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

#[cfg(test)]
mod tests;
