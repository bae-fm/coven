//! Exact storage access for signed protocol objects and stored blob bodies.
//!
//! Every remote object is addressed by an [`ExactObjectRef`]. The logical key
//! supplies domain separation and the physical locator selects the one provider
//! object whose stored size and hash the signed reference authenticates. Prefix
//! enumeration and provider names never select protocol authority.
use std::num::NonZeroU64;
use std::path::Path;

use serde::{Deserialize, Deserializer, Serialize};

use crate::protocol::membership::AuthorHead;
use crate::protocol::store_commit::{ObjectHash, StoreDeviceRegistration, StoreProtocolError};

mod domains;
mod provider_binding;
mod rotation;

pub(crate) use domains::{
    CircleProtocolObjectDomain, ProtectedObjectDomain, ProtocolObjectDomain,
    RecipientSealedProtocolObjectDomain, SignedStoreProtocolObjectDomain,
    StoreEncryptedProtocolObjectDomain,
};
pub use provider_binding::*;
#[cfg(test)]
pub(crate) use rotation::LocalRotation;
pub(crate) use rotation::RotationPending;
#[cfg(test)]
pub(crate) use rotation::RotationPendingState;
pub(crate) use rotation::{RotationGate, ROTATION_GATE_STATE_KEY};

/// Authenticated storage context for one immutable semantic object.
///
/// Store protection cannot be paired with a Circle-encrypted domain:
///
/// ```compile_fail
/// use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain};
/// use crate::ObjectHash;
///
/// let root = ObjectHash::digest(b"store");
/// let _ = ProtocolObjectContext::signed_plaintext(root, ProtocolObjectDomain::CircleMetadata);
/// ```
///
/// Circle protection cannot be paired with a Store domain:
///
/// ```compile_fail
/// use crate::storage::{ProtocolObjectContext, ProtocolObjectDomain};
/// use crate::{EncryptionService, ObjectHash};
///
/// let root = ObjectHash::digest(b"store");
/// let encryption = EncryptionService::from_key([7; 32]);
/// let _ = ProtocolObjectContext::circle(
///     root,
///     ProtocolObjectDomain::StoreCommit,
///     encryption,
/// );
/// ```
pub(crate) struct ProtocolObjectContext {
    store_root_hash: ObjectHash,
    domain: ProtectedObjectDomain,
    protection: ProtocolObjectProtection,
}

#[derive(Clone)]
pub(crate) enum ProtocolObjectProtection {
    StoreEncrypted,
    SignedPlaintext,
    Circle(crate::encryption::EncryptionService),
    RecipientSealed,
}

impl ProtocolObjectContext {
    pub(crate) fn store_encrypted(
        store_root_hash: ObjectHash,
        domain: StoreEncryptedProtocolObjectDomain,
    ) -> Self {
        Self {
            store_root_hash,
            domain: domain.0,
            protection: ProtocolObjectProtection::StoreEncrypted,
        }
    }

    pub(crate) fn signed_plaintext(
        store_root_hash: ObjectHash,
        domain: SignedStoreProtocolObjectDomain,
    ) -> Self {
        Self {
            store_root_hash,
            domain: domain.0,
            protection: ProtocolObjectProtection::SignedPlaintext,
        }
    }

    pub(crate) fn circle(
        store_root_hash: ObjectHash,
        domain: CircleProtocolObjectDomain,
        encryption: crate::encryption::EncryptionService,
    ) -> Self {
        Self {
            store_root_hash,
            domain: domain.0,
            protection: ProtocolObjectProtection::Circle(encryption),
        }
    }

    pub(crate) fn recipient_sealed(
        store_root_hash: ObjectHash,
        domain: RecipientSealedProtocolObjectDomain,
    ) -> Self {
        Self {
            store_root_hash,
            domain: domain.0,
            protection: ProtocolObjectProtection::RecipientSealed,
        }
    }

    pub(crate) fn store_root_hash(&self) -> ObjectHash {
        self.store_root_hash
    }

    pub(crate) fn domain(&self) -> ProtectedObjectDomain {
        self.domain
    }

    pub(crate) fn protection(&self) -> &ProtocolObjectProtection {
        &self.protection
    }

    pub(crate) fn validate_path(&self, semantic_prefix: &str) -> Result<(), StorageError> {
        let metadata = self.domain.metadata();
        if semantic_prefix.contains("/copies/") || !metadata.path.accepts(semantic_prefix) {
            return Err(StorageError::Parse(format!(
                "object domain {:?} does not accept semantic path {semantic_prefix:?}",
                self.domain
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_extension(&self, extension: &str) -> Result<(), StorageError> {
        if extension != self.domain.extension() {
            return Err(StorageError::Parse(format!(
                "object domain {:?} does not accept extension {extension:?}",
                self.domain
            )));
        }
        Ok(())
    }

    pub(crate) fn validate_reference(
        &self,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<(), StorageError> {
        self.validate_slot(object.slot(), semantic_prefix)
    }

    pub(crate) fn validate_slot(
        &self,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(), StorageError> {
        self.validate_path(semantic_prefix)?;
        let expected = format!("{semantic_prefix}{}", self.domain.extension());
        if slot.logical_key() != expected {
            return Err(StorageError::Parse(format!(
                "protocol object {:?} does not match semantic path {semantic_prefix:?}",
                slot.logical_key()
            )));
        }
        Ok(())
    }
}

/// Protection selected by the audience authority that prepares a blob spool.
#[derive(Clone)]
pub(crate) enum BlobSpoolProtection {
    Opaque(crate::encryption::EncryptionService),
    Browsable,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BlobSpoolWrite {
    Created,
    Reused,
}

#[derive(Clone, Copy)]
pub(crate) struct BlobWriteAuthority<'a> {
    pub reference: &'a crate::protocol::store_commit::StoreDeviceRegistrationRef,
    pub registration: &'a crate::protocol::store_commit::StoreDeviceRegistration,
}

impl<'a> BlobWriteAuthority<'a> {
    pub(crate) fn new(
        registration: &'a crate::protocol::store_commit::ReferencedStoreDeviceRegistration,
    ) -> Self {
        Self {
            reference: registration.reference(),
            registration: registration.value(),
        }
    }
}

/// Exact stored representation of one immutable object.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(deny_unknown_fields)]
pub struct ExactObjectRef {
    slot: ObjectSlot,
    stored_size: u64,
    stored_hash: ObjectHash,
}

impl ExactObjectRef {
    pub fn new(slot: ObjectSlot, stored_size: u64, stored_hash: ObjectHash) -> Self {
        Self {
            slot,
            stored_size,
            stored_hash,
        }
    }

    pub fn slot(&self) -> &ObjectSlot {
        &self.slot
    }

    pub fn stored_size(&self) -> u64 {
        self.stored_size
    }

    pub fn stored_hash(&self) -> ObjectHash {
        self.stored_hash
    }

    pub fn verify(&self, bytes: &[u8]) -> Result<(), StorageError> {
        if bytes.len() as u64 != self.stored_size || ObjectHash::digest(bytes) != self.stored_hash {
            return Err(StorageError::InvalidContent(format!(
                "exact object {} does not match stored size/hash",
                self.slot.logical_key()
            )));
        }
        Ok(())
    }

    /// Check independently computed file facts against the stored identity.
    /// Reading the file and computing its facts is the filesystem owner's
    /// operation; this value only compares.
    pub(crate) fn verify_stored_facts(
        &self,
        path: &Path,
        size: u64,
        hash: ObjectHash,
    ) -> Result<(), StorageError> {
        if size != self.stored_size || hash != self.stored_hash {
            return Err(StorageError::InvalidContent(format!(
                "exact file {} does not match stored identity for {}",
                path.display(),
                self.slot.logical_key()
            )));
        }
        Ok(())
    }
}

/// Immutable stored bytes and the exact reference derived from them.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedExactObject {
    reference: ExactObjectRef,
    stored_bytes: Vec<u8>,
}

impl<'de> serde::Deserialize<'de> for PreparedExactObject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            reference: ExactObjectRef,
            stored_bytes: Vec<u8>,
        }

        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.reference, fields.stored_bytes).map_err(serde::de::Error::custom)
    }
}

impl PreparedExactObject {
    pub fn new(reference: ExactObjectRef, stored_bytes: Vec<u8>) -> Result<Self, StorageError> {
        reference.verify(&stored_bytes)?;
        Ok(Self {
            reference,
            stored_bytes,
        })
    }

    pub fn reference(&self) -> &ExactObjectRef {
        &self.reference
    }

    pub fn stored_bytes(&self) -> &[u8] {
        &self.stored_bytes
    }
}

/// Error type for storage operations.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum StorageError {
    #[error("storage operation failed: {0}")]
    Storage(String),
    #[error("{operation}; storage cleanup failed: {cleanup}")]
    CleanupFailed {
        #[source]
        operation: Box<StorageError>,
        cleanup: Box<StorageError>,
    },
    #[error("{operation}; exact response-loss readback failed: {readback}")]
    UnresolvedOutcome {
        #[source]
        operation: Box<StorageError>,
        readback: Box<StorageError>,
    },
    #[error("storage configuration is invalid: {0}")]
    Configuration(String),
    #[error("storage object parse failed: {0}")]
    Parse(String),
    #[error("object not found: {0}")]
    NotFound(String),
    #[error("storage object already exists: {0}")]
    AlreadyExists(String),
    #[error("reserved storage slot contains different bytes: {0}")]
    SlotCollision(String),
    /// The object read back from its exact reference opened to bytes other than
    /// the ones the caller sealed into it.
    #[error("exact readback differs from the sealed bytes: {0}")]
    ReadbackMismatch(String),
    #[error("decryption failed: {0}")]
    Decryption(String),
    #[error("remote blob content is invalid: {0}")]
    InvalidContent(String),
    #[error("local blob filesystem failed: {0}")]
    LocalFilesystem(String),
    /// This device has not adopted a store-key rotation the cloud already
    /// committed; see [`RotationPending`].
    #[error("{0}")]
    RotationPending(#[from] RotationPending),
}

impl StorageError {
    pub fn is_transport(&self) -> bool {
        match self {
            Self::Storage(_) => true,
            Self::CleanupFailed { operation, .. } | Self::UnresolvedOutcome { operation, .. } => {
                operation.is_transport()
            }
            _ => false,
        }
    }

    pub fn cleanup_causes(&self) -> Option<(&StorageError, &StorageError)> {
        match self {
            Self::CleanupFailed { operation, cleanup } => Some((operation, cleanup)),
            _ => None,
        }
    }
}

impl From<coven_foundation::store_dir::PathTokenError> for StorageError {
    /// A blob id/namespace/cloud_path that can't form a safe object key is bad
    /// data, surfaced so the caller refuses the blob rather than reaching storage
    /// with a key that could escape its prefix.
    fn from(e: coven_foundation::store_dir::PathTokenError) -> Self {
        StorageError::Parse(format!("unsafe blob path: {e}"))
    }
}

/// Provider-specific physical address for a caller-reserved immutable slot.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum PhysicalObjectLocator {
    LogicalKey,
    Opaque(String),
}

/// Exact logical and physical location persisted before an immutable write.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectSlot {
    logical_key: String,
    physical: PhysicalObjectLocator,
}

impl<'de> Deserialize<'de> for ObjectSlot {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Fields {
            logical_key: String,
            physical: PhysicalObjectLocator,
        }

        let fields = Fields::deserialize(deserializer)?;
        Self::new(fields.logical_key, fields.physical).map_err(serde::de::Error::custom)
    }
}

impl ObjectSlot {
    pub fn logical(logical_key: String) -> Result<Self, StorageError> {
        Self::new(logical_key, PhysicalObjectLocator::LogicalKey)
    }

    pub fn opaque(logical_key: String, provider_id: String) -> Result<Self, StorageError> {
        Self::new(logical_key, PhysicalObjectLocator::Opaque(provider_id))
    }

    fn new(logical_key: String, physical: PhysicalObjectLocator) -> Result<Self, StorageError> {
        let slot = Self {
            logical_key,
            physical,
        };
        slot.validate()?;
        Ok(slot)
    }

    pub fn validate(&self) -> Result<(), StorageError> {
        if self.logical_key.is_empty() {
            return Err(StorageError::Configuration(
                "object slot logical key is empty".to_string(),
            ));
        }
        if matches!(&self.physical, PhysicalObjectLocator::Opaque(value) if value.is_empty()) {
            return Err(StorageError::Configuration(
                "object slot provider locator is empty".to_string(),
            ));
        }
        Ok(())
    }

    pub fn logical_key(&self) -> &str {
        &self.logical_key
    }

    pub fn physical(&self) -> &PhysicalObjectLocator {
        &self.physical
    }

    /// Reject this slot when the provider requires the logical key to be the
    /// physical object locator.
    pub(crate) fn require_logical_key_for(&self, provider: &str) -> Result<(), StorageError> {
        self.validate()?;
        if self.physical != PhysicalObjectLocator::LogicalKey {
            return Err(StorageError::Configuration(format!(
                "{provider} slot for {} must use its logical key",
                self.logical_key
            )));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub(crate) struct VerifiedObject<T> {
    pub value: T,
    pub bytes: Vec<u8>,
    pub semantic_hash: ObjectHash,
    pub object: ExactObjectRef,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreObjectError {
    #[error("{0}")]
    Storage(
        #[from]
        #[source]
        StorageError,
    ),
    #[error("Store object {key:?} is invalid for semantic object {semantic_prefix:?}: {source}")]
    InvalidObject {
        semantic_prefix: String,
        key: String,
        #[source]
        source: Box<StoreProtocolError>,
    },
}

/// Decode the JSON body of one protocol object. Bytes that do not parse as `T`
/// are malformed for the slot they were read from.
pub(crate) fn decode_protocol_object<T: serde::de::DeserializeOwned>(
    bytes: &[u8],
) -> Result<T, StoreProtocolError> {
    serde_json::from_slice(bytes).map_err(|error| StoreProtocolError::Malformed(error.to_string()))
}

/// Reject an object that names a different Store root than the one it was read
/// under.
pub(crate) fn verify_store_root(
    expected: ObjectHash,
    actual: ObjectHash,
) -> Result<(), StoreProtocolError> {
    if actual != expected {
        return Err(StoreProtocolError::StoreRootMismatch { expected, actual });
    }
    Ok(())
}

pub(crate) fn verify_membership_head_reference(
    head: &AuthorHead,
    expected_coord: &crate::protocol::membership::MembershipCoord,
    expected_head_hash: ObjectHash,
    registration: &StoreDeviceRegistration,
) -> Result<(), StoreProtocolError> {
    if head.entry_coord() != *expected_coord
        || head.head_hash() != expected_head_hash
        || registration.author_pubkey != expected_coord.author_pubkey
        || !head.verify(registration)
    {
        return Err(StoreProtocolError::Malformed(
            "exact membership head differs from its reference or certified author".to_string(),
        ));
    }
    Ok(())
}

/// One loaded protocol object: its typed value, exact bytes, reference, and
/// prepared upload representation.
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

#[cfg(test)]
mod tests;
