//! Exact-reference access to Store protocol objects.

use super::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
};
use crate::protocol::membership::{AuthorHead, MembershipEntry, MembershipEntryRef};
use crate::protocol::store_commit::{
    membership_entry_semantic_prefix, ObjectHash, StoreDeviceRegistration, StoreProtocolError,
};

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

pub(crate) async fn run_blocking_object_verification<T>(
    semantic_prefix: &str,
    object: &ExactObjectRef,
    verify: Box<dyn FnOnce() -> Result<T, StoreProtocolError> + Send>,
) -> Result<T, StoreObjectError>
where
    T: Send + 'static,
{
    crate::sync::blocking::run(verify)
        .await
        .map_err(|error| {
            StoreObjectError::Storage(StorageError::Storage(format!(
                "Store object verification task failed: {error}"
            )))
        })?
        .map_err(|source| StoreObjectError::InvalidObject {
            semantic_prefix: semantic_prefix.to_string(),
            key: object.slot().logical_key().to_string(),
            source: Box::new(source),
        })
}

pub(crate) async fn prepare_membership_entry(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    entry: &MembershipEntry,
) -> Result<(PreparedExactObject, MembershipEntryRef), StoreObjectError> {
    let coord = entry.coord();
    let semantic_prefix = membership_entry_semantic_prefix(
        &coord.author_pubkey,
        &coord.author_owner_grant,
        coord.stream_id,
        coord.seq,
        coord.entry_hash,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipEntry,
    );
    let slot = storage
        .allocate_protocol_slot(&context, &semantic_prefix, ".json")
        .await?;
    let prepared = storage.prepare_protocol_object(
        &context,
        slot,
        &semantic_prefix,
        serde_json::to_vec(entry).expect("membership entry serialization cannot fail"),
    )?;
    let reference = MembershipEntryRef {
        coord,
        object: prepared.reference().clone(),
    };
    Ok((prepared, reference))
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
