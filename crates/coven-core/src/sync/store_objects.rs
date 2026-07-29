//! Exact-reference access to Store protocol objects.

use super::membership::{
    AuthorHead, MembershipEntry, MembershipEntryRef, MembershipHeadRef,
    StoreMembershipConflictResolution, StoreMembershipConflictResolutionRef,
};
use super::storage::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
};
use super::store_commit::{
    membership_entry_semantic_prefix, membership_resolution_semantic_prefix, ObjectHash,
    StoreDeviceRegistration, StoreProtocolError,
};

#[derive(Clone, Debug)]
pub struct VerifiedObject<T> {
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

/// Open one supplied exact reference and verify its typed semantic contents.
async fn load_exact_object<T>(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    object: &ExactObjectRef,
    semantic_prefix: &str,
    semantic_hash: ObjectHash,
    verify: impl FnOnce(&[u8]) -> Result<T, StoreProtocolError> + Send + 'static,
) -> Result<VerifiedObject<T>, StoreObjectError>
where
    T: Send + 'static,
{
    let bytes = storage
        .read_protocol_object(context, object, semantic_prefix)
        .await?;
    let verify_bytes = bytes.clone();
    let value = Box::pin(run_blocking_object_verification(
        semantic_prefix,
        object,
        Box::new(move || verify(&verify_bytes)),
    ))
    .await?;
    Ok(VerifiedObject {
        value,
        bytes,
        semantic_hash,
        object: object.clone(),
    })
}

pub(crate) async fn run_blocking_object_verification<T>(
    semantic_prefix: &str,
    object: &ExactObjectRef,
    verify: Box<dyn FnOnce() -> Result<T, StoreProtocolError> + Send>,
) -> Result<T, StoreObjectError>
where
    T: Send + 'static,
{
    super::blocking::run(verify)
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

pub async fn prepare_membership_entry(
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

pub async fn load_membership_entry_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &MembershipEntryRef,
) -> Result<VerifiedObject<MembershipEntry>, StoreObjectError> {
    let coord = &reference.coord;
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
    let expected_coord = coord.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        coord.entry_hash,
        move |bytes| {
            let entry: MembershipEntry = decode_protocol_object(bytes)?;
            if entry.coord() != expected_coord
                || !super::membership::verify_membership_entry(&entry)
            {
                return Err(StoreProtocolError::Malformed(
                    "exact membership entry differs from its reference".to_string(),
                ));
            }
            Ok(entry)
        },
    )
    .await
}

pub async fn load_membership_head_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &MembershipHeadRef,
    registration: &StoreDeviceRegistration,
) -> Result<VerifiedObject<AuthorHead>, StoreObjectError> {
    let coord = &reference.coord;
    let semantic_prefix = reference
        .object
        .slot()
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| StoreObjectError::InvalidObject {
            semantic_prefix: reference.object.slot().logical_key().to_string(),
            key: reference.object.slot().logical_key().to_string(),
            source: Box::new(StoreProtocolError::Malformed(
                "membership head slot has no .json suffix".to_string(),
            )),
        })?;
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let expected_coord = coord.clone();
    let expected_head_hash = reference.head_hash;
    let expected_registration = registration.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        semantic_prefix,
        reference.head_hash,
        move |bytes| {
            let head: AuthorHead = decode_protocol_object(bytes)?;
            if head.entry_coord() != expected_coord
                || head.head_hash() != expected_head_hash
                || !head.verify(&expected_registration)
            {
                return Err(StoreProtocolError::Malformed(
                    "exact membership head differs from its reference".to_string(),
                ));
            }
            Ok(head)
        },
    )
    .await
}

pub async fn load_membership_resolution_ref(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    reference: &StoreMembershipConflictResolutionRef,
) -> Result<VerifiedObject<StoreMembershipConflictResolution>, StoreObjectError> {
    let semantic_prefix = membership_resolution_semantic_prefix(
        reference.conflict_hash,
        &reference.resolver_pubkey,
        reference.resolution_hash,
    );
    let context = ProtocolObjectContext::signed_plaintext(
        store_root_hash,
        ProtocolObjectDomain::StoreMembershipResolution,
    );
    let expected = reference.clone();
    load_exact_object(
        storage,
        &context,
        &reference.object,
        &semantic_prefix,
        reference.resolution_hash,
        move |bytes| {
            let resolution: StoreMembershipConflictResolution = decode_protocol_object(bytes)?;
            if resolution.store_root_hash != store_root_hash
                || resolution.conflict_hash != expected.conflict_hash
                || resolution.resolver_pubkey != expected.resolver_pubkey
                || resolution.resolution_hash() != expected.resolution_hash
                || !resolution.verify_signature()
            {
                return Err(StoreProtocolError::Malformed(
                    "exact membership resolution differs from its reference".to_string(),
                ));
            }
            Ok(resolution)
        },
    )
    .await
}
