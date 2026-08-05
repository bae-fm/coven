//! Exact-reference access to Store protocol objects.

use super::SyncStorage;
use crate::protocol::membership::{MembershipEntry, MembershipEntryRef};
use crate::protocol::objects::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    StoreObjectError,
};
use crate::protocol::store_commit::{
    membership_entry_semantic_prefix, ObjectHash, StoreProtocolError,
};

pub(crate) async fn run_blocking_object_verification<T>(
    semantic_prefix: &str,
    object: &ExactObjectRef,
    verify: Box<dyn FnOnce() -> Result<T, StoreProtocolError> + Send>,
) -> Result<T, StoreObjectError>
where
    T: Send + 'static,
{
    crate::blocking::run(verify)
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

/// Publication that is not trusted until it has been proven. Every protocol
/// object this crate writes is read back through its signed semantic prefix and
/// required to open to the bytes that were sealed into it; a differing readback
/// is [`StorageError::ReadbackMismatch`], which callers classify as invalid
/// durable state rather than as a transport failure.
#[async_trait::async_trait]
pub(crate) trait VerifiedObjectWrites: SyncStorage {
    async fn verify_readback(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        prefix: &str,
        expected: &[u8],
    ) -> Result<(), StorageError> {
        if self.read_protocol_object(context, object, prefix).await? == expected {
            return Ok(());
        }
        Err(StorageError::ReadbackMismatch(
            object.slot().logical_key().to_string(),
        ))
    }

    async fn create_and_verify(
        &self,
        context: &ProtocolObjectContext,
        prepared: &PreparedExactObject,
        prefix: &str,
        expected: &[u8],
    ) -> Result<(), StorageError> {
        self.create_protocol_object(prepared).await?;
        self.verify_readback(context, prepared.reference(), prefix, expected)
            .await
    }
}

#[async_trait::async_trait]
impl<T: SyncStorage + ?Sized> VerifiedObjectWrites for T {}

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
