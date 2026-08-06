//! The storage service trait: exact-slot providers expose protocol-object and
//! blob operations over the addressing and protection model in
//! [`coven_protocol::objects`].

use async_trait::async_trait;

use std::path::Path;

use coven_protocol::objects::{
    BlobSpoolProtection, BlobSpoolWrite, BlobWriteAuthority, ExactObjectRef, ObjectSlot,
    PreparedExactObject, ProtocolObjectContext, ResolvedProviderBinding, StorageError,
};

#[async_trait]
pub(crate) trait SyncStorage: Send + Sync {
    /// Return the cloud home's fixed blob path representation.
    fn blob_path_scheme(&self) -> crate::storage::BlobPathScheme;

    /// Verify that the retained provider session is reachable and usable.
    async fn probe_provider(&self) -> Result<(), StorageError>;

    /// Apply and read back one provider membership-access state.
    async fn set_member_access(
        &self,
        state: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, StorageError>;

    async fn read_provider_object(&self, key: &str) -> Result<Vec<u8>, StorageError>;

    async fn write_provider_object(
        &self,
        key: &str,
        stored_bytes: Vec<u8>,
    ) -> Result<(), StorageError>;

    async fn list_provider_objects(&self, prefix: &str) -> Result<Vec<String>, StorageError>;

    async fn provider_object_exists(&self, key: &str) -> Result<bool, StorageError>;

    async fn delete_provider_object(&self, key: &str) -> Result<(), StorageError>;

    /// The two independent provider clients this session retains for
    /// cross-principal probing. The probe protocol is a surface of its own: an
    /// adapter hands it over rather than restating each of its steps.
    fn provider_probes(&self) -> &crate::storage::provider_probe::ProviderProbeStorage;

    /// Observe the exact object identity currently occupying `slot` without
    /// opening its protocol bytes.
    async fn observe_exact_slot(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<ExactObjectRef>, StorageError>;

    /// Delete whatever occupies `slot` and prove the exact slot is absent.
    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &ObjectSlot,
    ) -> Result<(), StorageError>;

    /// Return the cloud home's fixed Store blob opening protection. Circle blobs
    /// use their exact activated Circle key instead.
    fn store_blob_protection(&self) -> Result<BlobSpoolProtection, StorageError>;

    /// Resolve the provider corpus and authenticated principal used by this
    /// adapter. Registrations bind the principal before allocating descendants.
    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, StorageError>;

    /// Reserve the exact provider slot for a protocol object.
    async fn allocate_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<ObjectSlot, StorageError>;

    /// Seal canonical protocol bytes once and bind their exact stored size/hash.
    fn prepare_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        slot: ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<PreparedExactObject, StorageError>;

    /// Create the prepared bytes at their reserved slot, settling lost responses
    /// by exact readback and refusing different bytes at an occupied slot.
    async fn create_protocol_object(
        &self,
        prepared: &PreparedExactObject,
    ) -> Result<(), StorageError>;

    /// Read and open one exact Store protocol object using the signed
    /// semantic prefix as encryption AAD.
    async fn read_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError>;

    /// Read one predecessor-reserved successor slot and return both its opened
    /// bytes and the completed exact reference derived from the stored bytes.
    async fn read_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, ExactObjectRef), StorageError>;

    /// Read one predecessor-reserved successor slot while retaining its exact
    /// stored representation for a durable retry journal.
    async fn read_prepared_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, PreparedExactObject), StorageError>;

    /// Delete one exact Store protocol object and verify absence.
    async fn delete_protocol_object(&self, object: &ExactObjectRef) -> Result<(), StorageError>;

    /// Reserve the exact provider slot for a stored blob body.
    async fn allocate_blob_slot(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
    ) -> Result<ObjectSlot, StorageError>;

    /// Verify one plaintext source against its locator and write the exact stored
    /// representation to an atomically committed, directory-synced spool file.
    async fn seal_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        protection: BlobSpoolProtection,
        plaintext_file: &Path,
        spool_file: &Path,
    ) -> Result<BlobSpoolWrite, StorageError>;

    /// Derive an exact reference from an immutable stored blob file.
    async fn prepare_blob_object(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        slot: ObjectSlot,
        stored_file: &Path,
    ) -> Result<coven_protocol::blob::locator::StoredBlobRef, StorageError>;

    /// Create the exact stored blob body from its immutable local file.
    async fn create_blob_object_from_file(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        authority: &BlobWriteAuthority<'_>,
        stored_file: &Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), StorageError>;

    /// Read one exact stored blob body and verify its signed size/hash reference.
    async fn verify_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError>;

    /// Download and exact-verify the stored object, open it under the
    /// audience-owned protection, and return an unpublished plaintext file only
    /// after its locator size and hash have also been verified.
    async fn stage_verified_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
        dest: &Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, StorageError>;

    /// Open a reader that serves plaintext ranges of a stored blob by fetching
    /// only the sealed chunks covering each range. The ranged counterpart of
    /// [`Self::stage_verified_blob_plaintext`], which materializes the whole
    /// blob; a host seeking around a large blob opens this instead so a range
    /// costs its own bytes rather than the object's.
    async fn open_blob_range_reader(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
    ) -> Result<crate::storage::BlobRangeReader, StorageError>;

    /// Delete one exact stored blob body.
    async fn delete_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError>;
}

#[async_trait]
impl<T> SyncStorage for std::sync::Arc<T>
where
    T: SyncStorage + ?Sized,
{
    fn blob_path_scheme(&self) -> crate::storage::BlobPathScheme {
        (**self).blob_path_scheme()
    }

    async fn probe_provider(&self) -> Result<(), StorageError> {
        (**self).probe_provider().await
    }

    async fn set_member_access(
        &self,
        state: crate::storage::cloud::CloudAccessState,
    ) -> Result<crate::storage::cloud::CloudAccessOutcome, StorageError> {
        (**self).set_member_access(state).await
    }

    async fn read_provider_object(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        (**self).read_provider_object(key).await
    }

    async fn write_provider_object(
        &self,
        key: &str,
        stored_bytes: Vec<u8>,
    ) -> Result<(), StorageError> {
        (**self).write_provider_object(key, stored_bytes).await
    }

    async fn list_provider_objects(&self, prefix: &str) -> Result<Vec<String>, StorageError> {
        (**self).list_provider_objects(prefix).await
    }

    async fn provider_object_exists(&self, key: &str) -> Result<bool, StorageError> {
        (**self).provider_object_exists(key).await
    }

    async fn delete_provider_object(&self, key: &str) -> Result<(), StorageError> {
        (**self).delete_provider_object(key).await
    }

    fn provider_probes(&self) -> &crate::storage::provider_probe::ProviderProbeStorage {
        (**self).provider_probes()
    }

    async fn observe_exact_slot(
        &self,
        slot: &ObjectSlot,
    ) -> Result<Option<ExactObjectRef>, StorageError> {
        (**self).observe_exact_slot(slot).await
    }

    async fn delete_exact_slot_and_verify_absent(
        &self,
        slot: &ObjectSlot,
    ) -> Result<(), StorageError> {
        (**self).delete_exact_slot_and_verify_absent(slot).await
    }

    fn store_blob_protection(&self) -> Result<BlobSpoolProtection, StorageError> {
        (**self).store_blob_protection()
    }

    async fn provider_binding(&self) -> Result<ResolvedProviderBinding, StorageError> {
        (**self).provider_binding().await
    }

    async fn allocate_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
    ) -> Result<ObjectSlot, StorageError> {
        (**self)
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
    }

    fn prepare_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        slot: ObjectSlot,
        semantic_prefix: &str,
        data: Vec<u8>,
    ) -> Result<PreparedExactObject, StorageError> {
        (**self).prepare_protocol_object(context, slot, semantic_prefix, data)
    }

    async fn create_protocol_object(
        &self,
        prepared: &PreparedExactObject,
    ) -> Result<(), StorageError> {
        (**self).create_protocol_object(prepared).await
    }

    async fn read_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError> {
        (**self)
            .read_protocol_object(context, object, semantic_prefix)
            .await
    }

    async fn read_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, ExactObjectRef), StorageError> {
        (**self)
            .read_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn read_prepared_protocol_slot(
        &self,
        context: &ProtocolObjectContext,
        slot: &ObjectSlot,
        semantic_prefix: &str,
    ) -> Result<(Vec<u8>, PreparedExactObject), StorageError> {
        (**self)
            .read_prepared_protocol_slot(context, slot, semantic_prefix)
            .await
    }

    async fn delete_protocol_object(&self, object: &ExactObjectRef) -> Result<(), StorageError> {
        (**self).delete_protocol_object(object).await
    }

    async fn allocate_blob_slot(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
    ) -> Result<ObjectSlot, StorageError> {
        (**self).allocate_blob_slot(locator, authority).await
    }

    async fn seal_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        protection: BlobSpoolProtection,
        plaintext_file: &Path,
        spool_file: &Path,
    ) -> Result<BlobSpoolWrite, StorageError> {
        (**self)
            .seal_blob_to_spool(locator, authority, protection, plaintext_file, spool_file)
            .await
    }

    async fn prepare_blob_object(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        slot: ObjectSlot,
        stored_file: &Path,
    ) -> Result<coven_protocol::blob::locator::StoredBlobRef, StorageError> {
        (**self)
            .prepare_blob_object(locator, authority, slot, stored_file)
            .await
    }

    async fn create_blob_object_from_file(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        authority: &BlobWriteAuthority<'_>,
        stored_file: &Path,
        progress: &crate::storage::cloud::UploadProgress<'_>,
    ) -> Result<(), StorageError> {
        (**self)
            .create_blob_object_from_file(blob, authority, stored_file, progress)
            .await
    }

    async fn verify_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        (**self).verify_blob_object(blob).await
    }

    async fn stage_verified_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
        dest: &Path,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, StorageError> {
        (**self)
            .stage_verified_blob_plaintext(blob, protection, dest)
            .await
    }

    async fn open_blob_range_reader(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
    ) -> Result<crate::storage::BlobRangeReader, StorageError> {
        (**self).open_blob_range_reader(blob, protection).await
    }

    async fn delete_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError> {
        (**self).delete_blob_object(blob).await
    }
}
