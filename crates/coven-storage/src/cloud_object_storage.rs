//! The storage service trait: exact-slot providers expose protocol-object and
//! blob operations over the addressing and protection model in
//! [`coven_protocol::objects`].

use async_trait::async_trait;

use std::path::Path;

use coven_protocol::objects::{
    BlobSpoolProtection, BlobSpoolWrite, BlobWriteAuthority, ExactObjectRef, ObjectSlot,
    PreparedExactObject, ProtocolObjectContext, ResolvedProviderBinding, StorageError,
};

pub const BLOB_TOMBSTONE_PREFIX: &str = "blob_tombstones/";

pub fn blob_tombstone_object_id(
    stored: &coven_protocol::blob::locator::StoredBlobRef,
) -> coven_protocol::store_commit::ObjectHash {
    coven_protocol::remote_object::remote_object_id(stored.object())
}

pub fn blob_tombstone_key(
    stored: &coven_protocol::blob::locator::StoredBlobRef,
    suffix: &str,
) -> String {
    format!(
        "{BLOB_TOMBSTONE_PREFIX}{}{suffix}",
        blob_tombstone_object_id(stored)
    )
}

#[derive(Clone, Debug)]
pub enum ListedBlobTombstone {
    Opened {
        object_id: coven_protocol::store_commit::ObjectHash,
        plaintext: Vec<u8>,
    },
    InvalidKey {
        provider_key: String,
    },
    InvalidBody {
        provider_key: String,
        source: std::sync::Arc<coven_keys::encryption::EncryptionError>,
    },
}

#[async_trait]
pub trait CloudSyncObjectStorage: Send + Sync {
    /// Return the cloud home's fixed blob path representation.
    fn blob_path_scheme(&self) -> crate::BlobPathScheme;

    /// Verify that the retained provider session is reachable and usable.
    async fn probe_provider(&self) -> Result<(), StorageError>;

    /// The running total of provider operations issued through this storage's
    /// home, so a run's stage timings can report each stage's count beside its
    /// wall time. `None` when nothing is counting — see
    /// [`CloudHome::provider_requests`](crate::cloud::CloudHome::provider_requests).
    fn provider_requests(
        &self,
    ) -> Option<std::sync::Arc<dyn coven_foundation::stage_timing::ProviderRequests>>;

    /// Apply and read back one provider membership-access state.
    async fn set_member_access(
        &self,
        state: crate::cloud::CloudAccessState,
    ) -> Result<crate::cloud::CloudAccessOutcome, StorageError>;

    async fn read_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<Option<Vec<u8>>, StorageError>;

    async fn write_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
        plaintext: Vec<u8>,
    ) -> Result<(), StorageError>;

    async fn list_blob_tombstones(&self) -> Result<Vec<ListedBlobTombstone>, StorageError>;

    async fn blob_tombstone_exists(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<bool, StorageError>;

    async fn delete_blob_tombstone(
        &self,
        stored: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError>;

    #[cfg(any(test, feature = "test-utils"))]
    async fn read_provider_bytes_for_test(&self, key: &str) -> Result<Vec<u8>, StorageError>;

    #[cfg(any(test, feature = "test-utils"))]
    async fn write_provider_bytes_for_test(
        &self,
        key: &str,
        bytes: Vec<u8>,
    ) -> Result<(), StorageError>;

    #[cfg(any(test, feature = "test-utils"))]
    async fn list_provider_keys_for_test(&self, prefix: &str) -> Result<Vec<String>, StorageError>;

    #[cfg(any(test, feature = "test-utils"))]
    async fn provider_key_exists_for_test(&self, key: &str) -> Result<bool, StorageError>;

    async fn reserve_cross_principal_response_slot(
        &self,
        probe_id: coven_protocol::provider::ProviderProbeId,
    ) -> Result<ObjectSlot, coven_protocol::provider::ProviderProbeError>;

    async fn prepare_cross_principal_challenge(
        &self,
        publication_journal: &dyn coven_protocol::provider::DeviceJoinChallengePublicationJournal,
        probe_id: coven_protocol::provider::ProviderProbeId,
        store: &coven_protocol::StoreProviderBinding,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeChallenge,
        coven_protocol::provider::ProviderProbeError,
    >;

    async fn settle_cross_principal_challenge(
        &self,
        publication_journal: &dyn coven_protocol::provider::DeviceJoinChallengePublicationJournal,
        authorization: &coven_protocol::provider::DeviceJoinChallengePublicationAuthorization,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalChallengeContext,
        store: &coven_protocol::StoreProviderBinding,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeChallenge,
        coven_protocol::provider::ProviderProbeError,
    >;

    async fn create_cross_principal_response(
        &self,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        context: &coven_protocol::provider::CrossPrincipalResponseContext,
        store: &coven_protocol::StoreProviderBinding,
        administrator_signing_pubkey: &str,
        peer_signer: &coven_keys::keys::UserKeypair,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeResponse,
        coven_protocol::provider::ProviderProbeError,
    >;

    async fn complete_cross_principal_probe(
        &self,
        journal: &dyn coven_protocol::provider::ProviderProbeJournal,
        challenge: &coven_protocol::provider::CrossPrincipalProbeChallenge,
        response: &coven_protocol::provider::CrossPrincipalProbeResponse,
        context: &coven_protocol::provider::CrossPrincipalResponseContext,
        store: &coven_protocol::StoreProviderBinding,
        administrator_signer: &dyn coven_keys::keys::DeviceSigningAuthority,
        peer_signing_pubkey: &str,
    ) -> Result<
        coven_protocol::provider::CrossPrincipalProbeReceipt,
        coven_protocol::provider::ProviderProbeError,
    >;

    async fn probe_exact_slots(
        &self,
        journal: &dyn coven_protocol::provider::ProviderProbeJournal,
        probe_id: coven_protocol::provider::ProviderProbeId,
        binding: &ResolvedProviderBinding,
    ) -> Result<
        coven_protocol::provider::ExactSlotProbeReceipt,
        coven_protocol::provider::ProviderProbeError,
    >;

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

    /// The value identity of the key used for new Store blobs. Browsable homes
    /// have no key fingerprint; the key-bearing service remains inside storage.
    fn store_blob_key_fingerprint(
        &self,
    ) -> Result<Option<coven_keys::encryption::KeyFingerprint>, StorageError>;

    /// Create the signed root's proof that this storage owns the Store key.
    fn create_store_key_confirmation(
        &self,
        creation_id: coven_protocol::store_commit::StoreCreationId,
    ) -> Result<coven_protocol::store_commit::StoreKeyConfirmation, StorageError>;

    /// Verify the signed root's Store-key proof with this storage's retained key.
    fn verify_store_key_confirmation(
        &self,
        creation_id: coven_protocol::store_commit::StoreCreationId,
        confirmation: &coven_protocol::store_commit::StoreKeyConfirmation,
    ) -> Result<(), StorageError>;

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

    /// Open a locally retained prepared object without fetching it from the
    /// provider. This verifies spool bytes at publication boundaries while the
    /// provider adapter separately proves the stored representation.
    async fn open_prepared_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        prepared: &PreparedExactObject,
        semantic_prefix: &str,
    ) -> Result<Vec<u8>, StorageError>;

    /// Verify that a locally retained prepared object opens to the canonical
    /// bytes its durable journal records.
    async fn verify_prepared_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        prepared: &PreparedExactObject,
        semantic_prefix: &str,
        expected: &[u8],
    ) -> Result<(), StorageError> {
        if self
            .open_prepared_protocol_object(context, prepared, semantic_prefix)
            .await?
            == expected
        {
            return Ok(());
        }
        Err(StorageError::PreparedObjectMismatch(
            prepared.reference().slot().logical_key().to_string(),
        ))
    }

    /// Verify the retained semantic bytes before creating their exact stored
    /// representation at the provider.
    async fn create_verified_protocol_object(
        &self,
        context: &ProtocolObjectContext,
        prepared: &PreparedExactObject,
        semantic_prefix: &str,
        expected: &[u8],
    ) -> Result<(), StorageError> {
        self.verify_prepared_protocol_object(context, prepared, semantic_prefix, expected)
            .await?;
        self.create_protocol_object(prepared).await
    }

    /// Create the prepared bytes at their reserved slot, settling ambiguous
    /// responses through the provider's configured exact-upload verification.
    /// A successful return guarantees that every client can immediately read
    /// the object through its exact slot or reference. Prefix listings may lag;
    /// an exact read may not report the created object as absent.
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

    /// Read and open one exact Store protocol object while reporting cumulative
    /// provider bytes as each response buffer arrives.
    async fn read_protocol_object_with_progress(
        &self,
        context: &ProtocolObjectContext,
        object: &ExactObjectRef,
        semantic_prefix: &str,
        progress: crate::cloud::DownloadProgress,
    ) -> Result<Vec<u8>, StorageError>;

    /// Name every slot under `listing_prefix` that holds a `context` object.
    ///
    /// A listing is not evidence. It decides which bytes are worth fetching and
    /// nothing else: each slot it yields is read and verified exactly as one
    /// named by a signed reference would be, so a listing that omits, invents,
    /// or reorders entries changes how many round trips a reader makes and
    /// never what it believes. Slots whose logical key is not one this domain
    /// writes are dropped here rather than fetched.
    ///
    /// This exists because a chain of slots that each name the next costs one
    /// round trip per link to walk, while the slots themselves are named by
    /// coordinate and so share a prefix a provider can enumerate at once.
    async fn list_protocol_slots(
        &self,
        context: &ProtocolObjectContext,
        listing_prefix: &str,
    ) -> Result<Vec<ObjectSlot>, StorageError>;

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
    /// representation through the caller-owned spool stage. The stage's owner
    /// determines the file and directory durability barriers.
    async fn seal_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        protection: BlobSpoolProtection,
        plaintext_file: &Path,
        spool: coven_foundation::local_file::AtomicStagedFile,
        progress: crate::cloud::PreparationProgress,
    ) -> Result<BlobSpoolWrite, StorageError>;

    /// Seal a Store-audience blob without handing the Store key to the caller.
    async fn seal_store_blob_to_spool(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &BlobWriteAuthority<'_>,
        plaintext_file: &Path,
        spool: coven_foundation::local_file::AtomicStagedFile,
        progress: crate::cloud::PreparationProgress,
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
        control: &crate::cloud::UploadControl,
    ) -> Result<(), StorageError>;

    /// Read one exact stored blob body and verify its signed size/hash reference.
    async fn verify_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError>;

    /// Download and exact-verify the stored object into the caller-owned stage,
    /// open it under the audience-owned protection, and return the unpublished
    /// plaintext only after its locator size and hash have also been verified.
    async fn stage_verified_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        protection: BlobSpoolProtection,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: crate::cloud::DownloadProgress,
    ) -> Result<coven_foundation::local_file::AtomicStagedFile, StorageError>;

    /// Open and verify a Store-audience blob without exposing the Store key.
    async fn stage_verified_store_blob_plaintext(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
        stage: coven_foundation::local_file::AtomicStagedFile,
        progress: crate::cloud::DownloadProgress,
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
    ) -> Result<crate::BlobRangeReader, StorageError>;

    /// Open Store-audience ranges without exposing the Store key.
    async fn open_store_blob_range_reader(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<crate::BlobRangeReader, StorageError>;

    /// Delete one exact stored blob body.
    async fn delete_blob_object(
        &self,
        blob: &coven_protocol::blob::locator::StoredBlobRef,
    ) -> Result<(), StorageError>;
}
