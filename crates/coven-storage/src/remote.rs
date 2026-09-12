//! `CloudSyncObjectStorage` implementation backed by any `CloudHome`.
//!
//! Resolves protocol slots and blob locators to exact provider objects. Each
//! protocol domain declares its protection: Store data uses the home's
//! [`CloudCipher`], Circle data uses the supplied Circle cipher, and signed
//! readable or recipient-sealed records pass through unchanged. Blob locators
//! declare their audience, scope, and readable or opaque path. Exact object
//! references retain the resulting provider address and stored-byte identity.

use async_trait::async_trait;
use std::path::Path;
use std::sync::{Arc, RwLock};

use super::provider_probe::ProviderProbeStorage;
use super::CloudSyncObjectStorage;
use crate::cloud::{BlobBody, CloudHomeError, ExactCloudHome};
use coven_keys::encryption::{
    EncryptionError, EncryptionService, KeyTag, NoncePolicy, SealedBlobHeader,
    SEALED_BLOB_HEADER_LEN,
};
use coven_keys::keys::UserKeypair;
use coven_protocol::objects::ObjectSlot;
#[cfg(test)]
use coven_protocol::objects::ProtocolObjectDomain;
use coven_protocol::objects::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectProtection,
    ResolvedProviderBinding, RotationGate, RotationPending, StorageError,
};
use coven_protocol::store_commit::ObjectHash;

mod blob_io;
mod cipher;
mod rotation;
mod storage_impl;

#[cfg(any(test, feature = "test-utils"))]
pub use blob_io::open_sealed_blob;
pub use blob_io::BlobChunking;
pub use blob_io::{BlobPathScheme, BlobRangeReader};
#[cfg(any(test, feature = "test-utils"))]
pub use cipher::CloudKeyringFacts;
pub use cipher::{
    cloud_aad_context, AdoptedCloudKeyRotation, CloudKeyringMerge, CloudSyncCipherStateAccess,
};
pub use rotation::{CloudSyncRotationStateAccess, PendingRotation, RotationStateError};

/// Protection for payloads assigned to this cipher. `Encrypted` seals under
/// its keyring; `Plaintext` preserves the bytes. Protocol domains and blob
/// locators determine which cipher applies.
#[derive(Clone)]
pub enum CloudCipher {
    Encrypted(EncryptionService),
    Plaintext,
}

/// `CloudSyncObjectStorage` that delegates raw I/O to a `CloudHome` and handles the path
/// layout and the at-rest protection (its [`CloudCipher`]).
pub struct CloudSyncConnection {
    /// `Arc` because ranged readers retain this provider across awaits.
    home: Arc<dyn ExactCloudHome>,
    provider_probes: ProviderProbeStorage,
    cipher: Arc<RwLock<CloudCipher>>,
    /// Whether a committed rotation is outstanding — see [`PendingRotation`].
    /// Shared the same way `cipher` is, so a member removal or a refresh cycle
    /// that discovers a rotation this device can't adopt blocks every seal path,
    /// not just the one that discovered it.
    pending_rotation: Arc<PendingRotation>,
    /// How blob objects are keyed. Unlike the cipher, the scheme does not rotate
    /// over a home's life, so it is a plain field with no lock.
    blob_paths: BlobPathScheme,
    /// How this installation chunks blobs and how wide its range requests are.
    blob_chunking: BlobChunking,
    store_id: String,
    /// The Store identity used to verify that blob append authority names this
    /// connection's author in its device registration.
    keypair: UserKeypair,
}

impl CloudSyncConnection {
    pub fn new(
        home: Arc<dyn ExactCloudHome>,
        cipher: CloudCipher,
        blob_paths: BlobPathScheme,
        store_id: impl Into<String>,
        keypair: UserKeypair,
    ) -> Self {
        let provider_probes = ProviderProbeStorage::new(home.clone());
        CloudSyncConnection {
            home,
            provider_probes,
            cipher: Arc::new(RwLock::new(cipher)),
            pending_rotation: Arc::new(PendingRotation::none()),
            blob_paths,
            blob_chunking: BlobChunking::DEFAULT,
            store_id: store_id.into(),
            keypair,
        }
    }

    /// The running total of provider operations issued through this
    /// connection's home — by this connection and by any other over the same
    /// home, which on a device join is how the plaintext bootstrap reads and
    /// the encrypted reads after them land in one total.
    pub fn provider_requests(
        &self,
    ) -> Option<Arc<dyn coven_foundation::stage_timing::ProviderRequests>> {
        self.home.provider_requests()
    }

    /// Seal and read blobs with `chunking` instead of [`BlobChunking::DEFAULT`].
    /// The chunk size applies to blobs this storage seals from now on; already
    /// stored blobs keep the size their own headers record, so installations
    /// with different settings read each other's blobs unchanged.
    pub fn with_blob_chunking(mut self, chunking: BlobChunking) -> Self {
        self.blob_chunking = chunking;
        self
    }

    pub fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_paths
    }

    pub fn store_id(&self) -> &str {
        &self.store_id
    }

    pub async fn probe(&self) -> Result<(), CloudHomeError> {
        self.home.probe().await
    }

    fn validate_blob_locator_home(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
    ) -> Result<(), StorageError> {
        let valid = matches!(
            (locator, self.blob_paths, self.cipher.is_plaintext()),
            (
                coven_protocol::blob::locator::BlobLocator::Opaque { .. },
                BlobPathScheme::Hashed,
                false
            ) | (
                coven_protocol::blob::locator::BlobLocator::Browsable { .. },
                BlobPathScheme::Plain,
                true
            )
        );
        if !valid {
            return Err(StorageError::InvalidContent(
                "blob locator protection does not match the cloud home's fixed storage mode"
                    .to_string(),
            ));
        }
        Ok(())
    }

    async fn validate_blob_append_authority(
        &self,
        locator: &coven_protocol::blob::locator::BlobLocator,
        authority: &coven_protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<(), StorageError> {
        authority
            .reference
            .verify_registration(authority.registration)?;
        if locator.uploader() != authority.reference {
            return Err(StorageError::InvalidContent(format!(
                "blob locator uploader {:?} differs from its exact write authority",
                locator.uploader()
            )));
        }
        if authority.registration.author_pubkey != hex::encode(self.keypair.public_key()) {
            return Err(StorageError::InvalidContent(
                "blob write authority is not this device's identity key".to_string(),
            ));
        }
        let live = self
            .home
            .provider_binding()
            .await
            .map_err(StorageError::from)?;
        if live.device != authority.registration.provider {
            return Err(StorageError::InvalidContent(
                "blob write authority differs from the authenticated provider principal"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub fn uses_identity(&self, identity: &UserKeypair) -> bool {
        self.keypair.public_key() == identity.public_key()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn connection_for_test_identity(&self, identity: UserKeypair) -> Self {
        Self::new(
            self.home.clone(),
            self.cipher.read().unwrap().clone(),
            self.blob_paths,
            self.store_id.clone(),
            identity,
        )
        .with_blob_chunking(self.blob_chunking)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn connection_for_test_identity_and_home(
        &self,
        identity: UserKeypair,
        home: Arc<dyn ExactCloudHome>,
    ) -> Self {
        Self::new(
            home,
            self.cipher.read().unwrap().clone(),
            self.blob_paths,
            self.store_id.clone(),
            identity,
        )
        .with_blob_chunking(self.blob_chunking)
    }

    pub fn is_plaintext(&self) -> bool {
        self.cipher.read().unwrap().is_plaintext()
    }

    fn cipher_suffix(&self) -> &'static str {
        self.cipher.read().unwrap().suffix()
    }

    fn open_stored_data(
        &self,
        stored: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        self.cipher.read().unwrap().open(stored, aad_context)
    }

    fn seal_stored_data(
        &self,
        plaintext: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        let cipher = self.cipher.read().unwrap();
        self.pending_rotation.check(cipher.current_generation())?;
        Ok(cipher.seal(plaintext, aad_context))
    }

    fn seal_protocol_data(
        &self,
        context: &ProtocolObjectContext,
        plaintext: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, StorageError> {
        match context.protection() {
            ProtocolObjectProtection::StoreEncrypted => {
                self.seal_stored_data(plaintext, aad_context)
            }
            ProtocolObjectProtection::SignedPlaintext
            | ProtocolObjectProtection::RecipientSealed => {
                Ok(CloudCipher::Plaintext.seal(plaintext, aad_context))
            }
            ProtocolObjectProtection::Circle(encryption) => {
                Ok(CloudCipher::Encrypted(encryption.clone()).seal(plaintext, aad_context))
            }
        }
    }

    async fn verify_and_open_protocol_data(
        &self,
        operation: &'static str,
        context: &ProtocolObjectContext,
        object: ExactObjectRef,
        stored: Vec<u8>,
        aad_context: Vec<u8>,
    ) -> Result<Vec<u8>, StorageError> {
        let cipher = match context.protection() {
            ProtocolObjectProtection::StoreEncrypted => self.cipher.read().unwrap().clone(),
            ProtocolObjectProtection::SignedPlaintext => CloudCipher::Plaintext,
            ProtocolObjectProtection::Circle(encryption) => {
                CloudCipher::Encrypted(encryption.clone())
            }
            ProtocolObjectProtection::RecipientSealed => CloudCipher::Plaintext,
        };
        run_storage_cpu(
            operation,
            Box::new(move || {
                object.verify(&stored)?;
                cipher
                    .open(stored, &aad_context)
                    .map_err(|source| StorageError::Decryption {
                        context: format!("protocol object {}", object.slot().logical_key()),
                        source,
                    })
            }),
        )
        .await
    }

    async fn identify_and_open_protocol_data(
        &self,
        context: &ProtocolObjectContext,
        slot: ObjectSlot,
        stored: Vec<u8>,
        aad_context: Vec<u8>,
    ) -> Result<(Vec<u8>, PreparedExactObject), StorageError> {
        let cipher = match context.protection() {
            ProtocolObjectProtection::StoreEncrypted => self.cipher.read().unwrap().clone(),
            ProtocolObjectProtection::SignedPlaintext => CloudCipher::Plaintext,
            ProtocolObjectProtection::Circle(encryption) => {
                CloudCipher::Encrypted(encryption.clone())
            }
            ProtocolObjectProtection::RecipientSealed => CloudCipher::Plaintext,
        };
        run_storage_cpu(
            "identify and open protocol slot",
            Box::new(move || {
                let object = ExactObjectRef::new(
                    slot.clone(),
                    stored.len() as u64,
                    ObjectHash::digest(&stored),
                );
                let prepared = PreparedExactObject::new(object, stored.clone())?;
                let opened = cipher.open(stored, &aad_context).map_err(|source| {
                    StorageError::Decryption {
                        context: format!("protocol object {}", slot.logical_key()),
                        source,
                    }
                })?;
                Ok((opened, prepared))
            }),
        )
        .await
    }

    #[cfg(test)]
    async fn blob_write_registration(
        &self,
        label: &str,
    ) -> coven_protocol::store_commit::ReferencedStoreDeviceRegistration {
        use coven_protocol::store_commit::{
            DeviceStreamAnchor, StoreCreationId, StoreDeviceRegistration,
            StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreRootRef,
        };

        let root_bytes = format!("{label} Store root").into_bytes();
        let root = StoreRootRef {
            store_root_id: ObjectHash::digest(format!("{label} root id").as_bytes()),
            store_root_hash: ObjectHash::digest(&root_bytes),
            object: ExactObjectRef::new(
                ObjectSlot::logical(format!("store-v1/store-protocol-root/{label}.json")).unwrap(),
                root_bytes.len() as u64,
                ObjectHash::digest(&root_bytes),
            ),
        };
        let anchor_slot = |stream: &str| {
            ObjectSlot::logical(format!(
                "store-v1/test-device-streams/{label}/{stream}.json"
            ))
            .unwrap()
        };
        let provider = CloudSyncObjectStorage::provider_binding(self)
            .await
            .unwrap()
            .device;
        let registration = StoreDeviceRegistration::signed(
            root,
            StoreDeviceRegistrationOrigin::Founder {
                creation_id: StoreCreationId::from_nonce(label),
            },
            provider,
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: anchor_slot("acknowledgements"),
            },
            &self.keypair,
        )
        .unwrap();
        let bytes = registration.to_bytes();
        let reference = StoreDeviceRegistrationRef::from_registration(
            &registration,
            ExactObjectRef::new(
                ObjectSlot::logical(format!(
                    "store-v1/devices/{}/registration.json",
                    registration.device_id
                ))
                .unwrap(),
                bytes.len() as u64,
                ObjectHash::digest(&bytes),
            ),
        );
        coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
            reference,
            registration,
        )
        .expect("construct test blob write registration")
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn keyring_facts_for_test(&self) -> Option<CloudKeyringFacts> {
        match &*self.cipher.read().unwrap() {
            CloudCipher::Encrypted(encryption) => {
                Some(CloudKeyringFacts::from_encryption(encryption))
            }
            CloudCipher::Plaintext => None,
        }
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn adopt_key_rotation_for_test(
        &self,
        encryption: &EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<String, coven_keys::keys::KeyError> {
        CloudSyncCipherStateAccess::adopt_key_rotation(self, encryption, custody)
            .map(|adopted| adopted.fingerprint().to_string())
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn mark_rotation_committed_for_test(
        &self,
        generation: u64,
    ) -> Result<(), RotationStateError> {
        self.pending_rotation.mark_committed(generation)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn pending_rotation_generation_for_test(&self) -> Option<u64> {
        self.pending_rotation.pending_generation()
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn clear_rotation_gate_for_test(&self) {
        self.pending_rotation.install_durable_gate(None);
    }
}

impl CloudSyncCipherStateAccess for CloudSyncConnection {
    fn is_plaintext(&self) -> bool {
        self.cipher.is_plaintext()
    }

    fn suffix(&self) -> &'static str {
        self.cipher.suffix()
    }

    fn current_generation(&self) -> Option<u64> {
        self.cipher.current_generation()
    }

    fn current_fingerprint(&self) -> Option<String> {
        self.cipher.current_fingerprint()
    }

    fn open(&self, stored: Vec<u8>, aad_context: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.cipher.open(stored, aad_context)
    }

    fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        self.cipher.seal(plaintext, aad_context)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn open_sealed_blob_for_test(
        &self,
        stored: &[u8],
        aad_context: &[u8],
    ) -> Result<
        (coven_keys::encryption::KeyFingerprint, Vec<u8>),
        coven_keys::encryption::EncryptionError,
    > {
        self.cipher.open_sealed_blob_for_test(stored, aad_context)
    }

    fn merged_keyring(
        &self,
        new_encryption: &EncryptionService,
    ) -> Result<CloudKeyringMerge, EncryptionError> {
        self.cipher.merged_keyring(new_encryption)
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<Option<String>, coven_keys::keys::KeyError> {
        self.cipher.merge_key_rotation(new_encryption, custody)
    }
}

impl CloudSyncRotationStateAccess for CloudSyncConnection {
    fn mark_candidate(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError> {
        self.pending_rotation.mark_candidate(generation, mutation)
    }

    fn mark_committed_mutation(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError> {
        self.pending_rotation
            .mark_committed_mutation(generation, mutation)
    }

    fn gate(&self) -> Option<RotationGate> {
        self.pending_rotation.gate()
    }

    fn install_durable_gate(&self, gate: Option<RotationGate>) {
        self.pending_rotation.install_durable_gate(gate);
    }

    fn check(&self, live_generation: Option<u64>) -> Result<(), RotationPending> {
        self.pending_rotation.check(live_generation)
    }
}

async fn run_storage_cpu<T>(
    operation: &'static str,
    work: Box<dyn FnOnce() -> Result<T, StorageError> + Send>,
) -> Result<T, StorageError>
where
    T: Send + 'static,
{
    coven_foundation::blocking::run(work)
        .await
        .map_err(|source| StorageError::Blocking { operation, source })?
}

#[cfg(test)]
mod download_tests;
#[cfg(test)]
mod tests;
