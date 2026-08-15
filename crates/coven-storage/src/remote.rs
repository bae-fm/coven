//! `CloudSyncObjectStorage` implementation backed by any `CloudHome`.
//!
//! Handles the cloud home path layout (where keys, heads, images, etc. live)
//! and how objects are protected at rest. The underlying `CloudHome` only deals
//! in raw bytes and flat keys; this layer applies the [`CloudCipher`] — sealing
//! every object under the store key for an encrypted home, or storing it
//! verbatim for a plaintext one — and drives the object-key suffix off the same
//! choice (`.enc` for encrypted data-plane objects, no suffix for signed
//! control-plane and recipient-sealed objects).

use async_trait::async_trait;
use std::path::Path;
use std::sync::{Arc, RwLock};

use super::provider_probe::ProviderProbeStorage;
use super::CloudSyncObjectStorage;
use crate::cloud::{BlobBody, CloudFileReadError, CloudHomeError, ExactCloudHome};
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

/// How a cloud home protects its objects at rest. An `Encrypted` home seals
/// every object under the store key (the default); a `Plaintext` home stores
/// objects in the clear so the bucket is browsable, and drops the `.enc` suffix.
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
    /// The device's signing identity. The control objects this storage writes
    /// (its head, the min_schema floor) are signed with it so a reader can
    /// attribute and verify them; the at-rest cipher proves confidentiality, not
    /// authorship.
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

    /// The cloud object key for a blob under the home's [`BlobPathScheme`].
    ///
    /// **A cloud object is never rewritten with different bytes, so no two blobs ever
    /// share a key.** `Hashed` gets that from the key itself; `Plain` gets it from the
    /// blob's declared [`BlobReplacement`](coven_protocol::blob::BlobReplacement), which coven
    /// enforces where a blob is derived from its row, in the database layer's blob
    /// declarations — a replaceable blob's readable path must name it, and a write-once
    /// blob's row can never be repointed. Either way, an object's *presence* at a blob's
    /// key is proof of
    /// its *content*, which is what lets the push skip an upload without asking a sealed
    /// object what it holds.
    ///
    /// `Hashed` ignores `cloud_path` and shards by the id under the uploading
    /// device: `{namespace}/{uploader}/{ab}/{cd}/{id}` — the id is right there, and the
    /// `{uploader}` segment aligns the keyspace to the storage-access rule (a member
    /// writes only under its own public key), so `uploader` is required and a missing one
    /// is an error.
    ///
    /// `Plain` uses the consumer's `cloud_path` verbatim: `{namespace}/{cloud_path}`,
    /// keeping the bucket browsable. Plain blob naming carries no uploader segment
    /// and ignores `uploader`; the store still has membership authorization. A
    /// `Plain` home with no `cloud_path` is an error — coven never silently falls
    /// back to the hashed layout, which would scatter readable-path blobs under
    /// unfindable shard keys.
    pub fn blob_key(
        scheme: BlobPathScheme,
        namespace: &str,
        uploader: Option<&str>,
        id: &str,
        cloud_path: Option<&str>,
    ) -> Result<String, StorageError> {
        match scheme {
            BlobPathScheme::Hashed => {
                let uploader = uploader.ok_or_else(|| {
                    StorageError::Parse(format!(
                        "an opaque-home blob requires an uploader for {namespace}/{id}"
                    ))
                })?;
                Ok(coven_foundation::store_dir::StoreDir::uploader_hashed_key(
                    namespace, uploader, id,
                )?)
            }
            BlobPathScheme::Plain => {
                let path = cloud_path.ok_or_else(|| {
                    StorageError::Parse(format!(
                        "unobfuscated blob-path home requires a cloud_path for blob {namespace}/{id}"
                    ))
                })?;
                coven_foundation::store_dir::validate_path_token(namespace)?;
                coven_foundation::store_dir::validate_cloud_path(path)?;
                Ok(format!("{namespace}/{path}"))
            }
        }
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
            DeviceStreamAnchor::StoreAnnouncements {
                first_slot: anchor_slot("announcements"),
            },
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: anchor_slot("acknowledgements"),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: anchor_slot("snapshots"),
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

    fn remove_candidate(
        &self,
        generation: u64,
        mutation: ObjectHash,
    ) -> Result<(), RotationStateError> {
        self.pending_rotation.remove_candidate(generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), RotationStateError> {
        self.pending_rotation
            .replace_candidate_mutation(generation, previous, replacement)
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

async fn read_source_exact(
    source: &mut crate::local_file::PlaintextReader,
    len: usize,
    locator_hash: ObjectHash,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        let chunk = source.next_chunk(len - bytes.len()).await?;
        if chunk.is_empty() {
            return Err(StorageError::InvalidContent(format!(
                "blob {locator_hash} stored body ended after {} of {len} required bytes",
                bytes.len()
            )));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

#[cfg(test)]
mod tests;
