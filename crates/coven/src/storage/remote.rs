//! `SyncStorage` implementation backed by any `CloudHome`.
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
use super::SyncStorage;
use crate::protocol::objects::ObjectSlot;
#[cfg(test)]
use crate::protocol::objects::ProtocolObjectDomain;
use crate::protocol::objects::{
    ExactObjectRef, PreparedExactObject, ProtocolObjectContext, ProtocolObjectProtection,
    ResolvedProviderBinding, RotationGate, RotationPending, StorageError,
};
use crate::protocol::store_commit::ObjectHash;
use crate::storage::cloud::{BlobBody, CloudFileReadError, CloudHome, ExactSlotStorage};
use coven_keys::encryption::{
    EncryptionError, EncryptionService, KeyTag, NoncePolicy, SealedBlobHeader,
    SEALED_BLOB_HEADER_LEN,
};
use coven_keys::keys::UserKeypair;

mod blob_io;
mod cipher;
mod rotation;
mod storage_impl;

#[cfg(test)]
pub(crate) use blob_io::split_sealed_blob;
pub(crate) use blob_io::BlobChunking;
pub(crate) use blob_io::{BlobPathScheme, BlobRangeReader};
pub(crate) use cipher::{cloud_aad_context, CloudCipherAccess, CloudCipherState};
pub(crate) use rotation::{CloudRotationAccess, PendingRotation};

/// How a cloud home protects its objects at rest. An `Encrypted` home seals
/// every object under the store key (the default); a `Plaintext` home stores
/// objects in the clear so the bucket is browsable, and drops the `.enc` suffix.
macro_rules! define_cloud_cipher {
    ($visibility:vis) => {
        #[derive(Clone)]
        $visibility enum CloudCipher {
            Encrypted(EncryptionService),
            Plaintext,
        }
    };
}

#[cfg(any(test, feature = "test-utils"))]
define_cloud_cipher!(pub);
#[cfg(not(any(test, feature = "test-utils")))]
define_cloud_cipher!(pub(crate));

/// `SyncStorage` that delegates raw I/O to a `CloudHome` and handles the path
/// layout and the at-rest protection (its [`CloudCipher`]).
pub(crate) struct CloudSyncStorage {
    home: Arc<dyn CloudHome>,
    /// `Arc` (not `Box`) because a ranged read hands a clone to the
    /// [`BlobRangeReader`] it opens — the reader holds this client for the life
    /// of a stream and reads across awaits, so it is genuinely shared between
    /// this storage and the readers it opens, not owned by one.
    exact: Arc<dyn ExactSlotStorage>,
    provider_probes: ProviderProbeStorage,
    cipher: Arc<CloudCipherState>,
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

impl CloudSyncStorage {
    pub(crate) fn new(
        home: Arc<dyn CloudHome>,
        cipher: CloudCipher,
        blob_paths: BlobPathScheme,
        store_id: impl Into<String>,
        keypair: UserKeypair,
    ) -> Result<Self, crate::storage::cloud::CloudHomeError> {
        let exact = home.clone().exact_slot_storage().ok_or_else(|| {
            crate::storage::cloud::CloudHomeError::Configuration(
                "CloudSyncStorage requires exact-slot storage".to_string(),
            )
        })?;
        let exact_probe_peer = home.clone().exact_slot_storage().ok_or_else(|| {
            crate::storage::cloud::CloudHomeError::Configuration(
                "CloudSyncStorage requires a second exact-slot probe client".to_string(),
            )
        })?;
        let provider_probes = ProviderProbeStorage::new(exact.clone(), exact_probe_peer);
        Ok(CloudSyncStorage {
            home,
            exact,
            provider_probes,
            cipher: Arc::new(CloudCipherState::new(cipher)),
            pending_rotation: Arc::new(PendingRotation::none()),
            blob_paths,
            blob_chunking: BlobChunking::DEFAULT,
            store_id: store_id.into(),
            keypair,
        })
    }

    /// Seal and read blobs with `chunking` instead of [`BlobChunking::DEFAULT`].
    /// The chunk size applies to blobs this storage seals from now on; already
    /// stored blobs keep the size their own headers record, so installations
    /// with different settings read each other's blobs unchanged.
    pub(crate) fn with_blob_chunking(mut self, chunking: BlobChunking) -> Self {
        self.blob_chunking = chunking;
        self
    }

    pub(crate) fn blob_path_scheme(&self) -> BlobPathScheme {
        self.blob_paths
    }

    pub(crate) fn store_id(&self) -> &str {
        &self.store_id
    }

    fn validate_blob_locator_home(
        &self,
        locator: &crate::protocol::blob::locator::BlobLocator,
    ) -> Result<(), StorageError> {
        let valid = matches!(
            (locator, self.blob_paths, self.cipher.is_plaintext()),
            (
                crate::protocol::blob::locator::BlobLocator::Opaque { .. },
                BlobPathScheme::Hashed,
                false
            ) | (
                crate::protocol::blob::locator::BlobLocator::Browsable { .. },
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
        locator: &crate::protocol::blob::locator::BlobLocator,
        authority: &crate::protocol::objects::BlobWriteAuthority<'_>,
    ) -> Result<(), StorageError> {
        authority
            .reference
            .verify_registration(authority.registration)
            .map_err(|error| StorageError::InvalidContent(error.to_string()))?;
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
            .exact
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

    pub(crate) fn uses_identity(&self, identity: &UserKeypair) -> bool {
        self.keypair.public_key() == identity.public_key()
    }

    pub(crate) fn is_plaintext(&self) -> bool {
        self.cipher.is_plaintext()
    }

    fn cipher(&self) -> CloudCipher {
        self.cipher.snapshot()
    }

    /// The cipher to seal new data under — refuses while the cloud has committed
    /// a rotation this device has not adopted, rather than sealing under the
    /// generation the store has superseded. Every write that protects data under
    /// the store key calls this instead of reading `self.cipher()` directly;
    /// reads/opens are unaffected (they resolve their own generation from the
    /// ciphertext's tag) and keep reading the cipher plainly.
    fn cipher_for_seal(&self) -> Result<CloudCipher, StorageError> {
        let cipher = self.cipher();
        self.pending_rotation.check(&cipher)?;
        Ok(cipher)
    }

    fn protocol_cipher_for_seal(
        &self,
        context: &ProtocolObjectContext,
    ) -> Result<CloudCipher, StorageError> {
        match context.protection() {
            ProtocolObjectProtection::StoreEncrypted => self.cipher_for_seal(),
            ProtocolObjectProtection::SignedPlaintext => Ok(CloudCipher::Plaintext),
            ProtocolObjectProtection::Circle(encryption) => {
                Ok(CloudCipher::Encrypted(encryption.clone()))
            }
            ProtocolObjectProtection::RecipientSealed => Ok(CloudCipher::Plaintext),
        }
    }

    fn protocol_cipher_for_open(&self, context: &ProtocolObjectContext) -> CloudCipher {
        match context.protection() {
            ProtocolObjectProtection::StoreEncrypted => self.cipher(),
            ProtocolObjectProtection::SignedPlaintext => CloudCipher::Plaintext,
            ProtocolObjectProtection::Circle(encryption) => {
                CloudCipher::Encrypted(encryption.clone())
            }
            ProtocolObjectProtection::RecipientSealed => CloudCipher::Plaintext,
        }
    }

    /// The cloud object key for a blob under the home's [`BlobPathScheme`].
    ///
    /// **A cloud object is never rewritten with different bytes, so no two blobs ever
    /// share a key.** `Hashed` gets that from the key itself; `Plain` gets it from the
    /// blob's declared [`BlobReplacement`](crate::protocol::blob::BlobReplacement), which coven
    /// enforces where a blob is derived from its row ([`crate::database::BlobDecls`]) —
    /// a replaceable blob's readable path must name it, and a write-once blob's row can
    /// never be repointed. Either way, an object's *presence* at a blob's key is proof of
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
    pub(crate) fn blob_key(
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
    ) -> crate::protocol::store_commit::ReferencedStoreDeviceRegistration {
        use crate::protocol::store_commit::{
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
        let provider = SyncStorage::provider_binding(self).await.unwrap().device;
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
        crate::protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
            reference,
            registration,
        )
        .expect("construct test blob write registration")
    }

    #[cfg(test)]
    pub(crate) fn current_encryption(&self) -> Option<EncryptionService> {
        self.cipher.encryption()
    }

    #[cfg(test)]
    pub(crate) fn cipher_snapshot(&self) -> CloudCipher {
        self.cipher.snapshot()
    }

    #[cfg(test)]
    pub(crate) fn adopt_key_rotation_for_test(
        &self,
        encryption: &EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<String, coven_keys::keys::KeyError> {
        CloudCipherAccess::adopt_key_rotation(self, encryption, custody)
    }

    #[cfg(test)]
    pub(crate) fn mark_rotation_committed_for_test(&self, generation: u64) -> Result<(), String> {
        self.pending_rotation.mark_committed(generation)
    }

    #[cfg(test)]
    pub(crate) fn pending_rotation_generation_for_test(&self) -> Option<u64> {
        self.pending_rotation.pending_generation()
    }

    #[cfg(test)]
    pub(crate) fn clear_rotation_gate_for_test(&self) {
        self.pending_rotation.install_durable_gate(None);
    }
}

impl CloudCipherAccess for CloudSyncStorage {
    fn snapshot(&self) -> CloudCipher {
        self.cipher.snapshot()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<Option<String>, coven_keys::keys::KeyError> {
        self.cipher.merge_key_rotation(new_encryption, custody)
    }
}

impl CloudRotationAccess for CloudSyncStorage {
    fn mark_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        self.pending_rotation.mark_candidate(generation, mutation)
    }

    fn mark_committed_mutation(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        self.pending_rotation
            .mark_committed_mutation(generation, mutation)
    }

    fn remove_candidate(&self, generation: u64, mutation: ObjectHash) -> Result<(), String> {
        self.pending_rotation.remove_candidate(generation, mutation)
    }

    fn replace_candidate_mutation(
        &self,
        generation: u64,
        previous: ObjectHash,
        replacement: ObjectHash,
    ) -> Result<(), String> {
        self.pending_rotation
            .replace_candidate_mutation(generation, previous, replacement)
    }

    fn gate(&self) -> Option<RotationGate> {
        self.pending_rotation.gate()
    }

    fn install_durable_gate(&self, gate: Option<RotationGate>) {
        self.pending_rotation.install_durable_gate(gate);
    }

    fn check(&self, cipher: &CloudCipher) -> Result<(), RotationPending> {
        self.pending_rotation.check(cipher)
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
        .map_err(|error| StorageError::Storage(format!("{operation}: {error}")))?
}

async fn read_source_exact(
    source: &mut crate::storage::local_file::PlaintextReader,
    len: usize,
    locator_hash: ObjectHash,
) -> Result<Vec<u8>, StorageError> {
    let mut bytes = Vec::with_capacity(len);
    while bytes.len() < len {
        let chunk = source
            .next_chunk(len - bytes.len())
            .await
            .map_err(StorageError::LocalFilesystem)?;
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
