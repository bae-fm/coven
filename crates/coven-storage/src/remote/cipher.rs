use super::*;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKeyringMerge {
    live_key_count: usize,
    merged_key_count: usize,
    merged_generation: u64,
}

impl CloudKeyringMerge {
    pub fn live_key_count(&self) -> usize {
        self.live_key_count
    }

    pub fn merged_key_count(&self) -> usize {
        self.merged_key_count
    }

    pub fn merged_generation(&self) -> u64 {
        self.merged_generation
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AdoptedCloudKeyRotation {
    fingerprint: String,
    generation: u64,
}

#[cfg(any(test, feature = "test-utils"))]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CloudKeyringFacts {
    entries: Vec<(u64, [u8; 32])>,
    seal_key: [u8; 32],
    current_generation: u64,
}

#[cfg(any(test, feature = "test-utils"))]
impl CloudKeyringFacts {
    pub(super) fn from_encryption(encryption: &EncryptionService) -> Self {
        Self {
            entries: encryption.keyring_entries(),
            seal_key: encryption.key_bytes(),
            current_generation: encryption.current_generation(),
        }
    }

    pub fn entries(&self) -> &[(u64, [u8; 32])] {
        &self.entries
    }

    pub fn seal_key(&self) -> [u8; 32] {
        self.seal_key
    }

    pub fn current_generation(&self) -> u64 {
        self.current_generation
    }
}

impl AdoptedCloudKeyRotation {
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Closed access to one session's live at-rest keyring. Callers can use the
/// cipher but cannot take the retained key service out of its owner.
pub trait CloudSyncCipherStateAccess: Send + Sync {
    fn is_plaintext(&self) -> bool;
    fn suffix(&self) -> &'static str;
    fn current_generation(&self) -> Option<u64>;
    fn current_fingerprint(&self) -> Option<String>;
    fn open(&self, stored: Vec<u8>, aad_context: &[u8]) -> Result<Vec<u8>, EncryptionError>;
    fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8>;
    #[cfg(any(test, feature = "test-utils"))]
    fn open_sealed_blob_for_test(
        &self,
        stored: &[u8],
        aad_context: &[u8],
    ) -> Result<(coven_keys::encryption::KeyFingerprint, Vec<u8>), String>;
    fn merged_keyring(
        &self,
        new_encryption: &EncryptionService,
    ) -> Result<CloudKeyringMerge, EncryptionError>;
    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<Option<String>, coven_keys::keys::KeyError>;

    fn adopt_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<AdoptedCloudKeyRotation, coven_keys::keys::KeyError> {
        let fingerprint = match self.merge_key_rotation(new_encryption, custody)? {
            Some(fingerprint) => fingerprint,
            None => self
                .merged_keyring(new_encryption)
                .map_err(|error| coven_keys::keys::KeyError::Crypto(error.to_string()))
                .and_then(|status| {
                    if status.live_key_count() != status.merged_key_count() {
                        return Err(coven_keys::keys::KeyError::Crypto(
                            "live keyring changed without retaining an adopted rotation"
                                .to_string(),
                        ));
                    }
                    self.current_fingerprint().ok_or_else(|| {
                        coven_keys::keys::KeyError::Crypto(
                            "cannot rotate the key of a plaintext cloud home".to_string(),
                        )
                    })
                })?,
        };
        let generation = self.current_generation().ok_or_else(|| {
            coven_keys::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            )
        })?;
        Ok(AdoptedCloudKeyRotation {
            fingerprint,
            generation,
        })
    }
}

/// Adopt `new_encryption`'s generations into the live keyring, or report that
/// it held them all already.
///
/// Custody is written before the live keyring is replaced: a generation this
/// process starts sealing under must never be one custody has not stored, or a
/// restart would leave objects nothing can open. `Ok(None)` means nothing was
/// adopted, so nothing was written either.
fn merge_into(
    live: &mut EncryptionService,
    new_encryption: &EncryptionService,
    custody: &dyn coven_keys::keys::MasterKeyCustody,
) -> Result<Option<String>, coven_keys::keys::KeyError> {
    let merged = live
        .merged_with(new_encryption)
        .map_err(|error| coven_keys::keys::KeyError::Crypto(error.to_string()))?;
    if merged.key_count() == live.key_count() {
        return Ok(None);
    }
    custody.persist(&coven_keys::encryption::MasterKeyring::from(merged.clone()))?;
    *live = merged;
    Ok(Some(live.fingerprint()))
}

impl CloudSyncCipherStateAccess for RwLock<CloudCipher> {
    fn is_plaintext(&self) -> bool {
        self.read().unwrap().is_plaintext()
    }

    fn suffix(&self) -> &'static str {
        self.read().unwrap().suffix()
    }

    fn current_generation(&self) -> Option<u64> {
        match &*self.read().unwrap() {
            CloudCipher::Encrypted(encryption) => Some(encryption.current_generation()),
            CloudCipher::Plaintext => None,
        }
    }

    fn current_fingerprint(&self) -> Option<String> {
        match &*self.read().unwrap() {
            CloudCipher::Encrypted(encryption) => Some(encryption.fingerprint()),
            CloudCipher::Plaintext => None,
        }
    }

    fn open(&self, stored: Vec<u8>, aad_context: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.read().unwrap().open(stored, aad_context)
    }

    fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        self.read().unwrap().seal(plaintext, aad_context)
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn open_sealed_blob_for_test(
        &self,
        stored: &[u8],
        aad_context: &[u8],
    ) -> Result<(coven_keys::encryption::KeyFingerprint, Vec<u8>), String> {
        let cipher = self.read().unwrap();
        let CloudCipher::Encrypted(encryption) = &*cipher else {
            return Err("session is not encrypted".to_string());
        };
        super::blob_io::open_sealed_blob(stored, encryption, aad_context)
    }

    fn merged_keyring(
        &self,
        new_encryption: &EncryptionService,
    ) -> Result<CloudKeyringMerge, EncryptionError> {
        let cipher = self.read().unwrap();
        let CloudCipher::Encrypted(live) = &*cipher else {
            return Err(EncryptionError::KeyManagement(
                "cannot merge keys into a plaintext cloud home".to_string(),
            ));
        };
        let merged = live.merged_with(new_encryption)?;
        Ok(CloudKeyringMerge {
            live_key_count: live.key_count(),
            merged_key_count: merged.key_count(),
            merged_generation: merged.current_generation(),
        })
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn coven_keys::keys::MasterKeyCustody,
    ) -> Result<Option<String>, coven_keys::keys::KeyError> {
        let mut cipher = self.write().unwrap();
        let CloudCipher::Encrypted(live) = &mut *cipher else {
            return Err(coven_keys::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            ));
        };
        merge_into(live, new_encryption, custody)
    }
}

impl CloudCipher {
    pub(super) fn current_generation(&self) -> Option<u64> {
        match self {
            CloudCipher::Encrypted(encryption) => Some(encryption.current_generation()),
            CloudCipher::Plaintext => None,
        }
    }

    /// The at-rest cipher a home's storage mode selects: an opaque home seals
    /// under its store key (`Encrypted`), a browsable home stores in the clear
    /// (`Plaintext`). The sibling of [`BlobPathScheme::for_storage`] — together
    /// they map a [`HomeStorage`](coven_foundation::config::HomeStorage) to its
    /// (path scheme, at-rest cipher) pair.
    ///
    /// `encryption` is the store master service; it is required for (and only
    /// consulted on) an opaque home. `None` is returned only for an opaque home
    /// with no service (a locked store) — a browsable home is always
    /// `Plaintext` regardless. A host streaming a Remote blob opens a
    /// [`BlobRangeReader`] under this cipher, so a read applies the same
    /// protection the upload sealed under.
    pub fn for_storage(
        storage: coven_foundation::config::HomeStorage,
        encryption: Option<EncryptionService>,
    ) -> Option<Self> {
        if storage.is_opaque() {
            encryption.map(CloudCipher::Encrypted)
        } else {
            Some(CloudCipher::Plaintext)
        }
    }

    /// Protect an immutable Store object or mutable membership/key object for
    /// storage. Encrypted homes seal under the current store-key generation and
    /// prefix that generation in cleartext; plaintext homes return the bytes
    /// unchanged.
    pub fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        // A control object is always whole-home scoped; only blobs carry a scope.
        // This is exactly the master-scoped blob path: `encryption_for_scope`
        // maps `Master` to the store key itself.
        self.seal_scoped(
            coven_protocol::blob::BlobScope::Master,
            plaintext,
            aad_context,
        )
    }

    /// Recover a control object read from storage. Inverse of [`Self::seal`].
    pub fn open(&self, stored: Vec<u8>, aad_context: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        self.open_scoped(coven_protocol::blob::BlobScope::Master, stored, aad_context)
    }

    /// Protect a blob under its scope. Encrypted blobs carry the current
    /// store-key generation in cleartext, so a later read knows which
    /// generation to open with.
    pub fn seal_scoped(
        &self,
        scope: coven_protocol::blob::BlobScope,
        plaintext: Vec<u8>,
        aad_context: &[u8],
    ) -> Vec<u8> {
        match self {
            CloudCipher::Encrypted(master) => {
                ScopedBlobSealing::new(scope, master).seal(plaintext, aad_context)
            }
            CloudCipher::Plaintext => plaintext,
        }
    }

    /// Recover a blob under its resolved scope. Inverse of [`Self::seal_scoped`].
    pub fn open_scoped(
        &self,
        scope: coven_protocol::blob::BlobScope,
        stored: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        match self {
            CloudCipher::Encrypted(e) => open_scoped_encrypted(scope, e, &stored, aad_context),
            CloudCipher::Plaintext => Ok(stored),
        }
    }

    /// The object-key suffix this cipher implies: `.enc` for an encrypted home,
    /// empty for a plaintext one. Note `"x".strip_suffix("")` returns `Some("x")`,
    /// so the listing parsers strip an empty suffix as a clean no-op.
    pub fn suffix(&self) -> &'static str {
        match self {
            CloudCipher::Encrypted(_) => ".enc",
            CloudCipher::Plaintext => "",
        }
    }

    /// Whether this is a plaintext (unencrypted) home.
    pub fn is_plaintext(&self) -> bool {
        matches!(self, CloudCipher::Plaintext)
    }

    /// The final object length for a blob framed by `header` under this cipher:
    /// the key tag plus the sealed body for an encrypted home, the plaintext
    /// length verbatim for a browsable one. Known before a byte is sealed, so a
    /// streaming upload can declare its length up front.
    pub fn body_len(&self, header: SealedBlobHeader) -> u64 {
        match self {
            CloudCipher::Encrypted(_) => KeyTag::LEN as u64 + header.sealed_len(),
            CloudCipher::Plaintext => header.plaintext_len(),
        }
    }

    /// Open a streaming [`BlobBody`] over the local plaintext file at `file_path`,
    /// sealing each chunk under `scope`'s key for an encrypted home or passing the
    /// plaintext through for a browsable one — without ever reading or sealing the
    /// whole blob into memory. The streaming sibling of [`seal_scoped`](Self::seal_scoped),
    /// used by the upload drain.
    pub async fn open_body(
        &self,
        scope: coven_protocol::blob::BlobScope,
        file_path: &std::path::Path,
        aad_context: &[u8],
        chunk_size: std::num::NonZeroU32,
    ) -> Result<BlobBody, String> {
        let plaintext_len = coven_foundation::local_file::file_len(file_path).await?;
        let header = SealedBlobHeader::new(
            chunk_size,
            plaintext_len,
            &NoncePolicy::DerivedFromContext {
                context: aad_context.to_vec(),
            },
        );
        let reader = crate::local_file::open_reader(file_path).await?;
        Ok(match self {
            CloudCipher::Encrypted(encryption) => {
                ScopedBlobSealing::new(scope, encryption).into_body(header, reader, aad_context)
            }
            CloudCipher::Plaintext => {
                BlobBody::from_file_with_prefix(self.body_len(header), reader, None, Vec::new())
            }
        })
    }
}

/// The `EncryptionService` a blob's `scope` selects, against `master`: the
/// store master itself, or a per-scope key derived from it. The blob storage
/// methods and the outbox drain both turn a [`coven_protocol::blob::BlobScope`] into a
/// key the same way, so they share this one mapping. Only an encrypted home has
/// per-scope keys, so this is reached only from the [`CloudCipher::Encrypted`]
/// branches.
pub(crate) fn encryption_for_scope(
    scope: coven_protocol::blob::BlobScope,
    master: &EncryptionService,
) -> EncryptionService {
    match scope {
        coven_protocol::blob::BlobScope::Master => master.clone(),
        coven_protocol::blob::BlobScope::Derived(s) => master.derive_scoped(&s),
    }
}

pub fn cloud_aad_context(store_id: &str, cloud_key: &str) -> Vec<u8> {
    let mut context =
        Vec::with_capacity(std::mem::size_of::<u64>() * 2 + store_id.len() + cloud_key.len());
    context.extend_from_slice(&(store_id.len() as u64).to_le_bytes());
    context.extend_from_slice(store_id.as_bytes());
    context.extend_from_slice(&(cloud_key.len() as u64).to_le_bytes());
    context.extend_from_slice(cloud_key.as_bytes());
    context
}

pub(crate) fn protocol_object_aad_context(
    context: &ProtocolObjectContext,
    semantic_prefix: &str,
) -> Vec<u8> {
    let domain = context.domain().aad_label();
    let mut aad = Vec::with_capacity(
        context.store_root_hash().as_bytes().len()
            + std::mem::size_of::<u64>() * 2
            + domain.len()
            + semantic_prefix.len(),
    );
    aad.extend_from_slice(context.store_root_hash().as_bytes());
    aad.extend_from_slice(&(domain.len() as u64).to_le_bytes());
    aad.extend_from_slice(domain);
    aad.extend_from_slice(&(semantic_prefix.len() as u64).to_le_bytes());
    aad.extend_from_slice(semantic_prefix.as_bytes());
    aad
}

/// The key `scope` seals under plus the cleartext key-tag prefix every encrypted
/// object carries (the master seal key's fingerprint, so a later read resolves
/// the exact key to open with — for a derived scope it re-derives from that
/// master key).
pub(crate) struct ScopedBlobSealing {
    encryption: EncryptionService,
    key_tag: Vec<u8>,
}

impl ScopedBlobSealing {
    fn new(scope: coven_protocol::blob::BlobScope, master: &EncryptionService) -> Self {
        Self {
            encryption: encryption_for_scope(scope, master),
            key_tag: KeyTag::write(&master.seal_fingerprint()),
        }
    }

    fn seal(self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        let mut stored = self.key_tag;
        stored.extend(self.encryption.encrypt(&plaintext, aad_context));
        stored
    }

    fn into_body(
        self,
        header: SealedBlobHeader,
        reader: crate::local_file::PlaintextReader,
        aad_context: &[u8],
    ) -> BlobBody {
        let mut prefix = self.key_tag;
        prefix.extend_from_slice(&header.to_bytes());
        BlobBody::from_file_with_prefix(
            KeyTag::LEN as u64 + header.sealed_len(),
            reader,
            Some(
                self.encryption
                    .blob_sealer(
                        header,
                        &NoncePolicy::DerivedFromContext {
                            context: aad_context.to_vec(),
                        },
                        aad_context,
                    )
                    .expect("a blob header records the derived policy it was built under"),
            ),
            prefix,
        )
    }
}

pub(crate) fn opening_encryption_for_scope(
    scope: coven_protocol::blob::BlobScope,
    master: &EncryptionService,
    fingerprint: &[u8; 32],
) -> Result<EncryptionService, EncryptionError> {
    match scope {
        coven_protocol::blob::BlobScope::Master => master.service_for_fingerprint(fingerprint),
        coven_protocol::blob::BlobScope::Derived(scope_id) => {
            master.derive_scoped_for_fingerprint(fingerprint, &scope_id)
        }
    }
}

pub(crate) fn open_scoped_encrypted(
    scope: coven_protocol::blob::BlobScope,
    master: &EncryptionService,
    stored: &[u8],
    aad_context: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    let (fingerprint, ciphertext) = KeyTag::read(stored)?;
    opening_encryption_for_scope(scope, master, &fingerprint)?.decrypt(ciphertext, aad_context)
}
