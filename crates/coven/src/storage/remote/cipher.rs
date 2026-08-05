use super::*;

/// Every encrypted object carries this cleartext prefix naming the key it was
/// sealed under: magic, then the key's full SHA-256 fingerprint. A read resolves
/// that exact key from the keyring rather than trusting a generation number a
/// fork could reuse.
pub(super) const KEY_TAG_MAGIC: &[u8; 4] = b"CKF1";

pub(super) const KEY_FINGERPRINT_LEN: usize = 32;

/// How many bytes of key tag a sealed object carries before its payload — for a
/// blob, before the [`SealedBlobHeader`] that names its chunk size. The public
/// sibling of [`SEALED_BLOB_HEADER_LEN`], so a reader outside this crate can
/// locate a stored blob's header in the object's bytes.
pub(crate) const KEY_TAG_LEN: usize = KEY_TAG_MAGIC.len() + KEY_FINGERPRINT_LEN;

/// A sync session's fixed at-rest representation. The mode is selected once at
/// construction: plaintext has no mutable key state, while encrypted sessions
/// may merge new key generations without ever becoming plaintext.
pub(crate) struct CloudCipherState {
    mode: CloudCipherMode,
}

/// Read-only access to a session cipher snapshot. Production storage implements
/// this with [`CloudCipherState`], whose mode cannot change. The test-utils
/// implementation for a raw lock exists only for injected engine tests.
pub(crate) trait CloudCipherAccess: Send + Sync {
    fn snapshot(&self) -> CloudCipher;
    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError>;

    fn adopt_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<String, crate::keys::KeyError> {
        if let Some(fingerprint) = self.merge_key_rotation(new_encryption, custody)? {
            return Ok(fingerprint);
        }
        let CloudCipher::Encrypted(live) = self.snapshot() else {
            return Err(crate::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            ));
        };
        let retained = live
            .merged_with(new_encryption)
            .map_err(|error| crate::keys::KeyError::Crypto(error.to_string()))?;
        if retained.key_count() != live.key_count() {
            return Err(crate::keys::KeyError::Crypto(
                "live keyring changed without retaining an adopted rotation".to_string(),
            ));
        }
        Ok(live.fingerprint())
    }
}

pub(super) enum CloudCipherMode {
    Encrypted(RwLock<EncryptionService>),
    Plaintext,
}

impl CloudCipherState {
    pub(crate) fn new(cipher: CloudCipher) -> Self {
        let mode = match cipher {
            CloudCipher::Encrypted(encryption) => {
                CloudCipherMode::Encrypted(RwLock::new(encryption))
            }
            CloudCipher::Plaintext => CloudCipherMode::Plaintext,
        };
        Self { mode }
    }

    pub(crate) fn is_plaintext(&self) -> bool {
        matches!(self.mode, CloudCipherMode::Plaintext)
    }

    pub(crate) fn snapshot(&self) -> CloudCipher {
        match &self.mode {
            CloudCipherMode::Encrypted(encryption) => {
                CloudCipher::Encrypted(encryption.read().unwrap().clone())
            }
            CloudCipherMode::Plaintext => CloudCipher::Plaintext,
        }
    }

    pub(crate) fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        let CloudCipherMode::Encrypted(live) = &self.mode else {
            return Err(crate::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            ));
        };
        merge_into(&mut live.write().unwrap(), new_encryption, custody)
    }

    #[cfg(test)]
    pub(crate) fn encryption(&self) -> Option<EncryptionService> {
        match &self.mode {
            CloudCipherMode::Encrypted(encryption) => Some(encryption.read().unwrap().clone()),
            CloudCipherMode::Plaintext => None,
        }
    }
}

impl CloudCipherAccess for CloudCipherState {
    fn snapshot(&self) -> CloudCipher {
        CloudCipherState::snapshot(self)
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        CloudCipherState::merge_key_rotation(self, new_encryption, custody)
    }
}

impl<T: CloudCipherAccess + ?Sized> CloudCipherAccess for Arc<T> {
    fn snapshot(&self) -> CloudCipher {
        (**self).snapshot()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        (**self).merge_key_rotation(new_encryption, custody)
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
    custody: &dyn crate::keys::MasterKeyCustody,
) -> Result<Option<String>, crate::keys::KeyError> {
    let merged = live
        .merged_with(new_encryption)
        .map_err(|error| crate::keys::KeyError::Crypto(error.to_string()))?;
    if merged.key_count() == live.key_count() {
        return Ok(None);
    }
    custody.persist(&crate::encryption::MasterKeyring::from(merged.clone()))?;
    *live = merged;
    Ok(Some(live.fingerprint()))
}

#[cfg(test)]
impl CloudCipherAccess for RwLock<CloudCipher> {
    fn snapshot(&self) -> CloudCipher {
        self.read().unwrap().clone()
    }

    fn merge_key_rotation(
        &self,
        new_encryption: &EncryptionService,
        custody: &dyn crate::keys::MasterKeyCustody,
    ) -> Result<Option<String>, crate::keys::KeyError> {
        let mut cipher = self.write().unwrap();
        let CloudCipher::Encrypted(live) = &mut *cipher else {
            return Err(crate::keys::KeyError::Crypto(
                "cannot rotate the key of a plaintext cloud home".to_string(),
            ));
        };
        merge_into(live, new_encryption, custody)
    }
}

impl CloudCipher {
    /// The at-rest cipher a home's storage mode selects: an opaque home seals
    /// under its store key (`Encrypted`), a browsable home stores in the clear
    /// (`Plaintext`). The sibling of [`BlobPathScheme::for_storage`] — together
    /// they map a [`HomeStorage`](crate::config::HomeStorage) to its
    /// (path scheme, at-rest cipher) pair.
    ///
    /// `encryption` is the store master service; it is required for (and only
    /// consulted on) an opaque home. `None` is returned only for an opaque home
    /// with no service (a locked store) — a browsable home is always
    /// `Plaintext` regardless. A host streaming a Remote blob opens a
    /// [`BlobRangeReader`] under this cipher, so a read applies the same
    /// protection the upload sealed under.
    pub(crate) fn for_storage(
        storage: crate::config::HomeStorage,
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
    pub(crate) fn seal(&self, plaintext: Vec<u8>, aad_context: &[u8]) -> Vec<u8> {
        // A control object is always whole-home scoped; only blobs carry a scope.
        // This is exactly the master-scoped blob path: `encryption_for_scope`
        // maps `Master` to the store key itself.
        self.seal_scoped(
            crate::protocol::blob::BlobScope::Master,
            plaintext,
            aad_context,
        )
    }

    /// Recover a control object read from storage. Inverse of [`Self::seal`].
    pub(crate) fn open(
        &self,
        stored: Vec<u8>,
        aad_context: &[u8],
    ) -> Result<Vec<u8>, EncryptionError> {
        self.open_scoped(
            crate::protocol::blob::BlobScope::Master,
            stored,
            aad_context,
        )
    }

    /// Protect a blob under its scope. Encrypted blobs carry the current
    /// store-key generation in cleartext, so a later read knows which
    /// generation to open with.
    pub(crate) fn seal_scoped(
        &self,
        scope: crate::protocol::blob::BlobScope,
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
    pub(crate) fn open_scoped(
        &self,
        scope: crate::protocol::blob::BlobScope,
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
    pub(crate) fn suffix(&self) -> &'static str {
        match self {
            CloudCipher::Encrypted(_) => ".enc",
            CloudCipher::Plaintext => "",
        }
    }

    /// Whether this is a plaintext (unencrypted) home.
    pub(crate) fn is_plaintext(&self) -> bool {
        matches!(self, CloudCipher::Plaintext)
    }

    /// The final object length for a blob framed by `header` under this cipher:
    /// the key tag plus the sealed body for an encrypted home, the plaintext
    /// length verbatim for a browsable one. Known before a byte is sealed, so a
    /// streaming upload can declare its length up front.
    pub(crate) fn body_len(&self, header: SealedBlobHeader) -> u64 {
        match self {
            CloudCipher::Encrypted(_) => KEY_TAG_LEN as u64 + header.sealed_len(),
            CloudCipher::Plaintext => header.plaintext_len(),
        }
    }

    /// Open a streaming [`BlobBody`] over the local plaintext file at `file_path`,
    /// sealing each chunk under `scope`'s key for an encrypted home or passing the
    /// plaintext through for a browsable one — without ever reading or sealing the
    /// whole blob into memory. The streaming sibling of [`seal_scoped`](Self::seal_scoped),
    /// used by the upload drain.
    pub(crate) async fn open_body(
        &self,
        scope: crate::protocol::blob::BlobScope,
        file_path: &std::path::Path,
        aad_context: &[u8],
        chunk_size: std::num::NonZeroU32,
    ) -> Result<BlobBody, String> {
        let plaintext_len = crate::local_file::file_len(file_path).await?;
        let header = SealedBlobHeader::new(chunk_size, plaintext_len);
        let reader = crate::storage::local_file::open_reader(file_path).await?;
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
/// methods and the outbox drain both turn a [`crate::protocol::blob::BlobScope`] into a
/// key the same way, so they share this one mapping. Only an encrypted home has
/// per-scope keys, so this is reached only from the [`CloudCipher::Encrypted`]
/// branches.
pub(crate) fn encryption_for_scope(
    scope: crate::protocol::blob::BlobScope,
    master: &EncryptionService,
) -> EncryptionService {
    match scope {
        crate::protocol::blob::BlobScope::Master => master.clone(),
        crate::protocol::blob::BlobScope::Derived(s) => master.derive_scoped(&s),
    }
}

pub(crate) fn cloud_aad_context(store_id: &str, cloud_key: &str) -> Vec<u8> {
    let mut context =
        Vec::with_capacity(std::mem::size_of::<u64>() * 2 + store_id.len() + cloud_key.len());
    context.extend_from_slice(&(store_id.len() as u64).to_le_bytes());
    context.extend_from_slice(store_id.as_bytes());
    context.extend_from_slice(&(cloud_key.len() as u64).to_le_bytes());
    context.extend_from_slice(cloud_key.as_bytes());
    context
}

pub(super) fn protocol_object_aad_context(
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

pub(super) fn key_tag(fingerprint: &[u8; KEY_FINGERPRINT_LEN]) -> Vec<u8> {
    let mut tag = Vec::with_capacity(KEY_TAG_LEN);
    tag.extend_from_slice(KEY_TAG_MAGIC);
    tag.extend_from_slice(fingerprint);
    tag
}

pub(super) fn read_key_tag(
    stored: &[u8],
) -> Result<([u8; KEY_FINGERPRINT_LEN], &[u8]), EncryptionError> {
    if stored.len() < KEY_TAG_LEN {
        return Err(EncryptionError::Decryption(
            "ciphertext too short for key tag".to_string(),
        ));
    }
    if &stored[..KEY_TAG_MAGIC.len()] != KEY_TAG_MAGIC {
        return Err(EncryptionError::Decryption(
            "ciphertext missing key tag".to_string(),
        ));
    }
    let mut fingerprint = [0u8; KEY_FINGERPRINT_LEN];
    fingerprint.copy_from_slice(&stored[KEY_TAG_MAGIC.len()..KEY_TAG_LEN]);
    Ok((fingerprint, &stored[KEY_TAG_LEN..]))
}

/// The key `scope` seals under plus the cleartext key-tag prefix every encrypted
/// object carries (the master seal key's fingerprint, so a later read resolves
/// the exact key to open with — for a derived scope it re-derives from that
/// master key).
pub(super) struct ScopedBlobSealing {
    encryption: EncryptionService,
    key_tag: Vec<u8>,
}

impl ScopedBlobSealing {
    fn new(scope: crate::protocol::blob::BlobScope, master: &EncryptionService) -> Self {
        Self {
            encryption: encryption_for_scope(scope, master),
            key_tag: key_tag(&master.seal_fingerprint()),
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
        reader: crate::storage::local_file::PlaintextReader,
        aad_context: &[u8],
    ) -> BlobBody {
        let mut prefix = self.key_tag;
        prefix.extend_from_slice(&header.to_bytes());
        BlobBody::from_file_with_prefix(
            KEY_TAG_LEN as u64 + header.sealed_len(),
            reader,
            Some(self.encryption.blob_sealer(header, aad_context)),
            prefix,
        )
    }
}

pub(super) fn opening_encryption_for_scope(
    scope: crate::protocol::blob::BlobScope,
    master: &EncryptionService,
    fingerprint: &[u8; KEY_FINGERPRINT_LEN],
) -> Result<EncryptionService, EncryptionError> {
    match scope {
        crate::protocol::blob::BlobScope::Master => master.service_for_fingerprint(fingerprint),
        crate::protocol::blob::BlobScope::Derived(scope_id) => {
            master.derive_scoped_for_fingerprint(fingerprint, &scope_id)
        }
    }
}

pub(super) fn open_scoped_encrypted(
    scope: crate::protocol::blob::BlobScope,
    master: &EncryptionService,
    stored: &[u8],
    aad_context: &[u8],
) -> Result<Vec<u8>, EncryptionError> {
    let (fingerprint, ciphertext) = read_key_tag(stored)?;
    opening_encryption_for_scope(scope, master, &fingerprint)?.decrypt(ciphertext, aad_context)
}
