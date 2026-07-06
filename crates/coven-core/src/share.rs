//! Sharing an item to a non-member.
//!
//! A *share* grants one item's content to an outsider who holds only a secret in
//! a URL fragment — not a coven member, not in the signed membership chain. It is
//! the symmetric counterpart to membership: membership wraps the library master
//! key to a member's X25519 keypair (asymmetric `seal_box`); a share wraps one
//! [item key](crate::CovenHandle::mint_item_key) under a random per-share secret
//! with the same symmetric AEAD ([`EncryptionService`], XChaCha20-Poly1305) that
//! encrypts blobs. The recipient has no keypair, only the secret.
//!
//! Sharing is its own capability, independent of how the library is stored.
//! [`ShareProxy`] layers on top of whatever [`CloudHome`] the library already
//! uses (S3, Google Drive, Dropbox, OneDrive, iCloud) — it is *not* a storage
//! backend. [`ShareProxy::create`] writes two objects under `shares/{share_id}/`:
//! - `key.enc` — the item key wrapped under the per-share secret.
//! - `manifest.json` — the authorized blobs as `(namespace, id)` logical refs.
//!
//! Whatever serves `shares/{share_id}/*` does so unauthenticated, so the share's
//! security rests on two independent properties: the per-share secret stays in
//! the URL fragment (never sent on a request, so the server never sees it), and
//! `share_id` is high-entropy so the prefix is unguessable. The manifest
//! authorizes *fetch*; the wrapped item key authorizes *decrypt* — independent,
//! so a foreign blob ref in a manifest leaks only undecryptable ciphertext.
//!
//! This module is gated behind the `share-proxy` cargo feature.

use serde::{Deserialize, Serialize};

use crate::blob::BlobId;
use tracing::warn;

use crate::database::Database;
use crate::encryption::EncryptionService;
use crate::storage::cloud::{no_progress, CloudHome, CloudHomeError};

/// What can go wrong creating, opening, or revoking a share.
#[derive(Debug, thiserror::Error)]
pub enum ShareError {
    /// The item has no minted key. The host must
    /// [`mint_item_key`](crate::CovenHandle::mint_item_key) (and let it sync) before
    /// sharing — coven will not invent one, because a share recipient must hold
    /// the same key the item's blobs are encrypted under.
    #[error("no item key for {0}: mint_item_key must run before sharing the item")]
    ItemKeyAbsent(String),

    /// The owned database errored reading the item key.
    #[error("database error: {0}")]
    Db(#[from] crate::database::DbError),

    /// A cloud storage read/write/delete failed.
    #[error("storage error: {0}")]
    Storage(#[from] CloudHomeError),

    /// Serializing the manifest failed.
    #[error("manifest serialization error: {0}")]
    Manifest(#[from] serde_json::Error),

    /// Unwrapping `key.enc` failed, or it did not decrypt to a 32-byte key. A
    /// wrong fragment secret, a tampered object, or a malformed share all land
    /// here.
    #[error("could not open share: {0}")]
    Open(String),
}

/// What [`ShareProxy::create`] hands back to the host. The host builds the share
/// URL `{base}/share/{share_id}#{base64url(secret)}` — coven returns the secret
/// as raw bytes and lets the host choose the fragment encoding.
#[derive(Clone)]
pub struct ShareToken {
    /// The high-entropy id naming the `shares/{share_id}/` prefix. coven owns it
    /// (not a host row id), so it is unguessable independent of how the host
    /// names items.
    pub share_id: String,
    /// The per-share secret that unwraps the item key. A bearer secret: anyone
    /// who holds it can open the share, so it stays in the URL fragment and is
    /// never sent on a request.
    pub secret: [u8; 32],
}

impl std::fmt::Debug for ShareToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The secret is a bearer credential; never print it.
        f.debug_struct("ShareToken")
            .field("share_id", &self.share_id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

/// The blobs a share authorizes, as `(namespace, id)` logical refs — never hashed
/// cloud paths. coven hashes each ref to its cloud key internally (see
/// [`ShareManifest::allows`]) so neither the host nor whatever serves the share
/// reconstructs coven's `{namespace}/{ab}/{cd}/{id}` layout.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShareManifest {
    /// Each authorized blob's logical `(namespace, id)` reference.
    pub blobs: Vec<BlobId>,
}

impl ShareManifest {
    /// Whether `cloud_key` is one of the authorized blobs. Whatever serves the
    /// share resolves a requested object to its cloud key and asks coven; coven
    /// hashes each authorized [`BlobId`] to its cloud key with the same hashed
    /// layout ([`crate::library_dir::LibraryDir::hashed_path`]) and matches. The
    /// server therefore never learns coven's `{ab}/{cd}` partitioning. Sharing is
    /// inherently hashed: `allows` recomputes a blob's key from its id, which only
    /// the content-addressed layout supports — the plain (consumer-path) scheme
    /// has no id-derivable key, so share-proxy always uses the hashed layout.
    pub fn allows(&self, cloud_key: &str) -> bool {
        self.blobs.iter().any(|b| {
            // This runs on the unauthenticated server gate against host-supplied
            // refs read from a manifest that may have been tampered with in the
            // cloud. `hashed_path` refuses a ref whose id can't form a safe,
            // indexable cloud key (a traversal token, or one too short / misaligned
            // to take the `{ab}/{cd}` prefix) — exactly the bad data a panic-prone
            // slice would crash on. A real coven cloud key always has those leading
            // byte-pairs, so a refused ref can never match one.
            // Share-proxy always uses the hashed layout (see the doc comment).
            match crate::library_dir::LibraryDir::hashed_path(&b.namespace, &b.id) {
                Ok(key) => key == cloud_key,
                Err(e) => {
                    warn!(
                        namespace = %b.namespace,
                        id = %b.id,
                        "share manifest ref is not a usable coven blob id ({e}); skipping"
                    );
                    false
                }
            }
        })
    }
}

/// Prefix under which every share's objects live: `shares/{share_id}/...`.
const SHARE_PREFIX: &str = "shares";

/// The item key wrapped under the per-share secret.
const KEY_ENC_FILE: &str = "key.enc";

/// The authorized-blobs manifest.
const MANIFEST_FILE: &str = "manifest.json";
const SHARE_WRAP_AAD: &[u8] = b"coven-share-wrap-v1";
#[cfg(test)]
const SHARE_BLOB_TEST_AAD: &[u8] = b"coven-share-blob-test-v1";

/// Cloud object key for one of a share's objects, e.g.
/// `shares/{share_id}/key.enc`. The single home for the share layout, so
/// [`ShareProxy::create`] and [`ShareProxy::revoke`] name the same objects.
fn share_object_path(share_id: &str, filename: &str) -> String {
    format!("{SHARE_PREFIX}/{share_id}/{filename}")
}

/// Generate a high-entropy, unguessable `share_id`: 32 random bytes (256 bits,
/// well above the 122-bit floor the unauthenticated-server threat model
/// requires), hex-encoded. coven owns this id — it is not a host sequential row
/// id, which would be guessable and would couple the share to the host's id
/// scheme.
fn new_share_id() -> String {
    hex::encode(crate::encryption::generate_random_key())
}

/// Mints, revokes, and serves item shares over any [`CloudHome`].
///
/// Sharing is independent of how the library is stored — this is NOT a storage
/// backend; it layers on top of whatever `CloudHome` the library already uses
/// (S3, Drive, Dropbox, OneDrive, iCloud). It borrows the owned [`Database`] (to
/// read item keys) and that `CloudHome` (to write/delete the share's objects),
/// and mints or revokes shares over them.
///
/// The recipient side has neither — opening a share needs only the secret and the
/// wrapped key bytes — so that is the free [`open_share`] function, which
/// constructs no `ShareProxy`.
pub struct ShareProxy<'a> {
    db: &'a Database,
    cloud_home: &'a dyn CloudHome,
}

impl<'a> ShareProxy<'a> {
    /// Layer sharing over the library's `db` and `cloud_home`.
    pub fn new(db: &'a Database, cloud_home: &'a dyn CloudHome) -> Self {
        Self { db, cloud_home }
    }

    /// Export `item_id` as a share: wrap its item key under a fresh per-share
    /// secret and publish the wrapped key + the authorized-blobs manifest under a
    /// new high-entropy `shares/{share_id}/` prefix.
    ///
    /// `blobs` are the `(namespace, id)` logical refs the share authorizes for
    /// fetch — coven stores them verbatim in the manifest and hashes them to cloud
    /// keys only when gating a request ([`ShareManifest::allows`]).
    ///
    /// Errors with [`ShareError::ItemKeyAbsent`] if the item has no minted key:
    /// the host must mint it (and let it sync) first, since the recipient must
    /// hold the exact key the item's blobs are encrypted under.
    pub async fn create(
        &self,
        item_id: &str,
        blobs: Vec<BlobId>,
    ) -> Result<ShareToken, ShareError> {
        let item_key = self
            .db
            .item_key(item_id)
            .await?
            .ok_or_else(|| ShareError::ItemKeyAbsent(item_id.to_string()))?;

        let secret = crate::encryption::generate_random_key();
        let share_id = new_share_id();

        // Wrap the item key under the per-share secret with the symmetric AEAD.
        // This is the share wire format:
        // `key.enc = from_key(secret).encrypt(item_key, SHARE_WRAP_AAD)`, the exact bytes
        // `open_share` reverses.
        let key_enc = EncryptionService::from_key(secret).encrypt(&item_key, SHARE_WRAP_AAD);

        let manifest = ShareManifest { blobs };
        let manifest_json = serde_json::to_vec(&manifest)?;

        for (filename, data) in [(KEY_ENC_FILE, key_enc), (MANIFEST_FILE, manifest_json)] {
            self.cloud_home
                .write(
                    &share_object_path(&share_id, filename),
                    crate::storage::cloud::BlobBody::from_bytes(data),
                    &no_progress(),
                )
                .await?;
        }

        Ok(ShareToken { share_id, secret })
    }

    /// Revoke a share by deleting its objects, so whatever serves the share can no
    /// longer read the manifest or the wrapped key and the URL stops resolving.
    ///
    /// This is the storage-access cut: it severs the only unauthenticated path to
    /// the item's blobs. Bytes a recipient already downloaded are not clawed back
    /// (the envelope model) — claw-back-before-download needs item-key rotation, a
    /// separate capability.
    pub async fn revoke(&self, share_id: &str) -> Result<(), ShareError> {
        for filename in [KEY_ENC_FILE, MANIFEST_FILE] {
            self.cloud_home
                .delete(&share_object_path(share_id, filename))
                .await?;
        }
        Ok(())
    }
}

/// Recover an item key from a share's wrapped key, given the per-share secret.
///
/// This is a PURE function — it depends only on [`crate::encryption`], not on the
/// database or cloud storage — so it documents the share wire format for any
/// client that does not link coven's storage layer. A browser that cannot compile
/// coven as a whole (rusqlite-bundled, tokio-full, aws-sdk) opens a share by
/// matching this format with its own XChaCha20-Poly1305 primitive:
///
/// ```text
/// key.enc = EncryptionService::from_key(secret).encrypt(item_key, b"coven-share-wrap-v1")
/// item_key = EncryptionService::from_key(secret).decrypt(key.enc, b"coven-share-wrap-v1")
/// ```
///
/// `secret` is the URL-fragment bearer secret; `key_enc` is the bytes of
/// `shares/{share_id}/key.enc`. Returns the 32-byte item key, with which the
/// recipient decrypts the authorized blobs.
pub fn open_share(secret: &[u8; 32], key_enc: &[u8]) -> Result<[u8; 32], ShareError> {
    let item_key = EncryptionService::from_key(*secret)
        .decrypt(key_enc, SHARE_WRAP_AAD)
        .map_err(|e| ShareError::Open(e.to_string()))?;
    item_key.try_into().map_err(|v: Vec<u8>| {
        ShareError::Open(format!("unwrapped key is {} bytes, not 32", v.len()))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::sync::test_helpers::open_test_db;

    /// Read an object back from the cloud, panicking with the key on absence so a
    /// missing share object is a loud test failure, not a silent `None`.
    async fn read_cloud(home: &InMemoryCloudHome, key: &str) -> Vec<u8> {
        home.read(key)
            .await
            .unwrap_or_else(|e| panic!("read {key}: {e}"))
    }

    /// Round-trip: mint an item key, encrypt bytes under it, create a share, read
    /// `key.enc` back from the cloud, `open_share` to recover the item key, and
    /// decrypt the bytes. The recovered key and plaintext both match the
    /// originals — the share carries exactly the key the item's blobs use.
    #[tokio::test]
    async fn share_round_trip_recovers_key_and_plaintext() {
        let db = open_test_db();
        let home = InMemoryCloudHome::new();

        let item_key = db.mint_item_key("item-1").await.expect("mint item key");
        let plaintext = b"the shared item's content".to_vec();
        let ciphertext =
            EncryptionService::from_key(item_key).encrypt(&plaintext, SHARE_BLOB_TEST_AAD);

        let token = ShareProxy::new(&db, &home)
            .create(
                "item-1",
                vec![BlobId {
                    namespace: "audio".to_string(),
                    id: "blob-1".to_string(),
                }],
            )
            .await
            .expect("create share");

        let key_enc = read_cloud(&home, &share_object_path(&token.share_id, KEY_ENC_FILE)).await;
        let recovered = open_share(&token.secret, &key_enc).expect("open share");

        assert_eq!(recovered, item_key, "open_share recovers the item key");
        let decrypted = EncryptionService::from_key(recovered)
            .decrypt(&ciphertext, SHARE_BLOB_TEST_AAD)
            .expect("decrypt with the recovered item key");
        assert_eq!(decrypted, plaintext, "the recovered key decrypts the blob");
    }

    /// Creating a share for an item with no minted key surfaces
    /// [`ShareError::ItemKeyAbsent`] naming the item — coven does not invent a key
    /// a recipient could never match against the item's blobs.
    #[tokio::test]
    async fn create_without_item_key_errors() {
        let db = open_test_db();
        let home = InMemoryCloudHome::new();

        let err = ShareProxy::new(&db, &home)
            .create("never-minted", Vec::new())
            .await
            .expect_err("sharing an item with no key must error");
        assert!(
            matches!(&err, ShareError::ItemKeyAbsent(id) if id == "never-minted"),
            "the error names the offending item: {err}"
        );
        assert!(
            home.is_empty(),
            "no share objects are written when the item key is absent"
        );
    }

    /// The manifest gate matches a listed `(namespace, id)`'s cloud key and
    /// rejects an unlisted one. coven hashes the authorized refs to cloud keys
    /// internally, so whatever serves the share passes a cloud key and never
    /// reconstructs the layout.
    #[test]
    fn manifest_allows_listed_blob_only() {
        let manifest = ShareManifest {
            blobs: vec![
                BlobId {
                    namespace: "audio".to_string(),
                    id: "blob-1".to_string(),
                },
                BlobId {
                    namespace: "images".to_string(),
                    id: "cover-1".to_string(),
                },
            ],
        };

        // The authorized cloud key, hardcoded as the real `{namespace}/{ab}/{cd}/{id}`
        // layout: the dash-stripped id `blob1` partitions to `bl`/`ob`. Asserting
        // against the literal (not against `blob_key`'s own output) makes a
        // regression in the path layout fail this test instead of moving in
        // lockstep with it.
        assert!(
            manifest.allows("audio/bl/ob/blob-1"),
            "a listed (namespace, id) resolves to its authorized cloud key"
        );

        assert!(
            !manifest.allows("audio/bl/ob/blob-2"),
            "an unlisted (namespace, id) is rejected"
        );
        // A bare id with no layout is not a cloud key and never matches.
        assert!(!manifest.allows("blob-1"));
    }

    /// A manifest ref whose dash-stripped id is too short to partition (`{ab}/{cd}`
    /// needs four hex chars) must not panic the unauthenticated `allows` gate: a
    /// tampered cloud manifest could carry such a ref, and a panic on the server is
    /// a denial of service. It can never match a real cloud key, so `allows`
    /// reports a no-match.
    #[test]
    fn manifest_allows_does_not_panic_on_unindexable_ref() {
        let manifest = ShareManifest {
            blobs: vec![BlobId {
                namespace: "audio".to_string(),
                id: "ab".to_string(),
            }],
        };
        assert!(
            !manifest.allows("audio/ab/ab/ab"),
            "a ref too short to partition never matches and never panics"
        );
    }

    /// Pin `ShareManifest`'s serialized JSON shape. Whatever serves the share is a
    /// separate component that deserializes this manifest from the cloud, so the
    /// wire format is a cross-component contract: a field rename or a
    /// tuple-vs-struct change here would silently break it. This catches such a
    /// drift.
    #[test]
    fn manifest_json_shape_is_pinned() {
        let manifest = ShareManifest {
            blobs: vec![BlobId {
                namespace: "audio".to_string(),
                id: "blob-1".to_string(),
            }],
        };
        assert_eq!(
            serde_json::to_value(&manifest).expect("serialize manifest"),
            serde_json::json!({ "blobs": [{ "namespace": "audio", "id": "blob-1" }] }),
        );
    }

    /// After [`ShareProxy::revoke`], whatever serves the share can no longer read
    /// either share object: reading `key.enc` from the cloud errors, so the URL
    /// stops resolving.
    #[tokio::test]
    async fn revoke_removes_share_objects() {
        let db = open_test_db();
        let home = InMemoryCloudHome::new();

        db.mint_item_key("item-1").await.expect("mint item key");
        let share_proxy = ShareProxy::new(&db, &home);
        let token = share_proxy
            .create("item-1", Vec::new())
            .await
            .expect("create share");

        // Both objects exist before revocation.
        assert!(home
            .get(&share_object_path(&token.share_id, KEY_ENC_FILE))
            .is_some());
        assert!(home
            .get(&share_object_path(&token.share_id, MANIFEST_FILE))
            .is_some());

        share_proxy
            .revoke(&token.share_id)
            .await
            .expect("revoke share");

        assert!(
            matches!(
                home.read(&share_object_path(&token.share_id, KEY_ENC_FILE))
                    .await,
                Err(CloudHomeError::NotFound(_))
            ),
            "key.enc is gone after revocation"
        );
        assert!(
            matches!(
                home.read(&share_object_path(&token.share_id, MANIFEST_FILE))
                    .await,
                Err(CloudHomeError::NotFound(_))
            ),
            "manifest.json is gone after revocation"
        );
    }

    /// Cross-item isolation: each item gets an independent random key, so a share
    /// of item-1 yields item-1's key, which CANNOT decrypt a blob encrypted under
    /// item-2's key. The manifest authorizes fetch; the key authorizes decrypt —
    /// forcing item-2's blob ref into a share of item-1 still leaks only
    /// undecryptable ciphertext.
    #[tokio::test]
    async fn share_of_one_item_cannot_decrypt_another() {
        let db = open_test_db();
        let home = InMemoryCloudHome::new();

        let key_1 = db.mint_item_key("item-1").await.expect("mint item-1 key");
        let key_2 = db.mint_item_key("item-2").await.expect("mint item-2 key");
        assert_ne!(key_1, key_2, "each item gets an independent random key");

        // A blob encrypted under item-2's key.
        let item_2_plaintext = b"item-2 secret audio".to_vec();
        let item_2_ciphertext =
            EncryptionService::from_key(key_2).encrypt(&item_2_plaintext, SHARE_BLOB_TEST_AAD);

        // Share item-1 — even with item-2's blob forced into the manifest.
        let token = ShareProxy::new(&db, &home)
            .create(
                "item-1",
                vec![BlobId {
                    namespace: "audio".to_string(),
                    id: "item-2-blob".to_string(),
                }],
            )
            .await
            .expect("create share of item-1");

        let key_enc = read_cloud(&home, &share_object_path(&token.share_id, KEY_ENC_FILE)).await;
        let recovered = open_share(&token.secret, &key_enc).expect("open share");
        assert_eq!(recovered, key_1, "the share yields item-1's key");

        assert!(
            EncryptionService::from_key(recovered)
                .decrypt(&item_2_ciphertext, SHARE_BLOB_TEST_AAD)
                .is_err(),
            "item-1's key cannot decrypt item-2's blob — a foreign ref in the \
             manifest leaks only undecryptable ciphertext"
        );
    }
}
