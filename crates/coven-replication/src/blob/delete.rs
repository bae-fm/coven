//! The delete half of the blob engine: turn a queued blob deletion into a signed
//! cloud **tombstone**, hold the actual deletion for a wall-clock grace, then
//! reclaim the blob once that grace has passed.
//!
//! ## Why a tombstone and a grace, not an immediate delete
//!
//! A blob is shared cloud state referenced by DB rows on every device. Deleting
//! it the instant the deletion drains strands any device that still holds the
//! referencing row: an offline or lagging peer pulls the row's removal on its own
//! later cycle, but by then the blob is already gone, so it sees a row pointing at
//! nothing. A strict cross-device refcount would fix that, but it is
//! unrepresentable in an eventually-consistent bucket with no lock and no global
//! view of who still references what.
//!
//! So the deletion is recorded as a signed tombstone and the blob is kept for
//! [`BLOB_TOMBSTONE_GRACE`] — the convergence window. A device offline for less
//! than the grace is never stranded: it comes back, pulls the row removal, and
//! the blob is still there to be read in the meantime. Once the grace has passed,
//! a GC pass on any device deletes the blob and the tombstone. This is not a
//! self-heal — an unreferenced-but-not-yet-deleted blob is *correct* state during
//! the window; the immediate delete is what *created* the wrong state. The
//! tombstone is the durable record of the deletion; the grace prevents the strand;
//! the GC reclaims converged garbage.
//!
//! ## Why it is signed
//!
//! The bucket is untrusted: the at-rest cipher proves only confidentiality (the
//! store key is shared by every member), not authorship, so anyone who can write
//! the bucket could otherwise drop a tombstone that deletes a blob they were never
//! authorized to remove. The tombstone is therefore signed by its author (like
//! every other control object — heads, the snapshot meta/pointer), and the GC
//! verifies the signature *and* that the author is a current write-capable member
//! before acting on it. A tombstone that fails either check is skipped, never
//! acted on — the blob survives.
//!
//! Tombstones name the exact immutable provider object, not its reusable logical
//! key. Re-uploading the same logical blob allocates a different exact object, so
//! an older tombstone can reclaim only the object it signed and never needs a
//! cross-device cancel queue.

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use coven_database::{OutboxEntry, OutboxOperation};
use coven_keys::keys::{self, UserKeypair};
use coven_protocol::blob::locator::StoredBlobRef;
use coven_storage::CloudSyncObjectStorage;

/// The cloud key-prefix under which tombstones live. The suffix after this prefix
/// is the hash of the exact immutable provider object reference.
pub(crate) fn tombstone_object_id(
    stored: &StoredBlobRef,
) -> coven_protocol::store_commit::ObjectHash {
    coven_storage::blob_tombstone_object_id(stored)
}

pub(crate) fn stored_cloud_key(stored: &StoredBlobRef) -> &str {
    stored.object().slot().logical_key()
}

#[derive(Debug)]
enum ExistingTombstone {
    Valid,
    Absent,
    Invalid(InvalidTombstone),
}

#[derive(Debug, thiserror::Error)]
enum InvalidTombstone {
    #[error("open failed: {0}")]
    Open(#[source] coven_protocol::objects::StorageError),
    #[error("parse failed: {0}")]
    Parse(#[source] serde_json::Error),
    #[error("signed stored blob {actual:?} does not match {expected:?}")]
    StoredReference {
        actual: StoredBlobRef,
        expected: StoredBlobRef,
    },
    #[error("signature verification failed")]
    Signature,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum TombstoneDrainError {
    #[error("tombstone read failed: {0}")]
    Read(#[source] coven_protocol::objects::StorageError),
    #[error("tombstone serialization failed: {0}")]
    Serialize(#[source] serde_json::Error),
    #[error("tombstone write failed: {0}")]
    Write(#[source] coven_protocol::objects::StorageError),
    #[error("blob deletion database state: {0}")]
    Database(#[from] coven_database::DbError),
    #[error("pending delete query returned non-delete outbox entry {entry_id}")]
    NonDeleteOutboxEntry { entry_id: i64 },
    #[error("failed to record delete failure for {cloud_key} (entry {entry_id}): {source}")]
    RecordFailure {
        cloud_key: String,
        entry_id: i64,
        #[source]
        source: coven_database::DbError,
    },
    #[error("tombstone write failed for {cloud_key}: {source}")]
    Publish {
        cloud_key: String,
        #[source]
        source: Box<TombstoneDrainError>,
    },
    #[error("failed to validate an existing tombstone for {cloud_key}: {source}")]
    Validate {
        cloud_key: String,
        #[source]
        source: Box<TombstoneDrainError>,
    },
    #[error("failed to remove delete outbox entry {entry_id}: {source}")]
    RemoveOutboxEntry {
        entry_id: i64,
        #[source]
        source: coven_database::DbError,
    },
}

/// One coherent pass that drains queued blob deletions into signed tombstones.
/// The blob itself is **not** deleted here — the writer's tombstone collection
/// reclaims it once the tombstone has aged past [`BLOB_TOMBSTONE_GRACE`].
///
/// Coven records a delete intent atomically with the row or blob transition;
/// the drain records that intent durably in the cloud as a tombstone so every
/// device converges on the deletion, rather than deleting the blob out from
/// under a peer that has not pulled the row removal yet.
///
/// A failed tombstone attempt leaves the outbox row queued, records the attempt,
/// and fails the drain so the caller retries the whole operation. Rotation state
/// refuses every write while this device has not adopted a Store-key rotation
/// the cloud has already committed, leaving those rows queued for retry.
///
/// A valid tombstone already at the key is preserved. Its original `deleted_at`
/// fixes the reclaim deadline; rewriting it during a repeated drain would keep
/// moving that deadline and could prevent reclamation indefinitely.
///
/// The operation retains the exact database, cloud home, cipher, rotation state,
/// Store identity, writer identity, and clock used by validation, publication,
/// and retry recording.
pub(crate) struct TombstoneDrain<'a> {
    db: &'a coven_database::StoreDatabase,
    storage: &'a dyn CloudSyncObjectStorage,
    store_id: &'a str,
    keypair: &'a UserKeypair,
    clock: &'a dyn coven_foundation::clock::Clock,
}

/// Serialized form of a `blob_tombstones/{exact_object_hash}{suffix}` object: the durable,
/// signed record that a blob was deleted, plus when, so a GC pass can reclaim the
/// blob once the convergence grace has passed.
///
/// `author_pubkey`/`signature` cover the `BlobTombstoneFields` canonical payload
/// — including the exact stored reference (the slot the tombstone lives under
/// and the provider object it authorizes deleting) and `deleted_at` (so the age
/// can't be forged to dodge or shorten the grace). The GC verifies this
/// signature and authorizes the author against the membership chain before
/// deleting anything.
///
/// `store_id` is part of the signed payload but not stored: the reader supplies
/// its own store id to `Self::verify`, mirroring the snapshot meta/pointer.
/// A member of two stores cannot take one store's tombstone and replay it as
/// the other's — re-verifying under the second store's id fails, because the
/// signature was taken over the first's.
///
/// `author_pubkey` *is* stored: a tombstone's
/// author varies device to device, so the verifier learns who signed it and then
/// checks that author against the chain (the authorization step).
#[derive(Serialize, Deserialize)]
pub struct BlobTombstoneJson {
    /// The exact stored blob authorized for deletion. Its logical key determines
    /// the tombstone slot; its object reference determines which provider object
    /// GC may delete once the grace has passed.
    pub stored: StoredBlobRef,
    /// RFC 3339 wall-clock time the deletion was recorded. The grace is measured
    /// from here; it is signature-covered so the age can't be forged.
    pub deleted_at: String,
    /// Hex-encoded Ed25519 public key of the device that wrote this tombstone.
    pub author_pubkey: String,
    /// Hex-encoded detached signature over `BlobTombstoneFields`.
    pub signature: String,
}

/// The tombstone fields the signature covers, in declaration order. Excludes
/// `author_pubkey`/`signature` (the signature's own outputs). Includes
/// `store_id` (so a tombstone can't be replayed into a different store,
/// mirroring the snapshot payloads) and the exact stored reference (so a valid
/// tombstone can't be relocated or used to delete another provider object).
#[derive(Serialize)]
struct BlobTombstoneFields<'a> {
    store_id: &'a str,
    stored: &'a StoredBlobRef,
    deleted_at: &'a str,
}

impl BlobTombstoneJson {
    /// Build a tombstone for `stored` in `store_id` signed by `keypair`:
    /// fills `author_pubkey` with the device's public key and `signature` with the
    /// detached signature over the canonical payload (which binds `store_id`,
    /// the exact stored object and the deletion time). `store_id` is bound but
    /// not stored — the reader passes its own to `Self::verify`.
    pub(crate) fn signed(
        store_id: &str,
        stored: StoredBlobRef,
        deleted_at: String,
        keypair: &UserKeypair,
    ) -> Self {
        let payload = tombstone_signing_payload(store_id, &stored, &deleted_at);
        let sig = keypair.sign(&payload);
        BlobTombstoneJson {
            stored,
            deleted_at,
            author_pubkey: hex::encode(keypair.public_key()),
            signature: hex::encode(sig),
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`, bound to
    /// `store_id`. A tombstone that fails this is forged, corrupt, tampered (its
    /// stored reference or `deleted_at` changed after signing), or a different store's
    /// tombstone replayed here, and must not be acted on. Whether the author is
    /// *authorized* (a current write-capable member) is a separate check the GC
    /// runs after this.
    pub(crate) fn verify(&self, store_id: &str) -> bool {
        let payload = tombstone_signing_payload(store_id, &self.stored, &self.deleted_at);
        keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
    }
}

fn tombstone_signing_payload(store_id: &str, stored: &StoredBlobRef, deleted_at: &str) -> Vec<u8> {
    let fields = BlobTombstoneFields {
        store_id,
        stored,
        deleted_at,
    };
    serde_json::to_vec(&fields).expect("tombstone fields serialization cannot fail")
}

impl<'a> TombstoneDrain<'a> {
    async fn existing_tombstone_state(
        &self,
        expected_stored: &StoredBlobRef,
    ) -> Result<ExistingTombstone, TombstoneDrainError> {
        let decoded = match self.storage.read_blob_tombstone(expected_stored).await {
            Ok(Some(decoded)) => decoded,
            Ok(None) => return Ok(ExistingTombstone::Absent),
            Err(coven_protocol::objects::StorageError::InvalidContent(error)) => {
                return Ok(ExistingTombstone::Invalid(InvalidTombstone::Open(
                    coven_protocol::objects::StorageError::InvalidContent(error),
                )));
            }
            Err(error) => return Err(TombstoneDrainError::Read(error)),
        };
        let tombstone: BlobTombstoneJson = match serde_json::from_slice(&decoded) {
            Ok(tombstone) => tombstone,
            Err(error) => {
                return Ok(ExistingTombstone::Invalid(InvalidTombstone::Parse(error)));
            }
        };
        if &tombstone.stored != expected_stored {
            return Ok(ExistingTombstone::Invalid(
                InvalidTombstone::StoredReference {
                    actual: tombstone.stored,
                    expected: expected_stored.clone(),
                },
            ));
        }
        if !tombstone.verify(self.store_id) {
            return Ok(ExistingTombstone::Invalid(InvalidTombstone::Signature));
        }
        Ok(ExistingTombstone::Valid)
    }

    async fn write_signed_tombstone(
        &self,
        stored: &StoredBlobRef,
        deleted_at: &str,
    ) -> Result<(), TombstoneDrainError> {
        let tombstone = BlobTombstoneJson::signed(
            self.store_id,
            stored.clone(),
            deleted_at.to_string(),
            self.keypair,
        );
        let bytes = serde_json::to_vec(&tombstone).map_err(TombstoneDrainError::Serialize)?;
        self.storage
            .write_blob_tombstone(stored, bytes)
            .await
            .map_err(TombstoneDrainError::Write)
    }

    async fn record_outbox_failure(
        &self,
        entry: &OutboxEntry,
        cloud_key: &str,
        error: &str,
        attempted_at: &str,
    ) -> Result<(), TombstoneDrainError> {
        if let Err(record_error) = self
            .db
            .record_outbox_failure(
                entry,
                coven_database::OutboxFailure::other(error),
                attempted_at,
            )
            .await
        {
            return Err(TombstoneDrainError::RecordFailure {
                cloud_key: cloud_key.to_owned(),
                entry_id: entry.id,
                source: record_error,
            });
        }
        Ok(())
    }

    /// Bind every dependency used by one deletion-drain pass.
    pub(crate) fn new(
        db: &'a coven_database::StoreDatabase,
        storage: &'a dyn CloudSyncObjectStorage,
        store_id: &'a str,
        keypair: &'a UserKeypair,
        clock: &'a dyn coven_foundation::clock::Clock,
    ) -> Self {
        TombstoneDrain {
            db,
            storage,
            store_id,
            keypair,
            clock,
        }
    }

    /// Write each due deletion as a signed tombstone, then remove its outbox row.
    /// Existing valid tombstones keep their original deletion time; any failed
    /// validation or publication records retry state and fails the pass.
    pub(crate) async fn drain(&self) -> Result<usize, TombstoneDrainError> {
        let db = self.db;
        let clock = self.clock;
        let deletes = db.pending_blob_deletes().await?;

        let now = clock.now();
        let now_rfc = now.to_rfc3339();
        let mut count = 0;
        let scheduled_deletes = deletes
            .into_iter()
            .map(|entry| {
                crate::blob::retry::entry_in_backoff(&entry, now)
                    .map(|in_backoff| (entry, in_backoff))
            })
            .collect::<Result<Vec<_>, _>>()?;
        for (entry, in_backoff) in scheduled_deletes {
            let OutboxOperation::Delete { stored } = &entry.operation else {
                return Err(TombstoneDrainError::NonDeleteOutboxEntry { entry_id: entry.id });
            };
            let cloud_key = stored_cloud_key(stored);
            if in_backoff {
                continue;
            }

            // Write only when the slot is absent or invalid. A valid tombstone already
            // at the key carries the original `deleted_at` that the grace is measured
            // from; overwriting it with a fresh `now` would reset the grace, so a row
            // that re-drains (its prior row-removal failed) must not move the deadline.
            match self.existing_tombstone_state(stored).await {
                Ok(ExistingTombstone::Valid) => {
                    debug!(
                        %cloud_key,
                        "tombstone already exists; preserving its deleted_at (not resetting the grace)"
                    );
                }
                Ok(ExistingTombstone::Absent) => {
                    if let Err(e) = self.write_signed_tombstone(stored, &now_rfc).await {
                        let detail = e.to_string();
                        self.record_outbox_failure(&entry, cloud_key, &detail, &now_rfc)
                            .await?;
                        return Err(TombstoneDrainError::Publish {
                            cloud_key: cloud_key.to_owned(),
                            source: Box::new(e),
                        });
                    }
                    count += 1;
                }
                Ok(ExistingTombstone::Invalid(reason)) => {
                    warn!(
                        %cloud_key,
                        reason = %reason,
                        "replacing invalid tombstone object"
                    );
                    if let Err(e) = self.write_signed_tombstone(stored, &now_rfc).await {
                        let detail = e.to_string();
                        self.record_outbox_failure(&entry, cloud_key, &detail, &now_rfc)
                            .await?;
                        return Err(TombstoneDrainError::Publish {
                            cloud_key: cloud_key.to_owned(),
                            source: Box::new(e),
                        });
                    }
                    count += 1;
                }
                Err(e) => {
                    let detail = format!("tombstone validation failed: {e}");
                    self.record_outbox_failure(&entry, cloud_key, &detail, &now_rfc)
                        .await?;
                    return Err(TombstoneDrainError::Validate {
                        cloud_key: cloud_key.to_owned(),
                        source: Box::new(e),
                    });
                }
            }

            // The tombstone is present (written now or already there); drop the local
            // intent row. If this remove fails the row stays and the next drain finds
            // the tombstone already present, so it removes the row without touching the
            // tombstone — the deletion is never lost and the grace never moves.
            db.remove_blob_delete(&entry).await.map_err(|source| {
                TombstoneDrainError::RemoveOutboxEntry {
                    entry_id: entry.id,
                    source,
                }
            })?;
        }

        Ok(count)
    }
}
