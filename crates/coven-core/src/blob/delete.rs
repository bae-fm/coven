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

use crate::blob::locator::StoredBlobRef;
use crate::database::{Database, StoredBlobReferenceState};
use crate::db::{OutboxEntry, OutboxOperation};
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{no_progress, CloudHome};
use crate::sync::cloud_storage::{CloudCipherAccess, PendingRotation};
use crate::sync::membership::MembershipChain;
use crate::sync::storage::{StorageError, SyncStorage};

/// The default convergence window a host gets if it configures none: how long a
/// deleted blob is kept after its tombstone is written, before a GC pass reclaims
/// it. The host overrides it on the coven builder; [`gc_tombstones`] evaluates
/// whatever grace it is handed against the tombstone's `deleted_at`.
///
/// A device offline for less than the grace is never stranded by a deletion — when
/// it reconnects it pulls the removal of the row that referenced the blob, and the
/// blob is still present until then. The window is human-scale (days, not the
/// sub-second commit window the snapshot sweep's grace covers) because the device
/// it protects is a person's offline laptop or phone, not a concurrent writer
/// mid-publish.
pub const BLOB_TOMBSTONE_GRACE: chrono::Duration = chrono::Duration::days(7);

/// The cloud key-prefix under which tombstones live. The suffix after this prefix
/// is the hash of the exact immutable provider object reference.
const TOMBSTONE_PREFIX: &str = "blob_tombstones/";

fn tombstone_object_id(stored: &StoredBlobRef) -> crate::sync::store_commit::ObjectHash {
    crate::sync::remote_object::remote_object_id(stored.object())
}

fn tombstone_key(stored: &StoredBlobRef, suffix: &str) -> String {
    format!("{TOMBSTONE_PREFIX}{}{suffix}", tombstone_object_id(stored))
}

fn stored_cloud_key(stored: &StoredBlobRef) -> &str {
    stored.object().slot().logical_key()
}

#[derive(Debug, PartialEq, Eq)]
enum ExistingTombstone {
    Valid,
    Absent,
    Invalid(String),
}

/// Serialized form of a `blob_tombstones/{exact_object_hash}{suffix}` object: the durable,
/// signed record that a blob was deleted, plus when, so a GC pass can reclaim the
/// blob once the convergence grace has passed.
///
/// `author_pubkey`/`signature` cover the [`BlobTombstoneFields`] canonical payload
/// — including the exact stored reference (the slot the tombstone lives under
/// and the provider object it authorizes deleting) and `deleted_at` (so the age
/// can't be forged to dodge or shorten the grace). The GC verifies this
/// signature and authorizes the author against the membership chain before
/// deleting anything.
///
/// `store_id` is part of the signed payload but not stored: the reader supplies
/// its own store id to [`Self::verify`], mirroring the snapshot meta/pointer.
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
    /// Hex-encoded detached signature over [`BlobTombstoneFields`].
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
    /// not stored — the reader passes its own to [`Self::verify`].
    pub fn signed(
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
    pub fn verify(&self, store_id: &str) -> bool {
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

async fn existing_tombstone_state(
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    store_id: &str,
    key: &str,
    expected_stored: &StoredBlobRef,
) -> Result<ExistingTombstone, String> {
    let stored = match cloud_home.read(key).await {
        Ok(stored) => stored,
        Err(crate::storage::cloud::CloudHomeError::NotFound(_)) => {
            return Ok(ExistingTombstone::Absent)
        }
        Err(e) => return Err(format!("tombstone read failed: {e}")),
    };
    let aad_context = crate::sync::cloud_storage::cloud_aad_context(store_id, key);
    let decoded = match cipher.snapshot().open(stored, &aad_context) {
        Ok(decoded) => decoded,
        Err(e) => return Ok(ExistingTombstone::Invalid(format!("open failed: {e}"))),
    };
    let tombstone: BlobTombstoneJson = match serde_json::from_slice(&decoded) {
        Ok(tombstone) => tombstone,
        Err(e) => return Ok(ExistingTombstone::Invalid(format!("parse failed: {e}"))),
    };
    if &tombstone.stored != expected_stored {
        return Ok(ExistingTombstone::Invalid(format!(
            "signed stored blob {:?} does not match {expected_stored:?}",
            tombstone.stored
        )));
    }
    if !tombstone.verify(store_id) {
        return Ok(ExistingTombstone::Invalid(
            "signature verification failed".to_string(),
        ));
    }
    Ok(ExistingTombstone::Valid)
}

async fn write_signed_tombstone(
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    store_id: &str,
    key: &str,
    stored: &StoredBlobRef,
    deleted_at: &str,
    keypair: &UserKeypair,
) -> Result<(), String> {
    let tombstone =
        BlobTombstoneJson::signed(store_id, stored.clone(), deleted_at.to_string(), keypair);
    let bytes = serde_json::to_vec(&tombstone)
        .map_err(|e| format!("tombstone serialization failed: {e}"))?;
    let aad_context = crate::sync::cloud_storage::cloud_aad_context(store_id, key);
    let cipher = cipher.snapshot();
    pending_rotation.check(&cipher).map_err(|e| e.to_string())?;
    let sealed = cipher.seal(bytes, &aad_context);
    cloud_home
        .write(
            key,
            crate::storage::cloud::BlobBody::from_bytes(sealed),
            &no_progress(),
        )
        .await
        .map_err(|e| format!("tombstone write failed: {e}"))
}

/// Drain the queued blob deletes: for each, write a signed tombstone and remove
/// the outbox row. The blob itself is **not** deleted here — [`gc_tombstones`]
/// reclaims it once the tombstone has aged past [`BLOB_TOMBSTONE_GRACE`].
///
/// The host enqueues a delete via [`crate::database::Database::enqueue_delete`];
/// this records that intent durably in the cloud as a tombstone so every device
/// converges on the deletion, rather than deleting the blob out from under a peer
/// that hasn't pulled the row removal yet.
///
/// A failed tombstone attempt leaves the outbox row queued (the row is removed only
/// after the tombstone is present), records the attempt, and fails the drain so the
/// caller retries the whole operation. Returns the number of tombstones written.
/// `pending_rotation` refuses every write the same way while this device has not
/// adopted a store-key rotation the cloud has already committed — the rows stay
/// queued, retried once adoption clears the marker.
///
/// The write is idempotent on the tombstone's existence, not its contents: if a
/// tombstone already exists for the key it is left untouched and only the row is
/// removed. The grace is measured from the tombstone's `deleted_at`, so rewriting
/// it with a fresh `now` would push the reclaim deadline forward every time the
/// row re-drains (which happens whenever the row-removal failed last cycle) — a
/// blob could then never age out. Preserving the original tombstone holds the
/// grace deadline fixed from the first drain.
pub async fn drain_tombstones(
    db: &Database,
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    store_id: &str,
    keypair: &UserKeypair,
    clock: &dyn crate::clock::Clock,
) -> Result<usize, String> {
    Box::pin(drain_tombstones_inner(
        db,
        cloud_home,
        cipher,
        pending_rotation,
        store_id,
        keypair,
        clock,
    ))
    .await
}

async fn drain_tombstones_inner(
    db: &Database,
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    store_id: &str,
    keypair: &UserKeypair,
    clock: &dyn crate::clock::Clock,
) -> Result<usize, String> {
    let deletes = db
        .get_pending_cloud_deletes()
        .await
        .map_err(|e| format!("Failed to get pending deletes: {e}"))?;

    let now = clock.now();
    let now_rfc = now.to_rfc3339();
    let suffix = cipher.snapshot().suffix();
    let mut count = 0;
    for entry in deletes {
        let OutboxOperation::Delete { stored } = &entry.operation else {
            return Err(format!(
                "pending delete query returned non-delete outbox entry {}",
                entry.id
            ));
        };
        let cloud_key = stored_cloud_key(stored);
        if crate::blob::retry::entry_in_backoff(&entry, now) {
            continue;
        }

        let key = tombstone_key(stored, suffix);

        // Write only when the slot is absent or invalid. A valid tombstone already
        // at the key carries the original `deleted_at` that the grace is measured
        // from; overwriting it with a fresh `now` would reset the grace, so a row
        // that re-drains (its prior row-removal failed) must not move the deadline.
        match existing_tombstone_state(cloud_home, cipher, store_id, &key, stored).await {
            Ok(ExistingTombstone::Valid) => {
                debug!(
                    %cloud_key,
                    "tombstone already exists; preserving its deleted_at (not resetting the grace)"
                );
            }
            Ok(ExistingTombstone::Absent) => {
                if let Err(e) = write_signed_tombstone(
                    cloud_home,
                    cipher,
                    pending_rotation,
                    store_id,
                    &key,
                    stored,
                    &now_rfc,
                    keypair,
                )
                .await
                {
                    record_outbox_failure(db, &entry, cloud_key, &e, &now_rfc).await?;
                    return Err(format!("Tombstone write failed for {cloud_key}: {e}"));
                }
                count += 1;
            }
            Ok(ExistingTombstone::Invalid(reason)) => {
                warn!(
                    %cloud_key,
                    reason = %reason,
                    "replacing invalid tombstone object"
                );
                if let Err(e) = write_signed_tombstone(
                    cloud_home,
                    cipher,
                    pending_rotation,
                    store_id,
                    &key,
                    stored,
                    &now_rfc,
                    keypair,
                )
                .await
                {
                    record_outbox_failure(db, &entry, cloud_key, &e, &now_rfc).await?;
                    return Err(format!("Tombstone write failed for {cloud_key}: {e}"));
                }
                count += 1;
            }
            Err(e) => {
                let msg = format!("tombstone validation failed: {e}");
                record_outbox_failure(db, &entry, cloud_key, &msg, &now_rfc).await?;
                return Err(format!(
                    "Failed to validate an existing tombstone for {cloud_key}: {e}"
                ));
            }
        }

        // The tombstone is present (written now or already there); drop the local
        // intent row. If this remove fails the row stays and the next drain finds
        // the tombstone already present, so it removes the row without touching the
        // tombstone — the deletion is never lost and the grace never moves.
        db.remove_cloud_outbox_entry(&entry)
            .await
            .map_err(|error| {
                format!("Failed to remove delete outbox entry {}: {error}", entry.id)
            })?;
    }

    Ok(count)
}

async fn record_outbox_failure(
    db: &Database,
    entry: &OutboxEntry,
    cloud_key: &str,
    error: &str,
    attempted_at: &str,
) -> Result<(), String> {
    if let Err(record_error) = db
        .record_cloud_outbox_failure(entry, error, attempted_at)
        .await
    {
        return Err(format!(
            "Failed to record delete failure for {cloud_key} (entry {}): {record_error}",
            entry.id
        ));
    }
    Ok(())
}

/// Garbage-collect tombstones: delete each blob whose authentic tombstone has aged
/// past `grace`, then delete the tombstone. This is where the actual blob deletion
/// happens. `grace` is the host's convergence window (defaulting to
/// [`BLOB_TOMBSTONE_GRACE`]); the reader evaluates it against each tombstone's
/// authentic `deleted_at`, so members with different settings simply GC on their
/// own schedules and the earliest-configured one erases first.
///
/// For each tombstone under [`TOMBSTONE_PREFIX`], in order:
/// 1. Open it under the cipher and parse it. An object we can't open or parse is a
///    foreign store's (a shared bucket) or corrupt — skip it.
/// 2. Verify its signature under *this* store's id (binds the author, exact stored
///    object, deletion time, and store) and that the exact-object hash matches the
///    tombstone slot. Fail → skip, never act.
/// 3. Authorize the author against the membership chain, anchored to the device's
///    *pinned owner* — a current write-capable member of the chain founded by the
///    pinned owner (the same bar the snapshot restore path enforces). A non-member
///    tombstone, or one authored by the forged founder of a wiped/refounded chain,
///    is skipped, so a bucket writer can't forge a deletion.
/// 4. Age it by the *authentic* `deleted_at`. Inside the grace → leave it; a peer
///    may still be converging on the deletion, and a peer whose db still reads the
///    referencing row live+remote may simply not have pulled the retraction yet, so
///    a within-grace tombstone is never canceled on that basis. Past the grace → if
///    a live row still references the blob and resolves as remote, cancel the
///    tombstone (a re-reference outlived the deletion); if that row's locality can't
///    be resolved, skip and surface it. Otherwise re-check the tombstone still
///    exists, confirm the blob
///    is actually present (a tombstone whose blob is already gone is a leftover from
///    a pass that deleted the blob but failed to delete the tombstone — clean it up
///    without counting a reclaim), then delete the blob and the tombstone object.
///
/// The delete names the exact provider object signed by the tombstone. Another
/// upload at the same logical key receives a different immutable slot and cannot
/// be erased by this GC pass.
///
/// Authorization judges against `membership_chain`, the cycle's once-loaded chain
/// already anchored to the owner pinned on join/restore/found — not
/// trust-on-first-use: this GC runs in production and deletes user blobs, so it
/// must refuse a wiped-and-refounded chain exactly like the snapshot restore path.
/// The signature has already proven *who* authored the tombstone; the owner-anchored
/// chain proves they *may* delete. Tombstone collection is an initialized Store
/// operation, so its authorized membership chain is required.
///
/// Returns the number of blobs deleted this pass. Provider and local-state failures
/// fail the pass; invalid or unauthorized bucket objects remain non-actionable.
pub async fn gc_tombstones(
    db: &Database,
    cloud_home: &dyn CloudHome,
    storage: &dyn SyncStorage,
    cipher: &dyn CloudCipherAccess,
    store_id: &str,
    self_pubkey: &str,
    activated_uploaders: &std::collections::BTreeMap<
        crate::sync::store_commit::StoreDeviceRegistrationRef,
        crate::sync::store_commit::StoreDeviceRegistration,
    >,
    membership_chain: &MembershipChain,
    clock: &dyn crate::clock::Clock,
    grace: chrono::Duration,
) -> Result<usize, String> {
    let suffix = cipher.snapshot().suffix();
    let keys = cloud_home
        .list(TOMBSTONE_PREFIX)
        .await
        .map_err(|e| format!("Failed to list tombstones: {e}"))?;

    // A member physically deletes only blobs under its own `{namespace}/{self}/`
    // prefix; an owner additionally sweeps every other member's prefix (owners
    // retain bucket-wide delete, which a provider ACL can grant). This is what lets
    // the ACL express "members write/delete their own prefix, owners anywhere".
    let is_owner = membership_chain.is_owner_now(self_pubkey);

    let now = clock.now();
    let mut deleted = 0;
    for key in keys {
        // Recover the signed exact-object identity from the tombstone slot. A key
        // that doesn't fit this store's canonical layout is not actionable.
        let key_object_id = match key
            .strip_suffix(suffix)
            .and_then(|k| k.strip_prefix(TOMBSTONE_PREFIX))
            .and_then(|encoded| encoded.parse().ok())
        {
            Some(object_id) => object_id,
            None => {
                debug!("skipping tombstone key with unexpected format: {key}");
                continue;
            }
        };

        let stored = match cloud_home.read(&key).await {
            Ok(s) => s,
            Err(e) => return Err(format!("Failed to read tombstone {key}: {e}")),
        };
        let aad_context = crate::sync::cloud_storage::cloud_aad_context(store_id, &key);
        let decoded = match cipher.snapshot().open(stored, &aad_context) {
            Ok(d) => d,
            Err(e) => {
                // A tombstone we can't decrypt is a foreign store's object in a
                // shared bucket — skip it rather than abort the whole GC pass.
                debug!("skipping tombstone {key} this store cannot decrypt: {e}");
                continue;
            }
        };
        let tombstone: BlobTombstoneJson = match serde_json::from_slice(&decoded) {
            Ok(t) => t,
            Err(e) => {
                warn!("skipping unparseable tombstone {key}: {e}");
                continue;
            }
        };

        // Cross-check that the exact object signed inside the tombstone hashes to
        // the identity encoded in the tombstone slot.
        let tombstone_cloud_key = stored_cloud_key(&tombstone.stored);
        let tombstone_object_id = tombstone_object_id(&tombstone.stored);
        if tombstone_object_id != key_object_id {
            warn!(
                "skipping tombstone {key}: signed object {tombstone_object_id} does not match its slot"
            );
            continue;
        }

        // Verify the signature (binds author, exact stored object, deleted_at, and this
        // store). A tombstone that fails is forged, corrupt, tampered, or a
        // different store's — skip it, the blob survives.
        if !tombstone.verify(store_id) {
            warn!("skipping tombstone {key} with an invalid signature");
            continue;
        }

        // Authorize the author against the cycle's once-loaded membership chain,
        // already anchored to the pinned owner: only a current write-capable member
        // of the pinned-owner-founded chain may delete a blob. A non-member tombstone
        // (a bucket writer forging a deletion), or one authored by the forged founder
        // of a wiped/refounded chain, fails here and is skipped.
        if !membership_chain.can_write_now(&tombstone.author_pubkey) {
            warn!(
                "skipping tombstone {key}: author {} is not a current write-capable member",
                tombstone.author_pubkey
            );
            continue;
        }

        // Age it by the authenticated deletion time.
        let deleted_at = match chrono::DateTime::parse_from_rfc3339(&tombstone.deleted_at) {
            Ok(dt) => dt.with_timezone(&chrono::Utc),
            Err(e) => {
                // A verified tombstone with an unparseable timestamp can't be aged.
                // Keep it (never delete on an unageable tombstone) and surface it.
                warn!(
                    "skipping tombstone {key} with unparseable deleted_at {:?}: {e}",
                    tombstone.deleted_at
                );
                continue;
            }
        };
        if now.signed_duration_since(deleted_at) <= grace {
            // Still inside the convergence window: a peer may not have pulled the row
            // removal yet, so the blob must stay readable. The live-row cancel below
            // is deliberately gated behind this check: a peer whose db still reads the
            // row live+remote here may simply not have pulled the retraction yet, so a
            // within-grace tombstone is never canceled on that basis — canceling it
            // would strand the cloud blob once the writer's outbox row is gone. A later
            // pass reclaims it (or cancels, if a live row genuinely re-references the
            // blob) once the grace has passed. A legitimate skip, surfaced so an
            // operator can see why a tombstoned blob is still present.
            debug!(
                tombstone = %key,
                deleted_at = %tombstone.deleted_at,
                "skipping tombstone still inside the grace",
            );
            continue;
        }

        // Past grace. Only now does the live-row check run: cancel the tombstone if a
        // row still references its blob and resolves as remote. By the time grace has
        // expired, a peer that reads the row as live+remote has either not yet pulled
        // the retraction (the row then resolves gone or local and reclaim proceeds) or
        // genuinely observes a re-referencing row (canceling the deletion is correct).
        let gates = db.gates();
        let tables = db.synced_tables().to_vec();
        let exact_stored = tombstone.stored.clone();
        let row_reference = db
            .call(move |conn| {
                Database::stored_blob_reference_state_on(conn, &gates, &tables, &exact_stored)
            })
            .await
            .map_err(|e| format!("Failed to check live blob references: {e}"))?;
        match row_reference {
            StoredBlobReferenceState::LiveRemote => {
                cloud_home
                    .delete(&key)
                    .await
                    .map_err(|e| format!("Failed to cancel stale tombstone {key}: {e}"))?;
                debug!(
                    cloud_key = %tombstone_cloud_key,
                    "canceled tombstone because a live row still references its blob",
                );
                continue;
            }
            StoredBlobReferenceState::Unresolved => {
                return Err(format!(
                    "tombstone {key} has a live blob reference whose locality is unresolved"
                ));
            }
            StoredBlobReferenceState::NotLiveRemote => {}
        }

        // The exact locator names the activated device registration that uploaded
        // this object. Its author, or a current owner, may reclaim it.
        let uploader = activated_uploaders
            .get(tombstone.stored.locator().uploader())
            .ok_or_else(|| {
                format!(
                    "tombstone {key} names an unactivated blob uploader {}",
                    tombstone.stored.locator().uploader().device_id
                )
            })?;
        if uploader.author_pubkey != self_pubkey && !is_owner {
            debug!(
                tombstone = %key,
                uploader = %uploader.author_pubkey,
                "skipping reclaim of an object uploaded by another member",
            );
            continue;
        }

        // Re-check the tombstone still exists before reclaiming. Another GC worker
        // may have completed this exact deletion after our listing.
        match cloud_home.exists(&key).await {
            Ok(true) => {}
            Ok(false) => {
                debug!("tombstone {key} disappeared before reclaim; skipping");
                continue;
            }
            Err(e) => {
                return Err(format!(
                    "Failed to re-check tombstone {key} before reclaim: {e}"
                ))
            }
        }

        // Confirm the blob is actually present before deleting, so a leftover
        // tombstone whose blob a prior pass already reclaimed is cleaned up without
        // counting a phantom second reclaim.
        let blob_present = match storage.verify_blob_object(&tombstone.stored).await {
            Ok(()) => true,
            Err(StorageError::NotFound(_)) => false,
            Err(e) => {
                return Err(format!(
                    "Failed to check blob presence for {} before reclaim: {e}",
                    tombstone_cloud_key
                ))
            }
        };
        if blob_present {
            // Delete only the exact immutable object signed by the tombstone.
            storage
                .delete_blob_object(&tombstone.stored)
                .await
                .map_err(|e| {
                    format!(
                        "Failed to delete blob {} past the grace: {e}",
                        tombstone_cloud_key
                    )
                })?;
            deleted += 1;
            debug!(
                cloud_key = %tombstone_cloud_key,
                "reclaimed blob past the tombstone grace",
            );
        } else {
            debug!(
                cloud_key = %tombstone_cloud_key,
                "tombstone's blob already gone; cleaning up the leftover tombstone",
            );
        }

        // The blob is gone (deleted now or already absent); removing the durable
        // tombstone completes this idempotent reclaim operation.
        cloud_home
            .delete(&key)
            .await
            .map_err(|e| format!("Failed to delete tombstone {key} after reclaim: {e}"))?;
    }

    Ok(deleted)
}
