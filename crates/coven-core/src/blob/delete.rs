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
//! ## Deletion names one immutable generation
//!
//! Every new upload occupies a generated key that is never reused. A tombstone
//! therefore condemns one immutable object generation, and a later upload of the
//! same logical blob uses a different key. GC can race that upload without being
//! able to delete it. Pending upload/delete rows for the exact same key still
//! replace one another within one device, which only affects retries of the same
//! durable operation.

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::database::Database;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{no_progress, CloudHome};
use crate::sync::cloud_storage::{CloudCipherAccess, PendingRotation};
use crate::sync::membership::MembershipChain;
use crate::sync::membership_ops::{
    authorize_loaded_membership_author, MembershipAuthorRequirement,
};

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

/// The cloud key-prefix under which tombstones live. A tombstone for blob
/// `cloud_key` is stored at `blob_tombstones/{cloud_key}{suffix}` (suffix being the
/// cipher's: `.enc` encrypted, empty plaintext). Blob keys carry no suffix of their
/// own (unlike heads/snapshot/membership objects), so stripping the cipher suffix
/// and this prefix recovers the exact `cloud_key`.
const TOMBSTONE_PREFIX: &str = "blob_tombstones/";

/// The cloud object key a tombstone for `cloud_key` is stored at, under the
/// cipher's `suffix`.
fn tombstone_key(cloud_key: &str, suffix: &str) -> String {
    format!("{TOMBSTONE_PREFIX}{cloud_key}{suffix}")
}

/// The uploader segment of the current hashed generated layout
/// `{namespace}/{uploader}/.coven-generations/{ab}/{cd}/{id}/{generation}`, used to scope
/// reclaim to a member prefix. Plain generated keys return `None` because their
/// uploader segment is provenance inside a browsable layout, not a member ACL
/// prefix. The parser accepts only the current hashed layout.
fn blob_key_uploader(cloud_key: &str) -> Option<String> {
    crate::store_dir::StoreDir::parse_generated_blob_key(cloud_key)
        .map(|(_namespace, uploader, _id, _generation)| uploader)
}

#[derive(Debug, PartialEq, Eq)]
enum ExistingTombstone {
    Valid,
    Absent,
    Invalid(String),
}

/// Serialized form of a `blob_tombstones/{cloud_key}{suffix}` object: the durable,
/// signed record that a blob was deleted, plus when, so a GC pass can reclaim the
/// blob once the convergence grace has passed.
///
/// `author_pubkey`/`signature` cover the [`BlobTombstoneFields`] canonical payload
/// — including `cloud_key` (the slot the tombstone lives under, so a valid
/// tombstone can't be relocated to delete a different blob, mirroring how
/// [`crate::sync::signed_control`]'s `HeadJson` binds its `device_id`) and
/// `deleted_at` (so the age can't be forged to dodge or shorten the grace). The
/// GC verifies this signature and authorizes the author against the membership
/// chain before deleting anything.
///
/// `store_id` is part of the signed payload but not stored: the reader supplies
/// its own store id to [`Self::verify`], mirroring the snapshot meta/pointer.
/// A member of two stores cannot take one store's tombstone and replay it as
/// the other's — re-verifying under the second store's id fails, because the
/// signature was taken over the first's.
///
/// `author_pubkey` *is* stored (like `HeadJson` / the snapshot meta): a tombstone's
/// author varies device to device, so the verifier learns who signed it and then
/// checks that author against the chain (the authorization step).
#[derive(Serialize, Deserialize)]
pub struct BlobTombstoneJson {
    /// The cloud object key of the deleted blob. The slot this tombstone is stored
    /// under, and what the GC deletes once the grace has passed.
    pub cloud_key: String,
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
/// mirroring the snapshot payloads) and `cloud_key` (the slot, so a valid
/// tombstone can't be relocated to delete another blob).
#[derive(Serialize)]
struct BlobTombstoneFields<'a> {
    store_id: &'a str,
    cloud_key: &'a str,
    deleted_at: &'a str,
}

impl BlobTombstoneJson {
    /// Build a tombstone for `cloud_key` in `store_id` signed by `keypair`:
    /// fills `author_pubkey` with the device's public key and `signature` with the
    /// detached signature over the canonical payload (which binds `store_id`,
    /// the `cloud_key` slot, and the deletion time). `store_id` is bound but not
    /// stored — the reader passes its own to [`Self::verify`].
    pub fn signed(
        store_id: &str,
        cloud_key: String,
        deleted_at: String,
        keypair: &UserKeypair,
    ) -> Self {
        let payload = tombstone_signing_payload(store_id, &cloud_key, &deleted_at);
        let sig = keypair.sign(&payload);
        BlobTombstoneJson {
            cloud_key,
            deleted_at,
            author_pubkey: hex::encode(keypair.public_key()),
            signature: hex::encode(sig),
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`, bound to
    /// `store_id`. A tombstone that fails this is forged, corrupt, tampered (its
    /// `cloud_key` or `deleted_at` changed after signing), or a different store's
    /// tombstone replayed here, and must not be acted on. Whether the author is
    /// *authorized* (a current write-capable member) is a separate check the GC
    /// runs after this.
    pub fn verify(&self, store_id: &str) -> bool {
        let payload = tombstone_signing_payload(store_id, &self.cloud_key, &self.deleted_at);
        keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
    }
}

fn tombstone_signing_payload(store_id: &str, cloud_key: &str, deleted_at: &str) -> Vec<u8> {
    let fields = BlobTombstoneFields {
        store_id,
        cloud_key,
        deleted_at,
    };
    serde_json::to_vec(&fields).expect("tombstone fields serialization cannot fail")
}

async fn existing_tombstone_state(
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    store_id: &str,
    key: &str,
    expected_cloud_key: &str,
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
    if tombstone.cloud_key != expected_cloud_key {
        return Ok(ExistingTombstone::Invalid(format!(
            "signed cloud_key {:?} does not match {expected_cloud_key:?}",
            tombstone.cloud_key
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
    cloud_key: &str,
    deleted_at: &str,
    keypair: &UserKeypair,
) -> Result<(), String> {
    let tombstone = BlobTombstoneJson::signed(
        store_id,
        cloud_key.to_string(),
        deleted_at.to_string(),
        keypair,
    );
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
/// after the tombstone is present), with a per-row backoff before the next retry —
/// the deletion is not lost. Per-row failures are logged and skipped so one bad row
/// doesn't block the rest. Returns the number of tombstones written this pass.
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
    let deletes = db
        .get_pending_cloud_deletes()
        .await
        .map_err(|e| format!("Failed to get pending deletes: {e}"))?;

    let now = clock.now();
    let now_rfc = now.to_rfc3339();
    let suffix = cipher.snapshot().suffix();
    let mut count = 0;
    for entry in deletes {
        if let Some(last) = entry.last_attempt_at.as_deref() {
            match chrono::DateTime::parse_from_rfc3339(last) {
                Ok(last_dt) => {
                    let elapsed = now.signed_duration_since(last_dt.with_timezone(&chrono::Utc));
                    if elapsed < crate::blob::upload::backoff_window(entry.attempt_count) {
                        continue;
                    }
                }
                Err(e) => {
                    // Don't strand a deletion on a corrupt timestamp; log and retry.
                    warn!(
                        "Delete outbox entry {} has unparseable last_attempt_at {last:?}: {e}; retrying",
                        entry.id
                    );
                }
            }
        }

        let key = tombstone_key(&entry.cloud_key, suffix);

        // Write only when the slot is absent or invalid. A valid tombstone already
        // at the key carries the original `deleted_at` that the grace is measured
        // from; overwriting it with a fresh `now` would reset the grace, so a row
        // that re-drains (its prior row-removal failed) must not move the deadline.
        match existing_tombstone_state(cloud_home, cipher, store_id, &key, &entry.cloud_key).await {
            Ok(ExistingTombstone::Valid) => {
                debug!(
                    cloud_key = %entry.cloud_key,
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
                    &entry.cloud_key,
                    &now_rfc,
                    keypair,
                )
                .await
                {
                    warn!("Tombstone write failed for {}: {e}", entry.cloud_key);
                    record_delete_failure(db, entry.id, &entry.cloud_key, &e, &now_rfc).await;
                    continue;
                }
                count += 1;
            }
            Ok(ExistingTombstone::Invalid(reason)) => {
                warn!(
                    cloud_key = %entry.cloud_key,
                    reason = %reason,
                    "replacing invalid tombstone object"
                );
                if let Err(e) = write_signed_tombstone(
                    cloud_home,
                    cipher,
                    pending_rotation,
                    store_id,
                    &key,
                    &entry.cloud_key,
                    &now_rfc,
                    keypair,
                )
                .await
                {
                    warn!("Tombstone write failed for {}: {e}", entry.cloud_key);
                    record_delete_failure(db, entry.id, &entry.cloud_key, &e, &now_rfc).await;
                    continue;
                }
                count += 1;
            }
            Err(e) => {
                let msg = format!("tombstone validation failed: {e}");
                warn!(
                    "Failed to validate an existing tombstone for {}: {e}",
                    entry.cloud_key
                );
                record_delete_failure(db, entry.id, &entry.cloud_key, &msg, &now_rfc).await;
                continue;
            }
        }

        // The tombstone is present (written now or already there); drop the local
        // intent row. If this remove fails the row stays and the next drain finds
        // the tombstone already present, so it removes the row without touching the
        // tombstone — the deletion is never lost and the grace never moves.
        if let Err(e) = db.remove_cloud_outbox_entry(entry.id).await {
            warn!("Failed to remove outbox entry {}: {e}", entry.id);
        }
    }

    Ok(count)
}

/// Await an operation-scoped outbox failure-recording call and warn if the
/// recording itself fails.
async fn record_outbox_failure(
    record: impl std::future::Future<Output = Result<(), crate::database::DbError>>,
    kind: &str,
    cloud_key: &str,
    entry_id: i64,
) {
    if let Err(e) = record.await {
        warn!("Failed to record {kind} failure for {cloud_key} (entry {entry_id}): {e}");
    }
}

async fn record_delete_failure(
    db: &Database,
    entry_id: i64,
    cloud_key: &str,
    error: &str,
    attempted_at: &str,
) {
    record_outbox_failure(
        db.record_cloud_delete_failure(entry_id, error, attempted_at),
        "delete",
        cloud_key,
        entry_id,
    )
    .await;
}

/// GC's live-row check for a tombstoned blob's `cloud_key`: whether a live row
/// still references it and resolves as remote, the only state that cancels a
/// past-grace tombstone.
enum RowReference {
    /// No live row resolves the blob as remote — either nothing references it, or a
    /// referencing row resolves as local-only. A past-grace reclaim proceeds.
    NotLiveRemote,
    /// A live row references the blob and resolves as remote: cancel the tombstone.
    LiveRemote,
    /// A live row references the blob but its locality can't be resolved — the FK
    /// chain reaches no gated/remote terminus, or a row along it is missing. Neither
    /// cancel nor reclaim; skip and surface it.
    Unresolved,
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
/// 2. Verify its signature under *this* store's id (binds the author, the
///    `cloud_key` slot, the deletion time, and the store — a forged, tampered,
///    relocated, or cross-store tombstone fails) and that the signed `cloud_key`
///    matches the key the object is stored under. Fail → skip, never act.
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
///    exists (it may have been canceled because a live row re-referenced it), confirm the blob
///    is actually present (a tombstone whose blob is already gone is a leftover from
///    a pass that deleted the blob but failed to delete the tombstone — clean it up
///    without counting a reclaim), then delete the blob and the tombstone object.
///
/// The blob delete is a plain, unconditional delete on every provider. Its key
/// names one immutable generation, so a concurrent later upload cannot occupy the
/// object GC is deleting.
///
/// Authorization judges against `membership_chain`, the cycle's once-loaded chain
/// already anchored to the owner pinned on join/restore/found — not
/// trust-on-first-use: this GC runs in production and deletes user blobs, so it
/// must refuse a wiped-and-refounded chain exactly like the snapshot restore path.
/// The signature has already proven *who* authored the tombstone; the owner-anchored
/// chain proves they *may* delete. `None` is reserved for pre-initialization
/// callers; every initialized store has a membership chain, regardless of its
/// storage representation.
///
/// Returns the number of blobs deleted this pass. A per-tombstone error is logged
/// and skipped so one bad object doesn't block reclaiming the rest.
pub async fn gc_tombstones(
    db: &Database,
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    store_id: &str,
    self_pubkey: &str,
    membership_chain: Option<&MembershipChain>,
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
    let is_owner = membership_chain.is_some_and(|chain| chain.is_owner_now(self_pubkey));

    let now = clock.now();
    let mut deleted = 0;
    for key in keys {
        // Recover the blob's cloud_key from the object key: strip the cipher suffix
        // and the prefix. A key that doesn't fit the layout isn't ours — skip it.
        let key_cloud_key = match key
            .strip_suffix(suffix)
            .and_then(|k| k.strip_prefix(TOMBSTONE_PREFIX))
        {
            Some(k) => k.to_string(),
            None => {
                debug!("skipping tombstone key with unexpected format: {key}");
                continue;
            }
        };
        let stored = match cloud_home.read(&key).await {
            Ok(s) => s,
            Err(e) => {
                // A tombstone listed but not readable (a race with its own deletion,
                // or a transient error): skip this pass, retry next cycle.
                warn!("Failed to read tombstone {key}: {e}");
                continue;
            }
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

        // The signature binds the cloud_key, so a verified tombstone already names
        // the blob it authorizes deleting. Cross-check it matches the slot the
        // object is stored under: a mismatch is an object relocated to a foreign
        // key and must not act on the slot it was moved into.
        if tombstone.cloud_key != key_cloud_key {
            warn!(
                "skipping tombstone {key}: signed cloud_key {:?} does not match its slot",
                tombstone.cloud_key
            );
            continue;
        }

        // Verify the signature (binds author, cloud_key, deleted_at, and this
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
        // of a wiped/refounded chain, fails here and is skipped. Only a
        // pre-initialization caller can supply no chain and rely on the verified
        // signature alone.
        match authorize_loaded_membership_author(
            membership_chain,
            &tombstone.author_pubkey,
            MembershipAuthorRequirement::WriteCapable,
        ) {
            Ok(()) => {}
            Err(e) => {
                warn!("skipping tombstone {key}: {e}");
                continue;
            }
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
        let decls = db.blob_decls();
        let gates = db.gates();
        let cloud_key = tombstone.cloud_key.clone();
        let row_reference = db
            .call(move |conn| {
                let Some((table, pk)) = decls
                    .row_for_blob_cloud_key(conn, &cloud_key)
                    .map_err(|e| crate::database::DbError(e.to_string()))?
                else {
                    return Ok(RowReference::NotLiveRemote);
                };
                let Some(blob) = decls
                    .ref_for_row(conn, &table, &pk)
                    .map_err(|e| crate::database::DbError(e.to_string()))?
                else {
                    return Ok(RowReference::NotLiveRemote);
                };
                let recorded_location =
                    crate::database::Database::blob_location_on(conn, &blob.namespace, &blob.id)?;
                let parsed_hashed =
                    crate::store_dir::StoreDir::parse_generated_blob_key(&cloud_key);
                let scheme = if parsed_hashed.is_some() {
                    crate::sync::cloud_storage::BlobPathScheme::Hashed
                } else {
                    crate::sync::cloud_storage::BlobPathScheme::Plain
                };
                let location = recorded_location.ok_or_else(|| {
                    crate::database::DbError(format!(
                        "live remote blob {}/{} has no cloud location",
                        blob.namespace, blob.id
                    ))
                })?;
                let exact_key = crate::sync::cloud_storage::CloudSyncStorage::blob_key(
                    scheme,
                    &blob.namespace,
                    &location,
                    &blob.id,
                    blob.cloud_path.as_deref(),
                )
                .map_err(|e| crate::database::DbError(e.to_string()))?;
                if exact_key != cloud_key {
                    return Ok(RowReference::NotLiveRemote);
                }
                let reference = match gates
                    .root_kept_of(conn, &table, &pk)
                    .map_err(|e| crate::database::DbError(e.to_string()))?
                {
                    Some(true) => RowReference::LiveRemote,
                    Some(false) => RowReference::NotLiveRemote,
                    None => RowReference::Unresolved,
                };
                Ok(reference)
            })
            .await
            .map_err(|e| format!("Failed to check live blob references: {e}"))?;
        match row_reference {
            RowReference::LiveRemote => {
                if let Err(e) = cloud_home.delete(&key).await {
                    warn!("Failed to cancel stale tombstone {key} for live blob: {e}");
                    continue;
                }
                debug!(
                    cloud_key = %tombstone.cloud_key,
                    "canceled tombstone because a live row still references its blob",
                );
                continue;
            }
            RowReference::Unresolved => {
                // The FK chain reaches no locality terminus, or a row along it is
                // missing: neither cancel nor reclaim on locality the pass cannot
                // resolve. Keep the tombstone and surface it for a later pass.
                warn!(
                    "skipping tombstone {key}: a live row references its blob but its \
                     locality is unresolved",
                );
                continue;
            }
            RowReference::NotLiveRemote => {}
        }

        // Per-prefix reclaim. The blob may be deleted only from this device's own
        // prefix, unless this device is a current owner (which sweeps absent
        // members' orphans). A blob under another member's prefix is left standing
        // — its own GC, or an owner sweep, reclaims it — so the tombstone stays too,
        // for whichever party can act. A plain-scheme key has no member ACL prefix,
        // so this hashed-prefix authorization does not gate it.
        if let Some(uploader) = blob_key_uploader(&tombstone.cloud_key) {
            if uploader != self_pubkey && !is_owner {
                debug!(
                    tombstone = %key,
                    %uploader,
                    "skipping reclaim of a blob under another member's prefix; \
                     awaits its uploader or an owner sweep",
                );
                continue;
            }
        }

        // Re-check the tombstone still exists before reclaiming. Generated blob
        // locations are immutable: a later upload of the same blob id uses another
        // key, so this tombstone can target only the generation it names.
        match cloud_home.exists(&key).await {
            Ok(true) => {}
            Ok(false) => {
                debug!("tombstone {key} was removed before reclaim; skipping");
                continue;
            }
            Err(e) => {
                // Couldn't confirm the tombstone is still present: don't risk
                // deleting a possibly re-uploaded blob. Retry next pass.
                warn!("Failed to re-check tombstone {key} before reclaim: {e}");
                continue;
            }
        }

        // Confirm the blob is actually present before deleting, so a leftover
        // tombstone whose blob a prior pass already reclaimed is cleaned up without
        // counting a phantom second reclaim.
        let blob_present = match cloud_home.exists(&tombstone.cloud_key).await {
            Ok(present) => present,
            Err(e) => {
                warn!(
                    "Failed to check blob presence for {} before reclaim: {e}",
                    tombstone.cloud_key
                );
                continue;
            }
        };
        if blob_present {
            // The key names one immutable upload generation, so this delete cannot
            // target a later upload of the same blob id.
            if let Err(e) = cloud_home.delete(&tombstone.cloud_key).await {
                warn!(
                    "Failed to delete blob {} past the grace: {e}",
                    tombstone.cloud_key
                );
                continue;
            }
            deleted += 1;
            debug!(
                cloud_key = %tombstone.cloud_key,
                "reclaimed blob past the tombstone grace",
            );
        } else {
            debug!(
                cloud_key = %tombstone.cloud_key,
                "tombstone's blob already gone; cleaning up the leftover tombstone",
            );
        }

        // The blob is gone (deleted just now or already absent); remove the
        // tombstone. A failed delete here is harmless: the blob stays gone, and the
        // next pass finds the blob already absent and re-attempts only this cleanup.
        if let Err(e) = cloud_home.delete(&key).await {
            warn!("Failed to delete tombstone {key} after reclaiming its blob: {e}");
        }
    }

    Ok(deleted)
}
