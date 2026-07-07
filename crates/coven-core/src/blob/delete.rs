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
//! library key is shared by every member), not authorship, so anyone who can write
//! the bucket could otherwise drop a tombstone that deletes a blob they were never
//! authorized to remove. The tombstone is therefore signed by its author (like
//! every other control object — heads, the snapshot meta/pointer), and the GC
//! verifies the signature *and* that the author is a current write-capable member
//! before acting on it. A tombstone that fails either check is skipped, never
//! acted on — the blob survives.
//!
//! ## Upload cancels the deletion
//!
//! A blob re-uploaded after a deletion was queued must not be deleted. This is
//! enforced at two layers: at enqueue ([`crate::database::Database::enqueue_upload`]
//! drops any pending delete row for the same key, and vice versa — latest intent
//! wins within a device), and at upload completion ([`cancel_tombstone`], called
//! from [`crate::blob::upload::drain_uploads`] after a successful write, removes any
//! tombstone a *prior* cycle — possibly on another device — already wrote). A
//! re-upload thus wins over a pending deletion by construction.
//!
//! The completion cancel must not be lost if it fails, or a surviving tombstone
//! past its grace would delete the live re-uploaded blob. When the inline cancel
//! fails, a durable `cancel` outbox row is queued and [`drain_tombstone_cancels`]
//! retries it each cycle until the tombstone is gone, so the cancel survives cloud
//! errors and restarts: once a blob is successfully (re-)uploaded to a key, the
//! tombstone for that key is removed eventually, and the GC never reclaims it.

use serde::{Deserialize, Serialize};
use tracing::{debug, warn};

use crate::database::Database;
use crate::keys::{self, UserKeypair};
use crate::storage::cloud::{no_progress, CloudHome};
use crate::sync::cloud_storage::CloudCipher;
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

/// The uploader segment of a hashed blob cloud key
/// `{namespace}/{uploader}/{ab}/{cd}/{id}`, used to scope reclaim to a prefix, or
/// `None` for a plain-scheme key (`{namespace}/{cloud_path}`), which carries no
/// uploader and no per-member prefix. The rebuild guard confirms the key really is
/// a hashed key rather than a plain path that happens to have five segments.
fn blob_key_uploader(cloud_key: &str) -> Option<String> {
    let mut parts = cloud_key.split('/');
    let namespace = parts.next()?;
    let uploader = parts.next()?;
    let _first = parts.next()?;
    let _second = parts.next()?;
    let id = parts.next()?;
    if parts.next().is_some() {
        return None;
    }
    match crate::library_dir::LibraryDir::uploader_hashed_key(namespace, uploader, id) {
        Ok(rebuilt) if rebuilt == cloud_key => Some(uploader.to_string()),
        _ => None,
    }
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
/// `library_id` is part of the signed payload but not stored: the reader supplies
/// its own library id to [`Self::verify`], mirroring the snapshot meta/pointer.
/// A member of two libraries cannot take one library's tombstone and replay it as
/// the other's — re-verifying under the second library's id fails, because the
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
/// `library_id` (so a tombstone can't be replayed into a different library,
/// mirroring the snapshot payloads) and `cloud_key` (the slot, so a valid
/// tombstone can't be relocated to delete another blob).
#[derive(Serialize)]
struct BlobTombstoneFields<'a> {
    library_id: &'a str,
    cloud_key: &'a str,
    deleted_at: &'a str,
}

impl BlobTombstoneJson {
    /// Build a tombstone for `cloud_key` in `library_id` signed by `keypair`:
    /// fills `author_pubkey` with the device's public key and `signature` with the
    /// detached signature over the canonical payload (which binds `library_id`,
    /// the `cloud_key` slot, and the deletion time). `library_id` is bound but not
    /// stored — the reader passes its own to [`Self::verify`].
    pub fn signed(
        library_id: &str,
        cloud_key: String,
        deleted_at: String,
        keypair: &UserKeypair,
    ) -> Self {
        let payload = tombstone_signing_payload(library_id, &cloud_key, &deleted_at);
        let sig = keypair.sign(&payload);
        BlobTombstoneJson {
            cloud_key,
            deleted_at,
            author_pubkey: hex::encode(keypair.public_key()),
            signature: hex::encode(sig),
        }
    }

    /// Verify the embedded signature against the embedded `author_pubkey`, bound to
    /// `library_id`. A tombstone that fails this is forged, corrupt, tampered (its
    /// `cloud_key` or `deleted_at` changed after signing), or a different library's
    /// tombstone replayed here, and must not be acted on. Whether the author is
    /// *authorized* (a current write-capable member) is a separate check the GC
    /// runs after this.
    pub fn verify(&self, library_id: &str) -> bool {
        let payload = tombstone_signing_payload(library_id, &self.cloud_key, &self.deleted_at);
        keys::verify_signature_hex(&self.author_pubkey, &self.signature, &payload)
    }
}

fn tombstone_signing_payload(library_id: &str, cloud_key: &str, deleted_at: &str) -> Vec<u8> {
    let fields = BlobTombstoneFields {
        library_id,
        cloud_key,
        deleted_at,
    };
    serde_json::to_vec(&fields).expect("tombstone fields serialization cannot fail")
}

async fn existing_tombstone_state(
    cloud_home: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
    library_id: &str,
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
    let aad_context = crate::sync::cloud_storage::cloud_aad_context(library_id, key);
    let decoded = match cipher.read().unwrap().open(stored, &aad_context) {
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
    if !tombstone.verify(library_id) {
        return Ok(ExistingTombstone::Invalid(
            "signature verification failed".to_string(),
        ));
    }
    Ok(ExistingTombstone::Valid)
}

async fn write_signed_tombstone(
    cloud_home: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
    library_id: &str,
    key: &str,
    cloud_key: &str,
    deleted_at: &str,
    keypair: &UserKeypair,
) -> Result<(), String> {
    let tombstone = BlobTombstoneJson::signed(
        library_id,
        cloud_key.to_string(),
        deleted_at.to_string(),
        keypair,
    );
    let bytes = serde_json::to_vec(&tombstone)
        .map_err(|e| format!("tombstone serialization failed: {e}"))?;
    let aad_context = crate::sync::cloud_storage::cloud_aad_context(library_id, key);
    let sealed = cipher.read().unwrap().seal(bytes, &aad_context);
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
    cipher: &std::sync::RwLock<CloudCipher>,
    library_id: &str,
    keypair: &UserKeypair,
    clock: &dyn crate::clock::Clock,
) -> Result<usize, String> {
    let deletes = db
        .get_pending_cloud_deletes()
        .await
        .map_err(|e| format!("Failed to get pending deletes: {e}"))?;

    let now = clock.now();
    let now_rfc = now.to_rfc3339();
    let suffix = cipher.read().unwrap().suffix();
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
        match existing_tombstone_state(cloud_home, cipher, library_id, &key, &entry.cloud_key).await
        {
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
                    library_id,
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
                    library_id,
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

async fn record_delete_failure(
    db: &Database,
    entry_id: i64,
    cloud_key: &str,
    error: &str,
    attempted_at: &str,
) {
    if let Err(e) = db
        .record_cloud_delete_failure(entry_id, error, attempted_at)
        .await
    {
        warn!("Failed to record delete failure for {cloud_key} (entry {entry_id}): {e}");
    }
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
///    foreign library's (a shared bucket) or corrupt — skip it.
/// 2. Verify its signature under *this* library's id (binds the author, the
///    `cloud_key` slot, the deletion time, and the library — a forged, tampered,
///    relocated, or cross-library tombstone fails) and that the signed `cloud_key`
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
///    exists (it may have been canceled by a concurrent re-upload), confirm the blob
///    is actually present (a tombstone whose blob is already gone is a leftover from
///    a pass that deleted the blob but failed to delete the tombstone — clean it up
///    without counting a reclaim), then delete the blob and the tombstone object.
///
/// The blob delete is a plain, unconditional delete on every provider. A blob
/// re-uploaded to a key whose tombstone's grace already expired can therefore be
/// erased by a concurrent GC pass between the re-check and the delete; that loss
/// surfaces as a loud read error on the next read, never silent corruption, and a
/// host avoids it by writing new content at new (content-addressed or versioned)
/// blob keys rather than re-uploading at a deleted key.
///
/// Authorization judges against `membership_chain`, the cycle's once-loaded chain
/// already anchored to the owner pinned on join/restore/found — not
/// trust-on-first-use: this GC runs in production and deletes user blobs, so it
/// must refuse a wiped-and-refounded chain exactly like the snapshot restore path.
/// The signature has already proven *who* authored the tombstone; the owner-anchored
/// chain proves they *may* delete. `None` is the chain-less (browsable) library,
/// which has no membership to gate against.
///
/// Returns the number of blobs deleted this pass. A per-tombstone error is logged
/// and skipped so one bad object doesn't block reclaiming the rest.
pub async fn gc_tombstones(
    db: &Database,
    cloud_home: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
    library_id: &str,
    self_pubkey: &str,
    membership_chain: Option<&MembershipChain>,
    clock: &dyn crate::clock::Clock,
    grace: chrono::Duration,
) -> Result<usize, String> {
    let suffix = cipher.read().unwrap().suffix();
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
        let aad_context = crate::sync::cloud_storage::cloud_aad_context(library_id, &key);
        let decoded = match cipher.read().unwrap().open(stored, &aad_context) {
            Ok(d) => d,
            Err(e) => {
                // A tombstone we can't decrypt is a foreign library's object in a
                // shared bucket — skip it rather than abort the whole GC pass.
                debug!("skipping tombstone {key} this library cannot decrypt: {e}");
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
        // library). A tombstone that fails is forged, corrupt, tampered, or a
        // different library's — skip it, the blob survives.
        if !tombstone.verify(library_id) {
            warn!("skipping tombstone {key} with an invalid signature");
            continue;
        }

        // Authorize the author against the cycle's once-loaded membership chain,
        // already anchored to the pinned owner: only a current write-capable member
        // of the pinned-owner-founded chain may delete a blob. A non-member tombstone
        // (a bucket writer forging a deletion), or one authored by the forged founder
        // of a wiped/refounded chain, fails here and is skipped. A chain-less
        // (browsable) library has nothing to gate against, so the object stands on
        // its verified signature alone.
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
        // for whichever party can act. A plain-scheme key carries no uploader
        // segment (a browsable home has no membership ACL), so it is never gated.
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

        // Re-check the tombstone still exists before reclaiming. A re-upload's
        // cancel (inline or its durable retry) deletes the tombstone once the blob
        // is (re-)written, so a tombstone still present here means no re-upload has
        // completed; a tombstone now gone means one has, and the deletion is no
        // longer current — skip.
        match cloud_home.exists(&key).await {
            Ok(true) => {}
            Ok(false) => {
                debug!("tombstone {key} canceled before reclaim (re-upload won); skipping");
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
            // Plain, unconditional delete on every provider. A re-upload that lands
            // at this key between the tombstone re-check above and this delete is
            // erased here; that loss surfaces as a loud read error on the next read
            // (never silent corruption), and a host avoids it by writing new content
            // at new blob keys rather than re-uploading at a deleted key.
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

/// Cancel any tombstone for `cloud_key`: delete the tombstone object so a GC pass
/// won't reclaim the blob. Called from the upload drain after a blob is
/// successfully (re-)written, so a re-upload from any device cancels a deletion a
/// prior cycle already wrote — a re-upload wins over a pending deletion by
/// construction. The upload drain backs a failed inline call with a durable
/// `cancel` row that [`drain_tombstone_cancels`] retries, so the cancel is not
/// lost on a transient cloud error.
///
/// `suffix` is the cipher's object-key suffix (so the tombstone key matches what
/// [`drain_tombstones`] wrote). Deleting an absent tombstone is a no-op, so the
/// common case (no pending deletion) costs one idempotent delete and nothing else.
pub async fn cancel_tombstone(
    cloud_home: &dyn CloudHome,
    suffix: &str,
    cloud_key: &str,
) -> Result<(), String> {
    let key = tombstone_key(cloud_key, suffix);
    cloud_home
        .delete(&key)
        .await
        .map_err(|e| format!("failed to cancel tombstone for {cloud_key}: {e}"))
}

/// Drain the queued tombstone-cancels: for each, remove the tombstone object for
/// its `cloud_key`, then remove the outbox row. A cancel row is queued by the
/// upload drain only when its inline [`cancel_tombstone`] failed, so this is the
/// retry that makes the cancel durable: once a blob is successfully (re-)uploaded
/// to a key, the tombstone for that key is removed eventually, across cancel
/// failures and restarts, so a later GC never reclaims the live re-uploaded blob.
///
/// A failed cancel leaves the row queued (it is removed only after the tombstone
/// delete succeeds), so the next cycle retries. The delete is a no-op if the
/// tombstone is already gone, so a cancel that races the inline one — or a peer's
/// GC that already reclaimed and removed it — still completes cleanly. Returns the
/// number of cancels completed this pass.
pub async fn drain_tombstone_cancels(
    db: &Database,
    cloud_home: &dyn CloudHome,
    cipher: &std::sync::RwLock<CloudCipher>,
) -> Result<usize, String> {
    let cancels = db
        .get_pending_cloud_cancels()
        .await
        .map_err(|e| format!("Failed to get pending tombstone cancels: {e}"))?;

    let suffix = cipher.read().unwrap().suffix();
    let mut count = 0;
    for entry in cancels {
        if let Err(e) = cancel_tombstone(cloud_home, suffix, &entry.cloud_key).await {
            // Leave the row queued; the cancel must outlive a transient cloud
            // error so the tombstone is removed before the grace passes.
            warn!("Tombstone cancel retry failed for {}: {e}", entry.cloud_key);
            continue;
        }
        if let Err(e) = db.remove_cloud_outbox_entry(entry.id).await {
            // The tombstone is gone; a lingering cancel row just re-deletes an
            // already-absent tombstone next cycle (a no-op), so this is harmless.
            warn!("Failed to remove cancel outbox entry {}: {e}", entry.id);
        }
        count += 1;
    }

    Ok(count)
}
