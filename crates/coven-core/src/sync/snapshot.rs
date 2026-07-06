/// Snapshots and garbage collection for the sync system.
///
/// Periodically, a device creates a full snapshot of the database via
/// `VACUUM INTO`, and publishes it as a generation under its own `{author}` (its
/// hex public key): the DB image
/// (`snapshot/{author}/{seq}.db{suffix}`) and then the signed metadata
/// (`snapshot/{author}/{seq}_meta.json{suffix}`) are written first, then a single
/// signed pointer (`snapshot/current.json{suffix}`) carrying that generation's
/// `{author_pubkey, seq}` is written last. A reader follows the pointer to a
/// complete, self-consistent generation, so there is no window where a new DB
/// image is paired with a stale or missing meta. This lets new devices bootstrap
/// without replaying the entire changeset history, and enables GC of old
/// changesets and superseded generations.
///
/// Each device's generations live under its own `{author}` keyspace, so they are
/// globally unique: `seq` is the publisher's own `local_seq` (not a global id), so
/// two devices can publish at the *same* `seq`, but their objects are distinct
/// keys — a publisher can never overwrite a peer's generation. Reclaiming a
/// superseded generation is therefore owned by its author by construction: a
/// device lists and deletes only objects under its own `{author}` prefix, so it
/// can never delete a generation a peer wrote but has not yet pointed at. Its own
/// generations are safe to reclaim because a device never sweeps concurrently with
/// its own publish: the sweep runs at the end of `push_snapshot`, and the sync
/// loop runs cycles serially on one thread.
///
/// Snapshot creation policy: after every N changesets (default 100) or
/// T hours (default 24) since the last snapshot.
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

use rusqlite::Connection;
use sha2::{Digest, Sha256};
use tracing::{debug, info, warn};

use super::membership::MembershipChain;
use super::membership_ops::{
    authorize_loaded_membership_author, authorize_membership_author, load_membership_chain,
    MembershipAuthorAuthorizationError, MembershipAuthorRequirement,
};
use super::publish_blobs::ensure_publishable_blobs;
use super::session::SyncedTable;
use super::signed_control::{AckJson, SnapshotMetaJson, SnapshotPointerJson};
use super::storage::{StorageError, SyncStorage};
use crate::blob::{BlobRef, Provenance};
use crate::database::Database;
use crate::keys::UserKeypair;

/// Default: create a snapshot after this many changesets since the last one.
const SNAPSHOT_CHANGESET_THRESHOLD: u64 = 100;

/// Default: create a snapshot after this many hours since the last one.
const SNAPSHOT_HOURS_THRESHOLD: u64 = 24;

pub(crate) struct CreatedSnapshot {
    pub db_image: Vec<u8>,
    pub host_blobs: Vec<BlobRef>,
    pub publish_blobs: Vec<BlobRef>,
}

pub struct SnapshotBlobPreflight<'a> {
    pub db: &'a Database,
    pub blobs: &'a [BlobRef],
}

/// Error type for snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("VACUUM INTO failed: {0}")]
    VacuumFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("snapshot control JSON parse failed: {0}")]
    Parse(String),
    #[error("storage error: {0}")]
    Bucket(#[from] StorageError),
    #[error("decryption failed: {0}")]
    Decryption(String),
    /// No synced tables were registered, so we cannot determine which tables
    /// are safe to share. Emitting a snapshot here would either leak every
    /// local-only table or clear the whole DB — both wrong, so we refuse.
    #[error("no synced tables registered; refusing to emit an all-cleared snapshot")]
    NoSyncedTables,
    /// Scoping the snapshot copy down to shareable data failed (sqlite FFI):
    /// either clearing local-only tables or applying the row-level gate that
    /// excludes gated-false subtrees (the changeset gate cuts them too).
    #[error("failed to scope snapshot down to shareable data: {0}")]
    ClearFailed(String),
    /// The snapshot metadata's signature does not verify against its embedded
    /// author. The bucket is untrusted, so an unsigned, forged, or tampered meta
    /// (changed cursors or DB hash) is refused rather than adopted — `meta.cursors`
    /// are control input and the DB image is the whole catalog.
    #[error("snapshot metadata signature does not verify")]
    MetaSignatureInvalid,
    /// The snapshot pointer's signature does not verify against its embedded
    /// author. The pointer names which generation is live, so an unsigned, forged,
    /// or tampered pointer (changed `seq` or `db_hash`) would repoint the snapshot
    /// at a generation its author never published — refused rather than followed.
    #[error("snapshot pointer signature does not verify")]
    PointerSignatureInvalid,
    /// The DB hash the (verified) pointer commits to does not match the DB hash the
    /// (verified) generation metadata commits to. The pointer and the meta describe
    /// different images — a generation was assembled from mismatched objects — so
    /// the generation is refused.
    #[error("snapshot pointer DB hash does not match the generation metadata")]
    PointerMetaMismatch,
    /// The downloaded snapshot DB's hash does not match the hash the (verified)
    /// metadata commits to. The DB image was substituted after the meta was
    /// signed, or the two objects are from different pushes — either way the
    /// catalog is not the one its author signed, so it is refused.
    #[error("snapshot DB hash does not match the signed metadata")]
    DbHashMismatch,
    /// The snapshot's author is not authorized to publish a catalog image: not a
    /// current Owner of the library's membership chain, or the
    /// chain itself is not anchored to the library's owner (a wiped/refounded
    /// chain). The snapshot is refused rather than adopted.
    #[error("snapshot author is not an authorized owner: {0}")]
    UnauthorizedAuthor(String),
    /// The snapshot's synced-schema version is newer than this binary's top
    /// migration, so its DB image carries columns this binary's tables lack. The
    /// generation is refused before its image is downloaded; the same refusal is
    /// the at-open backstop in [`crate::migration::run_migrations`].
    #[error(
        "snapshot schema version {snapshot_version} is newer than this binary supports \
         ({supported}); update the app"
    )]
    SchemaTooNew {
        snapshot_version: u32,
        supported: u32,
    },
    #[error("snapshot blob preflight failed: {0}")]
    PublishBlobs(String),
}

fn prepare_snapshot_path(temp_dir: &Path) -> Result<std::path::PathBuf, SnapshotError> {
    let snapshot_path = temp_dir.join("snapshot.db");
    match std::fs::remove_file(&snapshot_path) {
        Ok(()) => {}
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(e) => return Err(SnapshotError::Io(e)),
    }
    Ok(snapshot_path)
}

fn cleanup_snapshot_path(path: &Path) {
    if let Err(e) = std::fs::remove_file(path) {
        if e.kind() != std::io::ErrorKind::NotFound {
            warn!(error = %e, path = %path.display(), "failed to remove temp snapshot");
        }
    }
}

fn read_and_remove_snapshot(path: &Path) -> Result<Vec<u8>, SnapshotError> {
    let bytes = std::fs::read(path)?;
    cleanup_snapshot_path(path);
    Ok(bytes)
}

fn write_snapshot_db(target_path: &Path, plaintext: &[u8]) -> Result<(), SnapshotError> {
    if let Some(parent) = target_path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(target_path, plaintext)?;
    Ok(())
}

/// SHA-256 of a snapshot DB image, hex-encoded. The hash the signed
/// [`SnapshotMetaJson`] and [`SnapshotPointerJson`] both commit to, so the same
/// bytes that round-trip through a generation's db object are what the signatures
/// bind.
fn snapshot_db_hash(db_image: &[u8]) -> String {
    hex::encode(Sha256::digest(db_image))
}

/// Result of bootstrapping from a snapshot.
#[derive(Debug)]
pub struct BootstrapResult {
    /// Per-device cursors from the snapshot metadata.
    /// The bootstrapping device should use these as initial sync_cursors.
    pub cursors: HashMap<String, u64>,
}

/// Create a snapshot of the database as bytes ready for storage.
///
/// Uses `VACUUM INTO` to create a clean copy of the database at a temp path,
/// then clears every non-synced table's data from that copy, reads the bytes,
/// returns the DB image. The storage layer seals it at
/// `snapshot/{author}/{seq}.db{suffix}`, where the cloud key is known and can be
/// authenticated as AEAD associated data.
///
/// A snapshot is restored byte-for-byte as the joining device's `library.db`
/// (no migration rebuild), so it must carry only data that is eligible to
/// cross devices — the host's declared synced tables. Local-only tables
/// (per-device paths, caches) and per-device sync bookkeeping must not ride
/// along; their schemas are kept, but their rows are deleted from the copy.
///
/// `conn` is the owned live connection; `tables` is the host's synced set.
pub fn create_snapshot(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
) -> Result<Vec<u8>, SnapshotError> {
    create_snapshot_with_host_blobs(conn, temp_dir, tables).map(|snapshot| snapshot.db_image)
}

pub(crate) fn create_snapshot_with_host_blobs(
    conn: &Connection,
    temp_dir: &Path,
    tables: &[SyncedTable],
) -> Result<CreatedSnapshot, SnapshotError> {
    // A snapshot with no synced set would either leak every local-only table or
    // clear the whole DB — both wrong. Refuse before doing any work.
    if tables.is_empty() {
        return Err(SnapshotError::NoSyncedTables);
    }

    let snapshot_path = prepare_snapshot_path(temp_dir)?;
    let path_str = snapshot_path
        .to_str()
        .expect("temp path should be valid UTF-8");

    // VACUUM INTO creates a clean, defragmented copy of the live database.
    if let Err(e) = conn.execute("VACUUM INTO ?1", [path_str]) {
        cleanup_snapshot_path(&snapshot_path);
        return Err(SnapshotError::VacuumFailed(e.to_string()));
    }

    // The copy is a whole-DB byte image, so it still holds every local-only
    // table's data. Strip those before reading: open the copy as its own
    // connection and DELETE from every table outside the synced set.
    if let Err(e) = clear_local_only_tables(&snapshot_path, tables) {
        cleanup_snapshot_path(&snapshot_path);
        return Err(e);
    }

    let publish_blobs = match snapshot_publish_blobs(&snapshot_path, tables) {
        Ok(blobs) => blobs,
        Err(e) => {
            cleanup_snapshot_path(&snapshot_path);
            return Err(e);
        }
    };
    let host_blobs = publish_blobs
        .iter()
        .filter(|blob| blob.provenance == Provenance::HostProvided)
        .cloned()
        .collect();

    // Read the cleared snapshot file. The storage implementation seals it at the
    // final cloud key so the AEAD context can bind that key.
    let plaintext = read_and_remove_snapshot(&snapshot_path)?;
    let plaintext_size = plaintext.len();

    info!(plaintext_size, "created snapshot");

    Ok(CreatedSnapshot {
        db_image: plaintext,
        host_blobs,
        publish_blobs,
    })
}

fn snapshot_publish_blobs(
    path: &Path,
    tables: &[SyncedTable],
) -> Result<Vec<BlobRef>, SnapshotError> {
    let conn = Connection::open(path)
        .map_err(|e| SnapshotError::ClearFailed(format!("failed to open snapshot copy: {e}")))?;
    let decls = crate::blob::decl::BlobDecls::from_tables(&conn, tables)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    let mut seen = HashSet::new();
    decls
        .refs_in_db(&conn)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))
        .map(|refs| {
            refs.into_iter()
                .filter(|blob| seen.insert((blob.namespace.clone(), blob.id.clone())))
                .collect()
        })
}

/// Delete every non-synced table's rows from the snapshot copy at `path`,
/// keeping all table schemas intact.
///
/// Opens `path` as its own connection (the copy must be edited in isolation from
/// the live DB). Errors if any step fails — a snapshot that silently dropped
/// synced data, or silently kept local-only data, is worse than no snapshot.
fn clear_local_only_tables(path: &Path, synced: &[SyncedTable]) -> Result<(), SnapshotError> {
    let conn = Connection::open(path)
        .map_err(|e| SnapshotError::ClearFailed(format!("failed to open snapshot copy: {e}")))?;
    clear_non_synced(&conn, synced)?;
    conn.close()
        .map_err(|(_, e)| SnapshotError::ClearFailed(format!("failed to close snapshot copy: {e}")))
}

/// On the snapshot-copy connection, scope it down to exactly what is eligible to
/// cross devices, then VACUUM to reclaim the freed pages:
///
/// 1. Table-level: DELETE every user table not in `synced` — local-only tables
///    keep their schema, lose their rows.
/// 2. Row-level: within the synced tables, DELETE the rows the gate excludes
///    (gated-false roots and their FK-descendants), so a private subtree does
///    not ride the snapshot to a restoring peer. This is the same exclusion the
///    outbound changeset gate applies; both reuse [`crate::sync::gate::Gates`].
fn clear_non_synced(conn: &Connection, synced: &[SyncedTable]) -> Result<(), SnapshotError> {
    for table in list_user_tables(conn)? {
        if synced.iter().any(|t| t.name() == table) {
            continue;
        }
        conn.execute_batch(&format!(
            "DELETE FROM {}",
            crate::sync::session::quote_ident(&table)
        ))
        .map_err(|e| SnapshotError::ClearFailed(format!("clear {table}: {e}")))?;
    }

    // The snapshot is a second propagation channel: the changeset gate cuts
    // gated-false rows on the wire, so the snapshot must drop them too or a
    // private subtree leaks to a restoring device. Reuse the changeset gate's
    // model rather than re-deriving the FK walk.
    let gates = crate::sync::gate::Gates::from_tables(conn, synced)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;
    gates
        .delete_gated_false(conn)
        .map_err(|e| SnapshotError::ClearFailed(e.to_string()))?;

    // Reclaim the pages freed by the DELETEs so the blob shrinks.
    conn.execute_batch("VACUUM")
        .map_err(|e| SnapshotError::ClearFailed(format!("vacuum: {e}")))?;
    Ok(())
}

/// List user table names (excluding sqlite internal `sqlite_%` tables).
fn list_user_tables(conn: &Connection) -> Result<Vec<String>, SnapshotError> {
    let mut stmt = conn
        .prepare("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        .map_err(|e| SnapshotError::ClearFailed(format!("prepare table list: {e}")))?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| SnapshotError::ClearFailed(format!("query table list: {e}")))?;
    rows.collect::<Result<Vec<_>, _>>()
        .map_err(|e| SnapshotError::ClearFailed(format!("step table list: {e}")))
}

/// Publish a snapshot generation to the sync storage and update the device head.
///
/// A snapshot is published as a generation under this device's own `{author}` (its
/// hex public key), keyed by `current_seq`: the DB image
/// (`snapshot/{author}/{seq}.db`) and then the signed metadata
/// (`snapshot/{author}/{seq}_meta.json`) are written first, then a single signed
/// pointer (`snapshot/current.json`) carrying this generation's `{author, seq}` is
/// written *last*. The pointer is the commit — a reader follows it to a generation
/// that is already whole, so there is no window in which a new DB image is visible
/// paired with a stale or missing meta.
///
/// The publish is atomic by construction. Until the pointer flips, every reader
/// still resolves the *previous* generation (or none), which is itself complete.
/// A crash after the meta/db writes but before the pointer leaves new orphan
/// objects under `snapshot/{author}/{seq}*` that nothing references; the old
/// pointer stays valid. A new generation with its meta written is reclaimed by a
/// later sweep of this device (it lists its own keyspace); a generation that
/// crashed before its meta was written has no meta, so no sweep lists it — a
/// same-seq publish overwrites it, otherwise it lingers as unreferenced storage.
/// No half-published state is ever observable, and nothing relies on a later pass
/// to repair a wrong state — the only durable effect of a partial publish is
/// unreferenced storage, never a torn read.
///
/// The DB image is written *before* its metadata: a generation is listed (it is
/// keyed by its meta object), and so becomes a reader/sweep candidate, only once
/// the db it names is already whole. A crash after the db but before the meta
/// leaves an unlisted, unreferenced db that no reader or sweep observes; a same-seq
/// publish overwrites it, otherwise it lingers (the sweep lists generations by
/// their meta, which this db lacks).
///
/// Keying each device's generations under its own `{author}` makes them globally
/// unique: `seq` is this device's own `local_seq`, not a global order, but a peer
/// publishing the same `seq` writes to `snapshot/{peer}/...`, a different key — so
/// a publish never touches a peer's generation. The db_hash bind the pointer and
/// meta both carry still defends against tampering (a swapped db image), but a
/// same-seq cross-device collision is now structurally impossible rather than
/// merely caught.
///
/// Both the metadata and the pointer are signed by `keypair` and bound to
/// `library_id`. The metadata signs the cursors and the DB hash; the pointer signs
/// the generation `seq` and the same DB hash. The
/// bucket is untrusted and the shared at-rest cipher proves only confidentiality,
/// so signing lets a bootstrapping/GC reader authenticate that a current member
/// published this generation for *this* library and that the pointer was not
/// repointed by a non-member.
#[allow(clippy::too_many_arguments)]
pub async fn push_snapshot(
    storage: &dyn SyncStorage,
    library_id: &str,
    snapshot_db_image: Vec<u8>,
    device_id: &str,
    applied_cursors: HashMap<String, u64>,
    current_seq: u64,
    schema_version: u32,
    keypair: &UserKeypair,
    clock: &dyn crate::clock::Clock,
    blob_preflight: SnapshotBlobPreflight<'_>,
) -> Result<(), SnapshotError> {
    let size = snapshot_db_image.len();

    // This generation lives under this device's own keyspace, keyed by its public
    // key. The same value is what the signed meta/pointer carry as `author_pubkey`,
    // so the pointer's `{author, seq}` resolves straight to these objects.
    let own_author = hex::encode(keypair.public_key());

    // Hash the exact DB image, before it moves into `put_snapshot`. Both
    // the signed meta and the signed pointer commit to this hash, so a reader that
    // downloads the generation re-hashes those same bytes and detects a
    // substituted image.
    let db_hash = snapshot_db_hash(&snapshot_db_image);

    if !blob_preflight.blobs.is_empty() {
        ensure_publishable_blobs(blob_preflight.db, storage, blob_preflight.blobs)
            .await
            .map_err(|e| SnapshotError::PublishBlobs(e.to_string()))?;
    }

    // Write the DB image first, before anything lists or points at this
    // generation. A generation becomes a sweep/reader candidate only once its meta
    // exists (written next), and a reader resolves it only once the pointer names it
    // (written last) — so however large and slow this upload is, no reader sees it
    // mid-write. A crash here leaves an unlisted, unreferenced db: invisible to
    // readers and to the sweep; a later publish reusing this seq overwrites it,
    // otherwise it lingers (the sweep lists generations by meta, which this db lacks).
    storage
        .put_snapshot(&own_author, current_seq, snapshot_db_image)
        .await?;

    // The snapshot DB is a VACUUM of this device's live database, so its
    // metadata must describe exactly what THIS device has applied — never
    // other devices' published heads, which may be ahead of what we pulled.
    // Claiming coverage we don't have lets GC delete un-snapshotted changesets
    // that no future restore can recover.
    let mut cursors: BTreeMap<String, u64> = applied_cursors.into_iter().collect();
    // Our own current_seq is included (our head hasn't been updated yet).
    cursors.insert(device_id.to_string(), current_seq);

    let meta = SnapshotMetaJson::signed(
        library_id,
        cursors,
        db_hash.clone(),
        schema_version,
        keypair,
    );
    let meta_json = serde_json::to_vec(&meta).map_err(|e| SnapshotError::Parse(e.to_string()))?;

    // Write the meta second: this lists the generation under this device's own
    // keyspace, and its db is already whole above — so a listed generation is always
    // complete. Still nothing points at the generation.
    storage
        .put_snapshot_meta(&own_author, current_seq, meta_json)
        .await?;

    // Commit: write the pointer last, naming this generation's `{author, seq}`. Only
    // now does a reader resolve it — and the db+meta it names are already whole.
    let pointer = SnapshotPointerJson::signed(library_id, current_seq, db_hash, keypair);
    let pointer_json =
        serde_json::to_vec(&pointer).map_err(|e| SnapshotError::Parse(e.to_string()))?;
    storage.put_snapshot_pointer(pointer_json).await?;

    // Update the head to record this snapshot's coverage (snapshot_seq). The head's
    // `last_sync` stamp is the only thing here that still needs the wall clock.
    let timestamp = clock.now().to_rfc3339();
    storage
        .put_head(device_id, current_seq, Some(current_seq), &timestamp)
        .await?;

    // The pointer now names `current_seq`; older generations this device published
    // are superseded. Reclaim them — listing only this device's own keyspace, so a
    // peer's generation is never touched. A failure here is logged, not fatal: the
    // publish already succeeded, and the leftover is unreferenced storage rather
    // than reader-visible state.
    if let Err(e) = delete_superseded_generations(storage, current_seq, &own_author).await {
        warn!(error = %e, "failed to delete superseded snapshot generations after publish");
    }

    info!(
        device_id,
        snapshot_seq = current_seq,
        size,
        "published snapshot generation to sync storage"
    );

    Ok(())
}

#[cfg(test)]
#[allow(clippy::too_many_arguments)]
async fn push_snapshot_without_blob_refs(
    storage: &dyn SyncStorage,
    library_id: &str,
    snapshot_db_image: Vec<u8>,
    device_id: &str,
    applied_cursors: HashMap<String, u64>,
    current_seq: u64,
    schema_version: u32,
    keypair: &UserKeypair,
    clock: &dyn crate::clock::Clock,
) -> Result<(), SnapshotError> {
    let db = crate::sync::test_helpers::open_test_db();
    push_snapshot(
        storage,
        library_id,
        snapshot_db_image,
        device_id,
        applied_cursors,
        current_seq,
        schema_version,
        keypair,
        clock,
        SnapshotBlobPreflight {
            db: &db,
            blobs: &[],
        },
    )
    .await
}

/// Reclaim superseded snapshot generations that THIS device published.
///
/// Each device's generations live under its own `{own_author}` keyspace, so the
/// sweep lists only that prefix ([`SyncStorage::list_own_snapshot_generations`]):
/// every candidate is by construction a generation this device wrote. There is no
/// per-candidate author check — the key prefix *is* the author, so a peer's
/// generation (under a different prefix) is never even a candidate, and the strand
/// a cross-device sweep would risk is structurally impossible.
///
/// Within this device's own generations, the just-published generation is live by
/// construction: `push_snapshot` wrote the pointer naming `just_published_seq`
/// before this sweep. Peer-authored live generations sit under a different
/// keyspace and are never candidates. Every other own generation is superseded and
/// safe to delete because this device wrote it, this device is the only one that
/// reclaims it, and the sync loop does not sweep concurrently with its own publish.
async fn delete_superseded_generations(
    storage: &dyn SyncStorage,
    just_published_seq: u64,
    own_author: &str,
) -> Result<(), SnapshotError> {
    for seq in storage.list_own_snapshot_generations(own_author).await? {
        // Authorship is already settled by the keyspace: every seq here is this
        // device's own generation, and the published seq is the one the caller made
        // live before sweeping.
        if seq == just_published_seq {
            continue;
        }

        storage.delete_snapshot_generation(own_author, seq).await?;
        debug!(seq, "deleted superseded snapshot generation");
    }
    Ok(())
}

/// Check whether it's time to create a new snapshot.
///
/// Returns true if:
/// - `changesets_since_snapshot` >= the changeset threshold (100), OR
/// - `hours_since_snapshot` >= the time threshold (24h), OR
/// - No snapshot has ever been created (`last_snapshot_seq` is None)
///   AND at least one changeset has been pushed.
pub fn should_create_snapshot(
    local_seq: u64,
    last_snapshot_seq: Option<u64>,
    hours_since_snapshot: Option<u64>,
) -> bool {
    // Never created a snapshot, and we have at least one changeset.
    let Some(snap_seq) = last_snapshot_seq else {
        return local_seq > 0;
    };

    let changesets_since = local_seq.saturating_sub(snap_seq);
    if changesets_since >= SNAPSHOT_CHANGESET_THRESHOLD {
        return true;
    }

    if let Some(hours) = hours_since_snapshot {
        if hours >= SNAPSHOT_HOURS_THRESHOLD && changesets_since > 0 {
            return true;
        }
    }

    false
}

/// Authorize a snapshot control object's `author_pubkey` against the library's
/// membership chain.
///
/// The bucket is untrusted: the at-rest cipher proves only confidentiality, so a
/// signed object that decrypts could still have been written by anyone with the
/// credential. The signature (checked by the caller) proves *who* authored the
/// object; this proves whether that author *may* publish a catalog image — the
/// same split the changeset path enforces. The author must be a current Owner — a
/// snapshot restates the whole catalog, so a Member may push changesets but may not
/// author a catalog image (and a read-only Follower may not either) — and, when an
/// owner is pinned, the chain must be anchored to that owner.
///
/// A chain-less (browsable/open) library has no membership to authorize against,
/// so the object is accepted on its (already verified) signature alone — open by
/// design, exactly as the pull keeps every head when no chain exists. An empty
/// chain *under a pinned owner* is instead a wiped `membership/*` (a takeover
/// attempt) and is refused.
///
/// `owner_pubkey` is the library's established owner (the chain founder the invite
/// pins) when known — `Some` on the join path. `None` on the restore path (the
/// owner is adopted trust-on-first-use from the chain's own founder after the
/// pull), so the chain is anchored to whatever founder it self-validates to.
///
/// This is the one owner-authorization check, shared by the read paths (resolving
/// a snapshot to bootstrap or GC from) and the sync cycle's pre-publish decision
/// (whether this device may author a snapshot at all), so the producer and the
/// readers can never disagree on who may write a catalog image.
pub(crate) async fn authorize_author(
    storage: &dyn SyncStorage,
    author_pubkey: &str,
    owner_pubkey: Option<&str>,
) -> Result<(), SnapshotError> {
    authorize_membership_author(
        storage,
        author_pubkey,
        owner_pubkey,
        MembershipAuthorRequirement::Owner,
    )
    .await
    .map_err(snapshot_authorization_error)
}

fn snapshot_authorization_error(e: MembershipAuthorAuthorizationError) -> SnapshotError {
    match e {
        MembershipAuthorAuthorizationError::ListMembershipEntries(e) => SnapshotError::Bucket(e),
        MembershipAuthorAuthorizationError::Unauthorized(e) => SnapshotError::UnauthorizedAuthor(e),
    }
}

struct SnapshotMembership {
    chain: Option<MembershipChain>,
}

impl SnapshotMembership {
    fn authorize_owner(&self, author_pubkey: &str) -> Result<(), SnapshotError> {
        authorize_loaded_membership_author(
            self.chain.as_ref(),
            author_pubkey,
            MembershipAuthorRequirement::Owner,
        )
        .map_err(SnapshotError::UnauthorizedAuthor)
    }

    fn current_member_pubkeys(&self) -> Option<HashSet<String>> {
        self.chain.as_ref().map(|chain| {
            chain
                .current_members()
                .into_iter()
                .map(|(pubkey, _)| pubkey)
                .collect()
        })
    }
}

async fn load_snapshot_membership(
    storage: &dyn SyncStorage,
    owner_pubkey: Option<&str>,
) -> Result<SnapshotMembership, SnapshotError> {
    match load_membership_chain(storage, owner_pubkey).await {
        Ok(Some(chain)) => Ok(SnapshotMembership { chain: Some(chain) }),
        Ok(None) => {
            debug!(
                "snapshot membership load skipped: library is chain-less (no membership, no pinned owner)"
            );
            Ok(SnapshotMembership { chain: None })
        }
        Err(e) => Err(snapshot_authorization_error(e)),
    }
}

struct ResolvedSnapshotMeta {
    author_pubkey: String,
    seq: u64,
    meta: SnapshotMetaJson,
    membership: SnapshotMembership,
}

/// Follow the snapshot pointer to the live generation and return its author and
/// sequence plus its authenticated metadata.
///
/// This is the single read path through the atomic-publish layout, shared by
/// bootstrap and GC. The pointer is the commit, so it is resolved first and never
/// bypassed:
///
/// 1. Read and parse the signed [`SnapshotPointerJson`]; verify its signature
///    (a forged/tampered pointer naming a fabricated generation is refused), and
///    authorize its author against the chain (a non-member cannot repoint).
/// 2. Read and parse that generation's signed [`SnapshotMetaJson`] from the
///    pointer's `{author_pubkey, seq}` keyspace; verify its signature, and
///    authorize *its* author too (the same owner bar).
/// 3. Cross-check that the pointer and the meta commit to the *same* `db_hash`, so
///    a generation assembled from mismatched objects is refused.
///
/// The DB image itself is not fetched here — bootstrap downloads and hashes it
/// against `meta.db_hash`; GC never needs the bytes. Returns the live generation's
/// `author_pubkey` and `seq` (so bootstrap can read its db, and so GC knows which
/// of its own generations is live) and the authenticated meta (whose cursors drive
/// both bootstrap and GC).
async fn resolve_current_meta(
    storage: &dyn SyncStorage,
    library_id: &str,
    owner_pubkey: Option<&str>,
) -> Result<ResolvedSnapshotMeta, SnapshotError> {
    // The pointer is the entry point. Its absence means no snapshot has been
    // published (a brand-new library) or the pointer object is missing — either
    // way there is no consistent generation to resolve, surfaced as the bucket's
    // NotFound.
    let pointer_json = storage
        .get_snapshot_pointer()
        .await
        .map_err(SnapshotError::Bucket)?;
    let pointer: SnapshotPointerJson =
        serde_json::from_slice(&pointer_json).map_err(|e| SnapshotError::Parse(e.to_string()))?;
    // Verifying under THIS library's id also refuses a pointer validly signed for a
    // different library (a member of two libraries replaying one's snapshot as the
    // other's): the signature was taken over the other library's id, so it fails
    // here as `PointerSignatureInvalid`.
    if !pointer.verify(library_id) {
        return Err(SnapshotError::PointerSignatureInvalid);
    }
    let membership = load_snapshot_membership(storage, owner_pubkey).await?;
    membership.authorize_owner(&pointer.author_pubkey)?;

    // Follow the pointer to the named generation's metadata — under the pointer's
    // own `{author_pubkey, seq}` keyspace — and authenticate it on its own terms
    // (the meta and the pointer are independently signed; in a normal publish the
    // same device authored both). Verifying under this library's id likewise
    // refuses a cross-library meta replay.
    let meta_json = storage
        .get_snapshot_meta(&pointer.author_pubkey, pointer.seq)
        .await
        .map_err(SnapshotError::Bucket)?;
    let meta: SnapshotMetaJson =
        serde_json::from_slice(&meta_json).map_err(|e| SnapshotError::Parse(e.to_string()))?;
    if !meta.verify(library_id) {
        return Err(SnapshotError::MetaSignatureInvalid);
    }
    membership.authorize_owner(&meta.author_pubkey)?;

    // The pointer and the meta must describe the same image. They are written
    // together in one publish, so a mismatch means the generation was assembled
    // from objects of different pushes — refuse it.
    if pointer.db_hash != meta.db_hash {
        return Err(SnapshotError::PointerMetaMismatch);
    }

    Ok(ResolvedSnapshotMeta {
        author_pubkey: pointer.author_pubkey,
        seq: pointer.seq,
        meta,
        membership,
    })
}

/// One current device's contribution to the reclaim floor: its head slot
/// (`device_id`) and its pull cursors on every other device (`ack`), or `None` if
/// it has not published a verifiable ack.
struct AckedDevice {
    device_id: String,
    ack: Option<BTreeMap<String, u64>>,
}

/// Reclaim changeset objects superseded by the live snapshot and already pulled by
/// every current device.
///
/// `push_snapshot` already reclaims superseded snapshot *generations* through its
/// own-keyspace sweep; this reclaims only per-device *changeset* logs, which
/// nothing else reclaims and which otherwise grow without bound.
///
/// The reclaim floor for a device `D` strands no current member — running or
/// about-to-bootstrap:
///
/// ```text
/// floor_D = min(
///     snapshot.cursors[D],                              // 0 if D absent from the meta
///     min over OTHER current devices D' of ack_{D'}[D], // 0 if D' has no ack / no entry for D
/// )
/// ```
///
/// with the ack term unbounded (so `floor_D = snapshot.cursors[D]`) when there is
/// no other current device. Changesets `seq <= floor_D` are reclaimed; `floor_D ==
/// 0` reclaims nothing for `D`. The snapshot-cursor term protects a fresh device
/// that bootstraps from the live snapshot and pulls each device *above* its cursor;
/// the min-ack term protects a member that is behind (its present-but-stale ack
/// pins the floor at or below what it still needs). `D` itself is excluded from the
/// min — it holds its whole log, so its missing self-entry must not force its own
/// floor to 0.
///
/// All control inputs are authenticated, because they decide what is deleted
/// fleet-wide: the pointer/meta and their agreement ([`resolve_current_meta`], the
/// cursors are control input); the membership chain (anchored to `owner_pubkey`
/// when pinned); each head's signature (via [`SyncStorage::list_heads`]); and each
/// ack's signature against its slot plus its author matching the head's author. An
/// ack signed by a non-member, relocated to another slot, or with forged cursors is
/// ignored — it contributes cursor 0, which only narrows reclamation.
pub async fn reclaim_superseded_changesets(
    storage: &dyn SyncStorage,
    library_id: &str,
    owner_pubkey: Option<&str>,
) -> Result<GcResult, SnapshotError> {
    // Resolve the live generation's authenticated per-device cursors. No pointer
    // means no snapshot has been published yet -- a joiner replays from 0, so there
    // is nothing to reclaim.
    let resolved = match resolve_current_meta(storage, library_id, owner_pubkey).await {
        Ok(resolved) => resolved,
        Err(SnapshotError::Bucket(StorageError::NotFound(_))) => {
            info!("no snapshot pointer found, skipping changeset reclamation");
            return Ok(GcResult {
                deleted: 0,
                errors: 0,
            });
        }
        Err(e) => return Err(e),
    };

    // The set of current member pubkeys, or `None` for a chain-less (browsable)
    // library, where there is no membership to authorize against and every verified
    // head is a participant -- the same open-by-design path the pull and snapshot
    // authorization take when no chain exists.
    let members = resolved.membership.current_member_pubkeys();

    // The current devices: verified heads whose author is a current member (every
    // head when chain-less). For each, its verified ack supplies the pull cursors
    // that feed the floor; a missing/invalid ack contributes cursor 0 everywhere.
    let heads = storage.list_heads().await?;
    let mut devices: Vec<AckedDevice> = Vec::new();
    let mut errors = 0u64;
    for head in heads {
        let is_current = members
            .as_ref()
            .map(|m| m.contains(&head.author_pubkey))
            .unwrap_or(true);
        if !is_current {
            continue;
        }
        let ack = match storage.get_ack(&head.device_id).await {
            Ok(bytes) => match serde_json::from_slice::<AckJson>(&bytes) {
                // Honor the ack only when its signature verifies against its slot
                // AND its author is the same key the head verified against -- so a
                // non-member-signed ack planted in a member's slot is ignored.
                Ok(ack)
                    if ack.verify(&head.device_id) && ack.author_pubkey == head.author_pubkey =>
                {
                    Some(ack.cursors)
                }
                Ok(_) => {
                    warn!(device_id = %head.device_id, "ignoring ack that fails its signature/author check");
                    None
                }
                Err(e) => {
                    warn!(device_id = %head.device_id, error = %e, "ignoring ack that fails to parse");
                    None
                }
            },
            Err(StorageError::NotFound(_)) => None,
            Err(e) => {
                warn!(device_id = %head.device_id, error = %e, "failed to read ack; treating the device as un-acked");
                errors += 1;
                None
            }
        };
        devices.push(AckedDevice {
            device_id: head.device_id,
            ack,
        });
    }

    let mut deleted = 0u64;
    for device in &devices {
        let snapshot_cursor = resolved
            .meta
            .cursors
            .get(&device.device_id)
            .copied()
            .unwrap_or(0);

        // The min over the OTHER current devices' acked cursor on this device. A
        // device with no verified ack, or whose ack has no entry for this device,
        // contributes 0. No other current device leaves the term unbounded.
        let ack_floor = devices
            .iter()
            .filter(|other| other.device_id != device.device_id)
            .map(|other| {
                other
                    .ack
                    .as_ref()
                    .and_then(|cursors| cursors.get(&device.device_id).copied())
                    .unwrap_or(0)
            })
            .min();

        let floor = match ack_floor {
            Some(acked) => snapshot_cursor.min(acked),
            None => snapshot_cursor,
        };
        if floor == 0 {
            continue;
        }

        let seqs = match storage.list_changesets(&device.device_id).await {
            Ok(seqs) => seqs,
            Err(e) => {
                warn!(device_id = %device.device_id, error = %e, "failed to list changesets for reclamation, skipping device");
                errors += 1;
                continue;
            }
        };
        for seq in seqs {
            if seq > floor {
                continue;
            }
            match storage.delete_changeset(&device.device_id, seq).await {
                Ok(()) => deleted += 1,
                Err(e) => {
                    warn!(device_id = %device.device_id, seq, error = %e, "failed to delete changeset during reclamation");
                    errors += 1;
                }
            }
        }
    }

    info!(deleted, errors, "changeset reclamation complete");
    Ok(GcResult { deleted, errors })
}

/// Result of a changeset reclamation run.
#[derive(Debug, PartialEq, Eq)]
pub struct GcResult {
    /// Number of changesets successfully deleted.
    pub deleted: u64,
    /// Number of errors encountered (logged but not fatal).
    pub errors: u64,
}

/// Bootstrap a new device from a snapshot.
///
/// Follows the snapshot pointer to the live generation, authenticates the whole
/// generation before touching disk, reads its DB image through the storage layer
/// (opened for an encrypted home, verbatim for a plaintext one), and writes the
/// database to `target_path`. The caller should then open this as their local
/// database and pull any changesets newer than the per-device cursors in the
/// result.
///
/// The bucket is untrusted, so a snapshot is held to the same authorship bar as a
/// changeset before it is adopted:
///
/// - The signed pointer's signature must verify (under this `library_id`, which
///   also refuses a different library's pointer replayed here) and its author must
///   be a current Owner (a non-member cannot repoint the live
///   snapshot).
/// - The named generation's signed metadata must verify (likewise bound to
///   `library_id`) and its author must be a current Owner (a forged,
///   unsigned, or cursor-poisoned meta is refused), and the pointer and meta must
///   agree on the DB hash.
/// - The downloaded DB's hash must match what that signed meta commits to (a
///   substituted catalog image is refused). With each device's generations in its
///   own keyspace, a same-seq cross-device collision is structurally impossible, so
///   this bind now defends only against tampering with the bytes.
/// - Membership is anchored to `owner_pubkey` when it is pinned (`Some` on join,
///   where the invite pins the founder; `None` on restore, where the chain is
///   anchored to its own founder and the owner is adopted trust-on-first-use after
///   the pull).
///
/// Because the reader resolves the pointer first, it always sees a complete,
/// self-consistent generation: there is no window in which a new DB image is
/// served against a stale or missing meta. Any failure refuses loudly with a typed
/// [`SnapshotError`] and writes nothing to `target_path`, so a forged or torn
/// snapshot can never be adopted.
///
/// Returns a `BootstrapResult` with per-device cursors so the caller knows
/// where to start pulling changesets from each device.
pub async fn bootstrap_from_snapshot(
    storage: &dyn SyncStorage,
    library_id: &str,
    owner_pubkey: Option<&str>,
    binary_schema_version: u32,
    target_path: &Path,
) -> Result<BootstrapResult, SnapshotError> {
    // Resolve the pointer to the live generation and authenticate it before
    // touching disk. The pointer absent means no snapshot has been published (a
    // brand-new library) or its object is missing; either way there is no
    // consistent generation to adopt and we refuse, writing nothing.
    let ResolvedSnapshotMeta {
        author_pubkey,
        seq,
        meta,
        membership: _,
    } = resolve_current_meta(storage, library_id, owner_pubkey).await?;

    // Refuse a generation whose synced-schema version is newer than this binary
    // can apply, before downloading the image: its DB carries columns this binary's
    // tables lack, and applying a later device's changesets into it would fail.
    // This is the fail-fast gate; the at-open `run_migrations` SchemaTooNew check is
    // the by-construction backstop if a device somehow gets past here.
    if meta.schema_version > binary_schema_version {
        return Err(SnapshotError::SchemaTooNew {
            snapshot_version: meta.schema_version,
            supported: binary_schema_version,
        });
    }

    // Download the named generation's DB image from its publisher's keyspace and
    // confirm it is the exact image the (now authenticated) meta and pointer commit
    // to, before opening or writing it.
    let plaintext = storage.get_snapshot(&author_pubkey, seq).await?;
    if snapshot_db_hash(&plaintext) != meta.db_hash {
        return Err(SnapshotError::DbHashMismatch);
    }

    write_snapshot_db(target_path, &plaintext)?;

    let cursors: HashMap<String, u64> = meta.cursors.into_iter().collect();
    info!(
        num_devices = cursors.len(),
        db_size = plaintext.len(),
        path = %target_path.display(),
        "bootstrapped from snapshot"
    );

    Ok(BootstrapResult { cursors })
}

/// Download the blob files the DB at `db_path` references but whose local file is
/// absent, returning true once every referenced blob is on local disk.
///
/// `bootstrap_from_snapshot` writes only the catalog DB; the incremental pull
/// that follows starts past the snapshot's per-device cursors, so the original
/// INSERT changesets that carried each row's eager blob (seq <= cursor)
/// are never re-walked and the per-changeset blob download never fires for them.
/// Without this reconciliation a bootstrapped device has the rows but none of the
/// files they point at (a synced album shows a placeholder cover). Only the
/// `CacheEager` blobs are reconciled: a `CacheLazy` blob (e.g. audio) is fetched on
/// first read, so a bootstrapped device need not download it up front — this scan
/// filters [`BlobDecls::refs_in_db`](crate::blob::decl::BlobDecls::refs_in_db) to
/// the `CacheEager` class, the same class the incremental pull downloads.
///
/// coven derives the blobs the DB at `db_path` references from the blob
/// declarations in `tables`, then downloads the `CacheEager` ones via the same
/// [`crate::sync::pull::download_blobs`] path the incremental pull uses — into the
/// evictable cache `storage/cache/<namespace>/<id>` under `library_dir`, skipping
/// any already present in either cache folder. A failed download is logged there
/// and reflected in the returned flag; the bootstrap that calls this refuses to
/// save the library unless the flag is true.
///
/// `refs_in_db` is a read-only enumeration run against a short-lived connection to
/// the same on-disk DB the `db` actor owns; `db` is still needed because
/// `download_blobs` resolves each blob's scope through it (an `Item`-scoped blob
/// reads its key from the `item_keys` rows). At bootstrap the pull has not started;
/// in a cycle this runs after the pull. It is read-only either way (a SELECT the
/// capture session records nothing from), so it does not re-record rows or race the
/// actor.
pub async fn reconcile_snapshot_blobs(
    db: &crate::database::Database,
    db_path: &Path,
    storage: &dyn SyncStorage,
    library_dir: &crate::library_dir::LibraryDir,
    tables: &[SyncedTable],
) -> Result<bool, crate::database::DbError> {
    let blobs: Vec<crate::sync::pull::BlobDownload> = {
        let conn = Connection::open(db_path).map_err(crate::database::DbError::from)?;
        let decls = crate::blob::decl::BlobDecls::from_tables(&conn, tables)
            .map_err(|e| crate::database::DbError(format!("blob decls: {e}")))?;
        decls
            .refs_in_db(&conn)
            .map_err(|e| crate::database::DbError(format!("blob decls: {e}")))?
            .into_iter()
            .filter(|blob| blob.fill == crate::blob::CacheFill::CacheEager)
            .map(crate::sync::pull::BlobDownload::from_installed_db)
            .collect()
    };

    if blobs.is_empty() {
        return Ok(true);
    }

    let total = blobs.len();
    // No in-changeset key map here: a snapshot's blobs take their keys from the
    // `item_keys` rows the snapshot itself carried into this DB, so resolution
    // goes through the DB (issue #111's pull path uses the map for keys minted in
    // the changeset being applied; the bootstrap has no such changeset). The blobs
    // are `CacheEager`, so `download_blobs` writes each into the evictable cache
    // `storage/cache/<namespace>/<id>` under `library_dir`.
    let all_ok = crate::sync::pull::download_blobs(
        db,
        blobs,
        storage,
        library_dir,
        &std::collections::HashMap::new(),
    )
    .await;
    if all_ok {
        info!(total, "snapshot blob reconciliation complete");
    } else {
        warn!(total, "some snapshot blob files are not local");
    }
    Ok(all_ok)
}

/// The library id the snapshot tests sign their meta/pointer under. The same id is
/// passed to `push_snapshot`/`bootstrap_from_snapshot`/`reclaim_superseded_changesets`,
/// so the signatures verify; a cross-library binding mismatch is exercised by its own
/// test. Shared by the `tests`, `authorization_tests`, and `reclaim_tests` modules.
#[cfg(test)]
const TEST_LIBRARY_ID: &str = "test-library";

/// Publish a full snapshot generation directly: the signed meta over `cursors`,
/// the db image, and the signed pointer naming `{author, seq}`.
#[cfg(test)]
async fn publish_signed_generation<I, K>(
    storage: &dyn SyncStorage,
    seq: u64,
    cursors: I,
    sealed_db: Vec<u8>,
    keypair: &UserKeypair,
) where
    I: IntoIterator<Item = (K, u64)>,
    K: Into<String>,
{
    let author = hex::encode(keypair.public_key());
    let db_hash = snapshot_db_hash(&sealed_db);
    let cursors = cursors
        .into_iter()
        .map(|(device_id, seq)| (device_id.into(), seq))
        .collect();
    let meta = SnapshotMetaJson::signed(TEST_LIBRARY_ID, cursors, db_hash.clone(), 0, keypair);
    storage
        .put_snapshot_meta(&author, seq, serde_json::to_vec(&meta).unwrap())
        .await
        .unwrap();
    storage.put_snapshot(&author, seq, sealed_db).await.unwrap();
    let pointer = SnapshotPointerJson::signed(TEST_LIBRARY_ID, seq, db_hash, keypair);
    storage
        .put_snapshot_pointer(serde_json::to_vec(&pointer).unwrap())
        .await
        .unwrap();
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::{local_files, CacheFill, ResolvedScope};
    use crate::database::DbError;
    use crate::sync::apply::apply_changeset_lww;
    use crate::sync::session::BlobDecl;
    use crate::sync::test_helpers::{
        open_test_db_with_user_and_host_blobs, temp_library_dir,
        test_synced_tables_with_user_and_host_blobs, MockSyncStorage,
    };
    use rusqlite::session::Session as RqSession;
    use rusqlite::{Connection, OptionalExtension};
    use std::collections::HashMap;

    // ---- in-process db helpers (rusqlite, the new `&Connection` API) ----

    /// The synthetic synced set the snapshot tests scope by.
    fn synced_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("notes").gated_by("shared"),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos"),
        ]
    }

    fn remote_root_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("notes").remote_root(),
            SyncedTable::new("note_tags"),
            SyncedTable::new("note_photos"),
        ]
    }

    /// A fresh in-memory connection with `foreign_keys=ON` and the synthetic
    /// notes/note_tags/note_photos schema.
    fn synced_conn() -> Connection {
        let c = Connection::open_in_memory().expect("open in-memory");
        c.execute_batch(
            "PRAGMA foreign_keys = ON;
            CREATE TABLE notes (
                id TEXT PRIMARY KEY, title TEXT NOT NULL, body TEXT,
                shared INTEGER NOT NULL DEFAULT 0,
                _updated_at TEXT NOT NULL, created_at TEXT NOT NULL
            );
            CREATE TABLE note_tags (
                id TEXT PRIMARY KEY, note_id TEXT NOT NULL, tag TEXT NOT NULL,
                _updated_at TEXT NOT NULL, created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );
            CREATE TABLE note_photos (
                id TEXT PRIMARY KEY, note_id TEXT NOT NULL, kind TEXT NOT NULL,
                _updated_at TEXT NOT NULL, created_at TEXT NOT NULL,
                FOREIGN KEY (note_id) REFERENCES notes (id) ON DELETE CASCADE
            );",
        )
        .expect("create schema");
        c
    }

    /// Open a SQLite database file by path as a standalone connection.
    fn open_db_at(path: &Path) -> Connection {
        Connection::open(path).expect("open db file")
    }

    fn exec(c: &Connection, sql: &str) {
        c.execute_batch(sql)
            .unwrap_or_else(|e| panic!("exec failed for {sql}: {e}"));
    }

    fn query_text(c: &Connection, sql: &str) -> String {
        c.query_row(sql, [], |r| r.get::<_, String>(0))
            .unwrap_or_else(|e| panic!("query_text failed for {sql}: {e}"))
    }

    fn query_int(c: &Connection, sql: &str) -> i64 {
        c.query_row(sql, [], |r| r.get::<_, i64>(0))
            .unwrap_or_else(|e| panic!("query_int failed for {sql}: {e}"))
    }

    fn row_exists(c: &Connection, sql: &str) -> bool {
        c.query_row(sql, [], |_| Ok(()))
            .optional()
            .unwrap_or_else(|e| panic!("row_exists failed for {sql}: {e}"))
            .is_some()
    }

    /// Apply a changeset's bytes with the production LWW path scoped to the test
    /// synced set.
    fn apply(c: &Connection, bytes: &[u8]) {
        apply_changeset_lww(c, bytes, &synced_tables(), crate::sync::hlc::now_wall_ms())
            .expect("apply changeset");
    }

    /// Record a changeset over the synced tables while `body` runs SQL against a
    /// fresh schema-only connection. Returns the recorded bytes.
    fn changeset_bytes_for(body: impl FnOnce(&Connection)) -> Vec<u8> {
        let c = synced_conn();
        let mut session = RqSession::new(&c).expect("session");
        for t in synced_tables() {
            session.attach(Some(t.name())).expect("attach");
        }
        body(&c);
        let mut buf = Vec::new();
        session.changeset_strm(&mut buf).expect("changeset");
        buf
    }

    /// The keypair the snapshot tests push and sign with, when they don't care
    /// who the author is (round-trip, cursor-honesty, GC). It is not registered in
    /// any chain, so these tests bootstrap with `owner = None` and an empty
    /// membership listing — the open-library path that authorizes on the signature
    /// alone. The membership-authorization tests build their own chained mock.
    fn test_keypair() -> UserKeypair {
        UserKeypair::generate()
    }

    // ---- should_create_snapshot tests ----

    #[test]
    fn snapshot_policy_no_previous_snapshot_with_changes() {
        assert!(should_create_snapshot(1, None, None));
        assert!(should_create_snapshot(50, None, None));
    }

    #[test]
    fn snapshot_policy_no_previous_snapshot_no_changes() {
        assert!(!should_create_snapshot(0, None, None));
    }

    #[test]
    fn snapshot_policy_below_threshold() {
        // 10 changesets since last snapshot, only 1 hour elapsed.
        assert!(!should_create_snapshot(60, Some(50), Some(1)));
    }

    #[test]
    fn snapshot_policy_changeset_threshold_reached() {
        // Exactly 100 changesets since snapshot.
        assert!(should_create_snapshot(150, Some(50), Some(1)));
        // Over 100.
        assert!(should_create_snapshot(200, Some(50), Some(1)));
    }

    #[test]
    fn snapshot_policy_time_threshold_reached() {
        // Only 10 changesets but 24+ hours have passed.
        assert!(should_create_snapshot(60, Some(50), Some(24)));
        assert!(should_create_snapshot(60, Some(50), Some(48)));
    }

    #[test]
    fn snapshot_policy_time_threshold_no_new_changes() {
        // 24 hours but zero changesets since snapshot.
        assert!(!should_create_snapshot(50, Some(50), Some(24)));
    }

    // ---- create_snapshot tests ----

    #[test]
    fn create_snapshot_produces_db_image() {
        let c = synced_conn();
        exec(
            &c,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'Note One', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let db_image = create_snapshot(&c, temp.path(), &synced_tables()).expect("create_snapshot");

        assert!(!db_image.is_empty());
        let plaintext = db_image;
        assert!(!plaintext.is_empty());
        assert!(
            plaintext.starts_with(b"SQLite format 3\0"),
            "snapshot should be a valid SQLite database"
        );
    }

    #[test]
    fn create_snapshot_accepts_apostrophe_in_temp_path() {
        let c = synced_conn();
        exec(
            &c,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'Note One', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::Builder::new()
            .prefix("snapshot-'")
            .tempdir()
            .unwrap();
        let encrypted = create_snapshot(&c, temp.path(), &synced_tables()).expect("snapshot");
        let plaintext = encrypted;

        let db_path = temp.path().join("verify.db");
        std::fs::write(&db_path, &plaintext).unwrap();
        let db2 = open_db_at(&db_path);

        assert_eq!(
            query_text(&db2, "SELECT title FROM notes WHERE id = 'n1'"),
            "Note One"
        );
    }

    #[test]
    fn create_snapshot_contains_data() {
        let c = synced_conn();
        exec(
            &c,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        exec(
            &c,
            "INSERT INTO note_tags (id, tag, note_id, _updated_at, created_at) \
             VALUES ('al1', 'Album One', 'a1', '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let encrypted = create_snapshot(&c, temp.path(), &synced_tables()).expect("snapshot");
        let plaintext = encrypted;

        let db_path = temp.path().join("verify.db");
        std::fs::write(&db_path, &plaintext).unwrap();
        let db2 = open_db_at(&db_path);

        assert_eq!(
            query_text(&db2, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One"
        );
        assert_eq!(
            query_text(&db2, "SELECT tag FROM note_tags WHERE id = 'al1'"),
            "Album One"
        );
    }

    /// A snapshot is a propagation channel between devices, so it carries only
    /// synced-table data. A non-synced table (here `device_local`, holding a
    /// filesystem path meaningful only on the device that wrote it) keeps its
    /// schema in the restored DB but none of its rows: the schema survives so
    /// the table still opens, while its device-local rows never cross to a
    /// restoring peer.
    #[tokio::test]
    async fn snapshot_does_not_carry_local_only_tables_to_a_restoring_device() {
        // --- Device A: a synced table + a device-local table ---
        let db_a = synced_conn();
        exec(
            &db_a,
            "CREATE TABLE device_local (note_id TEXT PRIMARY KEY, local_path TEXT NOT NULL)",
        );
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'Note One', 1, '0000000001000-0000-devA', '2026-01-01')",
        );
        exec(
            &db_a,
            "INSERT INTO device_local (note_id, local_path) \
             VALUES ('n1', '/tmp/device-local/path')",
        );

        let temp = tempfile::tempdir().unwrap();
        let encrypted = create_snapshot(&db_a, temp.path(), &synced_tables()).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            encrypted,
            "devA",
            HashMap::new(),
            1,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot");

        let target = temp.path().join("device_b.db");
        bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap_from_snapshot");
        let db_b = open_db_at(&target);

        // Synced data SHOULD cross.
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Note One",
            "synced-table data must survive a snapshot restore",
        );
        // Device-local data must NOT cross.
        assert!(
            !row_exists(&db_b, "SELECT 1 FROM device_local WHERE note_id = 'n1'"),
            "device-local row leaked to a peer via the snapshot",
        );
        // The table SCHEMA is preserved (only its rows are cleared).
        assert!(
            row_exists(
                &db_b,
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='device_local'",
            ),
            "non-synced table schema must survive: snapshot DELETEs rows, never DROPs tables",
        );
        assert_eq!(
            query_int(&db_b, "SELECT COUNT(*) FROM device_local"),
            0,
            "non-synced table must be empty in the restored DB",
        );
    }

    /// The fail-fast bootstrap gate: a snapshot whose synced-schema version is newer
    /// than this binary's top migration is refused before its image is downloaded.
    /// A binary at the writer's version — or newer — adopts it (a newer binary
    /// carries the image forward at open, which `run_migrations` covers).
    #[tokio::test]
    async fn bootstrap_refuses_a_snapshot_newer_than_this_binary() {
        let db_a = synced_conn();
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'Note One', 1, '0000000001000-0000-devA', '2026-01-01')",
        );
        let temp = tempfile::tempdir().unwrap();
        let encrypted = create_snapshot(&db_a, temp.path(), &synced_tables()).expect("snapshot");

        // Publish the generation stamped at synced-schema version 2.
        let storage = MockSyncStorage::new();
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            encrypted,
            "devA",
            HashMap::new(),
            1,
            2,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot");

        // A binary topping out at version 1 cannot apply a version-2 image: refused
        // before download, with the SchemaTooNew shape, writing nothing.
        let too_old = temp.path().join("too_old.db");
        let err = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 1, &too_old)
            .await
            .expect_err("a too-old binary must refuse a newer snapshot");
        assert!(matches!(
            err,
            SnapshotError::SchemaTooNew {
                snapshot_version: 2,
                supported: 1
            }
        ));
        assert!(
            !too_old.exists(),
            "nothing is written when the gate refuses"
        );

        // A binary at the writer's version adopts it.
        let same = temp.path().join("same.db");
        bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 2, &same)
            .await
            .expect("a binary at the snapshot's version bootstraps");
        assert!(same.exists());

        // A newer binary (version 3) adopts it too.
        let newer = temp.path().join("newer.db");
        bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 3, &newer)
            .await
            .expect("a newer binary bootstraps an older snapshot");
        assert!(newer.exists());
    }

    /// The snapshot is a second propagation channel, so it must honor the same
    /// row-level gate the outbound changeset does: a gated-false root (`notes`
    /// with `shared = 0`) and its FK-descendants (`note_tags`) are private and
    /// must never cross to a restoring device, while a gated-true root and its
    /// descendants must. The table-level clear keeps the `notes`/`note_tags`
    /// *schema* (both are synced tables); this verifies the *rows* are
    /// gate-scoped.
    #[tokio::test]
    async fn snapshot_does_not_carry_gated_false_rows_to_a_restoring_device() {
        let db_a = synced_conn();

        // A shared note with a child tag (both must cross).
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('pub', 'Public', 1, '0000000001000-0000-devA', '2026-01-01')",
        );
        exec(
            &db_a,
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('pub_t', 'pub', 'green', '0000000001000-0000-devA', '2026-01-01')",
        );
        // A private note with its own child tag (neither may cross).
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('priv', 'Private', 0, '0000000002000-0000-devA', '2026-01-01')",
        );
        exec(
            &db_a,
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('priv_t', 'priv', 'red', '0000000002000-0000-devA', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let encrypted = create_snapshot(&db_a, temp.path(), &synced_tables()).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            encrypted,
            "devA",
            HashMap::new(),
            1,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot");

        let target = temp.path().join("device_b.db");
        bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap_from_snapshot");
        let db_b = open_db_at(&target);

        assert!(
            row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'pub'"),
            "a gated-true note must survive the snapshot restore",
        );
        assert!(
            row_exists(&db_b, "SELECT 1 FROM note_tags WHERE id = 'pub_t'"),
            "a gated-true note's FK-child must survive the snapshot restore",
        );
        assert!(
            !row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'priv'"),
            "a gated-false note leaked to a peer via the snapshot",
        );
        assert!(
            !row_exists(&db_b, "SELECT 1 FROM note_tags WHERE id = 'priv_t'"),
            "a gated-false note's FK-descendant leaked to a peer via the snapshot",
        );
    }

    #[test]
    fn snapshot_carries_remote_root_rows_without_gate_truth() {
        let db_a = synced_conn();
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('remote', 'Remote Root', 0, '0000000001000-0000-devA', '2026-01-01')",
        );
        exec(
            &db_a,
            "INSERT INTO note_tags (id, note_id, tag, _updated_at, created_at) \
             VALUES ('remote_t', 'remote', 'blue', '0000000001000-0000-devA', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &remote_root_tables()).expect("snapshot");
        let plaintext = encrypted;
        let db_path = temp.path().join("remote_root_snapshot.db");
        std::fs::write(&db_path, &plaintext).unwrap();
        let db_b = open_db_at(&db_path);

        assert!(
            row_exists(&db_b, "SELECT 1 FROM notes WHERE id = 'remote'"),
            "a remote-root row must survive snapshot scoping",
        );
        assert!(
            row_exists(&db_b, "SELECT 1 FROM note_tags WHERE id = 'remote_t'"),
            "a remote-root descendant must survive snapshot scoping",
        );
    }

    /// coven's own bookkeeping tables (`sync_state`, `sync_cursors`,
    /// `cloud_outbox`, `local_blob_refs`) are per-device: a peer's cursors, pending
    /// outbox, HLC high-water, and external-file refs must NOT ride a snapshot to a
    /// restoring device — inheriting them would make the new device think it had
    /// already pulled the snapshotter's peers, replay the snapshotter's blob queue,
    /// or read a peer's local file paths. They are not in the synced set, so the
    /// table-level clear must strip their rows while keeping the schemas (so the
    /// restored DB opens and coven can immediately write its own fresh
    /// bookkeeping). This guards that present-but-empty invariant, which the other
    /// snapshot tests miss because their schema omits coven's tables.
    #[tokio::test]
    async fn snapshot_does_not_carry_bookkeeping_tables_to_a_restoring_device() {
        let db_a = synced_conn();
        // Add coven's bookkeeping tables (the snapshot source normally has them;
        // the synthetic test schema doesn't) and populate every one.
        crate::db::apply_coven_schema(&db_a).expect("create bookkeeping schema");
        exec(
            &db_a,
            "INSERT INTO sync_state (key, value) VALUES \
             ('local_seq', '42'), ('hlc_high_water', '0000000009000-0000-devA')",
        );
        exec(
            &db_a,
            "INSERT INTO sync_cursors (device_id, last_seq) VALUES ('devB', 7), ('devC', 3)",
        );
        exec(
            &db_a,
            &format!(
                "INSERT INTO cloud_outbox (operation, file_id, cloud_key, scope, created_at) \
                 VALUES ('upload', 'f1', 'blobs/f1', '{}', '2026-01-01')",
                crate::blob::BlobScope::Master.to_outbox_str()
            ),
        );
        exec(
            &db_a,
            "INSERT INTO local_blob_refs (blob_id, namespace, path, size) \
             VALUES ('f1', 'audio', '/tmp/external/track.flac', 1234)",
        );
        // A synced row that SHOULD cross, to prove the snapshot still carries data.
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'Keep me', 1, '0000000001000-0000-devA', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let encrypted = create_snapshot(&db_a, temp.path(), &synced_tables()).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            encrypted,
            "devA",
            HashMap::new(),
            1,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot");

        let target = temp.path().join("device_b.db");
        bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap_from_snapshot");
        let db_b = open_db_at(&target);

        // Synced data still crosses.
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Keep me",
            "synced data must survive alongside the bookkeeping clear",
        );

        // Each bookkeeping table is present (schema kept) but empty (rows cleared).
        for table in [
            "sync_state",
            "sync_cursors",
            "cloud_outbox",
            "local_blob_refs",
        ] {
            assert!(
                row_exists(
                    &db_b,
                    &format!("SELECT 1 FROM sqlite_master WHERE type='table' AND name='{table}'"),
                ),
                "bookkeeping table {table} schema must survive the snapshot restore",
            );
            assert_eq!(
                query_int(&db_b, &format!("SELECT COUNT(*) FROM {table}")),
                0,
                "{table} must be empty in the restored DB — a peer's bookkeeping \
                 (cursors/outbox/clock) must never ride a snapshot to a new device",
            );
        }
    }

    // ---- push_snapshot tests ----

    #[tokio::test]
    async fn push_snapshot_uploads_and_updates_head() {
        let storage = MockSyncStorage::new();
        let data = vec![1, 2, 3, 4, 5];

        // The snapshotting device has applied dev-2 up to seq 15.
        let applied = HashMap::from([("dev-2".to_string(), 15)]);

        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            data.clone(),
            "dev-1",
            applied,
            42,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push_snapshot should succeed");

        // Snapshot should be stored.
        assert_eq!(storage.current_snapshot_db().await, Some(data));

        // Head should be updated with snapshot_seq.
        let heads = storage.list_heads().await.unwrap();
        let dev1_head = heads.iter().find(|h| h.device_id == "dev-1").unwrap();
        assert_eq!(dev1_head.seq, 42);
        assert_eq!(dev1_head.snapshot_seq, Some(42));

        // Snapshot metadata reflects the applied cursors plus our own seq.
        let meta_json = storage
            .current_snapshot_meta()
            .await
            .expect("metadata should be written");
        let meta: SnapshotMetaJson = serde_json::from_slice(&meta_json).unwrap();
        assert_eq!(meta.cursors.get("dev-1"), Some(&42));
        assert_eq!(meta.cursors.get("dev-2"), Some(&15));
        assert_eq!(meta.cursors.len(), 2);
    }

    fn user_blob_decl() -> BlobDecl {
        BlobDecl::new("audio", Provenance::UserProvided, CacheFill::CacheLazy)
    }

    fn host_blob_decl() -> BlobDecl {
        BlobDecl::new("covers", Provenance::HostProvided, CacheFill::CacheEager)
    }

    async fn snapshot_from_blob_db(db: &Database, temp_dir: &Path) -> CreatedSnapshot {
        let tables =
            test_synced_tables_with_user_and_host_blobs(user_blob_decl(), host_blob_decl());
        let temp_dir = temp_dir.to_path_buf();
        db.call(move |conn| {
            create_snapshot_with_host_blobs(conn, &temp_dir, &tables)
                .map_err(|e| DbError(e.to_string()))
        })
        .await
        .expect("create blob-bearing snapshot")
    }

    async fn insert_snapshot_blob_rows(db: &Database, include_host_blob: bool) {
        crate::sync::test_helpers::exec(
            db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'WithAudio', NULL, 1, '0000000001000-0000-dev1', '2026-01-01');
             INSERT INTO note_photos (id, note_id, kind, size, _updated_at, created_at) \
             VALUES ('audio1', 'n1', 'audio', 11, '0000000001000-0000-dev1', '2026-01-01')",
        )
        .await;
        if include_host_blob {
            crate::sync::test_helpers::exec(
                db,
                "INSERT INTO note_covers (id, note_id, size, _updated_at, created_at) \
                 VALUES ('cover1', 'n1', 5, '0000000001000-0000-dev1', '2026-01-01')",
            )
            .await;
        }
    }

    #[tokio::test]
    async fn snapshot_with_local_user_provided_blob_aborts_before_publish_markers() {
        let db = open_test_db_with_user_and_host_blobs(user_blob_decl(), host_blob_decl());
        let (tmp, _ld) = temp_library_dir();
        let external = tmp.path().join("audio.flac");
        std::fs::write(&external, b"local audio").expect("write external file");
        db.register_external_blob("audio1", "audio", &external, 11)
            .await
            .expect("register external ref");
        insert_snapshot_blob_rows(&db, false).await;
        let snapshot = snapshot_from_blob_db(&db, tmp.path()).await;
        let storage = MockSyncStorage::new();

        let err = push_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            snapshot.db_image,
            "dev-1",
            HashMap::new(),
            7,
            db.schema_version(),
            &test_keypair(),
            &crate::clock::SystemClock,
            SnapshotBlobPreflight {
                db: &db,
                blobs: &snapshot.publish_blobs,
            },
        )
        .await
        .expect_err("local user-provided blob must abort snapshot publish");

        assert!(
            matches!(err, SnapshotError::PublishBlobs(_)),
            "local user-provided blob must fail during snapshot preflight: {err:?}",
        );
        assert!(storage.current_snapshot_db().await.is_none());
        assert!(storage.list_heads().await.expect("list heads").is_empty());
    }

    #[tokio::test]
    async fn snapshot_with_missing_remote_user_provided_blob_aborts_before_publish_markers() {
        let db = open_test_db_with_user_and_host_blobs(user_blob_decl(), host_blob_decl());
        let (tmp, _ld) = temp_library_dir();
        insert_snapshot_blob_rows(&db, false).await;
        let snapshot = snapshot_from_blob_db(&db, tmp.path()).await;
        let storage = MockSyncStorage::new();

        let err = push_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            snapshot.db_image,
            "dev-1",
            HashMap::new(),
            7,
            db.schema_version(),
            &test_keypair(),
            &crate::clock::SystemClock,
            SnapshotBlobPreflight {
                db: &db,
                blobs: &snapshot.publish_blobs,
            },
        )
        .await
        .expect_err("missing remote user-provided blob must abort snapshot publish");

        assert!(
            matches!(err, SnapshotError::PublishBlobs(_)),
            "missing remote user-provided blob must fail during snapshot preflight: {err:?}",
        );
        assert!(storage.current_snapshot_db().await.is_none());
        assert!(storage.list_heads().await.expect("list heads").is_empty());
    }

    #[tokio::test]
    async fn snapshot_with_remote_user_and_uploaded_host_blobs_publishes() {
        let db = open_test_db_with_user_and_host_blobs(user_blob_decl(), host_blob_decl());
        let (tmp, ld) = temp_library_dir();
        insert_snapshot_blob_rows(&db, true).await;
        local_files::store(&ld, "covers", "cover1", b"COVER")
            .await
            .expect("store host-provided cover");
        let snapshot = snapshot_from_blob_db(&db, tmp.path()).await;
        let storage = MockSyncStorage::new();
        storage
            .put_blob(
                "audio",
                "audio1",
                ResolvedScope::Master,
                None,
                b"AUDIO".to_vec(),
            )
            .await
            .expect("plant remote user-provided blob");
        crate::sync::service::upload_snapshot_host_blobs(&db, &storage, &ld, &snapshot.host_blobs)
            .await
            .expect("upload host-provided snapshot blobs");
        assert_eq!(
            storage
                .get_blob("covers", "cover1", ResolvedScope::Master, None)
                .await
                .expect("host cover uploaded"),
            b"COVER",
        );

        push_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            snapshot.db_image.clone(),
            "dev-1",
            HashMap::new(),
            7,
            db.schema_version(),
            &test_keypair(),
            &crate::clock::SystemClock,
            SnapshotBlobPreflight {
                db: &db,
                blobs: &snapshot.publish_blobs,
            },
        )
        .await
        .expect("snapshot is publishable once user and host blobs are remote");

        assert_eq!(storage.current_snapshot_db().await, Some(snapshot.db_image));
        assert_eq!(storage.list_heads().await.expect("list heads")[0].seq, 7);
    }

    // ---- bootstrap_from_snapshot tests ----

    #[tokio::test]
    async fn bootstrap_downloads_decrypts_and_writes_db() {
        // First create a snapshot from a real database.
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        let temp = tempfile::tempdir().unwrap();
        let encrypted = create_snapshot(&db, temp.path(), &synced_tables()).expect("snapshot");

        let storage = MockSyncStorage::new();
        publish_signed_generation(
            &storage,
            10,
            BTreeMap::from([("dev-1".to_string(), 10), ("dev-2".to_string(), 7)]),
            encrypted,
            &test_keypair(),
        )
        .await;

        let target = temp.path().join("bootstrapped.db");
        let result = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap");

        assert_eq!(result.cursors.get("dev-1"), Some(&10));
        assert_eq!(result.cursors.get("dev-2"), Some(&7));
        assert_eq!(result.cursors.len(), 2);
        assert!(target.exists());

        let db2 = open_db_at(&target);
        assert_eq!(
            query_text(&db2, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One"
        );
    }

    #[tokio::test]
    async fn bootstrap_fails_when_no_snapshot_exists() {
        let storage = MockSyncStorage::new();
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("nope.db");

        let result = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target).await;

        assert!(result.is_err());
        assert!(!target.exists());
    }

    // ---- Integration: create, push, bootstrap, verify ----

    #[tokio::test]
    async fn full_snapshot_round_trip() {
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        exec(
            &db,
            "INSERT INTO note_tags (id, tag, note_id, _updated_at, created_at) \
             VALUES ('al1', 'Album One', 'a1', '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        let encrypted = create_snapshot(&db, temp.path(), &synced_tables()).expect("snapshot");
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            encrypted,
            "dev-1",
            HashMap::new(),
            5,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let target = temp.path().join("device2.db");
        let result = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap");
        assert_eq!(result.cursors.get("dev-1"), Some(&5));

        let db2 = open_db_at(&target);
        assert_eq!(
            query_text(&db2, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One"
        );
        assert_eq!(
            query_text(&db2, "SELECT tag FROM note_tags WHERE id = 'al1'"),
            "Album One"
        );
    }

    /// Verify that a snapshot + subsequent changesets produces the same state
    /// as applying all changesets from scratch.
    #[tokio::test]
    async fn snapshot_plus_changesets_equals_full_replay() {
        let temp = tempfile::tempdir().unwrap();

        // --- Phase 1: create data, snapshot, then more data ---
        let db_source = synced_conn();

        // Initial data (before snapshot), captured as cs1.
        let mut session1 = RqSession::new(&db_source).expect("session");
        for t in synced_tables() {
            session1.attach(Some(t.name())).expect("attach");
        }
        exec(
            &db_source,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        exec(
            &db_source,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a2', 'Artist Two', 1, '0000000002000-0000-dev1', '2026-01-01')",
        );
        let mut cs1_bytes = Vec::new();
        session1.changeset_strm(&mut cs1_bytes).expect("cs1");
        drop(session1);

        // Snapshot after cs1.
        let snapshot_encrypted =
            create_snapshot(&db_source, temp.path(), &synced_tables()).expect("snapshot");

        // More data after snapshot, captured as cs2.
        let mut session2 = RqSession::new(&db_source).expect("session2");
        for t in synced_tables() {
            session2.attach(Some(t.name())).expect("attach");
        }
        exec(
            &db_source,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a3', 'Artist Three', 1, '0000000003000-0000-dev1', '2026-01-01')",
        );
        exec(
            &db_source,
            "UPDATE notes SET title = 'Artist One Updated' WHERE id = 'a1'",
        );
        let mut cs2_bytes = Vec::new();
        session2.changeset_strm(&mut cs2_bytes).expect("cs2");
        drop(session2);

        // --- Path A: bootstrap from snapshot + apply cs2 ---
        let snapshot_plain = snapshot_encrypted;
        let path_a = temp.path().join("path_a.db");
        std::fs::write(&path_a, &snapshot_plain).unwrap();
        let db_a = open_db_at(&path_a);
        apply(&db_a, &cs2_bytes);

        // --- Path B: fresh DB + apply cs1 + apply cs2 ---
        let db_b = synced_conn();
        apply(&db_b, &cs1_bytes);
        apply(&db_b, &cs2_bytes);

        // --- Compare: both paths should have identical data ---
        let count_a = query_int(&db_a, "SELECT COUNT(*) FROM notes");
        let count_b = query_int(&db_b, "SELECT COUNT(*) FROM notes");
        assert_eq!(count_a, count_b, "artist count should match");
        assert_eq!(count_a, 3);

        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a1'"),
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'a1'")
        );
        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a1'"),
            "Artist One Updated"
        );
        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a3'"),
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'a3'")
        );
        assert_eq!(
            query_text(&db_a, "SELECT title FROM notes WHERE id = 'a3'"),
            "Artist Three"
        );
    }

    /// A generation's db image written but no pointer is a crashed publish: the
    /// process died after the db/meta uploads but before the pointer flip. The
    /// pointer is the commit, so bootstrap finds none and refuses — it never seeds
    /// cursors from a heuristic on `heads`, and never adopts the orphan db.
    #[tokio::test]
    async fn bootstrap_fails_when_pointer_missing() {
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a1', 'Artist One', 1, '0000000001000-0000-dev1', '2026-01-01')",
        );
        let temp = tempfile::tempdir().unwrap();
        let encrypted = create_snapshot(&db, temp.path(), &synced_tables()).expect("snapshot");

        // Write a generation's db image under some device's keyspace but never
        // publish a pointer naming it (the crash-mid-publish state).
        let storage = MockSyncStorage::new();
        storage
            .put_snapshot("dev1pubkey", 7, encrypted)
            .await
            .unwrap();
        storage
            .put_head("dev-1", 20, Some(15), "2026-02-10T00:00:00Z")
            .await
            .unwrap();

        let target = temp.path().join("torn.db");
        let err = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect_err("bootstrap must refuse a generation with no pointer");
        assert!(
            matches!(err, SnapshotError::Bucket(StorageError::NotFound(_))),
            "expected Bucket(NotFound), got {err:?}",
        );
        assert!(
            !target.exists(),
            "no DB should be written when no pointer is published"
        );
    }

    /// THE torn-read guarantee. A second device begins publishing a newer
    /// generation and writes its DB image, but crashes (or simply hasn't yet)
    /// before writing that generation's meta and flipping the pointer. The bucket
    /// now holds a NEW db with no matching meta alongside the live (old)
    /// generation — an interleave/crash window in which a reader must resolve the
    /// pointer's complete generation and never pair the new db with the old meta.
    ///
    /// With the pointer, a bootstrapping device always resolves the generation the
    /// pointer names — a complete, self-consistent (db, meta) pair — and never the
    /// orphan db. It adopts the old generation's contents and cursors, untorn.
    #[tokio::test]
    async fn bootstrap_resolves_a_consistent_generation_despite_an_orphan_db() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // The live generation A (seq 5): a DB containing 'old', cursors {self: 5}.
        let db_a = synced_conn();
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'old', 1, '0000000001000-0000-self', '2026-01-01')",
        );
        let snap_a = create_snapshot(&db_a, temp.path(), &synced_tables()).expect("snap A");
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            snap_a,
            "self",
            HashMap::new(),
            5,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("publish generation A");

        // A newer generation B (seq 9) whose DB contains 'new' is written, but its
        // meta and the pointer flip never happen (a crashed/concurrent publish).
        // This is the orphan a reader must never pair with A's meta.
        let db_b = synced_conn();
        exec(
            &db_b,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n2', 'new', 1, '0000000002000-0000-self', '2026-01-01')",
        );
        let snap_b = create_snapshot(&db_b, temp.path(), &synced_tables()).expect("snap B");
        storage.put_snapshot("selfpubkey", 9, snap_b).await.unwrap();

        // Bootstrap resolves the pointer (still naming A) and adopts A's consistent
        // generation — the 'old' row, cursor {self: 5} — never B's orphan db.
        let target = temp.path().join("boot.db");
        let boot = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap resolves the consistent generation A");
        assert_eq!(
            boot.cursors.get("self"),
            Some(&5),
            "cursors come from the pointed generation's meta, not the orphan",
        );
        let restored = open_db_at(&target);
        assert!(
            row_exists(&restored, "SELECT 1 FROM notes WHERE id = 'n1'"),
            "the live generation's row is adopted",
        );
        assert!(
            !row_exists(&restored, "SELECT 1 FROM notes WHERE id = 'n2'"),
            "the orphan generation's db is never served (no torn read)",
        );
    }

    /// THE strand the per-author keyspace makes impossible. The bucket has no lock,
    /// so two devices publish concurrently — and the snapshot seq is each device's
    /// own `local_seq`, not a global id, so they can collide on the SAME seq. Device
    /// A is live at seq 5 (A authored it). Device B then writes its own generation at
    /// the SAME seq 5 — its db and meta are present — but has NOT yet flipped the
    /// pointer to it (B is mid-publish; B's seq 5 is not-yet-live). Device A's
    /// post-publish sweep, keyed by A's own pubkey, runs in this window.
    ///
    /// A lists only its OWN `snapshot/{author_a}/` keyspace, so B's seq 5 — under
    /// `snapshot/{author_b}/` — is not even a candidate: it survives untouched, and
    /// when B flips the pointer to it a joiner can still resolve it. A device never
    /// strands a peer's generation because it can only name objects under its own
    /// prefix, so two devices publishing the same seq hold distinct objects under
    /// distinct keys, never a single shared one.
    #[tokio::test]
    async fn sweep_never_deletes_a_peer_authored_same_seq_generation() {
        let storage = MockSyncStorage::new();
        let kp_a = test_keypair();
        let kp_b = test_keypair();
        let author_a = hex::encode(kp_a.public_key());
        let author_b = hex::encode(kp_b.public_key());

        // Device A publishes generation 5 and the pointer names it (A is live).
        publish_signed_generation(
            &storage,
            5,
            BTreeMap::<String, u64>::new(),
            vec![0xAu8],
            &kp_a,
        )
        .await;

        // Device B writes its own generation at the SAME seq 5's db + meta but does
        // NOT flip the pointer: B's seq 5 is present yet not-yet-live, under B's own
        // keyspace. (publish_signed_generation would flip the pointer, so write the
        // two objects directly to model the mid-publish window before the commit.)
        let db_b = vec![0xBu8];
        let db_hash_b = snapshot_db_hash(&db_b);
        let meta_b = SnapshotMetaJson::signed(
            TEST_LIBRARY_ID,
            std::collections::BTreeMap::<String, u64>::new(),
            db_hash_b,
            0,
            &kp_b,
        );
        storage
            .put_snapshot_meta(&author_b, 5, serde_json::to_vec(&meta_b).unwrap())
            .await
            .unwrap();
        storage
            .put_snapshot(&author_b, 5, db_b.clone())
            .await
            .unwrap();

        // A's seq-5 generation and B's seq-5 generation are DISTINCT objects: each
        // device's keyspace has exactly its own, and A's db image is untouched by B's
        // same-seq publish.
        assert_eq!(
            storage
                .list_own_snapshot_generations(&author_a)
                .await
                .unwrap(),
            vec![5],
            "A's keyspace holds A's seq 5",
        );
        assert_eq!(
            storage
                .list_own_snapshot_generations(&author_b)
                .await
                .unwrap(),
            vec![5],
            "B's keyspace holds B's same-seq generation, a distinct object",
        );
        // Each device's db image is its own bytes — neither same-seq publish aliased
        // the other.
        assert_eq!(
            storage.get_snapshot(&author_a, 5).await.unwrap(),
            vec![0xAu8],
            "A's same-seq db image is its own bytes, not overwritten by B's publish",
        );
        assert_eq!(
            storage.get_snapshot(&author_b, 5).await.unwrap(),
            db_b,
            "B's same-seq db image is its own bytes, not overwritten by A's publish",
        );
        assert_eq!(
            storage.current_snapshot_seq().await,
            Some(5),
            "the pointer still names A's generation; B is mid-publish",
        );

        // Device A's real sweep, keyed by A's pubkey, with A's just-published seq. It
        // lists only A's keyspace, so B's same-seq generation is never a candidate and
        // survives.
        delete_superseded_generations(&storage, 5, &author_a)
            .await
            .expect("A's sweep runs");
        assert_eq!(
            storage
                .list_own_snapshot_generations(&author_b)
                .await
                .unwrap(),
            vec![5],
            "A's sweep must not touch B's same-seq not-yet-live generation",
        );
        assert_eq!(
            storage.get_snapshot(&author_b, 5).await.unwrap(),
            db_b,
            "B's same-seq db image survives A's sweep intact",
        );
    }

    /// A device reclaims its OWN superseded generation. The device published
    /// generation 1, then generation 2 (the pointer now names 2). Its own
    /// post-publish sweep — keyed by its pubkey, just-published seq 2 — deletes its
    /// superseded generation 1 (in its keyspace, not live, not just-published), and
    /// keeps the live generation 2.
    ///
    /// The keyspace is what scopes the sweep: the same sweep keyed by a *different*
    /// device's pubkey lists that other (here empty) keyspace and reclaims nothing,
    /// so generation 1 survives — a device only ever deletes within the keyspace it
    /// owns.
    #[tokio::test]
    async fn sweep_deletes_its_own_superseded_generation() {
        let storage = MockSyncStorage::new();
        let kp = test_keypair();
        let own_author = hex::encode(kp.public_key());

        // The device publishes generation 1, then 2; the pointer now names 2 and
        // generation 1 is its own superseded generation. Both authored by `kp`.
        publish_signed_generation(&storage, 1, BTreeMap::<String, u64>::new(), vec![1u8], &kp)
            .await;
        publish_signed_generation(&storage, 2, BTreeMap::<String, u64>::new(), vec![2u8], &kp)
            .await;

        // A sweep keyed by a stranger's pubkey lists the stranger's keyspace — empty
        // — so it reclaims nothing, and this device's generation 1 survives: a device
        // structurally cannot reach another device's keyspace.
        let stranger = hex::encode(test_keypair().public_key());
        delete_superseded_generations(&storage, 2, &stranger)
            .await
            .expect("stranger-keyed sweep runs");
        let mut after_foreign = storage
            .list_own_snapshot_generations(&own_author)
            .await
            .unwrap();
        after_foreign.sort_unstable();
        assert_eq!(
            after_foreign,
            vec![1, 2],
            "a sweep keyed by another device's pubkey lists that empty keyspace and reclaims nothing",
        );

        // The real sweep, keyed by this device's own pubkey, reclaims its superseded
        // generation 1 and keeps the live generation 2.
        delete_superseded_generations(&storage, 2, &own_author)
            .await
            .expect("own sweep runs");
        assert_eq!(
            storage
                .list_own_snapshot_generations(&own_author)
                .await
                .unwrap(),
            vec![2],
            "the device's own superseded generation is reclaimed, the live one kept",
        );
    }

    /// The just-published generation is never deleted. The device authored
    /// generations 1 (superseded) and 2 (live by construction). A sweep keyed by
    /// its pubkey after publishing generation 2 keeps 2 and deletes the older own
    /// generation.
    #[tokio::test]
    async fn sweep_never_deletes_the_just_published_generation() {
        let storage = MockSyncStorage::new();
        let kp = test_keypair();
        let own_author = hex::encode(kp.public_key());

        // Generations 1 (superseded) and 2 (live), both authored by this device.
        publish_signed_generation(&storage, 1, BTreeMap::<String, u64>::new(), vec![1u8], &kp)
            .await;
        publish_signed_generation(&storage, 2, BTreeMap::<String, u64>::new(), vec![2u8], &kp)
            .await;

        // Sweep after publishing seq 2: the published generation remains, and the
        // older own generation is reclaimed.
        delete_superseded_generations(&storage, 2, &own_author)
            .await
            .expect("sweep runs");
        let after = storage
            .list_own_snapshot_generations(&own_author)
            .await
            .unwrap();
        assert_eq!(
            after,
            vec![2],
            "the published generation remains and older own generations are reclaimed",
        );
    }

    /// The just-published seq protects the live generation within this device's
    /// own keyspace. Pointer bytes are not part of this sweep's liveness decision:
    /// after publishing generation 2, generation 1 is superseded even if the pointer
    /// object later becomes unreadable.
    #[tokio::test]
    async fn sweep_reclaims_superseded_generation_when_pointer_is_unreadable() {
        let storage = MockSyncStorage::new();
        let kp = test_keypair();
        let own_author = hex::encode(kp.public_key());

        // Two own generations: 1 (superseded) and 2 (the pointer names it).
        publish_signed_generation(&storage, 1, BTreeMap::<String, u64>::new(), vec![1u8], &kp)
            .await;
        publish_signed_generation(&storage, 2, BTreeMap::<String, u64>::new(), vec![2u8], &kp)
            .await;

        // Overwrite the pointer with bytes that neither parse nor verify.
        storage
            .put_snapshot_pointer(b"not a valid signed pointer".to_vec())
            .await
            .unwrap();

        // Generation 2 is protected because it is the just-published generation.
        // Generation 1 is superseded within this device's keyspace.
        delete_superseded_generations(&storage, 2, &own_author)
            .await
            .expect("sweep runs");
        let after = storage
            .list_own_snapshot_generations(&own_author)
            .await
            .unwrap();
        assert_eq!(
            after,
            vec![2],
            "the superseded generation is reclaimed without reading the pointer",
        );
    }

    /// Two devices publish at the SAME seq and BOTH generations persist as
    /// independent objects in their own keyspaces. A bootstrap resolves the
    /// generation the live pointer names and adopts its catalog; the other device's
    /// same-seq generation is an untouched, independently-resolvable object. This is
    /// the global uniqueness the per-author keyspace buys: a same-`local_seq`
    /// collision across devices no longer aliases one object.
    #[tokio::test]
    async fn two_devices_same_seq_keep_both_generations() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();
        let kp_a = test_keypair();
        let kp_b = test_keypair();
        let author_a = hex::encode(kp_a.public_key());
        let author_b = hex::encode(kp_b.public_key());

        // Device A publishes a real generation at seq 7 (its catalog has 'a-row'),
        // and the pointer names A's generation (A is the live publisher).
        let db_a = synced_conn();
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a-row', 'A', 1, '0000000001000-0000-A', '2026-01-01')",
        );
        let snap_a = create_snapshot(&db_a, temp.path(), &synced_tables()).expect("snap A");
        publish_signed_generation(
            &storage,
            7,
            BTreeMap::from([("A".to_string(), 7)]),
            snap_a,
            &kp_a,
        )
        .await;

        // Device B builds its own generation at the SAME seq 7 (its catalog has
        // 'b-row') and writes the db + meta under B's keyspace, WITHOUT flipping the
        // pointer (A stays live). Keyed under B's own author, it is a distinct object
        // from A's seq-7 generation.
        let db_b = synced_conn();
        exec(
            &db_b,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('b-row', 'B', 1, '0000000002000-0000-B', '2026-01-01')",
        );
        let snap_b = create_snapshot(&db_b, temp.path(), &synced_tables()).expect("snap B");
        let db_hash_b = snapshot_db_hash(&snap_b);
        let meta_b = SnapshotMetaJson::signed(
            TEST_LIBRARY_ID,
            std::collections::BTreeMap::from([("B".to_string(), 7)]),
            db_hash_b,
            0,
            &kp_b,
        );
        storage
            .put_snapshot_meta(&author_b, 7, serde_json::to_vec(&meta_b).unwrap())
            .await
            .unwrap();
        storage.put_snapshot(&author_b, 7, snap_b).await.unwrap();

        // Both same-seq generations persist as independent objects, one per keyspace.
        assert_eq!(
            storage
                .list_own_snapshot_generations(&author_a)
                .await
                .unwrap(),
            vec![7],
            "A's keyspace holds A's seq 7",
        );
        assert_eq!(
            storage
                .list_own_snapshot_generations(&author_b)
                .await
                .unwrap(),
            vec![7],
            "B's keyspace holds B's same-seq generation, untouched by A's publish",
        );

        // The pointer names A's generation, so a joiner resolves and adopts A's
        // catalog — A's db image was never aliased by B's same-seq publish.
        let target = temp.path().join("boot.db");
        let boot = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap resolves A's live generation");
        assert_eq!(boot.cursors.get("A"), Some(&7));
        let restored = open_db_at(&target);
        assert!(
            row_exists(&restored, "SELECT 1 FROM notes WHERE id = 'a-row'"),
            "the live (A) generation's catalog is adopted",
        );
        assert!(
            !row_exists(&restored, "SELECT 1 FROM notes WHERE id = 'b-row'"),
            "B's same-seq generation is a separate object, not spliced into A's",
        );

        // B's generation remains independently resolvable: were B to flip the pointer
        // to its own `{author_b, 7}`, that generation's db+meta are intact under B's
        // keyspace (its db image hashes to what its signed meta commits to).
        let sealed_b = storage.get_snapshot(&author_b, 7).await.unwrap();
        assert_eq!(
            snapshot_db_hash(&sealed_b),
            meta_b.db_hash,
            "B's same-seq generation is whole and self-consistent in its own keyspace",
        );
    }

    // ---- snapshot cursor honesty (the overclaim bug) ----

    /// The core regression: a device that snapshots a DB it has NOT fully
    /// caught up to must record cursors describing what the snapshot DB
    /// actually contains — never another device's published head. If it
    /// overclaims, changeset reclamation deletes the un-snapshotted changeset and
    /// no future restore can recover it. With honest cursors, a restoring device
    /// bootstraps at the snapshot's seq and replays the post-snapshot edit forward.
    #[tokio::test]
    async fn snapshot_meta_reflects_applied_not_published() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // Owner device M inserts a note and is at applied seq K = 1.
        let k = 1u64;
        let cs_insert = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Release Draft', 1, '0000000001000-0000-M', '2026-01-01')",
            );
        });
        storage.store_changeset("M", k, &cs_insert, 0);

        // M later pushes a follow-up edit as seq K+1 = 2, raising M's
        // head to 2 — but this edit is NOT in any snapshot yet.
        let cs_update = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Release Draft', 1, '0000000001000-0000-M', '2026-01-01')",
            );
            exec(
                db,
                "UPDATE notes SET title = 'Release Managed', \
                 _updated_at = '0000000002000-0000-M' WHERE id = 'n1'",
            );
        });
        storage.store_changeset("M", k + 1, &cs_update, 0);

        // Device B is behind: it has applied M only up to K. B snapshots its state.
        let db_b = synced_conn();
        apply(&db_b, &cs_insert);
        let snapshot = create_snapshot(&db_b, temp.path(), &synced_tables()).expect("snapshot");

        let kp = test_keypair();
        let applied = HashMap::from([("M".to_string(), k)]);
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            snapshot,
            "B",
            applied,
            0,
            0,
            &kp,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let meta_json = storage.current_snapshot_meta().await.expect("meta");
        let meta: SnapshotMetaJson = serde_json::from_slice(&meta_json).unwrap();
        assert_eq!(
            meta.cursors.get("M"),
            Some(&k),
            "snapshot meta must reflect applied seq K, not published head K+1"
        );

        // The post-snapshot edit (seq K+1) is above the snapshot's honest cursor, so
        // it is preserved for a restoring device to replay.
        storage
            .get_changeset("M", k + 1)
            .await
            .expect("K+1 is above the snapshot cursor and must be preserved");

        let target = temp.path().join("device_c.db");
        let boot = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap");
        let c_cursor = *boot.cursors.get("M").unwrap_or(&0);

        let db_c = open_db_at(&target);
        for seq in storage.list_changesets("M").await.unwrap() {
            if seq <= c_cursor {
                continue;
            }
            let packed = storage.get_changeset("M", seq).await.unwrap();
            let (_env, bytes) = crate::sync::envelope::unpack(&packed).expect("unpack changeset");
            apply(&db_c, &bytes);
        }
        assert_eq!(
            query_text(&db_c, "SELECT title FROM notes WHERE id = 'n1'"),
            "Release Managed",
            "device C must receive the post-snapshot edit"
        );
    }

    /// End-to-end: owner inserts + snapshots, B bootstraps, owner pushes an
    /// UPDATE, B pulls it, B snapshots (honest meta), C bootstraps + pulls and
    /// also has the update. All through the real snapshot/GC/bootstrap funcs.
    #[tokio::test]
    async fn multi_device_managed_edit_reaches_restore() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // Owner inserts a note (seq 1) and snapshots its applied state.
        let cs1 = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Draft', 1, '0000000001000-0000-owner', '2026-01-01')",
            );
        });
        storage.store_changeset("owner", 1, &cs1, 0);

        let db_owner = synced_conn();
        apply(&db_owner, &cs1);
        let snap1 = create_snapshot(&db_owner, temp.path(), &synced_tables()).expect("snap1");
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            snap1,
            "owner",
            HashMap::new(),
            1,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push snap1");

        // Device B bootstraps and has the note.
        let b_path = temp.path().join("b.db");
        let b_boot = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &b_path)
            .await
            .expect("b bootstrap");
        let db_b = open_db_at(&b_path);
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Draft"
        );

        // Owner pushes a row-UPDATE changeset (seq 2).
        let cs2 = changeset_bytes_for(|db| {
            exec(
                db,
                "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
                 VALUES ('n1', 'Draft', 1, '0000000001000-0000-owner', '2026-01-01')",
            );
            exec(
                db,
                "UPDATE notes SET title = 'Published', \
                 _updated_at = '0000000002000-0000-owner' WHERE id = 'n1'",
            );
        });
        storage.store_changeset("owner", 2, &cs2, 0);

        // B pulls the update (everything past its bootstrap cursor).
        let mut b_cursors = b_boot.cursors.clone();
        let b_owner_cursor = *b_cursors.get("owner").unwrap_or(&0);
        for seq in storage.list_changesets("owner").await.unwrap() {
            if seq <= b_owner_cursor {
                continue;
            }
            let packed = storage.get_changeset("owner", seq).await.unwrap();
            let (_env, bytes) = crate::sync::envelope::unpack(&packed).expect("unpack changeset");
            apply(&db_b, &bytes);
            b_cursors.insert("owner".to_string(), seq);
        }
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Published"
        );

        // B snapshots its now-current state with honest cursors {owner: 2}.
        let snap2 = create_snapshot(&db_b, temp.path(), &synced_tables()).expect("snap2");
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            snap2,
            "B",
            b_cursors.clone(),
            0,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push snap2");

        // Device C bootstraps + pulls and must also have the update.
        let c_path = temp.path().join("c.db");
        let c_boot = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &c_path)
            .await
            .expect("c bootstrap");
        let db_c = open_db_at(&c_path);
        let c_owner_cursor = *c_boot.cursors.get("owner").unwrap_or(&0);
        for seq in storage.list_changesets("owner").await.unwrap() {
            if seq <= c_owner_cursor {
                continue;
            }
            let packed = storage.get_changeset("owner", seq).await.unwrap();
            let (_env, bytes) = crate::sync::envelope::unpack(&packed).expect("unpack changeset");
            apply(&db_c, &bytes);
        }
        assert_eq!(
            query_text(&db_c, "SELECT title FROM notes WHERE id = 'n1'"),
            "Published",
            "device C must receive the edit through B's snapshot + pull"
        );
    }

    /// A single-device library reclaims only seqs <= the snapshot's accurate cursor
    /// (no other current device, so the floor is just the snapshot cursor); a
    /// changeset pushed after the snapshot (absent from the snapshot DB) survives.
    #[tokio::test]
    async fn reclaim_never_deletes_changeset_absent_from_snapshot() {
        let storage = MockSyncStorage::new();
        let kp = test_keypair();
        for seq in 1..=3 {
            storage.store_changeset("M", seq, &[seq as u8], 0);
        }

        // Snapshot honestly covers M only through seq 2.
        let applied = HashMap::from([("M".to_string(), 2)]);
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            vec![0u8; 4],
            "M",
            applied,
            2,
            0,
            &kp,
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        reclaim_superseded_changesets(&storage, TEST_LIBRARY_ID, None)
            .await
            .expect("reclaim");

        assert_eq!(storage.list_changesets("M").await.unwrap(), vec![3]);
    }

    /// After bootstrap, the returned cursors never exceed what the snapshot DB
    /// actually contains — they equal the applied state the snapshot was taken
    /// from.
    #[tokio::test]
    async fn bootstrap_cursors_match_snapshot_contents() {
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // Snapshot taken from a state where M is applied through seq 7.
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'A', 1, '0000000001000-0000-M', '2026-01-01')",
        );
        let snap = create_snapshot(&db, temp.path(), &synced_tables()).expect("snap");

        let applied = HashMap::from([("M".to_string(), 7)]);
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            snap,
            "self",
            applied,
            0,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let target = temp.path().join("boot.db");
        let boot = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("bootstrap");
        assert_eq!(boot.cursors.get("M"), Some(&7));
    }

    /// A device that snapshots while another device's head is ahead writes
    /// cursors equal to its applied state, not the ahead head.
    #[tokio::test]
    async fn behind_device_snapshot_does_not_overclaim() {
        let storage = MockSyncStorage::new();

        // Device M's head is ahead at seq 9.
        storage.store_changeset("M", 9, &[9], 0);

        // The snapshotting device B has only applied M through seq 4.
        let applied = HashMap::from([("M".to_string(), 4)]);
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            vec![0u8; 4],
            "B",
            applied,
            0,
            0,
            &test_keypair(),
            &crate::clock::SystemClock,
        )
        .await
        .expect("push");

        let meta_json = storage.current_snapshot_meta().await.expect("meta");
        let meta: SnapshotMetaJson = serde_json::from_slice(&meta_json).unwrap();
        assert_eq!(
            meta.cursors.get("M"),
            Some(&4),
            "must record applied seq 4, not M's ahead head 9"
        );
    }
}

/// Snapshot authentication and authorization: a snapshot is held to the same bar
/// as a changeset before a joining/restoring device adopts it. These drive the
/// production-faithful [`crate::sync::test_helpers::MockSyncStorage`] (which
/// stores a real membership chain and signs through the production helpers), so
/// the forge they reproduce is exactly the bucket-writer attack — a non-member
/// who can write the bucket plants a catalog image or poisons the cursors.
#[cfg(test)]
mod authorization_tests {
    use super::*;
    use crate::keys::UserKeypair;
    use crate::sync::membership::{founder_entry, MemberRole, MembershipAction, MembershipChain};
    use crate::sync::test_helpers::{
        append_membership_entry, make_linked_entry, pubkey_hex, publish_membership_chain_head,
        MockSyncStorage,
    };

    /// A minimal snapshot DB image. The authorization checks operate on the
    /// metadata signature, the DB-hash binding, and the chain — none of which need
    /// a real SQLite image — so a fixed byte string stands in for the catalog.
    /// (The full create→push→bootstrap DB round-trip is covered in the sibling
    /// `tests` module; here the blob is just the thing the signature commits to.)
    fn fake_snapshot() -> Vec<u8> {
        b"catalog-image-bytes".to_vec()
    }

    /// Seed a one-owner founder chain into the mock and return the owner keypair.
    async fn found_chain(storage: &MockSyncStorage, owner: &UserKeypair) -> MembershipChain {
        let owner_pk = pubkey_hex(owner);
        let mut chain = MembershipChain::new();
        let entry = founder_entry(owner, "0000000001000-0000-owner");
        append_membership_entry(storage, &mut chain, &owner_pk, 1, entry).await;
        publish_membership_chain_head(storage, &chain, owner).await;
        chain
    }

    /// A snapshot signed by a current member (the owner) bootstraps: the signature
    /// verifies, the DB hash matches, and the author is an Owner of
    /// the chain anchored to the pinned owner.
    #[tokio::test]
    async fn bootstrap_accepts_snapshot_signed_by_member() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner pushes a signed snapshot");

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let boot = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect("a member-signed snapshot is adopted");
        assert_eq!(boot.cursors.get("owner-dev"), Some(&1));
        assert!(
            target.exists(),
            "the DB image is written for a valid snapshot"
        );
    }

    /// THE cross-library replay. A device that belongs to two libraries takes one
    /// library's catalog and re-seals it under the other's key, then re-signs the
    /// meta and pointer with its own (validly a member's) key and publishes them as
    /// the second library's live snapshot. Everything is internally valid — a real
    /// member's signature over a consistent generation — except the binding: the
    /// meta and pointer were signed for library X, and the joiner reads them as
    /// library Y. Because the signature covers `library_id`, re-verifying under Y's
    /// id fails, so the snapshot is refused and nothing is written — a generation
    /// signed for one library can never be adopted by another.
    #[tokio::test]
    async fn bootstrap_refuses_a_snapshot_bound_to_a_different_library() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        // The owner publishes a generation with valid signatures, db, and pointer —
        // but bound to library X.
        let library_x = "library-x";
        push_snapshot_without_blob_refs(
            &storage,
            library_x,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner publishes a generation bound to library X");

        // The joiner reads the same bucket as library Y (a different id). The meta
        // and pointer signatures were taken over X's id, so they don't verify under
        // Y — the replay is refused as a binding (signature) failure, even though
        // the author is a real member and the generation is internally consistent.
        let library_y = "library-y";
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err =
            bootstrap_from_snapshot(&storage, library_y, Some(&pubkey_hex(&owner)), 0, &target)
                .await
                .expect_err("a snapshot bound to a different library must be refused");
        assert!(
            matches!(err, SnapshotError::PointerSignatureInvalid),
            "expected PointerSignatureInvalid (the cross-library binding fails), got {err:?}",
        );
        assert!(
            !target.exists(),
            "no DB is written when the snapshot is bound to a different library",
        );
    }

    /// THE forge: a bucket writer who is not a member signs a snapshot with their
    /// own key and overwrites the objects. The author is checked against the
    /// membership chain, so the snapshot is refused and nothing is written — a
    /// snapshot is adopted only from a current Owner.
    #[tokio::test]
    async fn bootstrap_refuses_snapshot_signed_by_non_member() {
        let owner = UserKeypair::generate();
        let outsider = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        // The outsider forges a fully-signed snapshot+meta: valid signature, valid
        // DB hash — only the AUTHOR is unauthorized. This is exactly what a
        // bucket-write-capable non-member can produce.
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "evil-dev",
            HashMap::from([("victim".to_string(), u64::MAX)]),
            1,
            0,
            &outsider,
            &crate::clock::SystemClock,
        )
        .await
        .expect("the forged objects are written to the bucket");

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("a non-member-signed snapshot must be refused");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
        assert!(
            !target.exists(),
            "no DB is written when the snapshot author is unauthorized",
        );
    }

    /// THE repoint forge the pointer defends against. The owner publishes a valid
    /// generation; the db and meta are a real member's, signed and consistent. A
    /// non-member who can write the bucket overwrites ONLY the pointer with one
    /// they sign, naming that same generation and repeating its real db_hash — so
    /// the pointer/meta cross-check passes and the only thing wrong is the
    /// pointer's AUTHOR. Bootstrap authorizes the pointer's author against the
    /// chain, finds a non-member, and refuses: a non-member cannot repoint the live
    /// snapshot, even at an otherwise-valid generation. Nothing is written.
    #[tokio::test]
    async fn bootstrap_refuses_pointer_signed_by_non_member() {
        let owner = UserKeypair::generate();
        let outsider = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        // The owner publishes a real, member-signed generation at seq 1.
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner publishes a signed generation");

        // The outsider repoints: a pointer they sign, naming the real generation 1
        // (under the owner's keyspace) and committing to its real db_hash (so only the
        // author is illegitimate). The pointer keeps `author_pubkey = owner`, so it
        // still resolves the owner's generation; the outsider only re-signs it.
        let real_db_hash =
            snapshot_db_hash(&storage.get_snapshot(&pubkey_hex(&owner), 1).await.unwrap());
        let forged_pointer =
            SnapshotPointerJson::signed(TEST_LIBRARY_ID, 1, real_db_hash, &outsider);
        storage
            .put_snapshot_pointer(serde_json::to_vec(&forged_pointer).unwrap())
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("a non-member-signed pointer must be refused");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
        assert!(
            !target.exists(),
            "no DB is written when the pointer's author is not a member",
        );
    }

    /// A read-only Follower holds the library key (so it can seal a snapshot) but
    /// may not author a catalog image. A snapshot it signs is refused — the same
    /// write/read split the changeset path enforces.
    #[tokio::test]
    async fn bootstrap_refuses_snapshot_signed_by_follower() {
        let owner = UserKeypair::generate();
        let follower = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let mut chain = found_chain(&storage, &owner).await;
        // Owner adds the follower (read-only).
        let add = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &follower,
            MemberRole::Follower,
            "0000000002000-0000-owner",
        );
        append_membership_entry(&storage, &mut chain, &pubkey_hex(&owner), 2, add).await;
        publish_membership_chain_head(&storage, &chain, &owner).await;

        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "follower-dev",
            HashMap::new(),
            1,
            0,
            &follower,
            &crate::clock::SystemClock,
        )
        .await
        .expect("the follower writes a signed snapshot");

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("a follower-signed snapshot must be refused");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
        assert!(!target.exists());
    }

    /// Owner-only snapshots (#161): a snapshot restates the whole catalog, so even a
    /// write-capable Member may not author one — only an Owner. The owner's snapshot
    /// is adopted; a Member's, signed and internally consistent in every other way, is
    /// refused. Both the pointer and the meta authorize through `resolve_current_meta`,
    /// so the Member's is rejected at the pointer's author check before any DB image is
    /// fetched. (Contrast the changeset path, which a Member may still author.)
    #[tokio::test]
    async fn snapshot_authorization_is_owner_only() {
        let owner = UserKeypair::generate();
        let member = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        let mut chain = found_chain(&storage, &owner).await;
        // The owner adds a write-capable Member.
        let add = make_linked_entry(
            &chain,
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-owner",
        );
        append_membership_entry(&storage, &mut chain, &pubkey_hex(&owner), 2, add).await;
        publish_membership_chain_head(&storage, &chain, &owner).await;

        // The owner's snapshot is adopted.
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner pushes a signed snapshot");

        let temp = tempfile::tempdir().unwrap();
        let owner_target = temp.path().join("owner.db");
        bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &owner_target,
        )
        .await
        .expect("an owner-signed snapshot is adopted");
        assert!(owner_target.exists());

        // The Member overwrites the snapshot with one it signs. Its changesets are
        // accepted (it is write-capable), but a catalog image is owner-only — so
        // bootstrap refuses, the pointer's author judged not a current Owner.
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "member-dev",
            HashMap::new(),
            1,
            0,
            &member,
            &crate::clock::SystemClock,
        )
        .await
        .expect("the member writes a signed snapshot");

        let member_target = temp.path().join("member.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &member_target,
        )
        .await
        .expect_err("a member-signed snapshot must be refused");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
        assert!(!member_target.exists());
    }

    /// The DB image and the metadata are bound by one signature: a bucket writer
    /// who swaps the snapshot DB for a different image (leaving the owner's signed
    /// meta in place) is caught by the DB-hash check, even though the author is a
    /// real member. Nothing is written.
    #[tokio::test]
    async fn bootstrap_refuses_tampered_snapshot_db() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner pushes a signed snapshot");

        // Substitute the DB image after the fact, overwriting the owner's own
        // generation object; the signed meta and pointer (and the hash they commit
        // to) are untouched, so the bytes no longer match.
        storage
            .put_snapshot(
                &pubkey_hex(&owner),
                1,
                b"a-different-forged-catalog".to_vec(),
            )
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("a substituted DB image must be refused");
        assert!(
            matches!(err, SnapshotError::DbHashMismatch),
            "expected DbHashMismatch, got {err:?}",
        );
        assert!(!target.exists());
    }

    /// The pointer and the metadata each commit to the generation's DB hash, so a
    /// reader refuses a generation whose pointer and meta disagree on the hash. With
    /// each device's generations in its own keyspace a same-seq cross-device
    /// collision can no longer produce this, so the surviving threat is a TAMPER: a
    /// bucket writer who is a member re-signs the live pointer over a DIFFERENT db
    /// hash while leaving the generation's signed meta in place. The hashes disagree,
    /// the spliced pair is rejected at resolution before any DB image is downloaded,
    /// and nothing is written — the db_hash bind defends the pointer↔meta agreement.
    #[tokio::test]
    async fn bootstrap_refuses_a_pointer_committing_to_a_tampered_db_hash() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        // A complete, self-consistent generation at seq 1 (meta + db + pointer agree).
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner pushes a signed snapshot");

        // A bucket writer (here a member) overwrites ONLY the pointer with one they
        // re-sign over a DIFFERENT db image's hash, naming the same `{owner, seq 1}`.
        // The bucket now holds a pointer and a meta naming generation 1 but committing
        // to different hashes — a spliced generation a reader must refuse.
        let other_hash = snapshot_db_hash(b"a-different-devices-catalog");
        let spliced_pointer = SnapshotPointerJson::signed(TEST_LIBRARY_ID, 1, other_hash, &owner);
        storage
            .put_snapshot_pointer(serde_json::to_vec(&spliced_pointer).unwrap())
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("a pointer/meta hash mismatch must be refused");
        assert!(
            matches!(err, SnapshotError::PointerMetaMismatch),
            "expected PointerMetaMismatch, got {err:?}",
        );
        assert!(!target.exists());
    }

    /// The cursors are control input (a bootstrapping device starts pulling each
    /// peer past them), so they are signature-covered. An attacker who edits a
    /// cursor in the owner's signed meta — the bootstrap-skip / GC-mass-delete
    /// primitive — breaks the signature and the meta is refused.
    #[tokio::test]
    async fn bootstrap_refuses_cursor_poisoned_meta() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::from([("peer".to_string(), 5)]),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner pushes a signed snapshot");

        // Take the owner's signed meta and poison a cursor, leaving the author and
        // signature as the owner's. The signature no longer covers these bytes. The
        // poisoned meta keeps the original db_hash, so it still agrees with the
        // pointer — the refusal is the meta's own signature check, not the
        // pointer/meta cross-check.
        let meta_json = storage
            .get_snapshot_meta(&pubkey_hex(&owner), 1)
            .await
            .unwrap();
        let mut meta: SnapshotMetaJson = serde_json::from_slice(&meta_json).unwrap();
        meta.cursors.insert("peer".to_string(), u64::MAX);
        storage
            .put_snapshot_meta(&pubkey_hex(&owner), 1, serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("a cursor-poisoned meta must be refused");
        assert!(
            matches!(err, SnapshotError::MetaSignatureInvalid),
            "expected MetaSignatureInvalid, got {err:?}",
        );
        assert!(!target.exists());
    }

    /// Restore pins no owner up front: the chain is anchored to its own founder and
    /// the author must be a current Owner of it. An owner-signed
    /// snapshot is adopted (owner = None), while a non-member-signed one is still
    /// refused — so the trust-on-first-use restore path is not a hole.
    #[tokio::test]
    async fn restore_path_authorizes_against_the_chains_own_founder() {
        let owner = UserKeypair::generate();
        let outsider = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        // Member-signed snapshot: adopted even with no pinned owner.
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner pushes a signed snapshot");

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("restore.db");
        bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target)
            .await
            .expect("restore adopts a member-signed snapshot anchored to the chain founder");
        assert!(target.exists());

        // Now an outsider overwrites the snapshot. Even with no pinned owner, the
        // author is judged against the chain and refused.
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "evil-dev",
            HashMap::new(),
            1,
            0,
            &outsider,
            &crate::clock::SystemClock,
        )
        .await
        .expect("outsider overwrites the snapshot objects");

        let target2 = temp.path().join("restore2.db");
        let err = bootstrap_from_snapshot(&storage, TEST_LIBRARY_ID, None, 0, &target2)
            .await
            .expect_err("restore must refuse a non-member-signed snapshot");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
        assert!(!target2.exists());
    }

    /// Changeset reclamation trusts the metadata's cursors to decide what to delete
    /// fleet-wide, so it authenticates the meta first. A meta signed by a non-member
    /// is refused rather than consumed — closing the forged-cursor mass-delete
    /// primitive on the reclamation path too.
    #[tokio::test]
    async fn reclaim_refuses_non_member_signed_meta() {
        let owner = UserKeypair::generate();
        let outsider = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "evil-dev",
            HashMap::from([("victim".to_string(), u64::MAX)]),
            1,
            0,
            &outsider,
            &crate::clock::SystemClock,
        )
        .await
        .expect("outsider writes a forged meta");

        let err =
            reclaim_superseded_changesets(&storage, TEST_LIBRARY_ID, Some(&pubkey_hex(&owner)))
                .await
                .expect_err("reclamation must refuse a non-member-signed meta before deleting");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
    }

    /// An owner is pinned (an opaque library) but the `membership/*` listing is
    /// empty — a wiped chain under an otherwise-intact bucket. There is no founder
    /// to anchor to, so even a snapshot whose signature verifies cannot be
    /// authorized: bootstrap refuses rather than adopting it. (The chain-less path
    /// that accepts on signature alone is ONLY the no-pinned-owner browsable case;
    /// a pinned owner with no chain is the takeover this guards.)
    #[tokio::test]
    async fn bootstrap_refuses_empty_membership_when_owner_pinned() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        // Deliberately do NOT found a chain: the listing is empty.

        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "owner-dev",
            HashMap::new(),
            1,
            0,
            &owner,
            &crate::clock::SystemClock,
        )
        .await
        .expect("owner pushes a signed snapshot");

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("an empty chain under a pinned owner must be refused");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
        assert!(
            !target.exists(),
            "no DB is written when the chain is wiped under a pinned owner",
        );
    }

    /// The forged-chain / takeover case on the join path: the bucket carries a
    /// chain founded by one key, and the snapshot is signed by a current member of
    /// THAT chain — but the joiner pins a DIFFERENT owner (the founder its invite
    /// names). The chain is not anchored to the pinned owner, so it cannot
    /// authorize anyone and the snapshot is refused, even though its author is a
    /// member of the chain actually present.
    #[tokio::test]
    async fn bootstrap_refuses_chain_founded_by_a_different_owner() {
        let chain_founder = UserKeypair::generate();
        let pinned_owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        // The bucket's chain is founded by `chain_founder`, not the pinned owner.
        found_chain(&storage, &chain_founder).await;

        // The snapshot is signed by the chain's own founder — a valid member of the
        // chain that exists, the strongest forge a takeover can mount.
        push_snapshot_without_blob_refs(
            &storage,
            TEST_LIBRARY_ID,
            fake_snapshot(),
            "founder-dev",
            HashMap::new(),
            1,
            0,
            &chain_founder,
            &crate::clock::SystemClock,
        )
        .await
        .expect("the chain founder pushes a signed snapshot");

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        // The joiner pins the owner its invite names — a different key.
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&pinned_owner)),
            0,
            &target,
        )
        .await
        .expect_err("a chain not anchored to the pinned owner must be refused");
        assert!(
            matches!(err, SnapshotError::UnauthorizedAuthor(_)),
            "expected UnauthorizedAuthor, got {err:?}",
        );
        assert!(
            !target.exists(),
            "no DB is written when the chain is founded by a different owner",
        );
    }

    /// Snapshot metadata lacking the signed fields — a bare cursor map with no
    /// `author_pubkey`/`signature` — must not be adopted: it fails to deserialize
    /// into `SnapshotMetaJson`, so bootstrap refuses with the parse error rather
    /// than treating an unsigned, unauthored catalog as trusted. A valid signed
    /// pointer (so resolution gets past the pointer) names the generation whose
    /// meta is the unsigned shape.
    #[tokio::test]
    async fn bootstrap_refuses_unsigned_meta() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;
        let owner_hex = pubkey_hex(&owner);
        let sealed = fake_snapshot();
        let db_hash = snapshot_db_hash(&sealed);
        storage.put_snapshot(&owner_hex, 1, sealed).await.unwrap();

        // A valid owner-signed pointer naming generation 1 (under the owner's
        // keyspace) — the pointer step passes, so the refusal lands on the meta.
        let pointer = SnapshotPointerJson::signed(TEST_LIBRARY_ID, 1, db_hash, &owner);
        storage
            .put_snapshot_pointer(serde_json::to_vec(&pointer).unwrap())
            .await
            .unwrap();

        // Generation 1's metadata with none of the signing fields: a bare cursor map.
        let unsigned = serde_json::json!({
            "cursors": { "owner-dev": 1 },
        });
        storage
            .put_snapshot_meta(&owner_hex, 1, serde_json::to_vec(&unsigned).unwrap())
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            TEST_LIBRARY_ID,
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
        .await
        .expect_err("unsigned metadata must not be adopted");
        // The unsigned shape lacks the signed fields, so it fails to parse — refused
        // before any signature/authorization step on the meta, never silently
        // accepted.
        assert!(
            matches!(err, SnapshotError::Parse(_)),
            "expected Parse, got {err:?}",
        );
        assert!(!target.exists());
    }
}

/// Changeset reclamation's ack-floor: for each current device `D`, reclaim
/// `changes/D/{seq}` for `seq <= min(snapshot.cursors[D], min over OTHER current
/// devices of their acked cursor on D)`. These drive the
/// [`crate::sync::test_helpers::MockSyncStorage`], which models a membership
/// chain, per-device heads, signed acks, and the changeset/snapshot keyspaces.
#[cfg(test)]
mod reclaim_tests {
    use super::*;
    use crate::keys::UserKeypair;
    use crate::sync::membership::{founder_entry, MemberRole, MembershipAction, MembershipChain};
    use crate::sync::test_helpers::{
        append_membership_entry, make_linked_entry, pubkey_hex, publish_membership_chain_head,
        MockSyncStorage,
    };

    /// A device: its head slot (`id`) plus the membership keypair its head and ack
    /// are authored by. Changeset reclamation matches each device's ack author
    /// against its head author, so the two must share a key — as a real device's do.
    struct Device {
        id: String,
        kp: UserKeypair,
    }

    impl Device {
        fn new(id: &str) -> Self {
            Device {
                id: id.to_string(),
                kp: UserKeypair::generate(),
            }
        }
    }

    /// Found a one-owner chain and return the owner keypair. The owner authors the
    /// snapshot (a current owner); it has no head, so it is not itself a
    /// current device.
    async fn found_chain(storage: &MockSyncStorage) -> (UserKeypair, MembershipChain) {
        let owner = UserKeypair::generate();
        let owner_pk = pubkey_hex(&owner);
        let mut chain = MembershipChain::new();
        let entry = founder_entry(&owner, "0000000001000-0000-owner");
        append_membership_entry(storage, &mut chain, &owner_pk, 1, entry).await;
        publish_membership_chain_head(storage, &chain, &owner).await;
        (owner, chain)
    }

    /// Append a membership entry authored by `owner` at chain seq `seq` (2..=9). A
    /// timestamp derived from `seq` keeps the entries lexically ordered, the order
    /// `MembershipChain::from_entries` sorts by.
    async fn append_entry(
        storage: &MockSyncStorage,
        chain: &mut MembershipChain,
        owner: &UserKeypair,
        action: MembershipAction,
        subject: &Device,
        seq: u64,
    ) {
        let ts = format!("000000000{seq}000-0000-owner");
        let entry = make_linked_entry(chain, owner, action, &subject.kp, MemberRole::Member, &ts);
        append_membership_entry(storage, chain, &pubkey_hex(owner), seq, entry).await;
        publish_membership_chain_head(storage, chain, owner).await;
    }

    /// Publish `device`'s pull-ack (`cursors`: peer_id -> seq), signed by its own
    /// key so its author matches its head.
    async fn publish_ack(storage: &MockSyncStorage, device: &Device, cursors: &[(&str, u64)]) {
        let cursors = cursors
            .iter()
            .map(|(device_id, seq)| (device_id.to_string(), *seq))
            .collect();
        let ack = AckJson::signed(&device.id, cursors, &device.kp);
        storage
            .put_ack(&device.id, serde_json::to_vec(&ack).unwrap())
            .await
            .unwrap();
    }

    /// Store changesets `changes/{device_id}/{seq}` for `seq` in `1..=n`.
    async fn add_log(storage: &MockSyncStorage, device_id: &str, n: u64) {
        for seq in 1..=n {
            storage
                .put_changeset(device_id, seq, vec![seq as u8])
                .await
                .unwrap();
        }
    }

    async fn reclaim(storage: &MockSyncStorage, owner: &UserKeypair) -> GcResult {
        reclaim_superseded_changesets(storage, TEST_LIBRARY_ID, Some(&pubkey_hex(owner)))
            .await
            .expect("reclaim")
    }

    #[tokio::test]
    async fn reclaim_loads_snapshot_membership_once() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 1).await;
        storage.publish_head_as("A", 1, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 1)], vec![0u8], &owner).await;
        publish_ack(&storage, &b, &[("A", 1)]).await;

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 1);
        assert_eq!(
            (
                storage.membership_list_count(),
                storage.membership_get_count()
            ),
            (1, 3)
        );
    }

    /// 1. Behind member kept: the snapshot covers A->5, but the one other current
    ///    device acks A->3, so the floor is 3 — 1..=3 are reclaimed, 4..=5 survive.
    #[tokio::test]
    async fn behind_member_pins_the_floor() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 5)], vec![0u8], &owner).await;
        publish_ack(&storage, &b, &[("A", 3)]).await;

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 3);
        assert_eq!(storage.list_changesets("A").await.unwrap(), vec![4, 5]);
    }

    /// 2. New-bootstrapper protection: every other current device acks A->5, but the
    ///    live snapshot's cursor is only 3 (no newer snapshot yet), so the floor is 3
    ///    — 4..=5 survive for a fresh device bootstrapping from that snapshot.
    #[tokio::test]
    async fn snapshot_cursor_pins_the_floor_for_bootstrappers() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 3)], vec![0u8], &owner).await;
        publish_ack(&storage, &b, &[("A", 5)]).await;

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 3);
        assert_eq!(storage.list_changesets("A").await.unwrap(), vec![4, 5]);
    }

    /// 3. Full reclaim: every other current device acks A->5 AND the snapshot covers
    ///    A->5, so the floor is 5 and the whole log is reclaimed.
    #[tokio::test]
    async fn full_reclaim_when_snapshot_and_acks_agree() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 5)], vec![0u8], &owner).await;
        publish_ack(&storage, &b, &[("A", 5)]).await;

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 5);
        assert!(storage.list_changesets("A").await.unwrap().is_empty());
    }

    /// 4. Missing ack pauses reclamation: a current member with a head but no ack
    ///    contributes cursor 0, pinning every floor to 0 — nothing is reclaimed.
    #[tokio::test]
    async fn missing_ack_pauses_reclamation() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 5)], vec![0u8], &owner).await;
        // B publishes no ack.

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 0);
        assert_eq!(
            storage.list_changesets("A").await.unwrap(),
            vec![1, 2, 3, 4, 5]
        );
    }

    /// 5. Removed member releases reclamation: same as the missing-ack case, but the
    ///    ack-less device is removed from the chain, so it is no longer a current
    ///    device and no longer counted — the floor returns to the snapshot cursor.
    #[tokio::test]
    async fn removed_member_releases_reclamation() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        append_entry(
            &storage,
            &mut chain,
            &owner,
            MembershipAction::Remove,
            &b,
            4,
        )
        .await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 5)], vec![0u8], &owner).await;
        // B (removed) publishes no ack.

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 5);
        assert!(storage.list_changesets("A").await.unwrap().is_empty());
    }

    /// 6. Non-member ack ignored: an ack planted in member B's slot but signed by a
    ///    non-member — claiming A->5 — is ignored (its author does not match B's
    ///    head), so the forged high cursor cannot raise the floor and trigger
    ///    deletion. B contributes 0, so nothing is reclaimed.
    #[tokio::test]
    async fn non_member_ack_is_ignored() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 5)], vec![0u8], &owner).await;

        // An outsider signs an ack for B's slot claiming B has pulled A->5.
        let outsider = UserKeypair::generate();
        let forged = AckJson::signed("B", BTreeMap::from([("A".to_string(), 5u64)]), &outsider);
        storage
            .put_ack("B", serde_json::to_vec(&forged).unwrap())
            .await
            .unwrap();

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 0);
        assert_eq!(
            storage.list_changesets("A").await.unwrap(),
            vec![1, 2, 3, 4, 5]
        );
    }

    /// 7. Single-device library: with no other current device the ack term is
    ///    unbounded, so the floor is just the snapshot cursor (3) — 1..=3 reclaimed,
    ///    4..=5 survive.
    #[tokio::test]
    async fn single_device_floor_is_the_snapshot_cursor() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        publish_signed_generation(&storage, 1, [("A", 3)], vec![0u8], &owner).await;

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 3);
        assert_eq!(storage.list_changesets("A").await.unwrap(), vec![4, 5]);
    }

    /// 8. Self-exclusion: A's own ack carries no entry for itself (a device does not
    ///    ack its own log). A is excluded from its own min, so its missing self-entry
    ///    does not force A's floor to 0 — the floor is B's ack on A (4), not 0.
    #[tokio::test]
    async fn device_is_excluded_from_its_own_floor() {
        let storage = MockSyncStorage::new();
        let (owner, mut chain) = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &mut chain, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_signed_generation(&storage, 1, [("A", 5)], vec![0u8], &owner).await;
        // A acks only its peer B (no self-entry for A); B acks A->4.
        publish_ack(&storage, &a, &[("B", 2)]).await;
        publish_ack(&storage, &b, &[("A", 4)]).await;

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 4);
        assert_eq!(storage.list_changesets("A").await.unwrap(), vec![5]);
    }
}
