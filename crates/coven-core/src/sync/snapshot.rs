/// Snapshots and garbage collection for the sync system.
///
/// Periodically, a device creates a full snapshot of the database via
/// `VACUUM INTO`, seals it through the home's [`CloudCipher`], and publishes it as
/// a generation under its own `{author}` (its hex public key): the DB image
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

use super::cloud_storage::CloudCipher;
use super::membership_ops::load_anchored_chain;
use super::session::SyncedTable;
use super::signed_control::{AckJson, SnapshotMetaJson, SnapshotPointerJson};
use super::storage::{StorageError, SyncStorage};
use crate::keys::UserKeypair;

/// Default: create a snapshot after this many changesets since the last one.
const SNAPSHOT_CHANGESET_THRESHOLD: u64 = 100;

/// Default: create a snapshot after this many hours since the last one.
const SNAPSHOT_HOURS_THRESHOLD: u64 = 24;

/// Error type for snapshot operations.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    #[error("VACUUM INTO failed: {0}")]
    VacuumFailed(String),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
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
    /// current write-capable member of the library's membership chain, or the
    /// chain itself is not anchored to the library's owner (a wiped/refounded
    /// chain). The snapshot is refused rather than adopted.
    #[error("snapshot author is not an authorized member: {0}")]
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
    #[error("unsupported snapshot file operation: {0}")]
    Unsupported(String),
}

pub trait SnapshotFiles: crate::MaybeThreadSafe {
    fn prepare_snapshot_path(&self, temp_dir: &Path) -> Result<std::path::PathBuf, SnapshotError>;
    fn cleanup_snapshot_path(&self, path: &Path);
    fn read_and_remove_snapshot(&self, path: &Path) -> Result<Vec<u8>, SnapshotError>;
    fn write_snapshot_db(&self, target_path: &Path, plaintext: &[u8]) -> Result<(), SnapshotError>;
}

#[cfg(not(target_arch = "wasm32"))]
static SNAPSHOT_FILES: std::sync::OnceLock<&'static dyn SnapshotFiles> = std::sync::OnceLock::new();

#[cfg(target_arch = "wasm32")]
thread_local! {
    static SNAPSHOT_FILES: std::cell::Cell<Option<&'static dyn SnapshotFiles>> =
        std::cell::Cell::new(None);
}

#[cfg(not(target_arch = "wasm32"))]
pub fn register_snapshot_files(files: &'static dyn SnapshotFiles) {
    let _ = SNAPSHOT_FILES.set(files);
}

#[cfg(target_arch = "wasm32")]
pub fn register_snapshot_files(files: &'static dyn SnapshotFiles) {
    SNAPSHOT_FILES.with(|slot| slot.set(Some(files)));
}

#[cfg(not(target_arch = "wasm32"))]
fn snapshot_files() -> Result<&'static dyn SnapshotFiles, SnapshotError> {
    if let Some(files) = SNAPSHOT_FILES.get().copied() {
        Ok(files)
    } else {
        #[cfg(test)]
        {
            Ok(&test_snapshot_files::TEST_SNAPSHOT_FILES)
        }
        #[cfg(not(test))]
        {
            Err(SnapshotError::Unsupported(
                "snapshot file backend is not registered".to_string(),
            ))
        }
    }
}

#[cfg(target_arch = "wasm32")]
fn snapshot_files() -> Result<&'static dyn SnapshotFiles, SnapshotError> {
    SNAPSHOT_FILES.with(|slot| slot.get()).ok_or_else(|| {
        SnapshotError::Unsupported("snapshot file backend is not registered".to_string())
    })
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod test_snapshot_files {
    use super::{SnapshotError, SnapshotFiles};
    use std::path::{Path, PathBuf};

    pub(super) static TEST_SNAPSHOT_FILES: TestSnapshotFiles = TestSnapshotFiles;

    pub(super) struct TestSnapshotFiles;

    impl SnapshotFiles for TestSnapshotFiles {
        fn prepare_snapshot_path(&self, temp_dir: &Path) -> Result<PathBuf, SnapshotError> {
            let snapshot_path = temp_dir.join("snapshot.db");
            match std::fs::remove_file(&snapshot_path) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(SnapshotError::Io(e)),
            }
            Ok(snapshot_path)
        }

        fn cleanup_snapshot_path(&self, path: &Path) {
            let _ = std::fs::remove_file(path);
        }

        fn read_and_remove_snapshot(&self, path: &Path) -> Result<Vec<u8>, SnapshotError> {
            let bytes = std::fs::read(path)?;
            self.cleanup_snapshot_path(path);
            Ok(bytes)
        }

        fn write_snapshot_db(
            &self,
            target_path: &Path,
            plaintext: &[u8],
        ) -> Result<(), SnapshotError> {
            if let Some(parent) = target_path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(target_path, plaintext)?;
            Ok(())
        }
    }
}

/// SHA-256 of a snapshot's stored (sealed) bytes, hex-encoded. The hash the
/// signed [`SnapshotMetaJson`] and [`SnapshotPointerJson`] both commit to, so the
/// same bytes that round-trip through a generation's db object are what the
/// signatures bind.
fn snapshot_db_hash(sealed: &[u8]) -> String {
    hex::encode(Sha256::digest(sealed))
}

/// Result of bootstrapping from a snapshot.
#[derive(Debug)]
pub struct BootstrapResult {
    /// Per-device cursors from the snapshot metadata.
    /// The bootstrapping device should use these as initial sync_cursors.
    pub cursors: HashMap<String, u64>,
}

/// Create a snapshot of the database as bytes sealed for storage.
///
/// Uses `VACUUM INTO` to create a clean copy of the database at a temp path,
/// then clears every non-synced table's data from that copy, reads the bytes,
/// seals them through the home's [`CloudCipher`] (encrypting for an encrypted
/// home, verbatim for a plaintext one), and returns the sealed blob.
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
    cipher: &CloudCipher,
) -> Result<Vec<u8>, SnapshotError> {
    // A snapshot with no synced set would either leak every local-only table or
    // clear the whole DB — both wrong. Refuse before doing any work.
    if tables.is_empty() {
        return Err(SnapshotError::NoSyncedTables);
    }

    let files = snapshot_files()?;
    let snapshot_path = files.prepare_snapshot_path(temp_dir)?;
    let path_str = snapshot_path
        .to_str()
        .expect("temp path should be valid UTF-8");

    // VACUUM INTO creates a clean, defragmented copy of the live database.
    let vacuum = format!("VACUUM INTO '{}'", path_str.replace('\'', "''"));
    if let Err(e) = conn.execute_batch(&vacuum) {
        files.cleanup_snapshot_path(&snapshot_path);
        return Err(SnapshotError::VacuumFailed(e.to_string()));
    }

    // The copy is a whole-DB byte image, so it still holds every local-only
    // table's data. Strip those before reading: open the copy as its own
    // connection and DELETE from every table outside the synced set.
    if let Err(e) = clear_local_only_tables(&snapshot_path, tables) {
        files.cleanup_snapshot_path(&snapshot_path);
        return Err(e);
    }

    // Read the cleared snapshot file and seal it for storage.
    let plaintext = files.read_and_remove_snapshot(&snapshot_path)?;

    let sealed = cipher.seal(&plaintext);

    info!(
        plaintext_size = plaintext.len(),
        sealed_size = sealed.len(),
        "created snapshot"
    );

    Ok(sealed)
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
    let mut tables = Vec::new();
    for row in rows {
        tables.push(row.map_err(|e| SnapshotError::ClearFailed(format!("step table list: {e}")))?);
    }
    Ok(tables)
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
    sealed_snapshot: Vec<u8>,
    device_id: &str,
    applied_cursors: HashMap<String, u64>,
    current_seq: u64,
    schema_version: u32,
    keypair: &UserKeypair,
    clock: &dyn crate::clock::Clock,
) -> Result<(), SnapshotError> {
    let size = sealed_snapshot.len();

    // This generation lives under this device's own keyspace, keyed by its public
    // key. The same value is what the signed meta/pointer carry as `author_pubkey`,
    // so the pointer's `{author, seq}` resolves straight to these objects.
    let own_author = hex::encode(keypair.public_key);

    // Hash the exact bytes we store, before they move into `put_snapshot`. Both
    // the signed meta and the signed pointer commit to this hash, so a reader that
    // downloads the generation re-hashes those same bytes and detects a
    // substituted image.
    let db_hash = snapshot_db_hash(&sealed_snapshot);

    // Write the DB image first, before anything lists or points at this
    // generation. A generation becomes a sweep/reader candidate only once its meta
    // exists (written next), and a reader resolves it only once the pointer names it
    // (written last) — so however large and slow this upload is, no reader sees it
    // mid-write. A crash here leaves an unlisted, unreferenced db: invisible to
    // readers and to the sweep; a later publish reusing this seq overwrites it,
    // otherwise it lingers (the sweep lists generations by meta, which this db lacks).
    storage
        .put_snapshot(&own_author, current_seq, sealed_snapshot)
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
    let meta_json =
        serde_json::to_vec(&meta).map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;

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
        serde_json::to_vec(&pointer).map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;
    storage.put_snapshot_pointer(pointer_json).await?;

    // Update the head to record this snapshot's coverage (snapshot_seq). The head's
    // `last_sync` stamp is the only thing here that still needs the wall clock.
    let timestamp = clock.now().to_rfc3339();
    storage
        .put_head(device_id, current_seq, Some(current_seq), &timestamp)
        .await?;

    // The pointer now names `current_seq`; older generations this device published
    // are superseded. Reclaim them — listing only this device's own keyspace, so a
    // peer's generation is never touched. A failure here is logged, not fatal — the
    // publish already succeeded; the leftover is unreferenced storage a later sweep
    // by this device reclaims, never a wrong state the reader can see.
    if let Err(e) =
        delete_superseded_generations(storage, library_id, current_seq, &own_author).await
    {
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

/// Reclaim superseded snapshot generations that THIS device published.
///
/// Each device's generations live under its own `{own_author}` keyspace, so the
/// sweep lists only that prefix ([`SyncStorage::list_own_snapshot_generations`]):
/// every candidate is by construction a generation this device wrote. There is no
/// per-candidate author check — the key prefix *is* the author, so a peer's
/// generation (under a different prefix) is never even a candidate, and the strand
/// a cross-device sweep would risk is structurally impossible.
///
/// Within this device's own generations, a delete is gated on two facts:
///
/// 1. **Not just-published.** The generation is not the one this caller just
///    published (`just_published_seq`).
/// 2. **Not live.** The generation is not the one the pointer names *right now*.
///    The pointer is re-read here; if it names this device's own `{own_author,
///    seq}`, that `seq` is protected. (A peer-authored live generation isn't in
///    this device's keyspace, so it can't be a candidate anyway — but re-reading
///    the pointer also covers this device having flipped to a newer generation
///    concurrently, e.g. a GC sweep racing a publish.)
///
/// A generation that is neither just-published nor live is unreferenced and safe to
/// delete: this device wrote it and is the only one that reclaims it, and it does
/// not sweep concurrently with its own publish (the sync loop runs cycles
/// serially), so the generation can no longer become live. This is space
/// reclamation of correct-but-superseded objects, not repair of a wrong state.
async fn delete_superseded_generations(
    storage: &dyn SyncStorage,
    library_id: &str,
    just_published_seq: u64,
    own_author: &str,
) -> Result<(), SnapshotError> {
    // Re-read the live pointer so a concurrent flip is observed, and establish which
    // of this device's own generations (if any) is the live one. The live generation
    // is this device's own only when the pointer resolves to an author equal to this
    // device; a peer-authored live generation is under a different prefix and is never
    // a candidate below. If the pointer can't be read or verified, liveness is
    // unknown — skip the sweep entirely rather than delete on incomplete information.
    // A later sweep, once a readable pointer is present, reclaims; deleting now could
    // remove a generation whose liveness we never confirmed.
    let live_own_seq: Option<u64> = match storage.get_snapshot_pointer().await {
        Ok(bytes) => match serde_json::from_slice::<SnapshotPointerJson>(&bytes) {
            Ok(pointer) if pointer.verify(library_id) => {
                if pointer.author_pubkey == own_author {
                    Some(pointer.seq)
                } else {
                    // The live generation is a peer's (different keyspace): no own
                    // generation is live, so every own one below is superseded.
                    None
                }
            }
            Ok(_) => {
                warn!("snapshot sweep: live pointer signature does not verify; skipping this sweep until a readable pointer is present");
                return Ok(());
            }
            Err(e) => {
                warn!(error = %e, "snapshot sweep: live pointer failed to parse; skipping this sweep until a readable pointer is present");
                return Ok(());
            }
        },
        Err(StorageError::NotFound(_)) => {
            // No pointer visible yet. This device just wrote one before sweeping, so
            // this is an eventual-consistency lag, not a permanent absence; liveness
            // can't be established this pass, so skip and let a later sweep reclaim.
            debug!("snapshot sweep: no live pointer visible; skipping this sweep");
            return Ok(());
        }
        Err(e) => return Err(SnapshotError::Bucket(e)),
    };

    for seq in storage.list_own_snapshot_generations(own_author).await? {
        // Never delete the live generation or the one we just published. Authorship
        // is already settled by the keyspace — every seq here is this device's own.
        if seq == just_published_seq || live_own_seq == Some(seq) {
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
    let entries = storage
        .list_membership_entries()
        .await
        .map_err(SnapshotError::Bucket)?;

    if entries.is_empty() {
        // No chain. For an owner-pinned (opaque) library this is a wiped
        // `membership/*` — a takeover attempt — so refuse. A library with no
        // pinned owner is browsable/open and legitimately has no chain.
        if let Some(owner) = owner_pubkey {
            return Err(SnapshotError::UnauthorizedAuthor(format!(
                "membership chain is empty but owner {owner} is pinned (wiped membership/*)"
            )));
        }
        // Chain-less, no owner pinned: a browsable/open library has no membership
        // to authorize against, so the object is accepted on its verified signature
        // alone (open by design, exactly as the pull keeps every head when no chain
        // exists). Log the skip so this authorization bail-out is visible rather
        // than a silent default.
        debug!(
            author = %author_pubkey,
            "snapshot author authorization skipped: library is chain-less (no membership, \
             no pinned owner), so authorization is not applicable"
        );
        return Ok(());
    }

    // Non-empty chain: load + validate it and anchor to the pinned owner (the same
    // load+anchor the pull cycle runs). A chain that won't validate, or one founded
    // by a key other than the pinned owner, refuses the snapshot.
    let chain = load_anchored_chain(storage, &entries, owner_pubkey)
        .await
        .map_err(|e| SnapshotError::UnauthorizedAuthor(e.to_string()))?;

    if !chain.is_owner_now(author_pubkey) {
        return Err(SnapshotError::UnauthorizedAuthor(format!(
            "author {author_pubkey} is not a current owner"
        )));
    }

    Ok(())
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
///    authorize *its* author too (the same write-capable bar).
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
) -> Result<(String, u64, SnapshotMetaJson), SnapshotError> {
    // The pointer is the entry point. Its absence means no snapshot has been
    // published (a brand-new library) or the pointer object is missing — either
    // way there is no consistent generation to resolve, surfaced as the bucket's
    // NotFound.
    let pointer_json = storage
        .get_snapshot_pointer()
        .await
        .map_err(SnapshotError::Bucket)?;
    let pointer: SnapshotPointerJson = serde_json::from_slice(&pointer_json)
        .map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;
    // Verifying under THIS library's id also refuses a pointer validly signed for a
    // different library (a member of two libraries replaying one's snapshot as the
    // other's): the signature was taken over the other library's id, so it fails
    // here as `PointerSignatureInvalid`.
    if !pointer.verify(library_id) {
        return Err(SnapshotError::PointerSignatureInvalid);
    }
    authorize_author(storage, &pointer.author_pubkey, owner_pubkey).await?;

    // Follow the pointer to the named generation's metadata — under the pointer's
    // own `{author_pubkey, seq}` keyspace — and authenticate it on its own terms
    // (the meta and the pointer are independently signed; in a normal publish the
    // same device authored both). Verifying under this library's id likewise
    // refuses a cross-library meta replay.
    let meta_json = storage
        .get_snapshot_meta(&pointer.author_pubkey, pointer.seq)
        .await
        .map_err(SnapshotError::Bucket)?;
    let meta: SnapshotMetaJson = serde_json::from_slice(&meta_json)
        .map_err(|e| SnapshotError::Io(std::io::Error::other(e)))?;
    if !meta.verify(library_id) {
        return Err(SnapshotError::MetaSignatureInvalid);
    }
    authorize_author(storage, &meta.author_pubkey, owner_pubkey).await?;

    // The pointer and the meta must describe the same image. They are written
    // together in one publish, so a mismatch means the generation was assembled
    // from objects of different pushes — refuse it.
    if pointer.db_hash != meta.db_hash {
        return Err(SnapshotError::PointerMetaMismatch);
    }

    Ok((pointer.author_pubkey, pointer.seq, meta))
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
    let meta = match resolve_current_meta(storage, library_id, owner_pubkey).await {
        Ok((_author, _seq, meta)) => meta,
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
    let entries = storage.list_membership_entries().await?;
    let members: Option<HashSet<String>> = if entries.is_empty() {
        None
    } else {
        let chain = load_anchored_chain(storage, &entries, owner_pubkey)
            .await
            .map_err(|e| SnapshotError::UnauthorizedAuthor(e.to_string()))?;
        Some(
            chain
                .current_members()
                .into_iter()
                .map(|(pk, _)| pk)
                .collect(),
        )
    };

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
        let snapshot_cursor = meta.cursors.get(&device.device_id).copied().unwrap_or(0);

        // The min over the OTHER current devices' acked cursor on this device. A
        // device with no verified ack, or whose ack has no entry for this device,
        // contributes 0. No other current device leaves the term unbounded.
        let mut ack_floor: Option<u64> = None;
        for other in &devices {
            if other.device_id == device.device_id {
                continue;
            }
            let term = other
                .ack
                .as_ref()
                .and_then(|cursors| cursors.get(&device.device_id).copied())
                .unwrap_or(0);
            ack_floor = Some(ack_floor.map_or(term, |current| current.min(term)));
        }

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
/// generation before touching disk, opens its DB through the home's
/// [`CloudCipher`] (decrypting for an encrypted home, verbatim for a plaintext
/// one), and writes the plaintext database to `target_path`. The caller should
/// then open this as their local database and pull any changesets newer than the
/// per-device cursors in the result.
///
/// The bucket is untrusted, so a snapshot is held to the same authorship bar as a
/// changeset before it is adopted:
///
/// - The signed pointer's signature must verify (under this `library_id`, which
///   also refuses a different library's pointer replayed here) and its author must
///   be a current write-capable member (a non-member cannot repoint the live
///   snapshot).
/// - The named generation's signed metadata must verify (likewise bound to
///   `library_id`) and its author must be a current write-capable member (a forged,
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
    cipher: &CloudCipher,
    owner_pubkey: Option<&str>,
    binary_schema_version: u32,
    target_path: &Path,
) -> Result<BootstrapResult, SnapshotError> {
    // Resolve the pointer to the live generation and authenticate it before
    // touching disk. The pointer absent means no snapshot has been published (a
    // brand-new library) or its object is missing; either way there is no
    // consistent generation to adopt and we refuse, writing nothing.
    let (author, seq, meta) = resolve_current_meta(storage, library_id, owner_pubkey).await?;

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
    let sealed = storage.get_snapshot(&author, seq).await?;
    if snapshot_db_hash(&sealed) != meta.db_hash {
        return Err(SnapshotError::DbHashMismatch);
    }

    let plaintext = cipher
        .open(&sealed)
        .map_err(|e| SnapshotError::Decryption(e.to_string()))?;

    snapshot_files()?.write_snapshot_db(target_path, &plaintext)?;

    let cursors: HashMap<String, u64> = meta.cursors.into_iter().collect();
    info!(
        num_devices = cursors.len(),
        db_size = plaintext.len(),
        path = %target_path.display(),
        "bootstrapped from snapshot"
    );

    Ok(BootstrapResult { cursors })
}

/// `sync_state` key holding `"1"` while the snapshot blob reconciliation has not
/// yet fully succeeded for this library, absent once it has. A bootstrap sets it;
/// each sync cycle runs the reconciliation while it is set and clears it on the
/// first run that lands every referenced blob. See [`reconcile_snapshot_blobs`].
pub const SNAPSHOT_BLOB_BACKFILL_PENDING: &str = "snapshot_blob_backfill_pending";

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
/// any already present in either cache folder. A failed download is logged there and reflected
/// in the returned flag; the bootstrap that calls this records the not-yet-complete
/// state in
/// [`SNAPSHOT_BLOB_BACKFILL_PENDING`], and each subsequent sync cycle re-runs this
/// until it returns true, so a blob whose object was not yet in the cloud (or whose
/// download hit a transient error) at bootstrap is fetched on a later cycle rather
/// than lost. A clear flag means no cycle runs this, so a caught-up library pays
/// nothing.
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
    let blobs: Vec<crate::blob::BlobRef> = {
        let conn = Connection::open(db_path).map_err(crate::database::DbError::from)?;
        let decls = crate::blob::decl::BlobDecls::from_tables(&conn, tables)
            .map_err(|e| crate::database::DbError(format!("blob decls: {e}")))?;
        decls
            .refs_in_db(&conn)
            .map_err(|e| crate::database::DbError(format!("blob decls: {e}")))?
            .into_iter()
            .filter(|blob| blob.fill == crate::blob::CacheFill::CacheEager)
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
        warn!(
            total,
            "some snapshot blob files are not yet local; a later sync cycle reconciles them"
        );
    }
    Ok(all_ok)
}

/// The library id the snapshot tests sign their meta/pointer under. The same id is
/// passed to `push_snapshot`/`bootstrap_from_snapshot`/`reclaim_superseded_changesets`,
/// so the signatures verify; a cross-library binding mismatch is exercised by its own
/// test. Shared by the `tests`, `authorization_tests`, and `reclaim_tests` modules.
#[cfg(test)]
fn test_library_id() -> &'static str {
    "test-library"
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::encryption::EncryptionService;
    use crate::sync::apply::apply_changeset_lww;
    use crate::sync::storage::{DeviceHead, MinSchemaVersion};
    use async_trait::async_trait;
    use rusqlite::session::Session as RqSession;
    use rusqlite::{Connection, OptionalExtension};
    use std::collections::HashMap;
    use std::sync::Mutex;

    // ---- in-process db helpers (rusqlite, the new `&Connection` API) ----

    /// The synthetic synced set the snapshot tests scope by.
    fn synced_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("notes").gated_by("shared"),
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

    /// Full-featured mock storage for snapshot tests.
    ///
    /// Snapshots are stored exactly as the cloud lays them out: each generation's
    /// db and meta under a per-`(author, seq)` key, plus a single pointer the
    /// publish writes last. Keying generations under their author (not a flat seq)
    /// is what lets the cross-device tests exercise the real globally-unique
    /// keyspace and the torn-read/GC behavior.
    struct MockSyncStorage {
        changesets: Mutex<HashMap<String, Vec<u8>>>,
        heads: Mutex<HashMap<String, (u64, Option<u64>)>>,
        /// Per-generation snapshot db images, keyed by (author, seq).
        snapshot_dbs: Mutex<HashMap<(String, u64), Vec<u8>>>,
        /// Per-generation snapshot metadata, keyed by (author, seq).
        snapshot_metas: Mutex<HashMap<(String, u64), Vec<u8>>>,
        /// The pointer naming the live generation (None until the first publish).
        snapshot_pointer: Mutex<Option<Vec<u8>>>,
        min_schema_version: Mutex<Option<u32>>,
        /// Per-device signed pull-acks, keyed by device_id (`acks/{device_id}.json`).
        acks: Mutex<HashMap<String, Vec<u8>>>,
    }

    impl MockSyncStorage {
        fn new() -> Self {
            MockSyncStorage {
                changesets: Mutex::new(HashMap::new()),
                heads: Mutex::new(HashMap::new()),
                snapshot_dbs: Mutex::new(HashMap::new()),
                snapshot_metas: Mutex::new(HashMap::new()),
                snapshot_pointer: Mutex::new(None),
                min_schema_version: Mutex::new(None),
                acks: Mutex::new(HashMap::new()),
            }
        }

        /// Helper to add a changeset directly.
        fn add_changeset(&self, device_id: &str, seq: u64, data: Vec<u8>) {
            let key = format!("{device_id}/{seq}");
            self.changesets.lock().unwrap().insert(key, data);

            let mut heads = self.heads.lock().unwrap();
            let entry = heads.entry(device_id.to_string()).or_insert((0, None));
            if seq > entry.0 {
                entry.0 = seq;
            }
        }

        /// The live generation's `{author, seq}`, or None if nothing is published.
        fn current_pointer_target(&self) -> Option<(String, u64)> {
            let pointer = self.snapshot_pointer.lock().unwrap();
            let bytes = pointer.as_ref()?;
            let parsed: SnapshotPointerJson = serde_json::from_slice(bytes).expect("parse pointer");
            Some((parsed.author_pubkey, parsed.seq))
        }

        /// The seq the pointer currently names, or None if nothing is published.
        fn current_pointer_seq(&self) -> Option<u64> {
            self.current_pointer_target().map(|(_, seq)| seq)
        }

        /// The live generation's db image (the one the pointer names).
        fn get_stored_snapshot(&self) -> Option<Vec<u8>> {
            let target = self.current_pointer_target()?;
            self.snapshot_dbs.lock().unwrap().get(&target).cloned()
        }

        /// The live generation's metadata (the one the pointer names).
        fn get_stored_snapshot_meta(&self) -> Option<Vec<u8>> {
            let target = self.current_pointer_target()?;
            self.snapshot_metas.lock().unwrap().get(&target).cloned()
        }
    }

    #[cfg_attr(not(target_arch = "wasm32"), async_trait)]
    #[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
    impl SyncStorage for MockSyncStorage {
        async fn list_heads(&self) -> Result<Vec<DeviceHead>, StorageError> {
            let heads = self.heads.lock().unwrap();
            Ok(heads
                .iter()
                .map(|(id, (seq, snap))| DeviceHead {
                    device_id: id.clone(),
                    seq: *seq,
                    snapshot_seq: *snap,
                    last_sync: None,
                    author_pubkey: String::new(),
                })
                .collect())
        }

        async fn get_changeset(&self, device_id: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
            let key = format!("{device_id}/{seq}");
            let cs = self.changesets.lock().unwrap();
            cs.get(&key).cloned().ok_or(StorageError::NotFound(key))
        }

        async fn put_changeset(
            &self,
            device_id: &str,
            seq: u64,
            data: Vec<u8>,
        ) -> Result<(), StorageError> {
            let key = format!("{device_id}/{seq}");
            self.changesets.lock().unwrap().insert(key, data);
            Ok(())
        }

        async fn put_head(
            &self,
            device_id: &str,
            seq: u64,
            snapshot_seq: Option<u64>,
            _timestamp: &str,
        ) -> Result<(), StorageError> {
            let mut heads = self.heads.lock().unwrap();
            let entry = heads.entry(device_id.to_string()).or_insert((0, None));
            entry.0 = seq;
            if snapshot_seq.is_some() {
                entry.1 = snapshot_seq;
            }
            Ok(())
        }

        async fn put_blob(
            &self,
            _namespace: &str,
            _id: &str,
            _scope: crate::blob::ResolvedScope,
            _cloud_path: Option<&str>,
            _data: Vec<u8>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_blob(
            &self,
            namespace: &str,
            id: &str,
            _scope: crate::blob::ResolvedScope,
            _cloud_path: Option<&str>,
        ) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound(format!("{namespace}/{id}")))
        }

        async fn read_blob_range(
            &self,
            namespace: &str,
            id: &str,
            _scope: crate::blob::ResolvedScope,
            _cloud_path: Option<&str>,
            _source_size: u64,
            _offset: u64,
            _len: u64,
        ) -> Result<Vec<u8>, StorageError> {
            // Snapshot tests never stream a blob; no object store to slice.
            Err(StorageError::NotFound(format!("{namespace}/{id}")))
        }

        async fn put_snapshot(
            &self,
            author: &str,
            seq: u64,
            data: Vec<u8>,
        ) -> Result<(), StorageError> {
            self.snapshot_dbs
                .lock()
                .unwrap()
                .insert((author.to_string(), seq), data);
            Ok(())
        }

        async fn get_snapshot(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
            self.snapshot_dbs
                .lock()
                .unwrap()
                .get(&(author.to_string(), seq))
                .cloned()
                .ok_or(StorageError::NotFound(format!(
                    "snapshot/{author}/{seq}.db"
                )))
        }

        async fn delete_changeset(&self, device_id: &str, seq: u64) -> Result<(), StorageError> {
            let key = format!("{device_id}/{seq}");
            self.changesets.lock().unwrap().remove(&key);
            Ok(())
        }

        async fn list_changesets(&self, device_id: &str) -> Result<Vec<u64>, StorageError> {
            let prefix = format!("{device_id}/");
            let cs = self.changesets.lock().unwrap();
            let mut seqs: Vec<u64> = cs
                .keys()
                .filter_map(|k| k.strip_prefix(&prefix).and_then(|s| s.parse().ok()))
                .collect();
            seqs.sort();
            Ok(seqs)
        }

        async fn put_ack(&self, device_id: &str, data: Vec<u8>) -> Result<(), StorageError> {
            self.acks
                .lock()
                .unwrap()
                .insert(device_id.to_string(), data);
            Ok(())
        }

        async fn get_ack(&self, device_id: &str) -> Result<Vec<u8>, StorageError> {
            self.acks
                .lock()
                .unwrap()
                .get(device_id)
                .cloned()
                .ok_or_else(|| StorageError::NotFound(format!("acks/{device_id}.json")))
        }

        async fn get_min_schema_version(&self) -> Result<Option<MinSchemaVersion>, StorageError> {
            Ok(self
                .min_schema_version
                .lock()
                .unwrap()
                .map(|version| MinSchemaVersion {
                    version,
                    author_pubkey: String::new(),
                }))
        }

        async fn set_min_schema_version(&self, version: u32) -> Result<(), StorageError> {
            *self.min_schema_version.lock().unwrap() = Some(version);
            Ok(())
        }

        async fn put_membership_entry(
            &self,
            _author_pubkey: &str,
            _seq: u64,
            _data: Vec<u8>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_membership_entry(
            &self,
            author_pubkey: &str,
            seq: u64,
        ) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound(format!(
                "membership/{author_pubkey}/{seq}"
            )))
        }

        async fn list_membership_entries(&self) -> Result<Vec<(String, u64)>, StorageError> {
            Ok(vec![])
        }

        async fn put_wrapped_key(
            &self,
            _user_pubkey: &str,
            _data: Vec<u8>,
        ) -> Result<(), StorageError> {
            Ok(())
        }

        async fn get_wrapped_key(&self, user_pubkey: &str) -> Result<Vec<u8>, StorageError> {
            Err(StorageError::NotFound(format!("keys/{user_pubkey}")))
        }

        async fn delete_wrapped_key(&self, _user_pubkey: &str) -> Result<(), StorageError> {
            Ok(())
        }

        async fn put_snapshot_meta(
            &self,
            author: &str,
            seq: u64,
            data: Vec<u8>,
        ) -> Result<(), StorageError> {
            self.snapshot_metas
                .lock()
                .unwrap()
                .insert((author.to_string(), seq), data);
            Ok(())
        }

        async fn get_snapshot_meta(&self, author: &str, seq: u64) -> Result<Vec<u8>, StorageError> {
            self.snapshot_metas
                .lock()
                .unwrap()
                .get(&(author.to_string(), seq))
                .cloned()
                .ok_or(StorageError::NotFound(format!(
                    "snapshot/{author}/{seq}_meta.json"
                )))
        }

        async fn put_snapshot_pointer(&self, data: Vec<u8>) -> Result<(), StorageError> {
            *self.snapshot_pointer.lock().unwrap() = Some(data);
            Ok(())
        }

        async fn get_snapshot_pointer(&self) -> Result<Vec<u8>, StorageError> {
            self.snapshot_pointer
                .lock()
                .unwrap()
                .clone()
                .ok_or(StorageError::NotFound("snapshot/current.json".into()))
        }

        async fn list_own_snapshot_generations(
            &self,
            author: &str,
        ) -> Result<Vec<u64>, StorageError> {
            // List only this author's own keyspace — the meta objects under
            // `snapshot/{author}/` — exactly as `CloudSyncStorage` does. Ownership is
            // structural: a peer's generations live under a different prefix and are
            // never listed here.
            let mut seqs: Vec<u64> = self
                .snapshot_metas
                .lock()
                .unwrap()
                .keys()
                .filter(|(a, _)| a == author)
                .map(|(_, seq)| *seq)
                .collect();
            seqs.sort_unstable();
            Ok(seqs)
        }

        async fn delete_snapshot_generation(
            &self,
            author: &str,
            seq: u64,
        ) -> Result<(), StorageError> {
            // Remove the db first, the meta last: the meta is what `list` keys a
            // generation by, so a crash between the two leaves it still listed (and
            // re-deletable), never a meta-less db.
            let key = (author.to_string(), seq);
            self.snapshot_dbs.lock().unwrap().remove(&key);
            self.snapshot_metas.lock().unwrap().remove(&key);
            Ok(())
        }
    }

    /// An encrypted-home cipher over a fixed key, the default the snapshot tests
    /// run against. Plaintext-home snapshot round-tripping is covered end-to-end
    /// through the real cycle in `delete_propagation_tests`.
    fn test_encryption() -> CloudCipher {
        CloudCipher::Encrypted(EncryptionService::new_with_key(&[0x42u8; 32]))
    }

    /// The keypair the snapshot tests push and sign with, when they don't care
    /// who the author is (round-trip, cursor-honesty, GC). It is not registered in
    /// any chain, so these tests bootstrap with `owner = None` and an empty
    /// membership listing — the open-library path that authorizes on the signature
    /// alone. The membership-authorization tests build their own chained mock.
    fn test_keypair() -> UserKeypair {
        UserKeypair::generate()
    }

    /// Publish a full snapshot generation directly: the signed meta over `cursors`,
    /// the db image, and the signed pointer naming `{author, seq}` — all consistent
    /// on the generation's DB hash, exactly as `push_snapshot` writes them, under the
    /// publishing device's own keyspace. The single snapshot-staging helper the tests
    /// use when they set cursors (or a seq) directly without driving `push_snapshot`.
    ///
    /// `keypair` is the generation's author, so the generation lands under
    /// `snapshot/{author}/{seq}` and the pointer commits to that author. The
    /// superseded-generation sweep lists only its own keyspace, so a test that stages
    /// a generation it expects its own sweep to reclaim must stage it under the same
    /// keypair it later sweeps with; one staging a peer's generation uses a different
    /// keypair.
    ///
    /// GC tests pass a placeholder db (GC never fetches the snapshot DB — only the
    /// pointer/meta signatures and the cursors drive deletion). Bootstrap tests
    /// pass the real sealed bytes, so the DB-image hash check passes.
    async fn publish_signed_generation(
        storage: &MockSyncStorage,
        seq: u64,
        cursors: HashMap<String, u64>,
        sealed_db: Vec<u8>,
        keypair: &UserKeypair,
    ) {
        let author = hex::encode(keypair.public_key);
        let db_hash = snapshot_db_hash(&sealed_db);
        let meta = SnapshotMetaJson::signed(
            test_library_id(),
            cursors.into_iter().collect(),
            db_hash.clone(),
            0,
            keypair,
        );
        storage
            .put_snapshot_meta(&author, seq, serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();
        storage.put_snapshot(&author, seq, sealed_db).await.unwrap();
        let pointer = SnapshotPointerJson::signed(test_library_id(), seq, db_hash, keypair);
        storage
            .put_snapshot_pointer(serde_json::to_vec(&pointer).unwrap())
            .await
            .unwrap();
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
    fn create_snapshot_produces_encrypted_db() {
        let c = synced_conn();
        exec(
            &c,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'Note One', NULL, 1, '0000000001000-0000-dev1', '2026-01-01')",
        );

        let temp = tempfile::tempdir().unwrap();
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&c, temp.path(), &synced_tables(), &enc).expect("create_snapshot");

        assert!(!encrypted.is_empty());
        let plaintext = enc.open(&encrypted).expect("open should succeed");
        assert!(!plaintext.is_empty());
        assert!(
            plaintext.starts_with(b"SQLite format 3\0"),
            "snapshot should be a valid SQLite database"
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
        let enc = test_encryption();
        let encrypted = create_snapshot(&c, temp.path(), &synced_tables(), &enc).expect("snapshot");
        let plaintext = enc.open(&encrypted).expect("open");

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
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot(
            &storage,
            test_library_id(),
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
        bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snapshot");

        // Publish the generation stamped at synced-schema version 2.
        let storage = MockSyncStorage::new();
        push_snapshot(
            &storage,
            test_library_id(),
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
        let err = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 1, &too_old)
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
        bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 2, &same)
            .await
            .expect("a binary at the snapshot's version bootstraps");
        assert!(same.exists());

        // A newer binary (version 3) adopts it too.
        let newer = temp.path().join("newer.db");
        bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 3, &newer)
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
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot(
            &storage,
            test_library_id(),
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
        bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        exec(&db_a, crate::db::MIGRATION_SQL);
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
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        push_snapshot(
            &storage,
            test_library_id(),
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
        bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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

        push_snapshot(
            &storage,
            test_library_id(),
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
        assert_eq!(storage.get_stored_snapshot(), Some(data));

        // Head should be updated with snapshot_seq.
        let heads = storage.list_heads().await.unwrap();
        let dev1_head = heads.iter().find(|h| h.device_id == "dev-1").unwrap();
        assert_eq!(dev1_head.seq, 42);
        assert_eq!(dev1_head.snapshot_seq, Some(42));

        // Snapshot metadata reflects the applied cursors plus our own seq.
        let meta_json = storage
            .get_stored_snapshot_meta()
            .expect("metadata should be written");
        let meta: SnapshotMetaJson = serde_json::from_slice(&meta_json).unwrap();
        assert_eq!(meta.cursors.get("dev-1"), Some(&42));
        assert_eq!(meta.cursors.get("dev-2"), Some(&15));
        assert_eq!(meta.cursors.len(), 2);
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
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let storage = MockSyncStorage::new();
        publish_signed_generation(
            &storage,
            10,
            HashMap::from([("dev-1".to_string(), 10), ("dev-2".to_string(), 7)]),
            encrypted,
            &test_keypair(),
        )
        .await;

        let target = temp.path().join("bootstrapped.db");
        let result = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("nope.db");

        let result =
            bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target).await;

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
        let enc = test_encryption();
        let storage = MockSyncStorage::new();

        let encrypted =
            create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snapshot");
        push_snapshot(
            &storage,
            test_library_id(),
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
        let result = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        let enc = test_encryption();
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
            create_snapshot(&db_source, temp.path(), &synced_tables(), &enc).expect("snapshot");

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
        let snapshot_plain = enc.open(&snapshot_encrypted).unwrap();
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
        let enc = test_encryption();
        let encrypted =
            create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snapshot");

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
        let err = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // The live generation A (seq 5): a DB containing 'old', cursors {self: 5}.
        let db_a = synced_conn();
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'old', 1, '0000000001000-0000-self', '2026-01-01')",
        );
        let snap_a = create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snap A");
        push_snapshot(
            &storage,
            test_library_id(),
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
        let snap_b = create_snapshot(&db_b, temp.path(), &synced_tables(), &enc).expect("snap B");
        storage.put_snapshot("selfpubkey", 9, snap_b).await.unwrap();

        // Bootstrap resolves the pointer (still naming A) and adopts A's consistent
        // generation — the 'old' row, cursor {self: 5} — never B's orphan db.
        let target = temp.path().join("boot.db");
        let boot = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        let author_a = hex::encode(kp_a.public_key);
        let author_b = hex::encode(kp_b.public_key);

        // Device A publishes generation 5 and the pointer names it (A is live).
        publish_signed_generation(&storage, 5, HashMap::new(), vec![0xAu8], &kp_a).await;

        // Device B writes its own generation at the SAME seq 5's db + meta but does
        // NOT flip the pointer: B's seq 5 is present yet not-yet-live, under B's own
        // keyspace. (publish_signed_generation would flip the pointer, so write the
        // two objects directly to model the mid-publish window before the commit.)
        let db_b = vec![0xBu8];
        let db_hash_b = snapshot_db_hash(&db_b);
        let meta_b = SnapshotMetaJson::signed(
            test_library_id(),
            std::collections::BTreeMap::new(),
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
            storage.current_pointer_seq(),
            Some(5),
            "the pointer still names A's generation; B is mid-publish",
        );

        // Device A's real sweep, keyed by A's pubkey, with A's just-published seq. It
        // lists only A's keyspace, so B's same-seq generation is never a candidate and
        // survives.
        delete_superseded_generations(&storage, test_library_id(), 5, &author_a)
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
        let own_author = hex::encode(kp.public_key);

        // The device publishes generation 1, then 2; the pointer now names 2 and
        // generation 1 is its own superseded generation. Both authored by `kp`.
        publish_signed_generation(&storage, 1, HashMap::new(), vec![1u8], &kp).await;
        publish_signed_generation(&storage, 2, HashMap::new(), vec![2u8], &kp).await;

        // A sweep keyed by a stranger's pubkey lists the stranger's keyspace — empty
        // — so it reclaims nothing, and this device's generation 1 survives: a device
        // structurally cannot reach another device's keyspace.
        let stranger = hex::encode(test_keypair().public_key);
        delete_superseded_generations(&storage, test_library_id(), 2, &stranger)
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
        delete_superseded_generations(&storage, test_library_id(), 2, &own_author)
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

    /// The live and just-published generations are never deleted, even when both are
    /// this device's own. The device authored generations 1 (superseded) and 2
    /// (live). A sweep keyed by its pubkey but told it *just published 1* must still
    /// keep 1 (it is the just-published seq) and keep 2 (it is live) — so neither the
    /// just-published guard nor the live-pointer guard ever deletes a generation a
    /// reader could still need, regardless of authorship.
    #[tokio::test]
    async fn sweep_never_deletes_the_live_or_just_published_generation() {
        let storage = MockSyncStorage::new();
        let kp = test_keypair();
        let own_author = hex::encode(kp.public_key);

        // Generations 1 (superseded) and 2 (live), both authored by this device.
        publish_signed_generation(&storage, 1, HashMap::new(), vec![1u8], &kp).await;
        publish_signed_generation(&storage, 2, HashMap::new(), vec![2u8], &kp).await;

        // Sweep claiming seq 1 as just-published: 1 is protected as just-published, 2
        // as live — nothing is deleted even though both are this device's own.
        delete_superseded_generations(&storage, test_library_id(), 1, &own_author)
            .await
            .expect("sweep runs");
        let mut after = storage
            .list_own_snapshot_generations(&own_author)
            .await
            .unwrap();
        after.sort_unstable();
        assert_eq!(
            after,
            vec![1, 2],
            "neither the live nor the just-published generation is ever deleted",
        );
    }

    /// When the live pointer can't be read or verified, the sweep can't establish
    /// which of this device's generations is live, so it deletes nothing rather than
    /// guess. A later sweep with a readable pointer reclaims; deleting on an
    /// unverifiable pointer could remove a generation that is in fact live.
    #[tokio::test]
    async fn sweep_skips_when_the_live_pointer_is_unverifiable() {
        let storage = MockSyncStorage::new();
        let kp = test_keypair();
        let own_author = hex::encode(kp.public_key);

        // Two own generations: 1 (superseded) and 2 (the pointer names it).
        publish_signed_generation(&storage, 1, HashMap::new(), vec![1u8], &kp).await;
        publish_signed_generation(&storage, 2, HashMap::new(), vec![2u8], &kp).await;

        // Overwrite the pointer with bytes that neither parse nor verify.
        storage
            .put_snapshot_pointer(b"not a valid signed pointer".to_vec())
            .await
            .unwrap();

        // The sweep can't establish liveness, so it returns having deleted nothing.
        delete_superseded_generations(&storage, test_library_id(), 2, &own_author)
            .await
            .expect("sweep returns Ok, having skipped");
        let mut after = storage
            .list_own_snapshot_generations(&own_author)
            .await
            .unwrap();
        after.sort_unstable();
        assert_eq!(
            after,
            vec![1, 2],
            "an unverifiable pointer makes the sweep skip, keeping every generation",
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
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();
        let kp_a = test_keypair();
        let kp_b = test_keypair();
        let author_a = hex::encode(kp_a.public_key);
        let author_b = hex::encode(kp_b.public_key);

        // Device A publishes a real generation at seq 7 (its catalog has 'a-row'),
        // and the pointer names A's generation (A is the live publisher).
        let db_a = synced_conn();
        exec(
            &db_a,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('a-row', 'A', 1, '0000000001000-0000-A', '2026-01-01')",
        );
        let snap_a = create_snapshot(&db_a, temp.path(), &synced_tables(), &enc).expect("snap A");
        publish_signed_generation(
            &storage,
            7,
            HashMap::from([("A".to_string(), 7)]),
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
        let snap_b = create_snapshot(&db_b, temp.path(), &synced_tables(), &enc).expect("snap B");
        let db_hash_b = snapshot_db_hash(&snap_b);
        let meta_b = SnapshotMetaJson::signed(
            test_library_id(),
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
        let boot = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        let enc = test_encryption();
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
        storage.add_changeset("M", k, cs_insert.clone());

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
        storage.add_changeset("M", k + 1, cs_update.clone());

        // Device B is behind: it has applied M only up to K. B snapshots its state.
        let db_b = synced_conn();
        apply(&db_b, &cs_insert);
        let snapshot =
            create_snapshot(&db_b, temp.path(), &synced_tables(), &enc).expect("snapshot");

        let kp = test_keypair();
        let applied = HashMap::from([("M".to_string(), k)]);
        push_snapshot(
            &storage,
            test_library_id(),
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

        let meta_json = storage.get_stored_snapshot_meta().expect("meta");
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
        let boot = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
            .await
            .expect("bootstrap");
        let c_cursor = *boot.cursors.get("M").unwrap_or(&0);

        let db_c = open_db_at(&target);
        for seq in storage.list_changesets("M").await.unwrap() {
            if seq <= c_cursor {
                continue;
            }
            let bytes = storage.get_changeset("M", seq).await.unwrap();
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
        let enc = test_encryption();
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
        storage.add_changeset("owner", 1, cs1.clone());

        let db_owner = synced_conn();
        apply(&db_owner, &cs1);
        let snap1 = create_snapshot(&db_owner, temp.path(), &synced_tables(), &enc).expect("snap1");
        push_snapshot(
            &storage,
            test_library_id(),
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
        let b_boot = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &b_path)
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
        storage.add_changeset("owner", 2, cs2.clone());

        // B pulls the update (everything past its bootstrap cursor).
        let mut b_cursors = b_boot.cursors.clone();
        let b_owner_cursor = *b_cursors.get("owner").unwrap_or(&0);
        for seq in storage.list_changesets("owner").await.unwrap() {
            if seq <= b_owner_cursor {
                continue;
            }
            let bytes = storage.get_changeset("owner", seq).await.unwrap();
            apply(&db_b, &bytes);
            b_cursors.insert("owner".to_string(), seq);
        }
        assert_eq!(
            query_text(&db_b, "SELECT title FROM notes WHERE id = 'n1'"),
            "Published"
        );

        // B snapshots its now-current state with honest cursors {owner: 2}.
        let snap2 = create_snapshot(&db_b, temp.path(), &synced_tables(), &enc).expect("snap2");
        push_snapshot(
            &storage,
            test_library_id(),
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
        let c_boot = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &c_path)
            .await
            .expect("c bootstrap");
        let db_c = open_db_at(&c_path);
        let c_owner_cursor = *c_boot.cursors.get("owner").unwrap_or(&0);
        for seq in storage.list_changesets("owner").await.unwrap() {
            if seq <= c_owner_cursor {
                continue;
            }
            let bytes = storage.get_changeset("owner", seq).await.unwrap();
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
            storage.add_changeset("M", seq, vec![seq as u8]);
        }

        // Snapshot honestly covers M only through seq 2.
        let applied = HashMap::from([("M".to_string(), 2)]);
        push_snapshot(
            &storage,
            test_library_id(),
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

        reclaim_superseded_changesets(&storage, test_library_id(), None)
            .await
            .expect("reclaim");

        assert_eq!(storage.list_changesets("M").await.unwrap(), vec![3]);
    }

    /// After bootstrap, the returned cursors never exceed what the snapshot DB
    /// actually contains — they equal the applied state the snapshot was taken
    /// from.
    #[tokio::test]
    async fn bootstrap_cursors_match_snapshot_contents() {
        let enc = test_encryption();
        let temp = tempfile::tempdir().unwrap();
        let storage = MockSyncStorage::new();

        // Snapshot taken from a state where M is applied through seq 7.
        let db = synced_conn();
        exec(
            &db,
            "INSERT INTO notes (id, title, shared, _updated_at, created_at) \
             VALUES ('n1', 'A', 1, '0000000001000-0000-M', '2026-01-01')",
        );
        let snap = create_snapshot(&db, temp.path(), &synced_tables(), &enc).expect("snap");

        let applied = HashMap::from([("M".to_string(), 7)]);
        push_snapshot(
            &storage,
            test_library_id(),
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
        let boot = bootstrap_from_snapshot(&storage, test_library_id(), &enc, None, 0, &target)
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
        storage.add_changeset("M", 9, vec![9]);

        // The snapshotting device B has only applied M through seq 4.
        let applied = HashMap::from([("M".to_string(), 4)]);
        push_snapshot(
            &storage,
            test_library_id(),
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

        let meta_json = storage.get_stored_snapshot_meta().expect("meta");
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
    use crate::encryption::EncryptionService;
    use crate::keys::UserKeypair;
    use crate::sync::membership::{founder_entry, MemberRole, MembershipAction};
    use crate::sync::test_helpers::{make_entry, pubkey_hex, MockSyncStorage};

    /// The encrypted-home cipher these tests seal snapshots under.
    fn cipher() -> CloudCipher {
        CloudCipher::Encrypted(EncryptionService::new_with_key(&[0x42u8; 32]))
    }

    /// A minimal sealed snapshot blob. The authorization checks operate on the
    /// metadata signature, the DB-hash binding, and the chain — none of which need
    /// a real SQLite image — so a fixed byte string stands in for the catalog.
    /// (The full create→push→bootstrap DB round-trip is covered in the sibling
    /// `tests` module; here the blob is just the thing the signature commits to.)
    fn fake_snapshot() -> Vec<u8> {
        cipher().seal(b"catalog-image-bytes")
    }

    /// Seed a one-owner founder chain into the mock and return the owner keypair.
    async fn found_chain(storage: &MockSyncStorage, owner: &UserKeypair) {
        let entry = founder_entry(owner, "0000000001000-0000-owner");
        storage
            .put_membership_entry(&pubkey_hex(owner), 1, serde_json::to_vec(&entry).unwrap())
            .await
            .unwrap();
    }

    /// A snapshot signed by a current member (the owner) bootstraps: the signature
    /// verifies, the DB hash matches, and the author is a write-capable member of
    /// the chain anchored to the pinned owner.
    #[tokio::test]
    async fn bootstrap_accepts_snapshot_signed_by_member() {
        let owner = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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
        push_snapshot(
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
        let err = bootstrap_from_snapshot(
            &storage,
            library_y,
            &cipher(),
            Some(&pubkey_hex(&owner)),
            0,
            &target,
        )
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
    /// snapshot is adopted only from a current write-capable member.
    #[tokio::test]
    async fn bootstrap_refuses_snapshot_signed_by_non_member() {
        let owner = UserKeypair::generate();
        let outsider = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        // The outsider forges a fully-signed snapshot+meta: valid signature, valid
        // DB hash — only the AUTHOR is unauthorized. This is exactly what a
        // bucket-write-capable non-member can produce.
        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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
        push_snapshot(
            &storage,
            test_library_id(),
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
            SnapshotPointerJson::signed(test_library_id(), 1, real_db_hash, &outsider);
        storage
            .put_snapshot_pointer(serde_json::to_vec(&forged_pointer).unwrap())
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            test_library_id(),
            &cipher(),
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
        found_chain(&storage, &owner).await;
        // Owner adds the follower (read-only).
        let add = make_entry(
            &owner,
            MembershipAction::Add,
            &follower,
            MemberRole::Follower,
            "0000000002000-0000-owner",
        );
        storage
            .put_membership_entry(&pubkey_hex(&owner), 2, serde_json::to_vec(&add).unwrap())
            .await
            .unwrap();

        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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
        found_chain(&storage, &owner).await;
        // The owner adds a write-capable Member.
        let add = make_entry(
            &owner,
            MembershipAction::Add,
            &member,
            MemberRole::Member,
            "0000000002000-0000-owner",
        );
        storage
            .put_membership_entry(&pubkey_hex(&owner), 2, serde_json::to_vec(&add).unwrap())
            .await
            .unwrap();

        // The owner's snapshot is adopted.
        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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
        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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

        push_snapshot(
            &storage,
            test_library_id(),
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
                cipher().seal(b"a-different-forged-catalog"),
            )
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            test_library_id(),
            &cipher(),
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
        push_snapshot(
            &storage,
            test_library_id(),
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
        let other_hash = snapshot_db_hash(&cipher().seal(b"a-different-devices-catalog"));
        let spliced_pointer = SnapshotPointerJson::signed(test_library_id(), 1, other_hash, &owner);
        storage
            .put_snapshot_pointer(serde_json::to_vec(&spliced_pointer).unwrap())
            .await
            .unwrap();

        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("boot.db");
        let err = bootstrap_from_snapshot(
            &storage,
            test_library_id(),
            &cipher(),
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

        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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
    /// the author must be a current write-capable member of it. A member-signed
    /// snapshot is adopted (owner = None), while a non-member-signed one is still
    /// refused — so the trust-on-first-use restore path is not a hole.
    #[tokio::test]
    async fn restore_path_authorizes_against_the_chains_own_founder() {
        let owner = UserKeypair::generate();
        let outsider = UserKeypair::generate();
        let storage = MockSyncStorage::new();
        found_chain(&storage, &owner).await;

        // Member-signed snapshot: adopted even with no pinned owner.
        push_snapshot(
            &storage,
            test_library_id(),
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
        bootstrap_from_snapshot(&storage, test_library_id(), &cipher(), None, 0, &target)
            .await
            .expect("restore adopts a member-signed snapshot anchored to the chain founder");
        assert!(target.exists());

        // Now an outsider overwrites the snapshot. Even with no pinned owner, the
        // author is judged against the chain and refused.
        push_snapshot(
            &storage,
            test_library_id(),
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
        let err =
            bootstrap_from_snapshot(&storage, test_library_id(), &cipher(), None, 0, &target2)
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

        push_snapshot(
            &storage,
            test_library_id(),
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
            reclaim_superseded_changesets(&storage, test_library_id(), Some(&pubkey_hex(&owner)))
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

        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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
        push_snapshot(
            &storage,
            test_library_id(),
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
            test_library_id(),
            &cipher(),
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
        let pointer = SnapshotPointerJson::signed(test_library_id(), 1, db_hash, &owner);
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
            test_library_id(),
            &cipher(),
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
            matches!(err, SnapshotError::Io(_)),
            "expected a parse failure (Io), got {err:?}",
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
    use crate::sync::membership::{founder_entry, MemberRole, MembershipAction};
    use crate::sync::test_helpers::{make_entry, pubkey_hex, MockSyncStorage};

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
    /// snapshot (a write-capable member); it has no head, so it is not itself a
    /// current device.
    async fn found_chain(storage: &MockSyncStorage) -> UserKeypair {
        let owner = UserKeypair::generate();
        let entry = founder_entry(&owner, "0000000001000-0000-owner");
        storage
            .put_membership_entry(&pubkey_hex(&owner), 1, serde_json::to_vec(&entry).unwrap())
            .await
            .unwrap();
        owner
    }

    /// Append a membership entry authored by `owner` at chain seq `seq` (2..=9). A
    /// timestamp derived from `seq` keeps the entries lexically ordered, the order
    /// `MembershipChain::from_entries` sorts by.
    async fn append_entry(
        storage: &MockSyncStorage,
        owner: &UserKeypair,
        action: MembershipAction,
        subject: &Device,
        seq: u64,
    ) {
        let ts = format!("000000000{seq}000-0000-owner");
        let entry = make_entry(owner, action, &subject.kp, MemberRole::Member, &ts);
        storage
            .put_membership_entry(&pubkey_hex(owner), seq, serde_json::to_vec(&entry).unwrap())
            .await
            .unwrap();
    }

    /// Publish the live snapshot generation (meta + db + pointer) signed by `owner`,
    /// carrying `cursors` (device_id -> seq). The db image is a placeholder —
    /// reclamation reads only the pointer/meta signatures and the cursors.
    async fn publish_snapshot(
        storage: &MockSyncStorage,
        owner: &UserKeypair,
        seq: u64,
        cursors: &[(&str, u64)],
    ) {
        let cursors: BTreeMap<String, u64> =
            cursors.iter().map(|(d, s)| (d.to_string(), *s)).collect();
        let db = vec![0u8];
        let db_hash = snapshot_db_hash(&db);
        let author = pubkey_hex(owner);
        let meta = SnapshotMetaJson::signed(test_library_id(), cursors, db_hash.clone(), 0, owner);
        storage
            .put_snapshot_meta(&author, seq, serde_json::to_vec(&meta).unwrap())
            .await
            .unwrap();
        storage.put_snapshot(&author, seq, db).await.unwrap();
        let pointer = SnapshotPointerJson::signed(test_library_id(), seq, db_hash, owner);
        storage
            .put_snapshot_pointer(serde_json::to_vec(&pointer).unwrap())
            .await
            .unwrap();
    }

    /// Publish `device`'s pull-ack (`cursors`: peer_id -> seq), signed by its own
    /// key so its author matches its head.
    async fn publish_ack(storage: &MockSyncStorage, device: &Device, cursors: &[(&str, u64)]) {
        let cursors: BTreeMap<String, u64> =
            cursors.iter().map(|(d, s)| (d.to_string(), *s)).collect();
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
        reclaim_superseded_changesets(storage, test_library_id(), Some(&pubkey_hex(owner)))
            .await
            .expect("reclaim")
    }

    /// 1. Behind member kept: the snapshot covers A->5, but the one other current
    ///    device acks A->3, so the floor is 3 — 1..=3 are reclaimed, 4..=5 survive.
    #[tokio::test]
    async fn behind_member_pins_the_floor() {
        let storage = MockSyncStorage::new();
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 5)]).await;
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
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 3)]).await;
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
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 5)]).await;
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
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 5)]).await;
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
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &owner, MembershipAction::Add, &b, 3).await;
        append_entry(&storage, &owner, MembershipAction::Remove, &b, 4).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 5)]).await;
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
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 5)]).await;

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
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 3)]).await;

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
        let owner = found_chain(&storage).await;
        let a = Device::new("A");
        let b = Device::new("B");
        append_entry(&storage, &owner, MembershipAction::Add, &a, 2).await;
        append_entry(&storage, &owner, MembershipAction::Add, &b, 3).await;
        add_log(&storage, "A", 5).await;
        storage.publish_head_as("A", 5, &a.kp);
        storage.publish_head_as("B", 0, &b.kp);
        publish_snapshot(&storage, &owner, 1, &[("A", 5)]).await;
        // A acks only its peer B (no self-entry for A); B acks A->4.
        publish_ack(&storage, &a, &[("B", 2)]).await;
        publish_ack(&storage, &b, &[("A", 4)]).await;

        let result = reclaim(&storage, &owner).await;
        assert_eq!(result.deleted, 4);
        assert_eq!(storage.list_changesets("A").await.unwrap(), vec![5]);
    }
}
