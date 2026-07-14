//! Sync cycle orchestration.
//!
//! Runs a single sync cycle (gate + push local changes, pull remote changes,
//! manage snapshots) and initializes sync infrastructure. All connection access
//! goes through the owned [`Database`]. Local changes are published from the
//! durable pending-changeset journal, which each host write appends to inside its
//! own journaled transaction — so a host write landing mid-cycle is captured for
//! the next outgoing changeset, while the pull's apply is a plain connection write
//! that is never journaled and so never echoes applied rows.

use std::path::PathBuf;
use std::str::FromStr;

use tracing::{debug, info, warn};

use crate::blob::BlobTransitionObserver;
use crate::changeset::RowChange;
use crate::database::{Database, DbError, PendingChangesetBatch};
use crate::keys::{MasterKeyCustody, UserKeypair};
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{
    BlobPathScheme, CloudCipherAccess, CloudCipherState, CloudSyncStorage, PendingRotation,
    RotationPending,
};
use super::hlc::Hlc;
use super::publish_blobs::{ensure_publishable_changeset_blobs, PublishBlobError};
use super::pull::HeldChangeset;
use super::service::DeferredLocalBlobDisposition;
use super::status::DeviceActivity;
use super::storage::SyncStorage;

/// Result of a single sync cycle.
#[derive(Debug)]
pub struct SyncCycleResult {
    /// Number of remote changesets that were applied.
    pub changesets_applied: u64,
    /// Changesets from a newer schema version that we couldn't apply. The cursor
    /// is held at the first such seq for each device until the app updates.
    pub skipped_schema: u64,
    /// Changesets skipped because their author is not a write-capable member,
    /// judged against the exact membership entry they are signed under (forged or
    /// revoked, not a propagation lag). The cursor advanced past them so the
    /// device isn't stuck; the count is per-cycle and surfaces as a warning.
    pub rejected_unauthorized: u64,
    /// Changesets whose signature did not verify (forged or corrupt). The cursor
    /// is held at the bad seq for that device, and the count surfaces as a warning.
    pub invalid_signatures: u64,
    /// Changesets whose present cloud object failed validation or apply. The
    /// cursor is held at the bad seq for that device. Carries per-changeset
    /// detail (device, seq, reason) so a host can say which changesets are
    /// stalled, not only how many.
    pub held_changesets: Vec<HeldChangeset>,
    /// Changesets rejected because SQLite reported a non-retryable constraint
    /// conflict. Their rows and cursor rolled back; the count is per-cycle and
    /// surfaces as a warning.
    pub constraint_conflicts: u64,
    /// Per-device activity of the other devices seen in the sync storage —
    /// device id, its member's author key, latest seq, and RFC 3339 last-sync
    /// time — so a host can render which devices synced and when.
    pub device_activity: Vec<DeviceActivity>,
    /// RFC 3339 timestamp of when this cycle completed.
    pub sync_time: String,
    /// Blobs needed before apply failed to download; their changesets and cursors
    /// remain pending.
    pub asset_downloads_failed: bool,
    /// Post-commit local blob cleanup still has durable filesystem work pending.
    /// Its corresponding rows and cursors are already durable.
    pub local_blob_cleanup_pending: bool,
    /// Row changes from applied changesets, for the host to map to domain events.
    pub row_changes: Vec<RowChange>,
    /// The outbox drain broke this cycle to publish a just-completed make_remote
    /// (coven flipped a root's gate the moment its last blob landed), so the loop
    /// should run the next cycle promptly to drain + publish the rest instead of
    /// waiting the idle interval.
    pub resume_drain_promptly: bool,
    /// Set when this device has not adopted a store-key rotation the cloud has
    /// already committed. While set, this cycle sealed nothing new for the
    /// cloud — no changeset, blob, tombstone, or snapshot — even though pull and
    /// local writes proceeded normally; the pending local changeset (if any)
    /// stays queued undrained until a later cycle adopts the rotation. A host
    /// surfaces this as why sync is paused, distinct from a hard failure.
    pub rotation_pending: Option<RotationPending>,
}

/// Path for staging outgoing changeset bytes that survived a push failure.
pub fn staging_path(store_dir: &StoreDir) -> PathBuf {
    store_dir.join("sync_staging.bin")
}

const STAGED_PENDING_CHANGESET_ID_KEY: &str = "staged_pending_changeset_id";

/// Clear the staged changeset after a successful push.
pub async fn clear_staged_changeset(store_dir: &StoreDir) -> Result<(), String> {
    match crate::local_blob::remove_file(&staging_path(store_dir)).await {
        Ok(true) => Ok(()),
        Ok(false) => Err("staged changeset file was absent when clearing it".to_string()),
        Err(error) => Err(format!("failed to remove staged changeset: {error}")),
    }
}

/// Read a previously staged changeset (if any) for retry.
pub async fn read_staged_changeset(store_dir: &StoreDir) -> Result<Option<Vec<u8>>, String> {
    let path = staging_path(store_dir);
    match crate::local_blob::exists(&path).await {
        Ok(true) => match crate::local_blob::read(&path).await {
            Ok(data) if !data.is_empty() => Ok(Some(data)),
            Ok(_) => Err("staged changeset file is empty".to_string()),
            Err(error) => Err(format!("failed to read staged changeset: {error}")),
        },
        Ok(false) => Ok(None),
        Err(error) => Err(format!("failed to check staged changeset: {error}")),
    }
}

async fn read_sync_state<T>(db: &Database, key: &str) -> Result<Option<T>, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match db.get_sync_state(key).await {
        Ok(Some(value)) => value
            .parse::<T>()
            .map(Some)
            .map_err(|e| format!("Corrupt {key} value: {e}")),
        Ok(None) => Ok(None),
        Err(e) => Err(format!("Failed to read {key}: {e}")),
    }
}

async fn delete_sync_state(db: &Database, key: &'static str) -> Result<(), String> {
    db.delete_sync_state(key)
        .await
        .map_err(|e| format!("Failed to delete {key}: {e}"))
}

fn parse_sync_state<T>(key: &str, value: &str) -> Result<T, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse::<T>()
        .map_err(|e| format!("Corrupt {key} value: {e}"))
}

/// Errors from publishing a changeset and its head.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ChangesetPublishError {
    #[error("outgoing changeset validation failed: {0}")]
    InvalidPacked(String),
    #[error("{0}")]
    BlobPreflight(#[from] PublishBlobError),
    #[error("{0}")]
    Storage(#[from] super::storage::StorageError),
}

/// Push a changeset to the sync storage and update the device head.
pub(crate) async fn push_changeset(
    storage: &dyn SyncStorage,
    db: &Database,
    device_id: &str,
    seq: u64,
    packed: Vec<u8>,
    timestamp: &str,
) -> Result<(), ChangesetPublishError> {
    let (envelope, changeset) = super::envelope::unpack(&packed)
        .map_err(|error| ChangesetPublishError::InvalidPacked(error.to_string()))?;
    if envelope.device_id != device_id || envelope.seq != seq {
        return Err(ChangesetPublishError::InvalidPacked(format!(
            "expected {device_id}/{seq}, packed envelope names {}/{}",
            envelope.device_id, envelope.seq
        )));
    }
    if envelope.changeset_size != changeset.len() {
        return Err(ChangesetPublishError::InvalidPacked(format!(
            "envelope declares {} changeset bytes, packed payload has {}",
            envelope.changeset_size,
            changeset.len()
        )));
    }
    if envelope.author_pubkey.is_none() || envelope.signature.is_none() {
        return Err(ChangesetPublishError::InvalidPacked(
            "outgoing changeset is unsigned".to_string(),
        ));
    }
    if !super::envelope::verify_changeset_signature(&envelope, &changeset) {
        return Err(ChangesetPublishError::InvalidPacked(
            "outgoing changeset signature does not verify".to_string(),
        ));
    }
    ensure_publishable_changeset_blobs(db, storage, &packed).await?;
    storage.put_changeset(device_id, seq, packed).await?;
    storage.put_head(device_id, seq, timestamp).await?;
    Ok(())
}

/// Commit a successful changeset push: advance `local_seq`, then clear the
/// staging record. The order matters — `local_seq` is persisted BEFORE the
/// staged_seq marker and the staged file are cleared, so a crash between them
/// leaves the staged changeset for an idempotent re-push at the same seq while
/// `local_seq` is already advanced, so no later changeset can reuse it and
/// overwrite the pushed one on the remote. Shared by the staged-retry and
/// direct-push arms so the ordering can't drift between them.
async fn commit_push_success(
    db: &Database,
    store_dir: &StoreDir,
    seq: u64,
    pending_changeset_max_id: Option<i64>,
    local_seq: &mut u64,
) -> Result<(), String> {
    *local_seq = seq;
    db.set_sync_state("local_seq", &seq.to_string())
        .await
        .map_err(|e| format!("Failed to persist local_seq after push: {e}"))?;
    if let Some(max_id) = pending_changeset_max_id {
        db.clear_pending_changesets_through(max_id)
            .await
            .map_err(|e| format!("Failed to clear pending changesets after push: {e}"))?;
    }
    delete_sync_state(db, STAGED_PENDING_CHANGESET_ID_KEY).await?;
    db.set_sync_state("staged_seq", "")
        .await
        .map_err(|e| format!("Failed to clear staged_seq after push: {e}"))?;
    clear_staged_changeset(store_dir).await?;
    drain_published_blob_drop_intents(db, store_dir, seq).await?;
    Ok(())
}

async fn persist_staged_push_state(
    db: &Database,
    seq: u64,
    pending_changeset_max_id: i64,
    deferred_local_blob_drops: &[super::service::DeferredLocalBlobDrop],
    consumed_make_remote_intents: &[(String, String)],
) -> Result<(), String> {
    let drops = deferred_local_blob_drops.to_vec();
    let consumed = consumed_make_remote_intents.to_vec();
    db.call(move |conn| {
        let tx = conn.unchecked_transaction().map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO sync_state (key, value) VALUES ('staged_seq', ?1) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            [seq.to_string()],
        )
        .map_err(DbError::from)?;
        tx.execute(
            "INSERT INTO sync_state (key, value) VALUES (?1, ?2) \
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![
                STAGED_PENDING_CHANGESET_ID_KEY,
                pending_changeset_max_id.to_string()
            ],
        )
        .map_err(DbError::from)?;
        for drop in &drops {
            insert_published_blob_drop_intent(&tx, seq, drop)?;
        }
        // Consume the make_remote intents in the same transaction that records the
        // drop intents carrying their pin choice: the intent's deletion and the
        // disposition record commit together, so no crash can drop the intent while
        // leaving the disposition unrecorded (the retry would then default it).
        for (root_table, root_id) in &consumed {
            Database::delete_make_remote_intent_on(&tx, root_table, root_id)?;
        }
        tx.commit().map_err(DbError::from)
    })
    .await
    .map_err(|e| format!("Failed to persist staged push state: {e}"))
}

#[derive(Clone)]
struct PublishedBlobDropIntent {
    seq: u64,
    drop: super::service::DeferredLocalBlobDrop,
}

async fn drain_published_blob_drop_intents(
    db: &Database,
    store_dir: &StoreDir,
    max_seq: u64,
) -> Result<(), String> {
    let intents = load_published_blob_drop_intents(db, max_seq).await?;
    for intent in intents {
        match apply_published_blob_drop_intent(db, store_dir, &intent).await {
            Ok(()) => clear_published_blob_drop_intent(db, &intent).await?,
            Err(error) => warn!(
                seq = intent.seq,
                namespace = %intent.drop.namespace,
                blob_id = %intent.drop.id,
                error = %error,
                "published blob local-store cleanup remains pending"
            ),
        }
    }
    Ok(())
}

async fn load_published_blob_drop_intents(
    db: &Database,
    max_seq: u64,
) -> Result<Vec<PublishedBlobDropIntent>, String> {
    db.call(move |conn| {
        let mut stmt = conn
            .prepare(
                "SELECT seq, namespace, blob_id, size, disposition \
                 FROM published_blob_drop_intents \
                 WHERE seq <= ?1 \
                 ORDER BY seq, namespace, blob_id",
            )
            .map_err(DbError::from)?;
        let intents = stmt
            .query_map([max_seq as i64], |row| {
                let size: Option<i64> = row.get(3)?;
                let size = size.ok_or_else(|| {
                    rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Integer,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "published blob drop intent is missing size",
                        )),
                    )
                })?;
                if size < 0 {
                    return Err(rusqlite::Error::FromSqlConversionFailure(
                        3,
                        rusqlite::types::Type::Integer,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("published blob drop intent has negative size {size}"),
                        )),
                    ));
                }
                let disposition_raw: String = row.get(4)?;
                let disposition = disposition_from_db(&disposition_raw).map_err(|message| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            message,
                        )),
                    )
                })?;
                Ok(PublishedBlobDropIntent {
                    seq: row.get::<_, i64>(0)? as u64,
                    drop: super::service::DeferredLocalBlobDrop {
                        namespace: row.get(1)?,
                        id: row.get(2)?,
                        size: size as u64,
                        disposition,
                    },
                })
            })
            .map_err(DbError::from)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(DbError::from)?;
        Ok(intents)
    })
    .await
    .map_err(|e| format!("Failed to load published blob drop intents: {e}"))
}

async fn apply_published_blob_drop_intent(
    db: &Database,
    store_dir: &StoreDir,
    intent: &PublishedBlobDropIntent,
) -> Result<(), String> {
    super::service::apply_deferred_local_blob_drop(db, store_dir, &intent.drop)
        .await
        .map_err(|e| e.to_string())
}

async fn clear_published_blob_drop_intent(
    db: &Database,
    intent: &PublishedBlobDropIntent,
) -> Result<(), String> {
    let seq = intent.seq;
    let namespace = intent.drop.namespace.clone();
    let id = intent.drop.id.clone();
    db.call(move |conn| {
        conn.execute(
            "DELETE FROM published_blob_drop_intents \
             WHERE seq = ?1 AND namespace = ?2 AND blob_id = ?3",
            rusqlite::params![seq as i64, namespace, id],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .map_err(|e| format!("Failed to clear published blob drop intent: {e}"))
}

/// Record a published blob's local-store disposition, keyed by the `seq` of the
/// changeset whose publication makes the blob Remote. The existing drain
/// (`drain_published_blob_drop_intents`) applies it only once that seq is pushed,
/// so the local copy is never touched before the row that shares it is durable.
///
/// Two commits write here: the host-provided make_remote flip commit records the
/// authoritative disposition first, and the inline-push staging commit records a
/// disposition for every host blob in the pushed changeset — which includes the
/// blob the flip just re-emitted, but with the default disposition, since the flip
/// consumed the make_remote intent that carried `retain_pinned`. `DO NOTHING` keeps
/// the first (authoritative) record, so the flip's pin/eager choice wins over the
/// inline re-scan's default; it also makes a crash-retried stage idempotent.
pub(crate) fn insert_published_blob_drop_intent(
    tx: &rusqlite::Transaction<'_>,
    seq: u64,
    drop: &super::service::DeferredLocalBlobDrop,
) -> Result<(), DbError> {
    tx.execute(
        "INSERT INTO published_blob_drop_intents \
         (seq, namespace, blob_id, size, disposition) \
         VALUES (?1, ?2, ?3, ?4, ?5) \
         ON CONFLICT(seq, namespace, blob_id) DO NOTHING",
        rusqlite::params![
            seq as i64,
            drop.namespace,
            drop.id,
            drop.size as i64,
            disposition_to_db(drop.disposition),
        ],
    )
    .map(|_| ())
    .map_err(DbError::from)
}

fn disposition_to_db(disposition: DeferredLocalBlobDisposition) -> &'static str {
    match disposition {
        DeferredLocalBlobDisposition::Drop => "drop",
        DeferredLocalBlobDisposition::Cache => "cache",
        DeferredLocalBlobDisposition::Pin => "pin",
    }
}

fn disposition_from_db(raw: &str) -> Result<DeferredLocalBlobDisposition, String> {
    match raw {
        "drop" => Ok(DeferredLocalBlobDisposition::Drop),
        "cache" => Ok(DeferredLocalBlobDisposition::Cache),
        "pin" => Ok(DeferredLocalBlobDisposition::Pin),
        other => Err(format!(
            "unknown disposition in published blob drop intent: {other}"
        )),
    }
}

struct SnapshotCut {
    snapshot: super::snapshot::CreatedSnapshot,
    applied_cursors: std::collections::HashMap<String, u64>,
}

async fn capture_snapshot_cut(
    db: &Database,
    temp_dir: PathBuf,
    tables: Vec<super::session::SyncedTable>,
) -> Result<SnapshotCut, DbError> {
    db.call(move |conn| {
        let snapshot = super::snapshot::create_snapshot_with_host_blobs(conn, &temp_dir, &tables)
            .map_err(|e| DbError(e.to_string()))?;
        let applied_cursors = Database::get_all_sync_cursors_on(conn)?;
        Ok(SnapshotCut {
            snapshot,
            applied_cursors,
        })
    })
    .await
}

/// Run a single sync cycle: drain pending local changes + gate + push, pull,
/// bookkeeping, snapshot.
///
/// All connection access goes through `db`. Local writes are published from the
/// durable pending-changeset journal; the pull's apply is a plain connection
/// write that is never journaled, so applied rows are never republished as this
/// device's own changes.
/// Loads/persists all cycle state (local_seq, cursors, staging, snapshots) through
/// `db`'s bookkeeping API rather than keeping mutable state across calls.
pub(crate) async fn run_single_sync_cycle(
    storage: &dyn SyncStorage,
    store_id: &str,
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    db: &Database,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    store_dir: &StoreDir,
    cloud_home: Option<&dyn CloudHome>,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<SyncCycleResult, String> {
    // The synced-table set is owned by the Database; read it once here.
    let tables = db.synced_tables();

    // Load + anchor the membership chain ONCE for the whole cycle, before anything
    // this cycle pushes, judges, or decrypts. Every authorization decision below —
    // the refresh's key-rotation authorization, the pull's changeset/head checks,
    // the outgoing write-grant binding, the snapshot-author check, and the tombstone
    // GC — judges this one chain state, instead of each re-listing and re-downloading
    // it (which also let two reads disagree mid-cycle). Fail-closed: for an
    // owner-pinned store a chain that can't be listed, is wiped, or won't anchor
    // is a tamper/takeover, so this aborts the cycle and retries next time — never
    // falling open to "no rules apply". A membership change a peer publishes
    // mid-cycle is picked up next cycle, the same convergence model as everything
    // else the cycle reads.
    let membership = super::pull::load_cycle_membership(storage, db)
        .await
        .map_err(|e| format!("load membership chain: {e}"))?;

    // Refresh authorization/decryption state BEFORE anything this cycle pushes,
    // judges, or decrypts. Membership and the rotatable store key are
    // per-cycle preconditions, not init-time bootstraps:
    // re-read them now so a removed member's writes are rejected and a rotated key
    // is adopted on a running device without a restart. Runs before the blob drain
    // so the drain (and every push/pull below) uses the current key. A failure here
    // aborts the cycle and retries next time — a refresh that can't complete must
    // not also corrupt state. Adoption itself failing is not this kind of failure —
    // see `rotation_pending` below.
    if let Some(ch) = cloud_home {
        refresh_authorization_state(
            ch,
            cipher,
            pending_rotation,
            db,
            user_keypair,
            custody,
            store_id,
            &membership,
        )
        .await?;
    }

    // Whether this device has adopted everything the store has committed. Read
    // once, right after the refresh that is the one place this cycle could adopt
    // a rotation, and used below to skip every write that would otherwise seal
    // new data under a generation the store has already superseded: the blob
    // upload drain, the host-provided make_remote completion, the inline
    // host-provided blob upload inside `service::sync`, the tombstone write
    // drain, both changeset-push paths, and the snapshot. Pull, local writes, and
    // delete-only tombstone GC is unaffected — the gate
    // is on sealing for the cloud, not on using the store. An unadoptable
    // rotation — including one whose activation entry is not yet visible — is
    // marked pending by the refresh and pauses exactly this set; it never aborts
    // the cycle.
    let rotation_pending = pending_rotation.check(&cipher.snapshot()).err();
    if let Some(pending) = &rotation_pending {
        warn!(
            committed_generation = pending.committed_generation,
            live_generation = pending.live_generation,
            "sync paused: this device has not adopted a committed store-key rotation; \
             sealing nothing new for the cloud until it adopts"
        );
    }

    // Load persisted sync state — DB errors abort the cycle (a transient SQLite
    // error must not make us treat the device as brand-new at seq 0). None (key
    // not set yet) legitimately defaults to 0 / None.
    let mut local_seq = read_sync_state(db, "local_seq").await?.unwrap_or(0);
    let snapshot_seq: Option<u64> = read_sync_state(db, "snapshot_seq").await?;
    let last_snapshot_time: Option<chrono::DateTime<chrono::Utc>> =
        read_sync_state::<chrono::DateTime<chrono::FixedOffset>>(db, "last_snapshot_time")
            .await?
            .map(|time| time.with_timezone(&chrono::Utc));
    let staged_seq: Option<u64> = read_sync_state::<String>(db, "staged_seq")
        .await?
        .filter(|value| !value.is_empty())
        .map(|value| parse_sync_state("staged_seq", &value))
        .transpose()?;
    let staged_pending_changeset_id: Option<i64> =
        read_sync_state::<String>(db, STAGED_PENDING_CHANGESET_ID_KEY)
            .await?
            .filter(|value| !value.is_empty())
            .map(|value| parse_sync_state(STAGED_PENDING_CHANGESET_ID_KEY, &value))
            .transpose()?;
    match (staged_seq, staged_pending_changeset_id) {
        (None, Some(_)) => {
            return Err("staged pending-journal marker exists without staged_seq".to_string())
        }
        (Some(_), None) => {
            if matches!(
                db.pending_changeset_batch()
                    .await
                    .map_err(|e| format!("Failed to validate staged pending journal: {e}"))?,
                PendingChangesetBatch::Pending { .. }
            ) {
                return Err(
                    "staged_seq exists without its pending-journal marker while journal rows remain"
                        .to_string(),
                );
            }
        }
        _ => {}
    }
    drain_published_blob_drop_intents(db, store_dir, local_seq).await?;

    // One wall-clock reading for this whole cycle. Every head this cycle writes
    // records it as the device's `last_sync` (RFC 3339, per the `put_head`
    // contract), and the status built at the end reports the same instant — so a
    // device's published head and its own status agree on when it last synced. The
    // changeset envelope timestamp is a separate HLC stamp (`timestamp` below); it
    // orders causally and must not be confused with this display time.
    let sync_time = clock.now().to_rfc3339();

    // Drain the blob engine's upload queue. Blob-before-row ordering is enforced by
    // the gate column: a root being made Remote stays gated off until its last
    // user-provided blob lands, and coven flips it on inside the drain (the
    // make_remote completion), breaking the drain so this cycle publishes the
    // now-shareable subtree instead of waiting for the whole batch. The changeset is
    // gated per row, not by a global "any upload pending" flag. The drain reports
    // whether it broke to publish, which drives the loop's cadence below.
    let mut resume_drain_promptly = false;
    if let Some(ch) = cloud_home {
        if rotation_pending.is_none() {
            match crate::blob::upload::drain_uploads(
                db,
                ch,
                cipher,
                pending_rotation,
                store_id,
                store_dir,
                clock,
                hlc,
                observer,
            )
            .await
            {
                Ok(outcome) => {
                    resume_drain_promptly = outcome.yielded_for_publish;
                    if outcome.uploaded > 0 {
                        info!(count = outcome.uploaded, "Drained blob uploads");
                    }
                }
                Err(e) => warn!("Blob upload drain error: {e}"),
            }
        }
    }

    // Retry a staged changeset left behind by a failed push in an earlier cycle.
    // Staging holds the gated, push-ready bytes so a lost push can re-push exactly
    // them next cycle without re-deriving; the span below stages-then-pushes.
    if let Some(seq) = staged_seq {
        if rotation_pending.is_some() {
            debug!(
                seq,
                "rotation pending; leaving the staged changeset queued until adoption"
            );
        } else if let Some(staged_data) = read_staged_changeset(store_dir).await? {
            info!(seq, "Retrying staged changeset push");

            match push_changeset(storage, db, device_id, seq, staged_data, &sync_time).await {
                Ok(()) => {
                    info!(seq, "Staged changeset push succeeded");
                    commit_push_success(
                        db,
                        store_dir,
                        seq,
                        staged_pending_changeset_id,
                        &mut local_seq,
                    )
                    .await?;
                }
                Err(e) => return Err(format!("Staged changeset push failed: {e}")),
            }
        } else {
            return Err(format!(
                "staged_seq {seq} has no staged changeset payload; preserving sequence and pending journal"
            ));
        }
    }

    let timestamp = hlc.now().to_string();

    if rotation_pending.is_some() {
        debug!(
            "rotation pending; leaving ready host-provided make_remote intents queued until adoption"
        );
    } else if super::service::complete_host_provided_make_remotes(
        db, tables, storage, &timestamp, store_dir, local_seq,
    )
    .await
    .map_err(|e| format!("Host-provided make_remote completion failed: {e}"))?
    {
        resume_drain_promptly = true;
    }

    let pending_changesets = db
        .pending_changeset_batch()
        .await
        .map_err(|e| format!("Failed to load pending changesets: {e}"))?;
    let (pending_changeset_max_id, outgoing_changeset) = match &pending_changesets {
        PendingChangesetBatch::Empty => (None, Vec::new()),
        PendingChangesetBatch::Pending { max_id, changeset } => (Some(*max_id), changeset.clone()),
    };

    // Run the core gate + push-prep + pull.
    let sync_result = super::service::sync(
        device_id,
        db,
        tables,
        outgoing_changeset,
        local_seq,
        storage,
        &timestamp,
        "background sync",
        user_keypair,
        store_dir,
        membership.chain.as_ref(),
        membership.pinned_owner.as_deref(),
        rotation_pending.is_some(),
    )
    .await
    .map_err(|e| format!("Sync cycle error: {e}"))?;

    // Propagate the pending changesets. The gate already cut any row whose
    // gate column is off (the host keeps a blob-bearing row gated until its
    // blobs upload), so whatever the gate emitted is safe to publish now —
    // there is no global upload deferral. Stage the bytes before pushing so a
    // push failure re-pushes exactly these gated bytes; the staged-retry above
    // re-pushes on the next cycle.
    if let Some(outgoing) = &sync_result.outgoing {
        if rotation_pending.is_some() {
            debug!(
                seq = outgoing.seq,
                "rotation pending; leaving the outgoing changeset queued until adoption"
            );
        } else {
            let seq = outgoing.seq;
            let pending_changeset_max_id = pending_changeset_max_id
                .ok_or_else(|| "outgoing changeset without pending journal rows".to_string())?;

            // The staged file is the sole record that seq N's exact bytes may
            // already be on the remote, so it must be directory-durable: the
            // recovery branch below reads its presence to decide whether the
            // push committed, and that inference has to hold across power loss,
            // not only a process crash. (Cache blobs are re-fetchable and use
            // the cheaper `write_atomic`, which skips the parent-directory
            // fsync.)
            crate::local_blob::write_atomic_durable(&staging_path(store_dir), &outgoing.packed)
                .await
                .map_err(|e| format!("Failed to stage outgoing changeset: {e}"))?;
            persist_staged_push_state(
                db,
                seq,
                pending_changeset_max_id,
                &outgoing.deferred_local_blob_drops,
                &outgoing.consumed_make_remote_intents,
            )
            .await?;

            match push_changeset(
                storage,
                db,
                device_id,
                seq,
                outgoing.packed.clone(),
                &sync_time,
            )
            .await
            {
                Ok(()) => {
                    commit_push_success(
                        db,
                        store_dir,
                        seq,
                        Some(pending_changeset_max_id),
                        &mut local_seq,
                    )
                    .await?;
                    info!(seq, "Pushed changeset");
                }
                Err(e) => {
                    warn!(seq, "Push failed, changeset staged for retry: {e}");
                }
            }
        }
    } else if let Some(max_id) = pending_changeset_max_id {
        db.clear_pending_changesets_through(max_id)
            .await
            .map_err(|e| format!("Failed to clear gated-empty pending changesets: {e}"))?;
    }

    // Publish this device's signed pull-ack: how far it has pulled every other
    // device, the cursor vector each accepted apply persisted with its rows.
    // Changeset reclamation reads it
    // to compute a floor that strands no member; nothing else consumes it. A stale
    // or failed ack only narrows the next reclamation — it never blocks a pull or a
    // push — so a failure here is logged, not fatal. Runs every cycle so the acked
    // cursors track pull progress whether or not a snapshot is published.
    let ack = super::signed_control::AckJson::signed(
        device_id,
        sync_result.updated_cursors.clone().into_iter().collect(),
        user_keypair,
    );
    match serde_json::to_vec(&ack) {
        Ok(bytes) => {
            if let Err(e) = storage.put_ack(device_id, bytes).await {
                warn!(device_id = %device_id, "Failed to publish pull-ack: {e}");
            }
        }
        Err(e) => warn!(device_id = %device_id, "Failed to serialize pull-ack: {e}"),
    }

    // Republish our head every cycle, even when we pushed no changeset of our
    // own. push_changeset writes the head only when this device produces a
    // changeset — so a device that only pulls would otherwise never refresh
    // its head. The head's last-sync time is what the sync-status view reads
    // to show how recently each device synced; writing it here after the pull
    // keeps that current. Best-effort: a transient failure leaves last cycle's
    // head, and the next cycle republishes unconditionally, so we log rather
    // than abort.
    if let Err(e) = storage.put_head(device_id, local_seq, &sync_time).await {
        warn!("Failed to republish head after pull: {e}");
    }

    // Flush the clock's high-water mark so a restart re-seeds past it. The pull
    // already advanced the clock in the row-and-cursor commit closure, so
    // `high_water` here reflects both that
    // advance and any host stamps minted this cycle (e.g. the changeset envelope
    // timestamp), since it reads the clock's current state. A persist error aborts
    // the cycle rather than risking a backward jump after restart.
    db.set_sync_state(
        crate::sync::hlc::HIGHWATER_STATE_KEY,
        &hlc.high_water().to_string(),
    )
    .await
    .map_err(|e| format!("Failed to persist HLC high-water mark: {e}"))?;

    // Turn queued blob deletes into signed cloud tombstones (the deletion's
    // durable record), then GC tombstones whose convergence grace has passed
    // (the actual blob deletion). Holding the blob for the grace
    // keeps a peer that still references the row from being stranded; the
    // signature stops a non-member forging a deletion. (This is blob-tombstone
    // GC; changeset reclamation runs separately, after a snapshot is published.)
    if let Some(ch) = cloud_home {
        if rotation_pending.is_none() {
            match crate::blob::delete::drain_tombstones(
                db,
                ch,
                cipher,
                pending_rotation,
                store_id,
                user_keypair,
                clock,
            )
            .await
            {
                Ok(n) if n > 0 => info!(count = n, "Wrote blob tombstones"),
                Err(e) => warn!("Tombstone drain error: {e}"),
                _ => {}
            }
        }
        // Authorize every reclaim against the cycle's once-loaded chain, already
        // anchored to the device's pinned owner (set on join/restore/found). A
        // per-tombstone re-load would both repeat the cycle's listing and risk
        // judging a different chain state; the load's own fail-closed anchor is
        // what keeps deleting user blobs on an unverifiable owner impossible.
        match crate::blob::delete::gc_tombstones(
            db,
            ch,
            cipher,
            store_id,
            &hex::encode(user_keypair.public_key()),
            membership.chain.as_ref(),
            clock,
            db.blob_tombstone_grace(),
        )
        .await
        {
            Ok(n) if n > 0 => {
                info!(count = n, "Reclaimed blobs past the tombstone grace")
            }
            Err(e) => warn!("Tombstone GC error: {e}"),
            _ => {}
        }
    }

    // Check snapshot policy.
    let hours_since = last_snapshot_time.map(|t| {
        let elapsed = clock.now().signed_duration_since(t);
        elapsed.num_hours().max(0) as u64
    });

    // Initial sync: store has data but the pending journal produced no changeset
    // (data was inserted before the cycle ran — e.g. user connected a provider to
    // an existing store). Push a snapshot so the existing data reaches the cloud.
    let is_initial_sync =
        local_seq == 0 && snapshot_seq.is_none() && sync_result.outgoing.is_none();

    // The snapshot is the second channel that propagates rows to peers. It
    // applies the same row-level gate as the changeset push (create_snapshot runs
    // the gate's delete_gated_false), so a row whose gate column is off — which
    // the host keeps off until its blobs upload — is already excluded. No global
    // upload deferral is needed: the snapshot can never carry a row whose blobs
    // aren't in the cloud.
    // Owner-only snapshots: a snapshot restates the whole catalog — the image a new
    // device bootstraps from wholesale — so only a current Owner may author one.
    // Decide whether a snapshot is both due and permitted BEFORE create_snapshot
    // (the VACUUM), so a non-owner never builds an image, publishes one readers
    // would reject, or runs the reclaim a publish triggers. A non-owner's rows still
    // propagate via the changeset push above.
    let snapshot_due = is_initial_sync
        || super::snapshot::should_create_snapshot(local_seq, snapshot_seq, hours_since);
    let may_snapshot = if rotation_pending.is_some() {
        // A snapshot restates and re-seals the whole catalog under the store key —
        // exactly the kind of new cloud content the pending rotation must block.
        false
    } else if snapshot_due {
        // Judge against the cycle's once-loaded chain, the same acceptance-side rule
        // the readers apply: an initialized store requires a current Owner. A caller
        // before initialization can have no chain and is accepted on its verified
        // identity alone. The chain was already listed, anchored, and (for an
        // owner-pinned store) fail-closed at the top of the cycle, so the only
        // outcome here is authorized-or-not: an unauthorized result skips the
        // snapshot.
        let our_pk = hex::encode(user_keypair.public_key());
        match super::membership_ops::authorize_loaded_membership_author(
            membership.chain.as_ref(),
            &our_pk,
            super::membership_ops::MembershipAuthorRequirement::Owner,
        ) {
            Ok(()) => true,
            Err(reason) => {
                info!(
                    device = %our_pk,
                    owner = membership.pinned_owner.as_deref().unwrap_or("<none>"),
                    %reason,
                    "Snapshot skipped: this device may not author a snapshot"
                );
                false
            }
        }
    } else {
        false
    };

    if may_snapshot {
        if is_initial_sync {
            info!("Initial sync: pushing snapshot of existing store data");
        } else {
            info!("Snapshot policy triggered, creating snapshot");
        }

        // Scratch the snapshot copy in the store dir, not the shared system
        // temp dir: create_snapshot writes a fixed `snapshot.db` filename, so two
        // stores syncing concurrently (or parallel tests) would otherwise race
        // on one `/tmp/snapshot.db`. A store's own cycles run serially.
        let temp_dir = store_dir.as_ref().to_path_buf();
        let snapshot_result = capture_snapshot_cut(db, temp_dir, tables.to_vec()).await;

        match snapshot_result {
            Ok(cut) => {
                super::service::upload_snapshot_host_blobs(
                    db,
                    storage,
                    store_dir,
                    &cut.snapshot.host_blobs,
                    &timestamp,
                )
                .await
                .map_err(|e| format!("Snapshot host-provided blob upload failed: {e}"))?;

                match super::snapshot::push_snapshot(
                    storage,
                    store_id,
                    cut.snapshot.db_image,
                    device_id,
                    cut.applied_cursors,
                    local_seq,
                    db.schema_version(),
                    user_keypair,
                    clock,
                    super::snapshot::SnapshotBlobPreflight {
                        db,
                        blobs: &cut.snapshot.publish_blobs,
                    },
                )
                .await
                {
                    Ok(()) => {
                        db.set_sync_state("snapshot_seq", &local_seq.to_string())
                            .await
                            .map_err(|e| format!("Failed to persist snapshot_seq: {e}"))?;
                        db.set_sync_state("last_snapshot_time", &clock.now().to_rfc3339())
                            .await
                            .map_err(|e| format!("Failed to persist last_snapshot_time: {e}"))?;

                        info!(local_seq, "Snapshot created and pushed");

                        // Reclaim changeset logs the fresh snapshot now covers and
                        // every current device has acked. A fresh snapshot most
                        // relaxes the snapshot-cursor floor term, so this runs at
                        // snapshot cadence to maximize reclaim. Anchor authorization
                        // to the device's pinned owner (the same pin the tombstone GC
                        // reads); a read failure skips reclaim this cycle rather than
                        // falling back to trust-on-first-use. Logged-not-fatal: a
                        // leftover changeset is unreferenced storage the next snapshot's
                        // reclaim sweeps, never a wrong state a reader observes.
                        match db
                            .get_sync_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
                            .await
                        {
                            Ok(pinned_owner) => {
                                match super::snapshot::reclaim_superseded_changesets(
                                    storage,
                                    store_id,
                                    pinned_owner.as_deref(),
                                    Some(db),
                                )
                                .await
                                {
                                    Ok(r) if r.deleted > 0 || r.errors > 0 => info!(
                                        deleted = r.deleted,
                                        errors = r.errors,
                                        "Reclaimed superseded changesets"
                                    ),
                                    Ok(_) => {}
                                    Err(e) => warn!("Changeset reclamation error: {e}"),
                                }
                            }
                            Err(e) => {
                                warn!("Changeset reclamation skipped: failed to read pinned owner: {e}")
                            }
                        }
                    }
                    Err(e) => warn!("Failed to push snapshot: {e}"),
                }
            }
            Err(e) => warn!("Failed to create snapshot: {e}"),
        }
    }

    // Build status from remote heads. Reuse this cycle's `sync_time` so the
    // status's `last_sync_time` matches the head this cycle wrote.
    let core_status = super::status::build_sync_status(
        &sync_result.pull.remote_heads,
        device_id,
        Some(&sync_time),
    );

    let constraint_conflicts = sync_result.pull.constraint_conflicts.len() as u64;
    Ok(SyncCycleResult {
        changesets_applied: sync_result.pull.changesets_applied,
        skipped_schema: sync_result.pull.skipped_schema,
        rejected_unauthorized: sync_result.pull.rejected_unauthorized.len() as u64,
        invalid_signatures: sync_result.pull.invalid_signatures.len() as u64,
        held_changesets: sync_result.pull.held_changesets,
        constraint_conflicts,
        device_activity: core_status.other_devices,
        sync_time,
        asset_downloads_failed: sync_result.pull.asset_downloads_failed,
        local_blob_cleanup_pending: sync_result.pull.local_blob_cleanup_pending,
        row_changes: sync_result.pull.row_changes,
        resume_drain_promptly,
        rotation_pending,
    })
}

/// Refresh this device's authorization/decryption state at the top of a cycle:
/// the membership chain (re-anchored to the pinned owner) and the rotatable
/// store key. Membership and key state are per-cycle preconditions, not
/// init-time bootstraps — without this a running device acts on a stale member
/// set and keeps a dead store key after a rotation it did not perform,
/// recovering only on restart.
///
/// A plaintext (browsable) home still has the owner-anchored membership chain
/// loaded for this cycle, but it has no wrapped store key to rotate. The key
/// refresh is therefore a no-op there; membership authorization is not.
///
/// Fail-closed: for an initialized store the cycle's shared membership
/// load has already aborted the cycle if the chain can't be listed, is wiped, or
/// won't anchor — so `membership.chain` is present whenever an owner is pinned.
///
/// A rotation this refresh discovers but cannot adopt (no custody handed to this
/// cycle, or custody's own persist fails) is not a reason to abort the cycle —
/// `pending_rotation` marks the committed generation instead, and the caller
/// gates every seal on it for the rest of this cycle. Membership state that
/// can't be resolved at all (an invisible activation, a read failure) still
/// aborts: those mean this device doesn't reliably know the current state, which
/// is a different condition from "knows the state and can't adopt it yet".
#[allow(clippy::too_many_arguments)]
async fn refresh_authorization_state(
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    db: &Database,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    store_id: &str,
    membership: &super::pull::CycleMembership,
) -> Result<(), String> {
    // A plaintext home has no encrypted store key to rotate. Its membership
    // chain remains load-bearing elsewhere in the cycle.
    if cipher.snapshot().is_plaintext() {
        debug!("refresh: plaintext home, nothing to refresh");
        return Ok(());
    }

    // The store's founder, pinned at create/join/restore, anchors chain identity;
    // wrapped-key adoption is authorized against the current Owner set from that
    // anchored chain. Without a pinned owner there is nothing to anchor against — a
    // production store always has one, since founding precedes any sync cycle — so
    // its absence means there is no shared state to refresh this cycle; skip it. The
    // cycle load couples the pinned owner with its anchored chain (an owner-pinned
    // store that can't produce a valid chain aborted the load), so an owner here
    // always travels with a chain; a pinned owner WITHOUT a chain contradicts that
    // invariant and fails loud rather than reading as "not founded".
    let chain = match (
        membership.pinned_owner.as_deref(),
        membership.chain.as_ref(),
    ) {
        (Some(_), Some(chain)) => chain,
        (None, _) => {
            debug!("refresh: no owner pinned yet (store not founded); nothing to anchor against");
            return Ok(());
        }
        (Some(owner), None) => {
            return Err(format!(
                "refresh: owner {owner} is pinned but the cycle's membership load \
                 produced no chain — the load's invariant is broken"
            ));
        }
    };

    let current_owners: Vec<String> = chain
        .current_members()
        .into_iter()
        .filter_map(|(pubkey, role)| {
            (role == super::membership::MemberRole::Owner).then_some(pubkey)
        })
        .collect();
    // The visible activation coordinates are the cycle's raw membership LIST — an
    // entry is "visible" as soon as it is listed, which is the view the wrapped-key
    // activation gate checks against (distinct from the committed chain above).
    let visible_membership_coords = membership
        .listed_entries
        .iter()
        .map(|(author_pubkey, seq)| super::membership::MembershipCoord {
            author_pubkey: author_pubkey.clone(),
            seq: *seq,
        })
        .collect::<Vec<_>>();

    // 2. Adopt a rotated store key. Scan the current Owners' prefixes for this
    //    device's re-wrapped key (`keys/{owner}/{self}`), authenticating each
    //    against the owner whose prefix it sits under and taking the highest
    //    generation. The signature binds (store_id, recipient, author, sealed),
    //    so a bucket writer can't substitute it, relocate it, or change its signer.
    //    If the decrypted keyring carries a strictly newer generation, swap the
    //    live cipher (and persist to the keyring) via `apply_key_rotation`, so
    //    this same cycle's push/pull/blob ops use it.
    let live_keyring = match cipher.snapshot() {
        super::cloud_storage::CloudCipher::Encrypted(encryption) => encryption,
        super::cloud_storage::CloudCipher::Plaintext => {
            return Err("refresh: plaintext home cannot enter encrypted key refresh".to_string())
        }
    };
    match super::invite::unwrap_store_keyring_for_owners_with_activation(
        cloud_home,
        user_keypair,
        store_id,
        current_owners.iter().map(String::as_str),
        Some(&visible_membership_coords),
    )
    .await
    {
        Ok(new_encryption) => {
            // Key identity is the key itself, not its generation number: adopt if
            // the scan resolved any key the live keyring does not already hold —
            // including a fork at the SAME generation number two owners minted at
            // once, which a generation comparison would wrongly ignore. Merging
            // (not comparing generations) is what makes a concurrent-rotation fork
            // converge instead of partition.
            let merged = live_keyring.merged_with(&new_encryption);
            if merged.key_count() == live_keyring.key_count() {
                // Every key this scan resolved is already held. Not adopted — and,
                // crucially, `pending_rotation` is NOT cleared here (only a
                // successful adoption clears it), so an earlier mark that a stale
                // rescan (a decoy wrap from a non-rotating owner, or a LIST lag)
                // can't re-observe still survives.
                debug!("refresh: wrapped store key adds nothing new; keeping the live keyring");
            } else {
                match custody {
                    None => {
                        pending_rotation.mark_committed(merged.current_generation());
                        info!(
                            committed_generation = merged.current_generation(),
                            "refresh: found a rotated store key but this cycle has no \
                             master-key custody to adopt it; sealing is paused until a \
                             cycle with custody adopts it"
                        );
                    }
                    Some(custody) => {
                        match super::membership_ops::apply_key_rotation(
                            new_encryption,
                            custody,
                            cipher,
                            pending_rotation,
                        ) {
                            Ok(fingerprint) => info!(%fingerprint, "Adopted rotated store key"),
                            Err(e) => warn!(
                                "refresh: could not adopt a rotated store key ({e}); sealing \
                                 is paused until adoption succeeds"
                            ),
                        }
                    }
                }
            }
        }
        // No wrapped key for this device under any current owner: a solo store
        // that has never shared (its creation key is the store key), or a device
        // removed from the store (each owner deleted its `keys/{owner}/{self}`).
        // Nothing to adopt; keep the live key. A *remaining* member always has a
        // `keys/{owner}/{self}` re-wrapped on rotation, so this is never a current
        // member silently stuck on a stale key.
        Err(super::invite::InviteError::CloudHome(
            crate::storage::cloud::CloudHomeError::NotFound(_),
        )) => {
            debug!("refresh: no wrapped key for this device; keeping the live key");
        }
        Err(super::invite::InviteError::InactiveWrappedKey {
            activation,
            generation,
        }) => {
            // A rotated wrap whose activation entry is not yet visible names a
            // committed generation this device cannot yet adopt (an owner
            // overwrote the wrap before its Remove entry uploaded, or the reader's
            // LIST lags the entry). This is a pending rotation, not a cycle
            // failure: pause sealing at the wrap's committed generation and let
            // the cycle proceed — pull and local writes run, every seal path is
            // gated on `rotation_pending`. Adoption completes on a later cycle
            // once the activation entry is visible.
            pending_rotation.mark_committed(generation);
            info!(
                committed_generation = generation,
                activation = %format!("{}/{}", activation.author_pubkey, activation.seq),
                "refresh: a rotated wrapped store key's activation entry is not yet \
                 visible; sealing is paused until it is and this device adopts"
            );
        }
        Err(e) => return Err(format!("refresh: read this device's wrapped key: {e}")),
    }

    // Durably record whatever the marker now holds — a newly-marked pending
    // rotation, or its clearing on adoption — before this cycle seals anything.
    // A restart mid-pause must not forget the pause and seal under the superseded
    // generation just because a fresh cloud scan happens to lag behind it.
    super::cloud_storage::persist_pending_rotation(db, pending_rotation)
        .await
        .map_err(|e| format!("refresh: persist pending rotation: {e}"))?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum InitSyncError {
    #[error("no synced tables configured; pass a non-empty synced-table set before sync starts")]
    NoSyncedTables,
    #[error("cloud cipher and blob path scheme describe different storage modes")]
    IncoherentStorageRepresentation,
    #[error("membership chain bootstrap/anchor failed: {0}")]
    MembershipAnchor(String),
    #[error("restoring the persisted pending rotation failed: {0}")]
    PendingRotationRestore(String),
}

/// Establish the storage representation and signed owner anchor over an
/// already-built [`CloudSyncStorage`], returning the only runnable sync session.
pub async fn init_sync_over_storage(
    db: &Database,
    storage: CloudSyncStorage,
) -> Result<SyncComponents, InitSyncError> {
    // Integration guard. The host declared its synced tables on the builder; an
    // empty set means a synced store would attach nothing, every changeset would
    // come out empty, and sync would silently become snapshot-only. Refuse loudly
    // instead of pretending to sync.
    if db.synced_tables().is_empty() {
        return Err(InitSyncError::NoSyncedTables);
    }

    let cipher = storage.cipher_state().clone();
    let cipher_is_plaintext = cipher.is_plaintext();
    let representation_is_coherent = matches!(
        (cipher_is_plaintext, storage.blob_path_scheme()),
        (true, BlobPathScheme::Plain) | (false, BlobPathScheme::Hashed)
    );
    if !representation_is_coherent {
        return Err(InitSyncError::IncoherentStorageRepresentation);
    }

    let hlc = db.hlc();
    let user_keypair = storage.user_keypair().clone();
    ensure_owner_anchored_chain(&storage, db, &user_keypair, &hlc)
        .await
        .map_err(InitSyncError::MembershipAnchor)?;

    // Restore any durably-recorded pending rotation into this connection's marker
    // before the first cycle seals anything, so a restart that interrupted an
    // unadopted rotation resumes paused rather than sealing under the superseded
    // generation.
    if !cipher_is_plaintext {
        crate::sync::cloud_storage::restore_pending_rotation(
            db,
            &storage.shared_pending_rotation(),
        )
        .await
        .map_err(|e| InitSyncError::PendingRotationRestore(e.to_string()))?;
    }

    let store_id = storage.store_id().to_string();
    let device_id = hlc.device_id().to_string();
    let pending_rotation = storage.shared_pending_rotation();
    info!("Sync initialized (device: {device_id})");

    Ok(SyncComponents {
        storage: std::sync::Arc::new(storage),
        db: db.clone(),
        hlc,
        store_id,
        device_id,
        cipher,
        pending_rotation,
        user_keypair,
    })
}

/// Establish or verify the owner-anchored membership chain for a store.
/// Returns once the chain is established and verified, or an error to abort sync.
///
/// Cloud publication and the local trust transaction cannot be one transaction,
/// so this completes an interrupted own founder publication idempotently. A
/// founder entry without its signed head is uncommitted; a committed own founder
/// is validated before the owner and complete head floor are recorded together.
/// A committed chain founded by a different key is never adopted.
pub async fn ensure_owner_anchored_chain(
    storage: &dyn SyncStorage,
    db: &Database,
    owner_keypair: &UserKeypair,
    hlc: &Hlc,
) -> Result<(), String> {
    use super::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let our_pk = hex::encode(owner_keypair.public_key());
    let pinned = db
        .get_sync_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|e| format!("read pinned owner: {e}"))?;
    let entries = storage
        .list_membership_entries()
        .await
        .map_err(|e| format!("list membership entries: {e}"))?;
    let expected_owner = pinned.as_deref().unwrap_or(&our_pk);
    let loaded =
        super::membership_ops::load_and_persist_owner_anchor(storage, &entries, expected_owner, db)
            .await
            .map_err(|error| error.to_string())?;
    if loaded.is_some() {
        return Ok(());
    }
    if let Some(pinned) = pinned {
        return Err(format!(
            "membership chain has no committed heads but owner {pinned} is pinned \
             — refusing (wiped or tampered membership/*)"
        ));
    }

    publish_or_complete_founder(storage, owner_keypair, hlc).await?;
    let committed_entries = storage
        .list_membership_entries()
        .await
        .map_err(|e| format!("list membership entries after founder publish: {e}"))?;
    super::membership_ops::load_and_persist_owner_anchor(storage, &committed_entries, &our_pk, db)
        .await
        .map_err(|error| error.to_string())?
        .ok_or_else(|| {
            "founder publish produced no signed committed membership head".to_string()
        })?;
    info!(owner = %our_pk, "Founded store: wrote owner-anchored founder entry");
    Ok(())
}

async fn publish_or_complete_founder(
    storage: &dyn SyncStorage,
    owner_keypair: &UserKeypair,
    hlc: &Hlc,
) -> Result<(), String> {
    use super::membership::{MembershipChain, MembershipCoord};
    use super::storage::StorageError;

    let owner_pubkey = hex::encode(owner_keypair.public_key());
    let coord = MembershipCoord {
        author_pubkey: owner_pubkey.clone(),
        seq: 1,
    };
    match storage.get_membership_entry(&owner_pubkey, 1).await {
        Ok(bytes) => {
            let entry = super::membership_ops::parse_membership_entry_at(&coord, &bytes)?;
            let chain = MembershipChain::from_entries_with_coords(vec![(coord, entry)])
                .map_err(|error| format!("invalid interrupted founder entry: {error}"))?;
            if !chain.is_founded_by(&owner_pubkey) {
                return Err(
                    "interrupted founder entry is not this storage identity's founder".to_string(),
                );
            }
            storage
                .put_membership_entry(&owner_pubkey, 1, bytes)
                .await
                .map_err(|error| format!("re-publish interrupted founder entry: {error}"))?;
            super::membership_ops::publish_membership_head(storage, &chain, owner_keypair)
                .await
                .map_err(|error| format!("publish interrupted founder head: {error}"))?;
            Ok(())
        }
        Err(StorageError::NotFound(_)) => {
            let timestamp = hlc.now().to_string();
            super::membership_ops::write_founder_entry(storage, owner_keypair, &timestamp).await
        }
        Err(error) => Err(format!("read interrupted founder entry: {error}")),
    }
}

/// Components needed to run sync cycles.
///
/// Owns the exact database, storage, register clock, device identity, at-rest
/// cipher, pending-rotation marker, and signing identity that initialization
/// checked. Callers cannot replace any of them before running a cycle.
pub struct SyncComponents {
    storage: std::sync::Arc<CloudSyncStorage>,
    db: Database,
    hlc: std::sync::Arc<Hlc>,
    /// The store this sync loop is for. Binds the snapshot meta/pointer it
    /// publishes so a member of two stores can't replay one's catalog as the
    /// other's.
    store_id: String,
    device_id: String,
    cipher: std::sync::Arc<CloudCipherState>,
    pending_rotation: std::sync::Arc<PendingRotation>,
    user_keypair: UserKeypair,
}

impl SyncComponents {
    pub fn storage(&self) -> &std::sync::Arc<CloudSyncStorage> {
        &self.storage
    }

    pub fn hlc(&self) -> &std::sync::Arc<Hlc> {
        &self.hlc
    }

    pub fn user_keypair(&self) -> &UserKeypair {
        &self.user_keypair
    }

    pub fn blob_path_scheme(&self) -> BlobPathScheme {
        self.storage.blob_path_scheme()
    }

    pub fn current_encryption(&self) -> Option<crate::encryption::EncryptionService> {
        self.cipher.encryption()
    }

    pub fn self_uploader(&self) -> String {
        self.storage.self_uploader()
    }

    pub async fn drain_uploads(
        &self,
        clock: &dyn crate::clock::Clock,
        store_dir: &StoreDir,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<crate::blob::upload::DrainOutcome, DbError> {
        crate::blob::upload::drain_uploads(
            &self.db,
            self.storage.cloud_home(),
            &self.cipher,
            &self.pending_rotation,
            &self.store_id,
            store_dir,
            clock,
            &self.hlc,
            observer,
        )
        .await
    }

    pub async fn invite_member(
        &self,
        public_key_hex: &str,
        invitee_email: Option<&str>,
        role: super::membership::MemberRole,
        store_name: &str,
    ) -> Result<crate::join_code::InviteCode, super::membership_ops::MembershipOpsError> {
        let encryption = self
            .current_encryption()
            .ok_or(super::membership_ops::MembershipOpsError::NotEncryptedHome)?;
        super::membership_ops::invite_member(
            &*self.storage,
            self.storage.cloud_home(),
            &self.user_keypair,
            &self.hlc,
            public_key_hex,
            invitee_email,
            role,
            &encryption,
            &self.store_id,
            store_name,
        )
        .await
    }

    pub async fn remove_member(
        &self,
        public_key_hex: &str,
        custody: &dyn MasterKeyCustody,
    ) -> Result<String, super::membership_ops::MembershipOpsError> {
        let encryption = self
            .current_encryption()
            .ok_or(super::membership_ops::MembershipOpsError::NotEncryptedHome)?;
        super::membership_ops::remove_member(
            &*self.storage,
            self.storage.cloud_home(),
            &self.user_keypair,
            &self.hlc,
            public_key_hex,
            &self.store_id,
            &encryption,
            custody,
            &self.cipher,
            &self.pending_rotation,
        )
        .await
    }

    pub async fn persist_pending_rotation(&self) -> Result<(), DbError> {
        super::cloud_storage::persist_pending_rotation(&self.db, &self.pending_rotation).await
    }

    pub fn adopt_key_rotation(
        &self,
        encryption: crate::encryption::EncryptionService,
        custody: &dyn MasterKeyCustody,
    ) -> Result<String, crate::keys::KeyError> {
        super::membership_ops::apply_key_rotation(
            encryption,
            custody,
            &self.cipher,
            &self.pending_rotation,
        )
    }

    pub async fn run_cycle(
        &self,
        clock: &dyn crate::clock::Clock,
        custody: Option<&dyn MasterKeyCustody>,
        store_dir: &StoreDir,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<SyncCycleResult, String> {
        run_single_sync_cycle(
            &*self.storage,
            &self.store_id,
            &self.device_id,
            &self.hlc,
            clock,
            &self.db,
            &self.cipher,
            &self.pending_rotation,
            &self.user_keypair,
            custody,
            store_dir,
            Some(self.storage.cloud_home()),
            observer,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn snapshot_image_and_cursor_vector_share_one_database_cut() {
        let db = crate::sync::test_helpers::open_test_db();
        db.call(|conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            tx.execute(
                "INSERT INTO notes \
                 (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('n1', 'covered-at-one', NULL, 1, \
                         '0000000001000-0000-dev1', '2026-01-01')",
                [],
            )
            .map_err(DbError::from)?;
            Database::advance_sync_cursor_on(&tx, "dev1", 0, 1)?;
            tx.commit().map_err(DbError::from)
        })
        .await
        .expect("seed covered state");

        let temp = tempfile::tempdir().expect("snapshot temp dir");
        let cut = capture_snapshot_cut(&db, temp.path().to_path_buf(), db.synced_tables().to_vec())
            .await
            .expect("capture snapshot cut");

        db.call(|conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            tx.execute(
                "UPDATE notes SET title = 'covered-at-two', \
                 _updated_at = '0000000002000-0000-dev1' WHERE id = 'n1'",
                [],
            )
            .map_err(DbError::from)?;
            Database::advance_sync_cursor_on(&tx, "dev1", 1, 2)?;
            tx.commit().map_err(DbError::from)
        })
        .await
        .expect("advance live state after snapshot cut");

        let image_path = temp.path().join("captured.db");
        std::fs::write(&image_path, &cut.snapshot.db_image).expect("write captured image");
        let image = rusqlite::Connection::open(&image_path).expect("open captured image");
        let captured_title: String = image
            .query_row("SELECT title FROM notes WHERE id = 'n1'", [], |row| {
                row.get(0)
            })
            .expect("captured row");

        assert_eq!(captured_title, "covered-at-one");
        assert_eq!(cut.applied_cursors.get("dev1"), Some(&1));
        assert_eq!(
            crate::sync::test_helpers::query_text(&db, "SELECT title FROM notes WHERE id = 'n1'",)
                .await,
            "covered-at-two",
        );
        assert_eq!(
            db.get_all_sync_cursors().await.unwrap().get("dev1"),
            Some(&2)
        );
    }
}
