//! Sync cycle orchestration.
//!
//! Runs a single sync cycle (gate + push local changes, pull remote changes,
//! manage snapshots) and initializes sync infrastructure. All connection access
//! goes through the owned [`Database`]. Local changes are published from the
//! durable pending-changeset journal, which each host write appends to inside its
//! own journaled transaction — so a host write landing mid-cycle is captured for
//! the next outgoing changeset, while the pull's apply is a plain connection write
//! that is never journaled and so never echoes applied rows.

#[cfg(test)]
use std::future::Future;
use std::path::PathBuf;
#[cfg(test)]
use std::pin::Pin;
use std::str::FromStr;

use tracing::{debug, info, warn};

use crate::blob::BlobTransitionObserver;
use crate::changeset::RowChange;
use crate::database::{Database, DbError};
use crate::keys::{MasterKeyCustody, UserKeypair};
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{
    BlobPathScheme, CloudCipherAccess, CloudCipherState, CloudSyncStorage, PendingRotation,
    RotationPending,
};
use super::cycle_engine::{AuthorizedCycleEngine, CycleEngine, PostPullCycleEngine};
use super::hlc::Hlc;
use super::service::DeferredLocalBlobDisposition;
use super::status::DeviceActivity;
use super::storage::SyncStorage;
use super::store_pull::HeldStorePosition;

/// Result of a single sync cycle.
#[derive(Debug)]
pub struct SyncCycleResult {
    /// Number of remote changesets that were applied.
    pub changesets_applied: u64,
    /// Changesets whose present cloud object failed validation or apply. The
    /// position is held at the bad seq for that device. Carries per-changeset
    /// detail (device, seq, reason) so a host can say which changesets are
    /// stalled, not only how many.
    pub held_positions: Vec<HeldStorePosition>,
    /// Per-device activity of the other devices seen in the sync storage —
    /// device id, its member's author key, latest seq, and RFC 3339 last-sync
    /// time — so a host can render which devices synced and when.
    pub device_activity: Vec<DeviceActivity>,
    /// RFC 3339 timestamp of when this cycle completed.
    pub sync_time: String,
    /// Blobs needed before apply failed to download; their changesets and positions
    /// remain pending.
    pub asset_downloads_failed: bool,
    /// Post-commit local blob cleanup still has durable filesystem work pending.
    /// Its corresponding rows and positions are already durable.
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SyncCycleFailure {
    Offline(String),
    Failed(String),
}

impl SyncCycleFailure {
    pub(crate) fn operation<E>(operation: &str, error: E) -> Self
    where
        E: std::error::Error + 'static,
    {
        let offline = error_chain_contains_transport(&error);
        let message = format!("{operation}: {error}");
        if offline {
            Self::Offline(message)
        } else {
            Self::Failed(message)
        }
    }

    pub fn is_offline(&self) -> bool {
        matches!(self, Self::Offline(_))
    }

    #[cfg(test)]
    pub(crate) fn contains(&self, pattern: &str) -> bool {
        self.to_string().contains(pattern)
    }
}

impl From<String> for SyncCycleFailure {
    fn from(message: String) -> Self {
        Self::Failed(message)
    }
}

impl std::fmt::Display for SyncCycleFailure {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Offline(message) | Self::Failed(message) => formatter.write_str(message),
        }
    }
}

impl std::error::Error for SyncCycleFailure {}

fn error_chain_contains_transport(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(source) = current {
        if source
            .downcast_ref::<super::storage::StorageError>()
            .is_some_and(super::storage::StorageError::is_transport)
            || source
                .downcast_ref::<crate::storage::cloud::CloudHomeError>()
                .is_some_and(|error| {
                    matches!(
                        error,
                        crate::storage::cloud::CloudHomeError::Transport(_)
                            | crate::storage::cloud::CloudHomeError::Io(_)
                    )
                })
            || source
                .downcast_ref::<super::storage::CoordinationError>()
                .is_some_and(|error| matches!(error, super::storage::CoordinationError::Storage(_)))
        {
            return true;
        }
        current = source.source();
    }
    false
}

#[cfg(test)]
mod sync_cycle_failure_tests {
    use super::*;

    #[test]
    fn registration_transport_source_is_offline() {
        let error = super::super::store_registration::StoreRegistrationError::Object(
            super::super::store_objects::StoreObjectError::Storage(
                super::super::storage::StorageError::Storage("provider unavailable".to_string()),
            ),
        );

        let object = std::error::Error::source(&error).expect("object source");
        assert!(object
            .downcast_ref::<super::super::store_objects::StoreObjectError>()
            .is_some());
        let storage = object.source().expect("storage source");
        assert!(storage
            .downcast_ref::<super::super::storage::StorageError>()
            .is_some());

        assert!(SyncCycleFailure::operation("register", error).is_offline());
    }

    #[test]
    fn registration_configuration_source_is_failed() {
        let error = super::super::store_registration::StoreRegistrationError::Object(
            super::super::store_objects::StoreObjectError::Storage(
                super::super::storage::StorageError::Configuration("missing bucket".to_string()),
            ),
        );

        assert!(!SyncCycleFailure::operation("register", error).is_offline());
    }

    #[test]
    fn unavailable_coordination_is_failed() {
        let error = super::super::storage::CoordinationError::Unavailable(
            "provider has no coordination capability".to_string(),
        );

        assert!(!SyncCycleFailure::operation("coordinate", error).is_offline());
    }
}

async fn read_protocol_state<T>(db: &Database, key: &str) -> Result<Option<T>, String>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    match db.get_protocol_state(key).await {
        Ok(Some(value)) => value
            .parse::<T>()
            .map(Some)
            .map_err(|e| format!("Corrupt {key} value: {e}")),
        Ok(None) => Ok(None),
        Err(e) => Err(format!("Failed to read {key}: {e}")),
    }
}

#[derive(Clone)]
struct PublishedBlobDropIntent {
    seq: u64,
    drop: super::service::DeferredLocalBlobDrop,
}

pub(crate) async fn drain_published_blob_drop_intents(
    db: &Database,
    store_dir: &StoreDir,
    max_seq: u64,
) -> Result<(), String> {
    let intents = load_published_blob_drop_intents(db, max_seq).await?;
    for intent in intents {
        apply_published_blob_drop_intent(db, store_dir, &intent).await?;
        clear_published_blob_drop_intent(db, &intent).await?;
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
                "SELECT seq, namespace, blob_id, size, plaintext_hash, locator_hash, disposition \
                 FROM published_blob_drop_intents \
                 WHERE seq <= ?1 \
                   AND NOT EXISTS (\
                       SELECT 1 FROM store_write_blob_leases lease \
                       WHERE lease.namespace = published_blob_drop_intents.namespace \
                         AND lease.blob_id = published_blob_drop_intents.blob_id\
                   ) \
                 ORDER BY seq, namespace, blob_id, locator_hash",
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
                let plaintext_hash = row.get::<_, String>(4)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        4,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid published blob plaintext hash: {error}"),
                        )),
                    )
                })?;
                let locator_hash = row.get::<_, String>(5)?.parse().map_err(|error| {
                    rusqlite::Error::FromSqlConversionFailure(
                        5,
                        rusqlite::types::Type::Text,
                        Box::new(std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid published blob locator hash: {error}"),
                        )),
                    )
                })?;
                let disposition_raw: String = row.get(6)?;
                let disposition = disposition_from_db(&disposition_raw).map_err(|message| {
                    rusqlite::Error::FromSqlConversionFailure(
                        6,
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
                        plaintext_hash,
                        locator_hash,
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
    let locator_hash = intent.drop.locator_hash.to_string();
    db.call(move |conn| {
        conn.execute(
            "DELETE FROM published_blob_drop_intents \
             WHERE seq = ?1 AND namespace = ?2 AND blob_id = ?3 AND locator_hash = ?4",
            rusqlite::params![seq as i64, namespace, id, locator_hash],
        )
        .map(|_| ())
        .map_err(DbError::from)
    })
    .await
    .map_err(|e| format!("Failed to clear published blob drop intent: {e}"))
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
    coverage: super::store_commit::CommitFrontier,
}

async fn capture_snapshot_cut(
    db: &Database,
    temp_dir: PathBuf,
    tables: Vec<super::session::SyncedTable>,
) -> Result<SnapshotCut, DbError> {
    let write_policy = db.write_policy();
    db.call(move |conn| {
        let pending: i64 = conn
            .query_row(
                "SELECT EXISTS(
                    SELECT 1 FROM store_writes
                    WHERE status != '\"local_only\"'
                      AND json_extract(status, '$.published') IS NULL
                )",
                [],
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        if pending != 0 {
            return Err(DbError::Message(
                "snapshot cut refused while unpublished Store writes exist".to_string(),
            ));
        }
        let snapshot = super::snapshot::create_snapshot_with_host_blobs(conn, &temp_dir, &tables)
            .map_err(|e| DbError::Message(e.to_string()))?;
        let coverage = super::store_commit::CommitFrontier::from_refs(
            write_policy,
            Database::materialized_frontier_on(conn, None)?,
        )
        .map_err(|error| DbError::Message(format!("snapshot coverage: {error}")))?;
        Ok(SnapshotCut { snapshot, coverage })
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
/// Loads/persists all cycle state (local_seq, positions, staging, snapshots) through
/// `db`'s bookkeeping API rather than keeping mutable state across calls.
#[cfg(test)]
pub(crate) async fn run_single_sync_cycle(
    storage: &dyn SyncStorage,
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
) -> Result<SyncCycleResult, SyncCycleFailure> {
    let authorization = CycleEngine::load(storage, None, db)
        .await?
        .authorize()
        .await?;
    Box::pin(run_single_sync_cycle_with_authorization(
        device_id,
        hlc,
        clock,
        cipher,
        pending_rotation,
        user_keypair,
        custody,
        None,
        store_dir,
        cloud_home,
        observer,
        authorization,
    ))
    .await
}

#[allow(clippy::too_many_arguments)]
#[cfg(test)]
pub(crate) async fn run_single_sync_cycle_with_coordination(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn super::storage::CoordinationStorage>,
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    db: &Database,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    routing_encryption: Option<&crate::encryption::EncryptionService>,
    store_dir: &StoreDir,
    cloud_home: Option<&dyn CloudHome>,
    observer: Option<&dyn BlobTransitionObserver>,
) -> Result<SyncCycleResult, SyncCycleFailure> {
    let authorization = CycleEngine::load(storage, serial_coordination, db)
        .await?
        .authorize()
        .await?;
    let cycle_future: Pin<Box<dyn Future<Output = _> + Send + '_>> =
        Box::pin(run_single_sync_cycle_with_authorization(
            device_id,
            hlc,
            clock,
            cipher,
            pending_rotation,
            user_keypair,
            custody,
            routing_encryption,
            store_dir,
            cloud_home,
            observer,
            authorization,
        ));
    cycle_future.await
}

#[allow(clippy::too_many_arguments)]
async fn run_single_sync_cycle_with_authorization(
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    routing_encryption: Option<&crate::encryption::EncryptionService>,
    store_dir: &StoreDir,
    cloud_home: Option<&dyn CloudHome>,
    observer: Option<&dyn BlobTransitionObserver>,
    authorization: AuthorizedCycleEngine<'_>,
) -> Result<SyncCycleResult, SyncCycleFailure> {
    let prepared = match Box::pin(prepare_cycle_before_pull(
        device_id,
        hlc,
        clock,
        cipher,
        pending_rotation,
        user_keypair,
        custody,
        routing_encryption,
        store_dir,
        cloud_home,
        observer,
        &authorization,
    ))
    .await?
    {
        CycleBeforePull::Continue(prepared) => prepared,
        CycleBeforePull::Complete(result) => return Ok(result),
    };
    let store_pull = authorization.pull(store_dir, user_keypair).await?;
    let post_pull = authorization.after_pull().await?;
    let completed = Box::pin(complete_cycle_after_pull(
        device_id,
        hlc,
        clock,
        user_keypair,
        store_dir,
        &authorization,
        &post_pull,
        prepared,
        store_pull,
    ))
    .await?;
    if completed.rotation_pending.is_none() {
        authorization
            .stage_and_publish_ack(user_keypair, &completed.sync_time)
            .await?;
        Box::pin(reclaim_cycle_packages(device_id, user_keypair, &post_pull)).await?;
    }
    let core_status = super::status::build_sync_status(
        &completed.store_pull.visible_heads,
        device_id,
        Some(&completed.sync_time),
    );
    Ok(SyncCycleResult {
        changesets_applied: completed.store_pull.changesets_applied,
        held_positions: completed.store_pull.held_positions,
        device_activity: core_status.other_devices,
        sync_time: completed.sync_time,
        asset_downloads_failed: completed.store_pull.asset_downloads_failed,
        local_blob_cleanup_pending: completed.local_blob_cleanup_pending,
        row_changes: completed.store_pull.row_changes,
        resume_drain_promptly: completed.resume_drain_promptly,
        rotation_pending: completed.rotation_pending,
    })
}

struct PreparedCycle {
    last_snapshot_time: Option<chrono::DateTime<chrono::Utc>>,
    last_snapshot_position: Option<u64>,
    has_snapshot: bool,
    sync_time: String,
    resume_drain_promptly: bool,
    rotation_pending: Option<RotationPending>,
}

enum CycleBeforePull {
    Continue(PreparedCycle),
    Complete(SyncCycleResult),
}

struct CompletedPullCycle {
    store_pull: super::store_pull::StorePullResult,
    local_blob_cleanup_pending: bool,
    sync_time: String,
    resume_drain_promptly: bool,
    rotation_pending: Option<RotationPending>,
}

#[allow(clippy::too_many_arguments)]
async fn prepare_cycle_before_pull(
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    routing_encryption: Option<&crate::encryption::EncryptionService>,
    store_dir: &StoreDir,
    cloud_home: Option<&dyn CloudHome>,
    observer: Option<&dyn BlobTransitionObserver>,
    authorization: &AuthorizedCycleEngine<'_>,
) -> Result<CycleBeforePull, SyncCycleFailure> {
    let db = authorization.db();
    let storage = authorization.storage();
    let store_root = authorization.store_root();
    let store_root_hash = store_root.store_root_hash;
    let protocol_store_id = store_root.store_root_id.to_string();

    // Refresh authorization/decryption state BEFORE anything this cycle pushes,
    // judges, or decrypts. Membership and the rotatable store key are
    // per-cycle preconditions, not init-time bootstraps:
    // re-read them now so a removed member's writes are rejected and a rotated key
    // is adopted on a running device without a restart. Runs before the blob drain
    // so the drain (and every push/pull below) uses the current key. A failure here
    // aborts the cycle and retries next time — a refresh that can't complete must
    // not also corrupt state. Adoption itself failing is not this kind of failure —
    // see `rotation_pending` below.
    let recipient = crate::keys::public_key_hex(user_keypair);
    let wrapped_keys = authorization.wrapped_keys(&recipient)?;
    refresh_authorization_state(
        storage,
        store_root_hash,
        cipher,
        pending_rotation,
        db,
        user_keypair,
        custody,
        &protocol_store_id,
        &wrapped_keys,
    )
    .await
    .map_err(|error| SyncCycleFailure::operation("refresh authorization state", error))?;

    // Whether this device has adopted everything the store has committed. Read
    // once, right after the refresh that is the one place this cycle could adopt
    // a rotation, and used below to skip every write that would otherwise seal
    // new data under a generation the store has already superseded: the blob
    // upload drain, the inline blob upload inside `service::sync`, the tombstone
    // write drain, both changeset-push paths, and the snapshot. Pull, local writes,
    // and delete-only tombstone GC are unaffected — the gate
    // is on sealing for the cloud, not on using the store. An unadoptable
    // rotation is marked pending by the refresh and pauses exactly this set; it
    // never aborts the cycle.
    let rotation_pending = pending_rotation.check(&cipher.snapshot()).err();
    if let Some(pending) = &rotation_pending {
        warn!(
            committed_generation = pending.committed_generation,
            live_generation = pending.live_generation,
            "sync paused: this device has not adopted a committed store-key rotation; \
             sealing nothing new for the cloud until it adopts"
        );
    }

    if let Some(home) = cloud_home {
        if rotation_pending.is_none() {
            let drained = crate::blob::delete::drain_tombstones(
                db,
                home,
                cipher,
                pending_rotation,
                &protocol_store_id,
                user_keypair,
                clock,
            )
            .await
            .map_err(|error| format!("drain queued blob tombstones: {error}"))?;
            if drained > 0 {
                info!(count = drained, "Drained blob tombstones");
            }
        }
        let reclaimed = authorization
            .gc_tombstones(
                home,
                cipher,
                &protocol_store_id,
                &hex::encode(user_keypair.public_key()),
                clock,
                db.blob_tombstone_grace(),
            )
            .await
            .map_err(|error| format!("garbage-collect blob tombstones: {error}"))?;
        if reclaimed > 0 {
            info!(count = reclaimed, "Reclaimed tombstoned blobs");
        }
    }

    let local_seq = db
        .latest_local_store_position()
        .await
        .map_err(|error| format!("read local Store position: {error}"))?
        .map_or(0, |reference| reference.coord.sequence());
    let last_snapshot_time: Option<chrono::DateTime<chrono::Utc>> =
        read_protocol_state::<chrono::DateTime<chrono::FixedOffset>>(db, "last_snapshot_time")
            .await?
            .map(|time| time.with_timezone(&chrono::Utc));
    let last_snapshot = db
        .latest_local_store_snapshot()
        .await
        .map_err(|error| format!("read latest exact Store snapshot: {error}"))?;
    let last_snapshot_position = match last_snapshot.as_ref() {
        None => None,
        Some(snapshot) => Some(
            authorization
                .snapshot_position(snapshot, device_id, user_keypair)
                .await?,
        ),
    };
    let has_snapshot = last_snapshot.is_some();
    drain_published_blob_drop_intents(db, store_dir, local_seq).await?;

    // One wall-clock reading for this whole cycle. Store acknowledgements and
    // the status built at the end record the same instant. Store write commits
    // carry a separate HLC stamp (`timestamp` below) for causal ordering.
    let sync_time = clock.now().to_rfc3339();

    let mut resume_drain_promptly = false;
    if rotation_pending.is_none() {
        let outcome = crate::blob::upload::drain_uploads(
            db,
            storage,
            store_dir,
            clock,
            hlc,
            routing_encryption,
            observer,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("drain queued blob uploads", error))?;
        if outcome.failures.has_transport_failure() {
            return Err(SyncCycleFailure::operation(
                "upload queued blobs",
                outcome.failures,
            ));
        }
        resume_drain_promptly = outcome.yielded_for_publish;
        if outcome.uploaded > 0 {
            info!(count = outcome.uploaded, "Drained blob uploads");
        }
    }

    if rotation_pending.is_none() {
        let published = authorization
            .drain_store_writes()
            .await
            .map_err(|error| SyncCycleFailure::operation("publish queued Store writes", error))?;
        if published > 0 {
            info!(published, "Published queued Store writes");
        }
    }

    if authorization.should_stop_before_pull().await? {
        return Ok(CycleBeforePull::Complete(SyncCycleResult {
            changesets_applied: 0,
            held_positions: Vec::new(),
            device_activity: Vec::new(),
            sync_time,
            asset_downloads_failed: false,
            local_blob_cleanup_pending: false,
            row_changes: Vec::new(),
            resume_drain_promptly: false,
            rotation_pending,
        }));
    }

    Ok(CycleBeforePull::Continue(PreparedCycle {
        last_snapshot_time,
        last_snapshot_position,
        has_snapshot,
        sync_time,
        resume_drain_promptly,
        rotation_pending,
    }))
}

#[allow(clippy::too_many_arguments)]
async fn complete_cycle_after_pull(
    device_id: &str,
    hlc: &Hlc,
    clock: &dyn crate::clock::Clock,
    user_keypair: &UserKeypair,
    store_dir: &StoreDir,
    authorization: &AuthorizedCycleEngine<'_>,
    post_pull: &PostPullCycleEngine<'_, '_>,
    prepared: PreparedCycle,
    store_pull: super::store_pull::StorePullResult,
) -> Result<CompletedPullCycle, SyncCycleFailure> {
    let db = authorization.db();
    let storage = authorization.storage();
    let PreparedCycle {
        last_snapshot_time,
        last_snapshot_position,
        has_snapshot,
        sync_time,
        resume_drain_promptly,
        rotation_pending,
    } = prepared;
    let tables = db.synced_tables();

    // A Serial registration appends to the exact global predecessor. Materialize
    // the selected global prefix first so the registration commit and its durable
    // predecessor enter one continuous local chain. The same pull also installs
    // the membership state that decides whether this active member may register.
    if rotation_pending.is_none() {
        authorization
            .ensure_active_registration(user_keypair, &sync_time)
            .await?;
    }

    let staged_store_batch = if rotation_pending.is_none() {
        let mut staged_any = false;
        loop {
            let staged = authorization
                .prepare_pending_store_write(device_id, &sync_time, user_keypair, store_dir)
                .await?;
            if !staged {
                break;
            }
            staged_any = true;
            let published = authorization
                .drain_store_writes()
                .await
                .map_err(|error| SyncCycleFailure::operation("publish Store write", error))?;
            if published > 0 {
                info!(published, "Published Store writes");
            }
        }
        staged_any
    } else {
        false
    };

    let local_seq = db
        .latest_local_store_position()
        .await
        .map_err(|error| format!("read local Store position after publish: {error}"))?
        .map_or(0, |position| position.coord.sequence());
    drain_published_blob_drop_intents(db, store_dir, local_seq).await?;
    let local_blob_cleanup_pending = crate::blob::local_cleanup::drain(db, store_dir)
        .await
        .map_err(|error| format!("drain local blob cleanup after Store publication: {error}"))?
        || store_pull.local_blob_cleanup_pending;

    // Flush the clock's high-water mark so a restart re-seeds past it. Store pull
    // advances the clock in the row-and-materialized-position commit closure, so
    // `high_water` reflects remote commits and host stamps minted this cycle. A
    // persist error aborts the cycle rather than risking a backward jump.
    db.set_protocol_state(
        crate::sync::hlc::HIGHWATER_STATE_KEY,
        &hlc.high_water().to_string(),
    )
    .await
    .map_err(|e| format!("Failed to persist HLC high-water mark: {e}"))?;

    // Check snapshot policy.
    let hours_since = last_snapshot_time.map(|t| {
        let elapsed = clock.now().signed_duration_since(t);
        elapsed.num_hours().max(0) as u64
    });

    // Initial sync: the store has data but no Store write published it (for example,
    // a provider was connected to an existing store). Publish a snapshot so the
    // existing data reaches the cloud.
    let is_initial_sync = local_seq == 0 && !has_snapshot && !staged_store_batch;

    // The snapshot is the second channel that propagates rows to peers. It
    // applies the same row-level gate as the changeset push, captures every
    // surviving blob row in the immutable cut, and publishes that exact blob
    // closure before activating the snapshot metadata.
    // Owner-only snapshots: a snapshot restates the whole catalog — the image a new
    // device bootstraps from wholesale — so only a current Owner may author one.
    // Decide whether a snapshot is both due and permitted BEFORE create_snapshot
    // (the VACUUM), so a non-owner never builds an image, publishes one readers
    // would reject, or runs the reclaim a publish triggers. A non-owner's rows still
    // propagate via the changeset push above.
    let resumed_snapshot = super::store_snapshot::drain_outbound_store_snapshot(storage, db)
        .await
        .map_err(|error| SyncCycleFailure::operation("publish pending Store snapshot", error))?
        .is_some();
    let snapshot_due = !resumed_snapshot
        && (is_initial_sync
            || super::snapshot::should_create_snapshot(
                local_seq,
                last_snapshot_position,
                hours_since,
            ));
    let may_snapshot = if rotation_pending.is_some() {
        // A snapshot restates and re-seals the whole catalog under the store key —
        // exactly the kind of new cloud content the pending rotation must block.
        false
    } else if snapshot_due {
        // Judge against the policy-shaped post-pull membership authority using the
        // same current-Owner rule as readers. Merge retains the chain verified at
        // cycle authorization; Serial uses the authorization materialized by this
        // pull. Loading either authority fails the cycle, while a verified non-owner
        // skips the snapshot.
        let our_pk = hex::encode(user_keypair.public_key());
        let authorized = post_pull.may_author_snapshot(&our_pk);
        match authorized {
            Ok(()) => true,
            Err(reason) => {
                info!(device = %our_pk, %reason, "Snapshot skipped: this device may not author a snapshot");
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
                let meta = authorization
                    .push_snapshot(
                        cut.snapshot,
                        cut.coverage,
                        db.schema_version(),
                        user_keypair,
                        sync_time.clone(),
                    )
                    .await?;
                info!(local_seq, snapshot = %meta.snapshot_hash(), "Snapshot created and pushed");
            }
            Err(e) => warn!("Failed to create snapshot: {e}"),
        }
    }

    Ok(CompletedPullCycle {
        store_pull,
        local_blob_cleanup_pending,
        sync_time,
        resume_drain_promptly,
        rotation_pending,
    })
}

async fn reclaim_cycle_packages(
    device_id: &str,
    user_keypair: &UserKeypair,
    post_pull: &PostPullCycleEngine<'_, '_>,
) -> Result<(), SyncCycleFailure> {
    match post_pull.reclaim_packages(device_id, user_keypair).await {
        Ok(result) if result.packages_deleted > 0 => info!(
            packages = result.packages_deleted,
            copies = result.physical_copies_deleted,
            "Reclaimed snapshot-covered Store packages"
        ),
        Ok(_) => {}
        Err(
            error @ (super::store_reclaim::StoreReclaimError::NoSnapshot
            | super::store_reclaim::StoreReclaimError::MissingRegisteredDevice { .. }
            | super::store_reclaim::StoreReclaimError::MissingAcknowledgement { .. }
            | super::store_reclaim::StoreReclaimError::StaleAcknowledgement { .. }),
        ) => info!(%error, "Store package reclamation is awaiting coverage"),
        Err(error) => return Err(SyncCycleFailure::operation("reclaim Store packages", error)),
    }
    Ok(())
}

/// Refresh this device's authorization/decryption state at the top of a cycle:
/// the policy-shaped membership state and the rotatable store key. Membership
/// and key state are per-cycle preconditions, not
/// init-time bootstraps — without this a running device acts on a stale member
/// set and keeps a dead store key after a rotation it did not perform,
/// recovering only on restart.
///
/// A plaintext (browsable) home still loads membership for authorization, but it
/// has no wrapped store key to rotate. The key refresh is therefore a no-op.
///
/// A rotation this refresh discovers but cannot adopt (no custody handed to this
/// cycle, or custody's own persist fails) is not a reason to abort the cycle —
/// `pending_rotation` marks the committed generation instead, and the caller
/// gates every seal on it for the rest of this cycle. Membership state that
/// can't be resolved at all (a conflict or exact-object read failure) still
/// aborts: those mean this device doesn't reliably know the current state, which
/// is a different condition from "knows the state and can't adopt it yet".
#[derive(Debug, thiserror::Error)]
enum AuthorizationRefreshError {
    #[error("read this device's wrapped key: {0}")]
    WrappedKey(#[source] super::invite::InviteError),
    #[error("refresh state is invalid: {0}")]
    InvalidState(String),
    #[error("persist pending rotation: {0}")]
    Database(String),
}

#[allow(clippy::too_many_arguments)]
async fn refresh_authorization_state(
    storage: &dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    db: &Database,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    store_id: &str,
    wrapped_keys: &[super::wrapped_store_key::WrappedStoreKeyRef],
) -> Result<(), AuthorizationRefreshError> {
    // A plaintext home has no encrypted store key to rotate. Its policy-shaped
    // membership authority remains load-bearing elsewhere in the cycle.
    if cipher.snapshot().is_plaintext() {
        debug!("refresh: plaintext home, nothing to refresh");
        return Ok(());
    }

    // Adopt every exact wrapped-key ref selected by current membership or Serial
    // commit authority. Each ref binds its semantic path and bytes; the value's
    // signature binds the Store, recipient, generation, author, and sealed keyring.
    // A new key is persisted and installed through `apply_key_rotation`, so this
    // same cycle's push, pull, and blob operations use it.
    let live_keyring = match cipher.snapshot() {
        super::cloud_storage::CloudCipher::Encrypted(encryption) => encryption,
        super::cloud_storage::CloudCipher::Plaintext => {
            return Err(AuthorizationRefreshError::InvalidState(
                "plaintext home cannot enter encrypted key refresh".to_string(),
            ))
        }
    };
    if wrapped_keys.is_empty() {
        debug!("refresh: no activated wrapped key for this device; keeping the live key");
        return Ok(());
    }
    match super::invite::unwrap_store_keyring_for_refs(
        storage,
        store_root_hash,
        user_keypair,
        store_id,
        wrapped_keys,
    )
    .await
    {
        Ok(new_encryption) => {
            // Key identity is the key itself, not its generation number: adopt if
            // the authority resolved any key the live keyring does not already hold —
            // including a fork at the SAME generation number two owners minted at
            // once, which a generation comparison would wrongly ignore. Merging
            // (not comparing generations) is what makes a concurrent-rotation fork
            // converge instead of partition.
            let merged = live_keyring.merged_with(&new_encryption);
            if merged.key_count() == live_keyring.key_count() {
                // Every authority-selected key is already held. Not adopted — and,
                // crucially, `pending_rotation` is NOT cleared here (only a
                // successful adoption clears it), so an earlier failed local
                // adoption remains visible until that key is installed.
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
        Err(error) => return Err(AuthorizationRefreshError::WrappedKey(error)),
    }

    // Durably record whatever the marker now holds — a newly-marked pending
    // rotation, or its clearing on adoption — before this cycle seals anything.
    // A restart mid-pause must not forget the pause and seal under the superseded
    // generation just because a fresh cloud scan happens to lag behind it.
    super::cloud_storage::persist_pending_rotation(db, pending_rotation)
        .await
        .map_err(|error| AuthorizationRefreshError::Database(error.to_string()))?;

    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum InitSyncError {
    #[error("no synced tables configured; pass a non-empty synced-table set before sync starts")]
    NoSyncedTables,
    #[error("cloud cipher and blob path scheme describe different storage modes")]
    IncoherentStorageRepresentation,
    #[error("Store row routing initialization failed: {0}")]
    RowRouting(String),
    #[error("Store protocol root failed: {0}")]
    StoreProtocolRoot(String),
    #[error("membership chain bootstrap/anchor failed: {0}")]
    MembershipAnchor(String),
    #[error("restoring the persisted pending rotation failed: {0}")]
    PendingRotationRestore(String),
}

/// Establish the storage representation and signed owner anchor over an
/// already-built [`CloudSyncStorage`], returning the only runnable sync session.
#[derive(Debug, Clone)]
pub enum StoreInitialization {
    CreateStore,
    OpenStore {
        expected_store_root: super::store_commit::StoreRootRef,
    },
}

pub async fn init_sync_over_storage(
    db: &Database,
    storage: CloudSyncStorage,
    initialization: StoreInitialization,
    routing_encryption: Option<crate::encryption::EncryptionService>,
) -> Result<SyncComponents, InitSyncError> {
    // Integration guard. The host declared its synced tables on the builder; an
    // empty set means a synced store would attach nothing, every changeset would
    // come out empty, and sync would silently become snapshot-only. Refuse loudly
    // instead of pretending to sync.
    if db.synced_tables().is_empty() {
        return Err(InitSyncError::NoSyncedTables);
    }
    Database::validate_store_write_routing(
        db.gates().as_ref(),
        db.write_policy(),
        routing_encryption.as_ref(),
    )
    .map_err(|error| InitSyncError::RowRouting(error.into_message()))?;

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
    let store_id = storage.store_id().to_string();
    let store_protocol_root = match initialization {
        StoreInitialization::CreateStore => {
            super::store_protocol_root::create_store(
                db,
                &storage,
                &hlc.now().to_string(),
                &user_keypair,
            )
            .await
        }
        StoreInitialization::OpenStore {
            expected_store_root,
        } => super::store_protocol_root::open_store(db, &storage, &expected_store_root).await,
    }
    .map_err(|error| InitSyncError::StoreProtocolRoot(error.to_string()))?;
    let store_root_ref = db
        .local_store_root_ref()
        .await
        .map_err(|error| InitSyncError::StoreProtocolRoot(error.to_string()))?
        .ok_or_else(|| {
            InitSyncError::StoreProtocolRoot(
                "opened Store root has no durable exact reference".to_string(),
            )
        })?;
    match store_protocol_root.descriptor.write_policy {
        crate::WritePolicy::MergeConcurrent => {
            ensure_owner_anchored_chain(
                &storage,
                db,
                &store_root_ref,
                &store_protocol_root,
                &user_keypair,
            )
            .await
            .map_err(InitSyncError::MembershipAnchor)?;
        }
        crate::WritePolicy::Serial => {
            ensure_serial_founder_authorization(
                &storage,
                db,
                &store_root_ref,
                &store_protocol_root,
            )
            .await
            .map_err(InitSyncError::MembershipAnchor)?;
        }
    }

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

    let mut device_id = db
        .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
        .await
        .map_err(|error| InitSyncError::StoreProtocolRoot(error.to_string()))?;
    if device_id.is_none()
        && store_protocol_root.descriptor.founder_pubkey
            == crate::keys::public_key_hex(&user_keypair)
    {
        super::store_registration::install_existing_founder_device(
            db,
            &storage,
            &store_root_ref,
            &user_keypair,
        )
        .await
        .map_err(|error| InitSyncError::StoreProtocolRoot(error.to_string()))?;
        device_id = db
            .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
            .await
            .map_err(|error| InitSyncError::StoreProtocolRoot(error.to_string()))?;
    }
    let device_id = device_id.ok_or_else(|| {
        InitSyncError::StoreProtocolRoot(
            "initialized Store has no local device registration id".to_string(),
        )
    })?;
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
        routing_encryption,
    })
}

pub(crate) async fn ensure_serial_founder_authorization(
    storage: &dyn super::storage::SyncStorage,
    db: &Database,
    root_ref: &super::store_commit::StoreRootRef,
    root: &super::store_commit::StoreProtocolRoot,
) -> Result<(), String> {
    let pinned = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| format!("read pinned Serial founder: {error}"))?;
    if pinned
        .as_ref()
        .is_some_and(|founder| founder != &root.descriptor.founder_pubkey)
    {
        return Err(format!(
            "pinned Serial founder {:?} does not match Store root founder {:?}",
            pinned.as_deref(),
            root.descriptor.founder_pubkey
        ));
    }
    let authorization = db
        .serial_authorization_state()
        .await
        .map_err(|error| format!("read Serial authorization state: {error}"))?;
    match (pinned, authorization) {
        (Some(_), Some(authorization)) => {
            if authorization.membership.store_root_hash() != root.object_hash() {
                return Err("Serial membership state belongs to another Store root".to_string());
            }
            Ok(())
        }
        (None, None) => {
            let founder = super::store_objects::load_founder_registration(storage, root_ref)
                .await
                .map_err(|error| error.to_string())?;
            let founder_ref = super::store_commit::StoreDeviceRegistrationRef::from_registration(
                &founder.value,
                founder.object,
            );
            let authorization = super::membership::SerialAuthorizationState::from_founder(
                root_ref,
                root,
                &founder_ref,
                &founder.value,
            )
            .map_err(|error| error.to_string())?;
            db.install_serial_root_authorization(
                root.descriptor.founder_pubkey.clone(),
                authorization,
            )
            .await
            .map_err(|error| error.to_string())
        }
        _ => Err("Serial founder pin and authorization are only valid together".to_string()),
    }
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
    root: &super::store_commit::StoreRootRef,
    store_protocol_root: &super::store_commit::StoreProtocolRoot,
    owner_keypair: &UserKeypair,
) -> Result<(), String> {
    if root.store_root_hash != store_protocol_root.object_hash() {
        return Err("local Store root reference differs from the opened Store root".to_string());
    }
    let chain = super::membership_ops::load_and_persist_owner_anchor(
        storage,
        root,
        &crate::keys::public_key_hex(owner_keypair),
        db,
    )
    .await
    .map_err(|error| error.to_string())?;
    let founder = chain
        .founder_entry()
        .ok_or_else(|| "membership founder is absent from Store membership chain".to_string())?;
    if store_protocol_root
        .descriptor
        .validate_merge_founder_entry(founder)
        .is_err()
    {
        return Err("membership founder does not match Store protocol root".to_string());
    }
    Ok(())
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
    routing_encryption: Option<crate::encryption::EncryptionService>,
}

struct MembershipOperationContext<'a> {
    encryption: crate::encryption::EncryptionService,
    serial: Option<super::membership_ops::SerialMembershipContext<'a>>,
}

impl SyncComponents {
    fn device_exclusion_coordination(
        &self,
    ) -> Result<
        Option<&dyn super::storage::CoordinationStorage>,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        match self.db.write_policy() {
            crate::WritePolicy::MergeConcurrent => Ok(None),
            crate::WritePolicy::Serial => {
                self.storage
                    .serial_coordination()
                    .map(Some)
                    .map_err(|error| {
                        super::store_device_exclusion::StoreDeviceExclusionError::InvalidState(
                            format!("Serial coordination: {error}"),
                        )
                    })
            }
        }
    }

    async fn membership_operation_context(
        &self,
    ) -> Result<MembershipOperationContext<'_>, super::membership_ops::MembershipOpsError> {
        let encryption = self
            .current_encryption()
            .ok_or(super::membership_ops::MembershipOpsError::NotEncryptedHome)?;
        let serial = match self.db.write_policy() {
            crate::WritePolicy::MergeConcurrent => None,
            crate::WritePolicy::Serial => {
                let coordination = self.storage.serial_coordination().map_err(|error| {
                    super::membership_ops::MembershipOpsError::Serial(
                        super::store_outbound::StoreOutboundError::Coordination(error),
                    )
                })?;
                let device_id = self
                    .db
                    .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                    .await
                    .map_err(|error| {
                        super::membership_ops::MembershipOpsError::Database(error.to_string())
                    })?
                    .ok_or(super::store_outbound::StoreOutboundError::MissingState {
                        key: crate::database::LOCAL_DEVICE_ID_STATE_KEY,
                    })?;
                Some(super::membership_ops::SerialMembershipContext {
                    coordination,
                    device_id,
                })
            }
        };
        Ok(MembershipOperationContext { encryption, serial })
    }

    #[doc(hidden)]
    pub fn database(&self) -> &Database {
        &self.db
    }

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
            &*self.storage,
            store_dir,
            clock,
            &self.hlc,
            self.routing_encryption.as_ref(),
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
        let context = self.membership_operation_context().await?;
        super::membership_ops::invite_member_with_coordination(
            &*self.storage,
            self.storage.cloud_home(),
            &self.user_keypair,
            &self.hlc,
            public_key_hex,
            invitee_email,
            role,
            &context.encryption,
            &self.store_id,
            store_name,
            &self.db,
            context.serial,
        )
        .await
    }

    pub async fn remove_member(
        &self,
        public_key_hex: &str,
        custody: &dyn MasterKeyCustody,
    ) -> Result<String, super::membership_ops::MembershipOpsError> {
        let context = self.membership_operation_context().await?;
        super::membership_ops::remove_member_with_coordination(
            &*self.storage,
            self.storage.cloud_home(),
            &self.user_keypair,
            &self.hlc,
            public_key_hex,
            &self.store_id,
            &context.encryption,
            custody,
            &self.cipher,
            &self.pending_rotation,
            &self.db,
            context.serial,
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

    fn circle_coordination(
        &self,
    ) -> Result<
        Option<&dyn super::storage::CoordinationStorage>,
        super::circle_ops::CircleOperationError,
    > {
        if matches!(self.blob_path_scheme(), BlobPathScheme::Plain) {
            return Err(super::circle_ops::CircleOperationError::BrowsableStorage);
        }
        match self.db.write_policy() {
            crate::WritePolicy::MergeConcurrent => Ok(None),
            crate::WritePolicy::Serial => {
                self.storage
                    .serial_coordination()
                    .map(Some)
                    .map_err(|error| {
                        super::circle_ops::CircleOperationError::InvalidState(format!(
                            "Serial coordination: {error}"
                        ))
                    })
            }
        }
    }

    pub async fn create_circle(
        &self,
        name: &str,
    ) -> Result<super::circle::CircleId, super::circle_ops::CircleOperationError> {
        let coordination = self.circle_coordination()?;
        super::circle_ops::create_circle(
            &self.db,
            &*self.storage,
            coordination,
            &self.device_id,
            &self.hlc.now().to_string(),
            name,
            &self.user_keypair,
        )
        .await
    }

    pub async fn rename_circle(
        &self,
        circle_id: super::circle::CircleId,
        name: &str,
    ) -> Result<(), super::circle_ops::CircleOperationError> {
        let coordination = self.circle_coordination()?;
        super::circle_ops::rename_circle(
            &self.db,
            &*self.storage,
            coordination,
            &self.device_id,
            &self.hlc.now().to_string(),
            circle_id,
            name,
            &self.user_keypair,
        )
        .await
    }

    pub async fn propose_device_exclusion(
        &self,
        target: &super::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        let coordination = self.device_exclusion_coordination()?;
        super::store_device_exclusion::propose_device_exclusion(
            &self.db,
            &*self.storage,
            coordination,
            &self.user_keypair,
            target,
        )
        .await
    }

    pub async fn cancel_device_exclusion(
        &self,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        let coordination = self.device_exclusion_coordination()?;
        super::store_device_exclusion::cancel_device_exclusion(
            &self.db,
            &*self.storage,
            coordination,
            &self.user_keypair,
            proposal,
        )
        .await
    }

    pub async fn finalize_device_exclusion(
        &self,
        proposal: &super::store_commit::StoreDeviceExclusionProposalRef,
    ) -> Result<
        super::store_device_exclusion::StoreDeviceExclusionResult,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        let coordination = self.device_exclusion_coordination()?;
        super::store_device_exclusion::finalize_device_exclusion(
            &self.db,
            &*self.storage,
            coordination,
            &self.user_keypair,
            proposal,
        )
        .await
    }

    pub async fn get_device_exclusion_operations(
        &self,
    ) -> Result<
        Vec<super::store_device_exclusion::StoreDeviceExclusionOperationInfo>,
        super::store_device_exclusion::StoreDeviceExclusionError,
    > {
        super::store_device_exclusion::get_device_exclusion_operations(&self.db).await
    }

    pub async fn run_cycle(
        &self,
        clock: &dyn crate::clock::Clock,
        custody: Option<&dyn MasterKeyCustody>,
        store_dir: &StoreDir,
        observer: Option<&dyn BlobTransitionObserver>,
    ) -> Result<SyncCycleResult, SyncCycleFailure> {
        let serial_coordination = match self.db.write_policy() {
            crate::WritePolicy::MergeConcurrent => None,
            crate::WritePolicy::Serial => {
                Some(self.storage.serial_coordination().map_err(|error| {
                    SyncCycleFailure::operation("load Serial coordination", error)
                })?)
            }
        };
        let engine = CycleEngine::load(&*self.storage, serial_coordination, &self.db).await?;
        engine.resume_operations(&self.user_keypair).await?;
        let authorization = engine.authorize().await?;
        run_single_sync_cycle_with_authorization(
            &self.device_id,
            &self.hlc,
            clock,
            &self.cipher,
            &self.pending_rotation,
            &self.user_keypair,
            custody,
            self.routing_encryption.as_ref(),
            store_dir,
            Some(self.storage.cloud_home()),
            observer,
            authorization,
        )
        .await
    }
}
