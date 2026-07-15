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
use crate::database::{Database, DbError};
use crate::keys::{MasterKeyCustody, UserKeypair};
use crate::storage::cloud::CloudHome;
use crate::store_dir::StoreDir;

use super::cloud_storage::{
    BlobPathScheme, CloudCipherAccess, CloudCipherState, CloudSyncStorage, PendingRotation,
    RotationPending,
};
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
                   AND NOT EXISTS (\
                       SELECT 1 FROM store_write_blob_leases lease \
                       WHERE lease.namespace = published_blob_drop_intents.namespace \
                         AND lease.blob_id = published_blob_drop_intents.blob_id\
                   ) \
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
            drop.disposition.as_db(),
        ],
    )
    .map(|_| ())
    .map_err(DbError::from)
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
            return Err(DbError(
                "snapshot cut refused while unpublished Store writes exist".to_string(),
            ));
        }
        let snapshot = super::snapshot::create_snapshot_with_host_blobs(conn, &temp_dir, &tables)
            .map_err(|e| DbError(e.to_string()))?;
        let coverage = super::store_commit::CommitFrontier::from_positions(
            write_policy,
            Database::materialized_frontier_on(conn, None)?,
        )
        .map_err(|error| DbError(format!("snapshot coverage: {error}")))?;
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
#[cfg(any(test, feature = "test-utils"))]
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
) -> Result<SyncCycleResult, SyncCycleFailure> {
    run_single_sync_cycle_with_coordination(
        storage,
        None,
        store_id,
        device_id,
        hlc,
        clock,
        db,
        cipher,
        pending_rotation,
        user_keypair,
        custody,
        None,
        store_dir,
        cloud_home,
        observer,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_single_sync_cycle_with_coordination(
    storage: &dyn SyncStorage,
    serial_coordination: Option<&dyn super::storage::CoordinationStorage>,
    store_id: &str,
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
    // The synced-table set is owned by the Database; read it once here.
    let tables = db.synced_tables();

    let store_root_hash = db
        .get_protocol_state(crate::database::STORE_ROOT_HASH_STATE_KEY)
        .await
        .map_err(|error| format!("read store protocol root hash: {error}"))?
        .ok_or_else(|| "store protocol root hash is absent".to_string())?
        .parse()
        .map_err(|error| format!("store protocol root hash is invalid: {error}"))?;

    // Resolve the policy's authorization state before this cycle pushes, judges,
    // or decrypts. MergeConcurrent anchors its causal membership chain once for
    // the cycle. Serial verifies and replays the exact chain selected by the
    // compare-and-swap head. A missing or unverifiable state aborts the cycle;
    // authorization never falls open to "no rules apply".
    let (merge_membership, serial_authorization) = match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => (
            Some(
                super::pull::load_cycle_membership(storage, db)
                    .await
                    .map_err(|error| SyncCycleFailure::operation("load membership chain", error))?,
            ),
            None,
        ),
        crate::WritePolicy::Serial => (
            None,
            Some(
                super::store_pull::load_serial_cycle_authorization(
                    storage,
                    serial_coordination
                        .ok_or_else(|| "Serial coordination capability is absent".to_string())?,
                    store_root_hash,
                )
                .await
                .map_err(|error| SyncCycleFailure::operation("load Serial authorization", error))?,
            ),
        ),
    };
    let merge_chain = merge_membership
        .as_ref()
        .and_then(|membership| membership.chain.as_ref());

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
        let (current_owners, visible_activations) =
            match (merge_membership.as_ref(), serial_authorization.as_ref()) {
                (Some(membership), None) => {
                    let chain = match (
                        membership.pinned_owner.as_deref(),
                        membership.chain.as_ref(),
                    ) {
                        (Some(_), Some(chain)) => chain,
                        (None, _) => {
                            return Err("MergeConcurrent cycle has no pinned membership founder"
                                .to_string()
                                .into())
                        }
                        (Some(owner), None) => {
                            return Err(format!(
                                "owner {owner} is pinned but the cycle has no membership chain"
                            )
                            .into())
                        }
                    };
                    let owners: Vec<String> = chain
                        .current_members()
                        .into_iter()
                        .filter_map(|(pubkey, role)| {
                            (role == super::membership::MemberRole::Owner).then_some(pubkey)
                        })
                        .collect();
                    let visible = membership
                        .listed_entries
                        .iter()
                        .cloned()
                        .map(super::wrapped_store_key::WrappedKeyActivation::MergeConcurrent)
                        .collect();
                    (owners, visible)
                }
                (None, Some(serial)) => {
                    let owners: Vec<String> = serial
                        .authorization
                        .membership
                        .current_members()
                        .into_iter()
                        .filter_map(|(pubkey, role)| {
                            (role == super::membership::MemberRole::Owner).then_some(pubkey)
                        })
                        .collect();
                    (owners, serial.visible_activations.clone())
                }
                _ => {
                    return Err("cycle has no single policy-shaped membership state"
                        .to_string()
                        .into())
                }
            };
        refresh_authorization_state(
            ch,
            cipher,
            pending_rotation,
            db,
            user_keypair,
            custody,
            store_id,
            &current_owners,
            &visible_activations,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("refresh authorization state", error))?;
    }

    // Whether this device has adopted everything the store has committed. Read
    // once, right after the refresh that is the one place this cycle could adopt
    // a rotation, and used below to skip every write that would otherwise seal
    // new data under a generation the store has already superseded: the blob
    // upload drain, the host-provided make_remote completion, the inline
    // host-provided blob upload inside `service::sync`, the tombstone write
    // drain, both changeset-push paths, and the snapshot. Pull, local writes, and
    // delete-only paths (tombstone GC and cancel-drain) are unaffected — the gate
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

    let local_seq = db
        .latest_outbound_store_position()
        .await
        .map_err(|error| format!("read local Store position: {error}"))?
        .map_or(0, |position| position.seq);
    let last_snapshot_time: Option<chrono::DateTime<chrono::Utc>> =
        read_protocol_state::<chrono::DateTime<chrono::FixedOffset>>(db, "last_snapshot_time")
            .await?
            .map(|time| time.with_timezone(&chrono::Utc));
    let last_snapshot_position: Option<super::store_commit::CommitPosition> = db
        .get_protocol_state(crate::database::LAST_SNAPSHOT_POSITION_STATE_KEY)
        .await
        .map_err(|error| format!("read last snapshot position: {error}"))?
        .map(|raw| {
            serde_json::from_str(&raw)
                .map_err(|error| format!("last snapshot position is invalid: {error}"))
        })
        .transpose()?;
    let has_snapshot = db
        .get_protocol_state(crate::database::LAST_SNAPSHOT_HASH_STATE_KEY)
        .await
        .map_err(|error| format!("read last snapshot hash: {error}"))?
        .is_some();
    drain_published_blob_drop_intents(db, store_dir, local_seq).await?;

    // One wall-clock reading for this whole cycle. Store acknowledgements and
    // the status built at the end record the same instant. Store write commits
    // carry a separate HLC stamp (`timestamp` below) for causal ordering.
    let sync_time = clock.now().to_rfc3339();

    // The cloud handle + object-key suffix the inline host-provided upload path uses to
    // cancel a pending tombstone after a (re-)upload — the same invariant the outbox
    // drain holds. `None` on a cloud-less run, which has no tombstones to cancel.
    let blob_object_suffix = cipher.snapshot().suffix();
    let host_upload_cancel = cloud_home.map(|ch| super::service::HostUploadCloud {
        cloud_home: ch,
        suffix: blob_object_suffix,
        now_rfc: &sync_time,
    });

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
                routing_encryption,
                observer,
            )
            .await
            {
                Ok(outcome) => {
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
                Err(e) => warn!("Blob upload drain error: {e}"),
            }
        }

        // Retry any tombstone-cancel an upload's inline cancel could not complete.
        // Runs right after the upload drain (and before the tombstone GC below), so
        // a blob re-uploaded this cycle has its tombstone removed before the GC
        // could reclaim it. A cancel that still fails stays queued for the next
        // cycle, backed off like the delete drain — the live re-uploaded blob must
        // never lose its tombstone-cancel.
        match crate::blob::delete::drain_tombstone_cancels(db, ch, cipher, clock).await {
            Ok(n) if n > 0 => info!(count = n, "Completed pending tombstone cancels"),
            Err(e) => warn!("Tombstone cancel drain error: {e}"),
            _ => {}
        }
    }

    if rotation_pending.is_none() {
        let published = super::store_outbound::drain_store_writes_with_coordination(
            db,
            storage,
            serial_coordination,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("publish queued Store writes", error))?;
        if published > 0 {
            info!(published, "Published queued Store writes");
        }
    }

    let serial_head_after_publish = match serial_authorization.as_ref() {
        Some(_) => Some(
            super::store_pull::load_serial_cycle_authorization(
                storage,
                serial_coordination
                    .ok_or_else(|| "Serial coordination capability is absent".to_string())?,
                store_root_hash,
            )
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("reload Serial authorization after publication", error)
            })?
            .head,
        ),
        None => None,
    };

    if let (Some(authoritative_head), Some(branch)) = (
        serial_head_after_publish,
        db.unresolved_serial_branch()
            .await
            .map_err(|error| format!("read unresolved Serial branch: {error}"))?,
    ) {
        let stale = branch.base != authoritative_head;
        if !branch.conflicted && stale {
            db.mark_serial_branch_conflict(branch.branch_id, branch.base, authoritative_head)
                .await
                .map_err(|error| format!("record Serial branch conflict: {error}"))?;
        }
        if branch.conflicted || stale {
            return Ok(SyncCycleResult {
                changesets_applied: 0,
                held_positions: Vec::new(),
                device_activity: Vec::new(),
                sync_time,
                asset_downloads_failed: false,
                local_blob_cleanup_pending: false,
                row_changes: Vec::new(),
                resume_drain_promptly: false,
                rotation_pending,
            });
        }
    }

    let timestamp = hlc.now().to_string();
    let local_seq_before_stage = db
        .latest_outbound_store_position()
        .await
        .map_err(|error| format!("read local Store position: {error}"))?
        .map_or(0, |position| position.seq);

    if rotation_pending.is_some() {
        debug!(
            "rotation pending; leaving ready host-provided make_remote intents queued until adoption"
        );
    } else if super::service::complete_host_provided_make_remotes(
        db,
        tables,
        storage,
        &timestamp,
        store_dir,
        local_seq_before_stage,
        routing_encryption,
        host_upload_cancel.as_ref(),
    )
    .await
    .map_err(|error| SyncCycleFailure::operation("complete host-provided make_remote", error))?
    {
        resume_drain_promptly = true;
    }

    let store_pull = super::store_pull::pull_store_commits_with_coordination(
        db,
        tables,
        storage,
        serial_coordination,
        store_root_hash,
        device_id,
        store_dir,
        merge_chain,
    )
    .await
    .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))?;
    let serial_membership_after_pull = match db.write_policy() {
        crate::WritePolicy::MergeConcurrent => None,
        crate::WritePolicy::Serial => Some(
            db.serial_membership_state()
                .await
                .map_err(|error| format!("read materialized Serial membership: {error}"))?
                .ok_or_else(|| "materialized Serial membership is absent".to_string())?,
        ),
    };

    // A Serial registration appends to the exact global predecessor. Materialize
    // the selected global prefix first so the registration commit and its durable
    // predecessor enter one continuous local chain. The same pull also installs
    // the membership state that decides whether this active member may register.
    if rotation_pending.is_none() {
        super::store_registration::ensure_active_registration_with_coordination(
            db,
            storage,
            serial_coordination,
            user_keypair,
            merge_chain,
            &sync_time,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("publish Store device registration", error))?;
    }

    let staged_store_batch = if rotation_pending.is_none() {
        let mut staged_any = false;
        loop {
            let staged = super::store_outbound::prepare_pending_store_write_with_coordination(
                db,
                storage,
                serial_coordination,
                device_id,
                &sync_time,
                user_keypair,
                store_dir,
                merge_chain,
                host_upload_cancel.as_ref(),
            )
            .await
            .map_err(|error| SyncCycleFailure::operation("prepare Store write", error))?;
            if !staged {
                break;
            }
            staged_any = true;
            let published = super::store_outbound::drain_store_writes_with_coordination(
                db,
                storage,
                serial_coordination,
            )
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
        .latest_outbound_store_position()
        .await
        .map_err(|error| format!("read local Store position after publish: {error}"))?
        .map_or(0, |position| position.seq);
    drain_published_blob_drop_intents(db, store_dir, local_seq).await?;

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
        // A still-pending tombstone-cancel means a blob was re-uploaded this
        // cycle (or earlier) but its cancel couldn't reach the cloud, so the
        // tombstone is still present though the blob is live. Reclaiming now would
        // delete that re-upload, so skip the GC entirely while any cancel is
        // pending — the initiating loop must retry the durable cancels before
        // reclaiming.
        let cancels_pending = match db.get_pending_cloud_cancels().await {
            Ok(cancels) => !cancels.is_empty(),
            Err(e) => {
                // Can't confirm the cancel queue is clear — don't risk reclaiming.
                warn!("Tombstone GC skipped: failed to read pending cancels: {e}");
                true
            }
        };
        if cancels_pending {
            debug!("tombstone cancels still pending; skipping reclaim this cycle");
        } else {
            // Authorize every reclaim against the policy state materialized by this
            // cycle: the founder-anchored MergeConcurrent chain, or the Serial
            // membership produced by the exact pulled global prefix.
            match crate::blob::delete::gc_tombstones(
                db,
                ch,
                cipher,
                store_id,
                &hex::encode(user_keypair.public_key()),
                merge_chain,
                serial_membership_after_pull.as_ref(),
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
    }

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
    let resumed_snapshot = super::store_snapshot::drain_outbound_store_snapshot(storage, db)
        .await
        .map_err(|error| SyncCycleFailure::operation("publish pending Store snapshot", error))?
        .is_some();
    let snapshot_due = !resumed_snapshot
        && (is_initial_sync
            || super::snapshot::should_create_snapshot(
                local_seq,
                if has_snapshot {
                    Some(
                        last_snapshot_position
                            .as_ref()
                            .map_or(0, |position| position.seq),
                    )
                } else {
                    None
                },
                hours_since,
            ));
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
        let authorized = match (
            merge_membership.as_ref(),
            serial_membership_after_pull.as_ref(),
        ) {
            (Some(_), None) => super::membership_ops::authorize_loaded_membership_author(
                merge_chain,
                &our_pk,
                super::membership_ops::MembershipAuthorRequirement::Owner,
            )
            .map_err(|error| error.to_string()),
            (None, Some(serial)) => serial
                .is_owner(&our_pk)
                .then_some(())
                .ok_or_else(|| "not a current Serial Owner".to_string()),
            _ => Err("cycle has no single policy-shaped membership state".to_string()),
        };
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
                super::service::upload_snapshot_host_blobs(
                    db,
                    storage,
                    store_dir,
                    &cut.snapshot.host_blobs,
                    host_upload_cancel.as_ref(),
                )
                .await
                .map_err(|error| {
                    SyncCycleFailure::operation("upload snapshot host-provided blobs", error)
                })?;

                let meta = super::store_snapshot::push_store_snapshot(
                    storage,
                    store_root_hash,
                    cut.snapshot,
                    cut.coverage,
                    db.schema_version(),
                    user_keypair,
                    sync_time.clone(),
                    merge_chain,
                    db,
                )
                .await
                .map_err(|error| SyncCycleFailure::operation("publish Store snapshot", error))?;
                info!(local_seq, snapshot = %meta.snapshot_hash(), "Snapshot created and pushed");
            }
            Err(e) => warn!("Failed to create snapshot: {e}"),
        }
    }

    if rotation_pending.is_none() {
        super::store_ack::drain_outbound_store_acks(db, storage)
            .await
            .map_err(|error| {
                SyncCycleFailure::operation("publish queued Store acknowledgement", error)
            })?;
        let frontier = db
            .materialized_frontier()
            .await
            .map_err(|error| format!("read Store acknowledgement frontier: {error}"))?;
        let frontier =
            super::store_commit::CommitFrontier::from_positions(db.write_policy(), frontier)
                .map_err(|error| format!("shape Store acknowledgement frontier: {error}"))?;
        super::store_ack::stage_store_ack(db, frontier, sync_time.clone(), user_keypair)
            .await
            .map_err(|error| format!("stage Store acknowledgement: {error}"))?;
        super::store_ack::drain_outbound_store_acks(db, storage)
            .await
            .map_err(|error| SyncCycleFailure::operation("publish Store acknowledgement", error))?;
        let reclaim_membership = match (
            merge_membership.as_ref(),
            serial_membership_after_pull.as_ref(),
        ) {
            (Some(membership), None) => super::store_reclaim::ReclaimMembership::MergeConcurrent {
                membership: membership
                    .chain
                    .as_ref()
                    .ok_or_else(|| "MergeConcurrent cycle has no membership chain".to_string())?,
                listing_proof: membership.listing_proof,
            },
            (None, Some(serial)) => super::store_reclaim::ReclaimMembership::Serial(serial),
            _ => {
                return Err("cycle has no single policy-shaped membership state"
                    .to_string()
                    .into())
            }
        };
        match super::store_reclaim::reclaim_store_packages(
            db,
            storage,
            store_root_hash,
            reclaim_membership,
        )
        .await
        {
            Ok(result) if result.packages_deleted > 0 => info!(
                packages = result.packages_deleted,
                copies = result.physical_copies_deleted,
                "Reclaimed snapshot-covered Store packages"
            ),
            Ok(_) => {}
            Err(error) => warn!(%error, "Store package reclamation refused"),
        }
    }

    // Build status from remote heads. Reuse this cycle's `sync_time` so the
    // status's `last_sync_time` matches the head this cycle wrote.
    let core_status =
        super::status::build_sync_status(&store_pull.visible_heads, device_id, Some(&sync_time));
    Ok(SyncCycleResult {
        changesets_applied: store_pull.changesets_applied,
        held_positions: store_pull.held_positions,
        device_activity: core_status.other_devices,
        sync_time,
        asset_downloads_failed: store_pull.asset_downloads_failed,
        local_blob_cleanup_pending: store_pull.local_blob_cleanup_pending,
        row_changes: store_pull.row_changes,
        resume_drain_promptly,
        rotation_pending,
    })
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
/// can't be resolved at all (an invisible activation, a read failure) still
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
    cloud_home: &dyn CloudHome,
    cipher: &dyn CloudCipherAccess,
    pending_rotation: &PendingRotation,
    db: &Database,
    user_keypair: &UserKeypair,
    custody: Option<&dyn MasterKeyCustody>,
    store_id: &str,
    current_owners: &[String],
    visible_activations: &[super::wrapped_store_key::WrappedKeyActivation],
) -> Result<(), AuthorizationRefreshError> {
    // A plaintext home has no encrypted store key to rotate. Its membership
    // chain remains load-bearing elsewhere in the cycle.
    if cipher.snapshot().is_plaintext() {
        debug!("refresh: plaintext home, nothing to refresh");
        return Ok(());
    }

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
            return Err(AuthorizationRefreshError::InvalidState(
                "plaintext home cannot enter encrypted key refresh".to_string(),
            ))
        }
    };
    match super::invite::unwrap_store_keyring_for_owners_with_activation(
        cloud_home,
        user_keypair,
        store_id,
        current_owners.iter().map(String::as_str),
        Some(visible_activations),
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
                activation = ?activation,
                "refresh: a rotated wrapped store key's activation entry is not yet \
                 visible; sealing is paused until it is and this device adopts"
            );
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
        expected_store_root_hash: super::store_commit::ObjectHash,
        expected_founder: String,
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
    .map_err(|error| InitSyncError::RowRouting(error.0))?;

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
                &store_id,
                &hlc.now().to_string(),
                &user_keypair,
            )
            .await
        }
        StoreInitialization::OpenStore {
            expected_store_root_hash,
            expected_founder,
        } => {
            super::store_protocol_root::open_store(
                db,
                &storage,
                expected_store_root_hash,
                &store_id,
                &expected_founder,
            )
            .await
        }
    }
    .map_err(|error| InitSyncError::StoreProtocolRoot(error.to_string()))?;
    match store_protocol_root.write_policy {
        crate::WritePolicy::MergeConcurrent => {
            ensure_owner_anchored_chain(&storage, db, &store_protocol_root, &user_keypair)
                .await
                .map_err(InitSyncError::MembershipAnchor)?;
        }
        crate::WritePolicy::Serial => {
            ensure_serial_founder_authorization(db, &store_protocol_root)
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
        routing_encryption,
    })
}

async fn ensure_serial_founder_authorization(
    db: &Database,
    root: &super::store_commit::StoreProtocolRoot,
) -> Result<(), String> {
    let pinned = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|error| format!("read pinned Serial founder: {error}"))?;
    if pinned
        .as_ref()
        .is_some_and(|founder| founder != &root.author_pubkey)
    {
        return Err(format!(
            "pinned Serial founder {:?} does not match Store root founder {:?}",
            pinned.as_deref(),
            root.author_pubkey
        ));
    }
    let membership = db
        .serial_membership_state()
        .await
        .map_err(|error| format!("read Serial membership state: {error}"))?;
    let generation = db
        .serial_key_generation()
        .await
        .map_err(|error| format!("read Serial key generation: {error}"))?;
    match (pinned, membership, generation) {
        (Some(_), Some(membership), Some(_)) => {
            if membership.store_root_hash() != root.object_hash() {
                return Err("Serial membership state belongs to another Store root".to_string());
            }
            Ok(())
        }
        (None, None, None) => {
            let authorization = super::membership::SerialAuthorizationState::from_founder(
                root.object_hash(),
                &root.founder,
            )
            .map_err(|error| error.to_string())?;
            db.install_serial_root_authorization(root.author_pubkey.clone(), authorization)
                .await
                .map_err(|error| error.to_string())
        }
        _ => Err(
            "Serial founder pin, membership, and key generation are only valid together"
                .to_string(),
        ),
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
    store_protocol_root: &super::store_commit::StoreProtocolRoot,
    owner_keypair: &UserKeypair,
) -> Result<(), String> {
    use super::membership_ops::OWNER_PUBKEY_STATE_KEY;

    let our_pk = hex::encode(owner_keypair.public_key());
    let pinned = db
        .get_protocol_state(OWNER_PUBKEY_STATE_KEY)
        .await
        .map_err(|e| format!("read pinned owner: {e}"))?;
    let store_root_hash = store_protocol_root.object_hash();
    let entries = super::membership_ops::list_membership_entries(storage, store_root_hash)
        .await
        .map_err(|e| format!("list membership entries: {e}"))?;
    if pinned
        .as_deref()
        .is_some_and(|owner| owner != store_protocol_root.author_pubkey)
    {
        return Err(format!(
            "pinned owner {:?} does not match store protocol root founder {:?}",
            pinned.as_deref(),
            store_protocol_root.author_pubkey
        ));
    }
    let expected_owner = &store_protocol_root.author_pubkey;
    let loaded = super::membership_ops::load_and_persist_owner_anchor(
        storage,
        store_root_hash,
        &entries,
        expected_owner,
        db,
    )
    .await
    .map_err(|error| error.to_string())?;
    if let Some(chain) = loaded {
        if chain.entries().first() != Some(&store_protocol_root.founder) {
            return Err("membership founder does not match store protocol root".to_string());
        }
        return Ok(());
    }
    if let Some(pinned) = pinned {
        return Err(format!(
            "membership chain has no committed heads but owner {pinned} is pinned \
             — refusing (wiped or tampered membership/*)"
        ));
    }

    if our_pk != store_protocol_root.author_pubkey {
        return Err(format!(
            "membership root is absent and local identity {our_pk} cannot republish founder {}",
            store_protocol_root.author_pubkey
        ));
    }

    publish_or_complete_founder(
        storage,
        store_root_hash,
        &store_protocol_root.founder,
        owner_keypair,
    )
    .await?;
    let committed_entries =
        super::membership_ops::list_membership_entries(storage, store_root_hash)
            .await
            .map_err(|e| format!("list membership entries after founder publish: {e}"))?;
    super::membership_ops::load_and_persist_owner_anchor(
        storage,
        store_root_hash,
        &committed_entries,
        &our_pk,
        db,
    )
    .await
    .map_err(|error| error.to_string())?
    .ok_or_else(|| "founder publish produced no signed committed membership head".to_string())?;
    info!(owner = %our_pk, "Founded store: wrote owner-anchored founder entry");
    Ok(())
}

async fn publish_or_complete_founder(
    storage: &dyn SyncStorage,
    store_root_hash: super::store_commit::ObjectHash,
    store_protocol_root_founder: &super::membership::MembershipEntry,
    owner_keypair: &UserKeypair,
) -> Result<(), String> {
    use super::membership::MembershipChain;
    use super::store_objects::{append_membership_entry_object, load_membership_entry_slot};

    let owner_pubkey = hex::encode(owner_keypair.public_key());
    let founder_coord = store_protocol_root_founder.coord();
    match load_membership_entry_slot(
        storage,
        store_root_hash,
        &owner_pubkey,
        &founder_coord.author_owner_grant,
        1,
    )
    .await
    {
        Ok(Some(verified)) => {
            let bytes = verified.bytes;
            let entry = verified.value;
            let coord = entry.coord();
            let entry = super::membership_ops::parse_membership_entry_at(&coord, &bytes)?;
            let chain =
                MembershipChain::from_entries_with_coords(vec![(coord.clone(), entry.clone())])
                    .map_err(|error| format!("invalid interrupted founder entry: {error}"))?;
            if !chain.is_founded_by(&owner_pubkey) {
                return Err(
                    "interrupted founder entry is not this storage identity's founder".to_string(),
                );
            }
            if chain.entries().first() != Some(store_protocol_root_founder) {
                return Err(
                    "interrupted founder entry does not match store protocol root".to_string(),
                );
            }
            append_membership_entry_object(storage, store_root_hash, &coord, &entry)
                .await
                .map_err(|error| format!("re-publish interrupted founder entry: {error}"))?;
            super::membership_ops::publish_membership_head(
                storage,
                store_root_hash,
                &chain,
                owner_keypair,
            )
            .await
            .map_err(|error| format!("publish interrupted founder head: {error}"))?;
            Ok(())
        }
        Ok(None) => {
            let coord = founder_coord;
            append_membership_entry_object(
                storage,
                store_root_hash,
                &coord,
                store_protocol_root_founder,
            )
            .await
            .map_err(|error| format!("publish store protocol root founder entry: {error}"))?;
            let chain = MembershipChain::from_entries_with_coords(vec![(
                coord,
                store_protocol_root_founder.clone(),
            )])
            .map_err(|error| format!("store protocol root founder is invalid: {error}"))?;
            super::membership_ops::publish_membership_head(
                storage,
                store_root_hash,
                &chain,
                owner_keypair,
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("publish store protocol root founder head: {error}"))
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
    routing_encryption: Option<crate::encryption::EncryptionService>,
}

struct MembershipOperationContext<'a> {
    encryption: crate::encryption::EncryptionService,
    serial: Option<super::membership_ops::SerialMembershipContext<'a>>,
}

impl SyncComponents {
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
            self.storage.cloud_home(),
            &self.cipher,
            &self.pending_rotation,
            &self.store_id,
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
        run_single_sync_cycle_with_coordination(
            &*self.storage,
            serial_coordination,
            &self.store_id,
            &self.device_id,
            &self.hlc,
            clock,
            &self.db,
            &self.cipher,
            &self.pending_rotation,
            &self.user_keypair,
            custody,
            self.routing_encryption.as_ref(),
            store_dir,
            Some(self.storage.cloud_home()),
            observer,
        )
        .await
    }
}
