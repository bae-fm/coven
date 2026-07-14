//! Causal discovery and atomic materialization for immutable Store commits.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use super::apply::{resolve_and_apply_changeset_with_schema_on, ValidatedChangeset};
use super::conflict::TableSchema;
use super::membership::MembershipChain;
use super::pull::{
    advance_max_updated_at, cache_eager_blobs, download_blobs, introduced_blob_uploads,
    local_blob_cleanup_intents,
};
use super::session::SyncedTable;
use super::storage::SyncStorage;
use super::store_commit::{
    CommitPosition, ObjectHash, StoreBatchCommit, StoreDeviceHead, StoreProtocolError,
};
use super::store_objects::{list_visible_heads, load_commit_slot, load_package, StoreObjectError};
use crate::blob::local_cleanup::{self, LocalBlobCleanupIntent};
use crate::changeset::RowChange;
use crate::database::{Database, DbError};
use crate::store_dir::StoreDir;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStorePositionReason {
    MissingCommit,
    MissingPackage,
    MissingPredecessor(CommitPosition),
    MissingDependency {
        device_id: String,
        position: CommitPosition,
    },
    NewerSchema {
        local: u32,
        required: u32,
    },
    Unauthorized,
    InvalidChangeset(String),
    InvalidRowIdentity {
        table: String,
        reason: String,
    },
    BlobDownloadFailed,
    ForeignKeyDependency,
    ConstraintConflict(Vec<String>),
    HashMismatch {
        referenced_device_id: String,
        referenced_position: CommitPosition,
        materialized_hash: ObjectHash,
    },
    InvalidSignature,
    WrongSlot(String),
    ObjectCollision(String),
    ObjectUnreadable {
        key: String,
        detail: String,
    },
    InvalidObject(String),
    HeadAuthorMismatch {
        head_author: String,
        commit_author: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HeldStoreCoordinate {
    Head {
        device_id: String,
        seq: u64,
        head_hash: ObjectHash,
    },
    Commit {
        device_id: String,
        position: CommitPosition,
    },
    Package {
        device_id: String,
        seq: u64,
        package_hash: ObjectHash,
    },
    Dependency {
        dependent_device_id: String,
        dependent_position: CommitPosition,
        required_device_id: String,
        required_position: CommitPosition,
    },
}

impl HeldStoreCoordinate {
    pub fn device_id(&self) -> &str {
        match self {
            Self::Head { device_id, .. }
            | Self::Commit { device_id, .. }
            | Self::Package { device_id, .. } => device_id,
            Self::Dependency {
                dependent_device_id,
                ..
            } => dependent_device_id,
        }
    }

    pub fn seq(&self) -> u64 {
        match self {
            Self::Head { seq, .. } | Self::Package { seq, .. } => *seq,
            Self::Commit { position, .. } => position.seq,
            Self::Dependency {
                dependent_position, ..
            } => dependent_position.seq,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldStorePosition {
    pub coordinate: HeldStoreCoordinate,
    pub reason: HeldStorePositionReason,
}

#[derive(Debug)]
pub struct StorePullResult {
    pub changesets_applied: u64,
    pub devices_pulled: u64,
    pub held_positions: Vec<HeldStorePosition>,
    pub visible_heads: Vec<StoreDeviceHead>,
    pub row_changes: Vec<RowChange>,
    pub asset_downloads_failed: bool,
    pub local_blob_cleanup_pending: bool,
    pub frontier: BTreeMap<String, CommitPosition>,
}

#[derive(Debug, thiserror::Error)]
pub enum StorePullError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("database: {0}")]
    Database(String),
    #[error("membership: {0}")]
    Membership(String),
}

impl From<DbError> for StorePullError {
    fn from(error: DbError) -> Self {
        Self::Database(error.0)
    }
}

#[derive(Clone)]
struct Candidate {
    commit: StoreBatchCommit,
    package: Vec<u8>,
}

enum ApplyOutcome {
    Applied(Vec<RowChange>),
    Held(HeldStorePositionReason),
}

/// Discover every visible immutable head, then repeatedly materialize any commit
/// whose exact predecessor and dependency positions are already durable.
#[allow(clippy::too_many_arguments)]
pub async fn pull_store_commits(
    db: &Database,
    tables: &[SyncedTable],
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    our_device_id: &str,
    store_dir: &StoreDir,
    membership: Option<&MembershipChain>,
) -> Result<StorePullResult, StorePullError> {
    let head_listing = list_visible_heads(storage, store_root_hash).await?;
    let mut first_failed_head_by_device = BTreeMap::new();
    for failure in head_listing.failures {
        let replace = first_failed_head_by_device
            .get(&failure.device_id)
            .is_none_or(|current: &super::store_objects::StoreHeadFailure| {
                failure.seq < current.seq
            });
        if replace {
            first_failed_head_by_device.insert(failure.device_id.clone(), failure);
        }
    }
    let mut latest_heads = BTreeMap::new();
    for verified in head_listing.heads {
        let device_id = verified.value.device_id.clone();
        if first_failed_head_by_device
            .get(&device_id)
            .is_some_and(|failure| verified.value.slot_sequence() >= failure.seq)
        {
            continue;
        }
        let replace = latest_heads.get(&device_id).is_none_or(
            |current: &super::store_objects::VerifiedCopies<StoreDeviceHead>| {
                verified.value.slot_sequence() > current.value.slot_sequence()
            },
        );
        if replace {
            latest_heads.insert(device_id, verified);
        }
    }
    let visible_heads: Vec<_> = latest_heads
        .values()
        .map(|verified| verified.value.clone())
        .collect();
    let mut held = Vec::new();
    for failure in first_failed_head_by_device.into_values() {
        let reason = held_object_error(failure.error);
        for head_hash in failure.semantic_hashes {
            held.push(HeldStorePosition {
                coordinate: HeldStoreCoordinate::Head {
                    device_id: failure.device_id.clone(),
                    seq: failure.seq,
                    head_hash,
                },
                reason: reason.clone(),
            });
        }
    }
    let mut candidates = BTreeMap::new();
    let coverage = db.snapshot_coverage_frontier().await?;

    for verified_head in latest_heads.into_values() {
        let head_hash = verified_head.semantic_hash;
        let head = verified_head.value;
        let Some(mut expected_position) = head.position.clone() else {
            continue;
        };
        if head.device_id == our_device_id {
            continue;
        }
        loop {
            match position_is_materialized(
                db,
                storage,
                store_root_hash,
                &coverage,
                &head.device_id,
                &expected_position,
            )
            .await?
            {
                MaterializedCheck::Yes => break,
                MaterializedCheck::Missing => {}
                MaterializedCheck::Held(reason) => {
                    held.push(held_commit(&head.device_id, expected_position, reason));
                    break;
                }
            }
            let verified_commit = match load_commit_slot(
                storage,
                store_root_hash,
                &head.device_id,
                expected_position.seq,
            )
            .await
            {
                Ok(Some(commit)) => commit,
                Ok(None) => {
                    held.push(held_commit(
                        &head.device_id,
                        expected_position,
                        HeldStorePositionReason::MissingCommit,
                    ));
                    break;
                }
                Err(error) => {
                    held.push(held_commit(
                        &head.device_id,
                        expected_position,
                        held_object_error(error),
                    ));
                    break;
                }
            };
            let commit = verified_commit.value;
            let commit_hash = commit.commit_hash();
            if commit_hash != expected_position.commit_hash {
                held.push(held_commit(
                    &head.device_id,
                    expected_position.clone(),
                    HeldStorePositionReason::HashMismatch {
                        referenced_device_id: head.device_id.clone(),
                        referenced_position: expected_position,
                        materialized_hash: commit_hash,
                    },
                ));
                break;
            }
            if head.author_pubkey != commit.author_pubkey {
                held.push(HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: head.device_id.clone(),
                        seq: head.slot_sequence(),
                        head_hash,
                    },
                    reason: HeldStorePositionReason::HeadAuthorMismatch {
                        head_author: head.author_pubkey,
                        commit_author: commit.author_pubkey,
                    },
                });
                break;
            }
            let predecessor = commit
                .previous_commit_hash
                .map(|commit_hash| CommitPosition {
                    seq: commit.seq - 1,
                    commit_hash,
                });
            if !membership_authorizes(db, storage, membership, &commit).await? {
                held.push(held_commit(
                    &commit.device_id,
                    commit.position(),
                    HeldStorePositionReason::Unauthorized,
                ));
                let Some(predecessor) = predecessor else {
                    break;
                };
                expected_position = predecessor;
                continue;
            }
            if commit.package.schema_version > db.schema_version() {
                held.push(held_commit(
                    &commit.device_id,
                    commit.position(),
                    HeldStorePositionReason::NewerSchema {
                        local: db.schema_version(),
                        required: commit.package.schema_version,
                    },
                ));
                let Some(predecessor) = predecessor else {
                    break;
                };
                expected_position = predecessor;
                continue;
            }
            let package = match load_package(storage, &commit).await {
                Ok(Some(package)) => package,
                Ok(None) => {
                    held.push(held_package(
                        &commit,
                        HeldStorePositionReason::MissingPackage,
                    ));
                    let Some(predecessor) = predecessor else {
                        break;
                    };
                    expected_position = predecessor;
                    continue;
                }
                Err(error) => {
                    held.push(held_package(&commit, held_object_error(error)));
                    let Some(predecessor) = predecessor else {
                        break;
                    };
                    expected_position = predecessor;
                    continue;
                }
            };
            candidates.insert(
                (commit.device_id.clone(), commit.seq),
                Candidate {
                    commit,
                    package: package.value,
                },
            );
            let Some(predecessor) = predecessor else {
                break;
            };
            expected_position = predecessor;
        }
    }

    let schema: Arc<TableSchema> = {
        let tables = tables.to_vec();
        Arc::new(
            db.call(move |conn| TableSchema::from_db(conn, &tables))
                .await?,
        )
    };
    let mut frontier = db.materialized_frontier().await?;
    let mut applied_devices = BTreeSet::new();
    let mut row_changes = Vec::new();
    let mut changesets_applied = 0_u64;
    let mut asset_downloads_failed = false;
    let mut blocked = BTreeMap::new();

    loop {
        let mut progressed = false;
        let keys: Vec<_> = candidates.keys().cloned().collect();
        for key in keys {
            let candidate = candidates
                .get(&key)
                .expect("candidate key came from the same map");
            match readiness(
                db,
                storage,
                store_root_hash,
                &coverage,
                &frontier,
                &candidate.commit,
            )
            .await?
            {
                Readiness::AlreadyMaterialized => {
                    candidates.remove(&key);
                    blocked.remove(&key);
                    progressed = true;
                }
                Readiness::Held(held_position) => {
                    blocked.insert(key, held_position);
                }
                Readiness::Ready => {
                    let candidate = candidates
                        .remove(&key)
                        .expect("ready candidate remains present");
                    match apply_candidate(db, storage, store_dir, schema.clone(), &candidate)
                        .await?
                    {
                        ApplyOutcome::Applied(changes) => {
                            frontier.insert(
                                candidate.commit.device_id.clone(),
                                candidate.commit.position(),
                            );
                            applied_devices.insert(candidate.commit.device_id);
                            row_changes.extend(changes);
                            changesets_applied =
                                changesets_applied.checked_add(1).ok_or_else(|| {
                                    StorePullError::Database(
                                        "Store apply count exceeded u64".to_string(),
                                    )
                                })?;
                            blocked.remove(&key);
                            progressed = true;
                        }
                        ApplyOutcome::Held(reason) => {
                            if matches!(reason, HeldStorePositionReason::BlobDownloadFailed) {
                                asset_downloads_failed = true;
                            }
                            candidates.insert(key.clone(), candidate);
                            let candidate = candidates
                                .get(&key)
                                .expect("held candidate was restored to the map");
                            blocked.insert(
                                key,
                                held_commit(
                                    &candidate.commit.device_id,
                                    candidate.commit.position(),
                                    reason,
                                ),
                            );
                        }
                    }
                }
            }
        }
        if !progressed {
            break;
        }
    }

    for (_, blocked_position) in blocked {
        held.push(blocked_position);
    }
    held.sort_by(|left, right| {
        (left.coordinate.device_id(), left.coordinate.seq())
            .cmp(&(right.coordinate.device_id(), right.coordinate.seq()))
    });
    let local_blob_cleanup_pending = local_cleanup::drain(db, store_dir).await?;

    Ok(StorePullResult {
        changesets_applied,
        devices_pulled: u64::try_from(applied_devices.len()).expect("device count fits in u64"),
        held_positions: held,
        visible_heads,
        row_changes,
        asset_downloads_failed,
        local_blob_cleanup_pending,
        frontier,
    })
}

async fn membership_authorizes(
    db: &Database,
    storage: &dyn SyncStorage,
    membership: Option<&MembershipChain>,
    commit: &StoreBatchCommit,
) -> Result<bool, StorePullError> {
    let Some(chain) = membership else {
        return Ok(true);
    };
    let Some(grant) = commit.membership_grant.as_ref() else {
        return Ok(false);
    };
    if chain.authorizes_write_at(grant, &commit.author_pubkey) {
        return Ok(true);
    }
    let owner = db
        .get_protocol_state(super::membership_ops::OWNER_PUBKEY_STATE_KEY)
        .await?;
    let entries = super::membership_ops::list_membership_entries(storage)
        .await
        .map_err(|error| StorePullError::Membership(error.to_string()))?;
    let refreshed = super::membership_ops::load_anchored_chain_with_candidates(
        storage,
        &entries,
        std::slice::from_ref(grant),
        owner.as_deref(),
        Some(db),
    )
    .await
    .map_err(|error| StorePullError::Membership(error.to_string()))?;
    Ok(refreshed
        .is_some_and(|refreshed| refreshed.authorizes_write_at(grant, &commit.author_pubkey)))
}

enum Readiness {
    Ready,
    AlreadyMaterialized,
    Held(HeldStorePosition),
}

enum MaterializedCheck {
    Yes,
    Missing,
    Held(HeldStorePositionReason),
}

fn held_object_error(error: StoreObjectError) -> HeldStorePositionReason {
    match error {
        StoreObjectError::CandidateUnreadable { key, source } => {
            HeldStorePositionReason::ObjectUnreadable {
                key,
                detail: source.to_string(),
            }
        }
        StoreObjectError::SemanticFork { slot, hashes } => {
            HeldStorePositionReason::ObjectCollision(format!(
                "semantic slot {slot} contains hashes {hashes:?}"
            ))
        }
        StoreObjectError::Collision {
            semantic_prefix,
            key,
            reason,
        } => HeldStorePositionReason::ObjectCollision(format!(
            "candidate {key} under {semantic_prefix}: {reason}"
        )),
        StoreObjectError::InvalidCandidate { source, .. } => match *source {
            StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
            StoreProtocolError::RelocatedSlot { .. }
            | StoreProtocolError::RelocatedPackage { .. }
            | StoreProtocolError::StoreRootMismatch { .. }
            | StoreProtocolError::StoreMismatch { .. }
            | StoreProtocolError::FounderMismatch { .. } => {
                HeldStorePositionReason::WrongSlot(source.to_string())
            }
            StoreProtocolError::ObjectHashMismatch { .. }
            | StoreProtocolError::PackageHashMismatch { .. } => {
                HeldStorePositionReason::InvalidObject(source.to_string())
            }
            source => HeldStorePositionReason::InvalidObject(source.to_string()),
        },
        error => HeldStorePositionReason::ObjectUnreadable {
            key: "Store object slot".to_string(),
            detail: error.to_string(),
        },
    }
}

async fn readiness(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    coverage: &BTreeMap<String, CommitPosition>,
    frontier: &BTreeMap<String, CommitPosition>,
    commit: &StoreBatchCommit,
) -> Result<Readiness, StorePullError> {
    if let Some(current) = frontier.get(&commit.device_id) {
        if commit.seq <= current.seq {
            match position_is_materialized(
                db,
                storage,
                store_root_hash,
                coverage,
                &commit.device_id,
                &commit.position(),
            )
            .await?
            {
                MaterializedCheck::Yes => return Ok(Readiness::AlreadyMaterialized),
                MaterializedCheck::Missing => {}
                MaterializedCheck::Held(reason) => {
                    return Ok(Readiness::Held(held_commit(
                        &commit.device_id,
                        commit.position(),
                        reason,
                    )))
                }
            }
            return Ok(Readiness::Held(held_commit(
                &commit.device_id,
                commit.position(),
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: commit.device_id.clone(),
                    referenced_position: commit.position(),
                    materialized_hash: current.commit_hash,
                },
            )));
        }
        if commit.seq != current.seq + 1 || commit.previous_commit_hash != Some(current.commit_hash)
        {
            return Ok(Readiness::Held(held_commit(
                &commit.device_id,
                commit.position(),
                HeldStorePositionReason::MissingPredecessor(CommitPosition {
                    seq: commit.seq - 1,
                    commit_hash: commit
                        .previous_commit_hash
                        .expect("verified non-initial commit has a predecessor"),
                }),
            )));
        }
    } else if commit.seq != 1 || commit.previous_commit_hash.is_some() {
        return Ok(Readiness::Held(held_commit(
            &commit.device_id,
            commit.position(),
            HeldStorePositionReason::MissingPredecessor(CommitPosition {
                seq: commit.seq - 1,
                commit_hash: commit
                    .previous_commit_hash
                    .expect("verified non-initial commit has a predecessor"),
            }),
        )));
    }

    for (device_id, position) in &commit.dependencies {
        match position_is_materialized(db, storage, store_root_hash, coverage, device_id, position)
            .await?
        {
            MaterializedCheck::Yes => {}
            MaterializedCheck::Missing => {
                return Ok(Readiness::Held(held_dependency(
                    commit,
                    device_id,
                    position,
                    HeldStorePositionReason::MissingDependency {
                        device_id: device_id.clone(),
                        position: position.clone(),
                    },
                )))
            }
            MaterializedCheck::Held(reason) => {
                return Ok(Readiness::Held(held_dependency(
                    commit, device_id, position, reason,
                )))
            }
        }
    }
    Ok(Readiness::Ready)
}

async fn position_is_materialized(
    db: &Database,
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    coverage: &BTreeMap<String, CommitPosition>,
    device_id: &str,
    position: &CommitPosition,
) -> Result<MaterializedCheck, DbError> {
    if let Some(actual) = db.exact_materialized_hash(device_id, position.seq).await? {
        if actual != position.commit_hash {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: device_id.to_string(),
                    referenced_position: position.clone(),
                    materialized_hash: actual,
                },
            ));
        }
        return Ok(MaterializedCheck::Yes);
    }
    let Some(covered) = coverage.get(device_id) else {
        return Ok(MaterializedCheck::Missing);
    };
    if position.seq > covered.seq {
        return Ok(MaterializedCheck::Missing);
    }
    let mut seq = covered.seq;
    let mut expected = covered.commit_hash;
    while seq > position.seq {
        let commit = match load_commit_slot(storage, store_root_hash, device_id, seq).await {
            Ok(Some(commit)) => commit,
            Ok(None) => return Ok(MaterializedCheck::Missing),
            Err(error) => return Ok(MaterializedCheck::Held(held_object_error(error))),
        };
        if commit.value.commit_hash() != expected {
            return Ok(MaterializedCheck::Held(
                HeldStorePositionReason::HashMismatch {
                    referenced_device_id: device_id.to_string(),
                    referenced_position: CommitPosition {
                        seq,
                        commit_hash: expected,
                    },
                    materialized_hash: commit.value.commit_hash(),
                },
            ));
        }
        expected = commit
            .value
            .previous_commit_hash
            .expect("verified retained commit above sequence one has a predecessor");
        seq -= 1;
    }
    if expected != position.commit_hash {
        return Ok(MaterializedCheck::Held(
            HeldStorePositionReason::HashMismatch {
                referenced_device_id: device_id.to_string(),
                referenced_position: position.clone(),
                materialized_hash: expected,
            },
        ));
    }
    Ok(MaterializedCheck::Yes)
}

async fn apply_candidate(
    db: &Database,
    storage: &dyn SyncStorage,
    store_dir: &StoreDir,
    schema: Arc<TableSchema>,
    candidate: &Candidate,
) -> Result<ApplyOutcome, StorePullError> {
    let changeset = match ValidatedChangeset::new(candidate.package.clone(), schema) {
        Ok(changeset) => changeset,
        Err(error) => {
            return Ok(ApplyOutcome::Held(match error {
                super::session::ChangesetIdentityError::Row(error) => {
                    HeldStorePositionReason::InvalidRowIdentity {
                        table: error.table().to_string(),
                        reason: error.to_string(),
                    }
                }
                error => HeldStorePositionReason::InvalidChangeset(error.to_string()),
            }))
        }
    };
    let changes = match crate::changeset::walk(changeset.bytes()) {
        Ok(changes) => changes,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let old_changes = match crate::changeset::walk_old(changeset.bytes()) {
        Ok(changes) => changes,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let blob_decls = db.blob_decls();
    let eager = match cache_eager_blobs(&blob_decls, &old_changes, &changes) {
        Ok(eager) => eager,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    if !download_blobs(
        db,
        eager,
        storage,
        store_dir,
        Some(&candidate.commit.author_pubkey),
    )
    .await
    {
        return Ok(ApplyOutcome::Held(
            HeldStorePositionReason::BlobDownloadFailed,
        ));
    }
    let uploads = match introduced_blob_uploads(
        &blob_decls,
        &old_changes,
        &changes,
        Some(&candidate.commit.author_pubkey),
    ) {
        Ok(uploads) => uploads,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let cleanup = match local_blob_cleanup_intents(&blob_decls, &old_changes, &changes) {
        Ok(cleanup) => cleanup,
        Err(error) => {
            return Ok(ApplyOutcome::Held(
                HeldStorePositionReason::InvalidChangeset(error.to_string()),
            ))
        }
    };
    let outcome = commit_candidate(db, candidate, changes, changeset, uploads, cleanup).await?;
    #[cfg(any(test, feature = "test-utils"))]
    if matches!(outcome, ApplyOutcome::Applied(_)) {
        db.reach_test_point(crate::database::DatabaseTestPoint::PullAfterRemoteCommit {
            device_id: candidate.commit.device_id.clone(),
            seq: candidate.commit.seq,
        })
        .await;
    }
    Ok(outcome)
}

async fn commit_candidate(
    db: &Database,
    candidate: &Candidate,
    changes: Vec<RowChange>,
    changeset: ValidatedChangeset<Vec<u8>>,
    uploads: Vec<(String, String, String)>,
    cleanup: Vec<LocalBlobCleanupIntent>,
) -> Result<ApplyOutcome, StorePullError> {
    let commit = candidate.commit.clone();
    let returned_changes = changes.clone();
    let receiver_wall_ms = db.receive_wall_ms();
    let blob_decls = db.blob_decls();
    let mut changeset_max = None;
    advance_max_updated_at(
        &mut changeset_max,
        &changes,
        changeset.schema(),
        receiver_wall_ms,
    );
    let hlc = db.hlc();
    let outcome = db
        .call(move |conn| {
            let tx = conn.unchecked_transaction().map_err(DbError::from)?;
            let apply = resolve_and_apply_changeset_with_schema_on(
                &tx,
                changeset,
                receiver_wall_ms,
                &uploads,
            )?;
            if !apply.constraint_conflict_tables.is_empty() {
                tx.rollback().map_err(DbError::from)?;
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::ConstraintConflict(apply.constraint_conflict_tables),
                ));
            }
            if apply.had_fk_violations {
                tx.rollback().map_err(DbError::from)?;
                return Ok(ApplyOutcome::Held(
                    HeldStorePositionReason::ForeignKeyDependency,
                ));
            }
            for intent in cleanup {
                local_cleanup::record_if_unreferenced_on(&tx, &blob_decls, &intent)?;
            }
            Database::record_materialized_commit_on(&tx, &commit)?;
            tx.commit().map_err(DbError::from)?;
            if let Some(max_applied) = changeset_max.as_ref() {
                hlc.advance_past(max_applied);
            }
            Ok(ApplyOutcome::Applied(returned_changes))
        })
        .await?;
    Ok(outcome)
}

fn held_commit(
    device_id: &str,
    position: CommitPosition,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Commit {
            device_id: device_id.to_string(),
            position,
        },
        reason,
    }
}

fn held_package(commit: &StoreBatchCommit, reason: HeldStorePositionReason) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Package {
            device_id: commit.device_id.clone(),
            seq: commit.seq,
            package_hash: commit.package.content_hash,
        },
        reason,
    }
}

fn held_dependency(
    dependent: &StoreBatchCommit,
    required_device_id: &str,
    required_position: &CommitPosition,
    reason: HeldStorePositionReason,
) -> HeldStorePosition {
    HeldStorePosition {
        coordinate: HeldStoreCoordinate::Dependency {
            dependent_device_id: dependent.device_id.clone(),
            dependent_position: dependent.position(),
            required_device_id: required_device_id.to_string(),
            required_position: required_position.clone(),
        },
        reason,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::keys::UserKeypair;
    use crate::storage::cloud::test_utils::InMemoryCloudHome;
    use crate::storage::cloud::{CloudHome, SequentialCopyIdGenerator};
    use crate::sync::cloud_storage::{BlobPathScheme, CloudCipher, CloudSyncStorage};
    use crate::sync::membership::founder_entry;
    use crate::sync::store_commit::{store_protocol_root_semantic_prefix, StoreProtocolRoot};
    use crate::sync::store_objects::append_and_verify;
    use crate::sync::store_outbound::{drain_outbound_store_batches, stage_pending_store_batch};
    use crate::sync::test_helpers::{
        host_exec, open_test_db, open_test_db_schema, query_text, row_exists, temp_store_dir,
        test_synced_tables,
    };

    fn storage(
        home: &InMemoryCloudHome,
        keypair: &UserKeypair,
        copy_source: &str,
    ) -> CloudSyncStorage {
        CloudSyncStorage::new(
            Arc::new(home.clone()),
            CloudCipher::Plaintext,
            BlobPathScheme::Plain,
            "causal-ordering-test",
            keypair.clone(),
        )
        .with_copy_ids(Arc::new(SequentialCopyIdGenerator::new(copy_source)))
    }

    async fn bind_database(db: &Database, device_id: &str, store_root_hash: ObjectHash) {
        db.set_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY, device_id)
            .await
            .expect("bind local device id");
        db.set_protocol_state(
            crate::database::STORE_ROOT_HASH_STATE_KEY,
            &store_root_hash.to_string(),
        )
        .await
        .expect("bind store protocol root");
    }

    async fn publish_pending(
        db: &Database,
        storage: &CloudSyncStorage,
        device_id: &str,
        keypair: &UserKeypair,
        store_dir: &StoreDir,
    ) -> CommitPosition {
        assert!(stage_pending_store_batch(
            db,
            storage,
            device_id,
            "2026-01-01T00:00:00Z",
            keypair,
            store_dir,
            None,
            None,
        )
        .await
        .expect("stage causal Store commit"));
        assert_eq!(
            drain_outbound_store_batches(db, storage)
                .await
                .expect("publish causal Store commit"),
            1
        );
        db.latest_local_store_position()
            .await
            .expect("read published Store position")
            .expect("published Store position exists")
    }

    async fn publish_package(
        storage: &CloudSyncStorage,
        store_root_hash: ObjectHash,
        device_id: &str,
        package: &[u8],
        keypair: &UserKeypair,
    ) -> CommitPosition {
        let commit = StoreBatchCommit::signed(
            store_root_hash,
            device_id.to_string(),
            1,
            None,
            BTreeMap::new(),
            None,
            1,
            package,
            keypair,
        )
        .expect("sign Store commit");
        let head = StoreDeviceHead::signed(
            store_root_hash,
            device_id.to_string(),
            Some(commit.position()),
            "2026-01-01T00:00:00Z".to_string(),
            keypair,
        )
        .expect("sign Store head");
        append_and_verify(storage, &commit.package.object_key, ".pkg", package)
            .await
            .expect("publish package");
        append_and_verify(
            storage,
            &crate::sync::store_commit::commit_semantic_prefix(device_id, 1, commit.commit_hash()),
            ".json",
            &commit.to_bytes(),
        )
        .await
        .expect("publish commit");
        append_and_verify(
            storage,
            &crate::sync::store_commit::head_semantic_prefix(device_id, 1, head.head_hash()),
            ".json",
            &head.to_bytes(),
        )
        .await
        .expect("publish head");
        commit.position()
    }

    fn independent_test_tables() -> Vec<SyncedTable> {
        vec![
            SyncedTable::new("notes", crate::sync::session::RowIdentity::IndependentUuid)
                .gated_by("shared"),
            SyncedTable::new(
                "note_tags",
                crate::sync::session::RowIdentity::IndependentUuid,
            ),
            SyncedTable::new(
                "note_photos",
                crate::sync::session::RowIdentity::IndependentUuid,
            ),
        ]
    }

    async fn setup_store() -> (InMemoryCloudHome, UserKeypair, ObjectHash) {
        let home = InMemoryCloudHome::new();
        home.sort_listings();
        let identity = UserKeypair::generate();
        let founder = founder_entry(
            "causal-ordering-test",
            &identity,
            "0000000000001-0000-founder",
        );
        let store_protocol_root =
            StoreProtocolRoot::signed("causal-ordering-test".to_string(), founder, 1, &identity)
                .expect("sign store protocol root");
        let store_root_hash = store_protocol_root.object_hash();
        append_and_verify(
            &storage(&home, &identity, "store-protocol-root"),
            &store_protocol_root_semantic_prefix(store_root_hash),
            ".json",
            &store_protocol_root.to_bytes(),
        )
        .await
        .expect("publish store protocol root");
        (home, identity, store_root_hash)
    }

    #[tokio::test]
    async fn invalid_remote_uuid_holds_exact_commit_while_another_device_applies() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        let invalid_package = crate::sync::test_helpers::capture_bytes(
            &source,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('not-a-uuid', 'invalid', NULL, 1, '0000000001000-0000-bad', '2026-01-01')",
            ],
        )
        .await;
        let valid_id = "f47ac10b-58cc-4372-a567-0e02b2c3d479";
        let valid_package = crate::sync::test_helpers::capture_bytes(
            &source,
            &[&format!(
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
                 VALUES ('{valid_id}', 'valid', NULL, 1, '0000000001001-0000-good', '2026-01-01')"
            )],
        )
        .await;
        let shared_storage = storage(&home, &identity, "identity-receipt");
        let invalid_position = publish_package(
            &shared_storage,
            store_root_hash,
            "dev-a-invalid",
            &invalid_package,
            &identity,
        )
        .await;
        let valid_position = publish_package(
            &shared_storage,
            store_root_hash,
            "dev-z-valid",
            &valid_package,
            &identity,
        )
        .await;

        let receiver = open_test_db_schema(
            independent_test_tables(),
            crate::sync::test_helpers::test_migrations(),
        );
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let pulled = pull_store_commits(
            &receiver,
            &independent_test_tables(),
            &shared_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("invalid identity is a held commit");

        assert_eq!(pulled.changesets_applied, 1);
        assert_eq!(pulled.held_positions.len(), 1);
        assert!(matches!(
            &pulled.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Commit { device_id, position },
                reason: HeldStorePositionReason::InvalidRowIdentity { table, reason },
            } if device_id == "dev-a-invalid"
                && position == &invalid_position
                && table == "notes"
                && reason.contains("not-a-uuid")
        ));
        assert_eq!(pulled.frontier.get("dev-z-valid"), Some(&valid_position));
        assert!(!pulled.frontier.contains_key("dev-a-invalid"));
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-a-invalid", 1)
                .await
                .expect("read invalid position"),
            None
        );
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-z-valid", 1)
                .await
                .expect("read valid position"),
            Some(valid_position.commit_hash)
        );
        assert!(!row_exists(&receiver, "SELECT 1 FROM notes WHERE id = 'not-a-uuid'").await);
        assert!(
            row_exists(
                &receiver,
                &format!("SELECT 1 FROM notes WHERE id = '{valid_id}'")
            )
            .await
        );
    }

    async fn remove_appended_prefix(
        home: &InMemoryCloudHome,
        prefix: &str,
    ) -> Vec<(String, Vec<u8>)> {
        let removed: Vec<_> = home
            .appended_keys()
            .into_iter()
            .filter(|key| key.starts_with(prefix))
            .map(|key| {
                let bytes = home.get_appended(&key).expect("listed appended bytes");
                (key, bytes)
            })
            .collect();
        let listing = home
            .list_appended(prefix)
            .await
            .expect("list appended candidates");
        for locator in listing.objects {
            home.remove_appended_candidate(&locator);
        }
        removed
    }

    fn restore_appended(home: &InMemoryCloudHome, removed: Vec<(String, Vec<u8>)>) {
        for (key, bytes) in removed {
            home.insert_appended_candidate(&key, bytes);
        }
    }

    fn unique_note_db() -> Database {
        open_test_db_schema(
            vec![SyncedTable::new(
                "unique_notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            vec![crate::migration::Migration::run(
                1,
                "unique-note-schema",
                |conn| {
                    conn.execute_batch(
                        "CREATE TABLE unique_notes (\
                           id TEXT PRIMARY KEY,\
                           slug TEXT NOT NULL UNIQUE,\
                           title TEXT NOT NULL,\
                           _updated_at TEXT NOT NULL,\
                           created_at TEXT NOT NULL\
                         ) STRICT;",
                    )
                    .map_err(DbError::from)
                },
            )],
        )
    }

    async fn assert_update_before_insert_converges(inserter: &str, updater: &str) {
        let home = InMemoryCloudHome::new();
        home.sort_listings();
        let identity = UserKeypair::generate();
        let founder = founder_entry(
            "causal-ordering-test",
            &identity,
            "0000000000001-0000-founder",
        );
        let store_protocol_root =
            StoreProtocolRoot::signed("causal-ordering-test".to_string(), founder, 1, &identity)
                .expect("sign store protocol root");
        let store_root_hash = store_protocol_root.object_hash();
        let setup_storage = storage(&home, &identity, "setup");
        append_and_verify(
            &setup_storage,
            &store_protocol_root_semantic_prefix(store_root_hash),
            ".json",
            &store_protocol_root.to_bytes(),
        )
        .await
        .expect("publish store protocol root");

        let insert_db = open_test_db();
        bind_database(&insert_db, inserter, store_root_hash).await;
        host_exec(
            &insert_db,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'orig', NULL, 1, '0000000001000-0000-ins', '2026-01-01')",
        )
        .await;
        let (_insert_tmp, insert_dir) = temp_store_dir();
        let insert_storage = storage(&home, &identity, "insert");
        publish_pending(
            &insert_db,
            &insert_storage,
            inserter,
            &identity,
            &insert_dir,
        )
        .await;

        let update_db = open_test_db();
        bind_database(&update_db, updater, store_root_hash).await;
        let (_update_tmp, update_dir) = temp_store_dir();
        let update_storage = storage(&home, &identity, "update");
        let first_pull = pull_store_commits(
            &update_db,
            &test_synced_tables(),
            &update_storage,
            store_root_hash,
            updater,
            &update_dir,
            None,
        )
        .await
        .expect("updater observes the insert");
        assert_eq!(first_pull.changesets_applied, 1);
        host_exec(
            &update_db,
            "UPDATE notes SET title = 'updated', _updated_at = '0000000002000-0000-upd' \
             WHERE id = 'n1'",
        )
        .await;
        publish_pending(&update_db, &update_storage, updater, &identity, &update_dir).await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let receiver_storage = storage(&home, &identity, "receiver");
        let pull = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("receiver applies the causal ready queue");

        assert_eq!(pull.changesets_applied, 2);
        assert!(pull.held_positions.is_empty());
        assert_eq!(
            query_text(&receiver, "SELECT title FROM notes WHERE id = 'n1'").await,
            "updated",
            "the UPDATE must wait for and then apply after its causal INSERT",
        );
    }

    #[tokio::test]
    async fn update_applied_before_its_insert_diverges_notfound_omit() {
        assert_update_before_insert_converges("dev-z", "dev-a").await;
    }

    #[tokio::test]
    async fn update_and_insert_converge_in_the_opposite_discovery_order() {
        assert_update_before_insert_converges("dev-a", "dev-z").await;
    }

    #[tokio::test]
    async fn latest_head_walks_and_applies_every_unseen_predecessor() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = open_test_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        let publisher_storage = storage(&home, &identity, "publisher");

        host_exec(
            &publisher,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'first', NULL, 1, '0000000001000-0000-pub', '2026-01-01')",
        )
        .await;
        publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        host_exec(
            &publisher,
            "UPDATE notes SET title = 'second', _updated_at = '0000000002000-0000-pub' \
             WHERE id = 'n1'",
        )
        .await;
        let second = publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        assert_eq!(second.seq, 2);

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let result = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("walk latest head ancestry");

        assert_eq!(result.changesets_applied, 2);
        assert!(result.held_positions.is_empty());
        assert_eq!(
            query_text(&receiver, "SELECT title FROM notes WHERE id = 'n1'").await,
            "second"
        );
    }

    #[tokio::test]
    async fn row_and_materialized_position_roll_back_together_on_frontier_failure() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = open_test_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        host_exec(
            &publisher,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'atomic', NULL, 1, '0000000001000-0000-pub', '2026-01-01')",
        )
        .await;
        let position = publish_pending(
            &publisher,
            &storage(&home, &identity, "publisher"),
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        receiver
            .call(|conn| {
                conn.execute_batch(
                    "CREATE TEMP TRIGGER fail_materialized_frontier \
                     BEFORE INSERT ON materialized_commits \
                     BEGIN SELECT RAISE(ABORT, 'injected frontier failure'); END;",
                )
                .map_err(DbError::from)
            })
            .await
            .expect("install frontier fault");
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let first = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-first"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await;
        assert!(matches!(first, Err(StorePullError::Database(_))));
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            receiver
                .call(|conn| {
                    conn.query_row("SELECT COUNT(*) FROM notes WHERE id = 'n1'", [], |row| {
                        row.get::<_, i64>(0)
                    })
                    .map_err(DbError::from)
                })
                .await
                .unwrap(),
            0,
            "the row must roll back when its exact position cannot commit",
        );

        receiver
            .call(|conn| {
                conn.execute_batch("DROP TRIGGER fail_materialized_frontier")
                    .map_err(DbError::from)
            })
            .await
            .expect("remove frontier fault");
        let retry = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-retry"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("retry atomic apply");
        assert_eq!(retry.changesets_applied, 1);
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            Some(position.commit_hash)
        );
    }

    #[tokio::test]
    async fn host_write_captures_the_exact_materialized_dependency_frontier() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        bind_database(&source, "dev-source", store_root_hash).await;
        let (_source_tmp, source_dir) = temp_store_dir();
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('source', 'source', NULL, 1, '0000000001000-0000-src', '2026-01-01')",
        )
        .await;
        let source_position = publish_pending(
            &source,
            &storage(&home, &identity, "source"),
            "dev-source",
            &identity,
            &source_dir,
        )
        .await;

        let writer = open_test_db();
        bind_database(&writer, "dev-writer", store_root_hash).await;
        let (_writer_tmp, writer_dir) = temp_store_dir();
        let writer_storage = storage(&home, &identity, "writer");
        pull_store_commits(
            &writer,
            &test_synced_tables(),
            &writer_storage,
            store_root_hash,
            "dev-writer",
            &writer_dir,
            None,
        )
        .await
        .expect("materialize dependency");
        host_exec(
            &writer,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('writer', 'writer', NULL, 1, '0000000002000-0000-wrt', '2026-01-01')",
        )
        .await;
        let dependencies: BTreeMap<String, CommitPosition> = serde_json::from_str(
            &query_text(
                &writer,
                "SELECT dependencies FROM pending_changesets ORDER BY id DESC LIMIT 1",
            )
            .await,
        )
        .expect("parse captured dependencies");
        assert_eq!(
            dependencies.get("dev-source"),
            Some(&source_position),
            "the host transaction records the exact frontier it observed",
        );

        let writer_position = publish_pending(
            &writer,
            &writer_storage,
            "dev-writer",
            &identity,
            &writer_dir,
        )
        .await;
        let commit = load_commit_slot(
            &writer_storage,
            store_root_hash,
            "dev-writer",
            writer_position.seq,
        )
        .await
        .expect("load writer commit")
        .expect("writer commit exists");
        assert_eq!(commit.value.dependencies, dependencies);
    }

    #[tokio::test]
    async fn missing_dependency_is_held_across_restart_then_applied_when_visible() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        bind_database(&source, "dev-source", store_root_hash).await;
        let (_source_tmp, source_dir) = temp_store_dir();
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('source', 'source', NULL, 1, '0000000001000-0000-src', '2026-01-01')",
        )
        .await;
        let source_position = publish_pending(
            &source,
            &storage(&home, &identity, "source"),
            "dev-source",
            &identity,
            &source_dir,
        )
        .await;

        let dependent = open_test_db();
        bind_database(&dependent, "dev-dependent", store_root_hash).await;
        let (_dependent_tmp, dependent_dir) = temp_store_dir();
        let dependent_storage = storage(&home, &identity, "dependent");
        pull_store_commits(
            &dependent,
            &test_synced_tables(),
            &dependent_storage,
            store_root_hash,
            "dev-dependent",
            &dependent_dir,
            None,
        )
        .await
        .expect("dependent observes source");
        host_exec(
            &dependent,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('dependent', 'dependent', NULL, 1, '0000000002000-0000-dep', '2026-01-01')",
        )
        .await;
        publish_pending(
            &dependent,
            &dependent_storage,
            "dev-dependent",
            &identity,
            &dependent_dir,
        )
        .await;

        let removed_heads = remove_appended_prefix(&home, "store-v1/heads/dev-source/").await;
        assert!(!removed_heads.is_empty());
        for attempt in ["receiver-before-restart", "receiver-after-restart"] {
            let receiver = open_test_db();
            bind_database(&receiver, attempt, store_root_hash).await;
            let (_receiver_tmp, receiver_dir) = temp_store_dir();
            let held = pull_store_commits(
                &receiver,
                &test_synced_tables(),
                &storage(&home, &identity, attempt),
                store_root_hash,
                attempt,
                &receiver_dir,
                None,
            )
            .await
            .expect("missing dependency is a held state");
            assert!(held.held_positions.iter().any(|position| {
                matches!(
                    &position.reason,
                    HeldStorePositionReason::MissingDependency {
                        device_id,
                        position,
                    } if device_id == "dev-source" && position == &source_position
                )
            }));
            assert_eq!(held.changesets_applied, 0);
        }

        restore_appended(&home, removed_heads);
        let receiver = open_test_db();
        bind_database(&receiver, "dev-final", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let applied = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-final"),
            store_root_hash,
            "dev-final",
            &receiver_dir,
            None,
        )
        .await
        .expect("dependency becomes visible");
        assert_eq!(applied.changesets_applied, 2);
        assert!(applied.held_positions.is_empty());
    }

    #[tokio::test]
    async fn missing_predecessor_is_held_until_the_exact_commit_reappears() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = open_test_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        let publisher_storage = storage(&home, &identity, "publisher");
        host_exec(
            &publisher,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'one', NULL, 1, '0000000001000-0000-pub', '2026-01-01')",
        )
        .await;
        publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        host_exec(
            &publisher,
            "UPDATE notes SET title = 'two', _updated_at = '0000000002000-0000-pub' \
             WHERE id = 'n1'",
        )
        .await;
        publish_pending(
            &publisher,
            &publisher_storage,
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;
        let removed = remove_appended_prefix(&home, "store-v1/commits/dev-publisher/1/").await;
        assert!(!removed.is_empty());

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let held = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("missing predecessor is held");
        assert!(held.held_positions.iter().any(|position| matches!(
            position.reason,
            HeldStorePositionReason::MissingPredecessor(_)
        )));
        assert_eq!(held.changesets_applied, 0);

        restore_appended(&home, removed);
        let retry = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-retry"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("predecessor reappears");
        assert_eq!(retry.changesets_applied, 2);
        assert!(retry.held_positions.is_empty());
    }

    #[tokio::test]
    async fn dependency_hash_mismatch_fails_against_the_durable_exact_position() {
        let (home, identity, store_root_hash) = setup_store().await;
        let source = open_test_db();
        bind_database(&source, "dev-source", store_root_hash).await;
        let (_source_tmp, source_dir) = temp_store_dir();
        host_exec(
            &source,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('source', 'source', NULL, 1, '0000000001000-0000-src', '2026-01-01')",
        )
        .await;
        publish_pending(
            &source,
            &storage(&home, &identity, "source"),
            "dev-source",
            &identity,
            &source_dir,
        )
        .await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let receiver_storage = storage(&home, &identity, "receiver");
        pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("materialize exact source position");

        let package_source = open_test_db();
        let package = crate::sync::test_helpers::capture_bytes(
            &package_source,
            &[
                "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
               VALUES ('dependent', 'dependent', NULL, 1, '0000000002000-0000-dep', '2026-01-01')",
            ],
        )
        .await;
        let fake_dependency = CommitPosition {
            seq: 1,
            commit_hash: ObjectHash::digest(b"not-the-source-commit"),
        };
        let mut dependencies = BTreeMap::new();
        dependencies.insert("dev-source".to_string(), fake_dependency.clone());
        let commit = StoreBatchCommit::signed(
            store_root_hash,
            "dev-dependent".to_string(),
            1,
            None,
            dependencies,
            None,
            receiver.schema_version(),
            &package,
            &identity,
        )
        .expect("sign mismatched dependency commit");
        let head = StoreDeviceHead::signed(
            store_root_hash,
            "dev-dependent".to_string(),
            Some(commit.position()),
            "2026-01-01T00:00:00Z".to_string(),
            &identity,
        )
        .expect("sign dependent head");
        append_and_verify(
            &receiver_storage,
            &commit.package.object_key,
            ".pkg",
            &package,
        )
        .await
        .unwrap();
        append_and_verify(
            &receiver_storage,
            &crate::sync::store_commit::commit_semantic_prefix(
                "dev-dependent",
                1,
                commit.commit_hash(),
            ),
            ".json",
            &commit.to_bytes(),
        )
        .await
        .unwrap();
        append_and_verify(
            &receiver_storage,
            &crate::sync::store_commit::head_semantic_prefix("dev-dependent", 1, head.head_hash()),
            ".json",
            &head.to_bytes(),
        )
        .await
        .unwrap();

        let result = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("dependency hash mismatch holds the dependent commit");
        assert_eq!(result.held_positions.len(), 1);
        assert!(matches!(
            &result.held_positions[0],
            HeldStorePosition {
                coordinate: HeldStoreCoordinate::Dependency {
                    dependent_device_id,
                    dependent_position,
                    required_device_id,
                    required_position,
                },
                reason: HeldStorePositionReason::HashMismatch {
                    materialized_hash,
                    ..
                },
            } if dependent_device_id == "dev-dependent"
                && dependent_position == &commit.position()
                && required_device_id == "dev-source"
                && required_position == &fake_dependency
                && materialized_hash != &fake_dependency.commit_hash
        ));
    }

    #[tokio::test]
    async fn non_fk_constraint_rolls_back_rows_and_exact_position_then_retries() {
        let (home, identity, store_root_hash) = setup_store().await;
        let publisher = unique_note_db();
        bind_database(&publisher, "dev-publisher", store_root_hash).await;
        let (_publisher_tmp, publisher_dir) = temp_store_dir();
        host_exec(
            &publisher,
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('partial', 'free-slug', 'Partial', '0000000001000-0000-pub', '2026-01-01'); \
             INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('remote', 'same-slug', 'Remote', '0000000001001-0000-pub', '2026-01-01')",
        )
        .await;
        let position = publish_pending(
            &publisher,
            &storage(&home, &identity, "publisher-unique"),
            "dev-publisher",
            &identity,
            &publisher_dir,
        )
        .await;

        let receiver = unique_note_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        host_exec(
            &receiver,
            "INSERT INTO unique_notes (id, slug, title, _updated_at, created_at) \
             VALUES ('local', 'same-slug', 'Local', '0000000002000-0000-rec', '2026-01-01')",
        )
        .await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let receiver_storage = storage(&home, &identity, "receiver-unique");
        let held = pull_store_commits(
            &receiver,
            &[SyncedTable::new(
                "unique_notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("uniqueness conflict is a held position");
        assert!(held.held_positions.iter().any(|held| matches!(
            &held.reason,
            HeldStorePositionReason::ConstraintConflict(tables)
                if tables == &["unique_notes".to_string()]
        )));
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            None
        );
        assert!(!row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'partial'").await);
        assert!(!row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'remote'").await);
        assert!(row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'local'").await);

        host_exec(&receiver, "DELETE FROM unique_notes WHERE id = 'local'").await;
        let retry = pull_store_commits(
            &receiver,
            &[SyncedTable::new(
                "unique_notes",
                crate::sync::session::RowIdentity::SharedKey,
            )],
            &receiver_storage,
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("constraint removed");
        assert_eq!(retry.changesets_applied, 1);
        assert_eq!(
            receiver
                .exact_materialized_hash("dev-publisher", 1)
                .await
                .unwrap(),
            Some(position.commit_hash)
        );
        assert!(row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'partial'").await);
        assert!(row_exists(&receiver, "SELECT 1 FROM unique_notes WHERE id = 'remote'").await);
    }

    async fn assert_concurrent_delete_update_converges(deleter: &str, updater: &str) {
        let (home, identity, store_root_hash) = setup_store().await;
        let base = open_test_db();
        bind_database(&base, "dev-base", store_root_hash).await;
        let (_base_tmp, base_dir) = temp_store_dir();
        host_exec(
            &base,
            "INSERT INTO notes (id, title, body, shared, _updated_at, created_at) \
             VALUES ('n1', 'base', NULL, 1, '0000000001000-0000-base', '2026-01-01')",
        )
        .await;
        publish_pending(
            &base,
            &storage(&home, &identity, "base"),
            "dev-base",
            &identity,
            &base_dir,
        )
        .await;

        let delete_db = open_test_db();
        bind_database(&delete_db, deleter, store_root_hash).await;
        let (_delete_tmp, delete_dir) = temp_store_dir();
        let delete_storage = storage(&home, &identity, "delete");
        pull_store_commits(
            &delete_db,
            &test_synced_tables(),
            &delete_storage,
            store_root_hash,
            deleter,
            &delete_dir,
            None,
        )
        .await
        .unwrap();

        let update_db = open_test_db();
        bind_database(&update_db, updater, store_root_hash).await;
        let (_update_tmp, update_dir) = temp_store_dir();
        let update_storage = storage(&home, &identity, "update");
        pull_store_commits(
            &update_db,
            &test_synced_tables(),
            &update_storage,
            store_root_hash,
            updater,
            &update_dir,
            None,
        )
        .await
        .unwrap();

        host_exec(&delete_db, "DELETE FROM notes WHERE id = 'n1'").await;
        publish_pending(&delete_db, &delete_storage, deleter, &identity, &delete_dir).await;
        host_exec(
            &update_db,
            "UPDATE notes SET title = 'updated', _updated_at = '0000000002000-0000-update' \
             WHERE id = 'n1'",
        )
        .await;
        publish_pending(&update_db, &update_storage, updater, &identity, &update_dir).await;

        let receiver = open_test_db();
        bind_database(&receiver, "dev-receiver", store_root_hash).await;
        let (_receiver_tmp, receiver_dir) = temp_store_dir();
        let result = pull_store_commits(
            &receiver,
            &test_synced_tables(),
            &storage(&home, &identity, "receiver-delete-update"),
            store_root_hash,
            "dev-receiver",
            &receiver_dir,
            None,
        )
        .await
        .expect("apply both causal-ready branches");
        assert_eq!(result.changesets_applied, 3);
        assert!(result.held_positions.is_empty());
        assert!(
            !row_exists(&receiver, "SELECT 1 FROM notes WHERE id = 'n1'").await,
            "concurrent delete wins regardless of ready-queue order",
        );
    }

    #[tokio::test]
    async fn delete_then_update_when_both_are_causal_ready_converges() {
        assert_concurrent_delete_update_converges("dev-a-delete", "dev-z-update").await;
    }

    #[tokio::test]
    async fn update_then_delete_when_both_are_causal_ready_converges() {
        assert_concurrent_delete_update_converges("dev-z-delete", "dev-a-update").await;
    }
}
