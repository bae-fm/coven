use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use super::*;
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup::{self, LocalBlobCleanupIntent};
use crate::database::{BlobActivation, Database, DbError, VerifiedMergeMaterialization};
use crate::store_dir::StoreDir;
use crate::sync::apply::{resolve_and_apply_changeset_with_policy_on, ValidatedChangeset};
use crate::sync::audience_package::{AudiencePackage, PackageAudience};
use crate::sync::circle_activation::{
    CircleMembershipAuthority, VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::conflict::{IncomingTimestampPolicy, TableSchema};
use crate::sync::membership::{MembershipChain, MembershipStatus};
use crate::sync::pull::{
    advance_max_updated_at, cache_eager_blobs, local_blob_cleanup_intents, verify_package_blobs,
};
use crate::sync::session::SyncedTable;
use crate::sync::storage::{
    BlobSpoolProtection, ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
};
use crate::sync::store_commit::{
    head_slot_prefix, CommitFrontier, DeviceStreamAnchor, ObjectHash,
    OpenedRetainedMergeHistorySummary, OwnerRecoveryNode, OwnerRecoveryNodeRef,
    ResolvedStoreDeviceState, RetainedVerifiedMergeHistorySummary, RetainedVerifiedRegistration,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitAnchor, StoreCommitCoord, StoreDeviceHead,
    StoreDeviceProposalAck, StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreDeviceStatus, StoreHistoryCut,
    StoreProtocolError, StoreRootRef, VerifiedStoreDeviceOperations,
};
use crate::sync::store_objects::{
    load_commit_ref, load_founder_registration_with_root, load_registration_ref,
    load_store_ack_ref, load_store_package, load_store_protocol_root, StoreObjectError,
};
use crate::sync::store_pull::*;
use crate::sync::{
    causal_grants, gate, hlc, membership, membership_ops, remote_object, retained_replay, session,
    store_commit, store_objects, store_outbound,
};

mod device_operations;
mod discovery;
mod history;
mod join_bootstrap;
mod materialization;
mod membership_control;
mod owner_promotion;
mod registration_authority;
mod replay;
mod retained_authority;
mod snapshot_authority;
mod terminal_authority;
mod terminal_cleanup;

pub(crate) use device_operations::{
    derive_local_post_device_state, load_local_commit_device_operations,
};
pub(crate) use discovery::*;
pub(crate) use history::*;
pub(in crate::sync::store_engine) use join_bootstrap::{
    materialize_device_join_activation, prepare_device_join_bootstrap,
};
pub(crate) use materialization::*;
pub(crate) use membership_control::*;
pub(in crate::sync::store_engine) use owner_promotion::{
    find_request_activation as find_owner_promotion_request_activation,
    verify_acceptance as verify_owner_promotion_acceptance,
};
pub(in crate::sync::store_engine) use registration_authority::load_device_join_authorization;
pub(crate) use registration_authority::{
    load_merge_predecessor_membership, load_merge_predecessor_membership_with_verified_activations,
    verify_merge_membership_state_ref,
};
use replay::*;
pub(crate) use retained_authority::*;
pub(in crate::sync::store_engine) use snapshot_authority::{
    verify_history_authority, verify_snapshot_for_acknowledgement, verify_snapshot_stability,
};
pub(super) use terminal_authority::*;
pub(crate) use terminal_cleanup::cleanup_merge_candidate;
pub(super) use terminal_cleanup::resume_merge_retraction_cleanups;

#[derive(Clone)]
enum MergeCandidateDeviceOperations {
    Verified(VerifiedStoreDeviceOperations),
    Pending,
}

#[derive(Clone)]
struct MergeCandidate {
    candidate: Candidate,
    activation_head: StoreDeviceHead,
    activation_head_object: ExactObjectRef,
    predecessor_membership: MembershipChain,
    device_operations: MergeCandidateDeviceOperations,
}

struct LoadedMergePredecessorMemberships {
    by_commit: BTreeMap<StoreBatchCommitRef, MembershipChain>,
}

impl LoadedMergePredecessorMemberships {
    fn membership_for(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Result<&MembershipChain, StorePullError> {
        self.by_commit.get(reference).ok_or_else(|| {
            StorePullError::Database(format!(
                "retained Merge commit {reference:?} has no loaded predecessor membership"
            ))
        })
    }
}

impl AuthorizedMergeStoreEngine<'_> {
    pub(in crate::sync::store_engine) async fn pull(
        &self,
        store_dir: &StoreDir,
        identity: &UserKeypair,
    ) -> Result<StorePullResult, SyncCycleFailure> {
        pull_store_commits(
            self.db(),
            self.db().synced_tables(),
            self.storage(),
            self.store_root().store_root_hash,
            store_dir,
            &self.membership,
            Some(identity),
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))
    }

    pub(in crate::sync::store_engine) async fn should_stop_before_pull(
        &self,
    ) -> Result<bool, SyncCycleFailure> {
        Ok(false)
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pull_store_commits<'a>(
    db: &'a Database,
    tables: &'a [crate::sync::session::SyncedTable],
    storage: &'a dyn SyncStorage,
    store_root_hash: crate::sync::store_commit::ObjectHash,
    store_dir: &'a StoreDir,
    membership: &'a crate::sync::membership::MembershipChain,
    identity: Option<&'a UserKeypair>,
) -> Pin<Box<dyn Future<Output = Result<StorePullResult, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let root = required_pull_root(db, store_root_hash).await?;
        let verified_root = load_store_protocol_root(storage, &root).await?.value;
        if verified_root.descriptor.write_policy != crate::WritePolicy::MergeConcurrent {
            return Err(StorePullError::Database(
                "durable write policy differs from the signed Store root".to_string(),
            ));
        }
        resume_merge_retraction_cleanups(db, storage, &root).await?;

        let local_frontier = db.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load discovery device-state frontier: {error}"))
        })?;
        let local_frontier = local_frontier
            .into_values()
            .map(|reference| match reference.coord {
                StoreCommitCoord::MergeConcurrent { stream_id, .. } => Ok((stream_id, reference)),
                StoreCommitCoord::Serial { .. } => Err(StorePullError::Database(
                    "Merge discovery frontier contains a Serial commit".to_string(),
                )),
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let (_, discovery_device_state) = db
            .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(local_frontier))
            .await?;

        let mut active = load_active_merge_registrations(db, storage, &root)
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load active Merge registrations: {error}"))
            })?;
        for recovered in
            discover_merge_owner_recoveries(storage, &root, &verified_root, membership).await?
        {
            if active
                .iter()
                .all(|(reference, _)| reference != &recovered.0)
            {
                active.push(recovered);
            }
        }
        let mut candidates = BTreeMap::new();
        let mut visible_heads = Vec::new();
        let mut held = Vec::new();
        for (registration_ref, registration) in active {
            let inactive_cut = match discovery_device_state
                .devices
                .get(&registration_ref.device_id)
            {
                Some(record) if record.registration != registration_ref => {
                    return Err(StorePullError::Database(format!(
                        "discovery device state names another registration for {}",
                        registration_ref.device_id
                    )));
                }
                Some(record) => match &record.status {
                    StoreDeviceStatus::Active => None,
                    StoreDeviceStatus::Inactive { accepted_cut, .. } => Some(accepted_cut),
                },
                None => None,
            };
            let discovered = discover_merge_stream(
                storage,
                &root,
                &registration_ref,
                &registration,
                inactive_cut,
            )
            .await
            .map_err(|error| {
                StorePullError::Database(format!(
                    "discover Merge stream for {}: {error}",
                    registration.device_id
                ))
            })?;
            if let Some(head) = discovered.latest_head {
                visible_heads.push(VerifiedStoreDeviceHead {
                    head,
                    author: registration.clone(),
                });
            }
            if let Some(block) = discovered.block {
                held.push(block.into_position());
            }
            for (activation_head_ref, activation_head, commit_ref, commit) in discovered.commits {
                if commit_ref.coord.sequence() != commit.seq() {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::InvalidObject(
                            "exact commit coordinate differs from signed sequence".to_string(),
                        ),
                    ));
                    continue;
                }
                let stream_id = commit_stream_id(&commit_ref.coord);
                if let Some(materialized) = db
                    .exact_materialized_ref(&stream_id, commit_ref.coord.sequence())
                    .await?
                {
                    if materialized == commit_ref {
                        continue;
                    }
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::HashMismatch {
                            referenced_device_id: stream_id,
                            referenced_commit: commit_ref.clone(),
                            materialized_hash: materialized.commit_hash,
                        },
                    ));
                    continue;
                }
                if let Some(package) = commit.store_package() {
                    if package.schema_version > db.schema_version() {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::NewerSchema {
                                local: db.schema_version(),
                                required: package.schema_version,
                            },
                        ));
                        continue;
                    }
                }
                let predecessor_membership = match load_merge_predecessor_membership(
                    storage,
                    &root,
                    &commit.membership_state,
                )
                .await
                {
                    Ok(membership) => membership,
                    Err(RegistrationLoadError::Object(error)) => {
                        held.push(held_commit(&commit_ref, held_object_error(error)));
                        continue;
                    }
                    Err(RegistrationLoadError::Invalid(error)) => {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::InvalidObject(error),
                        ));
                        continue;
                    }
                };
                let requires_accepted_predecessor = commit
                    .device_join_attempt_decisions()
                    .iter()
                    .any(|decision| {
                        matches!(
                            decision,
                            crate::sync::store_commit::DeviceJoinAttemptDecisionRef::Attempt(_)
                        )
                    })
                    || !commit.device_join_outcomes().is_empty()
                    || !commit.device_join_cleanup_receipts().is_empty()
                    || commit.device_registrations().iter().any(|activation| {
                        matches!(
                            activation.authority,
                            StoreDeviceRegistrationActivationRef::Join { .. }
                        )
                    });
                if requires_accepted_predecessor {
                    let predecessor_cut = commit
                        .order
                        .predecessor_cut()
                        .map_err(|error| StorePullError::Database(error.to_string()))?;
                    verify_history_authority(
                        storage,
                        &root,
                        &predecessor_cut,
                        &commit.membership_state,
                    )
                    .await?;
                }
                let predecessor_authority =
                    RegistrationPredecessorAuthority::MergeConcurrent(&predecessor_membership);
                let exact_predecessor = VerifiedAcceptedPredecessor::Exact;
                let registrations = match Box::pin(load_commit_registrations(
                    storage,
                    &root,
                    &commit,
                    &registration,
                    Some(&predecessor_authority),
                    requires_accepted_predecessor.then_some(&exact_predecessor),
                ))
                .await
                {
                    Ok(registrations) => registrations,
                    Err(RegistrationLoadError::Object(error)) => {
                        held.push(held_commit(&commit_ref, held_object_error(error)));
                        continue;
                    }
                    Err(RegistrationLoadError::Invalid(error)) => {
                        held.push(held_commit(
                            &commit_ref,
                            HeldStorePositionReason::InvalidObject(error),
                        ));
                        continue;
                    }
                };
                if !membership_authorizes(Some(&predecessor_membership), &commit, &registration) {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::Unauthorized,
                    ));
                    continue;
                }
                let package = match load_store_package(storage, &commit_ref, &commit).await {
                    Ok(package) => package.map(|package| package.value),
                    Err(error) => {
                        held.push(held_package(&commit_ref, &commit, held_object_error(error)));
                        continue;
                    }
                };
                let key = (
                    commit_stream_id(&commit_ref.coord),
                    commit_ref.coord.sequence(),
                );
                let device_operations = if commit.device_exclusion_proposals().is_empty()
                    && commit.device_exclusion_outcomes().is_empty()
                {
                    MergeCandidateDeviceOperations::Verified(
                        crate::sync::store_commit::VerifiedStoreDeviceOperations::without_exclusions(
                            &commit,
                        )
                        .map_err(|error| StorePullError::Database(error.to_string()))?,
                    )
                } else {
                    MergeCandidateDeviceOperations::Pending
                };
                candidates.insert(
                    key,
                    MergeCandidate {
                        activation_head,
                        activation_head_object: activation_head_ref.object,
                        candidate: Candidate {
                            commit_ref,
                            commit,
                            author: registration.clone(),
                            package,
                            registrations,
                        },
                        predecessor_membership,
                        device_operations,
                    },
                );
            }
        }

        let retained = Box::pin(db.retained_merge_replay_inputs()).await?;
        let mut loaded_predecessor_memberships = BTreeMap::new();
        for materialization in retained {
            if materialization.commit().membership_authority.is_none() {
                continue;
            }
            let membership = Box::pin(load_merge_predecessor_membership(
                storage,
                &root,
                &materialization.commit().membership_state,
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            loaded_predecessor_memberships.insert(materialization.commit_ref().clone(), membership);
        }
        for candidate in candidates.values() {
            loaded_predecessor_memberships.insert(
                candidate.candidate.commit_ref.clone(),
                candidate.predecessor_membership.clone(),
            );
        }
        let loaded_predecessor_memberships = LoadedMergePredecessorMemberships {
            by_commit: loaded_predecessor_memberships,
        };

        let schema: Arc<TableSchema> = {
            let tables = tables.to_vec();
            let gates = db.gates();
            Arc::new(
                db.call(move |conn| {
                    TableSchema::for_apply(
                        conn,
                        &tables,
                        &gates,
                        crate::WritePolicy::MergeConcurrent,
                    )
                })
                .await
                .map_err(|error| {
                    StorePullError::Database(format!("load synced table schema: {error}"))
                })?,
            )
        };
        let coverage = db.snapshot_coverage_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load snapshot coverage frontier: {error}"))
        })?;
        let mut frontier = db.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load materialized frontier: {error}"))
        })?;
        let mut applied_devices = BTreeSet::new();
        let mut row_changes = Vec::new();
        let mut changesets_applied = 0_u64;
        let mut asset_downloads_failed = false;
        let mut blocked = BTreeMap::new();

        loop {
            let mut progressed = false;
            let keys = candidates.keys().cloned().collect::<Vec<_>>();
            for key in keys {
                let candidate = candidates.get(&key).ok_or_else(|| {
                    StorePullError::Database(
                        "Merge candidate disappeared while evaluating readiness".to_string(),
                    )
                })?;
                let exclusion_freezes = db.store_device_exclusion_freezes().await?;
                let current_frontier = CommitFrontier::from_refs(
                    crate::WritePolicy::MergeConcurrent,
                    frontier.clone(),
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
                let CommitFrontier::MergeConcurrent(current_frontier) = current_frontier else {
                    return Err(StorePullError::Database(
                        "Merge pull produced a Serial materialized frontier".to_string(),
                    ));
                };
                let (_, current_device_state) = db
                    .store_device_state_for_history_cut(&StoreHistoryCut::MergeConcurrent(
                        current_frontier,
                    ))
                    .await?;
                match readiness(
                    db,
                    storage,
                    &root,
                    &coverage,
                    &frontier,
                    &current_device_state,
                    &exclusion_freezes,
                    &candidate.candidate.commit_ref,
                    &candidate.candidate.commit,
                )
                .await
                .map_err(|error| {
                    StorePullError::Database(format!(
                        "evaluate Store commit readiness for {}/{}: {error}",
                        key.0, key.1
                    ))
                })? {
                    Readiness::AlreadyMaterialized => {
                        candidates.remove(&key);
                        blocked.remove(&key);
                        progressed = true;
                    }
                    Readiness::Held(held_position) => {
                        blocked.insert(key, held_position);
                    }
                    Readiness::Ready => {
                        let candidate = candidates.remove(&key).ok_or_else(|| {
                            StorePullError::Database(
                                "ready Merge candidate disappeared before apply".to_string(),
                            )
                        })?;
                        match Box::pin(apply_candidate(
                            db,
                            storage,
                            &root,
                            store_dir,
                            schema.clone(),
                            &candidate,
                            &loaded_predecessor_memberships,
                            identity,
                        ))
                        .await?
                        {
                            ApplyOutcome::Applied(changes) => {
                                let stream_id =
                                    commit_stream_id(&candidate.candidate.commit_ref.coord);
                                frontier.insert(
                                    stream_id.clone(),
                                    candidate.candidate.commit_ref.clone(),
                                );
                                applied_devices.insert(stream_id);
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
                                let held_position =
                                    held_commit(&candidate.candidate.commit_ref, reason);
                                candidates.insert(key.clone(), candidate);
                                blocked.insert(key, held_position);
                            }
                        }
                    }
                }
            }
            if !progressed {
                break;
            }
        }

        held.extend(blocked.into_values());
        held.sort_by(|left, right| {
            (left.coordinate.device_id(), left.coordinate.seq())
                .cmp(&(right.coordinate.device_id(), right.coordinate.seq()))
        });
        let local_blob_cleanup_pending =
            local_cleanup::drain(db, store_dir).await.map_err(|error| {
                StorePullError::Database(format!("drain local blob cleanup intents: {error}"))
            })?;
        let devices_pulled = u64::try_from(applied_devices.len())
            .map_err(|_| StorePullError::Database("pulled device count exceeds u64".to_string()))?;

        Ok(StorePullResult {
            changesets_applied,
            devices_pulled,
            held_positions: held,
            visible_heads,
            serial_head: None,
            row_changes,
            asset_downloads_failed,
            local_blob_cleanup_pending,
            frontier,
        })
    })
}
