//! Causal discovery and atomic materialization for immutable Store commits.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tracing::debug;

use super::membership as membership_ops;
use super::reclaim as store_reclaim;
use super::*;
use crate::blob::decl::BlobDecls;
use crate::blob::local_cleanup;
use crate::changeset::RowChange;
use crate::database::{BlobActivation, Database, DbError, VerifiedMergeMaterialization};
use crate::store_dir::StoreDir;
use crate::sync::apply::{resolve_and_apply_changeset_with_policy_on, ValidatedChangeset};
use crate::sync::audience_package::{AudiencePackage, PackageAudience};
use crate::sync::circle_control::StoreMembershipStateRef;
use crate::sync::conflict::{IncomingTimestampPolicy, TableSchema};
use crate::sync::membership::{MembershipChain, MembershipStatus};
use crate::sync::session::SyncedTable;
use crate::sync::storage::{
    BlobSpoolProtection, ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, StorageError,
    SyncStorage,
};
use crate::sync::store::circle_controls::activation::{
    VerifiedCircleActivations, VerifiedStreamActivationPrefix,
};
use crate::sync::store::device_join;
use crate::sync::store::retained_replay;
use crate::sync::store::StoreError;
use crate::sync::store_commit::{
    head_slot_prefix, ActivatedStoreDeviceRegistrationRef, CirclePackageRef, CommitFrontier,
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinOutcomeBody, DeviceStreamAnchor,
    ObjectHash, OpenedRetainedMergeHistorySummary, OwnerRecoveryCursor, OwnerRecoveryNode,
    OwnerRecoveryNodeRef, OwnerRecoveryPosition, ResolvedStoreDeviceState,
    RetainedStoreDeviceExclusionOutcome, RetainedStoreDeviceExclusionProposal,
    RetainedStoreDeviceOperations, RetainedVerifiedMergeHistorySummary,
    RetainedVerifiedRegistration, StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord,
    StoreDeviceExclusionOutcome, StoreDeviceExclusionProof, StoreDeviceHead,
    StoreDeviceProposalAck, StoreDeviceProposalState, StoreDeviceRegistration,
    StoreDeviceRegistrationActivation, StoreDeviceRegistrationActivationRef,
    StoreDeviceRegistrationOrigin, StoreDeviceRegistrationRef, StoreDeviceStateRef,
    StoreDeviceStatus, StoreHistoryCut, StoreProtocolError, StoreRootRef, VerifiedStoreBatchCommit,
    VerifiedStoreDeviceOperations,
};
use crate::sync::store_objects::{
    load_circle_package, load_device_exclusion_outcome_ref, load_device_exclusion_proposal_ref,
    load_device_join_outcome_ref, load_founder_registration_with_root,
    load_owner_recovery_node_ref, load_owner_signed_device_join_attempt_ref,
    load_reclaim_authorization_ref, load_reclaim_receipt_ref, load_registration_ref_with_root,
    load_store_ack_predecessor, load_store_ack_ref, load_store_package, load_store_protocol_root,
    run_blocking_object_verification, StoreObjectError, VerifiedObject,
};
use crate::sync::{
    causal_grants, circle, gate, hlc, membership, provider, remote_object, session, store_commit,
    store_objects,
};

mod ancestry;
mod circle_packages;
mod device_join_attempt;
mod device_join_cleanup;
mod device_lifecycle_state;
mod device_operations;
mod discovery;
mod history;
mod join_activation;
mod join_bootstrap;
mod join_validation;
mod local_device_operations;
mod materialization;
mod membership_control;
mod model;
mod owner_promotion;
mod provider_access;
mod registration;
mod registration_authority;
mod registration_validation;
mod replay;
mod retained_authority;
mod root_validation;
mod snapshot_authority;
mod snapshot_evidence;
mod support;
mod terminal_authority;
mod terminal_cleanup;

pub use ancestry::StoreCommitVerifier;
pub(crate) use ancestry::{
    commit_position_covers, history_cut_covers, load_device_join_attempt_evidence_ref_with_root,
    load_provider_access_activation, CommitCoverageError, LoadedDeviceJoinAttemptEvidence,
};
pub(crate) use circle_packages::*;
pub(crate) use device_lifecycle_state::*;
pub(crate) use device_operations::*;
pub(crate) use join_activation::*;
pub(crate) use join_validation::*;
pub(crate) use model::{
    commit_stream_id, held_commit, held_dependency, held_package, parse_candidate_circle_package,
    parse_candidate_store_package, Candidate, LoadedCirclePackage, LocalStoreMembership,
    StorePullFuture,
};
pub use model::{
    HeldStoreCoordinate, HeldStorePosition, HeldStorePositionReason, StorePullError,
    StorePullMembershipError, StorePullResult, VerifiedStoreDeviceHead,
};
pub(crate) use registration::*;
pub(crate) use root_validation::*;
pub(crate) use snapshot_evidence::*;
pub(crate) use support::{
    advance_max_updated_at, cache_eager_blobs, download_blobs, load_cycle_membership_with_history,
    local_blob_cleanup_intents, verify_package_blobs, BlobDownload,
};
pub use support::{
    load_cycle_membership, BlobDownloadFailure, BlobDownloadFailureCause, BlobDownloadFailures,
    PullError,
};

pub(crate) use device_join_attempt::load_verified_device_join_attempt;
pub(in crate::sync::store) use device_join_cleanup::verify_device_join_cleanup_activation;
pub(crate) use discovery::*;
pub(crate) use history::*;
#[cfg(test)]
pub(in crate::sync::store) use join_bootstrap::prepare_device_join_bootstrap;
pub(in crate::sync::store) use join_bootstrap::{
    materialize_device_join_activation, verify_attempt_and_prepare_device_join_bootstrap,
};
pub(crate) use local_device_operations::{
    derive_local_post_device_state, load_local_commit_device_operations,
};
pub(crate) use materialization::*;
pub(crate) use membership_control::*;
pub(in crate::sync::store) use owner_promotion::{
    find_request_activation as find_owner_promotion_request_activation,
    verify_acceptance_from_request_activation,
    verify_acceptance_with_history as verify_owner_promotion_acceptance_with_history,
    VerifiedOwnerPromotionRequestActivation,
};
pub(in crate::sync::store) use provider_access::verify_accepted_provider_access_activation;
pub(in crate::sync::store) use registration::RegistrationLoadError;
pub(crate) use registration_authority::{
    load_merge_predecessor_membership_with_history,
    load_merge_predecessor_membership_with_verified_activations, verify_merge_membership_state_ref,
};
use registration_validation::load_merge_commit_registrations;
use replay::verified_terminal_merge_retractions;
pub(crate) use replay::{install_circle_bootstrap_image_on, replay_retained_merge_projection_on};
pub(crate) use retained_authority::*;
#[cfg(test)]
pub(in crate::sync::store) use snapshot_authority::verify_snapshots_for_acknowledgement;
pub(in crate::sync::store) use snapshot_authority::{
    verify_snapshot_stability_with_history, verify_snapshots_for_acknowledgement_with_history,
};
pub(super) use terminal_authority::*;
pub(crate) use terminal_cleanup::cleanup_circle_operation_candidate_with_history;
#[cfg(test)]
pub(crate) use terminal_cleanup::cleanup_merge_candidate;
pub(crate) use terminal_cleanup::cleanup_merge_candidate_with_history;
pub(super) use terminal_cleanup::resume_merge_retraction_cleanups;

#[cfg(test)]
mod tests;

#[derive(Clone)]
struct MergeCandidate {
    candidate: Candidate,
    activation_head: StoreDeviceHead,
    activation_head_object: ExactObjectRef,
    predecessor_membership: MembershipChain,
    device_operations: VerifiedStoreDeviceOperations,
    membership_control: Option<VerifiedCircleActivations>,
    membership_prefix: VerifiedMergeMembershipPrefix,
}

struct LoadedMergePredecessorMemberships {
    by_commit: BTreeMap<StoreBatchCommitRef, MembershipChain>,
}

#[derive(Debug)]
#[doc(hidden)]
pub struct StorePullExecution {
    pub result: StorePullResult,
    pub membership: MembershipChain,
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

impl AuthorizedStore<'_> {
    pub(crate) async fn pull(
        &mut self,
        store_dir: &StoreDir,
        identity: &UserKeypair,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<StorePullResult, SyncCycleFailure> {
        let authority = self.operation_authority();
        let execution = pull_store_commits_with_history(
            authority.database,
            authority.database.sqlite().synced_tables(),
            authority.history_verifier,
            store_dir,
            &*authority.membership,
            Some(identity),
            routing_encryption,
        )
        .await
        .map_err(|error| SyncCycleFailure::operation("pull Store commits", error))?;
        *authority.membership = execution.membership;
        Ok(execution.result)
    }

    pub(crate) async fn should_stop_before_pull(&self) -> Result<bool, SyncCycleFailure> {
        Ok(false)
    }
}

#[allow(clippy::too_many_arguments)]
#[doc(hidden)]
pub fn pull_store_commits<'a>(
    database: &'a StoreDatabase,
    tables: &'a [crate::sync::session::SyncedTable],
    storage: &'a dyn SyncStorage,
    store_root_hash: crate::sync::store_commit::ObjectHash,
    store_dir: &'a StoreDir,
    membership: &'a crate::sync::membership::MembershipChain,
    identity: Option<&'a UserKeypair>,
    routing_encryption: Option<&'a crate::encryption::EncryptionService>,
) -> Pin<Box<dyn Future<Output = Result<StorePullExecution, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let root = required_pull_root(database, store_root_hash).await?;
        let mut history_verifier = MergeHistoryVerifier::new(storage, &root).await?;
        pull_store_commits_with_history(
            database,
            tables,
            &mut history_verifier,
            store_dir,
            membership,
            identity,
            routing_encryption,
        )
        .await
    })
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn pull_store_commits_with_history<'a>(
    database: &'a StoreDatabase,
    tables: &'a [crate::sync::session::SyncedTable],
    history_verifier: &'a mut MergeHistoryVerifier<'_>,
    store_dir: &'a StoreDir,
    membership: &'a crate::sync::membership::MembershipChain,
    identity: Option<&'a UserKeypair>,
    routing_encryption: Option<&'a crate::encryption::EncryptionService>,
) -> Pin<Box<dyn Future<Output = Result<StorePullExecution, StorePullError>> + Send + 'a>> {
    Box::pin(async move {
        let db = database.sqlite();
        let storage = history_verifier.storage();
        let store_root_hash = history_verifier.root().store_root_hash;
        membership
            .ensure_resolved()
            .map_err(StorePullMembershipError::State)
            .map_err(StorePullError::Membership)?;
        let routing_key = if db.gates().has_scoped_graph() {
            let encryption = routing_encryption.ok_or_else(|| {
                StorePullError::Database(
                    "scoped Store pull requires row-routing encryption".to_string(),
                )
            })?;
            Some(
                crate::sync::circle::derive_row_routing_key(encryption, store_root_hash).map_err(
                    |error| StorePullError::Database(format!("derive row routing key: {error}")),
                )?,
            )
        } else {
            None
        };
        let retained_refs = database.retained_merge_materialization_refs().await?;
        history_verifier.verify_refs(retained_refs).await?;
        let retained_commit_proofs = history_verifier
            .history()
            .commits
            .iter()
            .map(|(reference, verified)| (reference.clone(), verified.verified.clone()))
            .collect();
        let retained = Box::pin(
            database.retained_merge_replay_inputs_with_verified_commits(retained_commit_proofs),
        )
        .await?;
        resume_merge_retraction_cleanups(database, history_verifier).await?;

        let local_frontier = database.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load discovery device-state frontier: {error}"))
        })?;
        let local_frontier = local_frontier
            .into_values()
            .map(|reference| (reference.coord.stream_id, reference))
            .collect::<BTreeMap<_, _>>();
        let (_, discovery_device_state) = database
            .store_device_state_for_history_cut(&StoreHistoryCut(local_frontier))
            .await?;

        let mut active = load_active_merge_registrations(database, history_verifier)
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load active Merge registrations: {error}"))
            })?;
        for recovered in discover_merge_owner_recoveries(history_verifier, membership).await? {
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
                history_verifier,
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
                if let Some(materialized) = database
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
                if let Err(error) = history_verifier.verify_refs([commit_ref.clone()]).await {
                    let reason = match error {
                        StorePullError::Object(error) => held_object_error(error),
                        error => HeldStorePositionReason::InvalidObject(error.to_string()),
                    };
                    held.push(held_commit(&commit_ref, reason));
                    continue;
                }
                let verified = history_verifier
                    .history()
                    .commits
                    .get(&commit_ref)
                    .ok_or_else(|| {
                        StorePullError::Database(
                            "Merge candidate is absent from its operation-verified history"
                                .to_string(),
                        )
                    })?;
                if verified.verified.value() != &commit {
                    held.push(held_commit(
                        &commit_ref,
                        HeldStorePositionReason::InvalidObject(
                            "Merge candidate differs from its operation-verified history"
                                .to_string(),
                        ),
                    ));
                    continue;
                }
                let predecessor_membership = verified.predecessor_membership.clone();
                let registrations = verified.registrations.clone();
                let device_operations = verified.operations.clone();
                let membership_control = verified
                    .membership_control
                    .as_ref()
                    .map(|control| control.activations.clone());
                let membership_prefix = verified_merge_membership_prefix(
                    &history_verifier.history().commits,
                    commit_predecessor_references(&commit),
                )?;
                let package = match load_store_package(storage, &verified.verified).await {
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
                candidates.insert(
                    key,
                    MergeCandidate {
                        activation_head,
                        activation_head_object: activation_head_ref.object,
                        candidate: Candidate {
                            verified: verified.verified.clone(),
                            package,
                            registrations,
                        },
                        predecessor_membership,
                        device_operations,
                        membership_control,
                        membership_prefix,
                    },
                );
            }
        }
        let mut loaded_predecessor_memberships = BTreeMap::new();
        for materialization in retained {
            if materialization.commit().membership_authority.is_none() {
                continue;
            }
            let membership = Box::pin(load_merge_predecessor_membership_with_history(
                history_verifier,
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
                candidate.candidate.commit_ref().clone(),
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
                db.call(move |conn| TableSchema::for_apply(conn, &tables, &gates))
                    .await
                    .map_err(|error| {
                        StorePullError::Database(format!("load synced table schema: {error}"))
                    })?,
            )
        };
        let coverage = database
            .snapshot_coverage_frontier()
            .await
            .map_err(|error| {
                StorePullError::Database(format!("load snapshot coverage frontier: {error}"))
            })?;
        let mut frontier = database.materialized_frontier().await.map_err(|error| {
            StorePullError::Database(format!("load materialized frontier: {error}"))
        })?;
        let mut applied_devices = BTreeSet::new();
        let mut row_changes = Vec::new();
        let mut changesets_applied = 0_u64;
        let mut asset_downloads_failed = false;
        let mut blocked = BTreeMap::new();
        let mut latest_membership = membership.clone();

        loop {
            let mut progressed = false;
            let mut keys = candidates.keys().cloned().collect::<Vec<_>>();
            keys.sort_by_key(|key| {
                (
                    candidates.get(key).is_none_or(|candidate| {
                        candidate.candidate.commit().circle_controls().is_empty()
                    }),
                    key.clone(),
                )
            });
            for key in keys {
                let candidate = candidates.get(&key).ok_or_else(|| {
                    StorePullError::Database(
                        "Merge candidate disappeared while evaluating readiness".to_string(),
                    )
                })?;
                let exclusion_freezes = database.store_device_exclusion_freezes().await?;
                let current_frontier = CommitFrontier::from_refs(frontier.clone())
                    .map_err(|error| StorePullError::Database(error.to_string()))?;
                let (_, current_device_state) = database
                    .store_device_state_for_history_cut(&StoreHistoryCut(current_frontier.0))
                    .await?;
                match readiness(
                    database,
                    history_verifier.commit_verifier(),
                    &coverage,
                    &frontier,
                    &current_device_state,
                    &exclusion_freezes,
                    candidate.candidate.commit_ref(),
                    candidate.candidate.commit(),
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
                            database,
                            history_verifier,
                            store_dir,
                            schema.clone(),
                            &candidate,
                            &loaded_predecessor_memberships,
                            identity,
                            &mut latest_membership,
                            routing_key.as_ref(),
                        ))
                        .await?
                        {
                            ApplyOutcome::Applied(changes) => {
                                let stream_id =
                                    commit_stream_id(&candidate.candidate.commit_ref().coord);
                                frontier.insert(
                                    stream_id.clone(),
                                    candidate.candidate.commit_ref().clone(),
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
                                    held_commit(candidate.candidate.commit_ref(), reason);
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

        Ok(StorePullExecution {
            result: StorePullResult {
                changesets_applied,
                devices_pulled,
                held_positions: held,
                visible_heads,
                row_changes,
                asset_downloads_failed,
                local_blob_cleanup_pending,
                frontier,
            },
            membership: latest_membership,
        })
    })
}
