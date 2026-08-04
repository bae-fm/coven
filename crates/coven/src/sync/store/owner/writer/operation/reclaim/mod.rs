//! Proof-gated deletion of exact Store packages covered by exact authority.

use std::sync::Arc;

use crate::database::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, ReclaimCommitActivation, StoreDatabase,
};
use crate::protocol::circle::{
    CircleControlCoord, CircleControlState, CircleEpochOrigin, CircleId,
};
use crate::protocol::objects::StoreObjectError;
use crate::protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use crate::protocol::reclaim::*;
use crate::protocol::store_commit::{
    snapshot_image_semantic_prefix, CommitFrontier, ObjectHash, StoreAckRef, StoreBatchCommitRef,
    StoreRootRef, StoreSnapshotLocator, VerifiedStoreBatchCommit,
};
use crate::storage::SyncStorage;
use crate::sync::store::owner::history::{
    CircleSnapshotStream, ReclaimHistory, SelectedCircleSnapshot,
};
use crate::sync::store::AuthorizedWriterOperation;

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct StoreReclaimResult {
    pub packages_deleted: u64,
    pub physical_copies_deleted: u64,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreReclaimError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error(transparent)]
    Database(#[from] crate::database::DbError),
    #[error(transparent)]
    Outbound(#[from] super::StoreError),
    #[error("Store reclaim journal: {0}")]
    Journal(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("no authorized complete Store snapshot is available for reclamation")]
    NoSnapshot,
    #[error("snapshot authorization history is invalid: {0}")]
    Authorization(String),
    #[error(
        "active Store device {device_id:?} for member {member:?} has no exact acknowledgement"
    )]
    MissingAcknowledgement { member: String, device_id: String },
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
    #[error("deleting the exact object activated by {activation} failed: {source}")]
    Delete {
        activation: ObjectHash,
        #[source]
        source: StorageError,
    },
}

impl From<super::pull::CommitCoverageError> for StoreReclaimError {
    fn from(error: super::pull::CommitCoverageError) -> Self {
        match error {
            super::pull::CommitCoverageError::Object(error) => Self::Object(error),
            super::pull::CommitCoverageError::MissingAncestry { commit_hash } => {
                Self::MissingAncestry { commit_hash }
            }
        }
    }
}

impl From<super::pull::StorePullError> for StoreReclaimError {
    fn from(error: super::pull::StorePullError) -> Self {
        match error {
            super::pull::StorePullError::Object(error) => Self::Object(error),
            super::pull::StorePullError::Storage(error) => Self::Storage(error),
            error => Self::Authorization(error.to_string()),
        }
    }
}

pub(super) struct AuthorizedReclaim<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: Arc<dyn SyncStorage>,
    root: StoreRootRef,
    membership: crate::protocol::membership::MembershipChain,
}

impl<'operation, 'storage> AuthorizedReclaim<'operation, 'storage> {
    pub(super) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: Arc<dyn SyncStorage>,
        root: StoreRootRef,
        membership: crate::protocol::membership::MembershipChain,
    ) -> Self {
        Self {
            writer,
            database,
            storage,
            root,
            membership,
        }
    }

    fn history(&mut self) -> ReclaimHistory<'_, 'storage> {
        self.writer.reclaim_history()
    }

    pub(super) async fn run(&mut self) -> Result<StoreReclaimResult, StoreReclaimError> {
        let database = self.database.clone();
        let membership = self.membership.clone();
        let mut packages_deleted = Box::pin(self.resume_operations()).await?;
        if !self.writer.is_current_owner(&membership) {
            return Ok(StoreReclaimResult {
                packages_deleted,
                physical_copies_deleted: packages_deleted,
            });
        }
        let registrations = database
            .activated_store_device_registration_records()
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        // A missing or unstable Store snapshot leaves Store packages uncovered but must
        // not block Circle package reclamation, which carries its own Circle coverage.
        let store_targets = match Box::pin(self.choose_snapshot(&registrations)).await {
            Ok(snapshot) => self
                .history()
                .store_package_targets(&snapshot.snapshot.meta.coverage)
                .await
                .map_err(StoreReclaimError::from)?
                .into_iter()
                .map(|(commit, package)| (commit, package, snapshot.clone()))
                .collect::<Vec<_>>(),
            Err(
                StoreReclaimError::NoSnapshot | StoreReclaimError::MissingAcknowledgement { .. },
            ) => Vec::new(),
            Err(error) => return Err(error),
        };
        for (commit, package, snapshot) in store_targets {
            if database
                .store_package_is_retained_for_replay(
                    self.root.clone(),
                    package.clone(),
                    commit.clone(),
                )
                .await?
            {
                continue;
            }
            Box::pin(self.prepare_authorization(ReclaimClaim::StorePackage(
                StorePackageReclaimClaim {
                    target: StorePackageReclaimTarget {
                        package,
                        activation: commit,
                    },
                    covering_snapshot: StoreSnapshotLocator {
                        author_registration: snapshot.snapshot.meta.author_registration.clone(),
                        snapshot: snapshot.snapshot.reference.clone(),
                    },
                    acknowledgements: snapshot.acknowledgements.clone(),
                },
            )))
            .await?;
        }
        Box::pin(self.prepare_circle_authorizations(&registrations)).await?;
        Box::pin(self.prepare_audience_blob_authorizations()).await?;
        packages_deleted = packages_deleted
            .checked_add(Box::pin(self.resume_operations()).await?)
            .ok_or_else(|| {
                StoreReclaimError::Authorization("reclaimed package count exceeded u64".to_string())
            })?;
        Ok(StoreReclaimResult {
            packages_deleted,
            physical_copies_deleted: packages_deleted,
        })
    }
}

/// Authorize deletion of every Circle package the Circle's current epoch cutoff
/// excludes. A package addressed to a closed epoch whose activating commit the
/// close cutoff does not accept never materializes on any device — it is invalid
/// by construction — so it is eligible once the successor epoch activated, with no
/// snapshot coverage or acknowledgement evidence required. Enumerated from this
/// device's accepted history rather than a snapshot cut, because such a package is
/// by definition outside every snapshot's coverage.
#[allow(clippy::too_many_arguments)]
impl AuthorizedReclaim<'_, '_> {
    async fn prepare_beyond_cutoff_circle_authorizations(
        &mut self,
        circle_id: CircleId,
        current_control: &CircleControlCoord,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let successor = database
            .verified_circle_activation(root.clone(), circle_id, current_control.clone())
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} current control is not a retained activation"
                ))
            })?;
        // Only a control that closed a predecessor epoch carries a cutoff; a Circle
        // whose current epoch closed nothing has no beyond-cutoff package to enumerate.
        if !matches!(
            successor.control.value.state(),
            CircleControlState::ActiveEpoch(active)
                if matches!(active.common.origin, CircleEpochOrigin::Closed { .. })
        ) {
            return Ok(());
        }
        let frontier = CommitFrontier::from_refs(database.materialized_frontier().await?)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let epochs = database.circle_replay_epoch_index(root.clone()).await?;
        let targets = self
            .history()
            .circle_package_targets(circle_id, &frontier)
            .await
            .map_err(StoreReclaimError::from)?;
        for (commit, package) in targets {
            // `permits` is the same predicate the pull path applies; a package it
            // accepts is live history. A package whose control it cannot resolve, or
            // that conflicts with the cutoff, errors rather than being reclaimed.
            if epochs
                .permits(&commit, circle_id, &package.control)
                .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
            {
                continue;
            }
            if database
                .circle_package_is_retained_for_replay(
                    root.clone(),
                    package.clone(),
                    commit.clone(),
                )
                .await?
            {
                continue;
            }
            Box::pin(self.prepare_authorization(ReclaimClaim::CirclePackage(
                CirclePackageReclaimClaim::BeyondEpochCutoff(CirclePackageBeyondCutoffClaim {
                    target: CirclePackageReclaimTarget {
                        package,
                        activation: commit,
                    },
                    successor_control: current_control.clone(),
                }),
            )))
            .await?;
        }
        Ok(())
    }
}

/// The package in one commit that could have published a blob of this audience.
/// The blob's own locator names its audience, and a commit carries at most one
/// package per audience, so the audience selects the package outright.
fn audience_blob_binding_package(
    commit: &crate::protocol::store_commit::StoreBatchCommit,
    audience: crate::blob::locator::RemoteAudience,
) -> Option<AudienceBlobBindingPackage> {
    match audience {
        crate::blob::locator::RemoteAudience::Store => commit
            .store_package()
            .cloned()
            .map(AudienceBlobBindingPackage::Store),
        crate::blob::locator::RemoteAudience::Circle(circle_id) => commit
            .circle_packages()
            .iter()
            .find(|package| package.circle_id == circle_id)
            .cloned()
            .map(AudienceBlobBindingPackage::Circle),
    }
}

/// Authorize deletion of every row blob no live row still binds in its audience.
/// Moving a row to another audience republishes its blob under a new locator and
/// drops the old binding, stranding the source ciphertext; nothing else ever
/// deletes it. The same orphan test the member-signed tombstone path applies
/// decides eligibility, and an image that still pins the blob holds it back.
impl AuthorizedReclaim<'_, '_> {
    async fn prepare_audience_blob_authorizations(&mut self) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        for (blob, owners) in database.stored_blob_reclaim_candidates().await? {
            if !database.stored_blob_is_row_orphaned(blob.clone()).await? {
                continue;
            }
            if database
                .audience_blob_is_retained_for_replay(blob.clone())
                .await?
            {
                continue;
            }
            // Which owning commit's package carries the binding is the one thing no
            // local state records — the audience picks the package within a commit, but
            // not which commit. Probe only that dimension.
            let mut binding = None;
            for owner in &owners {
                let commit = self
                    .history()
                    .load_ref(owner)
                    .await
                    .map_err(StoreReclaimError::from)?;
                if let Some(package) =
                    audience_blob_binding_package(commit.value(), blob.locator().audience())
                {
                    binding = Some((package, owner.clone()));
                    break;
                }
            }
            let Some((package, activation)) = binding else {
                tracing::debug!(
                    blob = %crate::protocol::remote_object::remote_object_id(blob.object()),
                    "skip orphaned blob whose owning commits name no package for its audience",
                );
                continue;
            };
            let target = AudienceBlobReclaimTarget {
                blob,
                package,
                activation,
            };
            Box::pin(self.prepare_authorization(ReclaimClaim::AudienceBlob(
                AudienceBlobReclaimClaim { target },
            )))
            .await?;
        }
        Ok(())
    }
}

/// Authorize deletion of every Circle snapshot image a later generation of the
/// same device's stream has superseded: that later generation's cut strictly
/// dominates the reclaimed one and every device holding active Circle access has
/// acknowledged it, so no reader will ever install from the older image again.
/// Only images are enumerated — the metadata chain is what a reader walks to find
/// any generation at all, so it is never a target.
#[allow(clippy::too_many_arguments)]
impl AuthorizedReclaim<'_, '_> {
    async fn prepare_circle_snapshot_image_authorizations(
        &mut self,
        circle_id: CircleId,
        streams: &[CircleSnapshotStream],
        stable: &[SelectedCircleSnapshot],
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        for stream in streams {
            for (reference, meta) in &stream.generations {
                let Some(superseding) = stable.iter().find(|candidate| {
                    candidate.author_registration == stream.author_registration
                        && candidate.reference.generation > reference.generation
                        && snapshot_supersedes_seed(
                            &candidate.meta.bootstrap.coverage,
                            &meta.bootstrap.coverage,
                        )
                }) else {
                    continue;
                };
                let target = CircleSnapshotImageReclaimTarget {
                    circle_id,
                    snapshot_author: stream.author_registration.clone(),
                    control: meta.control.clone(),
                    snapshot: reference.clone(),
                    image: meta.bootstrap.image.clone(),
                };
                if database
                    .circle_image_is_retained_for_replay(circle_id, target.image.clone())
                    .await?
                {
                    continue;
                }
                Box::pin(
                    self.prepare_authorization(ReclaimClaim::CircleSnapshotImage(
                        CircleSnapshotImageReclaimClaim {
                            target,
                            superseding: superseding.reference.clone(),
                        },
                    )),
                )
                .await?;
            }
        }
        Ok(())
    }
}

/// Enumerate every reclaimable Circle package: for each Circle this device holds
/// active access to, select the maximal acknowledgement-stable Circle snapshot,
/// and authorize each package its cut covers that is not still a retained replay
/// input. Mirrors the Store package pass over Circle coverage evidence.
#[allow(clippy::too_many_arguments)]
impl AuthorizedReclaim<'_, '_> {
    async fn prepare_circle_authorizations(
        &mut self,
        registrations: &[crate::protocol::store_commit::ReferencedStoreDeviceRegistration],
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        for input in database.circle_acknowledgement_publication_inputs().await? {
            let circle_id = input.circle_id;
            let control = input.control;
            // A package beyond its epoch's accepted cutoff never materializes anywhere,
            // so it needs no snapshot coverage and is enumerated whether or not this
            // Circle has a stable snapshot.
            Box::pin(self.prepare_beyond_cutoff_circle_authorizations(circle_id, &control)).await?;
            // Both remaining passes read the same evidence: every device's snapshot
            // stream and which of its generations every active-access device has
            // acknowledged. Read it once.
            let streams = self
                .history()
                .load_circle_snapshot_streams(circle_id, &control, registrations)
                .await?;
            let stable = self
                .history()
                .stable_circle_snapshots(circle_id, &streams)
                .await?;
            let selected = maximal_stable_circle_snapshot(&stable);
            Box::pin(self.prepare_circle_bootstrap_authorizations(circle_id, &control, selected))
                .await?;
            // A superseded snapshot generation's image is reclaimable on its own
            // stream's evidence, independent of which snapshot covers the packages.
            Box::pin(
                self.prepare_circle_snapshot_image_authorizations(circle_id, &streams, &stable),
            )
            .await?;
            let Some(selected) = selected else {
                continue;
            };
            let targets = self
                .history()
                .circle_package_targets(circle_id, &selected.meta.bootstrap.coverage)
                .await
                .map_err(StoreReclaimError::from)?;
            for (commit, package) in targets {
                if database
                    .circle_package_is_retained_for_replay(
                        self.root.clone(),
                        package.clone(),
                        commit.clone(),
                    )
                    .await?
                {
                    continue;
                }
                Box::pin(self.prepare_authorization(ReclaimClaim::CirclePackage(
                    CirclePackageReclaimClaim::SnapshotCovered(
                        CirclePackageSnapshotCoverageClaim {
                            target: CirclePackageReclaimTarget {
                                package,
                                activation: commit,
                            },
                            covering_snapshot: CircleSnapshotLocator {
                                author_registration: selected.author_registration.clone(),
                                circle_id,
                                control: selected.meta.control.clone(),
                                snapshot: selected.reference.clone(),
                            },
                            acknowledgements: selected.acknowledgements.clone(),
                        },
                    ),
                )))
                .await?;
            }
        }
        Ok(())
    }
}

/// The maximal stable Circle snapshot: the one whose cut no other stable
/// snapshot strictly dominates.
fn maximal_stable_circle_snapshot(
    stable: &[SelectedCircleSnapshot],
) -> Option<&SelectedCircleSnapshot> {
    let coverages: Vec<&CommitFrontier> = stable
        .iter()
        .map(|candidate| &candidate.meta.bootstrap.coverage)
        .collect();
    stable
        .iter()
        .filter(|candidate| {
            !coverages.iter().any(|other| {
                super::snapshot::coverage_dominates(other, &candidate.meta.bootstrap.coverage)
            })
        })
        .max_by_key(|candidate| candidate.reference.snapshot_hash)
}

/// A stable Circle snapshot strictly supersedes a bootstrap seed when its cut
/// covers the seed and is not equal to it — the recipient has moved to a later
/// sufficient snapshot, not merely re-published coverage at the seed's own cut.
/// The strict inequality is load-bearing: a snapshot whose cut equals the seed
/// leaves the recipient exactly at its bootstrap and must not reclaim it.
fn snapshot_supersedes_seed(cut: &CommitFrontier, seed: &CommitFrontier) -> bool {
    cut.covers(seed) && cut != seed
}

/// Authorize deletion of every Circle bootstrap image no recipient still needs.
/// Each device's activated acknowledgement names the exact coverage its
/// projection was seeded from; the seed image is superseded either when a stable
/// Circle snapshot's cut strictly dominates it — the "later sufficient snapshot"
/// every active-access device acknowledged — while the recipient still holds
/// access, or when the recipient's owner lost Circle authority under a successor
/// control. The coverage the Owner deletes is taken from the recipient's own
/// signed acknowledgement, never fabricated.
#[allow(clippy::too_many_arguments)]
impl AuthorizedReclaim<'_, '_> {
    async fn prepare_circle_bootstrap_authorizations(
        &mut self,
        circle_id: CircleId,
        current_control: &CircleControlCoord,
        selected: Option<&SelectedCircleSnapshot>,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let roster = database.circle_current_roster_members(circle_id).await?;
        // The maximal acknowledgement-stable Circle snapshot cut, if any. A seed a
        // still-active recipient holds is superseded only when this cut strictly
        // dominates it — a later sufficient snapshot every active device acknowledged.
        let stable_cut = selected.map(|selected| &selected.meta.bootstrap.coverage);
        for acknowledgement in database.activated_circle_acks(circle_id).await? {
            let ack = match self
                .history()
                .load_circle_acknowledgement(&acknowledgement)
                .await
            {
                Ok(ack) => ack,
                Err(error) => {
                    tracing::debug!(
                        circle_id = %circle_id,
                        "skip Circle acknowledgement for bootstrap reclaim: {error}"
                    );
                    continue;
                }
            };
            let Some(coverage) = ack.seeded_from.clone() else {
                // A founder/source device never seeded from an image — nothing to reclaim.
                continue;
            };
            let recipient = database
                .activated_store_device_registration(acknowledgement.registration.clone())
                .await?;
            let recipient_active = roster.contains(&recipient.value().author_pubkey);
            let seed = &coverage.bootstrap.coverage;
            let superseded_by_snapshot = stable_cut
                .as_ref()
                .is_some_and(|cut| snapshot_supersedes_seed(cut, seed));
            let proof = if recipient_active {
                if superseded_by_snapshot {
                    CircleBootstrapReclaimProof::RecipientCoverage {
                        acknowledgement: acknowledgement.clone(),
                    }
                } else {
                    // No later sufficient snapshot supersedes the recipient's live seed.
                    continue;
                }
            } else if database
                .circle_control_covers_strictly(
                    root.clone(),
                    circle_id,
                    current_control,
                    &coverage.control,
                )
                .await?
            {
                CircleBootstrapReclaimProof::LostAuthority {
                    acknowledgement: acknowledgement.clone(),
                    successor_control: current_control.clone(),
                }
            } else {
                continue;
            };
            let target = CircleBootstrapImageReclaimTarget { coverage };
            if database
                .circle_bootstrap_image_is_retained_for_replay(target.coverage.clone())
                .await?
            {
                continue;
            }
            Box::pin(
                self.prepare_authorization(ReclaimClaim::CircleBootstrapImage(
                    CircleBootstrapImageReclaimClaim { target, proof },
                )),
            )
            .await?;
        }
        Ok(())
    }
}

impl AuthorizedReclaim<'_, '_> {
    async fn prepare_authorization(
        &mut self,
        claim: ReclaimClaim,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let target = claim.target();
        if database
            .store_reclaim_operations()
            .await?
            .iter()
            .any(|operation| operation.authorization().target() == &target)
        {
            return Ok(());
        }
        let plan = self.writer.prepare_plan().await?;
        let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
            StoreReclaimError::Authorization(
                "Store reclaim authorization requires an active Owner grant".to_string(),
            )
        })?;
        let evidence = plan
            .sign_reclaim_evidence(claim)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        self.verify_evidence(&evidence).await?;
        let evidence_context = ProtocolObjectContext::store_encrypted(
            root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimEvidence,
        );
        let evidence_prefix = reclaim_evidence_semantic_prefix(evidence.evidence_hash());
        let evidence_slot = self
            .storage
            .allocate_protocol_slot(&evidence_context, &evidence_prefix, ".json")
            .await?;
        let evidence_prepared = self.storage.prepare_protocol_object(
            &evidence_context,
            evidence_slot,
            &evidence_prefix,
            evidence.to_bytes(),
        )?;
        let evidence_ref =
            ReclaimEvidenceRef::from_evidence(&evidence, evidence_prepared.reference().clone());
        let authorization = plan.sign_reclaim_authorization(
            evidence.claim.target(),
            evidence_ref.clone(),
            StoreReclaimAuthority {
                membership: plan.membership_state().clone(),
                owner_grant,
            },
        );
        let authorization_context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimAuthorization,
        );
        let authorization_prefix =
            reclaim_authorization_semantic_prefix(authorization.authorization_hash());
        let authorization_slot = self
            .storage
            .allocate_protocol_slot(&authorization_context, &authorization_prefix, ".json")
            .await?;
        let authorization_prepared = self.storage.prepare_protocol_object(
            &authorization_context,
            authorization_slot,
            &authorization_prefix,
            authorization.to_bytes(),
        )?;
        let authorization_ref = ReclaimAuthorizationRef::from_authorization(
            &authorization,
            authorization_prepared.reference().clone(),
        );
        let candidate = self
            .writer
            .prepare_candidate(
                plan,
                super::operations::StoreOperationBatch::ReclaimAuthorization(Box::new(
                    authorization_ref.clone(),
                )),
            )
            .await?;
        let operation = DurableStoreReclaimOperation::AuthorizationCandidate {
            object: Box::new(DurableStoreReclaimObject::Authorization {
                evidence_ref,
                evidence,
                evidence_prepared,
                authorization_ref,
                authorization,
                authorization_prepared,
            }),
            candidate: Box::new(candidate),
        };
        Box::pin(database.begin_store_reclaim_operation(operation)).await?;
        Ok(())
    }
}

impl AuthorizedReclaim<'_, '_> {
    async fn resume_operations(&mut self) -> Result<u64, StoreReclaimError> {
        let database = self.database.clone();
        let mut completed = 0_u64;
        loop {
            let operations = database.store_reclaim_operations().await?;
            let mut progressed = false;
            for operation in operations {
                match &operation {
                    DurableStoreReclaimOperation::AuthorizationCandidate { .. }
                    | DurableStoreReclaimOperation::ReceiptCandidate { .. } => {
                        Box::pin(self.drive_candidate(operation)).await?;
                        progressed = true;
                    }
                    DurableStoreReclaimOperation::AuthorizationReplacing { .. }
                    | DurableStoreReclaimOperation::ReceiptReplacing { .. } => {
                        Box::pin(self.finish_candidate_replacement(operation)).await?;
                        progressed = true;
                    }
                    DurableStoreReclaimOperation::Authorized { .. } => {
                        Box::pin(self.execute_delete(operation)).await?;
                        completed = completed.checked_add(1).ok_or_else(|| {
                            StoreReclaimError::Authorization(
                                "reclaimed package count exceeded u64".to_string(),
                            )
                        })?;
                        progressed = true;
                    }
                    DurableStoreReclaimOperation::AbsentVerified { .. } => {
                        Box::pin(self.prepare_receipt(operation)).await?;
                        progressed = true;
                    }
                    DurableStoreReclaimOperation::Completed { .. } => {}
                }
            }
            if !progressed {
                return Ok(completed);
            }
        }
    }
}

impl AuthorizedReclaim<'_, '_> {
    async fn execute_delete(
        &mut self,
        operation: DurableStoreReclaimOperation,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        let DurableStoreReclaimOperation::Authorized {
            authorization,
            activation,
        } = &operation
        else {
            return Err(StoreReclaimError::Authorization(
                "only an authorized reclaim can delete its target".to_string(),
            ));
        };
        let target = self.verify_authorized(authorization, activation).await?;
        if self.target_is_retained(&target).await? {
            return Err(StoreReclaimError::Authorization(
                "reclaim target remains retained for accepted replay".to_string(),
            ));
        }
        // A row blob has no protocol domain: it is addressed by its locator, so its
        // exact delete goes through the blob primitive rather than the protocol one.
        match &target {
            ReclaimTarget::AudienceBlob(blob) => self.storage.delete_blob_object(&blob.blob).await,
            _ => self.storage.delete_protocol_object(target.object()).await,
        }
        .map_err(|source| StoreReclaimError::Delete {
            activation: target.activation().object().stored_hash(),
            source,
        })?;
        self.verify_target_absent(&target).await?;
        database
            .mark_store_reclaim_target_absent(operation, target)
            .await?;
        Ok(())
    }
}

/// Whether the reclaim target is still an accepted-replay input for its audience.
/// The authority spine never becomes eligible while replay names it.
impl AuthorizedReclaim<'_, '_> {
    async fn target_is_retained(&self, target: &ReclaimTarget) -> Result<bool, StoreReclaimError> {
        let database = &self.database;
        let root = &self.root;
        match target {
            ReclaimTarget::StorePackage(target) => Ok(database
                .store_package_is_retained_for_replay(
                    root.clone(),
                    target.package.clone(),
                    target.activation.clone(),
                )
                .await?),
            ReclaimTarget::CirclePackage(target) => Ok(database
                .circle_package_is_retained_for_replay(
                    root.clone(),
                    target.package.clone(),
                    target.activation.clone(),
                )
                .await?),
            ReclaimTarget::CircleBootstrapImage(target) => Ok(database
                .circle_bootstrap_image_is_retained_for_replay(target.coverage.clone())
                .await?),
            ReclaimTarget::CircleSnapshotImage(target) => Ok(database
                .circle_image_is_retained_for_replay(target.circle_id, target.image.clone())
                .await?),
            ReclaimTarget::AudienceBlob(target) => Ok(database
                .audience_blob_is_retained_for_replay(target.blob.clone())
                .await?),
        }
    }
}

impl AuthorizedReclaim<'_, '_> {
    async fn verify_authorized(
        &mut self,
        authorization_ref: &ReclaimAuthorizationRef,
        activation: &ReclaimCommitActivation,
    ) -> Result<ReclaimTarget, StoreReclaimError> {
        let opened = self
            .history()
            .load_reclaim_authorization(authorization_ref)
            .await?;
        self.verify_authorization_activation(authorization_ref, activation)
            .await?;
        self.verify_evidence(&opened.evidence.value).await
    }
}

impl AuthorizedReclaim<'_, '_> {
    async fn verify_authorization_activation(
        &mut self,
        authorization: &ReclaimAuthorizationRef,
        activation: &ReclaimCommitActivation,
    ) -> Result<(), StoreReclaimError> {
        activation
            .validate()
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let commit_ref = activation.commit();
        let verified_commit = self
            .history()
            .load_ref(commit_ref)
            .await
            .map_err(StoreReclaimError::from)?;
        let commit_value = verified_commit.value();
        let author = verified_commit.author();
        if commit_value.reclaim_authorization() != Some(authorization) {
            return Err(StoreReclaimError::Authorization(
                "reclaim activation commit names another authorization".to_string(),
            ));
        }
        let commit = &activation.commit;
        let head = &activation.head;
        let opened = self.history().load_head(head, author, commit).await?;
        if opened.value.commit != *commit {
            return Err(StoreReclaimError::Authorization(
                "reclaim head activates another commit".to_string(),
            ));
        }
        let (_, accepted_head) = self
            .history()
            .exact_next_announcement_slot(
                &commit_value.author_registration,
                author,
                Some(&verified_commit),
            )
            .await?;
        if accepted_head.as_ref() != Some(head) {
            return Err(StoreReclaimError::Authorization(
                "reclaim activation head is not the exact accepted stream position".to_string(),
            ));
        }
        self.history()
            .verify_currently_materialized(commit)
            .await
            .map_err(StoreReclaimError::from)
    }
}

/// Re-verify a reclaim's eligibility from its signed evidence and current live
/// state, returning the exact object the authorization may delete. Runs both at
/// prepare time and again before deletion, so a change in coverage, acknowledgement,
/// or replay retention since authoring fails the delete loud.
impl AuthorizedReclaim<'_, '_> {
    async fn verify_evidence(
        &mut self,
        evidence: &ReclaimEvidence,
    ) -> Result<ReclaimTarget, StoreReclaimError> {
        let root = self.root.clone();
        evidence
            .verify()
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        if evidence.store_root_hash != root.store_root_hash {
            return Err(StoreReclaimError::Authorization(
                "reclaim evidence belongs to another Store root".to_string(),
            ));
        }
        match &evidence.claim {
            ReclaimClaim::StorePackage(claim) => {
                let activation = self
                    .history()
                    .load_ref(&claim.target.activation)
                    .await
                    .map_err(StoreReclaimError::from)?;
                Ok(ReclaimTarget::StorePackage(
                    self.verify_store_package_reclaim_claim(&activation, claim)
                        .await?,
                ))
            }
            ReclaimClaim::CirclePackage(claim) => {
                let activation = self
                    .history()
                    .load_ref(&claim.target().activation)
                    .await
                    .map_err(StoreReclaimError::from)?;
                Ok(ReclaimTarget::CirclePackage(
                    self.verify_circle_package_reclaim_claim(&activation, claim)
                        .await?,
                ))
            }
            ReclaimClaim::CircleBootstrapImage(claim) => Ok(ReclaimTarget::CircleBootstrapImage(
                self.verify_circle_bootstrap_image_reclaim_claim(claim)
                    .await?,
            )),
            ReclaimClaim::CircleSnapshotImage(claim) => Ok(ReclaimTarget::CircleSnapshotImage(
                self.verify_circle_snapshot_image_reclaim_claim(claim)
                    .await?,
            )),
            ReclaimClaim::AudienceBlob(claim) => {
                let activation = self
                    .history()
                    .load_ref(&claim.target.activation)
                    .await
                    .map_err(StoreReclaimError::from)?;
                Ok(ReclaimTarget::AudienceBlob(
                    self.verify_audience_blob_reclaim_claim(&activation, claim)
                        .await?,
                ))
            }
        }
    }

    /// Re-verify that a row blob is free. The package the claim names is re-read
    /// from storage and must itself bind this exact blob — the signed statement
    /// that published it — and the orphan test is re-run against this device's
    /// own materialized rows rather than taken from the claim.
    async fn verify_audience_blob_reclaim_claim(
        &self,
        activation: &VerifiedStoreBatchCommit,
        claim: &AudienceBlobReclaimClaim,
    ) -> Result<AudienceBlobReclaimTarget, StoreReclaimError> {
        if audience_blob_binding_package(activation.value(), claim.target.blob.locator().audience())
            .as_ref()
            != Some(&claim.target.package)
        {
            return Err(StoreReclaimError::Authorization(
                "audience blob reclaim activation names another package".to_string(),
            ));
        }
        let package = self
            .read_audience_blob_binding_package(&claim.target.package, &claim.target.activation)
            .await?;
        if !package
            .blob_bindings()
            .iter()
            .any(|binding| binding.blob() == &claim.target.blob)
        {
            return Err(StoreReclaimError::Authorization(
                "audience blob reclaim package does not bind the target blob".to_string(),
            ));
        }
        if !self
            .database
            .stored_blob_is_row_orphaned(claim.target.blob.clone())
            .await?
        {
            return Err(StoreReclaimError::Authorization(
                "a live row still binds the audience blob as a remote reference".to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    /// Read back the exact package body that published a blob. A Store package
    /// is sealed to the Store and a Circle package to its epoch, so the audience
    /// selects both the read context and the semantic prefix.
    async fn read_audience_blob_binding_package(
        &self,
        package: &AudienceBlobBindingPackage,
        activation: &StoreBatchCommitRef,
    ) -> Result<crate::protocol::audience_package::AudiencePackage, StoreReclaimError> {
        let (context, prefix, object) = match package {
            AudienceBlobBindingPackage::Store(package) => (
                ProtocolObjectContext::store_encrypted(
                    self.root.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                crate::protocol::store_commit::package_semantic_prefix(
                    package.candidate_family,
                    &activation.coord.stream_id.to_string(),
                    activation.coord.sequence(),
                    package.content_hash,
                ),
                &package.object,
            ),
            AudienceBlobBindingPackage::Circle(package) => {
                let access = self
                    .database
                    .circle_epoch_access(
                        self.root.clone(),
                        package.circle_id,
                        package.control.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "audience blob reclaim package key is not resolvable".to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        self.root.store_root_hash,
                        ProtocolObjectDomain::CirclePackage,
                    ),
                    crate::protocol::store_commit::circle_package_semantic_prefix(
                        package.circle_id,
                        package.package.candidate_family,
                        &activation.coord.stream_id.to_string(),
                        activation.coord.sequence(),
                        package.package.content_hash,
                    ),
                    &package.package.object,
                )
            }
        };
        let bytes = self
            .storage
            .read_protocol_object(&context, object, &prefix)
            .await?;
        crate::protocol::audience_package::AudiencePackage::parse(&bytes)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))
    }

    async fn verify_circle_package_reclaim_claim(
        &mut self,
        activation: &VerifiedStoreBatchCommit,
        claim: &CirclePackageReclaimClaim,
    ) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
        match claim {
            CirclePackageReclaimClaim::SnapshotCovered(claim) => {
                // Re-verify a Circle package reclaim: a stable Circle snapshot on the
                // same Circle covers the package's activating commit, and every device
                // holding active Circle access has acknowledged coverage dominating the
                // snapshot cut. Each acknowledgement reference names the exact control
                // that resolves the epoch key it was sealed under.
                let database = self.database.clone();
                let root = self.root.clone();
                let mut history = self.history();
                let circle_id = claim.target.package.circle_id;
                let snapshot_control = &claim.covering_snapshot.control;
                // Read snapshot metadata under the current control's retained keyring so a
                // snapshot sealed before a rotation still resolves its epoch key.
                let current_control = database
                    .current_circle_control(circle_id)
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(format!(
                            "Circle {circle_id} has no active control for reclaim stability"
                        ))
                    })?;
                let access = database
                    .circle_epoch_access(root, circle_id, current_control)
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(format!(
                            "Circle {circle_id} snapshot key is not resolvable from retained controls"
                        ))
                    })?;
                let author = history
                    .load_registration(&claim.covering_snapshot.author_registration)
                    .await?;
                let stream = history
                    .load_circle_snapshot_stream_refs(
                        circle_id,
                        &access,
                        &claim.covering_snapshot.author_registration,
                        &author.value,
                    )
                    .await
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
                let (_, snapshot) = stream
                    .into_iter()
                    .find(|(reference, _)| *reference == claim.covering_snapshot.snapshot)
                    .ok_or(StoreReclaimError::NoSnapshot)?;
                if snapshot.circle_id != circle_id
                    || snapshot.control != *snapshot_control
                    || snapshot.author_registration != claim.covering_snapshot.author_registration
                {
                    return Err(StoreReclaimError::Authorization(
                        "Circle reclaim snapshot differs from its exact locator".to_string(),
                    ));
                }
                let cut = &snapshot.bootstrap.coverage;
                let expected = history
                    .stable_circle_acknowledgements(circle_id, cut)
                    .await
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "Circle snapshot is not acknowledgement-stable across every active-access device"
                                .to_string(),
                        )
                    })?;
                if claim.acknowledgements != expected {
                    return Err(StoreReclaimError::Authorization(
                        "Circle reclaim acknowledgements differ from the active-access stability proof"
                            .to_string(),
                    ));
                }
                if !activation
                    .value()
                    .circle_packages()
                    .contains(&claim.target.package)
                    || !history
                        .snapshot_covers_target(cut, &claim.target.activation)
                        .await?
                {
                    return Err(StoreReclaimError::Authorization(
                        "Circle reclaim target is not the exact package covered by its snapshot"
                            .to_string(),
                    ));
                }
                Ok(claim.target.clone())
            }
            CirclePackageReclaimClaim::BeyondEpochCutoff(claim) => {
                self.verify_circle_package_beyond_cutoff_claim(activation, claim)
                    .await
            }
        }
    }

    /// Re-verify that a Circle package lies beyond its epoch's accepted close
    /// cutoff. The named successor control must be a retained activation whose
    /// closed-epoch origin names the epoch the package's own control belongs to,
    /// and the same replay-epoch predicate the pull path applies must refuse the
    /// package. A package the cutoff accepts, or one whose control the cutoff
    /// conflicts with, is not eligible under this arm and fails loud rather than
    /// falling back to coverage.
    async fn verify_circle_package_beyond_cutoff_claim(
        &self,
        activation: &VerifiedStoreBatchCommit,
        claim: &CirclePackageBeyondCutoffClaim,
    ) -> Result<CirclePackageReclaimTarget, StoreReclaimError> {
        if !activation
            .value()
            .circle_packages()
            .contains(&claim.target.package)
        {
            return Err(StoreReclaimError::Authorization(
                "Circle package reclaim activation names another package".to_string(),
            ));
        }
        let circle_id = claim.target.package.circle_id;
        let successor = self
            .database
            .verified_circle_activation(
                self.root.clone(),
                circle_id,
                claim.successor_control.clone(),
            )
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} beyond-cutoff successor control is not a retained activation"
                ))
            })?;
        let CircleControlState::ActiveEpoch(active) = successor.control.value.state() else {
            return Err(StoreReclaimError::Authorization(
                "Circle beyond-cutoff successor control is not an activated epoch".to_string(),
            ));
        };
        let CircleEpochOrigin::Closed {
            closed_epoch_id, ..
        } = &active.common.origin
        else {
            return Err(StoreReclaimError::Authorization(
                "Circle beyond-cutoff successor epoch did not close a predecessor".to_string(),
            ));
        };
        // The package must be addressed to the epoch that close cut off, not to
        // another epoch that merely happens to precede the successor.
        let package_control = self
            .database
            .verified_circle_activation(
                self.root.clone(),
                circle_id,
                claim.target.package.control.clone(),
            )
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} package control is not a retained activation"
                ))
            })?;
        if package_control.control.value.epoch_id() != *closed_epoch_id {
            return Err(StoreReclaimError::Authorization(
                "Circle beyond-cutoff package belongs to another epoch than the one closed"
                    .to_string(),
            ));
        }
        // Apply the exact predicate pull uses to skip a package beyond its
        // accepted cutoff. A package it permits remains live history.
        if self
            .database
            .circle_replay_epoch_index(self.root.clone())
            .await?
            .permits(
                &claim.target.activation,
                circle_id,
                &claim.target.package.control,
            )
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
        {
            return Err(StoreReclaimError::Authorization(
                "Circle package lies within its accepted epoch cutoff and is not reclaimable as beyond-cutoff"
                    .to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    /// Re-verify that a later generation of the reclaimed image's own stream
    /// supersedes it. The stream is re-walked from generation zero, so both the
    /// reclaimed generation and the named superseding one are re-read from their own
    /// signed metadata; the superseding generation must carry a cut that strictly
    /// dominates the reclaimed one, and every device holding active Circle access must
    /// have acknowledged that cut. Nothing the claim asserts about coverage,
    /// stability, or the image itself is taken on trust.
    async fn verify_circle_snapshot_image_reclaim_claim(
        &mut self,
        claim: &CircleSnapshotImageReclaimClaim,
    ) -> Result<CircleSnapshotImageReclaimTarget, StoreReclaimError> {
        let database = self.database.clone();
        let mut history = self.history();
        let circle_id = claim.target.circle_id;
        let current_control = database
            .current_circle_control(circle_id)
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} has no active control for snapshot image reclaim"
                ))
            })?;
        let author = database
            .activated_store_device_registration(claim.target.snapshot_author.clone())
            .await?;
        let author_stream = [author];
        let streams = history
            .load_circle_snapshot_streams(circle_id, &current_control, &author_stream)
            .await?;
        let [stream] = streams.as_slice() else {
            return Err(StoreReclaimError::Authorization(
                "Circle snapshot reclaim author's stream is not readable".to_string(),
            ));
        };
        let generation = stream
            .generations
            .iter()
            .find(|(reference, _)| *reference == claim.target.snapshot)
            .ok_or_else(|| {
                StoreReclaimError::Authorization(
                    "Circle snapshot reclaim target is absent from its author's stream".to_string(),
                )
            })?;
        if generation.1.circle_id != circle_id
            || generation.1.control != claim.target.control
            || generation.1.bootstrap.image != claim.target.image
        {
            return Err(StoreReclaimError::Authorization(
                "Circle snapshot reclaim target differs from its own signed generation".to_string(),
            ));
        }
        let superseding = stream
            .generations
            .iter()
            .find(|(reference, _)| *reference == claim.superseding)
            .ok_or_else(|| {
                StoreReclaimError::Authorization(
                    "Circle snapshot reclaim superseding generation is absent from the same stream"
                        .to_string(),
                )
            })?;
        if !snapshot_supersedes_seed(
            &superseding.1.bootstrap.coverage,
            &generation.1.bootstrap.coverage,
        ) {
            return Err(StoreReclaimError::Authorization(
            "Circle snapshot reclaim superseding generation does not strictly dominate the reclaimed cut"
                .to_string(),
        ));
        }
        if history
            .stable_circle_acknowledgements(circle_id, &superseding.1.bootstrap.coverage)
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
            .is_none()
        {
            return Err(StoreReclaimError::Authorization(
                "Circle snapshot reclaim superseding generation is not acknowledgement-stable"
                    .to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    async fn verify_store_package_reclaim_claim(
        &mut self,
        activation: &VerifiedStoreBatchCommit,
        claim: &StorePackageReclaimClaim,
    ) -> Result<StorePackageReclaimTarget, StoreReclaimError> {
        let mut history = self.history();
        let author = history
            .load_registration(&claim.covering_snapshot.author_registration)
            .await?;
        let (reference, metadata) = history
            .load_store_snapshot(
                &claim.covering_snapshot.author_registration,
                &author.value,
                &claim.covering_snapshot.snapshot,
            )
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let snapshot = crate::database::PublishedStoreSnapshot {
            reference,
            successor_slot: metadata.successor.next_slot.clone(),
            meta: metadata,
        };
        let authority = match history.verify_snapshot_stability(&snapshot).await {
            Ok(stability) => stability.into_authority(),
            Err(super::pull::StorePullError::SnapshotNotStable { member, device_id }) => {
                return Err(StoreReclaimError::MissingAcknowledgement { member, device_id });
            }
            Err(
                super::pull::StorePullError::SnapshotAuthorInactive
                | super::pull::StorePullError::SnapshotAuthorNotOwner,
            ) => return Err(StoreReclaimError::NoSnapshot),
            Err(error) => return Err(StoreReclaimError::Authorization(error.to_string())),
        };
        let mut expected_acknowledgements = authority
            .acknowledgements
            .values()
            .map(|acknowledgement| {
                acknowledgement
                    .latest()
                    .map(|(reference, _)| reference.clone())
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "snapshot stability acknowledgement proof chain is empty".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        expected_acknowledgements.sort();
        if claim.acknowledgements != expected_acknowledgements {
            return Err(StoreReclaimError::Authorization(
            "reclaim evidence acknowledgements differ from the activated snapshot stability proof"
                .to_string(),
        ));
        }
        if activation.value().store_package() != Some(&claim.target.package)
            || !history
                .snapshot_covers_target(&snapshot.meta.coverage, &claim.target.activation)
                .await?
        {
            return Err(StoreReclaimError::Authorization(
                "reclaim target is not the exact Store package covered by its snapshot".to_string(),
            ));
        }
        Ok(claim.target.clone())
    }

    /// Re-verify a Circle bootstrap image reclaim: the recipient's own activated
    /// acknowledgement names the exact coverage being deleted (`seeded_from`), and
    /// either the recipient advanced strictly past that seed while still holding
    /// active access, or it lost authority under an activated successor control. The
    /// acknowledgement reference names the exact control that resolves the epoch key
    /// it was sealed under.
    async fn verify_circle_bootstrap_image_reclaim_claim(
        &mut self,
        claim: &CircleBootstrapImageReclaimClaim,
    ) -> Result<CircleBootstrapImageReclaimTarget, StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let mut history = self.history();
        let activation = history
            .load_ref(&claim.target.coverage.activation_commit)
            .await
            .map_err(StoreReclaimError::from)?;
        if !activation
            .value()
            .circle_controls()
            .iter()
            .flat_map(|control| control.objects.access.iter())
            .any(|access| access.bootstrap.as_ref() == Some(&claim.target.coverage.bootstrap.image))
        {
            return Err(StoreReclaimError::Authorization(
                "Circle bootstrap reclaim activation names another image".to_string(),
            ));
        }
        let circle_id = claim.target.coverage.circle_id;
        let current_control = database
            .current_circle_control(circle_id)
            .await?
            .ok_or_else(|| {
                StoreReclaimError::Authorization(format!(
                    "Circle {circle_id} has no active control for bootstrap reclaim"
                ))
            })?;
        let acknowledgement_ref = claim.proof.acknowledgement();
        let acknowledgement = history
            .load_circle_acknowledgement(acknowledgement_ref)
            .await
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        // The recipient's signed acknowledgement is the sole authority for the coverage
        // the Owner deletes: the target must be exactly what the recipient said it was
        // seeded from, so the Owner never fabricates the image, cut, or activation.
        if acknowledgement.seeded_from.as_ref() != Some(&claim.target.coverage) {
            return Err(StoreReclaimError::Authorization(
                "Circle bootstrap reclaim target differs from the recipient's signed seed coverage"
                    .to_string(),
            ));
        }
        let recipient = database
            .activated_store_device_registration(acknowledgement_ref.registration.clone())
            .await?;
        let roster = database.circle_current_roster_members(circle_id).await?;
        match &claim.proof {
            CircleBootstrapReclaimProof::RecipientCoverage { .. } => {
                if !roster.contains(&recipient.value().author_pubkey) {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap recipient-coverage proof names a device outside the current roster"
                        .to_string(),
                ));
                }
                // Re-derive the maximal acknowledgement-stable Circle snapshot and require
                // its cut to strictly dominate the seed: the later sufficient snapshot the
                // recipient (with every active device) acknowledged past its bootstrap.
                let registrations = database
                    .activated_store_device_registration_records()
                    .await
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
                let seed = &claim.target.coverage.bootstrap.coverage;
                let streams = history
                    .load_circle_snapshot_streams(circle_id, &current_control, &registrations)
                    .await?;
                let stable = history.stable_circle_snapshots(circle_id, &streams).await?;
                let superseded = maximal_stable_circle_snapshot(&stable).is_some_and(|selected| {
                    snapshot_supersedes_seed(&selected.meta.bootstrap.coverage, seed)
                });
                if !superseded {
                    return Err(StoreReclaimError::Authorization(
                    "no acknowledgement-stable Circle snapshot strictly dominates the recipient's seed coverage"
                        .to_string(),
                ));
                }
            }
            CircleBootstrapReclaimProof::LostAuthority {
                successor_control, ..
            } => {
                if roster.contains(&recipient.value().author_pubkey) {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority proof names a device still in the current roster"
                        .to_string(),
                ));
                }
                if *successor_control != current_control {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority successor is not the current activated control"
                        .to_string(),
                ));
                }
                if !database
                    .circle_control_covers_strictly(
                        root.clone(),
                        circle_id,
                        successor_control,
                        &claim.target.coverage.control,
                    )
                    .await?
                {
                    return Err(StoreReclaimError::Authorization(
                    "Circle bootstrap lost-authority successor does not strictly cover the seed control"
                        .to_string(),
                ));
                }
            }
        }
        Ok(claim.target.clone())
    }

    async fn drive_candidate(
        &mut self,
        mut operation: DurableStoreReclaimOperation,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        loop {
            let (object, candidate) = match &operation {
                DurableStoreReclaimOperation::AuthorizationCandidate { object, candidate }
                | DurableStoreReclaimOperation::ReceiptCandidate {
                    object, candidate, ..
                } => (object.clone(), candidate.clone()),
                _ => {
                    return Err(StoreReclaimError::Authorization(
                        "Store reclaim journal has no publication candidate".to_string(),
                    ));
                }
            };
            Box::pin(object.create_exact_objects(self.storage.as_ref()))
                .await
                .map_err(|error| StoreReclaimError::Journal(error.to_string()))?;
            for remote in object
                .remote_objects(&candidate)
                .map_err(|error| StoreReclaimError::Journal(error.to_string()))?
            {
                if matches!(
                    &remote,
                    crate::protocol::remote_object::RemoteObjectRecord::RetainedAuthority(record)
                        if matches!(
                            record.identity.domain,
                            crate::protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimEvidence { .. }
                                | crate::protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimAuthorization { .. }
                                | crate::protocol::remote_object::RetainedAuthorityObjectDomain::ReclaimReceipt { .. }
                        )
                ) {
                    database
                        .mark_reusable_retained_authority_uploaded(remote)
                        .await?;
                }
            }
            // Scoped to the publication alone: the arms below re-derive a plan,
            // which takes this same turn.
            let outcome = {
                let _authorship = database.author_own_stream().await;
                Box::pin(self.writer.publish_prepared(candidate, None, None)).await?
            };
            match outcome {
                super::operations::StoreOperationPublicationOutcome::Activated(_) => {
                    return Ok(());
                }
                super::operations::StoreOperationPublicationOutcome::RepreparedCandidate(
                    replacement,
                ) => {
                    operation =
                        Box::pin(database.replace_store_reclaim_candidate(operation, *replacement))
                            .await?;
                }
                super::operations::StoreOperationPublicationOutcome::NonactivatedCandidate {
                    nonactivation,
                    ..
                } => {
                    let plan = self.writer.prepare_plan().await?;
                    let batch = match &*object {
                        DurableStoreReclaimObject::Authorization {
                            authorization_ref, ..
                        } => super::operations::StoreOperationBatch::ReclaimAuthorization(
                            Box::new(authorization_ref.clone()),
                        ),
                        DurableStoreReclaimObject::Receipt { receipt_ref, .. } => {
                            super::operations::StoreOperationBatch::ReclaimReceipt(Box::new(
                                receipt_ref.clone(),
                            ))
                        }
                    };
                    let replacement = self.writer.prepare_candidate(plan, batch).await?;
                    operation = Box::pin(database.begin_store_reclaim_candidate_replacement(
                        operation,
                        replacement,
                        *nonactivation,
                    ))
                    .await?;
                    Box::pin(self.finish_candidate_replacement(operation)).await?;
                    return Ok(());
                }
                super::operations::StoreOperationPublicationOutcome::Nonactivated(_)
                | super::operations::StoreOperationPublicationOutcome::Reprepared => {
                    return Err(StoreReclaimError::Authorization(
                        "Store reclaim publication returned acknowledgement-only state".to_string(),
                    ));
                }
            }
        }
    }

    async fn finish_candidate_replacement(
        &self,
        operation: DurableStoreReclaimOperation,
    ) -> Result<(), StoreReclaimError> {
        let database = &self.database;
        for target in database
            .store_reclaim_replacement_cleanup_targets(operation.clone())
            .await?
        {
            self.storage
                .delete_protocol_object(&target.object)
                .await
                .map_err(crate::protocol::objects::StoreObjectError::from)?;
            database
                .mark_candidate_cleanup_absent(target.object)
                .await?;
        }
        database
            .complete_store_reclaim_candidate_replacement(operation)
            .await?;
        Ok(())
    }

    async fn prepare_receipt(
        &mut self,
        operation: DurableStoreReclaimOperation,
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let membership = self.membership.clone();
        let DurableStoreReclaimOperation::AbsentVerified {
            authorization,
            target,
            ..
        } = &operation
        else {
            return Err(StoreReclaimError::Authorization(
                "only an authorized reclaim can be executed".to_string(),
            ));
        };
        let opened = self
            .history()
            .load_reclaim_authorization(authorization)
            .await?;
        if &opened.authorization.value.target != target {
            return Err(StoreReclaimError::Authorization(
                "durable absent target differs from its signed authorization".to_string(),
            ));
        }

        let plan = self.writer.prepare_plan().await?;
        let crate::protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(StoreReclaimError::Authorization(
                "provider execution requires resolved Store membership".to_string(),
            ));
        };
        let provider_admin = resolved.provider_admin.combined_state().clone();
        let provider_admin_grant = plan
            .effective_provider_admin_grant(&provider_admin)
            .ok_or_else(|| {
                StoreReclaimError::Authorization(
                    "local Store device is not an effective provider administrator".to_string(),
                )
            })?;
        let receipt = plan
            .sign_reclaim_receipt(authorization.clone(), provider_admin_grant)
            .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?;
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreReclaimReceipt,
        );
        let prefix = reclaim_receipt_semantic_prefix(receipt.receipt_hash());
        let slot = self
            .storage
            .allocate_protocol_slot(&context, &prefix, ".json")
            .await?;
        let prepared =
            self.storage
                .prepare_protocol_object(&context, slot, &prefix, receipt.to_bytes())?;
        let receipt_ref = ReclaimReceiptRef::from_receipt(&receipt, prepared.reference().clone());
        let candidate = self
            .writer
            .prepare_candidate(
                plan,
                super::operations::StoreOperationBatch::ReclaimReceipt(Box::new(
                    receipt_ref.clone(),
                )),
            )
            .await?;
        Box::pin(database.begin_store_reclaim_receipt(
            operation,
            DurableStoreReclaimObject::Receipt {
                receipt_ref,
                receipt,
                receipt_prepared: prepared,
            },
            candidate,
        ))
        .await?;
        Ok(())
    }
}

impl AuthorizedReclaim<'_, '_> {
    async fn verify_target_absent(&self, target: &ReclaimTarget) -> Result<(), StoreReclaimError> {
        let database = &self.database;
        let storage = self.storage.clone();
        let root = &self.root;
        // A row blob is addressed by its locator, not by a protocol domain prefix, so
        // its absence is confirmed through the same exact-object verification the blob
        // primitives use.
        if let ReclaimTarget::AudienceBlob(blob) = target {
            return match storage.verify_blob_object(&blob.blob).await {
                Err(StorageError::NotFound(_)) => Ok(()),
                Ok(()) => Err(StoreReclaimError::Authorization(
                    "reclaim target remains readable after exact deletion".to_string(),
                )),
                Err(error) => Err(StoreReclaimError::Storage(error)),
            };
        }
        let (context, prefix) = match target {
            ReclaimTarget::StorePackage(target) => (
                ProtocolObjectContext::store_encrypted(
                    root.store_root_hash,
                    ProtocolObjectDomain::StorePackage,
                ),
                crate::protocol::store_commit::package_semantic_prefix(
                    target.package.candidate_family,
                    &target.activation.coord.stream_id.to_string(),
                    target.activation.coord.sequence(),
                    target.package.content_hash,
                ),
            ),
            ReclaimTarget::CirclePackage(target) => {
                // A Circle package is sealed under the Circle epoch key; resolving its
                // access only builds the read context — a deleted object reads back
                // absent before any decryption.
                let access = database
                    .circle_epoch_access(
                        root.clone(),
                        target.package.circle_id,
                        target.package.control.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaim target Circle package key is not resolvable".to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        root.store_root_hash,
                        ProtocolObjectDomain::CirclePackage,
                    ),
                    crate::protocol::store_commit::circle_package_semantic_prefix(
                        target.package.circle_id,
                        target.package.package.candidate_family,
                        &target.activation.coord.stream_id.to_string(),
                        target.activation.coord.sequence(),
                        target.package.package.content_hash,
                    ),
                )
            }
            ReclaimTarget::CircleBootstrapImage(target) => {
                // A Circle bootstrap image is sealed under the Circle epoch key of the
                // control it activated under; resolving that access only builds the read
                // context — a deleted object reads back absent before any decryption. Its
                // readback prefix is the image object's own logical key without the domain
                // extension, so no recipient-sealed leaf field is needed to confirm absence.
                let access = database
                    .circle_epoch_access(
                        root.clone(),
                        target.coverage.circle_id,
                        target.coverage.control.clone(),
                    )
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaim target Circle bootstrap image key is not resolvable"
                                .to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        root.store_root_hash,
                        ProtocolObjectDomain::CircleBootstrapImage,
                    ),
                    crate::protocol::store_commit::semantic_prefix_from_exact_object(
                        &target.coverage.bootstrap.image.object,
                        crate::protocol::objects::ProtectedObjectDomain::CircleBootstrapImage
                            .extension(),
                    )
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?,
                )
            }
            ReclaimTarget::CircleSnapshotImage(target) => {
                // A standalone Circle snapshot image is sealed under the epoch key of the
                // control its generation was authored under; resolving that access only
                // builds the read context — a deleted object reads back absent before any
                // decryption.
                let access = database
                    .circle_epoch_access(root.clone(), target.circle_id, target.control.clone())
                    .await?
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaim target Circle snapshot image key is not resolvable"
                                .to_string(),
                        )
                    })?;
                (
                    access.protocol_context(
                        root.store_root_hash,
                        ProtocolObjectDomain::CircleSnapshotImage,
                    ),
                    crate::protocol::store_commit::semantic_prefix_from_exact_object(
                        &target.image.object,
                        crate::protocol::objects::ProtectedObjectDomain::CircleSnapshotImage
                            .extension(),
                    )
                    .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?,
                )
            }
            ReclaimTarget::AudienceBlob(_) => {
                return Err(StoreReclaimError::Authorization(
                    "audience blob reclaim target has no protocol object prefix".to_string(),
                ));
            }
        };
        match storage
            .read_protocol_object(&context, target.object(), &prefix)
            .await
        {
            Err(StorageError::NotFound(_)) => Ok(()),
            Ok(_) => Err(StoreReclaimError::Authorization(
                "reclaim target remains readable after exact deletion".to_string(),
            )),
            Err(error) => Err(StoreReclaimError::Storage(error)),
        }
    }
}

#[derive(Clone)]
struct VerifiedReclaimSnapshot {
    snapshot: crate::database::PublishedStoreSnapshot,
    acknowledgements: Vec<StoreAckRef>,
}

impl AuthorizedReclaim<'_, '_> {
    async fn choose_snapshot(
        &mut self,
        registrations: &[crate::protocol::store_commit::ReferencedStoreDeviceRegistration],
    ) -> Result<VerifiedReclaimSnapshot, StoreReclaimError> {
        let storage = self.storage.clone();
        let root = self.root.clone();
        let mut history = self.history();
        let mut authorized = Vec::new();
        for registration in registrations {
            for snapshot in history
                .load_store_snapshot_stream(registration.reference(), registration.value())
                .await
                .map_err(|error| StoreReclaimError::Authorization(error.to_string()))?
            {
                authorized.push(snapshot);
            }
        }
        let selected = match history
            .select_maximal_stable_store_snapshot(authorized)
            .await
        {
            Ok(Some(selected)) => selected,
            Ok(None) => return Err(StoreReclaimError::NoSnapshot),
            Err(super::pull::StorePullError::SnapshotNotStable { member, device_id }) => {
                return Err(StoreReclaimError::MissingAcknowledgement { member, device_id });
            }
            Err(
                super::pull::StorePullError::SnapshotAuthorInactive
                | super::pull::StorePullError::SnapshotAuthorNotOwner,
            ) => return Err(StoreReclaimError::NoSnapshot),
            Err(error) => return Err(StoreReclaimError::Authorization(error.to_string())),
        };
        let snapshot = selected.snapshot;
        let image = storage
            .read_protocol_object(
                &ProtocolObjectContext::store_encrypted(
                    root.store_root_hash,
                    ProtocolObjectDomain::StoreSnapshotImage,
                ),
                &snapshot.meta.image.object,
                &snapshot_image_semantic_prefix(
                    &snapshot.meta.author_registration.device_id.to_string(),
                    snapshot.meta.image.image_hash,
                ),
            )
            .await
            .map_err(StoreObjectError::from)?;
        if ObjectHash::digest(&image) != snapshot.meta.image.image_hash {
            return Err(StoreReclaimError::Authorization(
                "snapshot image differs from its signed exact reference".to_string(),
            ));
        }
        let authority = selected.stability.into_authority();
        let mut acknowledgements = authority
            .acknowledgements
            .values()
            .map(|acknowledgement| {
                acknowledgement
                    .latest()
                    .map(|(reference, _)| reference.clone())
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "snapshot stability acknowledgement proof chain is empty".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        acknowledgements.sort();
        Ok(VerifiedReclaimSnapshot {
            snapshot,
            acknowledgements,
        })
    }
}

#[cfg(test)]
mod tests;
