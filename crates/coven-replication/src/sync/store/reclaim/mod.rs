//! Proof-gated deletion of exact Store packages covered by exact authority.

use coven_database::StoreReclaimJournalError;
use std::sync::Arc;

mod candidates;
mod claims;
mod history;

use crate::sync::store::AuthorizedWriterOperation;
use coven_database::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, ReclaimCommitActivation, StoreDatabase,
};
use coven_protocol::circle::{CircleControlCoord, CircleControlState, CircleEpochOrigin, CircleId};
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use coven_protocol::reclaim::*;
use coven_protocol::store_commit::{
    snapshot_image_semantic_prefix, CommitFrontier, ObjectHash, StoreAckRef, StoreBatchCommitRef,
    StoreRootRef, StoreSnapshotLocator, VerifiedStoreBatchCommit,
};
use coven_storage::CloudSyncObjectStorage;
pub(crate) use history::{CircleSnapshotStream, ReclaimHistory, SelectedCircleSnapshot};

#[derive(Debug, PartialEq, Eq)]
pub struct StoreReclaimResult {
    pub packages_deleted: u64,
    pub physical_copies_deleted: u64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreReclaimError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error(transparent)]
    Database(#[from] coven_database::DbError),
    #[error(transparent)]
    Outbound(#[from] crate::sync::store::StoreError),
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

impl From<crate::sync::store::pull::CommitCoverageError> for StoreReclaimError {
    fn from(error: crate::sync::store::pull::CommitCoverageError) -> Self {
        match error {
            crate::sync::store::pull::CommitCoverageError::Object(error) => Self::Object(error),
            crate::sync::store::pull::CommitCoverageError::MissingAncestry { commit_hash } => {
                Self::MissingAncestry { commit_hash }
            }
        }
    }
}

impl From<crate::sync::store::pull::StorePullError> for StoreReclaimError {
    fn from(error: crate::sync::store::pull::StorePullError) -> Self {
        match error {
            crate::sync::store::pull::StorePullError::Object(error) => Self::Object(error),
            crate::sync::store::pull::StorePullError::Storage(error) => Self::Storage(error),
            error => Self::Authorization(error.to_string()),
        }
    }
}

use candidates::*;

pub(crate) struct AuthorizedReclaim<'operation, 'storage> {
    writer: &'operation mut AuthorizedWriterOperation<'storage>,
    database: StoreDatabase,
    storage: Arc<dyn CloudSyncObjectStorage>,
    root: StoreRootRef,
    membership: coven_protocol::membership::MembershipChain,
}

impl<'operation, 'storage> AuthorizedReclaim<'operation, 'storage> {
    pub(crate) fn new(
        writer: &'operation mut AuthorizedWriterOperation<'storage>,
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        root: StoreRootRef,
        membership: coven_protocol::membership::MembershipChain,
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
                    blob = %coven_protocol::remote_object::remote_object_id(blob.object()),
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

    async fn prepare_circle_authorizations(
        &mut self,
        registrations: &[coven_protocol::store_commit::ReferencedStoreDeviceRegistration],
    ) -> Result<(), StoreReclaimError> {
        let database = self.database.clone();
        for input in database.circle_acknowledgement_publication_inputs().await? {
            let circle_id = input.circle_id();
            let control = input.control().clone();
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
                crate::sync::store::commit_publication::operation::commit_plan::StoreOperationBatch::ReclaimAuthorization(Box::new(
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

#[cfg(test)]
mod tests;

pub(crate) async fn create_reclaim_exact_objects(
    object: &coven_database::DurableStoreReclaimObject,
    storage: &dyn CloudSyncObjectStorage,
) -> Result<(), StoreReclaimJournalError> {
    match object {
        coven_database::DurableStoreReclaimObject::Authorization {
            evidence,
            evidence_prepared,
            authorization,
            authorization_prepared,
            ..
        } => {
            storage
                .create_verified_protocol_object(
                    &ProtocolObjectContext::store_encrypted(
                        evidence.store_root_hash,
                        ProtocolObjectDomain::StoreReclaimEvidence,
                    ),
                    evidence_prepared,
                    &reclaim_evidence_semantic_prefix(evidence.evidence_hash()),
                    &evidence.to_bytes(),
                )
                .await?;
            storage
                .create_verified_protocol_object(
                    &ProtocolObjectContext::signed_plaintext(
                        authorization.store_root_hash,
                        ProtocolObjectDomain::StoreReclaimAuthorization,
                    ),
                    authorization_prepared,
                    &reclaim_authorization_semantic_prefix(authorization.authorization_hash()),
                    &authorization.to_bytes(),
                )
                .await
                .map_err(StoreReclaimJournalError::Storage)
        }
        coven_database::DurableStoreReclaimObject::Receipt {
            receipt,
            receipt_prepared,
            ..
        } => storage
            .create_verified_protocol_object(
                &ProtocolObjectContext::signed_plaintext(
                    receipt.store_root_hash,
                    ProtocolObjectDomain::StoreReclaimReceipt,
                ),
                receipt_prepared,
                &reclaim_receipt_semantic_prefix(receipt.receipt_hash()),
                &receipt.to_bytes(),
            )
            .await
            .map_err(StoreReclaimJournalError::Storage),
    }
}
