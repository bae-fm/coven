//! Exact object reclamation after accepted Store snapshot publication.
//! Store retirement follows the accepted boundary. Circle snapshots retain
//! their separate access acknowledgement requirements.

use std::collections::BTreeSet;
use std::sync::Arc;

mod candidates;
mod claims;
mod history;
mod snapshot_retirement;

use crate::sync::store::AuthorizedWriterOperation;
use coven_database::{
    DurableStoreReclaimObject, DurableStoreReclaimOperation, StoreDatabase,
    StoreReclaimJournalError,
};
use coven_protocol::circle::{CircleControlCoord, CircleControlState, CircleEpochOrigin, CircleId};
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use coven_protocol::reclaim::*;
use coven_protocol::store_commit::{
    snapshot_image_semantic_prefix, CommitFrontier, ObjectHash, StoreBatchCommitRef, StoreRootRef,
    VerifiedStoreBatchCommit,
};
use coven_storage::CloudSyncObjectStorage;
pub(crate) use history::{CircleSnapshotStream, ReclaimHistory, SelectedCircleSnapshot};

#[derive(Debug, PartialEq, Eq)]
pub struct StoreReclaimResult {
    pub packages_deleted: u64,
    pub physical_copies_deleted: u64,
    /// Operations the journal is left holding for a person: they failed with an
    /// error running them again cannot change, so every later cycle skips them
    /// until the host asks for one back.
    pub stuck: u64,
    /// What the Store-package leg did, so a run that deleted nothing says which
    /// step declined instead of reporting a bare zero. The leg is the one whose
    /// outcome was previously unobservable: its two commonest declines are
    /// turned into an empty target list on purpose, so that Store trouble does
    /// not block Circle reclaim, and that swallowed the reason with the error.
    pub store_packages: StorePackageReclaimReport,
}

/// What the Store-package leg of one reclaim run considered and what it did.
///
/// Counts rather than per-target lines: a store with hundreds of covered
/// commits would drown a cycle in log spam, and the question a reader has is
/// which step the targets died at, not which target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StorePackageReclaimReport {
    /// The coverage the leg had to work from, or why it had none.
    pub coverage: StorePackageReclaimCoverage,
    /// Package-bearing commits at or behind the coverage.
    pub targets_considered: u64,
    /// Targets left alone because a retained materialization still pins them
    /// for replay. A run where this equals `targets_considered` is one whose
    /// retained set has not been narrowed by a snapshot image projection.
    pub retained_for_replay: u64,
    /// Targets that already have a journalled operation, which blocks
    /// re-authorizing them.
    pub already_authorized: u64,
    /// Targets this run signed a fresh authorization for.
    pub authorized: u64,
}

/// The coverage the Store-package leg worked from, or why it had none.
///
/// A decline is a value here rather than a swallowed error because it is the
/// leg's ordinary outcome, not a failure: `run` deliberately continues to the
/// Circle legs when the Store leg has no coverage, and reporting the reason is
/// the only way a reader can tell that apart from having nothing to delete.
impl StorePackageReclaimReport {
    /// A report for a leg that has not looked at any target yet — the shape a
    /// declined leg keeps, and the starting point for one that proceeds.
    fn declined(coverage: StorePackageReclaimCoverage) -> Self {
        Self {
            coverage,
            targets_considered: 0,
            retained_for_replay: 0,
            already_authorized: 0,
            authorized: 0,
        }
    }
}

/// Whether a claim reached the provider or found its target already journalled.
///
/// An existing operation for a target blocks re-authorizing it, so the two are
/// worth telling apart: one is progress, the other is a target this run could
/// not have acted on however it was configured.
enum AuthorizationOutcome {
    Signed,
    AlreadyJournalled,
}

/// What one pass over the journal did.
struct ReclaimPass {
    packages_deleted: u64,
    /// Operations the journal holds stuck when the pass ended — the ones it
    /// marked plus the ones an earlier pass did.
    stuck: u64,
}

/// What advancing one journalled operation by one step did.
enum ReclaimStep {
    /// The operation moved to its next durable state.
    Advanced,
    /// The operation deleted its target.
    Deleted,
    /// Nothing to do this pass: the operation is finished, or it waits behind
    /// a blob reclaim that still has to re-read the package it deletes.
    Idle,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorePackageReclaimCoverage {
    /// The exact accepted snapshot the leg deleted behind.
    Snapshot {
        snapshot: coven_protocol::store_commit::AcceptedStoreSnapshotRef,
    },
    /// No accepted Store snapshot is available.
    NoSnapshot,
    /// This device is not the current owner, so it does not reclaim at all.
    NotOwner,
    /// Nothing this evaluation depends on has changed since the last one, so
    /// its answer is the last one. The steady state of a settled store, and the
    /// only outcome here that reaches the provider not at all.
    InputsUnchanged,
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
    Journal(#[from] StoreReclaimJournalError),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("no authorized complete Store snapshot is available for reclamation")]
    NoSnapshot,
    #[error("snapshot authorization history is invalid: {0}")]
    Authorization(String),
    #[error("snapshot authorization Store pull: {0}")]
    StorePull(#[source] Box<crate::sync::store::pull::StorePullError>),
    #[error("snapshot authorization Store protocol: {0}")]
    Protocol(#[from] coven_protocol::store_commit::StoreProtocolError),
    #[error("snapshot authorization audience package: {0}")]
    AudiencePackage(#[from] coven_protocol::audience_package::AudiencePackageError),
    #[error("snapshot authorization snapshot: {0}")]
    Snapshot(#[source] Box<crate::sync::store::SnapshotError>),
    #[error("snapshot authorization acknowledgement: {0}")]
    Acknowledgement(#[source] Box<crate::sync::store::StoreAckError>),
    #[error("snapshot authorization writer: {0}")]
    WriterAuthorization(#[source] Box<crate::sync::store::StoreWriterAuthorizationError>),
    #[error("exact Store ancestry is missing commit {commit_hash}")]
    MissingAncestry { commit_hash: ObjectHash },
    #[error("deleting exact object {object} failed: {source}")]
    Delete {
        object: ObjectHash,
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
        Self::StorePull(Box::new(error))
    }
}

impl From<crate::sync::store::SnapshotError> for StoreReclaimError {
    fn from(error: crate::sync::store::SnapshotError) -> Self {
        Self::Snapshot(Box::new(error))
    }
}

impl From<crate::sync::store::StoreAckError> for StoreReclaimError {
    fn from(error: crate::sync::store::StoreAckError) -> Self {
        Self::Acknowledgement(Box::new(error))
    }
}

impl From<crate::sync::store::StoreWriterAuthorizationError> for StoreReclaimError {
    fn from(error: crate::sync::store::StoreWriterAuthorizationError) -> Self {
        Self::WriterAuthorization(Box::new(error))
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

    pub(super) async fn run(
        &mut self,
        settled: &crate::sync::store::SettledCycle,
    ) -> Result<StoreReclaimResult, StoreReclaimError> {
        let database = self.database.clone();
        let membership = self.membership.clone();
        // Discover every blob reclaim before resuming old package work. A blob
        // reclaim re-reads the package that published it; the in-memory set
        // protects newly discovered claims until their durable operations are
        // published, while already journalled claims remain protected by the
        // database query in `deferred_to_blob_reclaim`.
        let blob_claims = Box::pin(self.audience_blob_reclaim_claims()).await?;
        let blob_packages = blob_claims
            .iter()
            .filter_map(|claim| match claim {
                ReclaimClaim::AudienceBlob(AudienceBlobReclaimClaim {
                    target: AudienceBlobReclaimTarget::Circle { source, .. },
                }) => Some(coven_protocol::remote_object::remote_object_id(
                    &source.package.package.object,
                )),
                _ => None,
            })
            .collect::<BTreeSet<_>>();
        // Journalled work next, always. An operation this device authorized and
        // did not finish is durable state waiting on its author, and gating that
        // behind "did anything change" would leave it waiting on an unrelated
        // event.
        let mut journal = Box::pin(self.resume_operations(&blob_packages)).await?;
        let mut packages_deleted = journal.packages_deleted;
        if !self.writer.is_current_owner(&membership) {
            return Ok(StoreReclaimResult {
                packages_deleted,
                physical_copies_deleted: packages_deleted,
                stuck: journal.stuck,
                store_packages: StorePackageReclaimReport::declined(
                    StorePackageReclaimCoverage::NotOwner,
                ),
            });
        }
        for claim in blob_claims {
            let (_, resumed) = Box::pin(self.authorize_and_resume(claim, &blob_packages)).await?;
            packages_deleted = packages_deleted
                .checked_add(resumed.packages_deleted)
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaimed package count exceeded u64".to_string(),
                    )
                })?;
            journal.stuck = resumed.stuck;
        }
        let resumed = Box::pin(self.resume_operations(&BTreeSet::new())).await?;
        packages_deleted = packages_deleted
            .checked_add(resumed.packages_deleted)
            .ok_or_else(|| {
                StoreReclaimError::Authorization("reclaimed package count exceeded u64".to_string())
            })?;
        journal.stuck = resumed.stuck;
        // The evaluation below walks every candidate snapshot's stability and
        // every device's acknowledgement chain. Its answer is a function of
        // facts this database holds, so running it again against the same ones
        // spends the provider to reach a conclusion already reached.
        let inputs = crate::sync::store::CycleInputs::read(&database, &membership)
            .await
            .map_err(StoreReclaimError::Database)?;
        if settled.reclaim_evaluated(&inputs) {
            return Ok(StoreReclaimResult {
                packages_deleted,
                physical_copies_deleted: packages_deleted,
                stuck: journal.stuck,
                store_packages: StorePackageReclaimReport::declined(
                    StorePackageReclaimCoverage::InputsUnchanged,
                ),
            });
        }
        let registrations = database
            .activated_store_device_registration_records()
            .await
            .map_err(StoreReclaimError::from)?;
        // A missing or unstable Store snapshot leaves Store packages uncovered but must
        // not block Circle package reclamation, which carries its own Circle coverage.
        let mut artifacts_deleted = 0;
        let (coverage, store_targets) = match Box::pin(self.choose_snapshot()).await {
            Ok(claim) => {
                let snapshot = claim.reference();
                let plan = self.writer.prepare_plan().await?;
                let coven_protocol::membership::MembershipStatus::Resolved(resolved) =
                    plan.membership().status()
                else {
                    return Err(StoreReclaimError::Authorization(
                        "snapshot retirement requires resolved membership".into(),
                    ));
                };
                if plan.owner_grant().is_some()
                    && plan
                        .effective_provider_admin_grant(resolved.provider_admin.combined_state())
                        .is_some()
                {
                    artifacts_deleted = self.retire_snapshot_artifacts(&claim, &plan).await?;
                } else {
                    tracing::debug!(
                        "skip accepted snapshot artifact deletion: this device lacks provider administration authority"
                    );
                }
                let targets = claim
                    .snapshot()
                    .meta
                    .history_summary
                    .reclaim
                    .packages
                    .values()
                    .filter_map(|target| match &target.package {
                        AudienceBlobBindingPackage::Store(package) => {
                            Some((target.activation.clone(), package.clone()))
                        }
                        AudienceBlobBindingPackage::Circle(_) => None,
                    })
                    .collect::<Vec<_>>();
                (StorePackageReclaimCoverage::Snapshot { snapshot }, targets)
            }
            // Store trouble must not block Circle reclamation, which carries
            // its own Circle coverage — so these two do not propagate. The
            // reason travels in the report instead of dying here, which is what
            // makes a cycle that deleted nothing say why.
            Err(StoreReclaimError::NoSnapshot) => {
                (StorePackageReclaimCoverage::NoSnapshot, Vec::new())
            }
            Err(error) => return Err(error),
        };
        let mut store_packages = StorePackageReclaimReport::declined(coverage);
        store_packages.targets_considered = store_targets.len() as u64;
        for (commit, package) in store_targets {
            if database
                .store_package_is_retained_for_replay(
                    self.root.clone(),
                    package.clone(),
                    commit.clone(),
                )
                .await?
            {
                store_packages.retained_for_replay += 1;
                continue;
            }
            let (authorized, resumed) = Box::pin(self.authorize_and_resume(
                ReclaimClaim::StorePackage(StorePackageReclaimClaim {
                    target: StorePackageReclaimTarget {
                        package,
                        activation: commit,
                    },
                }),
                &BTreeSet::new(),
            ))
            .await?;
            packages_deleted = packages_deleted
                .checked_add(resumed.packages_deleted)
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaimed package count exceeded u64".to_string(),
                    )
                })?;
            match authorized {
                AuthorizationOutcome::Signed => store_packages.authorized += 1,
                AuthorizationOutcome::AlreadyJournalled => store_packages.already_authorized += 1,
            }
        }
        let circle_deleted = Box::pin(self.prepare_circle_authorizations(&registrations)).await?;
        packages_deleted = packages_deleted
            .checked_add(circle_deleted)
            .ok_or_else(|| {
                StoreReclaimError::Authorization("reclaimed package count exceeded u64".to_string())
            })?;
        let final_journal = Box::pin(self.resume_operations(&BTreeSet::new())).await?;
        packages_deleted = packages_deleted
            .checked_add(final_journal.packages_deleted)
            .ok_or_else(|| {
                StoreReclaimError::Authorization("reclaimed package count exceeded u64".to_string())
            })?;
        // Recorded only once the evaluation has run all the way through, so a
        // run that failed partway is re-run rather than remembered as settled.
        settled.record_reclaim_evaluated(inputs);
        Ok(StoreReclaimResult {
            packages_deleted,
            physical_copies_deleted: packages_deleted.checked_add(artifacts_deleted).ok_or_else(
                || StoreReclaimError::Authorization("retired object count exceeded u64".into()),
            )?,
            stuck: final_journal.stuck,
            store_packages,
        })
    }

    async fn prepare_beyond_cutoff_circle_authorizations(
        &mut self,
        circle_id: CircleId,
        current_control: &CircleControlCoord,
    ) -> Result<u64, StoreReclaimError> {
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
            return Ok(0);
        }
        let mut packages_deleted = 0_u64;
        let frontier = CommitFrontier::from_refs(database.materialized_frontier().await?)
            .map_err(StoreReclaimError::from)?;
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
                .map_err(StoreReclaimError::from)?
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
                || database
                    .package_is_retained_by_pending_blob_reclaim(package.package.object.clone())
                    .await?
            {
                continue;
            }
            let (_, resumed) = Box::pin(self.authorize_and_resume(
                ReclaimClaim::CirclePackage(CirclePackageReclaimClaim::BeyondEpochCutoff(
                    CirclePackageBeyondCutoffClaim {
                        target: CirclePackageReclaimTarget {
                            package,
                            activation: commit,
                        },
                        successor_control: current_control.clone(),
                    },
                )),
                &BTreeSet::new(),
            ))
            .await?;
            packages_deleted = packages_deleted
                .checked_add(resumed.packages_deleted)
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaimed package count exceeded u64".to_string(),
                    )
                })?;
        }
        Ok(packages_deleted)
    }

    async fn audience_blob_reclaim_claims(
        &mut self,
    ) -> Result<Vec<ReclaimClaim>, StoreReclaimError> {
        let database = self.database.clone();
        let mut claims = Vec::new();
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
            if blob.locator().audience() == coven_protocol::blob::locator::RemoteAudience::Store {
                if self.history().store_blob_is_reclaimable(&blob).await? {
                    claims.push(ReclaimClaim::AudienceBlob(AudienceBlobReclaimClaim {
                        target: AudienceBlobReclaimTarget::Store { blob },
                    }));
                }
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
            let Some((AudienceBlobBindingPackage::Circle(package), activation)) = binding else {
                tracing::debug!(
                    blob = %coven_protocol::remote_object::remote_object_id(blob.object()),
                    "skip orphaned blob whose owning commits name no package for its audience",
                );
                continue;
            };
            let target = AudienceBlobReclaimTarget::Circle {
                blob,
                source: CirclePackageReclaimTarget {
                    package,
                    activation,
                },
            };
            claims.push(ReclaimClaim::AudienceBlob(AudienceBlobReclaimClaim {
                target,
            }));
        }
        Ok(claims)
    }

    async fn prepare_circle_snapshot_image_authorizations(
        &mut self,
        circle_id: CircleId,
        streams: &[CircleSnapshotStream],
        stable: &[SelectedCircleSnapshot],
    ) -> Result<u64, StoreReclaimError> {
        let database = self.database.clone();
        let mut packages_deleted = 0_u64;
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
                let (_, resumed) = Box::pin(self.authorize_and_resume(
                    ReclaimClaim::CircleSnapshotImage(CircleSnapshotImageReclaimClaim {
                        target,
                        superseding: superseding.reference.clone(),
                    }),
                    &BTreeSet::new(),
                ))
                .await?;
                packages_deleted = packages_deleted
                    .checked_add(resumed.packages_deleted)
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaimed package count exceeded u64".to_string(),
                        )
                    })?;
            }
        }
        Ok(packages_deleted)
    }

    async fn prepare_circle_authorizations(
        &mut self,
        registrations: &[coven_protocol::store_commit::ReferencedStoreDeviceRegistration],
    ) -> Result<u64, StoreReclaimError> {
        let database = self.database.clone();
        let mut packages_deleted = 0_u64;
        for input in database.circle_acknowledgement_publication_inputs().await? {
            let circle_id = input.circle_id();
            let control = input.control().clone();
            // A package beyond its epoch's accepted cutoff never materializes anywhere,
            // so it needs no snapshot coverage and is enumerated whether or not this
            // Circle has a stable snapshot.
            packages_deleted = packages_deleted
                .checked_add(
                    Box::pin(self.prepare_beyond_cutoff_circle_authorizations(circle_id, &control))
                        .await?,
                )
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaimed package count exceeded u64".to_string(),
                    )
                })?;
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
            packages_deleted = packages_deleted
                .checked_add(
                    Box::pin(
                        self.prepare_circle_bootstrap_authorizations(circle_id, &control, selected),
                    )
                    .await?,
                )
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaimed package count exceeded u64".to_string(),
                    )
                })?;
            // A superseded snapshot generation's image is reclaimable on its own
            // stream's evidence, independent of which snapshot covers the packages.
            packages_deleted = packages_deleted
                .checked_add(
                    Box::pin(self.prepare_circle_snapshot_image_authorizations(
                        circle_id, &streams, &stable,
                    ))
                    .await?,
                )
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaimed package count exceeded u64".to_string(),
                    )
                })?;
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
                    || database
                        .package_is_retained_by_pending_blob_reclaim(package.package.object.clone())
                        .await?
                {
                    continue;
                }
                let (_, resumed) = Box::pin(self.authorize_and_resume(
                    ReclaimClaim::CirclePackage(CirclePackageReclaimClaim::SnapshotCovered(
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
                    )),
                    &BTreeSet::new(),
                ))
                .await?;
                packages_deleted = packages_deleted
                    .checked_add(resumed.packages_deleted)
                    .ok_or_else(|| {
                        StoreReclaimError::Authorization(
                            "reclaimed package count exceeded u64".to_string(),
                        )
                    })?;
            }
        }
        Ok(packages_deleted)
    }

    async fn prepare_circle_bootstrap_authorizations(
        &mut self,
        circle_id: CircleId,
        current_control: &CircleControlCoord,
        selected: Option<&SelectedCircleSnapshot>,
    ) -> Result<u64, StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let mut packages_deleted = 0_u64;
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
            let (_, resumed) = Box::pin(self.authorize_and_resume(
                ReclaimClaim::CircleBootstrapImage(CircleBootstrapImageReclaimClaim {
                    target,
                    proof,
                }),
                &BTreeSet::new(),
            ))
            .await?;
            packages_deleted = packages_deleted
                .checked_add(resumed.packages_deleted)
                .ok_or_else(|| {
                    StoreReclaimError::Authorization(
                        "reclaimed package count exceeded u64".to_string(),
                    )
                })?;
        }
        Ok(packages_deleted)
    }

    async fn prepare_authorization(
        &mut self,
        claim: ReclaimClaim,
    ) -> Result<AuthorizationOutcome, StoreReclaimError> {
        let database = self.database.clone();
        let root = self.root.clone();
        let target = claim.target();
        if database
            .store_reclaim_operations()
            .await?
            .iter()
            .any(|operation| operation.authorization().target() == &target)
        {
            return Ok(AuthorizationOutcome::AlreadyJournalled);
        }
        let plan = self.writer.prepare_plan().await?;
        let owner_grant = plan.owner_grant().cloned().ok_or_else(|| {
            StoreReclaimError::Authorization(
                "Store reclaim authorization requires an active Owner grant".to_string(),
            )
        })?;
        let evidence = plan
            .sign_reclaim_evidence(claim)
            .map_err(StoreReclaimError::from)?;
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
                &plan,
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
        Ok(AuthorizationOutcome::Signed)
    }

    async fn authorize_and_resume(
        &mut self,
        claim: ReclaimClaim,
        protected_blob_packages: &BTreeSet<ObjectHash>,
    ) -> Result<(AuthorizationOutcome, ReclaimPass), StoreReclaimError> {
        let outcome = Box::pin(self.prepare_authorization(claim)).await?;
        let resumed = Box::pin(self.resume_operations(protected_blob_packages)).await?;
        Ok((outcome, resumed))
    }

    /// Run every operation the journal holds that a cycle may still run.
    ///
    /// A deterministic failure — anything whose error chain carries no
    /// transport fault — is final for that one operation: running it again
    /// reaches the same refusal, so it is marked stuck and the pass carries on
    /// with the operations behind it. A transport failure is the opposite: it
    /// says nothing about the operation, so the pass ends and the loop's
    /// backoff brings the whole thing round again.
    async fn resume_operations(
        &mut self,
        protected_blob_packages: &BTreeSet<ObjectHash>,
    ) -> Result<ReclaimPass, StoreReclaimError> {
        let database = self.database.clone();
        let mut packages_deleted = 0_u64;
        loop {
            let operations = database.runnable_store_reclaim_operations().await?;
            let mut progressed = false;
            for operation in operations {
                let operation_id = operation.operation_id();
                match Box::pin(self.run_operation(operation, protected_blob_packages)).await {
                    Ok(ReclaimStep::Deleted) => {
                        packages_deleted = packages_deleted.checked_add(1).ok_or_else(|| {
                            StoreReclaimError::Authorization(
                                "reclaimed package count exceeded u64".to_string(),
                            )
                        })?;
                        progressed = true;
                    }
                    Ok(ReclaimStep::Advanced) => progressed = true,
                    Ok(ReclaimStep::Idle) => {}
                    Err(error) if crate::sync::error::error_chain_contains_transport(&error) => {
                        return Err(error)
                    }
                    Err(error) => {
                        tracing::warn!(
                            operation = %operation_id,
                            "Store reclaim operation is stuck until the host asks for it: {error}"
                        );
                        database
                            .mark_store_reclaim_operation_stuck(operation_id, error.to_string())
                            .await?;
                    }
                }
            }
            if !progressed {
                let stuck = database.stuck_reclaim_operations().await?.len() as u64;
                return Ok(ReclaimPass {
                    packages_deleted,
                    stuck,
                });
            }
        }
    }

    /// Advance one journalled operation by one durable step.
    async fn run_operation(
        &mut self,
        operation: DurableStoreReclaimOperation,
        protected_blob_packages: &BTreeSet<ObjectHash>,
    ) -> Result<ReclaimStep, StoreReclaimError> {
        match &operation {
            DurableStoreReclaimOperation::AuthorizationCandidate { .. }
            | DurableStoreReclaimOperation::ReceiptCandidate { .. } => {
                Box::pin(self.drive_candidate(operation)).await?;
                Ok(ReclaimStep::Advanced)
            }
            DurableStoreReclaimOperation::Authorized { .. } => {
                // A package a pending blob reclaim still has to re-read waits
                // its turn: the blob operation runs in this same pass or a
                // later one, and the package goes after it. Not an error — the
                // journal holds both, and the order between them is the only
                // thing being decided.
                if self
                    .deferred_to_blob_reclaim(&operation, protected_blob_packages)
                    .await?
                {
                    return Ok(ReclaimStep::Idle);
                }
                Box::pin(self.execute_delete(operation)).await?;
                Ok(ReclaimStep::Deleted)
            }
            DurableStoreReclaimOperation::AbsentVerified { .. } => {
                Box::pin(self.prepare_receipt(operation)).await?;
                Ok(ReclaimStep::Advanced)
            }
            DurableStoreReclaimOperation::Completed { .. } => Ok(ReclaimStep::Idle),
        }
    }

    /// Whether `operation` deletes a package that a pending blob reclaim still
    /// names as the one that published its blob.
    async fn deferred_to_blob_reclaim(
        &self,
        operation: &DurableStoreReclaimOperation,
        protected_blob_packages: &BTreeSet<ObjectHash>,
    ) -> Result<bool, StoreReclaimError> {
        let package = match operation.authorization().target() {
            ReclaimTarget::StorePackage(target) => target.package.object.clone(),
            ReclaimTarget::CirclePackage(target) => target.package.package.object.clone(),
            _ => return Ok(false),
        };
        if protected_blob_packages
            .contains(&coven_protocol::remote_object::remote_object_id(&package))
        {
            return Ok(true);
        }
        Ok(self
            .database
            .package_is_retained_by_pending_blob_reclaim(package)
            .await?)
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
            ReclaimTarget::AudienceBlob(blob) => self.storage.delete_blob_object(blob.blob()).await,
            _ => self.storage.delete_protocol_object(target.object()).await,
        }
        .map_err(|source| StoreReclaimError::Delete {
            object: coven_protocol::remote_object::remote_object_id(target.object()),
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
mod audience_blob_order_tests;
#[cfg(test)]
mod authorization_tests;
#[cfg(test)]
mod snapshot_retirement_tests;
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
