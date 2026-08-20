//! Proof-gated deletion of exact Store packages covered by exact authority.
//!
//! # Which devices have to acknowledge a snapshot before reclaim deletes behind it
//!
//! Reclaim deletes the Store packages attached to commits at or behind a
//! snapshot's coverage. The commit announcements stay; only the row payload
//! goes. A device that has not yet materialized such a commit needs that
//! package to materialize it — unless it installs the snapshot image instead,
//! which already holds those rows. So the bar is: delete only what no current
//! member could still need to fetch. A device proves it needs nothing behind
//! the snapshot by having already materialized past the snapshot's coverage.
//!
//! The proof is an activated acknowledgement naming the snapshot exactly,
//! stating the snapshot's device state, and whose store cut covers the
//! snapshot's coverage. That last clause is the whole content: it says the
//! device stands at or past the coverage, and a device never moves backwards.
//!
//! ## The eligible set
//!
//! A snapshot `S` is reclaimable when every device in
//!
//! ```text
//!   { d : d is Active in the CURRENT device state
//!         and d is Active in the device state resolved at S's coverage
//!         and d's registering principal holds an active grant NOW }
//! ```
//!
//! has supplied that proof. Three conjuncts, and each rules out a shape that
//! can never supply a proof and never needs to. The third asks membership,
//! not device status: removing a member ends its grants and rotates the key
//! without touching the status of the devices it registered — those stay
//! Active for good, so a rule reading device status alone kept demanding
//! signatures from devices whose principals could never publish again, and
//! every snapshot behind them stayed unreclaimable.
//!
//! A device excluded after S's coverage is out. It was Active at S's coverage,
//! so the coverage-time state alone would demand its acknowledgement — but it
//! was excluded afterwards and will never publish again. An excluded device is
//! not a member: it cannot pull, cannot publish, and cannot re-enter except
//! through a fresh join, which bootstraps from a snapshot image at or past S.
//! There is no history behind S it could still fetch, so requiring its
//! signature demands a signature that can never exist. This is the shape that
//! blocks a store permanently — after any exclusion that postdates a snapshot's
//! coverage, that snapshot and every earlier one become unreclaimable forever.
//!
//! A device that joined after S's coverage is also out, and this one the
//! coverage-time state already handles, since it is absent there. It is worth
//! stating anyway, because the reason is not that the device is new: it is that
//! a join installs a snapshot image and materializes only the history past it,
//! so the device stands at or past S's coverage before it is ever active. It is
//! already in the position an acknowledgement would have proved.
//!
//! A device active at S's coverage and active now is in, with no relaxation. It
//! may have been idle since; it may hold nothing past S's coverage at all. It
//! is a current member that could still need what is behind S. It acknowledges,
//! or the snapshot is not reclaimable.
//!
//! A removed member's devices are out, whatever their device status says. A
//! removed member cannot pull, publish, or re-enter except by a fresh join,
//! which bootstraps at or past S — nothing behind S is reachable to it, and
//! its devices' Active status is a statement about the devices' own
//! lifecycle, not the principal's standing.
//!
//! ## What the joined-after leg rests on
//!
//! That leg assumes the snapshot a join installs has coverage at or past S's.
//! A join selects the maximal installable snapshot within its bootstrap cut, so
//! it normally lands on S or later. It does not have to: if S is rejected as
//! uninstallable the join falls back to an older generation, and Store snapshot
//! images are never themselves reclaimed, so an older generation stays
//! selectable indefinitely. A join that falls back below S's coverage then
//! needs packages reclaim has already deleted, and cannot complete. The hazard
//! is not introduced by this set — it is there today — but the leg is only as
//! sound as it is.
//!
//! ## Deliberately not part of the rule
//!
//! An acknowledgement of a later snapshot whose coverage is at or past S's
//! proves the same thing as an acknowledgement of S, but the match is by exact
//! snapshot reference, so it does not count for S. Accepting it would decouple
//! reclaim from acknowledgement timing. It is left out because reclaim already
//! selects the maximal acknowledged snapshot: if every device acknowledged that
//! later snapshot, that is what gets selected and reclaim proceeds past S
//! anyway. The narrowing would only ever matter for reclaiming S's own image,
//! which nothing reclaims.
//!
//! ## Where the set is computed
//!
//! `build_acknowledged_snapshot` walks the devices active at the coverage and
//! passes over the ones that are not active in the device state resolved at the
//! authority's accepted cut — the coverage extended to each device's latest
//! announcement, which is the newest state the walking device has verified.
//!
//! That set is a function of when it is asked, so a reclaim's evidence is
//! checked against it twice: when this device signs the evidence, and again
//! before it deletes, which can be a later cycle. The required set can only
//! shrink between those points — a device leaves it by being excluded, and one
//! that joins after the coverage was never in it — so the evidence check asks
//! that the claim carry every acknowledgement now required and permits it to
//! carry more. Requiring the two to match exactly would mean an exclusion
//! landing in that window left a signed authorization that could never execute
//! and, because an existing operation for a target blocks re-authorizing it,
//! never be replaced.
//!
//! ## The empty set, and why it holds
//!
//! If no device active at the coverage is still active, the set is empty and
//! the rule above is vacuously satisfied. That is arguably a licence to
//! reclaim, and the argument is sound as far as it goes: every current member
//! must then have joined after the coverage, and a join bootstraps from a
//! snapshot at or past `S`, so no current member needs anything behind `S` —
//! there is nobody left to ask because there is nobody left who could want it.
//!
//! It is held anyway. The joined-after leg rests on that bootstrap landing at
//! or past `S`, and the section above says where that can fail: a snapshot
//! rejected as uninstallable sends a join back to an older generation, and
//! Store snapshot images are never reclaimed, so an older generation stays
//! selectable indefinitely. With at least one live witness the acknowledgement
//! is independent evidence that some current device really is past the
//! coverage. With none, the only thing standing behind the deletion is that
//! same bootstrap assumption, which is exactly the one not yet settled. So
//! until the interaction with old-generation fallback is looked at on its own:
//! no live witness, no delete.
//!
//! This is a choice, not a derived bound, and it costs a reclaim that would
//! have been safe. It needs every device from the coverage era — the owner's
//! registration among them — to have been excluded before it can arise.

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StorePackageReclaimCoverage {
    /// The generation the leg deleted behind.
    Snapshot { generation: u64 },
    /// No snapshot every active device has acknowledged.
    NoSnapshot,
    /// A device that must acknowledge the snapshot has not, so nothing may be
    /// deleted behind it.
    MissingAcknowledgement { member: String, device_id: String },
    /// This device is not the current owner, so it does not reclaim at all.
    NotOwner,
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

    pub(super) async fn run(&mut self) -> Result<StoreReclaimResult, StoreReclaimError> {
        let database = self.database.clone();
        let membership = self.membership.clone();
        let mut packages_deleted = Box::pin(self.resume_operations()).await?;
        if !self.writer.is_current_owner(&membership) {
            return Ok(StoreReclaimResult {
                packages_deleted,
                physical_copies_deleted: packages_deleted,
                store_packages: StorePackageReclaimReport::declined(
                    StorePackageReclaimCoverage::NotOwner,
                ),
            });
        }
        let registrations = database
            .activated_store_device_registration_records()
            .await
            .map_err(StoreReclaimError::from)?;
        // A missing or unstable Store snapshot leaves Store packages uncovered but must
        // not block Circle package reclamation, which carries its own Circle coverage.
        let (coverage, store_targets) = match Box::pin(self.choose_snapshot(&registrations)).await {
            Ok(snapshot) => {
                let generation = snapshot.snapshot.reference.generation;
                let targets = self
                    .history()
                    .store_package_targets(&snapshot.snapshot.meta.coverage)
                    .await
                    .map_err(StoreReclaimError::from)?
                    .into_iter()
                    .map(|(commit, package)| (commit, package, snapshot.clone()))
                    .collect::<Vec<_>>();
                (
                    StorePackageReclaimCoverage::Snapshot { generation },
                    targets,
                )
            }
            // Store trouble must not block Circle reclamation, which carries
            // its own Circle coverage — so these two do not propagate. The
            // reason travels in the report instead of dying here, which is what
            // makes a cycle that deleted nothing say why.
            Err(StoreReclaimError::NoSnapshot) => {
                (StorePackageReclaimCoverage::NoSnapshot, Vec::new())
            }
            Err(StoreReclaimError::MissingAcknowledgement { member, device_id }) => (
                StorePackageReclaimCoverage::MissingAcknowledgement { member, device_id },
                Vec::new(),
            ),
            Err(error) => return Err(error),
        };
        let mut store_packages = StorePackageReclaimReport::declined(coverage);
        store_packages.targets_considered = store_targets.len() as u64;
        for (commit, package, snapshot) in store_targets {
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
            let authorized = Box::pin(self.prepare_authorization(ReclaimClaim::StorePackage(
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
            match authorized {
                AuthorizationOutcome::Signed => store_packages.authorized += 1,
                AuthorizationOutcome::AlreadyJournalled => store_packages.already_authorized += 1,
            }
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
            store_packages,
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
        Ok(AuthorizationOutcome::Signed)
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
