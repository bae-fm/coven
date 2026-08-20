//! Resolving the row data a device-join bootstrap installs.
//!
//! A bootstrap installs the joining device's whole history closure up to the
//! attempt activation and advances its materialized position over all of it.
//! Every commit past the newest snapshot the device installed carries its rows
//! in an encrypted package, and installation runs inside one database
//! transaction that cannot read the cloud — so the packages are read,
//! decrypted and verified here first, exactly the way an ordinary pull does.

use super::*;
use coven_database::{
    DeviceJoinBootstrapPlan, DeviceJoinBootstrapRowData, PreparedMergeMaterializationPackage,
    ResolvedDeviceJoinBootstrap,
};
use coven_foundation::stage_timing::StageTimings;
use coven_protocol::circle_activation::VerifiedCircleActivations;
use coven_protocol::membership::{LocalStoreMembership, MembershipChain};
use std::collections::BTreeMap;

impl PullHistory<'_, '_> {
    /// Report the breakdown whichever way the resolution ends, and say how much
    /// history it covered: a bootstrap over a store whose newest snapshot is a
    /// hundred changesets back reads a package per uncovered commit, and the
    /// only way to tell that cost from provider latency is to count both.
    pub(crate) async fn resolve_device_join_bootstrap(
        &mut self,
        plan: DeviceJoinBootstrapPlan,
        membership: &MembershipChain,
        identity: &UserKeypair,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
    ) -> Result<ResolvedDeviceJoinBootstrap, StorePullError> {
        let mut timings =
            StageTimings::counting("Device join bootstrap resolution", self.provider_requests());
        let outcome = Box::pin(self.resolve_device_join_bootstrap_staged(
            plan,
            membership,
            identity,
            routing_encryption,
            &mut timings,
        ))
        .await;
        timings.report();
        outcome
    }

    async fn resolve_device_join_bootstrap_staged(
        &mut self,
        plan: DeviceJoinBootstrapPlan,
        membership: &MembershipChain,
        identity: &UserKeypair,
        routing_encryption: Option<&coven_keys::encryption::EncryptionService>,
        timings: &mut StageTimings,
    ) -> Result<ResolvedDeviceJoinBootstrap, StorePullError> {
        let (plan, unrepresented) = self
            .unrepresented_device_join_bootstrap_commits(plan)
            .await
            .map_err(|error| {
                StorePullError::context("read device join bootstrap coverage", error)
            })?;
        let local_store_membership =
            LocalStoreMembership::from_membership(membership, Some(identity))
                .map_err(StorePullMembershipError::State)
                .map_err(StorePullError::Membership)?;
        let routing_key = if self.has_scoped_graph() {
            let encryption = routing_encryption.ok_or_else(|| {
                StorePullError::InvalidState(
                    "scoped device join bootstrap requires row-routing encryption".to_string(),
                )
            })?;
            Some(
                coven_protocol::circle::derive_row_routing_key(
                    encryption,
                    self.root().store_root_hash,
                )
                .map_err(|error| StorePullError::context("derive row routing key", error))?,
            )
        } else {
            None
        };
        let receiver_wall_ms = self.receive_wall_ms();
        let schema = self.package_schema().await.map_err(|error| {
            StorePullError::Database(coven_database::DbError::context(
                "load device join bootstrap package schema",
                error,
            ))
        })?;
        tracing::info!(
            plan_commits = plan.commits.len(),
            uncovered_commits = unrepresented.len(),
            "Device join bootstrap resolves the history its snapshot does not cover"
        );
        // The commit that failed is the one whose split is wanted, so the inner
        // breakdown is reported before the failure propagates.
        let mut inner =
            StageTimings::counting("Device join bootstrap row data", self.provider_requests());
        let row_data = Box::pin(self.resolve_bootstrap_row_data(
            &plan,
            unrepresented,
            local_store_membership,
            routing_key.as_ref(),
            &schema,
            timings,
            &mut inner,
        ))
        .await;
        inner.report();
        Ok(ResolvedDeviceJoinBootstrap {
            plan,
            row_data: row_data?,
            local_store_membership,
            routing_key,
            receiver_wall_ms,
        })
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_bootstrap_row_data(
        &mut self,
        plan: &DeviceJoinBootstrapPlan,
        unrepresented: Vec<StoreBatchCommitRef>,
        local_store_membership: LocalStoreMembership,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        schema: &std::sync::Arc<coven_database::TableSchema>,
        timings: &mut StageTimings,
        inner: &mut StageTimings,
    ) -> Result<BTreeMap<StoreBatchCommitRef, DeviceJoinBootstrapRowData>, StorePullError> {
        let mut row_data = BTreeMap::new();
        for reference in unrepresented {
            let prepared = plan
                .commits
                .iter()
                .find(|prepared| prepared.reference == reference)
                .ok_or_else(|| {
                    StorePullError::InvalidState(
                        "device join bootstrap coverage names a commit outside its plan"
                            .to_string(),
                    )
                })?;
            let candidate = Candidate {
                verified: prepared.commit.clone(),
                package: None,
                registrations: prepared.registrations.clone(),
            };
            let resolved = timings
                .stage(
                    "resolve commit row data",
                    Box::pin(Self::resolve_bootstrap_commit_row_data(
                        self,
                        candidate,
                        local_store_membership,
                        routing_key,
                        schema,
                        inner,
                    )),
                )
                .await?;
            row_data.insert(reference, resolved);
        }
        Ok(row_data)
    }

    async fn resolve_bootstrap_commit_row_data(
        &mut self,
        candidate: Candidate,
        local_store_membership: LocalStoreMembership,
        routing_key: Option<&coven_protocol::circle::RowRoutingKey>,
        schema: &std::sync::Arc<coven_database::TableSchema>,
        timings: &mut StageTimings,
    ) -> Result<DeviceJoinBootstrapRowData, StorePullError> {
        let verified = candidate.verified.clone();
        let commit = verified.value().clone();
        let reference = verified.reference().clone();
        // A Store membership control commit activates its Circle stream through
        // the same verification the pull performs; every other commit resolves
        // its activations from the Circle control objects it names.
        //
        // The joining device holds no Circle access of its own — its
        // registration is activated only after this bootstrap — so it reads the
        // activations without an identity. Circle rows reach it through the
        // Circle bootstrap image on its first pull, the same way any device that
        // gains access later receives them.
        let circle_activations = if commit.control().is_some() {
            timings
                .stage("verify commits", self.verify_refs([reference.clone()]))
                .await?;
            self.verified_commit(&reference)
                .and_then(|candidate| candidate.membership_control)
                .ok_or_else(|| {
                    StorePullError::InvalidState(
                        "device join bootstrap membership control is absent from its verified history"
                            .to_string(),
                    )
                })?
        } else if commit.circle_controls().is_empty() && commit.stream_activations().is_empty() {
            VerifiedCircleActivations::none(&commit, &reference)
                .map_err(StorePullError::Protocol)?
        } else {
            // Reading Circle activations judges the commit against its whole
            // verified predecessor closure, so the closure has to be in the
            // verifier first. A same-provider join carries its plan across from
            // the donor device and has verified none of it here yet.
            timings
                .stage(
                    "verify commits",
                    self.verify_refs(commit_predecessor_references(&commit)),
                )
                .await?;
            let membership_prefix =
                self.verified_membership_prefix(commit_predecessor_references(&commit))?;
            let verified_prefix = VerifiedStreamActivationPrefix::empty();
            timings
                .stage(
                    "read Circle activations",
                    self.circles().activations().load_payload(
                        &verified,
                        None,
                        routing_key,
                        &verified_prefix,
                        &membership_prefix,
                    ),
                )
                .await
                .map_err(super::CirclePackageReadError::from)?
        };
        let membership_closure = timings
            .stage(
                "read membership objects",
                self.verified_membership_objects(&reference, &commit),
            )
            .await?;
        let membership_objects = membership_closure
            .as_ref()
            .map(|closure| closure.objects().clone());
        let membership_remote_objects = membership_closure
            .map(VerifiedMergeMembershipClosure::into_remote_objects)
            .unwrap_or_default();

        let mut packages = Vec::new();
        if commit.store_package().is_some() {
            let bytes = timings
                .stage("read Store package", self.load_store_package(&reference))
                .await?
                .ok_or_else(|| {
                    StorePullError::InvalidState(format!(
                        "device join bootstrap commit {}/{} names a Store package it cannot read",
                        reference.coord.stream_id,
                        reference.coord.sequence()
                    ))
                })?
                .value;
            let package = candidate
                .parse_store_package(&bytes)
                .map_err(|reason| bootstrap_row_data_error(&reference, reason))?;
            packages.push(
                timings
                    .stage(
                        "fetch blobs",
                        self.prepare_bootstrap_package(&reference, package, schema),
                    )
                    .await?,
            );
        }
        let author = verified.author().clone();
        let circle_packages = timings
            .stage(
                "read Circle packages",
                self.circles().packages().load_applicable(
                    &verified,
                    circle_activations.circles(),
                    &author,
                    local_store_membership,
                ),
            )
            .await?;
        for loaded in &circle_packages {
            let package = candidate
                .parse_circle_package(loaded)
                .map_err(|reason| bootstrap_row_data_error(&reference, reason))?;
            packages.push(
                timings
                    .stage(
                        "fetch blobs",
                        self.prepare_bootstrap_package(&reference, package, schema),
                    )
                    .await?,
            );
        }
        Ok(DeviceJoinBootstrapRowData {
            circle_activations,
            membership_objects,
            membership_remote_objects,
            packages,
        })
    }

    async fn prepare_bootstrap_package(
        &self,
        reference: &StoreBatchCommitRef,
        package: coven_protocol::audience_package::AudiencePackage,
        schema: &std::sync::Arc<coven_database::TableSchema>,
    ) -> Result<PreparedMergeMaterializationPackage, StorePullError> {
        // A pull holds its position when a package is unreadable and retries the
        // commit later. A bootstrap has no position to hold — it installs the
        // whole closure at once — so an unreadable package fails the join.
        self.prepare_package(
            package,
            schema.clone(),
            super::history::PackageBlobPolicy::TrustBindings,
        )
        .await?
        .map_err(|reason| bootstrap_row_data_error(reference, reason))
    }
}

fn bootstrap_row_data_error(
    reference: &StoreBatchCommitRef,
    reason: HeldStorePositionReason,
) -> StorePullError {
    StorePullError::InvalidState(format!(
        "device join bootstrap cannot read the row data at {}/{}: {reason:?}",
        reference.coord.stream_id,
        reference.coord.sequence()
    ))
}
