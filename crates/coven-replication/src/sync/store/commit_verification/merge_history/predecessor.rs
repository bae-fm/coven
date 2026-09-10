use super::*;

pub(crate) fn predecessor_verifies_owner(
    predecessor: &MembershipChain,
    membership: &StoreMembershipStateRef,
    owner_pubkey: &str,
    owner_grant: &coven_protocol::membership::MembershipGrantId,
) -> bool {
    let resolved = predecessor.resolved();
    StoreMembershipStateRef::from_parts(
        predecessor.head_refs().to_vec(),
        membership.recovery().to_vec(),
        resolved.state_hash,
    )
    .is_ok_and(|expected| membership == &expected)
        && predecessor.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
}

pub(super) fn predecessor_provider_admin_state(
    predecessor: &MembershipChain,
) -> &provider::ProviderAdminState {
    predecessor.resolved().provider_admin.combined_state()
}

pub(super) fn predecessor_verifies_provider_administrator(
    predecessor: &MembershipChain,
    grant_id: &provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
    expected: &provider::ProviderAdminGrantRecord,
) -> bool {
    let state = predecessor_provider_admin_state(predecessor);
    state.authorizes(grant_id, executor) && state.records().get(grant_id) == Some(expected)
}

pub(super) fn predecessor_verifies_provider_administrator_grant(
    predecessor: &MembershipChain,
    grant_id: &provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
) -> bool {
    predecessor_provider_admin_state(predecessor).authorizes(grant_id, executor)
}

/// What a search of a commit's predecessor history found.
///
/// The third answer is the one an installed baseline forces. A device that
/// advanced its baseline retired the commits under it, so a position the
/// baseline covers is in this device's predecessor history — the coverage
/// exists because the device verified and materialized every commit behind it —
/// but the body that would answer anything further about it is gone.
pub(crate) enum PredecessorSearch<'a> {
    Found(&'a VerifiedMergeHistoryCommit),
    /// The reference is one the installed baseline restates.
    Covered,
    Absent,
}

#[derive(Clone, Copy)]
pub(crate) struct VerifiedMergePredecessorHistory<'a> {
    history: &'a VerifiedMergeHistory,
    frontier: &'a [StoreBatchCommitRef],
}

impl<'a> VerifiedMergePredecessorHistory<'a> {
    pub(crate) fn new(
        history: &'a VerifiedMergeHistory,
        frontier: &'a [StoreBatchCommitRef],
    ) -> Self {
        Self { history, frontier }
    }

    /// Search the predecessor closure for a commit whose body satisfies
    /// `matches`, down to the installed baseline.
    ///
    /// `expected` is the reference the caller is really after, so that a search
    /// that reaches the baseline can say whether the thing it wanted is under
    /// it rather than reporting a bare absence. Pass `None` when the search is
    /// over bodies rather than for one known position.
    pub(super) fn find(
        &self,
        expected: Option<&StoreBatchCommitRef>,
        mut matches: impl FnMut(&StoreBatchCommitRef, &StoreBatchCommit) -> bool,
    ) -> Result<PredecessorSearch<'a>, StorePullError> {
        if expected.is_some_and(|reference| self.history.superseded(reference)) {
            return Ok(PredecessorSearch::Covered);
        }
        let mut pending = self.frontier.to_vec();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            if self.history.superseded(&reference) {
                continue;
            }
            let verified = self.history.commits.get(&reference).ok_or_else(|| {
                StorePullError::InvalidState(
                    "verified Merge predecessor graph is missing an exact commit".to_string(),
                )
            })?;
            if matches(&reference, verified.verified.value()) {
                return Ok(PredecessorSearch::Found(verified));
            }
            pending.extend(commit_predecessor_references(verified.verified.value()));
        }
        Ok(PredecessorSearch::Absent)
    }

    pub(super) fn contains_join_attempt(
        &self,
        expected: coven_protocol::store_commit::DeviceJoinAttemptId,
    ) -> Result<bool, StorePullError> {
        if let Some(baseline) = self.history.baseline.snapshot() {
            for (reference, closure) in &baseline.meta.history_summary.pending_device_joins {
                let opening = closure.verified_commit(reference)?;
                if opening
                    .device_join_attempt_decisions()
                    .contains(&DeviceJoinAttemptDecisionRef::Attempt(expected))
                {
                    return Ok(true);
                }
            }
        }
        self.find(None, |_, commit| {
            commit.device_join_attempt_decisions().iter().any(|decision| {
                matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(opened) if *opened == expected)
            })
        })
        .map(|found| matches!(found, PredecessorSearch::Found(_)))
    }

    pub(super) fn contains_reclaim_authorization(
        &self,
        authorization: &coven_protocol::reclaim::ReclaimAuthorizationRef,
    ) -> Result<bool, StorePullError> {
        if self.history.baseline.snapshot().is_some_and(|baseline| {
            baseline
                .meta
                .history_summary
                .reclaim
                .authorizations
                .get(&authorization.authorization_hash)
                .is_some_and(|accepted| &accepted.authorization == authorization)
        }) {
            return Ok(true);
        }
        self.find(None, |_, commit| {
            commit.reclaim_authorization() == Some(authorization)
        })
        .map(|found| matches!(found, PredecessorSearch::Found(_)))
    }

    /// An exact live package in an authenticated checkpoint remains an accepted
    /// predecessor even when its separately retained body is no longer reachable
    /// through the suffix. The requested history must reach that checkpoint.
    fn checkpoint_authenticates_package(
        &self,
        package: &coven_protocol::reclaim::AudienceBlobBindingPackage,
        activation: &StoreBatchCommitRef,
    ) -> Result<bool, RegistrationLoadError> {
        let Some(baseline) = self.history.baseline.snapshot() else {
            return Ok(false);
        };
        let id = coven_protocol::remote_object::remote_object_id(package.object());
        if !baseline
            .meta
            .history_summary
            .reclaim
            .packages
            .get(&id)
            .is_some_and(|retained| retained.matches_package(package, activation))
        {
            return Ok(false);
        }
        let closure = verified_merge_commit_closure(self.history, self.frontier.iter().cloned())
            .map_err(registration_attempt_error)?;
        Ok(self
            .history
            .baseline
            .coverage()
            .commits()
            .values()
            .all(|reference| closure.contains(reference)))
    }

    /// Bind a row blob to the package that published it. The blob is never named in a
    /// commit body — only inside the package's bindings — so what a commit establishes
    /// is that the named package was activated by a commit in this device's
    /// predecessor history. The blob's own reference is self-binding: its object key is
    /// derived from its locator, which names the audience and uploading device, and
    /// the audience must be the one the package addresses. Reading the bindings
    /// themselves requires the package's audience key, which a Store member outside a
    /// Circle does not hold; the Owner re-reads them before authorizing any delete.
    pub(super) fn validate_package_bound_reclaim_target(
        &self,
        target: &coven_protocol::reclaim::ReclaimTarget,
        activation: &coven_protocol::reclaim::CirclePackageReclaimTarget,
    ) -> Result<(), RegistrationLoadError> {
        let coven_protocol::reclaim::ReclaimTarget::AudienceBlob(blob) = target else {
            return Err(RegistrationLoadError::Invalid(
                "reclaim target is not published by a package binding".to_string(),
            ));
        };
        if blob.blob().locator().audience()
            != coven_protocol::blob::locator::RemoteAudience::Circle(activation.package.circle_id)
        {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence blob names a package for another audience".to_string(),
            ));
        }
        let package =
            coven_protocol::reclaim::AudienceBlobBindingPackage::Circle(activation.package.clone());
        if self.checkpoint_authenticates_package(&package, &activation.activation)? {
            return Ok(());
        }
        let expected = activation.activation.clone();
        let activating = match self
            .find(Some(&expected), |candidate, _| candidate == &expected)
            .map_err(registration_attempt_error)?
        {
            PredecessorSearch::Found(activating) => activating,
            PredecessorSearch::Covered => {
                return Err(RegistrationLoadError::Invalid(
                    "retired blob package has no exact retained activation in predecessor history"
                        .to_string(),
                ));
            }
            PredecessorSearch::Absent => {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim evidence blob activation is absent from predecessor history"
                        .to_string(),
                ));
            }
        };
        let names_package = activating
            .verified
            .value()
            .circle_packages()
            .contains(&activation.package);
        if !names_package {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence blob package differs from its exact activation".to_string(),
            ));
        }
        Ok(())
    }

    /// Bind a reclaim target to the retained Store commit that published it: the
    /// commit must sit in this device's predecessor history and its body must name the
    /// exact object the evidence authorizes deleting.
    pub(super) fn validate_commit_activated_reclaim_target(
        &self,
        target: &coven_protocol::reclaim::ReclaimTarget,
        activating_commit: &StoreBatchCommitRef,
    ) -> Result<(), RegistrationLoadError> {
        let package = match target {
            coven_protocol::reclaim::ReclaimTarget::StorePackage(target) => Some(
                coven_protocol::reclaim::AudienceBlobBindingPackage::Store(target.package.clone()),
            ),
            coven_protocol::reclaim::ReclaimTarget::CirclePackage(target) => Some(
                coven_protocol::reclaim::AudienceBlobBindingPackage::Circle(target.package.clone()),
            ),
            _ => None,
        };
        if let Some(package) = &package {
            if self.checkpoint_authenticates_package(package, activating_commit)? {
                return Ok(());
            }
        }
        let expected = activating_commit.clone();
        let activation = match self
            .find(Some(&expected), |candidate, _| candidate == &expected)
            .map_err(registration_attempt_error)?
        {
            PredecessorSearch::Found(activation) => activation,
            PredecessorSearch::Covered => {
                if package.is_some() {
                    return Err(RegistrationLoadError::Invalid(
                        "retired package has no exact retained activation in predecessor history"
                            .to_string(),
                    ));
                }
                return Ok(());
            }
            PredecessorSearch::Absent => {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim evidence package activation is absent from predecessor history"
                        .to_string(),
                ));
            }
        };
        let names_target = match target {
            coven_protocol::reclaim::ReclaimTarget::StorePackage(store) => {
                activation.verified.value().store_package() == Some(&store.package)
            }
            coven_protocol::reclaim::ReclaimTarget::CirclePackage(circle) => activation
                .verified
                .value()
                .circle_packages()
                .contains(&circle.package),
            coven_protocol::reclaim::ReclaimTarget::CircleBootstrapImage(bootstrap) => activation
                .verified
                .value()
                .circle_controls()
                .iter()
                .flat_map(|control| control.objects.access.iter())
                .any(|access| {
                    access.bootstrap.as_ref() == Some(&bootstrap.coverage.bootstrap.image)
                }),
            coven_protocol::reclaim::ReclaimTarget::CircleSnapshotImage(_)
            | coven_protocol::reclaim::ReclaimTarget::AudienceBlob(_) => {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim target claims a Store commit activation it is not published by"
                        .to_string(),
                ));
            }
        };
        if !names_target {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence target differs from its exact package activation".to_string(),
            ));
        }
        Ok(())
    }
}

impl MergeHistoryVerifier<'_> {
    pub(crate) fn verified_circle_predecessors(
        &self,
        candidate: &StoreBatchCommit,
        circle_id: coven_protocol::circle::CircleId,
        prepared: &[&coven_protocol::circle_activation::VerifiedCircleActivations],
    ) -> Result<Vec<coven_protocol::circle_activation::VerifiedCircleReference>, StorePullError>
    {
        let closure =
            verified_merge_commit_closure(&self.history, commit_predecessor_references(candidate))?;
        let mut controls = Vec::new();
        for group in prepared
            .iter()
            .filter(|group| closure.contains(group.stream_activations().activating_commit()))
        {
            for activation in group
                .circles()
                .iter()
                .filter(|activation| activation.circle_id == circle_id)
            {
                let predecessor = self
                    .history
                    .commits
                    .get(group.stream_activations().activating_commit())
                    .ok_or_else(|| {
                        StorePullError::InvalidState(
                            "prepared Circle predecessor has no retained verified commit"
                                .to_string(),
                        )
                    })?;
                verify_circle_control_in_predecessor(&predecessor.verified, activation)?;
                controls.push(activation.clone());
            }
        }
        Ok(controls)
    }

    pub(crate) fn verify_prepared_circle_predecessor(
        &self,
        candidate: &StoreBatchCommit,
        activating_commit: &StoreBatchCommitRef,
        activation: &coven_protocol::circle_activation::VerifiedCircleReference,
    ) -> Result<(), StorePullError> {
        let frontier = commit_predecessor_references(candidate);
        let predecessors = VerifiedMergePredecessorHistory::new(&self.history, &frontier);
        let PredecessorSearch::Found(predecessor) =
            predecessors.find(None, |reference, _| reference == activating_commit)?
        else {
            return Err(StorePullError::InvalidState(
                "prepared Circle control is absent from the candidate's exact predecessor history"
                    .to_string(),
            ));
        };
        verify_circle_control_in_predecessor(&predecessor.verified, activation)
    }
}

fn verify_circle_control_in_predecessor(
    predecessor: &VerifiedStoreBatchCommit,
    activation: &coven_protocol::circle_activation::VerifiedCircleReference,
) -> Result<(), StorePullError> {
    if !predecessor
        .value()
        .circle_controls()
        .contains(&activation.reference)
    {
        return Err(StorePullError::InvalidState(
            "prepared Circle control is absent from its exact activating commit".to_string(),
        ));
    }
    coven_protocol::circle_activation::verify_control_context_for_verified_commit(
        &activation.reference,
        &activation.control,
        predecessor,
    )
    .map_err(crate::sync::store::circles::CircleOperationError::from)
    .map_err(crate::sync::store::circles::CirclePackageReadError::from)
    .map_err(StorePullError::from)
}
