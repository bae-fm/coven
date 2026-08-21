use super::*;

pub(crate) fn predecessor_verifies_owner(
    predecessor: &MembershipChain,
    membership: &StoreMembershipStateRef,
    owner_pubkey: &str,
    owner_grant: &coven_protocol::membership::MembershipGrantId,
) -> bool {
    let MembershipStatus::Resolved(resolved) = predecessor.status() else {
        return false;
    };
    StoreMembershipStateRef::from_parts(
        predecessor.head_refs().to_vec(),
        predecessor.resolution_refs().to_vec(),
        membership.recovery().to_vec(),
        resolved.state_hash,
    )
    .is_ok_and(|expected| membership == &expected)
        && predecessor.active_owner_grant(owner_pubkey).as_ref() == Some(owner_grant)
}

pub(super) fn predecessor_provider_admin_state(
    predecessor: &MembershipChain,
) -> Option<&provider::ProviderAdminState> {
    let MembershipStatus::Resolved(resolved) = predecessor.status() else {
        return None;
    };
    Some(resolved.provider_admin.combined_state())
}

pub(super) fn predecessor_verifies_provider_administrator(
    predecessor: &MembershipChain,
    grant_id: &provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
    expected: &provider::ProviderAdminGrantRecord,
) -> bool {
    let Some(state) = predecessor_provider_admin_state(predecessor) else {
        return false;
    };
    state.authorizes(grant_id, executor) && state.records().get(grant_id) == Some(expected)
}

pub(super) fn predecessor_verifies_provider_administrator_grant(
    predecessor: &MembershipChain,
    grant_id: &provider::ProviderAdminGrantId,
    executor: &StoreDeviceRegistrationRef,
) -> bool {
    predecessor_provider_admin_state(predecessor)
        .is_some_and(|state| state.authorizes(grant_id, executor))
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
        expected: &DeviceJoinAttemptRef,
    ) -> Result<bool, StorePullError> {
        self.find(None, |_, commit| {
            commit.device_join_attempt_decisions().iter().any(|decision| {
                matches!(decision, DeviceJoinAttemptDecisionRef::Attempt(reference) if reference == expected)
            })
        })
        .map(|found| matches!(found, PredecessorSearch::Found(_)))
    }

    fn contains_join_outcome(
        &self,
        expected: &DeviceJoinOutcomeRef,
    ) -> Result<bool, StorePullError> {
        self.find(None, |_, commit| {
            commit
                .device_join_outcomes()
                .binary_search(expected)
                .is_ok()
        })
        .map(|found| matches!(found, PredecessorSearch::Found(_)))
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
        activation: &coven_protocol::reclaim::PackageBlobBindingActivation<'_>,
    ) -> Result<(), RegistrationLoadError> {
        let coven_protocol::reclaim::ReclaimTarget::AudienceBlob(blob) = target else {
            return Err(RegistrationLoadError::Invalid(
                "reclaim target is not published by a package binding".to_string(),
            ));
        };
        let expected = activation.activation.clone();
        let activating = match self
            .find(Some(&expected), |candidate, _| candidate == &expected)
            .map_err(registration_attempt_error)?
        {
            PredecessorSearch::Found(activating) => activating,
            // The activation is under this device's replay baseline: it is in
            // the predecessor history, and its body — which is what would name
            // the package again — was retired with the rest of the history the
            // baseline restates. The device checked that binding when it
            // materialized the activation, which is why the position is covered
            // at all.
            PredecessorSearch::Covered => return Ok(()),
            PredecessorSearch::Absent => {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim evidence blob activation is absent from predecessor history"
                        .to_string(),
                ))
            }
        };
        let names_package = match activation.package {
            coven_protocol::reclaim::AudienceBlobBindingPackage::Store(package) => {
                activating.verified.value().store_package() == Some(package)
            }
            coven_protocol::reclaim::AudienceBlobBindingPackage::Circle(package) => activating
                .verified
                .value()
                .circle_packages()
                .contains(package),
        };
        if !names_package {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence blob package differs from its exact activation".to_string(),
            ));
        }
        if blob.blob.locator().audience() != activation.package.remote_audience() {
            return Err(RegistrationLoadError::Invalid(
                "reclaim evidence blob names a package for another audience".to_string(),
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
        let expected = activating_commit.clone();
        let activation = match self
            .find(Some(&expected), |candidate, _| candidate == &expected)
            .map_err(registration_attempt_error)?
        {
            PredecessorSearch::Found(activation) => activation,
            // Under the replay baseline. See the note on
            // `validate_package_bound_reclaim_target`: the position is in this
            // device's history on the coverage's authority, and the body that
            // would name the target again is retired.
            PredecessorSearch::Covered => return Ok(()),
            PredecessorSearch::Absent => {
                return Err(RegistrationLoadError::Invalid(
                    "reclaim evidence package activation is absent from predecessor history"
                        .to_string(),
                ))
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

    pub(super) fn validate_commit_join_cleanup_receipts(
        &self,
        activating_author: &StoreDeviceRegistration,
        predecessor: Option<&MembershipChain>,
        join_evidence: &VerifiedCommitJoinEvidence,
    ) -> Result<(), RegistrationLoadError> {
        let predecessor = predecessor.ok_or_else(|| {
            RegistrationLoadError::Invalid(
                "device join cleanup activation has no exact predecessor authority".to_string(),
            )
        })?;
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "device join cleanup activation author is not an active Owner".to_string(),
            ));
        }
        for loaded in &join_evidence.cleanup_receipts {
            if !self
                .contains_join_outcome(&loaded.receipt.cancellation)
                .map_err(registration_attempt_error)?
            {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup receipt outcome is absent from its verified predecessor history"
                        .to_string(),
                ));
            }
            let attempt = join_evidence.attempts.get(&loaded.attempt).ok_or_else(|| {
                RegistrationLoadError::Invalid(
                    "device join cleanup receipt has no verified exact attempt".to_string(),
                )
            })?;
            let expected_administrator = &attempt.provider_approval.request.offer.provider_admin;
            if !predecessor_verifies_provider_administrator(
                predecessor,
                &loaded.receipt.provider_admin_grant,
                &loaded.receipt.executor,
                expected_administrator,
            ) {
                return Err(RegistrationLoadError::Invalid(
                    "device join cleanup executor is not the exact effective provider administrator"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn verify_commit_join_evidence(
        &self,
        commit: &StoreBatchCommit,
        loaded: LoadedCommitJoinEvidence,
        predecessor: &MembershipChain,
    ) -> Result<VerifiedCommitJoinEvidence, StorePullError> {
        if loaded.attempts.is_empty() {
            return Ok(VerifiedCommitJoinEvidence {
                commit: commit.clone(),
                attempts: BTreeMap::new(),
                cleanup_receipts: loaded.cleanup_receipts,
            });
        }
        let mut attempts = BTreeMap::new();
        for (reference, evidence) in loaded.attempts {
            let approval = &evidence.attempt.value.provider_approval;
            let provider_admin = &approval.request.offer.provider_admin;
            let verifies_administrator = match approval.access_grant() {
                None => predecessor_verifies_provider_administrator(
                    predecessor,
                    &provider_admin.grant_id,
                    &provider_admin.administrator,
                    provider_admin,
                ),
                Some(access) => match self.find(Some(&access.activation), |candidate, _| {
                    candidate == &access.activation
                })? {
                    PredecessorSearch::Found(verified) => {
                        predecessor_verifies_provider_administrator(
                            &verified.predecessor_membership,
                            &access.grant.administrator_grant,
                            &verified.verified.value().author_registration,
                            provider_admin,
                        )
                    }
                    // The activation is under the replay baseline, which the
                    // device only holds because it verified that grant when it
                    // materialized the commit carrying it.
                    PredecessorSearch::Covered => true,
                    PredecessorSearch::Absent => false,
                },
            };
            if !verifies_administrator {
                return Err(StorePullError::InvalidState(
                    "device join attempt lacks exact Merge provider-administrator authority"
                        .to_string(),
                ));
            }
            attempts.insert(reference, evidence.attempt.value);
        }
        Ok(VerifiedCommitJoinEvidence {
            commit: commit.clone(),
            attempts,
            cleanup_receipts: loaded.cleanup_receipts,
        })
    }
}
