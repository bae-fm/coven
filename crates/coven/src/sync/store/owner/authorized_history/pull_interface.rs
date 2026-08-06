use super::*;

impl<'storage> AuthorizedStoreHistory<'storage> {
    pub(crate) async fn pull(
        &mut self,
        membership: &crate::protocol::membership::MembershipChain,
        identity: Option<&UserKeypair>,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<pull::StorePullExecution, pull::StorePullError> {
        pull::AuthorizedPull::load(self, membership, identity, routing_encryption)
            .await?
            .execute()
            .await
    }

    pub(crate) async fn pull_package_schema(
        &self,
    ) -> Result<std::sync::Arc<crate::database::TableSchema>, crate::database::DbError> {
        Ok(std::sync::Arc::new(
            self.database.table_schema_for_apply().await?,
        ))
    }

    pub(crate) fn pull_store_blob_protection(
        &self,
    ) -> Result<crate::protocol::objects::BlobSpoolProtection, crate::protocol::objects::StorageError>
    {
        self.blob_source.store_protection()
    }

    pub(crate) async fn prepare_pull_package(
        &self,
        package: crate::protocol::audience_package::AudiencePackage,
        blob_protection: crate::protocol::objects::BlobSpoolProtection,
        schema: std::sync::Arc<crate::database::TableSchema>,
    ) -> Result<
        Result<PreparedMergeMaterializationPackage, pull::HeldStorePositionReason>,
        pull::StorePullError,
    > {
        let changeset =
            match crate::database::ValidatedChangeset::new(package.changeset().to_vec(), schema) {
                Ok(changeset) => changeset,
                Err(crate::database::ChangesetIdentityError::Row(error)) => {
                    return Ok(Err(pull::HeldStorePositionReason::InvalidRowIdentity {
                        table: error.table().to_string(),
                        reason: error.to_string(),
                    }))
                }
                Err(error) => {
                    return Ok(Err(pull::HeldStorePositionReason::InvalidChangeset(
                        error.to_string(),
                    )))
                }
            };
        let changes = match crate::database::walk_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => return Ok(Err(pull::HeldStorePositionReason::InvalidChangeset(error))),
        };
        let old_changes = match crate::database::walk_old_changeset(changeset.bytes()) {
            Ok(changes) => changes,
            Err(error) => return Ok(Err(pull::HeldStorePositionReason::InvalidChangeset(error))),
        };
        let mut eager = Vec::new();
        for change in &changes {
            if change.op == crate::changeset::ChangeOp::Delete {
                continue;
            }
            let blob = match self.database.blob_ref_from_change(change) {
                Ok(blob) => blob,
                Err(error) => {
                    return Ok(Err(pull::HeldStorePositionReason::InvalidChangeset(
                        error.to_string(),
                    )))
                }
            };
            let Some(blob) = blob else {
                continue;
            };
            if blob.fill != crate::protocol::blob::CacheFill::CacheEager {
                continue;
            }
            let row_id = match change.pk() {
                Some(row_id) => row_id,
                None => {
                    return Ok(Err(pull::HeldStorePositionReason::InvalidChangeset(
                        format!(
                            "blob-bearing incoming row {:?} has no primary key",
                            change.table
                        ),
                    )))
                }
            };
            let matches = package
                .blob_bindings()
                .iter()
                .filter(|binding| {
                    binding.table() == change.table
                        && binding.row_id() == row_id
                        && binding.blob().locator().namespace() == blob.namespace
                        && binding.blob().locator().blob_id() == blob.id
                })
                .collect::<Vec<_>>();
            let [binding] = matches.as_slice() else {
                return Ok(Err(pull::HeldStorePositionReason::InvalidChangeset(
                    format!(
                        "incoming eager blob row {:?}/{row_id:?} has {} exact locator bindings",
                        change.table,
                        matches.len()
                    ),
                )));
            };
            eager.push(binding.blob().clone());
        }
        let mut verified = Vec::new();
        let mut failures = Vec::new();
        for binding in package.blob_bindings() {
            let stored = binding.blob();
            if verified.iter().any(|candidate| candidate == stored) {
                continue;
            }
            verified.push(stored.clone());
            let locator = stored.locator();
            let retain = eager.iter().any(|download| download == stored);
            if let Err(cause) = self
                .blob_source
                .verify_plaintext_with_protection(
                    &self.blob_cache,
                    stored,
                    blob_protection.clone(),
                    retain,
                )
                .await
            {
                failures.push(pull::BlobDownloadFailure {
                    namespace: locator.namespace().to_string(),
                    id: locator.blob_id().to_string(),
                    cause,
                });
            }
        }
        if !failures.is_empty() {
            let failures = pull::BlobDownloadFailures::new(failures);
            if failures.has_transport_failure() {
                return Err(pull::StorePullError::BlobDownloads(failures));
            }
            return Ok(Err(pull::HeldStorePositionReason::BlobDownloadFailed));
        }
        if let Err(error) = self
            .database
            .validate_local_blob_cleanup_changes(&old_changes, &changes)
        {
            return Ok(Err(pull::HeldStorePositionReason::InvalidChangeset(
                error.to_string(),
            )));
        }
        Ok(Ok(PreparedMergeMaterializationPackage {
            package,
            changeset,
        }))
    }

    pub(crate) fn pull_has_scoped_graph(&self) -> bool {
        self.database.has_scoped_graph()
    }

    pub(crate) fn pull_schema_version(&self) -> u32 {
        self.database.schema_version()
    }

    pub(crate) fn pull_receive_wall_ms(&self) -> u64 {
        self.database.receive_wall_ms()
    }

    pub(crate) async fn pull_materialized_frontier(
        &self,
    ) -> Result<std::collections::BTreeMap<String, StoreBatchCommitRef>, crate::database::DbError>
    {
        self.database.materialized_frontier().await
    }

    pub(crate) async fn pull_device_state_for_cut(
        &self,
        cut: &crate::protocol::store_commit::StoreHistoryCut,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), crate::database::DbError> {
        self.database.store_device_state_for_history_cut(cut).await
    }

    pub(crate) async fn pull_device_state_for_order(
        &self,
        order: &crate::protocol::store_commit::StoreCommitOrder,
    ) -> Result<(StoreDeviceStateRef, ResolvedStoreDeviceState), crate::database::DbError> {
        self.database.store_device_state_for_order(order).await
    }

    pub(crate) async fn pull_exact_materialized_ref(
        &self,
        stream_id: &str,
        sequence: u64,
    ) -> Result<Option<StoreBatchCommitRef>, crate::database::DbError> {
        self.database
            .exact_materialized_ref(stream_id, sequence)
            .await
    }

    pub(crate) async fn pull_snapshot_coverage(
        &self,
    ) -> Result<CommitFrontier, crate::database::DbError> {
        self.database.snapshot_coverage_frontier().await
    }

    pub(crate) async fn pull_exclusion_freezes(
        &self,
    ) -> Result<Vec<crate::protocol::store_commit::StoreDeviceProposalAck>, crate::database::DbError>
    {
        self.database.store_device_exclusion_freezes().await
    }

    pub(crate) async fn record_pull_circle_close_exclusions(
        &self,
        exclusions: Vec<crate::protocol::circle_activation::LocalCircleExclusion>,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .record_circle_close_exclusions(exclusions)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn commit_pull_materialization(
        &self,
        materialization: PreparedMergeMaterialization,
        retractions: Vec<crate::protocol::remote_object::VerifiedCandidateNonactivation>,
        local_store_membership: pull::LocalStoreMembership,
        routing_key: Option<crate::protocol::circle::RowRoutingKey>,
        receiver_wall_ms: u64,
    ) -> Result<crate::protocol::membership::ApplyOutcome, crate::database::DbError> {
        self.database
            .apply_received_merge_materialization(
                materialization,
                retractions,
                local_store_membership,
                routing_key,
                receiver_wall_ms,
            )
            .await
    }

    pub(crate) async fn prepare_pull_retained_history(
        &mut self,
    ) -> Result<Vec<crate::database::OwnedVerifiedMergeMaterialization>, pull::StorePullError> {
        let retained_refs = self.database.retained_merge_materialization_refs().await?;
        self.history_verifier.verify_refs(retained_refs).await?;
        let retained_commit_proofs = self.history_verifier.retained_commit_proofs();
        let retained = self
            .database
            .retained_merge_replay_inputs_with_verified_commits(
                self.history_verifier.verified_root().reference().clone(),
                retained_commit_proofs,
            )
            .await?;
        self.resume_merge_retraction_cleanups().await?;
        Ok(retained)
    }

    pub(crate) async fn load_active_pull_registrations(
        &self,
    ) -> Result<
        Vec<crate::protocol::store_commit::ReferencedStoreDeviceRegistration>,
        pull::StorePullError,
    > {
        let durable = self
            .database
            .activated_store_device_registration_records()
            .await?;
        let mut verified = Vec::with_capacity(durable.len());
        for expected in durable {
            let reference = expected.reference();
            let opened = self.history_verifier.load_registration(reference).await?;
            if &opened.value != expected.value() {
                return Err(pull::StorePullError::InvalidState(format!(
                    "activated Store registration {} differs from its exact remote bytes",
                    reference.device_id
                )));
            }
            if !matches!(
                opened.value.store_commits,
                crate::protocol::store_commit::DeviceStreamAnchor::StoreAnnouncements { .. }
            ) {
                return Err(pull::StorePullError::InvalidState(format!(
                    "activated Store registration {} has no Merge announcement anchor",
                    reference.device_id
                )));
            }
            verified.push(expected);
        }
        Ok(verified)
    }

    pub(crate) async fn discover_pull_owner_recoveries(
        &self,
        membership: &MembershipChain,
    ) -> Result<
        Vec<crate::protocol::store_commit::ReferencedStoreDeviceRegistration>,
        pull::StorePullError,
    > {
        self.history_verifier
            .discover_owner_recoveries(membership)
            .await
    }

    pub(crate) async fn discover_pull_stream(
        &mut self,
        registration_ref: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
        registration: &crate::protocol::store_commit::StoreDeviceRegistration,
        inactive_accepted_cut: Option<&crate::protocol::store_commit::StoreHistoryCut>,
    ) -> Result<pull::MergeStreamDiscovery, pull::StorePullError> {
        self.history_verifier
            .discover_merge_stream(registration_ref, registration, inactive_accepted_cut)
            .await
    }

    pub(crate) async fn verify_pull_refs(
        &mut self,
        references: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), pull::StorePullError> {
        self.history_verifier.verify_refs(references).await
    }

    pub(crate) fn verified_pull_commit(
        &self,
        reference: &StoreBatchCommitRef,
    ) -> Option<pull::VerifiedPullCandidate> {
        self.history_verifier.verified_pull_candidate(reference)
    }

    pub(crate) fn verified_pull_membership_prefix(
        &self,
        predecessors: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<VerifiedMergeMembershipPrefix, pull::StorePullError> {
        self.history_verifier
            .verified_membership_prefix(predecessors)
    }

    pub(crate) async fn load_pull_store_package(
        &mut self,
        reference: &StoreBatchCommitRef,
    ) -> Result<
        Option<crate::protocol::objects::VerifiedObject<Vec<u8>>>,
        crate::protocol::objects::StoreObjectError,
    > {
        self.history_verifier.load_store_package(reference).await
    }

    pub(crate) async fn load_pull_predecessor_membership(
        &mut self,
        state: &StoreMembershipStateRef,
    ) -> Result<MembershipChain, RegistrationLoadError> {
        self.history_verifier
            .load_predecessor_membership(state)
            .await
    }

    pub(crate) async fn materialized_reference_status(
        &mut self,
        coverage: &CommitFrontier,
        stream_id: &str,
        reference: &StoreBatchCommitRef,
    ) -> Result<pull::MaterializedCheck, pull::StorePullError> {
        if pull::commit_stream_id(&reference.coord) != stream_id {
            return Ok(pull::MaterializedCheck::Held(
                pull::HeldStorePositionReason::WrongSlot(format!(
                    "commit reference stream {} differs from dependency stream {stream_id}",
                    pull::commit_stream_id(&reference.coord)
                )),
            ));
        }
        if let Some(actual) = self
            .database
            .exact_materialized_ref(stream_id, reference.coord.sequence())
            .await?
        {
            if actual != *reference {
                return Ok(pull::MaterializedCheck::Held(
                    pull::HeldStorePositionReason::HashMismatch {
                        referenced_device_id: stream_id.to_string(),
                        referenced_commit: reference.clone(),
                        materialized_hash: actual.commit_hash,
                    },
                ));
            }
            return Ok(pull::MaterializedCheck::Yes);
        }
        Ok(self
            .history_verifier
            .covered_reference_status(coverage, stream_id, reference)
            .await)
    }

    pub(crate) async fn pull_readiness(
        &mut self,
        coverage: &CommitFrontier,
        frontier: &std::collections::BTreeMap<String, StoreBatchCommitRef>,
        device_state: &ResolvedStoreDeviceState,
        exclusion_freezes: &[crate::protocol::store_commit::StoreDeviceProposalAck],
        commit_ref: &StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<pull::Readiness, pull::StorePullError> {
        let stream_id = pull::commit_stream_id(&commit_ref.coord);
        if let Some(current) = frontier.get(&stream_id) {
            if commit_ref.coord.sequence() <= current.coord.sequence() {
                match self
                    .materialized_reference_status(coverage, &stream_id, commit_ref)
                    .await?
                {
                    pull::MaterializedCheck::Yes => {
                        return Ok(pull::Readiness::AlreadyMaterialized)
                    }
                    pull::MaterializedCheck::Missing => {
                        return Ok(pull::Readiness::Held(pull::HeldStorePosition::commit(
                            commit_ref,
                            pull::HeldStorePositionReason::MissingCommit,
                        )))
                    }
                    pull::MaterializedCheck::Held(reason) => {
                        return Ok(pull::Readiness::Held(pull::HeldStorePosition::commit(
                            commit_ref, reason,
                        )))
                    }
                }
            }
            if commit.order.predecessor() != Some(current) {
                let reason = match commit.order.predecessor() {
                    Some(missing) => {
                        pull::HeldStorePositionReason::MissingPredecessor(missing.clone())
                    }
                    None => pull::HeldStorePositionReason::InvalidObject(
                        "non-genesis Merge commit omits its exact predecessor".to_string(),
                    ),
                };
                return Ok(pull::Readiness::Held(pull::HeldStorePosition::commit(
                    commit_ref, reason,
                )));
            }
            if commit_ref.coord.sequence() != current.coord.sequence() + 1 {
                return Ok(pull::Readiness::Held(pull::HeldStorePosition::commit(
                        commit_ref,
                        pull::HeldStorePositionReason::InvalidObject(
                            "Merge commit sequence does not immediately follow its materialized frontier"
                                .to_string(),
                        ),
                    )));
            }
        } else if commit_ref.coord.sequence() != 1 || commit.order.predecessor().is_some() {
            let reason = match commit.order.predecessor() {
                Some(missing) => pull::HeldStorePositionReason::MissingPredecessor(missing.clone()),
                None => pull::HeldStorePositionReason::InvalidObject(
                    "Merge commit beyond genesis omits its exact predecessor".to_string(),
                ),
            };
            return Ok(pull::Readiness::Held(pull::HeldStorePosition::commit(
                commit_ref, reason,
            )));
        }

        for record in device_state.devices.values() {
            let target_stream = StreamActivation::device_authorized_stream_id(
                self.history_verifier
                    .verified_root()
                    .reference()
                    .store_root_hash,
                &record.registration,
                StreamAnchorDomain::StoreAnnouncements,
            );
            if target_stream.to_string() != stream_id {
                continue;
            }
            let StoreDeviceStatus::Inactive {
                terminals,
                accepted_cut,
            } = &record.status
            else {
                break;
            };
            let target_cut = accepted_cut.commits();
            let terminal_sequence = match target_cut.get(&target_stream) {
                Some(reference) => reference.coord.sequence(),
                None => 0,
            };
            if commit_ref.coord.sequence() > terminal_sequence {
                return Ok(pull::Readiness::Held(pull::HeldStorePosition::commit(
                    commit_ref,
                    pull::HeldStorePositionReason::InactiveDevice {
                        terminals: terminals.clone(),
                        accepted_cut: accepted_cut.clone(),
                    },
                )));
            }
            break;
        }

        for freeze in exclusion_freezes {
            let target_stream = StreamActivation::device_authorized_stream_id(
                self.history_verifier
                    .verified_root()
                    .reference()
                    .store_root_hash,
                &freeze.proposal.target,
                StreamAnchorDomain::StoreAnnouncements,
            );
            if target_stream.to_string() != stream_id {
                continue;
            }
            let target_cut = freeze.target_cut.commits();
            let frozen_sequence = match target_cut.get(&target_stream) {
                Some(reference) => reference.coord.sequence(),
                None => 0,
            };
            if commit_ref.coord.sequence() > frozen_sequence {
                return Ok(pull::Readiness::Held(pull::HeldStorePosition::commit(
                    commit_ref,
                    pull::HeldStorePositionReason::DeviceExclusionFreeze {
                        proposal: freeze.proposal.clone(),
                        target_cut: freeze.target_cut.clone(),
                    },
                )));
            }
        }

        for (required_stream, required_ref) in commit.merge_dependencies() {
            let required_stream = required_stream.to_string();
            match self
                .materialized_reference_status(coverage, &required_stream, required_ref)
                .await?
            {
                pull::MaterializedCheck::Yes => {}
                pull::MaterializedCheck::Missing => {
                    return Ok(pull::Readiness::Held(pull::HeldStorePosition::dependency(
                        commit_ref,
                        &required_stream,
                        required_ref,
                        pull::HeldStorePositionReason::MissingDependency {
                            device_id: required_stream.clone(),
                            commit: required_ref.clone(),
                        },
                    )))
                }
                pull::MaterializedCheck::Held(reason) => {
                    return Ok(pull::Readiness::Held(pull::HeldStorePosition::dependency(
                        commit_ref,
                        &required_stream,
                        required_ref,
                        reason,
                    )))
                }
            }
        }
        Ok(pull::Readiness::Ready)
    }

    pub(crate) async fn verified_pull_membership_objects(
        &mut self,
        commit_ref: &StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<
        Option<crate::sync::store::owner::verification::VerifiedMergeMembershipClosure>,
        pull::StorePullError,
    > {
        self.history_verifier
            .verified_membership_objects(commit_ref, commit)
            .await
    }

    pub(crate) async fn verify_pull_owner_recovery_activation(
        &self,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
    ) -> Result<
        Option<(
            crate::protocol::membership::MembershipGrantId,
            crate::protocol::store_commit::OwnerRecoveryActivationId,
        )>,
        pull::StorePullError,
    > {
        self.history_verifier
            .verify_owner_recovery_activation(commit)
            .await
    }

    pub(crate) async fn retain_pull_acknowledgement(
        &self,
        commit_ref: &StoreBatchCommitRef,
        commit: &crate::protocol::store_commit::StoreBatchCommit,
        author: &crate::protocol::store_commit::StoreDeviceRegistration,
    ) -> Result<
        Option<crate::protocol::store_commit::RetainedVerifiedActivatedAck>,
        pull::StorePullError,
    > {
        let acknowledgement = self
            .history_verifier
            .validate_commit_acknowledgement(commit, author)
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => pull::StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => pull::StorePullError::InvalidState(error),
            })?;
        match acknowledgement {
            Some((reference, value)) => self
                .history_verifier
                .retain_acknowledgement(commit_ref, commit, author, reference, value)
                .await
                .map(Some),
            None => Ok(None),
        }
    }

    pub(crate) fn remember_pull_commit(
        &mut self,
        commit: crate::protocol::store_commit::VerifiedStoreBatchCommit,
    ) -> Result<(), pull::StorePullError> {
        self.history_verifier
            .remember(commit)
            .map_err(pull::StorePullError::Protocol)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn verified_pull_terminal_retractions(
        &mut self,
        activation_head: &crate::protocol::store_commit::StoreDeviceHead,
        activation_head_object: &crate::protocol::objects::ExactObjectRef,
        activation_commit: &crate::protocol::store_commit::VerifiedStoreBatchCommit,
        activation_predecessor_state: &ResolvedStoreDeviceState,
        activation_predecessor_membership: &MembershipChain,
        device_operations: &crate::protocol::store_commit::VerifiedStoreDeviceOperations,
        loaded_predecessor_memberships: &pull::LoadedMergePredecessorMemberships,
    ) -> Result<
        Vec<crate::protocol::remote_object::VerifiedCandidateNonactivation>,
        pull::StorePullError,
    > {
        let root = self.history_verifier.verified_root().reference().clone();
        let retained = self
            .database
            .retained_merge_replay_inputs(root.clone())
            .await?;
        let mut verified_retained = BTreeMap::new();
        for materialization in &retained {
            let verified = self
                .history_verifier
                .authenticate_bytes(
                    materialization.commit_ref(),
                    &materialization.commit().to_bytes(),
                )
                .await?;
            if verified.value() != materialization.commit() {
                return Err(pull::StorePullError::InvalidState(
                    "retained Merge materialization differs from its authenticated commit"
                        .to_string(),
                ));
            }
            verified_retained.insert(materialization.commit_ref().clone(), verified);
        }
        let activation_commit_ref = activation_commit.reference();
        let activation_commit_value = activation_commit.value();
        let activation_head_ref = crate::protocol::store_commit::StoreDeviceHeadRef {
            head_hash: activation_head.head_hash(),
            object: activation_head_object.clone(),
        };
        let current_membership_ref = &activation_commit_value.membership_state;
        let MembershipStatus::Resolved(current_resolved) =
            activation_predecessor_membership.status()
        else {
            return Err(pull::StorePullError::InvalidState(
                "Merge terminal retraction witness membership is conflicted".to_string(),
            ));
        };
        let mut retractions = Vec::new();
        for materialization in &retained {
            let candidate = verified_retained
                .get(materialization.commit_ref())
                .expect("every retained Merge materialization was authenticated");
            let mut locator = self
                .database
                .author_exclusion_activation_for_candidate(
                    root.clone(),
                    materialization.commit_ref().clone(),
                    candidate.value().author_registration.clone(),
                )
                .await?;
            if locator.is_none() {
                let expected_stream =
                    crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
                        root.store_root_hash,
                        &candidate.value().author_registration,
                        crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
                    );
                for (exclusion, accepted_cut) in device_operations.exclusions() {
                    if exclusion.proposal.target != candidate.value().author_registration {
                        continue;
                    }
                    let accepted_cut = &accepted_cut.0;
                    let beyond_cutoff =
                        accepted_cut.get(&expected_stream).is_none_or(|reference| {
                            materialization.commit_ref().coord.sequence()
                                > reference.coord.sequence()
                        });
                    if beyond_cutoff {
                        locator =
                            Some(crate::database::AuthorExclusionActivationLocator::verified(
                                exclusion.clone(),
                                accepted_cut.clone(),
                                activation_commit_ref.clone(),
                                activation_head_ref.clone(),
                            ));
                        break;
                    }
                }
            }
            let Some(locator) = locator else {
                let Some(authority) = candidate.value().membership_authority.as_ref() else {
                    continue;
                };
                let predecessor_membership =
                    loaded_predecessor_memberships.membership_for(materialization.commit_ref())?;
                let MembershipStatus::Resolved(predecessor_resolved) =
                    predecessor_membership.status()
                else {
                    return Err(pull::StorePullError::InvalidState(
                        "retained candidate predecessor membership is conflicted".to_string(),
                    ));
                };
                let mut matching = predecessor_resolved
                    .active_grants()
                    .filter(|(_, record)| &record.creation_authority == authority);
                let Some((grant_id, _)) = matching.next() else {
                    return Err(pull::StorePullError::InvalidState(
                        "retained candidate has no exact predecessor grant authority".to_string(),
                    ));
                };
                if matching.next().is_some() {
                    return Err(pull::StorePullError::InvalidState(
                        "retained candidate authority identifies multiple predecessor grants"
                            .to_string(),
                    ));
                }
                if !matches!(
                    current_resolved.grants.get(grant_id),
                    Some(crate::protocol::causal_grants::GrantState::Tombstoned { .. })
                ) {
                    continue;
                }
                let nonactivation = self
                    .history_verifier
                    .verify_membership_grant_revocation_nonactivation(
                        grant_id,
                        current_membership_ref,
                        activation_commit_ref,
                        &activation_head_ref,
                        candidate,
                        materialization.activation_head(),
                        materialization.activation_head_object(),
                    )
                    .await?;
                retractions.push(nonactivation);
                continue;
            };
            let nonactivation = self
                .history_verifier
                .verify_author_exclusion_nonactivation(
                    &locator,
                    activation_head,
                    activation_head_object,
                    activation_commit,
                    activation_predecessor_state,
                    device_operations,
                    candidate,
                    materialization.activation_head(),
                    materialization.activation_head_object(),
                )
                .await?;
            retractions.push(nonactivation);
        }
        let mut verified_by_reference = retractions
            .into_iter()
            .map(|verified| {
                let reference = verified
                    .candidate_reference()
                    .map_err(pull::StorePullError::RemoteObject)?;
                Ok((reference, verified))
            })
            .collect::<Result<BTreeMap<_, _>, pull::StorePullError>>()?;
        loop {
            let mut additions = Vec::new();
            for materialization in &retained {
                if verified_by_reference.contains_key(materialization.commit_ref()) {
                    continue;
                }
                let candidate = verified_retained
                    .get(materialization.commit_ref())
                    .expect("every retained Merge materialization was authenticated");
                let dependency = pull::commit_predecessor_references(candidate.value())
                    .into_iter()
                    .find_map(|reference| {
                        verified_by_reference
                            .get(&reference)
                            .map(|verified| (reference, verified))
                    });
                let Some((_dependency_reference, dependency)) = dependency else {
                    continue;
                };
                let verified = crate::protocol::remote_object::VerifiedCandidateNonactivation::dependency_retraction(
                    dependency,
                    crate::protocol::store_commit::StoreBatchCommitDeletionTarget {
                        coord: materialization.commit_ref().coord.clone(),
                        object: materialization.commit_ref().object.clone(),
                        canonical_signed_bytes: candidate.value().to_bytes(),
                    },
                    candidate.author(),
                    materialization.activation_head_object().clone(),
                )
                .map_err(pull::StorePullError::RemoteObject)?;
                additions.push((materialization.commit_ref().clone(), verified));
            }
            if additions.is_empty() {
                break;
            }
            for (reference, verified) in additions {
                if verified_by_reference.insert(reference, verified).is_some() {
                    return Err(pull::StorePullError::InvalidState(
                        "transitive Merge retraction constructed duplicate proof".to_string(),
                    ));
                }
            }
        }
        let removed = verified_by_reference
            .keys()
            .cloned()
            .collect::<BTreeSet<_>>();
        if retained.iter().any(|materialization| {
            !removed.contains(materialization.commit_ref())
                && materialization
                    .history_summary()
                    .causal_cut
                    .values()
                    .any(|reference| removed.contains(reference))
        }) {
            return Err(pull::StorePullError::InvalidState(
                "surviving retained Merge summary contains a retracted dependency".to_string(),
            ));
        }
        Ok(verified_by_reference.into_values().collect())
    }
}
