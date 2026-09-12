use super::*;

pub(crate) fn derive_materialized_store_device_state_on(
    records: crate::store::store_session::StoreRecords<'_>,
    registrations: &mut dyn VerifiedRegistrationLookup,
    root: &coven_protocol::store_commit::StoreRootRef,
    commit: &StoreBatchCommit,
    device_operations: &VerifiedStoreDeviceOperations,
) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, DbError> {
    let mut device_state = records.declared_store_device_state(&commit.device_state)?;
    let recovery_author = commit
        .device_registrations()
        .iter()
        .find_map(|activation| {
            if activation.registration != commit.author_registration {
                return None;
            }
            let coven_protocol::store_commit::StoreDeviceRegistrationActivationRef::Recovery {
                node,
                ..
            } = &activation.authority
            else {
                return None;
            };
            Some((&activation.registration, node))
        })
        .map(|(registration_ref, node)| {
            let registration =
                registrations.activated_registration_on(records, root, registration_ref)?;
            let coven_protocol::store_commit::StoreDeviceRegistrationOrigin::Recovery {
                owner_grant,
                ..
            } = registration.origin.clone()
            else {
                return Err(DbError::Message(
                    "recovery activation author has a non-recovery registration origin".to_string(),
                ));
            };
            Ok((
                registration_ref.clone(),
                coven_protocol::store_commit::OwnerRecoveryCursor {
                    owner_grant,
                    position: coven_protocol::store_commit::OwnerRecoveryPosition::At {
                        node: node.clone(),
                    },
                },
            ))
        })
        .transpose()?;
    if let Some((registration, recovery)) = &recovery_author {
        device_state = device_state
            .activate_registration(registration.clone(), Some(recovery.clone()))
            .map_err(DbError::from)?;
    }
    let active_author = device_state
        .devices
        .get(&commit.author_registration.device_id)
        .is_some_and(|record| {
            record.registration == commit.author_registration
                && matches!(
                    record.status,
                    coven_protocol::store_commit::StoreDeviceStatus::Active
                )
        });
    if !active_author {
        return Err(DbError::Message(
            "materialized commit author is not active at its exact predecessor state".into(),
        ));
    }
    device_state = device_operations
        .apply_to(device_state)
        .map_err(DbError::from)?;
    for activation in commit.device_registrations() {
        if recovery_author
            .as_ref()
            .is_some_and(|(registration, _)| registration == &activation.registration)
        {
            continue;
        }
        device_state = device_state
            .activate_registration(activation.registration.clone(), None)
            .map_err(DbError::from)?;
    }
    let mut owner_recoveries = commit.stream_activations().iter().filter_map(|activation| {
        let coven_protocol::store_commit::StreamActivation::GrantAuthorized {
            author_registration,
            grant_id,
            anchor: anchor @ coven_protocol::store_commit::GrantStreamAnchor::OwnerRecovery { .. },
            ..
        } = activation
        else {
            return None;
        };
        Some((author_registration, grant_id, anchor))
    });
    let owner_recovery = owner_recoveries.next();
    if owner_recoveries.next().is_some() {
        return Err(DbError::Message(
            "materialized commit activates more than one Owner recovery stream".to_string(),
        ));
    }
    let owner_recovery = match owner_recovery {
        Some((registration, grant_id, anchor)) => {
            let registration =
                registrations.activated_registration_on(records, root, registration)?;
            Some((
                grant_id.clone(),
                coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
                    root,
                    &registration.author_pubkey,
                    grant_id,
                    anchor,
                )
                .map_err(DbError::from)?,
            ))
        }
        None => None,
    };
    if let Some((grant_id, activation)) = owner_recovery {
        device_state = device_state
            .activate_owner_recovery(grant_id, activation)
            .map_err(DbError::from)?;
    }
    Ok(device_state)
}

impl crate::store::store_session::StoreTransaction<'_, '_> {
    pub(super) fn derive_materialized_store_device_state(
        self,
        registrations: &mut dyn VerifiedRegistrationLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        commit: &StoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
    ) -> Result<coven_protocol::store_commit::ResolvedStoreDeviceState, DbError> {
        derive_materialized_store_device_state_on(
            crate::store::store_session::StoreRecords::new(self.transaction, self.store_dir),
            registrations,
            root,
            commit,
            device_operations,
        )
    }
}

impl<'transaction, 'connection> MergeMaterializationTransaction<'transaction, 'connection> {
    pub(crate) fn record_store_reclaim_activation(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
        commit: &StoreBatchCommit,
        commit_ref: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        if let Some(authorization) = commit.reclaim_authorization() {
            let operation_id = authorization.authorization_hash;
            let next = DurableStoreReclaimOperation::Authorized {
                authorization: authorization.clone(),
                activation: commit_ref.clone(),
            };
            next.validate().map_err(store_reclaim_journal_error)?;
            match load_store_reclaim_operation_on(self.store.transaction, operation_id)? {
                Some(expected)
                    if matches!(
                        &expected,
                        DurableStoreReclaimOperation::AuthorizationCandidate { object, .. }
                            if object.authorization_ref == *authorization
                    ) =>
                {
                    update_store_reclaim_operation_on(self.store.transaction, &expected, &next)?;
                    crate::store::clear_active_store_commit_for_owner_on(
                        self.store.transaction,
                        &crate::ActiveStorePublicationOwner::Reclaim(operation_id),
                        commit_ref,
                    )?;
                }
                Some(existing) if existing == next => {}
                Some(_) => {
                    return Err(DbError::Message(
                        "reclaim authorization conflicts with its durable operation".to_string(),
                    ));
                }
                None => insert_store_reclaim_operation_on(self.store.transaction, &next)?,
            }
        }
        if let Some(completion) = commit.reclaim_completion() {
            let operation_id = completion.authorization.authorization_hash;
            let expected = load_store_reclaim_operation_on(self.store.transaction, operation_id)?
                .ok_or_else(|| {
                DbError::Message("reclaim completion has no durable authorization".to_string())
            })?;
            let (authorization, authorization_activation, completes_local_candidate) =
                match &expected {
                    DurableStoreReclaimOperation::AuthorizationCandidate { .. } => {
                        return Err(DbError::Message(
                            "reclaim completion precedes authorization activation".to_string(),
                        ));
                    }
                    DurableStoreReclaimOperation::Authorized {
                        authorization,
                        activation,
                    } => (authorization.clone(), activation.clone(), false),
                    DurableStoreReclaimOperation::AbsentVerified {
                        authorization,
                        authorization_activation,
                        ..
                    } => (
                        authorization.clone(),
                        authorization_activation.clone(),
                        false,
                    ),
                    DurableStoreReclaimOperation::CompletionCandidate {
                        authorization,
                        authorization_activation,
                        candidate,
                    } if candidate.reference == *commit_ref => (
                        authorization.clone(),
                        authorization_activation.clone(),
                        true,
                    ),
                    DurableStoreReclaimOperation::CompletionCandidate { .. } => {
                        return Err(DbError::Message(
                            "reclaim completion differs from its durable candidate".to_string(),
                        ));
                    }
                    DurableStoreReclaimOperation::Completed { .. } => {
                        return Err(DbError::Message(
                            "reclaim authorization is already completed".to_string(),
                        ));
                    }
                };
            let next = DurableStoreReclaimOperation::Completed {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                completion_activation: commit_ref.clone(),
            };
            let reclaimed = ReclaimedStorePackage::completed(
                authorization,
                authorization_activation,
                commit_ref.clone(),
            )
            .map_err(store_reclaim_journal_error)?;
            record_reclaimed_store_package_on(
                self.store.transaction,
                Some(root.store_root_hash),
                &reclaimed,
            )?;
            update_store_reclaim_operation_on(self.store.transaction, &expected, &next)?;
            if completes_local_candidate {
                crate::store::clear_active_store_commit_for_owner_on(
                    self.store.transaction,
                    &crate::ActiveStorePublicationOwner::Reclaim(operation_id),
                    commit_ref,
                )?;
            }
        }
        Ok(())
    }

    pub(crate) fn complete_membership_journal(
        &self,
        completion: coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        acceptance: &crate::AcceptedStoreCommitEvidence,
        verified_commit: &VerifiedStoreBatchCommit,
        history_evidence: &coven_protocol::store_commit::RetainedMergeCommitEvidence,
    ) -> Result<(), DbError> {
        // The publication transaction resolves this evidence against its live
        // baseline and records any new materialization before completing journals.
        let candidate = acceptance.commit_ref();
        if candidate != verified_commit.reference() {
            return Err(DbError::Message(
                "membership completion differs from the accepted exact commit".into(),
            ));
        }
        match completion {
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion::MembershipCandidateAbandoned {
                intent_hash, original, remote_objects,
            } => self.complete_membership_candidate_abandonment(
                intent_hash, &original, &remote_objects, verified_commit,
            ),
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion::DeviceJoin { remote_objects } => {
                let object_ids = remote_objects.iter().map(|remote| remote.object_id()).collect::<Vec<_>>();
                if object_ids.is_empty() || object_ids.iter().collect::<BTreeSet<_>>().len() != object_ids.len() {
                    return Err(DbError::Message("device join activation graph is empty or repeats an exact object".into()));
                }
                self.activate_store_operation_remote_objects(candidate, &object_ids)
            }
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion::DeviceExclusion {
                operation,
                remote_objects,
            } => {
                let object_ids = remote_objects.iter().map(|remote| remote.object_id()).collect::<Vec<_>>();
                if object_ids.is_empty()
                    || object_ids.iter().collect::<BTreeSet<_>>().len() != object_ids.len()
                {
                    return Err(DbError::Message("exclusion activation graph is empty or repeats an exact object".into()));
                }
                self.activate_store_operation_remote_objects(candidate, &object_ids)?;
                crate::store::store_session::device_exclusion::complete_store_device_exclusion_activation_on(
                    self.store.transaction, &operation, acceptance,
                )
            }
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion::Mutation {
                intent_hash,
                progress_bytes,
                remote_objects,
            } => self.record_activated_membership_candidate_mutation(
                intent_hash,
                candidate,
                &remote_objects,
                progress_bytes,
                &history_evidence.membership_proof.as_ref().ok_or_else(|| {
                    DbError::Message("membership mutation completion has no exact membership proof".into())
                })?.entry_value,
            ),
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion::OwnerPromotion {
                transition,
                remote_objects,
            } => {
                let mut unique = std::collections::BTreeSet::new();
                let object_ids = remote_objects
                    .iter()
                    .map(|remote| remote.object_id())
                    .map(|object_id| {
                        if unique.insert(object_id) {
                            Ok(object_id)
                        } else {
                            Err(DbError::Message(
                                "activated Owner-promotion graph repeats an exact object"
                                    .to_string(),
                            ))
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                if object_ids.is_empty() {
                    return Err(DbError::Message(
                        "activated Owner-promotion graph is empty".to_string(),
                    ));
                }
                self.activate_store_operation_remote_objects(candidate, &object_ids)?;
                let (journal_key, target_key, previous_value, next_value, remote_objects) =
                    transition.into_values();
                self.store
                    .advance_owner_promotion_journal(
                        journal_key,
                        target_key,
                        previous_value,
                        next_value,
                        remote_objects,
                    )
            }
        }
    }

    fn complete_membership_candidate_abandonment(
        &self,
        intent_hash: ObjectHash,
        original: &coven_protocol::prepared_commit::PreparedStoreOperationCommit,
        remote_objects: &[coven_protocol::remote_object::RemoteObjectRecord],
        accepted: &VerifiedStoreBatchCommit,
    ) -> Result<(), DbError> {
        use crate::store::store_session::{
            active_store_publication, candidate_records, membership_mutations,
        };
        let tx = self.store.transaction;
        membership_mutations::require_membership_mutation_on(tx, intent_hash)?;
        let publication = original.prepared_membership_publication()?;
        let active =
            active_store_publication::load_active_store_publication_on(tx)?.ok_or_else(|| {
                DbError::Message("accepted membership abandonment has no owner".into())
            })?;
        let abandonment = active.membership_abandonment().ok_or_else(|| {
            DbError::Message("membership continuation has no prepared abandonment".into())
        })?;
        if abandonment.reference != *accepted.reference()
            || abandonment.commit != *accepted.value()
            || active.commit_reservation()
                != Some((
                    &original.commit.write_id,
                    &original.commit.author_registration,
                    &original.reference.coord,
                ))
            || remote_objects.len() != 1
            || remote_objects[0].object() != &accepted.reference().object
        {
            return Err(DbError::Message(
                "accepted abandonment differs from its reserved membership mutation".into(),
            ));
        }
        let nonactivation =
            coven_protocol::remote_object::CandidateNonactivation::from_durable_parts(
                &original.reference,
                &original.commit,
                coven_protocol::remote_object::CandidateNonactivationProof::AcceptedAbandonment {
                    abandonment: coven_protocol::store_commit::StoreBatchCommitDeletionTarget {
                        coord: accepted.reference().coord.clone(),
                        object: accepted.reference().object.clone(),
                        canonical_signed_bytes: accepted.to_bytes(),
                    },
                },
            )?;
        let retired = crate::RetiredStoreCandidate {
            nonactivation,
            inputs: crate::RetiredStoreCandidateInputs::Membership(publication),
            publications: vec![original.publication.reference()?],
        };
        let replacement = active.continue_membership_after_abandonment(retired.clone())?;
        self.activate_store_operation_remote_objects(
            accepted.reference(),
            &[remote_objects[0].object_id()],
        )?;
        candidate_records::begin_candidate_nonactivation_targets_on(
            tx,
            &original.reference,
            &retired.objects()?,
            &retired.nonactivation,
        )?;
        active_store_publication::update_active_store_publication_on(tx, &active, &replacement)
    }

    fn record_activated_membership_candidate_mutation(
        &self,
        intent_hash: ObjectHash,
        candidate: &StoreBatchCommitRef,
        remote_objects: &[coven_protocol::remote_object::RemoteObjectRecord],
        progress_bytes: Vec<u8>,
        entry: &coven_protocol::membership::MembershipEntry,
    ) -> Result<(), DbError> {
        let mut unique = std::collections::BTreeSet::new();
        let object_ids = remote_objects
            .iter()
            .map(|remote| remote.object_id())
            .map(|object_id| {
                if unique.insert(object_id) {
                    Ok(object_id)
                } else {
                    Err(DbError::Message(
                        "activated membership graph repeats an exact object".to_string(),
                    ))
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        self.activate_store_operation_remote_objects(candidate, &object_ids)?;
        if self
            .store
            .transaction
            .execute(
                "UPDATE outbound_membership_mutation SET progress_bytes = ?1 \
                 WHERE singleton = 1 AND intent_hash = ?2",
                rusqlite::params![progress_bytes, intent_hash.to_string()],
            )
            .map_err(DbError::from)?
            != 1
        {
            return Err(DbError::Message(
                "membership mutation changed during activated recording".to_string(),
            ));
        }
        if let Some(generation) = membership_rotation_generation(entry)? {
            super::commit_rotation_candidate_on(self.store.transaction, intent_hash, generation)?;
        }
        crate::store::clear_active_store_commit_for_owner_on(
            self.store.transaction,
            &crate::ActiveStorePublicationOwner::MembershipMutation,
            candidate,
        )?;
        Ok(())
    }

    pub(crate) fn record_obsolete_blob_cleanup_intent(
        &self,
        declarations: &crate::BlobDecls,
        intent: &crate::local_blob_cleanup_intents::LocalBlobCleanupIntent,
    ) -> Result<(), DbError> {
        crate::store::local_blob_cleanup::record_obsolete_copy_intents_on(
            self.store.transaction,
            declarations,
            intent,
        )
    }

    pub(crate) fn record_materialized_merge_commit(
        &self,
        registrations_lookup: &mut dyn VerifiedStoreLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        verified_commit: &VerifiedStoreBatchCommit,
        registrations: &[ActivatedStoreDeviceRegistration],
        acceptance: &crate::AcceptedStoreCommitEvidence,
        history_evidence: &coven_protocol::store_commit::RetainedMergeCommitEvidence,
        packages: &[AudiencePackage],
        package_application: Option<RetainedPackageApplication>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let device_operations =
            VerifiedStoreDeviceOperations::without_exclusions(commit).map_err(DbError::from)?;
        let circle_activations =
            VerifiedCircleActivations::none(commit, commit_ref).map_err(DbError::from)?;
        let materialization = VerifiedMergeMaterialization::verify(
            root,
            verified_commit,
            registrations,
            &device_operations,
            &circle_activations,
            acceptance,
            history_evidence,
            None,
            packages,
            package_application,
        )?;
        self.record_verified_merge_materialization(registrations_lookup, materialization)
    }

    pub(crate) fn record_verified_merge_materialization(
        &self,
        registrations_lookup: &mut dyn VerifiedStoreLookup,
        materialization: VerifiedMergeMaterialization<'_>,
    ) -> Result<OwnedVerifiedMergeMaterialization, DbError> {
        self.record_author_exclusion_activations(&materialization)?;
        let root = materialization.root();
        self.store
            .derive_materialized_store_device_state(
                registrations_lookup,
                root,
                materialization.commit(),
                materialization.device_operations(),
            )
            .map_err(|error| DbError::context("received declared device state", error))?;
        let (retained_commit_ref, retained) = self
            .store
            .retain_merge_materialization(registrations_lookup, root, &materialization)
            .map_err(|error| DbError::context("received canonical retained input", error))?;
        self.store
            .record_circle_bootstrap_coverage(
                registrations_lookup,
                root,
                materialization.commit_ref(),
                materialization.circle_activations(),
            )
            .map_err(|error| DbError::context("received Circle coverage", error))?;
        self.record_materialized_commit_with_device_operations(
            registrations_lookup,
            root,
            materialization.verified_commit(),
            materialization.device_operations(),
            materialization.circle_activations().stream_activations(),
            &retained_commit_ref,
        )
        .map_err(|error| DbError::context("received materialized commit record", error))?;
        Ok(retained)
    }

    pub(crate) fn record_materialized_commit_with_device_operations(
        &self,
        registrations: &mut dyn VerifiedRegistrationLookup,
        root: &coven_protocol::store_commit::StoreRootRef,
        verified_commit: &VerifiedStoreBatchCommit,
        device_operations: &VerifiedStoreDeviceOperations,
        stream_activations: &VerifiedStreamActivations,
        retention: &RetainedMergeMaterializationKey,
    ) -> Result<(), DbError> {
        let conn = self.store.transaction;
        let commit = verified_commit.value();
        let commit_ref = verified_commit.reference();
        let stored_registration: String = conn
            .query_row(
                "SELECT registration_object FROM store_device_registration_activations \
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (
                    commit.author_registration.device_id.to_string(),
                    commit.author_registration.registration_hash.to_string(),
                ),
                |row| row.get(0),
            )
            .map_err(DbError::from)?;
        let stored_registration: StoreDeviceRegistrationRef =
            serde_json::from_str(&stored_registration)
                .map_err(|error| DbError::context("materialized author registration ref", error))?;
        if stored_registration != commit.author_registration {
            return Err(DbError::Message(
                "materialized commit author registration differs from its activation".to_string(),
            ));
        }
        if root.store_root_hash != commit.store_root_hash {
            return Err(DbError::Message(
                "materialized commit belongs to a different Store root".to_string(),
            ));
        }
        let expected_stream =
            coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &commit.author_registration,
                coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        if commit_ref.coord.stream_id != expected_stream {
            return Err(DbError::Message(
                "materialization stream differs from its exact author registration".to_string(),
            ));
        }
        let stream_id = commit_ref.coord.stream_id.to_string();
        let sequence = commit_ref.coord.sequence;
        if sequence != commit.seq() {
            return Err(DbError::Message(
                "materialization coordinate differs from its signed commit".to_string(),
            ));
        }
        let predecessor = if commit.seq() == 1 {
            None
        } else if let Some(reference) =
            crate::store::materialized_commit_index::materialized_commit_ref_on(
                conn,
                &stream_id,
                commit.seq() - 1,
            )?
        {
            Some(reference)
        } else {
            conn.query_row(
                "SELECT commit_ref FROM snapshot_coverage \
                 WHERE device_id = ?1 AND seq = ?2",
                (
                    &stream_id,
                    Database::sequence_to_sqlite(&stream_id, commit.seq() - 1)?,
                ),
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(DbError::from)?
            .map(|reference| {
                serde_json::from_str(&reference)
                    .map_err(|error| DbError::context("snapshot coverage exact commit ref", error))
            })
            .transpose()?
        };
        if predecessor.as_ref() != commit.order.predecessor() {
            return Err(DbError::Message(format!(
                "Store commit {}/{} names predecessor {:?}, durable predecessor is {:?}",
                stream_id,
                commit.seq(),
                commit.order.predecessor(),
                predecessor
            )));
        }
        let device_state = self.store.derive_materialized_store_device_state(
            registrations,
            root,
            commit,
            device_operations,
        )?;
        self.record_activated_store_ack(commit, commit_ref)?;
        self.record_activated_circle_acks(commit, commit_ref)?;
        let seq = Database::sequence_to_sqlite(&stream_id, commit.seq())?;
        let commit_ref_json = serde_json::to_string(commit_ref)
            .map_err(|error| DbError::context("serialize exact Store commit ref", error))?;
        if retention.commit_ref != commit_ref_json {
            return Err(DbError::Message(
                "retained input names another exact commit".to_string(),
            ));
        }
        let retained_commit_ref = retention.commit_ref.as_str();
        let retained_input_hash = retention.input_hash.to_string();
        crate::store::store_device_state::record_store_device_snapshot_on(
            conn,
            commit_ref,
            &device_state,
        )?;
        conn.execute(
            "INSERT INTO materialized_commits
             (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            rusqlite::params![
                &stream_id,
                seq,
                &commit_ref_json,
                retained_commit_ref,
                retained_input_hash
            ],
        )
        .map_err(DbError::from)?;
        if stream_activations.as_slice() != commit.stream_activations() {
            return Err(DbError::Message(
                "verified stream activations differ from the materialized Store commit".to_string(),
            ));
        }
        if stream_activations.activating_commit() != commit_ref {
            return Err(DbError::Message(
                "verified stream activation commit differs from the materialized Store commit"
                    .to_string(),
            ));
        }
        crate::store::stream_activation_records::record_verified_stream_activations_on(
            conn,
            stream_activations,
            &commit_ref_json,
        )?;
        self.record_store_reclaim_activation(root, commit, commit_ref)
    }
}
