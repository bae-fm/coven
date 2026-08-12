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
        .apply_to(device_state, &commit.device_state)
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
        activation: &ReclaimCommitActivation,
    ) -> Result<(), DbError> {
        activation.validate().map_err(store_reclaim_journal_error)?;
        if activation.commit() != commit_ref {
            return Err(DbError::Message(
                "Store reclaim activation evidence names another commit".to_string(),
            ));
        }
        if let Some(authorization) = commit.reclaim_authorization() {
            let operation_id = authorization.authorization_hash;
            let next = DurableStoreReclaimOperation::Authorized {
                authorization: authorization.clone(),
                activation: activation.clone(),
            };
            next.validate().map_err(store_reclaim_journal_error)?;
            match load_store_reclaim_operation_on(self.store.transaction, operation_id)? {
                Some(expected)
                    if matches!(
                        &expected,
                        DurableStoreReclaimOperation::AuthorizationCandidate { object, .. }
                            | DurableStoreReclaimOperation::AuthorizationReplacing { object, .. }
                            if object.authorization_ref() == authorization
                    ) =>
                {
                    update_store_reclaim_operation_on(self.store.transaction, &expected, &next)?;
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
        if let Some(receipt) = commit.reclaim_receipt() {
            let operation_id = receipt.authorization.authorization_hash;
            let expected = load_store_reclaim_operation_on(self.store.transaction, operation_id)?
                .ok_or_else(|| {
                DbError::Message("reclaim receipt has no durable authorization".to_string())
            })?;
            let (authorization, authorization_activation) = match &expected {
                DurableStoreReclaimOperation::AuthorizationCandidate { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt precedes authorization activation".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::AuthorizationReplacing { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt precedes replacement authorization activation".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::Authorized {
                    authorization,
                    activation,
                } => (authorization.clone(), activation.clone()),
                DurableStoreReclaimOperation::AbsentVerified {
                    authorization,
                    authorization_activation,
                    ..
                } => (authorization.clone(), authorization_activation.clone()),
                DurableStoreReclaimOperation::ReceiptCandidate {
                    authorization,
                    authorization_activation,
                    object,
                    ..
                } if matches!(
                    &**object,
                    crate::DurableStoreReclaimObject::Receipt {
                        receipt_ref,
                        ..
                    } if receipt_ref == receipt
                ) =>
                {
                    (authorization.clone(), authorization_activation.clone())
                }
                DurableStoreReclaimOperation::ReceiptReplacing {
                    authorization,
                    authorization_activation,
                    object,
                    ..
                } if matches!(
                    &**object,
                    crate::DurableStoreReclaimObject::Receipt {
                        receipt_ref,
                        ..
                    } if receipt_ref == receipt
                ) =>
                {
                    (authorization.clone(), authorization_activation.clone())
                }
                DurableStoreReclaimOperation::ReceiptCandidate { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt differs from its durable candidate".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::ReceiptReplacing { .. } => {
                    return Err(DbError::Message(
                        "reclaim receipt differs from its replacement candidate".to_string(),
                    ));
                }
                DurableStoreReclaimOperation::Completed { .. } => {
                    return Err(DbError::Message(
                        "reclaim authorization already has a receipt".to_string(),
                    ));
                }
            };
            let next = DurableStoreReclaimOperation::Completed {
                authorization: authorization.clone(),
                authorization_activation: authorization_activation.clone(),
                receipt: receipt.clone(),
                receipt_activation: activation.clone(),
            };
            let reclaimed = ReclaimedStorePackage::receipted(
                authorization,
                authorization_activation,
                receipt.clone(),
                activation.clone(),
            )
            .map_err(store_reclaim_journal_error)?;
            record_reclaimed_store_package_on(
                self.store.transaction,
                Some(root.store_root_hash),
                &reclaimed,
            )?;
            update_store_reclaim_operation_on(self.store.transaction, &expected, &next)?;
        }
        Ok(())
    }

    pub(crate) fn replace_store_device_exclusion_freezes_from_replay(
        &self,
        root: &coven_protocol::store_commit::StoreRootRef,
    ) -> Result<(), DbError> {
        let existing = load_store_device_exclusion_freezes_on(self.store.transaction, root)?;
        let frontier = crate::store::materialized_commit_index::materialized_frontier_on(
            self.store.transaction,
            None,
        )?
        .into_values()
        .map(|reference| (reference.coord.stream_id, reference))
        .collect::<BTreeMap<_, _>>();
        let (_, state) = store_device_state_for_history_cut_on(
            self.store.transaction,
            &StoreHistoryCut(frontier),
        )?;
        let mut retained = Vec::new();
        for freeze in existing.into_values() {
            let proposal_state = state
                .devices
                .get(&freeze.proposal.target.device_id)
                .and_then(|record| record.proposals.get(&freeze.proposal.proposal_id));
            match proposal_state {
                Some(StoreDeviceProposalState::Pending { proposal })
                    if proposal == &freeze.proposal =>
                {
                    retained.push(freeze);
                }
                Some(StoreDeviceProposalState::Cancelled { outcome })
                    if outcome.proposal == freeze.proposal => {}
                Some(StoreDeviceProposalState::Superseded { proposal, .. })
                    if proposal == &freeze.proposal => {}
                None => {}
                Some(_) => {
                    return Err(DbError::Message(
                        "stored device exclusion freeze differs from replayed device state"
                            .to_string(),
                    ));
                }
            }
        }
        retained.sort_by_key(|freeze| freeze.proposal.proposal_id);
        replace_store_device_exclusion_freezes_on(self.store.transaction, &retained)
    }

    pub(crate) fn complete_membership_journal(
        &self,
        completion: coven_protocol::membership_mutation::StoreMembershipJournalCompletion,
        candidate: &StoreBatchCommitRef,
    ) -> Result<(), DbError> {
        match completion {
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion::Mutation {
                intent_hash,
                progress_bytes,
                remote_objects,
            } => self.record_activated_membership_candidate_mutation(
                intent_hash,
                candidate,
                &remote_objects,
                progress_bytes,
                crate::MembershipMutationActivation::WithoutRotation,
            ),
            coven_protocol::membership_mutation::StoreMembershipJournalCompletion::RotationMutation {
                intent_hash,
                progress_bytes,
                generation,
                remote_objects,
            } => self.record_activated_membership_candidate_mutation(
                intent_hash,
                candidate,
                &remote_objects,
                progress_bytes,
                crate::MembershipMutationActivation::Rotation { generation },
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

    fn record_activated_membership_candidate_mutation(
        &self,
        intent_hash: ObjectHash,
        candidate: &StoreBatchCommitRef,
        remote_objects: &[coven_protocol::remote_object::RemoteObjectRecord],
        progress_bytes: Vec<u8>,
        activation: crate::MembershipMutationActivation,
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
        if let crate::MembershipMutationActivation::Rotation { generation } = activation {
            super::commit_rotation_candidate_on(self.store.transaction, intent_hash, generation)?;
        }
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
        activation_head: &StoreDeviceHead,
        activation_head_object: &ExactObjectRef,
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
            activation_head,
            activation_head_object,
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
        self.store.derive_materialized_store_device_state(
            registrations_lookup,
            root,
            materialization.commit(),
            materialization.device_operations(),
        )?;
        let (retained_commit_ref, retained) = self.store.retain_merge_materialization(
            registrations_lookup,
            root,
            &materialization,
        )?;
        self.store.record_circle_bootstrap_coverage(
            registrations_lookup,
            root,
            materialization.commit_ref(),
            materialization.circle_activations(),
        )?;
        let activation = ReclaimCommitActivation::new(
            materialization.commit_ref().clone(),
            coven_protocol::store_commit::StoreDeviceHeadRef {
                head_hash: materialization.activation_head().head_hash(),
                object: materialization.activation_head_object().clone(),
            },
        )
        .map_err(store_reclaim_journal_error)?;
        self.record_materialized_commit_with_device_operations(
            registrations_lookup,
            root,
            materialization.verified_commit(),
            materialization.device_operations(),
            materialization.circle_activations().stream_activations(),
            &retained_commit_ref,
            &activation,
        )?;
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
        activation: &ReclaimCommitActivation,
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
        conn.execute(
            "INSERT INTO store_device_state_snapshots (commit_ref, state) VALUES (?1, ?2)",
            rusqlite::params![
                &commit_ref_json,
                serde_json::to_string(&device_state).map_err(|error| {
                    DbError::context("serialize materialized Store device state", error)
                })?,
            ],
        )
        .map_err(DbError::from)?;
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
        apply_store_device_exclusion_freezes_on(conn, root, &device_state, device_operations)?;
        self.record_store_reclaim_activation(root, commit, commit_ref, activation)
    }
}
