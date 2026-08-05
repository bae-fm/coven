use super::*;

fn held_protocol_error(error: StoreProtocolError) -> HeldStorePositionReason {
    match error {
        StoreProtocolError::InvalidSignature => HeldStorePositionReason::InvalidSignature,
        StoreProtocolError::RelocatedSlot { .. }
        | StoreProtocolError::RelocatedPackage { .. }
        | StoreProtocolError::StoreRootMismatch { .. }
        | StoreProtocolError::StoreMismatch { .. }
        | StoreProtocolError::FounderMismatch { .. } => {
            HeldStorePositionReason::WrongSlot(error.to_string())
        }
        error => HeldStorePositionReason::InvalidObject(error.to_string()),
    }
}

impl<'a> MergeHistoryVerifier<'a> {
    pub(crate) async fn discover_merge_stream(
        &mut self,
        registration_ref: &StoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        inactive_accepted_cut: Option<&StoreHistoryCut>,
    ) -> Result<MergeStreamDiscovery, StorePullError> {
        let DeviceStreamAnchor::StoreAnnouncements { first_slot } = &registration.store_commits
        else {
            return Err(StorePullError::Database(format!(
                "Store registration {} has no Merge announcement anchor",
                registration.device_id
            )));
        };
        let root = self.root.reference().clone();
        let stream_id = store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            registration_ref,
            store_commit::StreamAnchorDomain::StoreAnnouncements,
        );
        let maximum_sequence = inactive_accepted_cut.map(|cut| {
            cut.0
                .get(&stream_id)
                .map_or(0, |reference| reference.coord.sequence())
        });
        let activation = registration
            .store_announcement_activation(registration_ref)
            .map_err(|error| StorePullError::Database(error.to_string()))?
            .activation_id();
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreHead,
        );
        let mut slot = first_slot.clone();
        let mut predecessor = None;
        let mut sequence = 1_u64;
        let mut latest_head = None;
        let mut commits = Vec::new();
        let mut block = None;
        let mut visited = BTreeSet::new();

        loop {
            if maximum_sequence.is_some_and(|maximum| sequence > maximum) {
                break;
            }
            if !visited.insert(slot.clone()) {
                return Err(StorePullError::Database(format!(
                    "Store announcement stream {stream_id} repeats a reserved slot"
                )));
            }
            let semantic_prefix =
                store_commit::head_slot_prefix(&registration.device_id.to_string(), sequence);
            let (bytes, object) = match self
                .commit_verifier
                .read_protocol_slot(&context, &slot, &semantic_prefix)
                .await
            {
                Ok(opened) => opened,
                Err(StorageError::NotFound(_)) => break,
                Err(error) => return Err(StoreObjectError::Storage(error).into()),
            };
            let unverified: StoreDeviceHead = match serde_json::from_slice(&bytes) {
                Ok(head) => head,
                Err(error) => {
                    block = Some(MergeStreamBlock::Unauthenticated(HeldStorePosition {
                        coordinate: HeldStoreCoordinate::Head {
                            device_id: stream_id.to_string(),
                            seq: sequence,
                            head_hash: ObjectHash::digest(&bytes),
                        },
                        reason: HeldStorePositionReason::InvalidObject(error.to_string()),
                    }));
                    break;
                }
            };
            let authenticated = unverified.signature_is_valid_for(registration);
            let coord_matches = unverified.commit.coord.stream_id == stream_id
                && unverified.commit.coord.sequence == sequence;
            if !coord_matches
                || unverified.author_registration != *registration_ref
                || unverified.successor.activation != activation
                || unverified.successor.predecessor != predecessor
            {
                let position = HeldStorePosition {
                    coordinate: HeldStoreCoordinate::Head {
                        device_id: stream_id.to_string(),
                        seq: sequence,
                        head_hash: unverified.head_hash(),
                    },
                    reason: HeldStorePositionReason::WrongSlot(
                        "Store head differs from its activated successor chain".to_string(),
                    ),
                };
                block = Some(if authenticated {
                    MergeStreamBlock::Authenticated(position)
                } else {
                    MergeStreamBlock::Unauthenticated(position)
                });
                break;
            }
            let head = match StoreDeviceHead::parse_at(
                &bytes,
                root.store_root_hash,
                registration,
                &unverified.commit,
            ) {
                Ok(head) => head,
                Err(error) => {
                    let position = HeldStorePosition {
                        coordinate: HeldStoreCoordinate::Head {
                            device_id: stream_id.to_string(),
                            seq: sequence,
                            head_hash: unverified.head_hash(),
                        },
                        reason: held_protocol_error(error),
                    };
                    block = Some(if authenticated {
                        MergeStreamBlock::Authenticated(position)
                    } else {
                        MergeStreamBlock::Unauthenticated(position)
                    });
                    break;
                }
            };
            let commit = match self.load_ref(&unverified.commit).await {
                Ok(verified)
                    if verified.value().author_registration == *registration_ref
                        && verified.author() == registration =>
                {
                    verified.value().clone()
                }
                Ok(_) => {
                    block = Some(MergeStreamBlock::Authenticated(HeldStorePosition::commit(
                        &unverified.commit,
                        HeldStorePositionReason::Unauthorized,
                    )));
                    break;
                }
                Err(error) => {
                    let reason = match error {
                        StorePullError::Object(error) => held_object_error(error),
                        error => HeldStorePositionReason::InvalidObject(error.to_string()),
                    };
                    block = Some(MergeStreamBlock::Authenticated(HeldStorePosition::commit(
                        &unverified.commit,
                        reason,
                    )));
                    break;
                }
            };
            let next_slot = head.successor.next_slot.clone();
            let head_ref = StoreDeviceHeadRef {
                head_hash: head.head_hash(),
                object: object.clone(),
            };
            predecessor = Some(object);
            sequence = sequence.checked_add(1).ok_or_else(|| {
                StorePullError::Database(format!(
                    "Store announcement stream {stream_id} sequence overflow"
                ))
            })?;
            commits.push((head_ref, head.clone(), head.commit.clone(), commit));
            latest_head = Some(head);
            slot = next_slot;
        }

        Ok(MergeStreamDiscovery {
            latest_head,
            commits,
            block,
        })
    }

    pub(crate) async fn history_cut_covers(
        &mut self,
        cut: &StoreHistoryCut,
        target: &StoreBatchCommitRef,
    ) -> Result<bool, StorePullError> {
        let Some(covering) = cut.0.get(&target.coord.stream_id) else {
            return Ok(false);
        };
        self.commit_position_covers(covering, target)
            .await
            .map_err(|error| match error {
                CommitCoverageError::Object(error) => StorePullError::Object(error),
                CommitCoverageError::MissingAncestry { commit_hash } => StorePullError::Database(
                    format!("exact Store ancestry is missing commit {commit_hash}"),
                ),
            })
    }

    pub(crate) async fn registration_activation(
        &self,
        activated: &ActivatedStoreDeviceRegistrationRef,
        registration: &StoreDeviceRegistration,
        activating_author: &StoreDeviceRegistration,
        predecessor: &MembershipChain,
        verified_join_outcomes: &BTreeMap<DeviceJoinOutcomeRef, VerifiedCommitJoinOutcome>,
    ) -> Result<StoreDeviceRegistrationActivation, RegistrationLoadError> {
        if !predecessor.is_owner_now(&activating_author.author_pubkey) {
            return Err(RegistrationLoadError::Invalid(
                "registration activation commit author is not an active Owner at its predecessor"
                    .to_string(),
            ));
        }
        match (&registration.origin, &activated.authority) {
            (
                StoreDeviceRegistrationOrigin::Join {
                    attempt_id: origin_attempt,
                    outcome_slot,
                    ..
                },
                StoreDeviceRegistrationActivationRef::Join {
                    attempt_id,
                    outcome,
                },
            ) if origin_attempt == attempt_id && outcome_slot == outcome.slot() => {
                let verified = verified_join_outcomes.get(outcome).ok_or_else(|| {
                    RegistrationLoadError::Invalid(
                        "registration activation has no verified join outcome".to_string(),
                    )
                })?;
                let attempt = &verified.attempt;
                let owner = &verified.owner;
                if attempt.expected_registration != *registration
                    || attempt.registration_slot != *activated.registration.object.slot()
                    || !predecessor_verifies_owner(
                        predecessor,
                        &attempt.membership,
                        &owner.author_pubkey,
                        &attempt.owner_grant,
                    )
                {
                    return Err(RegistrationLoadError::Invalid(
                        "activated registration differs from its exact join attempt".to_string(),
                    ));
                }
                let outcome_value = &verified.outcome;
                if outcome_value.owner_registration != attempt.owner_registration
                    || outcome_value.owner_grant != attempt.owner_grant
                {
                    return Err(RegistrationLoadError::Invalid(
                        "join outcome signer differs from its exact attempt authority".to_string(),
                    ));
                }
                let DeviceJoinDisposition::Activated { readiness } = &outcome_value.disposition
                else {
                    return Err(RegistrationLoadError::Invalid(
                        "cancelled device join outcome cannot activate a registration".to_string(),
                    ));
                };
                let initial_ack = self
                    .load_store_ack(&readiness.initial_ack, registration)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                readiness
                    .verify(
                        outcome.attempt(),
                        attempt,
                        registration,
                        &readiness.initial_ack,
                        &initial_ack,
                    )
                    .map_err(|error| RegistrationLoadError::Invalid(error.to_string()))?;
                Ok(StoreDeviceRegistrationActivation::Join {
                    attempt_id: *attempt_id,
                    outcome: outcome.clone(),
                })
            }
            (
                StoreDeviceRegistrationOrigin::Recovery {
                    recovery_id: origin_recovery,
                    recovery_slot,
                    ..
                },
                StoreDeviceRegistrationActivationRef::Recovery { recovery_id, node },
            ) if origin_recovery == recovery_id && recovery_slot == node.slot() => {
                let node_value = self
                    .load_owner_recovery_node(node)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                let mut reached_ref = node.clone();
                let mut reached = node_value.clone();
                while let Some(predecessor_ref) = reached.predecessor.clone() {
                    let predecessor_node = self
                        .load_owner_recovery_node(&predecessor_ref)
                        .await
                        .map_err(RegistrationLoadError::Object)?
                        .value;
                    if predecessor_node.next_slot != *reached_ref.object.slot() {
                        return Err(RegistrationLoadError::Invalid(
                            "recovery node does not occupy its exact predecessor successor slot"
                                .to_string(),
                        ));
                    }
                    if predecessor_node.recovery_id != node_value.recovery_id {
                        return Err(RegistrationLoadError::Invalid(
                            "recovery predecessor belongs to another recovery operation"
                                .to_string(),
                        ));
                    }
                    reached_ref = predecessor_ref;
                    reached = predecessor_node;
                }
                if node_value.recovery_id != *recovery_id
                    || node_value.readiness.registration != activated.registration
                    || node_value.next_slot == *node.object.slot()
                    || registration.author_pubkey != node_value.owner_pubkey
                    || !predecessor_verifies_owner(
                        predecessor,
                        &node_value.membership,
                        &node_value.owner_pubkey,
                        &node_value.owner_grant,
                    )
                {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery node differs from its exact registration".to_string(),
                    ));
                }
                let initial_ack = self
                    .load_store_ack(&node_value.readiness.initial_ack, registration)
                    .await
                    .map_err(RegistrationLoadError::Object)?
                    .value;
                if initial_ack.sequence != 1
                    || initial_ack.successor.predecessor.is_some()
                    || initial_ack.registration != activated.registration
                    || initial_ack.store_cut != node_value.readiness.bootstrap_cut
                {
                    return Err(RegistrationLoadError::Invalid(
                        "recovery readiness differs from its initial acknowledgement".to_string(),
                    ));
                }
                Ok(StoreDeviceRegistrationActivation::Recovery {
                    recovery_id: *recovery_id,
                    node: node.clone(),
                })
            }
            _ => Err(RegistrationLoadError::Invalid(format!(
                "Store registration {} origin differs from its activation authority",
                registration.device_id
            ))),
        }
    }

    pub(crate) async fn predecessor_commit_matching(
        &mut self,
        order: &store_commit::StoreCommitOrder,
        mut matches: PredecessorCommitPredicate<'_>,
    ) -> Result<Option<VerifiedStoreBatchCommit>, RegistrationLoadError> {
        let mut pending = order
            .predecessor
            .iter()
            .chain(order.dependencies.values())
            .cloned()
            .collect::<Vec<_>>();
        let mut visited = BTreeSet::new();
        while let Some(reference) = pending.pop() {
            if !visited.insert(reference.clone()) {
                continue;
            }
            let commit = self
                .load_ref(&reference)
                .await
                .map_err(registration_attempt_error)?;
            if matches(&commit) {
                return Ok(Some(commit));
            }
            pending.extend(commit.value().order.predecessor.iter().cloned());
            pending.extend(commit.value().order.dependencies.values().cloned());
        }
        Ok(None)
    }

    pub(crate) async fn verify_refs(
        &mut self,
        tips: impl IntoIterator<Item = StoreBatchCommitRef>,
    ) -> Result<(), StorePullError> {
        let root = self.root.reference().clone();
        let mut pending = tips.into_iter().collect::<Vec<_>>();
        let mut loaded = BTreeMap::<StoreBatchCommitRef, VerifiedStoreBatchCommit>::new();
        while let Some(reference) = pending.pop() {
            if self.history.commits.contains_key(&reference) || loaded.contains_key(&reference) {
                continue;
            }
            let verified = self.load_ref(&reference).await?;
            pending.extend(commit_predecessor_references(verified.value()));
            loaded.insert(reference, verified);
        }

        let mut states = self
            .history
            .commits
            .iter()
            .map(|(reference, verified)| (reference.clone(), verified.state_after.clone()))
            .collect::<BTreeMap<_, _>>();
        while !loaded.is_empty() {
            let next = loaded.iter().find_map(|(reference, verified)| {
                commit_predecessor_references(verified.value())
                    .iter()
                    .all(|dependency| states.contains_key(dependency))
                    .then(|| reference.clone())
            });
            let Some(reference) = next else {
                return Err(StorePullError::Database(
                    "Merge history is cyclic or has an unresolved predecessor".to_string(),
                ));
            };
            let verified = loaded.remove(&reference).ok_or_else(|| {
                StorePullError::Database(
                    "selected exclusion-history commit disappeared before verification".to_string(),
                )
            })?;
            let commit = verified.value().clone();
            let author = verified.author().clone();
            let (_, accepted_head) = self
                .commit_verifier
                .exact_next_announcement_slot(&commit.author_registration, &author, Some(&verified))
                .await
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let activation_head_ref = accepted_head.ok_or_else(|| {
                StorePullError::Database(
                    "Merge history commit has no accepted announcement head".to_string(),
                )
            })?;
            let predecessor_state =
                verified_merge_predecessor_state(&self.history.genesis, &states, &commit)?;
            let verified_membership_prefix = verified_merge_membership_prefix(
                &self.history.commits,
                commit_predecessor_references(&commit),
            )?;
            let pending_resolution =
                Box::pin(self.verify_resolution_activation_acceptance(&commit)).await?;
            let membership = self
                .load_membership_at_verified_prefix(
                    &commit.membership_state.heads,
                    &commit.membership_state.resolutions,
                    &verified_membership_prefix,
                    pending_resolution.as_ref(),
                )
                .await
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            verified_membership_prefix
                .validate_complete_membership(&membership)
                .map_err(StorePullError::Database)?;
            verify_merge_membership_state_ref(
                &commit.membership_state,
                &membership,
                &predecessor_state,
            )?;
            if !membership_authorizes(Some(&membership), &commit, &author) {
                return Err(StorePullError::Database(
                    "Merge history commit lacks exact membership authority".to_string(),
                ));
            }
            let accepted_frontier = commit_predecessor_references(&commit);
            let registrations = Box::pin(self.load_merge_commit_registrations(
                &commit,
                &author,
                &membership,
                &accepted_frontier,
            ))
            .await?;
            let (authorized_predecessor, recovery_author) = predecessor_state
                .clone()
                .preactivate_recovery_author(&commit, &registrations)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            if !device_state_has_active_registration(
                &authorized_predecessor,
                &commit.author_registration,
            ) {
                return Err(StorePullError::Database(
                    "author exclusion history commit author is inactive at its predecessor"
                        .to_string(),
                ));
            }
            let resolver = DeviceStateResolver::Loaded {
                genesis: &self.history.genesis,
                states: &states,
            };
            let operations = Box::pin(self.commit_verifier.load_commit_device_operations(
                Some(&resolver),
                &commit,
                &authorized_predecessor,
                Some(&membership),
            ))
            .await
            .map_err(|error| match error {
                RegistrationLoadError::Object(error) => StorePullError::Object(error),
                RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
            })?;
            let acknowledgement = self
                .validate_commit_acknowledgement(&commit, &author)
                .await
                .map_err(|error| match error {
                    RegistrationLoadError::Object(error) => StorePullError::Object(error),
                    RegistrationLoadError::Invalid(error) => StorePullError::Database(error),
                })?;
            let membership_control =
                if let Some(store_commit::StoreControl { transition }) = commit.control() {
                    let (activations, conflict_resolution) =
                        Box::pin(self.verify_membership_control_with_retained_history(
                            &reference,
                            &commit,
                            &membership,
                            &predecessor_state,
                            pending_resolution.as_ref(),
                        ))
                        .await
                        .map_err(StorePullError::Database)?;
                    Some(VerifiedMergeMembershipControl {
                        activations,
                        head_activation: VerifiedMergeMembershipHeadActivation {
                            commit: reference.clone(),
                            transition: transition.clone(),
                        },
                        conflict_resolution,
                    })
                } else {
                    None
                };
            let owner_recovery = self
                .commit_verifier
                .verify_owner_recovery_activation(&commit)
                .await?;
            let state = operations
                .apply_to(authorized_predecessor, &commit.device_state)
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let state = state
                .apply_verified_lifecycle(
                    &commit,
                    &registrations,
                    recovery_author.as_ref(),
                    owner_recovery,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            let predecessor_histories = commit_predecessor_references(&commit)
                .iter()
                .map(|predecessor| {
                    self.history
                        .commits
                        .get(predecessor)
                        .map(|verified: &VerifiedMergeHistoryCommit| verified.history.clone())
                        .ok_or_else(|| {
                            StorePullError::Database(
                                "Merge history summary has an unresolved predecessor".to_string(),
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            let membership_closure = Box::pin(
                self.commit_verifier
                    .verified_merge_membership_objects(&reference, &commit),
            )
            .await?;
            let retained_registrations = registrations
                .iter()
                .map(|registration| registration.registration().clone())
                .collect();
            let retained_acknowledgement = match acknowledgement.clone() {
                Some((acknowledgement_ref, acknowledgement_value)) => Some(
                    self.retain_acknowledgement(
                        &reference,
                        &commit,
                        &author,
                        acknowledgement_ref,
                        acknowledgement_value,
                    )
                    .await?,
                ),
                None => None,
            };
            let successor = compose_merge_history_successor(
                &root,
                &commit,
                &reference,
                &membership,
                &author,
                state.clone(),
                predecessor_histories,
                MergeHistorySuccessorEvidence {
                    registrations: retained_registrations,
                    acknowledgement: retained_acknowledgement,
                    membership_proof: membership_closure.map(|closure| closure.proof),
                },
            )?;
            let activation_head = self
                .commit_verifier
                .load_head(&activation_head_ref, &author, &reference)
                .await?;
            let history = successor
                .summary
                .open(
                    &commit,
                    &reference,
                    &activation_head.value,
                    &activation_head_ref,
                    &state,
                )
                .map_err(|error| StorePullError::Database(error.to_string()))?;
            states.insert(reference.clone(), state.clone());
            self.history.commits.insert(
                reference,
                VerifiedMergeHistoryCommit {
                    verified,
                    predecessor_membership: membership,
                    predecessor_state,
                    state_after: state,
                    registrations,
                    operations,
                    acknowledgement,
                    membership_control,
                    activation_head: activation_head.value,
                    activation_head_object: activation_head.object,
                    history,
                },
            );
        }
        Ok(())
    }
}
