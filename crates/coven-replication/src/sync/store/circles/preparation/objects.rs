use super::*;

impl<'operation, 'storage> CircleCandidatePreparer<'operation, 'storage> {
    pub(super) async fn prepare_circle_object(
        &self,
        context: &ProtocolObjectContext,
        semantic_prefix: &str,
        extension: &str,
        bytes: Vec<u8>,
    ) -> Result<PreparedExactObject, CircleOperationError> {
        let slot = self
            .storage
            .allocate_protocol_slot(context, semantic_prefix, extension)
            .await
            .map_err(coven_protocol::objects::StoreObjectError::from)?;
        self.prepare_circle_object_at(context, slot, semantic_prefix, bytes)
    }

    pub(super) fn prepare_circle_object_at(
        &self,
        context: &ProtocolObjectContext,
        slot: coven_protocol::objects::ObjectSlot,
        semantic_prefix: &str,
        bytes: Vec<u8>,
    ) -> Result<PreparedExactObject, CircleOperationError> {
        self.storage
            .prepare_protocol_object(context, slot, semantic_prefix, bytes)
            .map_err(coven_protocol::objects::StoreObjectError::from)
            .map_err(CircleOperationError::from)
    }

    pub(super) async fn prepare_circle_activation_objects(
        &self,
        mut draft: CircleTransitionDraft,
        history: &CircleTransitionHistory,
        merged_branch_objects: &[CircleActivationObjects],
    ) -> Result<
        (
            PreparedCircleTransition,
            CircleActivationObjects,
            BTreeMap<String, PreparedExactObject>,
            Option<ExactObjectRef>,
            Vec<StreamActivation>,
        ),
        CircleOperationError,
    > {
        let storage = self.storage.as_ref();
        let root = &self.root;
        let local_writer = std::sync::Arc::clone(&self.local_writer);
        let identity_signer = local_writer.as_ref();
        let store_root_hash = root.store_root_hash;
        let encryption = EncryptionService::from(
            MasterKeyring::from_serialized(&draft.keyring).map_err(CircleOperationError::from)?,
        );
        if encryption.seal_key_fingerprint() != draft.metadata.key_fingerprint {
            return Err(CircleOperationError::InvalidState(
                "Circle transition metadata does not use the keyring seal key".to_string(),
            ));
        }
        let metadata_context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleMetadata,
            encryption.clone(),
        );
        let roster_context = ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleRoster,
            encryption.clone(),
        );
        let control_context = ProtocolObjectContext::store_encrypted(
            store_root_hash,
            ProtocolObjectDomain::CircleControl,
        );
        let previous_control = match history {
            CircleTransitionHistory::Founder => None,
            CircleTransitionHistory::Successor(reference) => Some(reference.as_ref()),
        };
        let previous_objects =
            previous_control.map(coven_protocol::store_commit::CircleControlRef::objects);
        let mut roster_entries =
            previous_objects.map_or_else(BTreeMap::new, |objects| objects.roster_entries.clone());
        let mut roster_heads =
            previous_objects.map_or_else(Vec::new, |objects| objects.roster_heads.clone());
        let mut roster_frontier = if matches!(
            &draft.policy.roster,
            coven_protocol::circle::CircleRosterDraftPolicy::Founder { .. }
        ) {
            Vec::new()
        } else {
            draft.control.value.access_epoch().roster.heads.clone()
        };
        let mut roster_resolutions = previous_objects
            .map_or_else(BTreeMap::new, |objects| objects.roster_resolutions.clone());
        let mut metadata_entries =
            previous_objects.map_or_else(BTreeMap::new, |objects| objects.metadata_entries.clone());
        let mut metadata_heads =
            previous_objects.map_or_else(Vec::new, |objects| objects.metadata_heads.clone());
        // A control-conflict resolution covers the losing branches too: union their
        // already-published objects into the seed so the resolution can verify both
        // its merged current frontier and historical authority references. Roster
        // heads are an object inventory: collapsing them by author stream would
        // discard the older head that created an Owner grant. Metadata heads carry
        // the current frontier because their signed predecessor links provide their
        // history. The draft control separately carries the signed current
        // frontiers that the resolution shaped.
        for branch in merged_branch_objects {
            roster_entries.extend(branch.roster_entries.clone());
            roster_resolutions.extend(branch.roster_resolutions.clone());
            metadata_entries.extend(branch.metadata_entries.clone());
            roster_heads.extend(branch.roster_heads.iter().cloned());
            for head in &branch.metadata_heads {
                coven_protocol::circle::merge_frontier_head(
                    &mut metadata_heads,
                    head.clone(),
                    |head| head.coord.stream_key(),
                    |head| head.coord.seq,
                );
            }
        }
        roster_heads.sort();
        roster_heads.dedup();
        metadata_heads.sort_by_key(|head| head.coord.stream_key());
        let mut prepared = BTreeMap::new();
        let mut stream_activations = Vec::new();
        let mut close_outcome = None;
        let mut close_cancellation = None;

        let policy_objects = {
            let owner_grant = draft.metadata.author_owner_grant.clone();
            let roster_stream = local_writer.circle_grant_authorized_stream_id(
                store_root_hash,
                &owner_grant,
                StreamAnchorDomain::CircleRoster {
                    circle_id: draft.circle_id,
                },
            );
            let metadata_stream = local_writer.circle_grant_authorized_stream_id(
                store_root_hash,
                &owner_grant,
                StreamAnchorDomain::CircleMetadata {
                    circle_id: draft.circle_id,
                },
            );
            let control_stream = local_writer.circle_grant_authorized_stream_id(
                store_root_hash,
                &owner_grant,
                StreamAnchorDomain::CircleControl {
                    circle_id: draft.circle_id,
                },
            );
            if roster_stream == metadata_stream
                || roster_stream == control_stream
                || metadata_stream == control_stream
            {
                return Err(CircleOperationError::InvalidState(
                    "Circle control, roster, and metadata domains derived the same stream"
                        .to_string(),
                ));
            }

            let roster_policy = std::mem::replace(
                &mut draft.policy.roster,
                coven_protocol::circle::CircleRosterDraftPolicy::Inherited,
            );
            let roster_successor = match roster_policy {
                coven_protocol::circle::CircleRosterDraftPolicy::Inherited => None,
                coven_protocol::circle::CircleRosterDraftPolicy::Founder { entry } => {
                    Some((true, None, entry))
                }
                coven_protocol::circle::CircleRosterDraftPolicy::Successor {
                    predecessor,
                    entry,
                } => Some((false, Some(predecessor), entry)),
            };
            let prepared_roster = if let Some((founder, predecessor_chain, mut entry)) =
                roster_successor
            {
                entry.body_mut().stream_id = roster_stream;
                entry.resign(identity_signer);
                let entry_prefix = circle_semantic_prefix(CircleSemanticSlot::RosterEntry {
                    circle_id: draft.circle_id,
                    coord: &entry.coord(),
                });
                let entry_prepared = self
                    .prepare_circle_object(
                        &roster_context,
                        &entry_prefix,
                        ".json",
                        serde_json::to_vec(&entry)
                            .expect("Circle roster entry serialization cannot fail"),
                    )
                    .await?;
                prepared.insert("roster-entry".to_string(), entry_prepared.clone());
                roster_entries.insert(entry.coord(), entry_prepared.reference().clone());

                let stream_key = entry.coord().stream_key();
                let prior_roster = roster_frontier
                    .iter()
                    .find(|head| head.coord.stream_key() == stream_key)
                    .cloned();
                let (current_slot, seq, predecessor, activation_id, activation) =
                    if let Some(reference) = &prior_roster {
                        let prefix = circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                            circle_id: draft.circle_id,
                            head: reference,
                        });
                        let bytes = read_exact_circle_object(
                            storage,
                            &roster_context,
                            &reference.object,
                            &prefix,
                        )
                        .await?;
                        let head: coven_protocol::circle::CircleRosterHead =
                            serde_json::from_slice(&bytes)?;
                        if !local_writer.verify_circle_roster_head(&head)
                            || head.entry_coord() != reference.coord
                            || head.head_hash() != reference.head_hash
                        {
                            return Err(CircleOperationError::InvalidState(
                                "Circle roster predecessor head failed verification".to_string(),
                            ));
                        }
                        (
                            head.successor.next_slot.clone(),
                            head.seq.checked_add(1).ok_or_else(|| {
                                CircleOperationError::InvalidState(
                                    "Circle roster sequence overflow".to_string(),
                                )
                            })?,
                            Some(reference.object.clone()),
                            head.successor.activation,
                            None,
                        )
                    } else {
                        let current_prefix =
                            circle_roster_head_prefix(draft.circle_id, &stream_key, 1);
                        let current_slot = storage
                            .allocate_protocol_slot(&roster_context, &current_prefix, ".json")
                            .await
                            .map_err(coven_protocol::objects::StoreObjectError::from)?;
                        let activation = local_writer.circle_grant_authorized_activation(
                            store_root_hash,
                            owner_grant.clone(),
                            GrantStreamAnchor::CircleRoster {
                                circle_id: draft.circle_id,
                                first_slot: current_slot.clone(),
                            },
                        );
                        (
                            current_slot,
                            1,
                            None,
                            activation.activation_id(),
                            Some(activation),
                        )
                    };
                if entry.seq != seq
                    || entry.previous_hash
                        != prior_roster
                            .as_ref()
                            .map(|reference| reference.coord.entry_hash)
                {
                    return Err(CircleOperationError::InvalidState(
                        "Circle roster successor differs from its exact author-stream predecessor"
                            .to_string(),
                    ));
                }
                let current_prefix = circle_roster_head_prefix(draft.circle_id, &stream_key, seq);
                let next_slot = storage
                    .allocate_protocol_slot(
                        &roster_context,
                        &circle_roster_head_prefix(
                            draft.circle_id,
                            &stream_key,
                            seq.checked_add(1).ok_or_else(|| {
                                CircleOperationError::InvalidState(
                                    "Circle roster sequence overflow".to_string(),
                                )
                            })?,
                        ),
                        ".json",
                    )
                    .await
                    .map_err(coven_protocol::objects::StoreObjectError::from)?;
                let head = local_writer.sign_circle_roster_head(
                    &entry,
                    entry_prepared.reference().clone(),
                    SuccessorLink {
                        activation: activation_id,
                        predecessor,
                        next_slot,
                    },
                );
                let head_prepared = self.prepare_circle_object_at(
                    &roster_context,
                    current_slot,
                    &current_prefix,
                    serde_json::to_vec(&head)
                        .expect("Circle roster head serialization cannot fail"),
                )?;
                let head_ref =
                    CircleRosterHeadRef::from_stored_head(&head, head_prepared.reference().clone());
                prepared.insert("roster-head".to_string(), head_prepared);
                roster_frontier.retain(|reference| reference.coord.stream_key() != stream_key);
                roster_frontier.push(head_ref.clone());
                roster_frontier.sort_by_key(|head| head.coord.stream_key());
                roster_heads.push(head_ref.clone());
                roster_heads.sort();
                if let Some(activation) = activation {
                    stream_activations.push(activation);
                }
                Some((founder, predecessor_chain, entry, head, head_ref))
            } else {
                None
            };

            if let Some((_, predecessor_chain, entry, head, reference)) = &prepared_roster {
                let exact_head = coven_protocol::circle::ExactCircleRosterHead::bind(
                    head.clone(),
                    reference.clone(),
                )
                .map_err(CircleOperationError::from)?;
                let chain = match predecessor_chain {
                    Some(predecessor) => {
                        predecessor.with_exact_successor(entry.clone(), exact_head)
                    }
                    None => coven_protocol::circle::CircleRosterChain::from_entries_with_heads(
                        vec![entry.clone()],
                        vec![exact_head],
                    ),
                }
                .map_err(CircleOperationError::from)?;
                draft.roster = chain.try_resolved().map_err(CircleOperationError::from)?;
            }

            let roster_state = coven_protocol::circle::MergeCircleRosterStateRef {
                heads: roster_frontier,
                resolutions: roster_resolutions.keys().cloned().collect(),
                state_hash: draft.roster.state_hash,
            };
            let (metadata_state, metadata_head) = if draft.policy.metadata_successor {
                let selects_authored_metadata =
                    draft.control.value.access_epoch().metadata.selected == draft.metadata.coord();
                draft.metadata.body_mut().author_roster = roster_state.clone();
                let prior_metadata = metadata_heads
                    .iter()
                    .find(|head| head.coord.stream_id == metadata_stream)
                    .cloned();
                let (metadata_slot, metadata_seq, metadata_previous, metadata_activation) =
                    if let Some(reference) = &prior_metadata {
                        let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                            circle_id: draft.circle_id,
                            head: reference,
                        });
                        let bytes = read_exact_circle_object(
                            storage,
                            &metadata_context,
                            &reference.object,
                            &prefix,
                        )
                        .await?;
                        let head: coven_protocol::circle::CircleMetadataHead =
                            serde_json::from_slice(&bytes)?;
                        if !local_writer.verify_circle_metadata_head(&head)
                            || head.coord() != reference.coord
                        {
                            return Err(CircleOperationError::InvalidState(
                                "Circle metadata predecessor head failed verification".to_string(),
                            ));
                        }
                        (
                            head.successor.next_slot.clone(),
                            head.seq.checked_add(1).ok_or_else(|| {
                                CircleOperationError::InvalidState(
                                    "Circle metadata sequence overflow".to_string(),
                                )
                            })?,
                            Some(head.tip_hash),
                            None,
                        )
                    } else {
                        let stream_key = coven_protocol::circle::CircleAuthorStreamKey {
                            author_pubkey: draft.metadata.author_pubkey.clone(),
                            device_id: draft.metadata.device_id.clone(),
                            stream_id: metadata_stream,
                            author_owner_grant: owner_grant.clone(),
                        };
                        let prefix = circle_metadata_head_prefix(draft.circle_id, &stream_key, 1);
                        let slot = storage
                            .allocate_protocol_slot(&metadata_context, &prefix, ".json")
                            .await
                            .map_err(coven_protocol::objects::StoreObjectError::from)?;
                        let activation = local_writer.circle_grant_authorized_activation(
                            store_root_hash,
                            owner_grant.clone(),
                            GrantStreamAnchor::CircleMetadata {
                                circle_id: draft.circle_id,
                                first_slot: slot.clone(),
                            },
                        );
                        (slot, 1, None, Some(activation))
                    };
                let metadata = draft.metadata.body_mut();
                metadata.stream_id = metadata_stream;
                metadata.seq = metadata_seq;
                metadata.previous_hash = metadata_previous;
                metadata.dependencies = metadata_heads
                    .iter()
                    .map(|head| head.coord.clone())
                    .collect();
                draft.metadata.resign(identity_signer);
                let metadata_prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataEntry {
                    circle_id: draft.circle_id,
                    coord: &draft.metadata.coord(),
                });
                let metadata_prepared = self
                    .prepare_circle_object(
                        &metadata_context,
                        &metadata_prefix,
                        ".json",
                        serde_json::to_vec(&draft.metadata)
                            .expect("Circle metadata serialization cannot fail"),
                    )
                    .await?;
                prepared.insert("metadata".to_string(), metadata_prepared.clone());
                metadata_entries.insert(
                    draft.metadata.coord(),
                    CircleMetadataObjectRef {
                        key_fingerprint: draft.metadata.key_fingerprint,
                        object: metadata_prepared.reference().clone(),
                    },
                );
                let metadata_activation_id = match &metadata_activation {
                    Some(activation) => activation.activation_id(),
                    None => {
                        let reference = prior_metadata.as_ref().expect("prior metadata head");
                        let prefix = circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                            circle_id: draft.circle_id,
                            head: reference,
                        });
                        let bytes = read_exact_circle_object(
                            storage,
                            &metadata_context,
                            &reference.object,
                            &prefix,
                        )
                        .await?;
                        let head: coven_protocol::circle::CircleMetadataHead =
                            serde_json::from_slice(&bytes)?;
                        head.successor.activation
                    }
                };
                let metadata_stream_key = draft.metadata.coord().stream_key();
                let metadata_next_slot = storage
                    .allocate_protocol_slot(
                        &metadata_context,
                        &circle_metadata_head_prefix(
                            draft.circle_id,
                            &metadata_stream_key,
                            metadata_seq.checked_add(1).ok_or_else(|| {
                                CircleOperationError::InvalidState(
                                    "Circle metadata sequence overflow".to_string(),
                                )
                            })?,
                        ),
                        ".json",
                    )
                    .await
                    .map_err(coven_protocol::objects::StoreObjectError::from)?;
                let metadata_head = local_writer.sign_circle_metadata_head(
                    &draft.metadata,
                    metadata_prepared.reference().clone(),
                    SuccessorLink {
                        activation: metadata_activation_id,
                        predecessor: prior_metadata.as_ref().map(|head| head.object.clone()),
                        next_slot: metadata_next_slot,
                    },
                );
                let metadata_head_prefix = circle_metadata_head_prefix(
                    draft.circle_id,
                    &metadata_stream_key,
                    metadata_seq,
                );
                let metadata_head_prepared = self.prepare_circle_object_at(
                    &metadata_context,
                    metadata_slot,
                    &metadata_head_prefix,
                    serde_json::to_vec(&metadata_head)
                        .expect("Circle metadata head serialization cannot fail"),
                )?;
                let metadata_head_ref = CircleMetadataHeadRef::from_stored_head(
                    &metadata_head,
                    metadata_head_prepared.reference().clone(),
                );
                prepared.insert("metadata-head".to_string(), metadata_head_prepared);
                metadata_heads.retain(|head| head.coord.stream_id != metadata_stream);
                metadata_heads.push(metadata_head_ref);
                metadata_heads.sort_by_key(|head| head.coord.stream_key());
                if let Some(activation) = metadata_activation {
                    stream_activations.push(activation);
                }

                let selected = if selects_authored_metadata
                    || draft
                        .control
                        .value
                        .value
                        .state
                        .access_epoch()
                        .metadata
                        .heads
                        .is_empty()
                {
                    draft.metadata.coord()
                } else {
                    draft
                        .control
                        .value
                        .value
                        .state
                        .access_epoch()
                        .metadata
                        .selected
                        .clone()
                };
                (
                    coven_protocol::circle::MergeCircleMetadataStateRef {
                        heads: metadata_heads.clone(),
                        selected: selected.clone(),
                        state_hash: if selected == draft.metadata.coord() {
                            draft.metadata.metadata_hash()
                        } else {
                            draft.control.value.access_epoch().metadata.state_hash
                        },
                    },
                    Some(metadata_head),
                )
            } else {
                let metadata_state = draft.control.value.access_epoch().metadata.clone();
                if !draft.metadata.verify()
                    || metadata_state.selected != draft.metadata.coord()
                    || !metadata_entries.contains_key(&metadata_state.selected)
                {
                    return Err(CircleOperationError::InvalidState(
                        "Circle transition inherited invalid selected metadata".to_string(),
                    ));
                }
                (metadata_state, None)
            };

            for access in &mut draft.access {
                if let CircleAccessDisposition::Active { roster, .. } =
                    &mut access.value.body_mut().disposition
                {
                    *roster = roster_state.clone();
                }
                access.value.resign(identity_signer);
                *access = PreparedAccessLeaf::seal(access.value.clone())?;
            }
            // A deletion carries no access material: its map is empty and it
            // publishes no leaves.
            let access_map = CircleAccessMap::from_leaves(&draft.access)?;

            let mut control_frontier = draft
                .control
                .value
                .value
                .state
                .access_epoch()
                .covered_control_heads
                .clone();
            if let Some(previous) = previous_control {
                let head_hash = previous.head_hash();
                let head_object = previous.head_object();
                control_frontier
                    .retain(|head| head.coord.stream_key() != previous.control().stream_key());
                control_frontier.push(coven_protocol::circle::MergeCircleControlHeadRef {
                    coord: previous.control().clone(),
                    head_hash,
                    object: head_object.clone(),
                });
            }
            control_frontier.sort_by_key(|head| head.coord.stream_key());
            let prior_control = control_frontier
                .iter()
                .find(|head| head.coord.stream_key().stream_id == control_stream)
                .cloned();
            let (control_slot, control_seq, control_previous, control_activation) =
                if let Some(reference) = &prior_control {
                    let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                        circle_id: draft.circle_id,
                        control: &reference.coord,
                    });
                    let bytes = read_exact_circle_object(
                        storage,
                        &control_context,
                        &reference.object,
                        &prefix,
                    )
                    .await?;
                    let head: coven_protocol::circle::CircleControlHead =
                        serde_json::from_slice(&bytes)?;
                    (
                        head.successor.next_slot.clone(),
                        head.control.seq.checked_add(1).ok_or_else(|| {
                            CircleOperationError::InvalidState(
                                "Circle control sequence overflow".to_string(),
                            )
                        })?,
                        Some(head.control.control_hash()),
                        None,
                    )
                } else {
                    let stream_key = coven_protocol::circle::CircleAuthorStreamKey {
                        author_pubkey: draft.control.value.author_pubkey.clone(),
                        device_id: local_writer.circle_device_id(),
                        stream_id: control_stream,
                        author_owner_grant: owner_grant.clone(),
                    };
                    let prefix = circle_control_head_prefix(draft.circle_id, &stream_key, 1);
                    let slot = storage
                        .allocate_protocol_slot(&control_context, &prefix, ".json")
                        .await
                        .map_err(coven_protocol::objects::StoreObjectError::from)?;
                    let activation = local_writer.circle_grant_authorized_activation(
                        store_root_hash,
                        owner_grant.clone(),
                        GrantStreamAnchor::CircleControl {
                            circle_id: draft.circle_id,
                            first_slot: slot.clone(),
                        },
                    );
                    (slot, 1, None, Some(activation))
                };

            let coven_protocol::circle::CircleControlValue {
                order,
                state,
                access,
                author_authority,
                membership_authority: _,
            } = &mut draft.control.value.body_mut().value;
            let access_epoch = state.access_epoch_mut();
            order.device_id = local_writer.circle_device_id();
            order.stream_id = control_stream;
            order.author_owner_grant = owner_grant.clone();
            order.seq = control_seq;
            order.previous_control_hash = control_previous;
            order.dependencies = control_frontier
                .iter()
                .filter(|head| head.coord.stream_key().stream_id != control_stream)
                .map(|head| head.coord.clone())
                .collect();
            access_epoch.roster = roster_state.clone();
            access_epoch.metadata = metadata_state;
            *access = access_map;
            access_epoch.covered_control_heads = control_frontier;
            if let (
                Some((true, _, entry, _, _)),
                coven_protocol::circle::MergeCircleOwnerAuthorityRef::Roster {
                    roster,
                    created_at,
                    ..
                },
            ) = (&prepared_roster, author_authority)
            {
                *roster = roster_state;
                *created_at = entry.coord();
            }
            if let Some(finalization) = draft.close_finalization.take() {
                let active_epoch = state.active_epoch_mut().ok_or_else(|| {
                    CircleOperationError::InvalidState(
                        "Circle close finalization did not construct an active epoch".to_string(),
                    )
                })?;
                let successor = coven_protocol::circle::CircleEpochSuccessor {
                    epoch_id: active_epoch.common.epoch_id,
                    key_fingerprint: active_epoch.common.key_fingerprint,
                    owners: active_epoch.common.owners.clone(),
                    access_digest: access.digest(),
                    metadata: active_epoch.metadata.clone(),
                    roster: active_epoch.roster.clone(),
                    store_membership: active_epoch.store_membership.clone(),
                };
                let outcome = coven_protocol::circle::CircleEpochCloseOutcome::signed(
                    &finalization.close_control,
                    &finalization.intent,
                    finalization.responses,
                    successor,
                    identity_signer,
                )?;
                let outcome_hash = outcome.outcome_hash();
                let close_id = outcome.close_id;
                active_epoch.common.origin = coven_protocol::circle::CircleEpochOrigin::Closed {
                    closed_epoch_id: finalization.close_control.value.epoch_id(),
                    close_control: finalization.close_control.coord.clone(),
                    close_id,
                    outcome_hash,
                    cutoff: outcome.cutoff.clone(),
                };
                let outcome_prefix =
                    coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                        draft.circle_id,
                        close_id,
                    );
                let outcome_prepared = self.prepare_circle_object_at(
                    &ProtocolObjectContext::store_encrypted(
                        store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseOutcome,
                    ),
                    finalization.outcome_slot,
                    &outcome_prefix,
                    coven_protocol::circle::CircleEpochCloseSlotValue::Outcome(outcome.clone())
                        .to_bytes(),
                )?;
                let outcome_ref = coven_protocol::circle::CircleEpochCloseOutcomeRef::from_outcome(
                    &outcome,
                    outcome_prepared.reference().clone(),
                )?;
                prepared.insert("epoch-close-outcome".to_string(), outcome_prepared);
                close_outcome = Some((outcome, outcome_ref));
            }
            if let Some(cancellation_draft) = draft.close_cancellation.take() {
                let cancellation = coven_protocol::circle::CircleEpochCloseCancellation::signed(
                    &cancellation_draft.close_control,
                    identity_signer,
                )?;
                let cancellation_prefix =
                    coven_protocol::circle::circle_epoch_close_outcome_semantic_prefix(
                        draft.circle_id,
                        cancellation.close_id,
                    );
                let cancellation_prepared = self.prepare_circle_object_at(
                    &ProtocolObjectContext::store_encrypted(
                        store_root_hash,
                        ProtocolObjectDomain::CircleEpochCloseOutcome,
                    ),
                    cancellation_draft.outcome_slot,
                    &cancellation_prefix,
                    coven_protocol::circle::CircleEpochCloseSlotValue::Cancellation(
                        cancellation.clone(),
                    )
                    .to_bytes(),
                )?;
                let cancellation_ref =
                    coven_protocol::circle::CircleEpochCloseCancellationRef::from_cancellation(
                        &cancellation,
                        cancellation_prepared.reference().clone(),
                    )?;
                prepared.insert(
                    "epoch-close-cancellation".to_string(),
                    cancellation_prepared,
                );
                close_cancellation = Some((cancellation, cancellation_ref));
            }
            draft.control.value.resign(identity_signer);
            draft.control.coord = draft.control.value.coord();
            draft.control.bytes = serde_json::to_vec(&draft.control.value)
                .expect("Circle control serialization cannot fail");

            let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
                circle_id: draft.circle_id,
                control: &draft.control.coord,
            });
            let control_prepared = self
                .prepare_circle_object(
                    &control_context,
                    &control_prefix,
                    ".json",
                    draft.control.bytes.clone(),
                )
                .await?;
            prepared.insert("control".to_string(), control_prepared.clone());
            let control_activation_id = match &control_activation {
                Some(activation) => activation.activation_id(),
                None => {
                    let reference = prior_control.as_ref().expect("prior control head");
                    let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                        circle_id: draft.circle_id,
                        control: &reference.coord,
                    });
                    let bytes = read_exact_circle_object(
                        storage,
                        &control_context,
                        &reference.object,
                        &prefix,
                    )
                    .await?;
                    let head: coven_protocol::circle::CircleControlHead =
                        serde_json::from_slice(&bytes)?;
                    head.successor.activation
                }
            };
            let control_stream_key = draft.control.coord.stream_key();
            let control_next_slot = storage
                .allocate_protocol_slot(
                    &control_context,
                    &circle_control_head_prefix(
                        draft.circle_id,
                        &control_stream_key,
                        control_seq.checked_add(1).ok_or_else(|| {
                            CircleOperationError::InvalidState(
                                "Circle control sequence overflow".to_string(),
                            )
                        })?,
                    ),
                    ".json",
                )
                .await
                .map_err(coven_protocol::objects::StoreObjectError::from)?;
            let control_head = local_writer.sign_circle_control_head(
                &draft.control.value,
                control_prepared.reference().clone(),
                SuccessorLink {
                    activation: control_activation_id,
                    predecessor: prior_control.as_ref().map(|head| head.object.clone()),
                    next_slot: control_next_slot,
                },
            );
            let control_head_prefix =
                circle_control_head_prefix(draft.circle_id, &control_stream_key, control_seq);
            let control_head_prepared = self.prepare_circle_object_at(
                &control_context,
                control_slot,
                &control_head_prefix,
                serde_json::to_vec(&control_head)
                    .expect("Circle control head serialization cannot fail"),
            )?;
            prepared.insert("control-head".to_string(), control_head_prepared.clone());
            if let Some(activation) = control_activation {
                stream_activations.push(activation);
            }

            CircleTransitionPolicyObjects {
                roster: prepared_roster.map(|(_, _, entry, head, _)| {
                    coven_protocol::circle::CircleRosterPolicyObjects { entry, head }
                }),
                metadata_head,
                control_head,
            }
        };

        let bootstraps = draft
            .access
            .iter()
            .filter_map(|access| match &access.value.disposition {
                CircleAccessDisposition::Active {
                    bootstrap: Some(bootstrap),
                    ..
                } => Some(CircleBootstrapObjectRef {
                    owner_pubkey: access.value.owner_pubkey.clone(),
                    epoch_id: access.value.epoch_id,
                    recipient_slot: access.value.recipient_slot.clone(),
                    image: bootstrap.image.clone(),
                }),
                CircleAccessDisposition::Active { .. } | CircleAccessDisposition::Inactive => None,
            })
            .collect::<Vec<_>>();

        let control = prepared
            .get("control")
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "prepared Circle graph lacks its control object".to_string(),
                )
            })?
            .reference()
            .clone();
        let control_head_object = prepared
            .get("control-head")
            .map(|object| object.reference().clone());
        stream_activations.sort();
        let close_intent = match draft.control.value.state() {
            coven_protocol::circle::CircleControlState::ActiveEpoch(_)
            | coven_protocol::circle::CircleControlState::Deleted(_) => None,
            coven_protocol::circle::CircleControlState::EpochClose(close) => {
                Some(close.intent.clone())
            }
        };
        let transition = PreparedCircleTransition {
            circle_id: draft.circle_id,
            epoch_id: draft.epoch_id,
            keyring: draft.keyring,
            roster: draft.roster,
            policy_objects,
            metadata: draft.metadata,
            close_intent: draft.close_intent,
            close_outcome: close_outcome.as_ref().map(|(outcome, _)| outcome.clone()),
            close_cancellation: close_cancellation
                .as_ref()
                .map(|(cancellation, _)| cancellation.clone()),
            access: draft.access,
            control: draft.control,
        };
        Ok((
            transition,
            CircleActivationObjects {
                control,
                close_intent,
                close_outcome: close_outcome.map(|(_, reference)| reference),
                close_cancellation: close_cancellation.map(|(_, reference)| reference),
                roster_entries,
                roster_heads,
                roster_resolutions,
                metadata_entries,
                metadata_heads,
                bootstraps,
            },
            prepared,
            control_head_object,
            stream_activations,
        ))
    }
}
