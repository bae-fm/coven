use super::*;

/// Carry one inherited entry inventory forward: everything the predecessor
/// introduced is now inherited from the commit that activated it, and
/// everything it already inherited keeps the activation it named.
fn inherited_origin(
    activating_commit: &coven_protocol::store_commit::StoreBatchCommitRef,
    origin: &CircleEntryOrigin,
) -> CircleEntryOrigin {
    match origin {
        CircleEntryOrigin::Introduced => CircleEntryOrigin::Inherited {
            activating_commit: activating_commit.clone(),
        },
        inherited @ CircleEntryOrigin::Inherited { .. } => inherited.clone(),
    }
}

/// The author-stream frontier of an inherited entry inventory: the deepest
/// position each stream reaches. A control's signed frontier is exactly this,
/// because its inventory is exactly the history that frontier reaches.
///
/// One position per stream is all a frontier can hold, and that is all an
/// inventory can contain: acceptance refuses a control that introduces an
/// entry at a position accepted history already filled, so no inventory
/// carrying two entries at one position can ever be inherited.
fn inventory_frontier<C: Clone>(
    coords: impl Iterator<Item = C>,
    stream_key: impl Fn(&C) -> coven_protocol::circle::CircleAuthorStreamKey,
    seq: impl Fn(&C) -> u64,
) -> Vec<C> {
    let mut frontier: BTreeMap<coven_protocol::circle::CircleAuthorStreamKey, C> = BTreeMap::new();
    for coord in coords {
        match frontier.entry(stream_key(&coord)) {
            std::collections::btree_map::Entry::Vacant(slot) => {
                slot.insert(coord);
            }
            std::collections::btree_map::Entry::Occupied(mut slot) => {
                if seq(&coord) > seq(slot.get()) {
                    slot.insert(coord);
                }
            }
        }
    }
    frontier.into_values().collect()
}

fn inherit_entries(
    activation: &CircleControlActivation,
    roster_entries: &mut BTreeMap<
        coven_protocol::circle::CircleRosterCoord,
        coven_protocol::store_commit::CircleRosterEntryRef,
    >,
    metadata_entries: &mut BTreeMap<
        coven_protocol::circle::CircleMetadataCoord,
        CircleMetadataObjectRef,
    >,
) {
    let objects = activation.reference.objects();
    for (coord, reference) in &objects.roster_entries {
        roster_entries.insert(
            coord.clone(),
            coven_protocol::store_commit::CircleRosterEntryRef {
                object: reference.object.clone(),
                origin: inherited_origin(&activation.activating_commit, &reference.origin),
            },
        );
    }
    for (coord, reference) in &objects.metadata_entries {
        metadata_entries.insert(
            coord.clone(),
            CircleMetadataObjectRef {
                key_fingerprint: reference.key_fingerprint,
                object: reference.object.clone(),
                origin: inherited_origin(&activation.activating_commit, &reference.origin),
            },
        );
    }
}

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
        merged_branches: &[CircleControlActivation],
    ) -> Result<
        (
            PreparedCircleTransition,
            CircleActivationObjects,
            BTreeMap<String, PreparedExactObject>,
        ),
        CircleOperationError,
    > {
        let local_writer = std::sync::Arc::clone(&self.local_writer);
        let identity_signer = local_writer.as_ref();
        let store_root_hash = self.root.store_root_hash;
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
            encryption,
        );
        let control_context = ProtocolObjectContext::store_encrypted(
            store_root_hash,
            ProtocolObjectDomain::CircleControl,
        );
        let previous = match history {
            CircleTransitionHistory::Founder => None,
            CircleTransitionHistory::Successor(activation) => Some(activation.as_ref()),
        };
        let mut roster_entries = BTreeMap::new();
        let mut metadata_entries = BTreeMap::new();
        if let Some(previous) = previous {
            inherit_entries(previous, &mut roster_entries, &mut metadata_entries);
        }
        // A control-conflict resolution covers the losing branches too: inherit
        // their already-accepted entries under the activations that introduced
        // them, so the resolution's own inventory still resolves every entry to
        // its exact earlier accepted activation.
        for branch in merged_branches {
            inherit_entries(branch, &mut roster_entries, &mut metadata_entries);
        }

        // Both frontiers are derived from the inherited inventory, never from
        // the draft's own control: the inventory is the accepted history, and a
        // control's signed frontier is exactly the tips that history reaches.
        let mut roster_frontier = inventory_frontier(
            roster_entries.keys().cloned(),
            coven_protocol::circle::CircleRosterCoord::stream_key,
            |coord| coord.seq,
        );
        let mut metadata_frontier = inventory_frontier(
            metadata_entries.keys().cloned(),
            coven_protocol::circle::CircleMetadataCoord::stream_key,
            |coord| coord.seq,
        );
        let mut prepared = BTreeMap::new();
        let mut close_outcome = None;
        let mut close_cancellation = None;
        let mut introduced_metadata = None;

        let policy_objects = {
            let device_id = local_writer.circle_device_id();
            let author_pubkey = local_writer.author_pubkey();
            let owner_grant = draft.metadata.author_owner_grant.clone();
            let stream_key = coven_protocol::circle::CircleAuthorStreamKey {
                author_pubkey: author_pubkey.clone(),
                device_id: device_id.clone(),
                author_owner_grant: owner_grant.clone(),
            };

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
            let prepared_roster = if let Some((founder, predecessor_chain, entry)) =
                roster_successor
            {
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
                roster_entries.insert(
                    entry.coord(),
                    coven_protocol::store_commit::CircleRosterEntryRef {
                        object: entry_prepared.reference().clone(),
                        origin: CircleEntryOrigin::Introduced,
                    },
                );
                let prior = roster_frontier
                    .iter()
                    .find(|coord| coord.stream_key() == stream_key)
                    .cloned();
                if entry.seq != prior.as_ref().map_or(1, |coord| coord.seq + 1)
                    || entry.previous_hash != prior.as_ref().map(|coord| coord.entry_hash)
                {
                    return Err(CircleOperationError::InvalidState(
                        "Circle roster successor differs from its exact author-stream predecessor"
                            .to_string(),
                    ));
                }
                roster_frontier.retain(|coord| coord.stream_key() != stream_key);
                roster_frontier.push(entry.coord());
                roster_frontier.sort_by_key(coven_protocol::circle::CircleRosterCoord::stream_key);
                let chain = match predecessor_chain {
                    Some(predecessor) => predecessor.with_successor(entry.clone()),
                    None => {
                        coven_protocol::circle::CircleRosterChain::from_entries(vec![entry.clone()])
                    }
                }
                .map_err(CircleOperationError::from)?;
                draft.roster = chain.try_resolved().map_err(CircleOperationError::from)?;
                Some((founder, entry))
            } else {
                None
            };

            let roster_state = coven_protocol::circle::MergeCircleRosterStateRef {
                frontier: roster_frontier,
                state_hash: draft.roster.state_hash,
            };
            let metadata_state = if draft.policy.metadata_successor {
                let selects_authored_metadata =
                    draft.control.value.access_epoch().metadata.selected == draft.metadata.coord();
                draft.metadata.body_mut().author_roster = roster_state.clone();
                let prior = metadata_frontier
                    .iter()
                    .find(|coord| coord.stream_key() == stream_key)
                    .cloned();
                let metadata = draft.metadata.body_mut();
                metadata.device_id = device_id.clone();
                metadata.author_owner_grant = owner_grant.clone();
                metadata.seq = prior.as_ref().map_or(1, |coord| coord.seq + 1);
                metadata.previous_hash = prior.as_ref().map(|coord| coord.metadata_hash);
                metadata.dependencies = metadata_frontier.clone();
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
                introduced_metadata = Some(draft.metadata.clone());
                metadata_entries.insert(
                    draft.metadata.coord(),
                    CircleMetadataObjectRef {
                        key_fingerprint: draft.metadata.key_fingerprint,
                        object: metadata_prepared.reference().clone(),
                        origin: CircleEntryOrigin::Introduced,
                    },
                );
                metadata_frontier.retain(|coord| coord.stream_key() != stream_key);
                metadata_frontier.push(draft.metadata.coord());
                metadata_frontier
                    .sort_by_key(coven_protocol::circle::CircleMetadataCoord::stream_key);

                let selected = if selects_authored_metadata
                    || draft
                        .control
                        .value
                        .access_epoch()
                        .metadata
                        .frontier
                        .is_empty()
                {
                    draft.metadata.coord()
                } else {
                    draft.control.value.access_epoch().metadata.selected.clone()
                };
                coven_protocol::circle::MergeCircleMetadataStateRef {
                    frontier: metadata_frontier.clone(),
                    state_hash: if selected == draft.metadata.coord() {
                        draft.metadata.metadata_hash()
                    } else {
                        draft.control.value.access_epoch().metadata.state_hash
                    },
                    selected,
                }
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
                metadata_state
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

            let mut covered_controls = draft.control.value.access_epoch().covered_controls.clone();
            if let Some(previous) = previous {
                covered_controls.retain(|covered| {
                    covered.coord.stream_key() != previous.reference.control().stream_key()
                });
                covered_controls.push(coven_protocol::circle::CircleControlActivationRef {
                    coord: previous.reference.control().clone(),
                    activating_commit: previous.activating_commit.clone(),
                });
            }
            covered_controls.sort_by_key(|covered| covered.coord.stream_key());
            let prior_control = covered_controls
                .iter()
                .find(|covered| covered.coord.stream_key() == stream_key)
                .cloned();

            let coven_protocol::circle::CircleControlValue {
                order,
                state,
                access,
                author_authority,
                membership_authority: _,
            } = &mut draft.control.value.body_mut().value;
            let access_epoch = state.access_epoch_mut();
            order.device_id = device_id;
            order.author_owner_grant = owner_grant;
            order.seq = prior_control
                .as_ref()
                .map_or(1, |covered| covered.coord.seq + 1);
            order.previous_control_hash = prior_control
                .as_ref()
                .map(|covered| covered.coord.control_hash);
            order.dependencies = covered_controls
                .iter()
                .filter(|covered| covered.coord.stream_key() != stream_key)
                .map(|covered| covered.coord.clone())
                .collect();
            access_epoch.roster = roster_state.clone();
            access_epoch.metadata = metadata_state;
            *access = access_map;
            access_epoch.covered_controls = covered_controls;
            if let Some((true, entry)) = &prepared_roster {
                author_authority.roster = roster_state;
                author_authority.created_at = entry.coord();
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

            CircleTransitionPolicyObjects {
                roster: prepared_roster
                    .map(|(_, entry)| coven_protocol::circle::CircleRosterPolicyObjects { entry }),
                metadata: introduced_metadata,
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
                metadata_entries,
                bootstraps,
            },
            prepared,
        ))
    }
}
