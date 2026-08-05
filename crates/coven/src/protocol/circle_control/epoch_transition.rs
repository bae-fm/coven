use super::drafts::*;
use super::*;

impl CircleTransitionDraft {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn close_epoch(
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        close_id: CircleEpochCloseId,
        close_intent: CircleEpochCloseIntent,
        intent: CircleEpochCloseIntentRef,
        frozen_device_state: StoreDeviceStateRef,
        participants: Vec<CircleEpochCloseParticipant>,
        provisional_frontier: CommitFrontier,
        outcome_slot: ObjectSlot,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let context = circle_successor_context(
            store_members,
            current_control,
            current_roster,
            current_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members,
            author_pubkey,
            epoch: active_epoch,
            grant_id,
            author_authority,
            key_fingerprint,
        } = context;
        let store_root_hash = current_control.value.store_root_hash;
        let circle_id = current_control.value.circle_id;
        let epoch_id = current_control.value.epoch_id();
        if close_intent.close_id != close_id
            || close_intent.intent_hash() != intent.intent_hash
            || close_intent.circle_id != circle_id
            || close_intent.epoch_id != epoch_id
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let roster_state = active_epoch.roster.clone();
        let mut control_value = CircleControlBody {
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: active_epoch
                        .covered_control_heads
                        .iter()
                        .find(|head| {
                            head.coord.author_pubkey == author_pubkey
                                && head.coord.device_id == device_id
                                && head.coord.author_owner_grant == grant_id
                        })
                        .map_or_else(
                            || {
                                AuthorStreamId::from_digest(generated_id_digest(
                                    ids,
                                    b"coven.circle-transition-draft-stream.v1\0",
                                ))
                            },
                            |head| head.coord.stream_id,
                        ),
                    author_owner_grant: grant_id,
                    seq: current_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(current_control.coord.control_hash()),
                    dependencies: vec![current_control.coord.clone()],
                },
                state: CircleControlState::EpochClose(CircleEpochClose {
                    close_id,
                    frozen_epoch: MergeActiveCircleEpoch {
                        common: active_epoch.common.clone(),
                        metadata: active_epoch.metadata.clone(),
                        roster: roster_state.clone(),
                        store_membership,
                        covered_control_heads: active_epoch.covered_control_heads.clone(),
                    },
                    intent,
                    frozen_device_state,
                    participants,
                    provisional_frontier,
                    outcome_slot,
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
        };
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &current_roster.members(),
            &control_value.access_epoch().store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .access_epoch_mut()
            .common
            .access_root = access.access_root();
        let control_value = Signed::sign(control_value, signer);
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Inherited,
                metadata_successor: false,
            },
            metadata: current_metadata.clone(),
            close_intent: Some(close_intent),
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn finalize_epoch_close(
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        device_id: &str,
        author_registration: &StoreDeviceRegistrationRef,
        metadata_stamp: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        mut store_members: Vec<(String, MemberRole)>,
        close_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_roster_chain: CircleRosterChain,
        current_metadata: &CircleMetadata,
        keyring: &str,
        intent: CircleEpochCloseIntent,
        responses: Vec<CircleEpochCloseSettlement>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = close_control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        if !close_control.verify()
            || current_roster_chain.try_resolved()? != *current_roster
            || current_roster.state_hash() != close.frozen_epoch.roster.state_hash
            || current_metadata.coord() != close.frozen_epoch.metadata.selected
            || current_metadata.epoch_id != close.frozen_epoch.common.epoch_id
            || current_metadata.key_fingerprint != close.frozen_epoch.common.key_fingerprint
            || intent.intent_hash() != close.intent.intent_hash
            || intent.close_id != close.close_id
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let author_pubkey = keys::public_key_hex(signer);
        store_members.sort_by(|left, right| left.0.cmp(&right.0));
        store_members.dedup_by(|left, right| left.0 == right.0);
        if !store_members
            .iter()
            .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
        {
            return Err(CircleTransitionError::AuthorNotStoreWriter);
        }
        let (grant_id, owner_record) = current_roster
            .active_grants()
            .find(|(_, record)| {
                record.member_pubkey == author_pubkey
                    && record.role == crate::protocol::circle::CircleRole::Owner
            })
            .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
        let author_authority = match &owner_record.creation_authority {
            CircleGrantCreationAuthority::Entry(created_at) => {
                MergeCircleOwnerAuthorityRef::Roster {
                    roster: close.frozen_epoch.roster.clone(),
                    grant_id: grant_id.clone(),
                    created_at: created_at.clone(),
                }
            }
            CircleGrantCreationAuthority::ConflictResolution(resolution) => {
                MergeCircleOwnerAuthorityRef::ConflictResolution {
                    conflict_hash: resolution.conflict_hash,
                    resolution_hash: resolution.resolution_hash,
                }
            }
        };
        let old_encryption = EncryptionService::from(
            MasterKeyring::from_serialized(keyring)
                .map_err(|_| CircleTransitionError::InvalidCurrentState)?,
        );
        if old_encryption.seal_key_fingerprint() != close.frozen_epoch.common.key_fingerprint {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let new_generation = old_encryption
            .current_generation()
            .checked_add(1)
            .ok_or(CircleTransitionError::SequenceOverflow)?;
        let encryption = old_encryption
            .with_appended_generation(new_generation, crate::encryption::generate_random_key())
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        let keyring = encryption
            .to_keyring_string()
            .map_err(|_| CircleTransitionError::InvalidCurrentState)?;
        let key_fingerprint = encryption.seal_key_fingerprint();
        let epoch_id = CircleEpochId::generate(ids);
        let roster = current_roster_chain.resolved_with_successor(intent.removal.clone())?;
        if roster.state_hash() != intent.remaining_roster_state_hash {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let roster_members = roster.members();
        let owners = roster_members
            .iter()
            .filter_map(|(pubkey, role)| {
                (*role == crate::protocol::circle::CircleRole::Owner).then_some(pubkey.clone())
            })
            .collect::<Vec<_>>();
        if owners.is_empty() {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let roster_state = MergeCircleRosterStateRef {
            heads: close.frozen_epoch.roster.heads.clone(),
            resolutions: close.frozen_epoch.roster.resolutions.clone(),
            state_hash: roster.state_hash(),
        };
        let metadata_stream =
            crate::protocol::store_commit::StreamActivation::grant_authorized_stream_id(
                close_control.value.store_root_hash,
                author_registration,
                grant_id,
                crate::protocol::store_commit::StreamAnchorDomain::CircleMetadata {
                    circle_id: close_control.value.circle_id,
                },
            );
        let prior_metadata = close
            .frozen_epoch
            .metadata
            .heads
            .iter()
            .find(|head| head.coord.stream_id == metadata_stream);
        let mut metadata = current_metadata.clone();
        metadata.epoch_id = epoch_id;
        metadata.metadata_stamp = metadata_stamp.to_string();
        metadata.author_pubkey = author_pubkey.clone();
        metadata.device_id = device_id.to_string();
        metadata.stream_id = metadata_stream;
        metadata.author_owner_grant = grant_id.clone();
        metadata.seq = prior_metadata.map_or(Ok(1), |head| {
            head.coord
                .seq
                .checked_add(1)
                .ok_or(CircleTransitionError::SequenceOverflow)
        })?;
        metadata.previous_hash = prior_metadata.map(|head| head.coord.metadata_hash);
        metadata.dependencies = close
            .frozen_epoch
            .metadata
            .heads
            .iter()
            .map(|head| head.coord.clone())
            .collect();
        metadata.author_roster = roster_state.clone();
        metadata.key_fingerprint = key_fingerprint;
        metadata.signature = keys::sign_hex(signer, &metadata.canonical_bytes()).1;
        let metadata_state = MergeCircleMetadataStateRef {
            heads: close.frozen_epoch.metadata.heads.clone(),
            selected: metadata.coord(),
            state_hash: metadata.metadata_hash(),
        };
        let mut control_value = CircleControlBody {
            store_root_hash: close_control.value.store_root_hash,
            circle_id: close_control.value.circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: close_control.value.value.order.stream_id,
                    author_owner_grant: grant_id.clone(),
                    seq: close_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(close_control.coord.control_hash()),
                    dependencies: vec![close_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: ActiveCircleEpochCore {
                        epoch_id,
                        key_fingerprint,
                        owners,
                        access_root: close.frozen_epoch.common.access_root,
                        origin: close.frozen_epoch.common.origin.clone(),
                    },
                    metadata: metadata_state,
                    roster: roster_state.clone(),
                    store_membership,
                    covered_control_heads: close.frozen_epoch.covered_control_heads.clone(),
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
        };
        let access = CircleAccessDraft::prepare(
            control_value.store_root_hash,
            candidate_family,
            control_value.circle_id,
            epoch_id,
            &keyring,
            key_fingerprint,
            &roster_state,
            &roster_members,
            &control_value.access_epoch().store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("Circle finalization constructs an active epoch")
            .common
            .access_root = access.access_root();
        let control_value = Signed::sign(control_value, signer);
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("Circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id: control.value.circle_id,
            epoch_id,
            keyring,
            roster,
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Successor {
                    predecessor: current_roster_chain,
                    entry: intent.removal.clone(),
                },
                metadata_successor: true,
            },
            metadata,
            close_intent: None,
            close_finalization: Some(CircleEpochCloseFinalizationDraft {
                close_control: close_control.clone(),
                intent,
                responses,
                outcome_slot: close.outcome_slot.clone(),
            }),
            close_cancellation: None,
            access,
            control,
        })
    }

    /// Reopen a frozen epoch by cancelling its close. The successor restores the
    /// frozen epoch's protocol identity — same epoch, key generation, roster and
    /// metadata frontiers, and origin — re-issuing only the control-bound access
    /// material to the reopening control.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn reopen_epoch(
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        mut store_members: Vec<(String, MemberRole)>,
        close_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let CircleControlState::EpochClose(close) = close_control.value.state() else {
            return Err(CircleTransitionError::InvalidCurrentState);
        };
        let frozen = &close.frozen_epoch;
        if !close_control.verify()
            || current_roster.state_hash() != frozen.roster.state_hash
            || current_metadata.coord() != frozen.metadata.selected
            || current_metadata.epoch_id != frozen.common.epoch_id
            || current_metadata.key_fingerprint != frozen.common.key_fingerprint
        {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let author_pubkey = keys::public_key_hex(signer);
        store_members.sort_by(|left, right| left.0.cmp(&right.0));
        store_members.dedup_by(|left, right| left.0 == right.0);
        if !store_members
            .iter()
            .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
        {
            return Err(CircleTransitionError::AuthorNotStoreWriter);
        }
        let (grant_id, owner_record) = current_roster
            .active_grants()
            .find(|(_, record)| {
                record.member_pubkey == author_pubkey
                    && record.role == crate::protocol::circle::CircleRole::Owner
            })
            .ok_or(CircleTransitionError::AuthorNotCircleOwner)?;
        let author_authority = match &owner_record.creation_authority {
            CircleGrantCreationAuthority::Entry(created_at) => {
                MergeCircleOwnerAuthorityRef::Roster {
                    roster: frozen.roster.clone(),
                    grant_id: grant_id.clone(),
                    created_at: created_at.clone(),
                }
            }
            CircleGrantCreationAuthority::ConflictResolution(resolution) => {
                MergeCircleOwnerAuthorityRef::ConflictResolution {
                    conflict_hash: resolution.conflict_hash,
                    resolution_hash: resolution.resolution_hash,
                }
            }
        };
        let encryption = EncryptionService::from(
            MasterKeyring::from_serialized(keyring)
                .map_err(|_| CircleTransitionError::InvalidCurrentState)?,
        );
        let key_fingerprint = encryption.seal_key_fingerprint();
        if key_fingerprint != frozen.common.key_fingerprint {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
        let epoch_id = frozen.common.epoch_id;
        let roster_state = frozen.roster.clone();
        let mut control_value = CircleControlBody {
            store_root_hash: close_control.value.store_root_hash,
            circle_id: close_control.value.circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: close_control.value.value.order.stream_id,
                    author_owner_grant: grant_id.clone(),
                    seq: close_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(close_control.coord.control_hash()),
                    dependencies: vec![close_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: ActiveCircleEpochCore {
                        epoch_id,
                        key_fingerprint,
                        owners: frozen.common.owners.clone(),
                        access_root: frozen.common.access_root,
                        origin: frozen.common.origin.clone(),
                    },
                    metadata: frozen.metadata.clone(),
                    roster: roster_state.clone(),
                    store_membership,
                    covered_control_heads: frozen.covered_control_heads.clone(),
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
        };
        let access = CircleAccessDraft::prepare(
            control_value.store_root_hash,
            candidate_family,
            control_value.circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &current_roster.members(),
            &control_value.access_epoch().store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("Circle reopen constructs an active epoch")
            .common
            .access_root = access.access_root();
        let control_value = Signed::sign(control_value, signer);
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("Circle control serialization cannot fail"),
            value: control_value,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id: control.value.circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Inherited,
                metadata_successor: false,
            },
            metadata: current_metadata.clone(),
            close_intent: None,
            close_finalization: None,
            close_cancellation: Some(CircleEpochCloseCancellationDraft {
                close_control: close_control.clone(),
                outcome_slot: close.outcome_slot.clone(),
            }),
            access,
            control,
        })
    }
}
