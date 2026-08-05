use super::drafts::*;
use super::*;

impl CircleTransitionDraft {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn founder(
        store_root_hash: ObjectHash,
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        device_id: &str,
        name: &str,
        metadata_stamp: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        mut store_members: Vec<(String, MemberRole)>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let author_pubkey = keys::public_key_hex(signer);
        store_members.sort_by(|left, right| left.0.cmp(&right.0));
        store_members.dedup_by(|left, right| left.0 == right.0);
        if !store_members
            .iter()
            .any(|(pubkey, role)| pubkey == &author_pubkey && role.can_write())
        {
            return Err(CircleTransitionError::AuthorNotStoreWriter);
        }
        let owner_grant =
            MembershipGrantId(generated_id_digest(ids, OWNER_GRANT_ID_GENERATION_DOMAIN));
        let author_stream_id = AuthorStreamId::from_digest(generated_id_digest(
            ids,
            b"coven.circle-transition-draft-stream.v1\0",
        ));
        let circle_id = CircleId::founder(store_root_hash, &author_pubkey, &owner_grant);
        let epoch_id = CircleEpochId::generate(ids);
        let keyring = MasterKeyring::generate();
        let encryption = EncryptionService::from(keyring.clone());
        let key_fingerprint = encryption.seal_key_fingerprint();
        let entry = CircleRosterEntry::founder(
            store_root_hash,
            circle_id,
            device_id,
            author_stream_id,
            owner_grant.clone(),
            signer,
        );
        let roster_objects = FounderRosterObjects {
            resolved: CircleRosterChain::from_entries(vec![entry.clone()])?.resolved(),
            entry,
        };
        let roster_state = MergeCircleRosterStateRef {
            heads: Vec::new(),
            resolutions: Vec::new(),
            state_hash: roster_objects.resolved.state_hash,
        };
        let metadata = CircleMetadata::founder(
            store_root_hash,
            circle_id,
            epoch_id,
            name,
            metadata_stamp,
            device_id,
            author_stream_id,
            owner_grant.clone(),
            roster_state.clone(),
            key_fingerprint,
            signer,
        )?;
        let metadata_state = MergeCircleMetadataStateRef {
            heads: Vec::new(),
            selected: metadata.coord(),
            state_hash: metadata.metadata_hash(),
        };
        let roster = roster_objects.resolved.clone();
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            &keyring.to_serialized(),
            key_fingerprint,
            &roster_state,
            &roster.members(),
            &store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        let common = ActiveCircleEpochCore {
            epoch_id,
            key_fingerprint,
            owners: vec![author_pubkey.clone()],
            access_root: access.access_root(),
            origin: CircleEpochOrigin::Founder,
        };
        let value = CircleControlValue {
            order: MergeCircleControlOrder {
                device_id: device_id.to_string(),
                stream_id: author_stream_id,
                author_owner_grant: owner_grant.clone(),
                seq: 1,
                previous_control_hash: None,
                dependencies: Vec::new(),
            },
            state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                common,
                metadata: metadata_state,
                roster: roster_state.clone(),
                store_membership,
                covered_control_heads: Vec::new(),
            }),
            author_authority: MergeCircleOwnerAuthorityRef::Roster {
                roster: roster_state,
                grant_id: owner_grant.clone(),
                created_at: roster_objects.entry.coord(),
            },
            membership_authority,
        };
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value,
            author_pubkey: author_pubkey.clone(),
            signature: String::new(),
        };
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let policy_objects = CircleTransitionDraftPolicy {
            roster: CircleRosterDraftPolicy::Founder {
                entry: roster_objects.entry,
            },
            metadata_successor: true,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_serialized(),
            roster,
            policy: policy_objects,
            metadata,
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn add_member(
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_roster_chain: CircleRosterChain,
        current_metadata: &CircleMetadata,
        keyring: &str,
        roster_stream: AuthorStreamId,
        member_pubkey: String,
        role: crate::protocol::circle::CircleRole,
        bootstrap: CircleBootstrapRef,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        if current_roster_chain.try_resolved()? != *current_roster {
            return Err(CircleTransitionError::InvalidCurrentState);
        }
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
        if !store_members
            .iter()
            .any(|(pubkey, _)| pubkey == &member_pubkey)
        {
            return Err(CircleTransitionError::MemberNotInStore(member_pubkey));
        }
        let entry = current_roster_chain.signed_set_member(
            device_id,
            roster_stream,
            member_pubkey.clone(),
            role,
            signer,
        )?;
        let roster = current_roster_chain.resolved_with_successor(entry.clone())?;
        let roster_state = MergeCircleRosterStateRef {
            heads: active_epoch.roster.heads.clone(),
            resolutions: active_epoch.roster.resolutions.clone(),
            state_hash: roster.state_hash,
        };
        let store_root_hash = current_control.value.store_root_hash;
        let circle_id = current_control.value.circle_id;
        let epoch_id = current_control.value.epoch_id();
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: roster_stream,
                    author_owner_grant: grant_id.clone(),
                    seq: current_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(current_control.coord.control_hash()),
                    dependencies: vec![current_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: active_epoch.common.clone(),
                    metadata: active_epoch.metadata.clone(),
                    roster: roster_state.clone(),
                    store_membership,
                    covered_control_heads: active_epoch.covered_control_heads.clone(),
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let bootstraps =
            std::collections::BTreeMap::from([(member_pubkey.clone(), bootstrap.clone())]);
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &roster.members(),
            &control_value
                .value
                .state
                .active_epoch()
                .expect("member addition constructs an active epoch")
                .store_membership,
            &store_members,
            &bootstraps,
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("member addition constructs an active epoch")
            .common
            .access_root = access.access_root();
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
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
            roster,
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Successor {
                    predecessor: current_roster_chain,
                    entry,
                },
                metadata_successor: false,
            },
            metadata: current_metadata.clone(),
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn rename(
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        device_id: &str,
        name: &str,
        metadata_stamp: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        if name.trim().is_empty() {
            return Err(CircleTransitionError::EmptyName);
        }
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
        let roster_state = current_control.value.roster_state_ref();

        let own_head = active_epoch.metadata.heads.iter().find(|head| {
            head.coord.author_pubkey == author_pubkey
                && head.coord.device_id == device_id
                && head.coord.author_owner_grant == grant_id
        });
        let author_stream_id = own_head.map_or_else(
            || {
                AuthorStreamId::from_digest(generated_id_digest(
                    ids,
                    b"coven.circle-transition-draft-stream.v1\0",
                ))
            },
            |head| head.coord.stream_id,
        );
        let metadata_seq = match own_head {
            Some(head) => head
                .coord
                .seq
                .checked_add(1)
                .ok_or(CircleTransitionError::SequenceOverflow)?,
            None => 1,
        };
        let metadata_previous = own_head.map(|head| head.coord.metadata_hash);
        let metadata_dependencies = active_epoch
            .metadata
            .heads
            .iter()
            .map(|head| head.coord.clone())
            .collect::<Vec<_>>();
        let metadata_state = active_epoch.metadata.clone();
        let mut control_value = CircleControlValue {
            order: MergeCircleControlOrder {
                device_id: device_id.to_string(),
                stream_id: author_stream_id,
                author_owner_grant: grant_id.clone(),
                seq: current_control
                    .value
                    .ordinal()
                    .checked_add(1)
                    .ok_or(CircleTransitionError::SequenceOverflow)?,
                previous_control_hash: Some(current_control.coord.control_hash()),
                dependencies: vec![current_control.coord.clone()],
            },
            state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                common: active_epoch.common.clone(),
                metadata: active_epoch.metadata.clone(),
                roster: active_epoch.roster.clone(),
                store_membership,
                covered_control_heads: active_epoch.covered_control_heads.clone(),
            }),
            author_authority,
            membership_authority,
        };
        let author_owner_grant = grant_id.clone();

        let mut metadata = CircleMetadata {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            epoch_id,
            name: name.to_string(),
            seq: metadata_seq,
            previous_hash: metadata_previous,
            dependencies: metadata_dependencies,
            metadata_stamp: metadata_stamp.to_string(),
            author_pubkey: author_pubkey.clone(),
            device_id: device_id.to_string(),
            stream_id: author_stream_id,
            author_owner_grant,
            author_roster: roster_state.clone(),
            key_fingerprint,
            signature: String::new(),
        };
        metadata.signature = keys::sign_hex(signer, &metadata.canonical_bytes()).1;

        let mut metadata_state = metadata_state;
        let selected = [current_metadata, &metadata]
            .into_iter()
            .max_by_key(|entry| {
                (
                    entry.metadata_stamp.as_str(),
                    entry.author_pubkey.as_str(),
                    entry.device_id.as_str(),
                    entry.metadata_hash(),
                )
            })
            .expect("current and successor metadata are non-empty");
        metadata_state.selected = selected.coord();
        metadata_state.state_hash = selected.metadata_hash();

        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &current_roster.members(),
            &control_value
                .state
                .active_epoch()
                .expect("rename constructs an active epoch")
                .store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        let active_epoch = control_value
            .state
            .active_epoch_mut()
            .expect("rename constructs an active epoch");
        active_epoch.common.access_root = access.access_root();
        active_epoch.metadata = metadata_state;
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: control_value,
            author_pubkey,
            signature: String::new(),
        };
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
        let policy_objects = CircleTransitionDraftPolicy {
            roster: CircleRosterDraftPolicy::Inherited,
            metadata_successor: true,
        };
        let access = access.finish(&control)?;
        Ok(Self {
            circle_id,
            epoch_id,
            keyring: keyring.to_string(),
            roster: current_roster.clone(),
            policy: policy_objects,
            metadata,
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    /// Build a successor of the chosen conflicting branch that causally covers
    /// every branch, collapsing a retained `ControlConflict` to a single
    /// resolved control. The successor inherits the chosen branch's epoch, key
    /// generation, and roster contents verbatim — it changes no membership, keys,
    /// or deletion intent. It merges the control, metadata, and roster head
    /// frontiers across every branch (the union of covered heads), so a device
    /// that authored a losing branch continues its own author streams instead of
    /// re-allocating their head slots. The name is not inherited from the chosen
    /// branch but re-derived as the deterministic metadata selection over the
    /// merged frontier: the metadata layer resolves its own conflict — the
    /// canonical maximum across every covered head — independent of which control
    /// branch the Owner chose. `losing_branches` are the retained branches other
    /// than `chosen`; preparation adds the chosen branch head, so the resolved
    /// control's causal dependencies and predecessor together name every branch.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn resolve(
        candidate_family: crate::protocol::store_commit::CandidateFamilyId,
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        chosen_control: &PreparedCircleControl,
        chosen_roster: &CircleMaterializedRoster,
        chosen_metadata: &CircleMetadata,
        keyring: &str,
        losing_branches: Vec<ResolvedConflictBranch>,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let context = circle_successor_context(
            store_members,
            chosen_control,
            chosen_roster,
            chosen_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members,
            author_pubkey,
            epoch: active_epoch,
            grant_id: _,
            author_authority,
            key_fingerprint,
        } = context;
        let store_root_hash = chosen_control.value.store_root_hash;
        let circle_id = chosen_control.value.circle_id;
        let epoch_id = chosen_control.value.epoch_id();
        let roster_state = chosen_control.value.roster_state_ref();

        // Merge the control, metadata, and roster head frontiers across every
        // branch: the chosen branch's frontiers extended by each losing branch's,
        // one head per author stream at its deepest position. Every branch's
        // heads become covered, so no author-stream head is re-allocated once the
        // conflict collapses. Preparation adds the chosen branch head and derives
        // the predecessor and dependencies from the control frontier, so the
        // resolved control directly names every branch.
        let mut covered_control_heads = active_epoch.covered_control_heads.clone();
        let mut metadata = active_epoch.metadata.clone();
        let mut roster = active_epoch.roster.clone();
        for branch in &losing_branches {
            merge_frontier_head(
                &mut covered_control_heads,
                branch.control_head.clone(),
                |head| head.coord.stream_key(),
                |head| head.coord.seq,
            );
            for head in &branch.metadata_heads {
                merge_frontier_head(
                    &mut metadata.heads,
                    head.clone(),
                    |head| head.coord.stream_key(),
                    |head| head.coord.seq,
                );
            }
            for head in &branch.roster_heads {
                merge_frontier_head(
                    &mut roster.heads,
                    head.clone(),
                    |head| head.coord.stream_key(),
                    |head| head.coord.seq,
                );
            }
        }
        covered_control_heads.sort_by_key(|head| head.coord.stream_key());
        metadata.heads.sort_by_key(|head| head.coord.stream_key());
        roster.heads.sort_by_key(|head| head.coord.stream_key());

        // The name is the deterministic metadata selection across the merged
        // frontier. Each branch's selected metadata is already the canonical
        // maximum over its own covered history, so the maximum across the branch
        // selections is the canonical selection over their union.
        let selected_metadata = std::iter::once(chosen_metadata)
            .chain(
                losing_branches
                    .iter()
                    .map(|branch| &branch.selected_metadata),
            )
            .max_by_key(|entry| {
                (
                    entry.metadata_stamp.clone(),
                    entry.author_pubkey.clone(),
                    entry.device_id.clone(),
                    entry.metadata_hash(),
                )
            })
            .expect("a resolution names at least the chosen branch's metadata")
            .clone();
        metadata.selected = selected_metadata.coord();
        metadata.state_hash = selected_metadata.metadata_hash();

        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id: active_epoch
                        .covered_control_heads
                        .iter()
                        .find(|head| head.coord.stream_key().author_pubkey == author_pubkey)
                        .map_or_else(
                            || {
                                AuthorStreamId::from_digest(generated_id_digest(
                                    ids,
                                    b"coven.circle-transition-draft-stream.v1\0",
                                ))
                            },
                            |head| head.coord.stream_id,
                        ),
                    author_owner_grant: chosen_metadata.author_owner_grant.clone(),
                    seq: chosen_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(chosen_control.coord.control_hash()),
                    dependencies: vec![chosen_control.coord.clone()],
                },
                state: CircleControlState::ActiveEpoch(MergeActiveCircleEpoch {
                    common: active_epoch.common.clone(),
                    metadata,
                    roster,
                    store_membership,
                    covered_control_heads,
                }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        let access = CircleAccessDraft::prepare(
            store_root_hash,
            candidate_family,
            circle_id,
            epoch_id,
            keyring,
            key_fingerprint,
            &roster_state,
            &chosen_roster.members(),
            &control_value
                .value
                .state
                .active_epoch()
                .expect("control resolution constructs an active epoch")
                .store_membership,
            &store_members,
            &std::collections::BTreeMap::new(),
            ids,
            signer,
        )?;
        control_value
            .value
            .state
            .active_epoch_mut()
            .expect("control resolution constructs an active epoch")
            .common
            .access_root = access.access_root();
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
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
            roster: chosen_roster.clone(),
            policy: CircleTransitionDraftPolicy {
                roster: CircleRosterDraftPolicy::Inherited,
                metadata_successor: false,
            },
            metadata: selected_metadata,
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access,
            control,
        })
    }

    /// Build the terminal deletion: a successor of the current control whose
    /// state is `Deleted`, freezing the epoch spine for historical verification
    /// and reclamation. It publishes no roster successor, metadata successor,
    /// access material, or bootstraps — its control inherits the predecessor's
    /// access root.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn delete(
        device_id: &str,
        store_membership: StoreMembershipStateRef,
        membership_authority: MembershipGrantCreationAuthority,
        store_members: Vec<(String, MemberRole)>,
        current_control: &PreparedCircleControl,
        current_roster: &CircleMaterializedRoster,
        current_metadata: &CircleMetadata,
        keyring: &str,
        ids: &dyn crate::id_provider::IdProvider,
        signer: &dyn crate::keys::IdentityKeyAuthority,
    ) -> Result<Self, CircleTransitionError> {
        let context = circle_delete_successor_context(
            store_members,
            current_control,
            current_roster,
            current_metadata,
            keyring,
            signer,
        )?;
        let CircleSuccessorContext {
            store_members: _,
            author_pubkey,
            epoch,
            grant_id,
            author_authority,
            key_fingerprint: _,
        } = context;
        let store_root_hash = current_control.value.store_root_hash;
        let circle_id = current_control.value.circle_id;
        let epoch_id = current_control.value.epoch_id();
        let frozen_epoch = MergeActiveCircleEpoch {
            common: epoch.common.clone(),
            metadata: epoch.metadata.clone(),
            roster: epoch.roster.clone(),
            store_membership,
            covered_control_heads: epoch.covered_control_heads.clone(),
        };
        let stream_id = epoch
            .covered_control_heads
            .iter()
            .find(|head| head.coord.stream_key().author_pubkey == author_pubkey)
            .map_or_else(
                || {
                    AuthorStreamId::from_digest(generated_id_digest(
                        ids,
                        b"coven.circle-transition-draft-stream.v1\0",
                    ))
                },
                |head| head.coord.stream_id,
            );
        let mut control_value = CircleControl {
            version: STORE_PROTOCOL_VERSION,
            store_root_hash,
            circle_id,
            value: CircleControlValue {
                order: MergeCircleControlOrder {
                    device_id: device_id.to_string(),
                    stream_id,
                    author_owner_grant: grant_id,
                    seq: current_control
                        .value
                        .ordinal()
                        .checked_add(1)
                        .ok_or(CircleTransitionError::SequenceOverflow)?,
                    previous_control_hash: Some(current_control.coord.control_hash()),
                    dependencies: vec![current_control.coord.clone()],
                },
                state: CircleControlState::Deleted(DeletedCircle { frozen_epoch }),
                author_authority,
                membership_authority,
            },
            author_pubkey,
            signature: String::new(),
        };
        control_value.signature = keys::sign_hex(signer, &control_value.canonical_bytes()).1;
        let control = PreparedCircleControl {
            coord: control_value.coord(),
            bytes: serde_json::to_vec(&control_value)
                .expect("circle control serialization cannot fail"),
            value: control_value,
        };
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
            close_intent: None,
            close_finalization: None,
            close_cancellation: None,
            access: Vec::new(),
            control,
        })
    }
}
