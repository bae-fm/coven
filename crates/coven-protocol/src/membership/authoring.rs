use super::conflict::conflict_retirement_barriers;
use super::*;

impl MembershipChain {
    pub(crate) fn resolved_with(
        &self,
        store_root_hash: ObjectHash,
        resolutions: &[(
            StoreMembershipConflictResolutionRef,
            StoreMembershipConflictResolution,
        )],
    ) -> Result<ResolvedStoreMembership, MembershipError> {
        match self.status() {
            MembershipStatus::Resolved(resolved) if resolutions.is_empty() => Ok(resolved.clone()),
            MembershipStatus::Conflict(conflict) => {
                resolve_store_membership_conflict(store_root_hash, conflict, resolutions)
            }
            MembershipStatus::Resolved(_) => Err(MembershipError::InvalidConflictResolution),
        }
    }

    pub fn signed_conflict_resolution(
        &self,
        store_root_hash: ObjectHash,
        selection: MembershipConflictSelection,
        replacement_membership: GrantStreamAnchor,
        replacement_acceptance: OwnerConflictResolutionAcceptance,
        signer: &UserKeypair,
    ) -> Result<StoreMembershipConflictResolution, MembershipError> {
        let MembershipStatus::Conflict(conflict) = self.status() else {
            return Err(MembershipError::Conflict);
        };
        let resolver_pubkey = keys::public_key_hex(signer);
        let (conflict_hash, heads, retired_owner_grants, records, effective_frontier) =
            match (conflict, &selection) {
                (
                    MembershipConflict::ConcurrentMemberAssignments {
                        conflict_hash,
                        heads,
                        effective_frontier,
                        conflicting_grants,
                        uncontested_grants,
                        grants,
                        ..
                    },
                    MembershipConflictSelection::MemberAssignment { grant },
                ) => {
                    if !conflicting_grants.contains_key(grant) {
                        return Err(MembershipError::InvalidConflictResolution);
                    }
                    let retired = uncontested_grants
                        .iter()
                        .filter_map(|(grant, record)| {
                            (record.member_pubkey == resolver_pubkey && record.role.is_owner())
                                .then_some(grant.clone())
                        })
                        .collect::<BTreeSet<_>>();
                    if retired.is_empty() {
                        return Err(MembershipError::SignerIsNotOwner(resolver_pubkey));
                    }
                    (
                        conflict_hash,
                        heads,
                        retired,
                        grants
                            .iter()
                            .map(|(grant, state)| (grant.clone(), state.record().clone()))
                            .collect(),
                        effective_frontier.clone(),
                    )
                }
                (
                    MembershipConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        involved_owner_grants,
                        maximal_valid_branches,
                        ..
                    },
                    MembershipConflictSelection::RevocationBranch {
                        heads: selected_heads,
                    },
                ) => {
                    let branch = maximal_valid_branches
                        .iter()
                        .find(|branch| branch.heads == *selected_heads)
                        .ok_or(MembershipError::InvalidConflictResolution)?;
                    let resolver_grants = branch
                        .active_grants()
                        .filter_map(|(grant, record)| {
                            (record.member_pubkey == resolver_pubkey && record.role.is_owner())
                                .then_some(grant.clone())
                        })
                        .collect::<BTreeSet<_>>();
                    if resolver_grants.is_empty() {
                        return Err(MembershipError::SignerIsNotOwner(resolver_pubkey));
                    }
                    let mut retired = involved_owner_grants.clone();
                    retired.extend(resolver_grants);
                    let records = maximal_valid_branches
                        .iter()
                        .flat_map(|branch| branch.grants.iter())
                        .map(|(grant, state)| (grant.clone(), state.record().clone()))
                        .collect();
                    let mut frontier = maximal_valid_branches
                        .iter()
                        .flat_map(|branch| branch.effective_frontier.iter().cloned())
                        .collect::<Vec<_>>();
                    frontier.sort();
                    frontier.dedup();
                    (conflict_hash, heads, retired, records, frontier)
                }
                _ => return Err(MembershipError::InvalidConflictResolution),
            };
        let replacement_grant = derive_store_resolution_grant(conflict_hash, &resolver_pubkey);
        let retirement_barriers = conflict_retirement_barriers(
            records,
            effective_frontier,
            &replacement_acceptance.device_state,
        )?;
        Ok(Signed::sign(
            StoreMembershipConflictResolutionBody {
                store_root_hash,
                conflict_hash: *conflict_hash,
                conflicting_heads: heads.clone(),
                retired_owner_grants,
                retirement_barriers,
                resolver_pubkey,
                selection,
                replacement_grant,
                replacement_membership,
                replacement_acceptance,
            },
            signer,
        ))
    }

    pub fn signed_set_member_with_anchor_and_wrapped_key_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        membership: Option<GrantStreamAnchor>,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let role = StoreMembershipRoleGrant::from_direct_assignment(role)?;
        let grant_id = self.next_member_grant_id_in_stream(signer, stream_id, &user_pubkey)?;
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            stream_id,
            user_pubkey,
            provider_account_email,
            role,
            grant_id,
            membership,
            wrapped_key,
            created_at,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: StoreMembershipRoleGrant,
        grant_id: MembershipGrantId,
        membership: Option<GrantStreamAnchor>,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, previous_hash) = self.next_stream_position(&author, &author_grant, stream_id)?;
        let replaces = self.active_grant_ids(&user_pubkey);
        let retirement_barriers = self.membership_retirement_barriers(&replaces, None)?;
        if role.is_owner() != membership.is_some() {
            return Err(MembershipError::InvalidOwnerMembershipAnchor(
                self.entries.len(),
            ));
        }
        let entry = Signed::sign(
            MembershipEntryBody {
                store_id: self
                    .store_id()
                    .expect("validated chain has a store id")
                    .to_string(),
                author_pubkey: author,
                author_owner_grant: author_grant,
                stream_id,
                seq,
                previous_hash,
                dependencies: self.effective_frontier(),
                resolution_dependencies: self.resolution_refs().to_vec(),
                created_at,
                change: MembershipChange::SetMember {
                    user_pubkey: user_pubkey.clone(),
                    provider_account_email,
                    role,
                    grant_id,
                    membership,
                    replaces,
                    retirement_barriers,
                    retirement_device_state: None,
                    wrapped_key,
                },
                provider_admin: None,
            },
            signer,
        );
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn signed_finalize_owner_promotion_in_stream(
        &self,
        root: &StoreRootRef,
        promoter: &StoreDeviceRegistration,
        candidate: &StoreDeviceRegistration,
        acceptance: OwnerPromotionAcceptance,
        signer: &UserKeypair,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        acceptance
            .request
            .verify(root, promoter)
            .map_err(|_| MembershipError::InvalidOwnerPromotion)?;
        acceptance
            .verify(candidate)
            .map_err(|_| MembershipError::InvalidOwnerPromotion)?;
        let request = &acceptance.request;
        let author = keys::public_key_hex(signer);
        let OwnerPromotionFinalization {
            author_stream,
            seq: requested_seq,
            previous_hash: requested_previous_hash,
        } = request.finalization;
        let (expected_seq, expected_previous_hash) =
            self.next_stream_position(&author, &request.promoter_owner_grant, author_stream)?;
        let Some(member) = self.active_grant(&request.member_grant) else {
            return Err(MembershipError::InvalidOwnerPromotion);
        };
        let membership = &acceptance.anchors.membership;
        let root_id = root.store_root_id.to_string();
        if author != promoter.author_pubkey
            || self.store_id() != Some(root_id.as_str())
            || self.active_owner_grant(&author) != Some(request.promoter_owner_grant.clone())
            || member.member_pubkey != request.member_pubkey
            || member.role != StoreMembershipRoleGrant::Member
            || self.active_grant_ids(&request.member_pubkey)
                != BTreeSet::from([request.member_grant.clone()])
            || expected_seq != requested_seq
            || expected_previous_hash != requested_previous_hash
            || self
                .state
                .grants
                .contains_key(&request.intended_owner_grant)
        {
            return Err(MembershipError::InvalidOwnerPromotion);
        }
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            author_stream,
            request.member_pubkey.clone(),
            member.provider_account_email.clone(),
            StoreMembershipRoleGrant::Owner {
                recovery: OwnerRecoveryAnchorRef::Promotion {
                    acceptance: Box::new(acceptance.clone()),
                },
            },
            request.intended_owner_grant.clone(),
            Some(membership.clone()),
            wrapped_key,
            created_at,
        )
    }

    pub fn signed_remove_member_with_wrapped_keys_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.signed_remove_member_with_barrier_state(
            signer,
            stream_id,
            user_pubkey,
            wrapped_keys,
            None,
            created_at,
        )
    }

    pub fn signed_remove_member_with_owner_barrier_state(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        device_state: StoreDeviceStateRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.signed_remove_member_with_barrier_state(
            signer,
            stream_id,
            user_pubkey,
            wrapped_keys,
            Some(device_state),
            created_at,
        )
    }

    fn signed_remove_member_with_barrier_state(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_keys: Vec<WrappedStoreKeyRef>,
        retirement_device_state: Option<StoreDeviceStateRef>,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let removes = self.active_grant_ids(&user_pubkey);
        if removes.is_empty() {
            return Err(MembershipError::NotAMember(user_pubkey));
        }
        let retains_owner = self.state.grants.iter().any(|(grant, state)| {
            !removes.contains(grant) && state.active().is_some_and(|record| record.role.is_owner())
        });
        if !retains_owner {
            return Err(MembershipError::NoActiveOwner);
        }
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, previous_hash) = self.next_stream_position(&author, &author_grant, stream_id)?;
        let retirement_barriers =
            self.membership_retirement_barriers(&removes, retirement_device_state.as_ref())?;
        let entry = Signed::sign(
            MembershipEntryBody {
                store_id: self
                    .store_id()
                    .expect("validated chain has a store id")
                    .to_string(),
                author_pubkey: author,
                author_owner_grant: author_grant,
                stream_id,
                seq,
                previous_hash,
                dependencies: self.effective_frontier(),
                resolution_dependencies: self.resolution_refs().to_vec(),
                created_at,
                change: MembershipChange::RemoveMember {
                    user_pubkey,
                    removes,
                    retirement_barriers,
                    retirement_device_state,
                    wrapped_keys,
                },
                provider_admin: None,
            },
            signer,
        );
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        Ok(entry)
    }

    pub fn signed_resolution_activation_in_stream(
        &self,
        store_root_hash: ObjectHash,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        reference: StoreMembershipConflictResolutionRef,
        resolution: &StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        self.ensure_resolved()?;
        let MembershipStatus::Resolved(resolved_before) = self.status() else {
            unreachable!("ensure_resolved accepted a conflict")
        };
        let author = keys::public_key_hex(signer);
        if !resolution.verify_signature()
            || resolution.store_root_hash != store_root_hash
            || reference.resolver_pubkey != author
            || !self.resolution_refs().contains(&reference)
            || self.active_owner_grant(&author) != Some(resolution.replacement_grant.clone())
        {
            return Err(MembershipError::InvalidConflictResolution);
        }
        let author_grant = resolution.replacement_grant.clone();
        if self
            .raw_stream_tip(&author, &author_grant, stream_id)
            .is_some()
        {
            return Err(MembershipError::ResolutionActivationRequiresFreshStream);
        }
        let entry = Signed::sign(
            MembershipEntryBody {
                store_id: self
                    .store_id()
                    .expect("validated chain has a store id")
                    .to_string(),
                author_pubkey: author,
                author_owner_grant: author_grant,
                stream_id,
                seq: 1,
                previous_hash: None,
                dependencies: self.effective_frontier(),
                resolution_dependencies: self.resolution_refs().to_vec(),
                created_at,
                change: MembershipChange::ResolutionActivation {
                    resolution: reference,
                },
                provider_admin: None,
            },
            signer,
        );
        let mut candidate = self.clone();
        candidate.add_entry(entry.clone())?;
        let MembershipStatus::Resolved(resolved_after) = candidate.status() else {
            return Err(MembershipError::InvalidConflictResolution);
        };
        if resolved_after.state_hash != resolved_before.state_hash {
            return Err(MembershipError::InvalidConflictResolution);
        }
        Ok(entry)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_set_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        provider_account_email: Option<String>,
        role: MemberRole,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let role = StoreMembershipRoleGrant::from_direct_assignment(role)?;
        let grant_id = self.next_member_grant_id_in_stream(signer, stream_id, &user_pubkey)?;
        let dependencies = self.effective_frontier();
        let wrapped_key = test_wrapped_key_ref(
            &keys::public_key_hex(signer),
            &user_pubkey,
            membership_causal_generation(&self.entries, &dependencies),
            b"Merge membership test wrap",
        );
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            stream_id,
            user_pubkey,
            provider_account_email,
            role,
            grant_id,
            None,
            wrapped_key,
            created_at,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn signed_promote_member_in_stream_for_test(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author_pubkey = keys::public_key_hex(signer);
        let dependencies = self.effective_frontier();
        let wrapped_key = test_wrapped_key_ref(
            &author_pubkey,
            &user_pubkey,
            membership_causal_generation(&self.entries, &dependencies),
            b"Merge Owner-promotion test wrap",
        );
        self.signed_promote_member_in_stream_with_wrapped_key_for_test(
            signer,
            stream_id,
            user_pubkey,
            wrapped_key,
            created_at,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn signed_promote_member_in_stream_with_wrapped_key_for_test(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        wrapped_key: WrappedStoreKeyRef,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let author_pubkey = keys::public_key_hex(signer);
        let promoter_owner_grant = self
            .active_owner_grant(&author_pubkey)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author_pubkey.clone()))?;
        let member_grants = self.active_grant_ids(&user_pubkey);
        let Some(member_grant) = member_grants.iter().next().cloned() else {
            return Err(MembershipError::InvalidOwnerPromotion);
        };
        if member_grants.len() != 1
            || self
                .active_grant(&member_grant)
                .is_none_or(|record| record.role != StoreMembershipRoleGrant::Member)
        {
            return Err(MembershipError::InvalidOwnerPromotion);
        }
        let (seq, previous_hash) =
            self.next_stream_position(&author_pubkey, &promoter_owner_grant, stream_id)?;
        let promotion_id = OwnerPromotionId::from_generated(format!(
            "test promotion {author_pubkey} {user_pubkey} {stream_id:?} {seq}"
        ));
        let store_root_hash = ObjectHash::digest(
            self.store_id()
                .expect("validated membership chain has a Store id")
                .as_bytes(),
        );
        let intended_owner_grant = crate::store_commit::derive_owner_promotion_grant(
            store_root_hash,
            promotion_id,
            &user_pubkey,
        );
        let membership_state_hash = match self.status() {
            MembershipStatus::Resolved(state) => state.state_hash,
            MembershipStatus::Conflict(_) => return Err(MembershipError::InvalidOwnerPromotion),
        };
        let object = |name: &str| {
            let slot = crate::objects::ObjectSlot::logical(format!(
                "test/owner-promotion/{promotion_id:?}/{name}.json"
            ))
            .expect("test Owner-promotion slot is valid");
            ExactObjectRef::new(slot, 1, ObjectHash::digest(name.as_bytes()))
        };
        let registration = |name: &str| StoreDeviceRegistrationRef {
            device_id: ObjectHash::digest(name.as_bytes())
                .to_string()
                .parse()
                .expect("digest is a valid Store device id"),
            registration_hash: ObjectHash::digest(format!("{name} registration").as_bytes()),
            object: object(&format!("{name}-registration")),
        };
        let candidate_stream = AuthorStreamId::from_bytes([0xA5; 32]);
        let activation_commit = crate::store_commit::StoreBatchCommitRef {
            coord: crate::store_commit::StoreCommitCoord {
                stream_id: candidate_stream,
                sequence: 1,
            },
            commit_hash: ObjectHash::digest(b"test Owner-promotion activation commit"),
            object: object("activation-commit"),
        };
        let membership = GrantStreamAnchor::StoreMembership {
            first_slot: crate::objects::ObjectSlot::logical(format!(
                "{}.json",
                crate::store_commit::membership_head_slot_prefix(
                    &user_pubkey,
                    &intended_owner_grant,
                    stream_id,
                    1,
                )
            ))
            .expect("test membership head slot is valid"),
        };
        let request = OwnerPromotionRequest::unsigned_for_test(OwnerPromotionRequestBody {
            promotion_id,
            store_root_hash,
            promoter_registration: registration("promoter"),
            promoter_owner_grant: promoter_owner_grant.clone(),
            member_pubkey: user_pubkey.clone(),
            member_grant,
            member_registration: registration("member"),
            intended_owner_grant: intended_owner_grant.clone(),
            predecessor_membership: crate::circle_control::StoreMembershipStateRef::from_parts(
                Vec::new(),
                Vec::new(),
                Vec::new(),
                membership_state_hash,
            )
            .expect("construct test predecessor membership"),
            predecessor_devices: StoreDeviceStateRef::from_resolved(
                crate::store_commit::CommitFrontier(BTreeMap::new()),
                &crate::store_commit::ResolvedStoreDeviceState {
                    devices: BTreeMap::new(),
                    recovery: Vec::new(),
                    state_hash: ObjectHash::digest(b"test Owner-promotion device state"),
                },
            )
            .expect("construct test predecessor device state"),
            finalization: OwnerPromotionFinalization {
                author_stream: stream_id,
                seq,
                previous_hash,
            },
        });
        let acceptance =
            OwnerPromotionAcceptance::unsigned_for_test(OwnerPromotionAcceptanceBody {
                request: Box::new(request),
                activation: OwnerPromotionRequestActivation {
                    commit: activation_commit,
                    head: crate::store_commit::StoreDeviceHeadRef {
                        head_hash: ObjectHash::digest(b"test Owner-promotion activation head"),
                        object: object("activation-head"),
                    },
                },
                anchors: OwnerPromotionAnchors {
                    membership: membership.clone(),
                    recovery: GrantStreamAnchor::OwnerRecovery {
                        first_slot: crate::objects::ObjectSlot::logical(format!(
                            "test/owner-promotion/{promotion_id:?}/recovery/1.json"
                        ))
                        .expect("test recovery slot is valid"),
                    },
                },
            });
        self.signed_set_role_grant_with_anchor_and_wrapped_key_in_stream(
            signer,
            stream_id,
            user_pubkey,
            None,
            StoreMembershipRoleGrant::Owner {
                recovery: OwnerRecoveryAnchorRef::Promotion {
                    acceptance: Box::new(acceptance),
                },
            },
            intended_owner_grant,
            Some(membership),
            wrapped_key,
            created_at,
        )
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn add_owner_for_test(
        &mut self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<(), MembershipError> {
        let member = self.signed_set_member_in_stream(
            signer,
            stream_id,
            user_pubkey.clone(),
            None,
            MemberRole::Member,
            format!("{created_at}: Member grant"),
        )?;
        self.add_entry(member)?;
        let promotion = self.signed_promote_member_in_stream_for_test(
            signer,
            stream_id,
            user_pubkey,
            created_at,
        )?;
        self.add_entry(promotion)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn signed_remove_member_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: String,
        created_at: String,
    ) -> Result<MembershipEntry, MembershipError> {
        let owner = keys::public_key_hex(signer);
        let dependencies = self.effective_frontier();
        let generation = membership_causal_generation(&self.entries, &dependencies)
            .checked_add(1)
            .ok_or(MembershipError::InvalidWrappedKeys(self.entries.len()))?;
        let wrapped_keys = self
            .current_members()
            .into_iter()
            .filter(|(member, _)| member != &user_pubkey)
            .map(|(member, _)| {
                test_wrapped_key_ref(&owner, &member, generation, b"Merge removal test wrap")
            })
            .collect();
        let removes = self.active_grant_ids(&user_pubkey);
        let mut recovery = removes
            .iter()
            .filter_map(|grant| {
                self.state
                    .grants
                    .get(grant)
                    .and_then(GrantState::active)
                    .filter(|record| record.role.is_owner())
                    .map(|record| OwnerRecoveryCursor {
                        owner_grant: grant.clone(),
                        position: OwnerRecoveryPosition::At {
                            node: OwnerRecoveryNodeRef {
                                owner_pubkey: record.member_pubkey.clone(),
                                owner_grant: grant.clone(),
                                sequence: 1,
                                node_hash: ObjectHash::digest(
                                    format!("test recovery node {grant}").as_bytes(),
                                ),
                                object: ExactObjectRef::new(
                                    crate::objects::ObjectSlot::logical(format!(
                                        "test/recovery/{grant}/1.json"
                                    ))
                                    .expect("test recovery node slot is valid"),
                                    1,
                                    ObjectHash::digest(format!("test recovery {grant}").as_bytes()),
                                ),
                            },
                        },
                    })
            })
            .collect::<Vec<_>>();
        recovery.sort();
        let device_state = (!recovery.is_empty()).then(|| {
            StoreDeviceStateRef::from_resolved(
                crate::store_commit::CommitFrontier(BTreeMap::new()),
                &crate::store_commit::ResolvedStoreDeviceState {
                    devices: BTreeMap::new(),
                    recovery,
                    state_hash: ObjectHash::digest(b"test membership retirement device state"),
                },
            )
            .expect("construct test membership retirement device state")
        });
        self.signed_remove_member_with_barrier_state(
            signer,
            stream_id,
            user_pubkey,
            wrapped_keys,
            device_state,
            created_at,
        )
    }
}
