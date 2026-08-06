use super::entry::store_membership_anchor_stream;
use super::*;

impl MembershipChain {
    pub fn from_entries_with_coords_and_heads_and_provider_admin(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<(MembershipHeadRef, AuthorHead)>,
        provider_admin: crate::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        let expected_store = entries
            .first()
            .map(|(_, entry)| entry.store_id.as_str())
            .ok_or(MembershipError::EmptyChain)?;
        if heads.iter().any(|(reference, head)| {
            reference.head_hash != head.head_hash()
                || head.store_id != expected_store
                || entries
                    .iter()
                    .find(|(coord, _)| *coord == head.entry_coord())
                    .is_none_or(|(_, entry)| head.body.resolutions != entry.resolution_dependencies)
        }) {
            return Err(MembershipError::MissingConflictHeads);
        }
        Self::from_entries_with_coords_and_head_refs(
            entries,
            heads.into_iter().map(|(reference, _)| reference).collect(),
            provider_admin,
        )
    }

    fn from_entries_with_coords_and_head_refs(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        head_refs: Vec<MembershipHeadRef>,
        provider_admin_genesis: crate::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        if entries.is_empty() {
            return Err(MembershipError::EmptyChain);
        }
        let (coords, entries): (Vec<_>, Vec<_>) = entries.into_iter().unzip();
        let mut chain = Self {
            entries,
            coords,
            state: CausalState::default(),
            included: BTreeSet::new(),
            status: None,
            head_refs,
            resolution_checkpoint: None,
            provider_admin_genesis,
        };
        chain.rebuild()?;
        Ok(chain)
    }

    pub fn entries(&self) -> &[MembershipEntry] {
        &self.entries
    }

    pub fn status(&self) -> &MembershipStatus {
        self.status
            .as_ref()
            .expect("a loaded membership chain always has status")
    }

    pub fn head_refs(&self) -> &[MembershipHeadRef] {
        &self.head_refs
    }

    pub fn head_ref_for_stream(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<&MembershipHeadRef> {
        self.head_refs.iter().find(|reference| {
            reference.coord.author_pubkey == author
                && reference.coord.author_owner_grant == *grant
                && reference.coord.stream_id == stream_id
        })
    }

    pub fn membership_anchor(&self, grant: &MembershipGrantId) -> Option<&GrantStreamAnchor> {
        self.entries
            .iter()
            .find_map(|entry| match &entry.change {
                MembershipChange::Founder {
                    owner_grant_id,
                    membership,
                    ..
                } if owner_grant_id == grant => Some(membership),
                MembershipChange::SetMember {
                    grant_id,
                    membership: Some(membership),
                    ..
                } if grant_id == grant => Some(membership),
                _ => None,
            })
            .or_else(|| {
                self.resolution_checkpoint
                    .as_ref()?
                    .grant_anchors
                    .get(grant)
            })
    }

    pub fn membership_stream_id(&self, grant: &MembershipGrantId) -> Option<AuthorStreamId> {
        let record = self.state.grants.get(grant)?.record();
        store_membership_anchor_stream(&record.member_pubkey, grant, self.membership_anchor(grant)?)
    }

    pub fn activated_membership_streams(&self) -> Vec<(MembershipStreamKey, GrantStreamAnchor)> {
        let mut streams = self
            .state
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                let record = state.record();
                let anchor = self.membership_anchor(grant)?.clone();
                let stream_id = self.membership_stream_id(grant)?;
                Some((
                    MembershipStreamKey {
                        author_pubkey: record.member_pubkey.clone(),
                        author_owner_grant: grant.clone(),
                        stream_id,
                    },
                    anchor,
                ))
            })
            .collect::<BTreeMap<_, _>>();
        let mut included = self.included.clone();
        if let MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
            maximal_valid_branches,
            ..
        }) = self.status()
        {
            for branch in maximal_valid_branches {
                included.extend(causal_grants::history_closure(
                    &self.entries,
                    &branch.effective_frontier,
                ));
            }
        }
        for (coord, entry) in self.entries_with_coords() {
            if !included.contains(coord) {
                continue;
            }
            let (owner_pubkey, grant, anchor) = match &entry.change {
                MembershipChange::SetMember {
                    user_pubkey,
                    role: StoreMembershipRoleGrant::Owner { .. },
                    grant_id,
                    membership: Some(membership),
                    ..
                } => (user_pubkey, grant_id, membership),
                _ => continue,
            };
            let stream_id = store_membership_anchor_stream(owner_pubkey, grant, anchor)
                .expect("validated Owner grant has a Store membership stream anchor");
            streams.insert(
                MembershipStreamKey {
                    author_pubkey: owner_pubkey.clone(),
                    author_owner_grant: grant.clone(),
                    stream_id,
                },
                anchor.clone(),
            );
        }
        streams.into_iter().collect()
    }

    pub fn activate_head_ref(
        &mut self,
        reference: MembershipHeadRef,
    ) -> Result<(), MembershipError> {
        if !self.coords.contains(&reference.coord) {
            return Err(MembershipError::MissingConflictHeads);
        }
        let stream = reference.coord.stream_key();
        self.head_refs
            .retain(|current| current.coord.stream_key() != stream);
        self.head_refs.push(reference);
        self.head_refs.sort();
        self.rebuild()
    }

    pub fn resolution_refs(&self) -> &[StoreMembershipConflictResolutionRef] {
        self.resolution_checkpoint
            .as_ref()
            .map_or(&[], |checkpoint| checkpoint.resolutions.as_slice())
    }

    pub fn conflict(&self) -> Option<&MembershipConflict> {
        match self.status() {
            MembershipStatus::Resolved(_) => None,
            MembershipStatus::Conflict(conflict) => Some(conflict),
        }
    }

    pub fn ensure_resolved(&self) -> Result<(), MembershipError> {
        match self.status() {
            MembershipStatus::Resolved(_) => Ok(()),
            MembershipStatus::Conflict(_) => Err(MembershipError::Conflict),
        }
    }

    pub(crate) fn entries_with_coords(
        &self,
    ) -> impl Iterator<Item = (&MembershipCoord, &MembershipEntry)> {
        self.coords.iter().zip(self.entries.iter())
    }

    pub fn store_id(&self) -> Option<&str> {
        self.entries.first().map(|entry| entry.store_id.as_str())
    }

    pub fn founder_coord(&self) -> Option<&MembershipCoord> {
        self.entries_with_coords().find_map(|(coord, entry)| {
            matches!(entry.change, MembershipChange::Founder { .. }).then_some(coord)
        })
    }

    pub(crate) fn founder_entry(&self) -> Option<&MembershipEntry> {
        self.entries
            .iter()
            .find(|entry| matches!(entry.change, MembershipChange::Founder { .. }))
    }

    pub fn founder_pubkey(&self) -> Option<&str> {
        self.founder_entry().and_then(|entry| match &entry.change {
            MembershipChange::Founder { owner_pubkey, .. } => Some(owner_pubkey.as_str()),
            MembershipChange::SetMember { .. }
            | MembershipChange::RemoveMember { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => None,
        })
    }

    pub fn is_founded_by(&self, owner_pubkey: &str) -> bool {
        self.founder_pubkey() == Some(owner_pubkey)
    }

    pub fn add_entry(&mut self, entry: MembershipEntry) -> Result<(), MembershipError> {
        self.add_entry_at(entry.coord(), entry)
    }

    pub fn add_entry_at(
        &mut self,
        coord: MembershipCoord,
        entry: MembershipEntry,
    ) -> Result<(), MembershipError> {
        self.entries.push(entry);
        self.coords.push(coord);
        if let Err(error) = self.rebuild() {
            self.entries.pop();
            self.coords.pop();
            self.rebuild().expect("previous membership chain validated");
            return Err(error);
        }
        Ok(())
    }

    pub fn with_exact_entry(&self, entry: &MembershipEntry) -> Result<Self, MembershipError> {
        let coord = entry.coord();
        if let Some((_, stored)) = self
            .entries_with_coords()
            .find(|(stored_coord, _)| **stored_coord == coord)
        {
            if stored != entry {
                return Err(MembershipError::ExactEntryMismatch {
                    coord: Box::new(coord),
                });
            }
            return Ok(self.clone());
        }
        let mut chain = self.clone();
        chain.add_entry_at(coord, entry.clone())?;
        Ok(chain)
    }

    pub fn contains_coord(&self, expected: &MembershipCoord) -> bool {
        self.coords.iter().any(|coord| coord == expected)
    }

    pub fn effectively_contains_coord(&self, expected: &MembershipCoord) -> bool {
        self.included.contains(expected)
    }

    pub(crate) fn contains_member_history(&self, pubkey: &str) -> bool {
        self.state
            .grants
            .values()
            .any(|state| state.record().member_pubkey == pubkey)
    }

    pub fn reusable_author_streams(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
    ) -> BTreeSet<AuthorStreamId> {
        self.effective_frontier()
            .into_iter()
            .filter(|coord| {
                coord.author_pubkey == author_pubkey
                    && coord.author_owner_grant == *grant
                    && self.raw_stream_tip(author_pubkey, grant, coord.stream_id)
                        == Some(coord.clone())
            })
            .map(|coord| coord.stream_id)
            .collect()
    }

    /// Effective authoring frontier after causal pruning.
    pub fn effective_frontier(&self) -> Vec<MembershipCoord> {
        causal_grants::stream_frontier(
            self.coords
                .iter()
                .filter(|coord| self.included.contains(*coord))
                .cloned(),
        )
    }

    pub fn causally_includes(&self, predecessor: &MembershipChain) -> bool {
        predecessor.included.is_subset(&self.included)
            && predecessor
                .resolution_refs()
                .iter()
                .all(|reference| self.resolution_refs().binary_search(reference).is_ok())
    }

    pub(crate) fn stream_tip(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<MembershipCoord> {
        self.effective_frontier().into_iter().find(|coord| {
            coord.author_pubkey == author_pubkey
                && coord.author_owner_grant == *grant
                && coord.stream_id == stream_id
        })
    }

    pub(crate) fn raw_stream_tip(
        &self,
        author_pubkey: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Option<MembershipCoord> {
        self.coords
            .iter()
            .filter(|coord| {
                coord.author_pubkey == author_pubkey
                    && coord.author_owner_grant == *grant
                    && coord.stream_id == stream_id
            })
            .max_by_key(|coord| coord.seq)
            .cloned()
    }

    pub(crate) fn next_member_grant_id_in_stream(
        &self,
        signer: &UserKeypair,
        stream_id: AuthorStreamId,
        user_pubkey: &str,
    ) -> Result<MembershipGrantId, MembershipError> {
        let author = keys::public_key_hex(signer);
        let author_grant = self
            .active_owner_grant(&author)
            .ok_or_else(|| MembershipError::SignerIsNotOwner(author.clone()))?;
        let (seq, _) = self.next_stream_position(&author, &author_grant, stream_id)?;
        Ok(derive_grant_id(
            self.store_id().expect("validated chain has a store id"),
            &author,
            &author_grant,
            stream_id,
            seq,
            user_pubkey,
        ))
    }

    pub fn next_stream_position(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: AuthorStreamId,
    ) -> Result<(u64, Option<ObjectHash>), MembershipError> {
        let raw_tip = self.raw_stream_tip(author, grant, stream_id);
        let effective_tip = self.stream_tip(author, grant, stream_id);
        if raw_tip != effective_tip {
            return Err(MembershipError::PrunedAuthorStream);
        }
        effective_tip.map_or(Ok((1, None)), |tip| {
            tip.seq
                .checked_add(1)
                .map(|seq| (seq, Some(tip.entry_hash)))
                .ok_or(MembershipError::SequenceExhausted)
        })
    }

    fn rebuild(&mut self) -> Result<(), MembershipError> {
        let expected_store = self
            .entries
            .first()
            .ok_or(MembershipError::EmptyChain)?
            .store_id
            .clone();
        if expected_store.is_empty() {
            return Err(MembershipError::InvalidFounder);
        }

        for (index, (coord, entry)) in self.entries_with_coords().enumerate() {
            if entry.require_version().is_err() {
                return Err(MembershipError::UnsupportedVersion(index));
            }
            if entry.store_id != expected_store {
                return Err(MembershipError::StoreMismatch {
                    index,
                    expected: expected_store.clone(),
                    actual: entry.store_id.clone(),
                });
            }
            if !verify_membership_entry(entry) {
                return Err(MembershipError::InvalidSignature(index));
            }
            let actual = entry.coord();
            if *coord != actual {
                return Err(MembershipError::CoordinateMismatch {
                    index,
                    expected: Box::new(coord.clone()),
                    actual: Box::new(actual),
                });
            }
            if !entry
                .dependencies
                .windows(2)
                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            {
                return Err(MembershipError::NonCanonicalDependencyFrontier { index });
            }
            let (barriers, retirement_device_state) = match &entry.change {
                MembershipChange::SetMember {
                    user_pubkey,
                    role,
                    grant_id,
                    replaces,
                    membership,
                    retirement_barriers,
                    retirement_device_state,
                    ..
                } => {
                    let valid_owner_origin = match role {
                        StoreMembershipRoleGrant::Owner {
                            recovery: OwnerRecoveryAnchorRef::Promotion { acceptance },
                        } => {
                            let request = &acceptance.request;
                            let anchors_match =
                                Some(&acceptance.anchors.membership) == membership.as_ref();
                            let finalization_matches = request.finalization.author_stream
                                == entry.stream_id
                                && request.finalization.seq == entry.seq
                                && request.finalization.previous_hash == entry.previous_hash;
                            request.member_pubkey == *user_pubkey
                                && replaces.len() == 1
                                && replaces.contains(&request.member_grant)
                                && request.intended_owner_grant == *grant_id
                                && request.promoter_owner_grant == entry.author_owner_grant
                                && anchors_match
                                && finalization_matches
                        }
                        StoreMembershipRoleGrant::Owner { .. } => false,
                        StoreMembershipRoleGrant::Member | StoreMembershipRoleGrant::Follower => {
                            membership.is_none()
                        }
                    };
                    if role.is_owner()
                        != membership.as_ref().is_some_and(|anchor| {
                            store_membership_anchor_stream(user_pubkey, grant_id, anchor).is_some()
                        })
                        || !valid_owner_origin
                    {
                        return Err(MembershipError::InvalidOwnerMembershipAnchor(index));
                    }
                    (retirement_barriers, retirement_device_state)
                }
                MembershipChange::RemoveMember {
                    retirement_barriers,
                    retirement_device_state,
                    ..
                } => (retirement_barriers, retirement_device_state),
                MembershipChange::ResolutionActivation { resolution } => {
                    if resolution.resolver_pubkey != entry.author_pubkey
                        || entry.seq != 1
                        || entry.previous_hash.is_some()
                        || entry
                            .dependencies
                            .iter()
                            .any(|dependency| dependency.stream_key() == entry.coord().stream_key())
                        || entry.author_owner_grant
                            != derive_store_resolution_grant(
                                &resolution.conflict_hash,
                                &resolution.resolver_pubkey,
                            )
                        || entry
                            .resolution_dependencies
                            .binary_search(resolution)
                            .is_err()
                        || self
                            .resolution_checkpoint
                            .as_ref()
                            .is_none_or(|checkpoint| {
                                let already_checkpointed =
                                    checkpoint.included.contains(&entry.coord())
                                        || checkpoint.raw_heads.contains(&entry.coord());
                                !already_checkpointed
                                    && (entry.dependencies != checkpoint.effective_frontier
                                        || entry.resolution_dependencies != checkpoint.resolutions)
                            })
                    {
                        return Err(MembershipError::InvalidResolutionActivation(index));
                    }
                    continue;
                }
                MembershipChange::ProviderAdmin => {
                    let Some(crate::provider::ProviderAdminMembershipChange {
                        owner_barriers, ..
                    }) = &entry.provider_admin
                    else {
                        return Err(MembershipError::InvalidProviderAdminChange(index));
                    };
                    if !entry.resolution_dependencies.is_empty()
                        || owner_barriers.values().any(|barrier| {
                            !barrier
                                .observed_streams
                                .windows(2)
                                .all(|pair| pair[0].stream_key() < pair[1].stream_key())
                        })
                    {
                        return Err(MembershipError::InvalidProviderAdminChange(index));
                    }
                    continue;
                }
                MembershipChange::Founder { .. } => continue,
            };
            if entry.provider_admin.is_some() {
                return Err(MembershipError::InvalidProviderAdminChange(index));
            }
            let owner_recoveries = barriers
                .values()
                .filter_map(|barrier| match barrier {
                    MergeMembershipGrantRetirementBarrier::Owner { barrier } => {
                        Some(&barrier.recovery)
                    }
                    MergeMembershipGrantRetirementBarrier::NonOwner { .. } => None,
                })
                .collect::<Vec<_>>();
            match (owner_recoveries.is_empty(), retirement_device_state) {
                (true, None) => {}
                (false, Some(state))
                    if owner_recoveries
                        .iter()
                        .all(|cursor| state.recovery().binary_search(cursor).is_ok()) => {}
                (true, Some(_)) => return Err(MembershipError::UnexpectedOwnerRecoveryState),
                (false, None | Some(_)) => return Err(MembershipError::MissingOwnerRecoveryState),
            }
            if let Some((grant, _)) = barriers.iter().find(|(_, barrier)| {
                !barrier
                    .author_streams()
                    .observed_streams
                    .windows(2)
                    .all(|pair| pair[0].stream_key() < pair[1].stream_key())
            }) {
                return Err(MembershipError::InvalidOwnerRevocationBarrier {
                    index,
                    grant: grant.clone(),
                });
            }
        }

        let founders = self
            .entries
            .iter()
            .filter_map(|entry| {
                let MembershipChange::Founder {
                    owner_pubkey,
                    owner_grant_id,
                    ..
                } = &entry.change
                else {
                    return None;
                };
                Some((entry, owner_pubkey, owner_grant_id))
            })
            .collect::<Vec<_>>();
        let [(founder, owner_pubkey, owner_grant_id)] = founders.as_slice() else {
            return Err(MembershipError::InvalidFounder);
        };
        if founder.author_pubkey != **owner_pubkey
            || founder.author_owner_grant != **owner_grant_id
            || founder.stream_id != derive_founder_stream_id(&founder.store_id, owner_pubkey)
            || founder.provider_admin.is_some()
        {
            return Err(MembershipError::InvalidFounder);
        }

        validate_provider_admin_controls(&self.entries, self.resolution_checkpoint.as_ref())?;
        validate_membership_retirement_barriers(
            &self.entries,
            self.resolution_checkpoint.as_ref(),
        )?;
        validate_membership_wrapped_keys(&self.entries, self.resolution_checkpoint.as_ref())?;

        let reduced = match &self.resolution_checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&self.entries, checkpoint)?,
            None => reduce_store_membership(&self.entries)?,
        };
        let checkpoint_grants = self
            .resolution_checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.grants);
        let provider_admin_seed = self
            .resolution_checkpoint
            .as_ref()
            .map_or(&self.provider_admin_genesis, |checkpoint| {
                &checkpoint.provider_admin
            });
        let (state_source, status) = match reduced {
            CausalGrantStatus::Resolved(reduced) => {
                let provider_admin = crate::provider::ProviderAdminState::reduce_merge(
                    provider_admin_seed,
                    &self.entries,
                    &reduced.included,
                )?;
                let resolved = resolved_store_membership(
                    &reduced,
                    checkpoint_grants,
                    provider_admin,
                    &self.entries,
                )?;
                (Some(reduced), MembershipStatus::Resolved(resolved))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::ConcurrentMemberAssignments {
                raw_heads,
                effective_frontier,
                member_pubkey,
                conflicting_grants,
                uncontested_grants,
                reduced,
            }) => {
                let heads = self.exact_head_refs(&raw_heads)?;
                let provider_admin = crate::provider::ProviderAdminState::reduce_merge(
                    provider_admin_seed,
                    &self.entries,
                    &reduced.included,
                )?;
                let grants = reduced
                    .grants
                    .iter()
                    .map(|(grant, state)| {
                        Ok((
                            grant.clone(),
                            map_store_grant_state(grant, state, checkpoint_grants, &self.entries)?,
                        ))
                    })
                    .collect::<Result<_, MembershipError>>()?;
                let conflict = MembershipConflict::ConcurrentMemberAssignments {
                    conflict_hash: membership_assignment_conflict_hash(
                        &heads,
                        &member_pubkey,
                        &conflicting_grants,
                    ),
                    heads,
                    effective_frontier,
                    member_pubkey,
                    conflicting_grants: map_store_grants(conflicting_grants, checkpoint_grants)?,
                    uncontested_grants: map_store_grants(uncontested_grants, checkpoint_grants)?,
                    grants,
                    provider_admin,
                };
                (Some(reduced), MembershipStatus::Conflict(conflict))
            }
            CausalGrantStatus::Conflict(CausalGrantConflict::RevocationCycle {
                raw_heads,
                cyclic_sources,
                involved_owner_grants,
                maximal_valid_branches,
            }) => {
                let heads = self.exact_head_refs(&raw_heads)?;
                let branches = maximal_valid_branches
                    .into_iter()
                    .map(|branch| -> Result<StoreMembershipBranch, MembershipError> {
                        let resolved = resolved_store_membership(
                            &branch.reduced,
                            checkpoint_grants,
                            crate::provider::ProviderAdminState::reduce_merge(
                                provider_admin_seed,
                                &self.entries,
                                &branch.reduced.included,
                            )?,
                            &self.entries,
                        )?;
                        Ok(StoreMembershipBranch {
                            heads: self.branch_head_refs(&branch.raw_heads)?,
                            effective_frontier: branch.effective_frontier,
                            grants: resolved.grants,
                            provider_admin: resolved.provider_admin,
                            state_hash: resolved.state_hash,
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let conflict_hash = membership_revocation_conflict_hash(
                    &heads,
                    &cyclic_sources,
                    &involved_owner_grants,
                );
                (
                    None,
                    MembershipStatus::Conflict(MembershipConflict::RevocationCycle {
                        conflict_hash,
                        heads,
                        cyclic_sources,
                        involved_owner_grants,
                        maximal_valid_branches: branches,
                    }),
                )
            }
        };
        if let Some(reduced) = state_source {
            self.state = CausalState {
                grants: reduced
                    .grants
                    .iter()
                    .map(|(grant, state)| {
                        Ok((
                            grant.clone(),
                            map_store_grant_state(grant, state, checkpoint_grants, &self.entries)?,
                        ))
                    })
                    .collect::<Result<_, MembershipError>>()?,
            };
            self.included = reduced.included;
        } else {
            self.state = CausalState::default();
            self.included.clear();
        }
        self.status = Some(status);
        Ok(())
    }

    pub fn apply_resolutions(
        &mut self,
        store_root_hash: ObjectHash,
        resolutions: &[(
            StoreMembershipConflictResolutionRef,
            StoreMembershipConflictResolution,
        )],
    ) -> Result<(), MembershipError> {
        let (raw_heads, effective_frontier) = match self.conflict() {
            Some(MembershipConflict::ConcurrentMemberAssignments {
                heads,
                effective_frontier,
                ..
            }) => (
                heads
                    .iter()
                    .map(|reference| reference.coord.clone())
                    .collect(),
                effective_frontier.clone(),
            ),
            Some(MembershipConflict::RevocationCycle {
                heads,
                maximal_valid_branches,
                ..
            }) => (
                heads
                    .iter()
                    .map(|reference| reference.coord.clone())
                    .collect(),
                causal_grants::selected_branch_frontier(resolutions, |(_, resolution)| {
                    let MembershipConflictSelection::RevocationBranch {
                        heads: selected_heads,
                    } = &resolution.selection
                    else {
                        return Err(MembershipError::InvalidConflictResolution);
                    };
                    maximal_valid_branches
                        .iter()
                        .find(|branch| branch.heads == *selected_heads)
                        .map(|branch| branch.effective_frontier.as_slice())
                        .ok_or(MembershipError::InvalidConflictResolution)
                })?,
            ),
            _ => return Err(MembershipError::InvalidConflictResolution),
        };
        let resolved = self.resolved_with(store_root_hash, resolutions)?;
        let grants = resolved.grants.clone();
        let mut grant_anchors = self
            .resolution_checkpoint
            .as_ref()
            .map_or_else(BTreeMap::new, |checkpoint| checkpoint.grant_anchors.clone());
        for entry in &self.entries {
            match &entry.change {
                MembershipChange::Founder {
                    owner_grant_id,
                    membership,
                    ..
                } => {
                    grant_anchors.insert(owner_grant_id.clone(), membership.clone());
                }
                MembershipChange::SetMember {
                    grant_id,
                    membership: Some(membership),
                    ..
                } => {
                    grant_anchors.insert(grant_id.clone(), membership.clone());
                }
                _ => {}
            }
        }
        for (_, resolution) in resolutions {
            grant_anchors.insert(
                resolution.replacement_grant.clone(),
                resolution.replacement_membership.clone(),
            );
        }
        let included = causal_grants::history_closure(&self.entries, &effective_frontier);
        self.resolution_checkpoint = Some(MembershipResolutionCheckpoint {
            raw_heads,
            effective_frontier: effective_frontier.clone(),
            grants: grants.clone(),
            grant_anchors,
            included: included.clone(),
            resolutions: causal_grants::checkpoint_resolution_refs(
                self.resolution_checkpoint
                    .as_ref()
                    .map(|checkpoint| checkpoint.resolutions.as_slice()),
                resolutions.iter().map(|(reference, _)| reference.clone()),
            ),
            provider_admin: resolved.provider_admin.combined_state().clone(),
        });
        self.state = CausalState { grants };
        self.included = included;
        self.status = Some(MembershipStatus::Resolved(resolved));
        Ok(())
    }

    fn exact_head_refs(
        &self,
        raw_heads: &[MembershipCoord],
    ) -> Result<Vec<MembershipHeadRef>, MembershipError> {
        crate::causal_grants::exact_head_refs(&self.head_refs, raw_heads, |reference| {
            &reference.coord
        })
        .ok_or(MembershipError::MissingConflictHeads)
    }

    fn branch_head_refs(
        &self,
        branch_heads: &[MembershipCoord],
    ) -> Result<Vec<MembershipHeadRef>, MembershipError> {
        let by_coord = self
            .head_refs
            .iter()
            .map(|reference| (reference.coord.clone(), reference.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut references = branch_heads
            .iter()
            .map(|coord| {
                by_coord
                    .get(coord)
                    .cloned()
                    .ok_or(MembershipError::MissingConflictHeads)
            })
            .collect::<Result<Vec<_>, _>>()?;
        references.sort();
        Ok(references)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub fn from_entries(entries: Vec<MembershipEntry>) -> Result<Self, MembershipError> {
        let provider_admin = test_provider_admin_genesis(&entries)?;
        Self::from_entries_with_coords_and_provider_admin(
            entries
                .into_iter()
                .map(|entry| (entry.coord(), entry))
                .collect(),
            provider_admin,
        )
    }

    #[cfg(test)]
    pub(crate) fn from_entries_with_coords_and_heads(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        heads: Vec<(MembershipHeadRef, AuthorHead)>,
    ) -> Result<Self, MembershipError> {
        let values = entries
            .iter()
            .map(|(_, entry)| entry.clone())
            .collect::<Vec<_>>();
        let provider_admin = test_provider_admin_genesis(&values)?;
        Self::from_entries_with_coords_and_heads_and_provider_admin(entries, heads, provider_admin)
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(crate) fn from_entries_with_coords_and_provider_admin(
        entries: Vec<(MembershipCoord, MembershipEntry)>,
        provider_admin: crate::provider::ProviderAdminState,
    ) -> Result<Self, MembershipError> {
        Self::from_entries_with_coords_and_head_refs(entries, Vec::new(), provider_admin)
    }

    /// Raw signed coverage: the greatest loaded coordinate in every stream,
    /// including suffixes removed by causal pruning.
    #[cfg(test)]
    pub fn author_heads(&self) -> Vec<MembershipCoord> {
        causal_grants::stream_frontier(self.coords.iter().cloned())
    }
}
