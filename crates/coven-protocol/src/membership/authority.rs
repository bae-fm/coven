use super::*;

impl MembershipChain {
    /// Membership changes consume the resolved grant and key authority they
    /// were prepared against. Unrelated device controls do not change that
    /// authority, but an accepted grant change requires a new candidate.
    pub fn validate_publication_predecessor(
        &self,
        entry: &MembershipEntry,
    ) -> Result<(), MembershipError> {
        match &entry.change {
            StoreAuthorityChange::SetMember { .. }
            | StoreAuthorityChange::RemoveMember { .. }
            | StoreAuthorityChange::ProviderAdmin => {}
            StoreAuthorityChange::Founder { .. } => return Err(MembershipError::InvalidFounder),
            StoreAuthorityChange::DeviceRegistrationActivation { .. }
            | StoreAuthorityChange::DeviceExclusionProposal { .. }
            | StoreAuthorityChange::DeviceExclusionOutcome { .. }
            | StoreAuthorityChange::ResolutionActivation { .. } => return Ok(()),
        }
        let MembershipStatus::Resolved(current) = self.status() else {
            return Err(MembershipError::Conflict);
        };
        if entry.resolution_dependencies != self.resolution_refs() {
            return Err(MembershipError::PublicationPredecessorChanged {
                coord: Box::new(entry.coord()),
            });
        }
        let included = causal_grants::history_closure(&self.entries, &entry.dependencies);
        let causal_past = self
            .entries
            .iter()
            .filter(|prior| included.contains(&prior.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = match &self.resolution_checkpoint {
            Some(checkpoint) => reduce_store_membership_from_checkpoint(&causal_past, checkpoint)?,
            None => reduce_store_membership(&causal_past)?,
        };
        let CausalGrantStatus::Resolved(reduced) = reduced else {
            return Err(MembershipError::Conflict);
        };
        let checkpoint_grants = self
            .resolution_checkpoint
            .as_ref()
            .map(|checkpoint| &checkpoint.grants);
        let provider_seed = self
            .resolution_checkpoint
            .as_ref()
            .map_or(&self.provider_admin_genesis, |checkpoint| {
                &checkpoint.provider_admin
            });
        let provider = crate::provider::ProviderAdminState::reduce_merge(
            provider_seed,
            &causal_past,
            &reduced.included,
        )?;
        let prepared =
            resolved_store_membership(&reduced, checkpoint_grants, provider, &causal_past)?;
        if prepared.state_hash != current.state_hash {
            return Err(MembershipError::PublicationPredecessorChanged {
                coord: Box::new(entry.coord()),
            });
        }
        Ok(())
    }

    pub fn can_write_now(&self, pubkey: &str) -> bool {
        if self.conflict().is_some() {
            return false;
        }
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.can_write())
    }

    /// Whether `pubkey` holds any active grant — a member of this Store now,
    /// whatever role it has.
    ///
    /// Distinct from any question about that member's devices. Removing a
    /// member ends its grants here and rotates the key; it does not mark the
    /// devices it registered inactive, because device status tracks a device's
    /// own lifecycle — a lost laptop belonging to a member in good standing.
    /// Anything asking "could this principal still be owed history" has to ask
    /// membership, not device status.
    pub fn is_member_now(&self, pubkey: &str) -> bool {
        if self.conflict().is_some() {
            return false;
        }
        !self.active_grants_for(pubkey).is_empty()
    }

    pub fn is_owner_now(&self, pubkey: &str) -> bool {
        if self.conflict().is_some() {
            return false;
        }
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.is_owner())
    }

    pub fn authorizes_write_authority(
        &self,
        authority: &MembershipGrantCreationAuthority,
        pubkey: &str,
    ) -> bool {
        let MembershipStatus::Resolved(resolved) = self.status() else {
            return false;
        };
        resolved.active_grants().any(|(_, record)| {
            record.member_pubkey == pubkey
                && record.role.can_write()
                && &record.creation_authority == authority
        })
    }

    /// The permanent retirement of this exact grant, if membership is resolved.
    pub fn write_authority_retirement(
        &self,
        authority: &MembershipGrantCreationAuthority,
        pubkey: &str,
    ) -> Option<&MembershipGrantRetirement> {
        let MembershipStatus::Resolved(resolved) = self.status() else {
            return None;
        };
        let grant = resolved.grants.values().find(|grant| {
            grant.record().creation_authority == *authority
                && grant.record().member_pubkey == pubkey
        })?;
        Some(
            grant
                .retirements()?
                .iter()
                .next()
                .expect("grant retirements are nonempty"),
        )
    }

    pub fn active_grant(&self, grant_id: &MembershipGrantId) -> Option<&MembershipGrantRecord> {
        let MembershipStatus::Resolved(resolved) = self.status() else {
            return None;
        };
        resolved.active_grant(grant_id)
    }

    pub fn current_members(&self) -> Vec<(String, MemberRole)> {
        let mut members = BTreeMap::new();
        for state in self.state.grants.values() {
            let Some(record) = state.active() else {
                continue;
            };
            members.insert(record.member_pubkey.clone(), record.role.role());
        }
        members.into_iter().collect()
    }

    pub fn active_wrapped_keys_for(&self, recipient_pubkey: &str) -> Vec<WrappedStoreKeyRef> {
        let active_grants = self.active_grant_ids(recipient_pubkey);
        self.entries_with_coords()
            .filter(|(coord, _)| self.included.contains(*coord))
            .flat_map(|(_, entry)| match &entry.change {
                StoreAuthorityChange::SetMember {
                    grant_id,
                    wrapped_key,
                    ..
                } if active_grants.contains(grant_id) => std::slice::from_ref(wrapped_key),
                StoreAuthorityChange::RemoveMember { wrapped_keys, .. } => wrapped_keys.as_slice(),
                StoreAuthorityChange::Founder { .. }
                | StoreAuthorityChange::SetMember { .. }
                | StoreAuthorityChange::DeviceRegistrationActivation { .. }
                | StoreAuthorityChange::DeviceExclusionProposal { .. }
                | StoreAuthorityChange::DeviceExclusionOutcome { .. }
                | StoreAuthorityChange::ProviderAdmin
                | StoreAuthorityChange::ResolutionActivation { .. } => &[],
            })
            .filter(|reference| reference.recipient_pubkey == recipient_pubkey)
            .cloned()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn wrapped_key_authority_for(
        &self,
        recipient_pubkey: &str,
    ) -> Result<Vec<WrappedStoreKeyRef>, MembershipError> {
        let active_grants = self.active_grants_for(recipient_pubkey);
        for (index, (rotation_coord, entry)) in self
            .entries_with_coords()
            .enumerate()
            .filter(|(_, (coord, _))| self.included.contains(*coord))
        {
            let StoreAuthorityChange::RemoveMember { wrapped_keys, .. } = &entry.change else {
                continue;
            };
            if wrapped_keys
                .iter()
                .any(|reference| reference.recipient_pubkey == recipient_pubkey)
            {
                continue;
            }
            let rotation_generation = wrapped_keys
                .first()
                .ok_or(MembershipError::InvalidWrappedKeys(index))?
                .generation;
            let covered_by_later_grant = !active_grants.is_empty()
                && active_grants.iter().all(|(active_grant, _)| {
                    let Some((_, creation)) = self.entries_with_coords().find(|(_, entry)| {
                        matches!(
                            &entry.change,
                            StoreAuthorityChange::SetMember { grant_id, .. }
                                if grant_id == *active_grant
                        )
                    }) else {
                        return false;
                    };
                    let StoreAuthorityChange::SetMember { wrapped_key, .. } = &creation.change
                    else {
                        return false;
                    };
                    wrapped_key.generation >= rotation_generation
                        && causal_grants::history_closure(&self.entries, &creation.dependencies)
                            .contains(rotation_coord)
                });
            if !covered_by_later_grant {
                return Err(MembershipError::MissingWrappedKeyCoverage {
                    recipient_pubkey: recipient_pubkey.to_string(),
                    rotation: Box::new(rotation_coord.clone()),
                });
            }
        }
        Ok(self.active_wrapped_keys_for(recipient_pubkey))
    }

    pub fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        self.active_grants_for(pubkey)
            .into_iter()
            .next()
            .and_then(|(_, record)| record.provider_account_email.as_deref())
    }

    pub fn write_grant_authority(&self, pubkey: &str) -> Option<MembershipGrantCreationAuthority> {
        self.active_grants_for(pubkey)
            .into_iter()
            .find(|(_, record)| record.role.can_write())
            .map(|(_, record)| record.creation_authority.clone())
    }

    pub fn active_grant_ids(&self, pubkey: &str) -> BTreeSet<MembershipGrantId> {
        self.active_grants_for(pubkey)
            .into_iter()
            .map(|(grant, _)| grant.clone())
            .collect()
    }

    pub fn active_owner_grant(&self, pubkey: &str) -> Option<MembershipGrantId> {
        self.active_grants_for(pubkey)
            .into_iter()
            .find(|(_, record)| record.role.is_owner())
            .map(|(grant, _)| grant.clone())
    }

    pub(super) fn membership_retirement_barriers(
        &self,
        grants: &BTreeSet<MembershipGrantId>,
        device_state: Option<&StoreDeviceStateRef>,
    ) -> Result<BTreeMap<MembershipGrantId, MergeMembershipGrantRetirementBarrier>, MembershipError>
    {
        let retires_owner = grants.iter().any(|grant| {
            self.state
                .grants
                .get(grant)
                .and_then(GrantState::active)
                .is_some_and(|record| record.role.is_owner())
        });
        if retires_owner && device_state.is_none() {
            return Err(MembershipError::MissingOwnerRecoveryState);
        }
        if !retires_owner && device_state.is_some() {
            return Err(MembershipError::UnexpectedOwnerRecoveryState);
        }
        let recovery = match device_state {
            Some(state) => state.recovery(),
            None => &[],
        };
        grants
            .iter()
            .map(|grant| {
                let record = self
                    .state
                    .grants
                    .get(grant)
                    .and_then(GrantState::active)
                    .ok_or_else(|| MembershipError::NotAMember(grant.to_string()))?;
                let author_streams = StoreGrantStreamBarrier {
                    observed_streams: self
                        .effective_frontier()
                        .into_iter()
                        .filter(|coord| coord.author_owner_grant == *grant)
                        .collect(),
                };
                let barrier = if record.role.is_owner() {
                    let cursor = recovery
                        .iter()
                        .find(|cursor| cursor.owner_grant == *grant)
                        .cloned()
                        .ok_or(MembershipError::MissingOwnerRecoveryState)?;
                    MergeMembershipGrantRetirementBarrier::Owner {
                        barrier: MergeStoreOwnerGrantBarrier {
                            author_streams,
                            recovery: cursor,
                        },
                    }
                } else {
                    MergeMembershipGrantRetirementBarrier::NonOwner { author_streams }
                };
                Ok((grant.clone(), barrier))
            })
            .collect()
    }

    pub(super) fn active_grants_for(
        &self,
        pubkey: &str,
    ) -> Vec<(&MembershipGrantId, &MembershipGrantRecord)> {
        self.state
            .grants
            .iter()
            .filter_map(|(grant, state)| {
                state
                    .active()
                    .filter(|record| record.member_pubkey == pubkey)
                    .map(|record| (grant, record))
            })
            .collect()
    }
}
