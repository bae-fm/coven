use super::*;

/// One sealed keyring a member may open: the entry that carries it and the
/// keyring generation that entry establishes.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ActivatedSealedKey {
    pub coord: MembershipCoord,
    pub generation: u64,
    pub key: SealedStoreKey,
}

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
            | StoreAuthorityChange::DeviceExclusionOutcome { .. } => return Ok(()),
        }
        let current = self.resolved();
        let included = causal_grants::history_closure(&self.entries, &entry.dependencies);
        let causal_past = self
            .entries
            .iter()
            .filter(|prior| included.contains(&prior.coord()))
            .cloned()
            .collect::<Vec<_>>();
        let reduced = reduce_store_membership(&causal_past)?;
        let provider = crate::provider::ProviderAdminState::reduce_merge(
            &self.provider_admin_genesis,
            &causal_past,
            &reduced.included,
        )?;
        let prepared = resolved_store_membership(&reduced, provider, &causal_past)?;
        if prepared.state_hash != current.state_hash {
            return Err(MembershipError::PublicationPredecessorChanged {
                coord: Box::new(entry.coord()),
            });
        }
        Ok(())
    }

    pub fn can_write_now(&self, pubkey: &str) -> bool {
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
        !self.active_grants_for(pubkey).is_empty()
    }

    pub fn is_owner_now(&self, pubkey: &str) -> bool {
        self.active_grants_for(pubkey)
            .iter()
            .any(|(_, record)| record.role.is_owner())
    }

    pub fn authorizes_write_authority(&self, authority: &MembershipCoord, pubkey: &str) -> bool {
        let resolved = self.resolved();
        resolved.active_grants().any(|(_, record)| {
            record.member_pubkey == pubkey
                && record.role.can_write()
                && &record.creation_authority == authority
        })
    }

    /// The permanent retirement of this exact grant.
    pub fn write_authority_retirement(
        &self,
        authority: &MembershipCoord,
        pubkey: &str,
    ) -> Option<&MembershipGrantRetirement> {
        let resolved = self.resolved();
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
        let resolved = self.resolved();
        resolved.active_grant(grant_id)
    }

    pub fn current_members(&self) -> Vec<(String, MemberRole)> {
        let mut members = BTreeMap::new();
        for state in self.resolved().grants.values() {
            let Some(record) = state.active() else {
                continue;
            };
            members.insert(record.member_pubkey.clone(), record.role.role());
        }
        members.into_iter().collect()
    }

    pub fn active_sealed_keys_for(&self, recipient_pubkey: &str) -> Vec<ActivatedSealedKey> {
        let active_grants = self.active_grant_ids(recipient_pubkey);
        self.entries_with_coords()
            .filter(|(coord, _)| self.included.contains(*coord))
            .filter_map(|(coord, entry)| match &entry.change {
                StoreAuthorityChange::SetMember {
                    user_pubkey,
                    grant_id,
                    sealed_key,
                    ..
                } if user_pubkey == recipient_pubkey && active_grants.contains(grant_id) => {
                    Some(ActivatedSealedKey {
                        coord: coord.clone(),
                        generation: membership_causal_generation(
                            &self.entries,
                            &entry.dependencies,
                        ),
                        key: sealed_key.clone(),
                    })
                }
                StoreAuthorityChange::RemoveMember {
                    rotation_generation,
                    sealed_keys,
                    ..
                } => sealed_keys
                    .get(recipient_pubkey)
                    .map(|key| ActivatedSealedKey {
                        coord: coord.clone(),
                        generation: *rotation_generation,
                        key: key.clone(),
                    }),
                StoreAuthorityChange::Founder { .. }
                | StoreAuthorityChange::SetMember { .. }
                | StoreAuthorityChange::DeviceRegistrationActivation { .. }
                | StoreAuthorityChange::DeviceExclusionProposal { .. }
                | StoreAuthorityChange::DeviceExclusionOutcome { .. }
                | StoreAuthorityChange::ProviderAdmin => None,
            })
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub fn sealed_key_authority_for(
        &self,
        recipient_pubkey: &str,
    ) -> Result<Vec<ActivatedSealedKey>, MembershipError> {
        let active_grants = self.active_grants_for(recipient_pubkey);
        for (rotation_coord, entry) in self
            .entries_with_coords()
            .filter(|(coord, _)| self.included.contains(*coord))
        {
            let StoreAuthorityChange::RemoveMember {
                rotation_generation,
                sealed_keys,
                ..
            } = &entry.change
            else {
                continue;
            };
            if sealed_keys.contains_key(recipient_pubkey) {
                continue;
            }
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
                    membership_causal_generation(&self.entries, &creation.dependencies)
                        >= *rotation_generation
                        && causal_grants::history_closure(&self.entries, &creation.dependencies)
                            .contains(rotation_coord)
                });
            if !covered_by_later_grant {
                return Err(MembershipError::MissingSealedKeyCoverage {
                    recipient_pubkey: recipient_pubkey.to_string(),
                    rotation: Box::new(rotation_coord.clone()),
                });
            }
        }
        Ok(self.active_sealed_keys_for(recipient_pubkey))
    }

    /// The keyring generation this entry establishes: a grant carries the
    /// keyring at its causal generation; a removal rotates to its own.
    pub fn keyring_generation_of(&self, entry: &MembershipEntry) -> Option<u64> {
        match &entry.change {
            StoreAuthorityChange::SetMember { .. } => Some(membership_causal_generation(
                &self.entries,
                &entry.dependencies,
            )),
            StoreAuthorityChange::RemoveMember {
                rotation_generation,
                ..
            } => Some(*rotation_generation),
            StoreAuthorityChange::Founder { .. }
            | StoreAuthorityChange::DeviceRegistrationActivation { .. }
            | StoreAuthorityChange::DeviceExclusionProposal { .. }
            | StoreAuthorityChange::DeviceExclusionOutcome { .. }
            | StoreAuthorityChange::ProviderAdmin => None,
        }
    }

    pub fn current_member_provider_email(&self, pubkey: &str) -> Option<&str> {
        self.active_grants_for(pubkey)
            .into_iter()
            .next()
            .and_then(|(_, record)| record.provider_account_email.as_deref())
    }

    pub fn write_grant_authority(&self, pubkey: &str) -> Option<MembershipCoord> {
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
            self.resolved()
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
                    .resolved()
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
        self.resolved()
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
