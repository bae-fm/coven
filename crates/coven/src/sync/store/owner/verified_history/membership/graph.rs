use super::*;

#[derive(Clone)]
pub(super) struct LoadedExactMembershipHead {
    pub(super) reference: MembershipHeadRef,
    pub(super) head: AuthorHead,
    pub(super) entry: MembershipEntry,
}

#[derive(Clone)]
pub(super) struct LoadedExactMembershipGraph {
    pub(super) entries: BTreeMap<MembershipCoord, MembershipEntry>,
    pub(super) heads: Vec<(MembershipHeadRef, AuthorHead)>,
    pub(super) path_heads: BTreeMap<MembershipCoord, LoadedExactMembershipHead>,
}

impl LoadedExactMembershipGraph {
    pub(super) fn head_refs(&self) -> Vec<MembershipHeadRef> {
        self.heads
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect()
    }

    pub(super) fn resolution_cut(&self) -> Vec<StoreMembershipConflictResolutionRef> {
        self.heads
            .iter()
            .flat_map(|(_, head)| head.body.resolutions.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }

    pub(super) fn validate_stream_anchors(
        &self,
        root: &StoreRootRef,
        chain: &MembershipChain,
    ) -> Result<(), AnchoredChainError> {
        for node in self.path_heads.values() {
            let grant = &node.reference.coord.author_owner_grant;
            let anchor = chain.membership_anchor(grant).ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "membership head author has no exact membership stream anchor".to_string(),
                )
            })?;
            let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
                return Err(AnchoredChainError::LoadFailed(
                    "membership head author uses a recovery stream anchor".to_string(),
                ));
            };
            if node.reference.coord.seq == 1 && node.reference.object.slot() != first_slot {
                return Err(AnchoredChainError::LoadFailed(
                    "membership stream does not begin at its grant-authorized slot".to_string(),
                ));
            }
            let expected = crate::protocol::store_commit::StreamActivation::grant_authorized(
                root.store_root_hash,
                node.head.body.author_registration.clone(),
                grant.clone(),
                anchor.clone(),
            )
            .activation_id();
            if node.head.body.successor.activation != expected {
                return Err(AnchoredChainError::LoadFailed(
                    "membership head carries another grant stream activation".to_string(),
                ));
            }
        }
        Ok(())
    }

    pub(super) fn add_exact_suffix(
        &self,
        chain: &mut MembershipChain,
    ) -> Result<(), AnchoredChainError> {
        let mut pending = self
            .entries
            .iter()
            .filter(|(coord, _)| !chain.contains_coord(coord))
            .map(|(coord, entry)| (coord.clone(), entry.clone()))
            .collect::<BTreeMap<_, _>>();
        while !pending.is_empty() {
            let next = pending.iter().find_map(|(coord, entry)| {
                let dependencies_loaded = entry
                    .dependencies
                    .iter()
                    .all(|dependency| chain.contains_coord(dependency));
                let predecessor_loaded = entry.previous_hash.is_none()
                    || self.entries.keys().any(|candidate| {
                        candidate.author_pubkey == coord.author_pubkey
                            && candidate.author_owner_grant == coord.author_owner_grant
                            && candidate.stream_id == coord.stream_id
                            && candidate.seq.checked_add(1) == Some(coord.seq)
                            && Some(candidate.entry_hash) == entry.previous_hash
                            && chain.contains_coord(candidate)
                    });
                (dependencies_loaded && predecessor_loaded).then(|| coord.clone())
            });
            let Some(coord) = next else {
                return Err(AnchoredChainError::LoadFailed(
                    "membership resolution suffix has an unresolved causal predecessor".to_string(),
                ));
            };
            let entry = pending
                .remove(&coord)
                .expect("selected membership resolution suffix entry remains pending");
            chain
                .add_entry_at(coord, entry)
                .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        }
        for (reference, _) in &self.heads {
            chain
                .activate_head_ref(reference.clone())
                .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        }
        Ok(())
    }
}

pub(super) fn validate_exact_membership_head_paths(
    graph: &LoadedExactMembershipGraph,
) -> Result<(), AnchoredChainError> {
    for requested in &graph.heads {
        let mut current = Some(&requested.0);
        let mut visited = BTreeSet::new();
        while let Some(reference) = current {
            if !visited.insert(reference.clone()) {
                return Err(AnchoredChainError::LoadFailed(
                    "membership head predecessor chain contains a cycle".to_string(),
                ));
            }
            let node = graph.path_heads.get(&reference.coord).ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "membership head predecessor is absent from its exact path".to_string(),
                )
            })?;
            if node.reference != *reference {
                return Err(AnchoredChainError::LoadFailed(
                    "membership coordinate selects different exact heads".to_string(),
                ));
            }
            match &node.head.body.predecessor {
                Some(predecessor) => {
                    if predecessor.coord.stream_key() != reference.coord.stream_key()
                        || predecessor.coord.seq.checked_add(1) != Some(reference.coord.seq)
                        || predecessor.coord.entry_hash
                            != node.entry.previous_hash.ok_or_else(|| {
                                AnchoredChainError::LoadFailed(
                                    "membership head successor entry omits its predecessor hash"
                                        .to_string(),
                                )
                            })?
                    {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership head does not extend its exact author stream".to_string(),
                        ));
                    }
                    let predecessor_node =
                        graph.path_heads.get(&predecessor.coord).ok_or_else(|| {
                            AnchoredChainError::LoadFailed(
                                "membership head predecessor is absent from its exact path"
                                    .to_string(),
                            )
                        })?;
                    if predecessor_node.reference != *predecessor
                        || predecessor_node.head.body.successor.next_slot
                            != *reference.object.slot()
                    {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership head does not occupy its predecessor-reserved slot"
                                .to_string(),
                        ));
                    }
                }
                None => {
                    if reference.coord.seq != 1 || node.entry.previous_hash.is_some() {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership stream begins without its exact first entry".to_string(),
                        ));
                    }
                }
            }
            current = node.head.body.predecessor.as_ref();
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum MembershipProjectionStatus {
    Included,
    OutsidePrefix,
}

fn membership_resolution_activations(
    graph: &LoadedExactMembershipGraph,
) -> Result<BTreeMap<StoreMembershipConflictResolutionRef, MembershipCoord>, AnchoredChainError> {
    let mut activations = BTreeMap::new();
    for node in graph.path_heads.values() {
        if let MembershipChange::ResolutionActivation { resolution } = &node.entry.change {
            if activations
                .insert(resolution.clone(), node.reference.coord.clone())
                .is_some()
            {
                return Err(AnchoredChainError::LoadFailed(
                    "membership resolution has multiple candidate activation heads".to_string(),
                ));
            }
        }
    }
    Ok(activations)
}

fn membership_projection_activation_status(
    graph: &LoadedExactMembershipGraph,
    prefix: &crate::sync::store::owner::verified_history::VerifiedMergeMembershipPrefix,
    coord: &MembershipCoord,
) -> Result<MembershipProjectionStatus, AnchoredChainError> {
    let node = graph.path_heads.get(coord).ok_or_else(|| {
        AnchoredChainError::LoadFailed(
            "membership projection dependency is absent from its candidate graph".to_string(),
        )
    })?;
    match (
        membership_entry_requires_store_activation(&node.entry),
        &node.head.activation,
    ) {
        (false, crate::protocol::membership::MembershipHeadActivation::Direct) => {
            Ok(MembershipProjectionStatus::Included)
        }
        (true, crate::protocol::membership::MembershipHeadActivation::StoreCommit { commit }) => prefix
            .classify_head(&node.reference, &node.head, commit)
            .map(|status| match status {
                crate::sync::store::owner::verified_history::VerifiedMergePrefixHeadStatus::Included => {
                    MembershipProjectionStatus::Included
                }
                crate::sync::store::owner::verified_history::VerifiedMergePrefixHeadStatus::OutsidePrefix => {
                    MembershipProjectionStatus::OutsidePrefix
                }
            })
            .map_err(AnchoredChainError::LoadFailed),
        (true, crate::protocol::membership::MembershipHeadActivation::Direct) => {
            Err(AnchoredChainError::LoadFailed(
                "membership authority change has no exact Store activation".to_string(),
            ))
        }
        (false, crate::protocol::membership::MembershipHeadActivation::StoreCommit { .. }) => {
            Err(AnchoredChainError::LoadFailed(
                "direct membership change carries an unrelated Store activation".to_string(),
            ))
        }
    }
}

fn membership_projection_dependencies(
    graph: &LoadedExactMembershipGraph,
    resolution_activations: &BTreeMap<StoreMembershipConflictResolutionRef, MembershipCoord>,
    coord: &MembershipCoord,
) -> Result<Vec<MembershipCoord>, AnchoredChainError> {
    let node = graph.path_heads.get(coord).ok_or_else(|| {
        AnchoredChainError::LoadFailed(
            "membership projection dependency is absent from its candidate graph".to_string(),
        )
    })?;
    let mut dependencies = node
        .head
        .body
        .predecessor
        .iter()
        .map(|reference| reference.coord.clone())
        .chain(node.entry.dependencies.iter().cloned())
        .collect::<Vec<_>>();
    for resolution in &node.entry.resolution_dependencies {
        let introduced_here = matches!(
            &node.entry.change,
            MembershipChange::ResolutionActivation { resolution: introduced }
                if introduced == resolution
        );
        if !introduced_here {
            dependencies.push(
                resolution_activations
                    .get(resolution)
                    .ok_or_else(|| {
                        AnchoredChainError::LoadFailed(
                            "membership resolution lacks its candidate activation head".to_string(),
                        )
                    })?
                    .clone(),
            );
        }
    }
    Ok(dependencies)
}

pub(super) fn membership_projection_statuses(
    graph: &LoadedExactMembershipGraph,
    prefix: &crate::sync::store::owner::verified_history::VerifiedMergeMembershipPrefix,
    resolution_activations: &BTreeMap<StoreMembershipConflictResolutionRef, MembershipCoord>,
) -> Result<BTreeMap<MembershipCoord, MembershipProjectionStatus>, AnchoredChainError> {
    let mut statuses = BTreeMap::new();
    let mut visiting = BTreeSet::new();
    for root in graph.path_heads.keys() {
        if statuses.contains_key(root) {
            continue;
        }
        let mut stack = vec![(root.clone(), false)];
        while let Some((coord, expanded)) = stack.pop() {
            if statuses.contains_key(&coord) {
                continue;
            }
            if expanded {
                let node = graph.path_heads.get(&coord).ok_or_else(|| {
                    AnchoredChainError::LoadFailed(
                        "membership projection dependency is absent from its candidate graph"
                            .to_string(),
                    )
                })?;
                let dependencies =
                    membership_projection_dependencies(graph, resolution_activations, &coord)?;
                let status = if dependencies.iter().any(|dependency| {
                    statuses.get(dependency) == Some(&MembershipProjectionStatus::OutsidePrefix)
                }) {
                    MembershipProjectionStatus::OutsidePrefix
                } else {
                    for resolution in &node.entry.resolution_dependencies {
                        if !prefix.verifies_conflict_resolution(resolution) {
                            return Err(AnchoredChainError::LoadFailed(
                                "in-prefix membership resolution lacks its verified Store authority"
                                    .to_string(),
                            ));
                        }
                    }
                    MembershipProjectionStatus::Included
                };
                visiting.remove(&coord);
                statuses.insert(coord, status);
                continue;
            }
            if !visiting.insert(coord.clone()) {
                return Err(AnchoredChainError::LoadFailed(
                    "membership projection dependency graph contains a cycle".to_string(),
                ));
            }
            let activation_status = membership_projection_activation_status(graph, prefix, &coord)?;
            if activation_status == MembershipProjectionStatus::OutsidePrefix {
                visiting.remove(&coord);
                statuses.insert(coord, MembershipProjectionStatus::OutsidePrefix);
                continue;
            }
            let dependencies =
                membership_projection_dependencies(graph, resolution_activations, &coord)?;
            stack.push((coord, true));
            for dependency in dependencies.into_iter().rev() {
                if !statuses.contains_key(&dependency) {
                    stack.push((dependency, false));
                }
            }
        }
    }
    Ok(statuses)
}

pub(super) fn project_membership_cut_to_store_prefix(
    graph: &LoadedExactMembershipGraph,
    prefix: &crate::sync::store::owner::verified_history::VerifiedMergeMembershipPrefix,
) -> Result<
    (
        Vec<MembershipHeadRef>,
        Vec<StoreMembershipConflictResolutionRef>,
    ),
    AnchoredChainError,
> {
    let resolution_activations = membership_resolution_activations(graph)?;
    let statuses = membership_projection_statuses(graph, prefix, &resolution_activations)?;
    let mut projected = Vec::new();
    for (candidate, _) in &graph.heads {
        let mut current = Some(candidate);
        let selected = loop {
            let Some(reference) = current else {
                break None;
            };
            match statuses.get(&reference.coord).copied().ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "membership projection status is absent from its candidate graph".to_string(),
                )
            })? {
                MembershipProjectionStatus::Included => break Some(reference.clone()),
                MembershipProjectionStatus::OutsidePrefix => {
                    current = graph
                        .path_heads
                        .get(&reference.coord)
                        .ok_or_else(|| {
                            AnchoredChainError::LoadFailed(
                                "membership projection cursor is absent from its exact path"
                                    .to_string(),
                            )
                        })?
                        .head
                        .body
                        .predecessor
                        .as_ref();
                }
            }
        };
        if let Some(selected) = selected {
            projected.push(selected);
        }
    }
    projected.sort_by_key(|reference| reference.coord.stream_key());
    validate_membership_floor(&projected).map_err(AnchoredChainError::LoadFailed)?;
    let resolutions = projected
        .iter()
        .map(|reference| {
            graph.path_heads.get(&reference.coord).ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "projected membership head is absent from its exact candidate path".to_string(),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .flat_map(|node| node.head.body.resolutions.iter().cloned())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    Ok((projected, resolutions))
}

pub(super) fn exact_membership_chain_from_graph(
    root: &StoreRootRef,
    graph: LoadedExactMembershipGraph,
    provider_admin: crate::protocol::provider::ProviderAdminState,
) -> Result<MembershipChain, AnchoredChainError> {
    let chain = MembershipChain::from_entries_with_coords_and_heads_and_provider_admin(
        graph
            .entries
            .iter()
            .map(|(coord, entry)| (coord.clone(), entry.clone()))
            .collect(),
        graph.heads.clone(),
        provider_admin,
    )
    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    graph.validate_stream_anchors(root, &chain)?;
    Ok(chain)
}

pub(super) fn validate_owner_grant_records(
    root_value: &crate::protocol::store_commit::StoreProtocolRoot,
    entries: &[MembershipEntry],
) -> Result<(), AnchoredChainError> {
    for entry in entries {
        match &entry.change {
            MembershipChange::Founder { creation_id, .. }
                if *creation_id == root_value.descriptor.creation_id => {}
            MembershipChange::Founder { .. } => {
                return Err(AnchoredChainError::LoadFailed(
                    "founder Owner grant carries another Store creation id".to_string(),
                ));
            }
            MembershipChange::SetMember {
                role:
                    crate::protocol::membership::StoreMembershipRoleGrant::Owner {
                        recovery:
                            crate::protocol::membership::OwnerRecoveryAnchorRef::Promotion { .. },
                    },
                ..
            } => {
                // The exact head's Store activation verified this entry's promotion
                // acceptance before records are reduced into a membership chain.
            }
            MembershipChange::SetMember {
                role: crate::protocol::membership::StoreMembershipRoleGrant::Owner { .. },
                ..
            } => {
                return Err(AnchoredChainError::LoadFailed(
                    "non-founder Owner grant does not carry promotion acceptance".to_string(),
                ));
            }
            MembershipChange::SetMember { .. }
            | MembershipChange::RemoveMember { .. }
            | MembershipChange::ProviderAdmin
            | MembershipChange::ResolutionActivation { .. } => {}
        }
    }
    Ok(())
}
