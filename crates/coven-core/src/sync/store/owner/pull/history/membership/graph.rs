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
    fn head_refs(&self) -> Vec<MembershipHeadRef> {
        self.heads
            .iter()
            .map(|(reference, _)| reference.clone())
            .collect()
    }

    fn resolution_cut(&self) -> Vec<StoreMembershipConflictResolutionRef> {
        self.heads
            .iter()
            .flat_map(|(_, head)| head.body.resolutions.iter().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect()
    }
}

type LoadedMembershipHeadFuture<'a> = Pin<
    Box<dyn Future<Output = Result<LoadedExactMembershipHead, AnchoredChainError>> + Send + 'a>,
>;

fn load_exact_membership_head_node<'a>(
    commit_verifier: &'a crate::sync::store::owner::StoreCommitVerifier<'_>,
    reference: &'a MembershipHeadRef,
) -> LoadedMembershipHeadFuture<'a> {
    Box::pin(async move {
        let head = load_exact_membership_head_with_root(commit_verifier, reference).await?;
        let loaded_entry = Box::pin(crate::sync::store_objects::load_membership_entry_ref(
            commit_verifier.storage(),
            commit_verifier.root().store_root_hash,
            &head.body.entry,
        ))
        .await
        .map_err(map_membership_object_error)?;
        if loaded_entry.value.resolution_dependencies != head.body.resolutions {
            return Err(AnchoredChainError::LoadFailed(
                "membership head and selected entry carry different resolution cuts".to_string(),
            ));
        }
        Ok(LoadedExactMembershipHead {
            reference: reference.clone(),
            head,
            entry: loaded_entry.value,
        })
    })
}

fn validate_exact_membership_head_paths(
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

fn validate_exact_membership_stream_anchors(
    root: &StoreRootRef,
    graph: &LoadedExactMembershipGraph,
    chain: &MembershipChain,
) -> Result<(), AnchoredChainError> {
    for node in graph.path_heads.values() {
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
        let expected = crate::sync::store_commit::StreamActivation::grant_authorized(
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
    prefix: &crate::sync::store::owner::pull::VerifiedMergeMembershipPrefix,
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
        (false, crate::sync::membership::MembershipHeadActivation::Direct) => {
            Ok(MembershipProjectionStatus::Included)
        }
        (true, crate::sync::membership::MembershipHeadActivation::StoreCommit { commit }) => prefix
            .classify_head(&node.reference, &node.head, commit)
            .map(|status| match status {
                crate::sync::store::owner::pull::VerifiedMergePrefixHeadStatus::Included => {
                    MembershipProjectionStatus::Included
                }
                crate::sync::store::owner::pull::VerifiedMergePrefixHeadStatus::OutsidePrefix => {
                    MembershipProjectionStatus::OutsidePrefix
                }
            })
            .map_err(AnchoredChainError::LoadFailed),
        (true, crate::sync::membership::MembershipHeadActivation::Direct) => {
            Err(AnchoredChainError::LoadFailed(
                "membership authority change has no exact Store activation".to_string(),
            ))
        }
        (false, crate::sync::membership::MembershipHeadActivation::StoreCommit { .. }) => {
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
    prefix: &crate::sync::store::owner::pull::VerifiedMergeMembershipPrefix,
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

fn project_membership_cut_to_store_prefix(
    graph: &LoadedExactMembershipGraph,
    prefix: &crate::sync::store::owner::pull::VerifiedMergeMembershipPrefix,
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

pub(super) async fn project_anchored_chain_to_verified_store_prefix(
    commit_verifier: &crate::sync::store::owner::StoreCommitVerifier<'_>,
    candidate_heads: &[MembershipHeadRef],
    prefix: &crate::sync::store::owner::pull::VerifiedMergeMembershipPrefix,
) -> Result<MembershipChain, AnchoredChainError> {
    let candidate = load_exact_membership_graph_objects(commit_verifier, candidate_heads).await?;
    let (heads, resolutions) = project_membership_cut_to_store_prefix(&candidate, prefix)?;
    let projected = load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
        commit_verifier,
        &heads,
        &resolutions,
        prefix,
        None,
    )
    .await?;
    prefix
        .validate_complete_membership(&projected)
        .map_err(AnchoredChainError::LoadFailed)?;
    Ok(projected)
}

type LoadedMembershipGraphFuture<'a> = Pin<
    Box<dyn Future<Output = Result<LoadedExactMembershipGraph, AnchoredChainError>> + Send + 'a>,
>;

fn load_exact_membership_graph<'a>(
    activation_authority: &'a mut MembershipActivationAuthority<'_, '_>,
    exact_heads: &'a [MembershipHeadRef],
) -> LoadedMembershipGraphFuture<'a> {
    Box::pin(async move {
        let graph = load_exact_membership_graph_objects(
            activation_authority.commit_verifier(),
            exact_heads,
        )
        .await?;
        for node in graph.path_heads.values() {
            if !Box::pin(validate_membership_head_activation(
                activation_authority,
                &node.reference,
                &node.head,
                &node.entry,
            ))
            .await?
            {
                return Err(AnchoredChainError::LoadFailed(
                    "exact membership state names an unactivated Store-bound head".to_string(),
                ));
            }
        }
        let entry_values = graph.entries.values().cloned().collect::<Vec<_>>();
        Box::pin(validate_provider_admin_records(
            activation_authority.commit_verifier(),
            &entry_values,
        ))
        .await?;
        validate_owner_grant_records(
            activation_authority.commit_verifier().verified_root(),
            &entry_values,
        )?;
        Ok(graph)
    })
}

pub(super) fn load_exact_membership_graph_objects<'a>(
    commit_verifier: &'a crate::sync::store::owner::StoreCommitVerifier<'_>,
    exact_heads: &'a [MembershipHeadRef],
) -> LoadedMembershipGraphFuture<'a> {
    Box::pin(async move {
        let mut entries = BTreeMap::new();
        let mut heads = Vec::with_capacity(exact_heads.len());
        let mut path_heads = BTreeMap::<MembershipCoord, LoadedExactMembershipHead>::new();
        for requested in exact_heads {
            let mut current = Some(requested.clone());
            let mut requested_head = None;
            while let Some(reference) = current {
                let node =
                    Box::pin(load_exact_membership_head_node(commit_verifier, &reference)).await?;
                if reference == *requested {
                    requested_head = Some((reference.clone(), node.head.clone()));
                }
                match entries.entry(reference.coord.clone()) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(node.entry.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(slot) => {
                        if slot.get() != &node.entry {
                            return Err(AnchoredChainError::LoadFailed(
                                "membership coordinate selects different exact entries".to_string(),
                            ));
                        }
                    }
                }
                match path_heads.entry(reference.coord.clone()) {
                    std::collections::btree_map::Entry::Vacant(slot) => {
                        slot.insert(node.clone());
                    }
                    std::collections::btree_map::Entry::Occupied(slot) => {
                        if slot.get().reference != node.reference
                            || slot.get().head != node.head
                            || slot.get().entry != node.entry
                        {
                            return Err(AnchoredChainError::LoadFailed(
                                "membership coordinate selects different exact head paths"
                                    .to_string(),
                            ));
                        }
                    }
                }
                current = node.head.body.predecessor.clone();
            }
            heads.push(requested_head.ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "requested exact membership head was not loaded".to_string(),
                )
            })?);
        }
        let graph = LoadedExactMembershipGraph {
            entries,
            heads,
            path_heads,
        };
        validate_exact_membership_head_paths(&graph)?;
        Ok(graph)
    })
}

#[cfg(test)]
pub(super) async fn assert_deep_valid_predecessor_path_is_iterative(
    commit_verifier: &crate::sync::store::owner::StoreCommitVerifier<'_>,
    heads: &[MembershipHeadRef],
) {
    let seed = load_exact_membership_graph_objects(commit_verifier, heads)
        .await
        .expect("load seed membership graph")
        .path_heads
        .into_values()
        .next()
        .expect("founder membership head");

    let mut path_heads = BTreeMap::new();
    let mut predecessor = None;
    for sequence in 1..=20_000_u64 {
        let mut node = seed.clone();
        node.entry.seq = sequence;
        node.entry.previous_hash = predecessor
            .as_ref()
            .map(|reference: &MembershipHeadRef| reference.coord.entry_hash);
        node.entry.dependencies.clear();
        node.entry.resolution_dependencies.clear();
        node.reference.coord = node.entry.coord();
        node.head.body.entry.coord = node.reference.coord.clone();
        node.head.body.predecessor = predecessor.clone();
        predecessor = Some(node.reference.clone());
        path_heads.insert(node.reference.coord.clone(), node);
    }
    let graph = LoadedExactMembershipGraph {
        entries: path_heads
            .iter()
            .map(|(coord, node)| (coord.clone(), node.entry.clone()))
            .collect(),
        heads: Vec::new(),
        path_heads,
    };
    let statuses = membership_projection_statuses(
        &graph,
        &crate::sync::store::owner::pull::VerifiedMergeMembershipPrefix::default(),
        &BTreeMap::new(),
    )
    .expect("project deep predecessor path");

    assert_eq!(statuses.len(), 20_000);
    assert!(statuses
        .values()
        .all(|status| *status == MembershipProjectionStatus::Included));
}

fn exact_membership_chain_from_graph(
    root: &StoreRootRef,
    graph: LoadedExactMembershipGraph,
    provider_admin: crate::sync::provider::ProviderAdminState,
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
    validate_exact_membership_stream_anchors(root, &graph, &chain)?;
    Ok(chain)
}

fn add_exact_membership_suffix(
    chain: &mut MembershipChain,
    graph: &LoadedExactMembershipGraph,
) -> Result<(), AnchoredChainError> {
    let mut pending = graph
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
                || graph.entries.keys().any(|candidate| {
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
    for (reference, _) in &graph.heads {
        chain
            .activate_head_ref(reference.clone())
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    }
    Ok(())
}

type LayeredMembershipFuture<'a> =
    Pin<Box<dyn Future<Output = Result<MembershipChain, AnchoredChainError>> + Send + 'a>>;

#[allow(clippy::too_many_arguments)]
fn load_layered_membership_chain<'a>(
    activation_authority: &'a mut MembershipActivationAuthority<'_, '_>,
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    graph: LoadedExactMembershipGraph,
    exact_resolutions: &'a [StoreMembershipConflictResolutionRef],
    provider_admin: &'a crate::sync::provider::ProviderAdminState,
    pending_resolution: Option<
        &'a crate::sync::store::owner::pull::VerifiedMergeConflictResolutionActivation,
    >,
) -> LayeredMembershipFuture<'a> {
    Box::pin(async move {
        let exact_heads = graph.head_refs();
        if exact_resolutions.is_empty() {
            if !graph.resolution_cut().is_empty() {
                return Err(AnchoredChainError::LoadFailed(
                    "membership signed heads name a nonempty resolution cut".to_string(),
                ));
            }
            return exact_membership_chain_from_graph(root, graph, provider_admin.clone());
        }

        let mut resolutions = BTreeMap::new();
        for reference in exact_resolutions {
            let value = Box::pin(crate::sync::store_objects::load_membership_resolution_ref(
                storage,
                root.store_root_hash,
                reference,
            ))
            .await
            .map_err(map_membership_object_error)?
            .value;
            match &mut *activation_authority {
                MembershipActivationAuthority::VerifiedPrefix { activations, .. } => {
                    let verified_by_prefix = activations.verifies_conflict_resolution(reference);
                    let verified_by_pending =
                        pending_resolution.is_some_and(|pending| pending.verifies(reference));
                    if !verified_by_prefix && !verified_by_pending {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership conflict resolution is absent from its verified Store authority"
                                .to_string(),
                        ));
                    }
                }
                MembershipActivationAuthority::History(history) => {
                    Box::pin(history.verify_owner_conflict_acceptance(
                        &value.replacement_acceptance,
                        &value.resolver_pubkey,
                    ))
                    .await
                    .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
                }
            }
            resolutions.insert(reference.clone(), value);
        }

        let target_cut = exact_resolutions.iter().cloned().collect::<BTreeSet<_>>();
        let mut activation_counts = BTreeMap::<_, usize>::new();
        for entry in graph.entries.values() {
            if let MembershipChange::ResolutionActivation { resolution } = &entry.change {
                if entry.resolution_dependencies == exact_resolutions {
                    *activation_counts.entry(resolution.clone()).or_default() += 1;
                }
            }
        }
        if let Some(pending) = pending_resolution {
            *activation_counts
                .entry(pending.reference().clone())
                .or_default() += 1;
        }
        if activation_counts.values().any(|count| *count != 1) {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution has multiple exact activations".to_string(),
            ));
        }
        let activated_here = activation_counts.keys().cloned().collect::<BTreeSet<_>>();
        if !activated_here.is_subset(&target_cut) || activated_here.is_empty() {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution cut has no exact activation layer".to_string(),
            ));
        }

        let first_resolution = resolutions
            .get(
                activated_here
                    .first()
                    .expect("activation layer is nonempty"),
            )
            .expect("activated resolution belongs to the exact cut");
        let conflict_heads = &first_resolution.conflicting_heads;
        if activated_here.iter().any(|reference| {
            resolutions.get(reference).is_none_or(|resolution| {
                resolution.conflict_hash != first_resolution.conflict_hash
                    || resolution.conflicting_heads != *conflict_heads
            })
        }) {
            return Err(AnchoredChainError::LoadFailed(
                "membership activation layer combines different conflicts".to_string(),
            ));
        }
        let conflict_graph =
            load_exact_membership_graph(activation_authority, conflict_heads).await?;
        let prior_cut = conflict_graph.resolution_cut();
        let prior_set = prior_cut.iter().cloned().collect::<BTreeSet<_>>();
        let introduced = target_cut
            .difference(&prior_set)
            .cloned()
            .collect::<BTreeSet<_>>();
        if activated_here != introduced {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution activations differ from the introduced cut".to_string(),
            ));
        }
        let mut chain = load_layered_membership_chain(
            activation_authority,
            storage,
            root,
            conflict_graph,
            &prior_cut,
            provider_admin,
            None,
        )
        .await?;
        let introduced_resolutions = introduced
            .iter()
            .map(|reference| {
                resolutions
                    .get(reference)
                    .cloned()
                    .map(|value| (reference.clone(), value))
                    .ok_or_else(|| {
                        AnchoredChainError::LoadFailed(
                            "introduced membership resolution is absent from its exact cut"
                                .to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>, _>>()?;
        chain
            .apply_resolutions(root.store_root_hash, &introduced_resolutions)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        add_exact_membership_suffix(&mut chain, &graph)?;
        validate_exact_membership_stream_anchors(root, &graph, &chain)?;
        if chain.head_refs() != exact_heads || chain.resolution_refs() != exact_resolutions {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution reconstruction differs from its exact state".to_string(),
            ));
        }
        Ok(chain)
    })
}

pub(super) async fn load_anchored_chain_at_exact_heads_with_root_impl(
    activation_authority: &mut MembershipActivationAuthority<'_, '_>,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &crate::sync::store_commit::StoreProtocolRoot,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
    pending_resolution: Option<
        &crate::sync::store::owner::pull::VerifiedMergeConflictResolutionActivation,
    >,
) -> Result<MembershipChain, AnchoredChainError> {
    validate_membership_floor(exact_heads).map_err(AnchoredChainError::LoadFailed)?;
    if !exact_resolutions.windows(2).all(|pair| pair[0] < pair[1]) {
        return Err(AnchoredChainError::LoadFailed(
            "membership resolution cut is not canonical".to_string(),
        ));
    }
    let founder_registration = activation_authority
        .commit_verifier()
        .load_founder_registration()
        .await
        .map_err(map_membership_object_error)?;
    let founder_registration_ref =
        crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
            &founder_registration.value,
            founder_registration.object,
        );
    let provider_admin = crate::sync::provider::ProviderAdminState::founder_from_root(
        root.clone(),
        founder_registration_ref,
        &root_value.descriptor.founder_provider_admin,
    );
    let graph = Box::pin(load_exact_membership_graph(
        activation_authority,
        exact_heads,
    ))
    .await?;
    let chain = load_layered_membership_chain(
        activation_authority,
        storage,
        root,
        graph,
        exact_resolutions,
        &provider_admin,
        pending_resolution,
    )
    .await?;
    if !chain.is_founded_by(owner_pubkey) {
        return Err(AnchoredChainError::FounderMismatch {
            founder: chain.founder_pubkey().map(str::to_string),
            owner: owner_pubkey.to_string(),
        });
    }
    Ok(chain)
}

async fn validate_provider_admin_records(
    commit_verifier: &crate::sync::store::owner::StoreCommitVerifier<'_>,
    entries: &[MembershipEntry],
) -> Result<(), AnchoredChainError> {
    for entry in entries {
        let Some(crate::sync::provider::ProviderAdminMembershipChange {
            change:
                crate::sync::provider::ProviderAdminChange::Set {
                    administrator,
                    provider,
                    capability,
                    ..
                },
            ..
        }) = &entry.provider_admin
        else {
            continue;
        };
        let registration = commit_verifier
            .load_registration(administrator)
            .await
            .map_err(map_membership_object_error)?;
        if registration.value.store_root != *commit_verifier.root()
            || registration.value.provider != *provider
        {
            return Err(AnchoredChainError::LoadFailed(
                "provider administrator grant does not match its exact device registration"
                    .to_string(),
            ));
        }
        capability
            .verify(
                &commit_verifier.verified_root().descriptor.provider,
                provider,
            )
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
    }
    Ok(())
}

fn validate_owner_grant_records(
    root_value: &crate::sync::store_commit::StoreProtocolRoot,
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
                    crate::sync::membership::StoreMembershipRoleGrant::Owner {
                        recovery: crate::sync::membership::OwnerRecoveryAnchorRef::Promotion { .. },
                    },
                ..
            } => {
                // The exact head's Store activation verified this entry's promotion
                // acceptance before records are reduced into a membership chain.
            }
            MembershipChange::SetMember {
                role: crate::sync::membership::StoreMembershipRoleGrant::Owner { .. },
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
