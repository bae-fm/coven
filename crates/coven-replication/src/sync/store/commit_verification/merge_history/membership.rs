use crate::sync::store::membership::AnchoredChainError;
use coven_protocol::membership::{
    validate_membership_floor, AuthorHead, MembershipChain, MembershipChange, MembershipCoord,
    MembershipEntry, MembershipGrantId, MembershipHeadRef, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionRef,
};
use coven_protocol::objects::StorageError;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::{GrantStreamAnchor, StoreRootRef};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

mod graph;

struct ExactMembershipStream {
    entries: Vec<(MembershipCoord, MembershipEntry)>,
    heads: Vec<(MembershipHeadRef, AuthorHead)>,
    resolutions: BTreeMap<StoreMembershipConflictResolutionRef, StoreMembershipConflictResolution>,
}

type LoadedMembershipGraphFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<graph::LoadedExactMembershipGraph, AnchoredChainError>>
            + Send
            + 'a,
    >,
>;

type LayeredMembershipFuture<'a> =
    Pin<Box<dyn Future<Output = Result<MembershipChain, AnchoredChainError>> + Send + 'a>>;

enum MembershipActivationAuthority<'operation, 'storage> {
    History {
        history: &'operation mut crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<
            'storage,
        >,
    },
    VerifiedPrefix {
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
        commit_verifier: &'operation crate::sync::store::commit_verification::commit::StoreCommitVerifier<'storage>,
        activations:
            &'operation crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix,
    },
}

pub(super) struct HistoryMembershipActivation<'operation, 'storage> {
    authority: MembershipActivationAuthority<'operation, 'storage>,
}

pub(super) struct VerifiedPrefixMembershipActivation<'operation, 'storage> {
    authority: MembershipActivationAuthority<'operation, 'storage>,
}

impl<'operation, 'storage> HistoryMembershipActivation<'operation, 'storage> {
    pub(super) fn new(
        history: &'operation mut crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<
            'storage,
        >,
    ) -> Self {
        Self {
            authority: MembershipActivationAuthority::History { history },
        }
    }

    pub(super) async fn load_exact_anchored_chain(
        &mut self,
        cursors: &[MembershipHeadRef],
        owner_pubkey: Option<&str>,
    ) -> Result<MembershipChain, AnchoredChainError> {
        self.authority
            .load_exact_anchored_chain(cursors, owner_pubkey)
            .await
    }

    pub(super) async fn load_at_exact_heads(
        &mut self,
        exact_heads: &[MembershipHeadRef],
        exact_resolutions: &[StoreMembershipConflictResolutionRef],
    ) -> Result<MembershipChain, AnchoredChainError> {
        self.authority
            .load_at_exact_heads(exact_heads, exact_resolutions, None)
            .await
    }

    #[cfg(any(test, feature = "test-utils"))]
    pub(super) async fn assert_deep_valid_predecessor_path_is_iterative(
        &mut self,
        heads: &[MembershipHeadRef],
    ) {
        let seed = self
            .authority
            .load_exact_membership_graph_objects(heads)
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
            let entry = node.entry.body_mut();
            entry.seq = sequence;
            entry.previous_hash = predecessor
                .as_ref()
                .map(|reference: &MembershipHeadRef| reference.coord.entry_hash);
            entry.dependencies.clear();
            entry.resolution_dependencies.clear();
            node.reference.coord = node.entry.coord();
            let head = node.head.body_mut();
            head.body.entry.coord = node.reference.coord.clone();
            head.body.predecessor = predecessor.clone();
            predecessor = Some(node.reference.clone());
            path_heads.insert(node.reference.coord.clone(), node);
        }
        let graph = graph::LoadedExactMembershipGraph {
            entries: path_heads
                .iter()
                .map(|(coord, node)| (coord.clone(), node.entry.clone()))
                .collect(),
            heads: Vec::new(),
            path_heads,
        };
        let statuses = graph::membership_projection_statuses(
            &graph,
            &crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix::default(),
            &BTreeMap::new(),
        )
        .expect("project deep predecessor path");

        assert_eq!(statuses.len(), 20_000);
        assert!(statuses
            .values()
            .all(|status| *status == graph::MembershipProjectionStatus::Included));
    }
}

impl<'operation, 'storage> VerifiedPrefixMembershipActivation<'operation, 'storage> {
    pub(super) fn new(
        root: &crate::sync::store::protocol_root::VerifiedStoreRoot,
        commit_verifier: &'operation crate::sync::store::commit_verification::commit::StoreCommitVerifier<'storage>,
        activations: &'operation crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix,
    ) -> Self {
        Self {
            authority: MembershipActivationAuthority::VerifiedPrefix {
                root: root.clone(),
                commit_verifier,
                activations,
            },
        }
    }

    pub(super) async fn load_at_exact_heads(
        &mut self,
        exact_heads: &[MembershipHeadRef],
        exact_resolutions: &[StoreMembershipConflictResolutionRef],
        pending_resolution: Option<
            &crate::sync::store::commit_verification::merge_history::VerifiedMergeConflictResolutionActivation,
        >,
    ) -> Result<MembershipChain, AnchoredChainError> {
        self.authority
            .load_at_exact_heads(exact_heads, exact_resolutions, pending_resolution)
            .await
    }

    pub(super) async fn project(
        &mut self,
        candidate_heads: &[MembershipHeadRef],
    ) -> Result<MembershipChain, AnchoredChainError> {
        let MembershipActivationAuthority::VerifiedPrefix { activations, .. } = &self.authority
        else {
            unreachable!("verified-prefix membership authority has one construction state")
        };
        let prefix = (*activations).clone();
        let candidate = self
            .authority
            .load_exact_membership_graph_objects(candidate_heads)
            .await?;
        let (heads, resolutions) =
            graph::project_membership_cut_to_store_prefix(&candidate, &prefix)?;
        let projected = self
            .authority
            .load_anchored_chain_at_exact_heads(&heads, &resolutions, None)
            .await?;
        prefix
            .validate_complete_membership(&projected)
            .map_err(AnchoredChainError::LoadFailed)?;
        Ok(projected)
    }
}

fn membership_entry_requires_store_activation(entry: &MembershipEntry) -> bool {
    match &entry.change {
        MembershipChange::Founder { .. } | MembershipChange::ProviderAdmin => false,
        MembershipChange::SetMember {
            role,
            retirement_barriers,
            ..
        } => {
            matches!(
                role,
                coven_protocol::membership::StoreMembershipRoleGrant::Owner { .. }
            ) || retirement_barriers.values().any(|barrier| {
                matches!(
                    barrier,
                    coven_protocol::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                )
            })
        }
        MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } => retirement_barriers.values().any(|barrier| {
            matches!(
                barrier,
                coven_protocol::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
            )
        }),
        MembershipChange::ResolutionActivation { .. } => true,
    }
}

impl<'storage> MembershipActivationAuthority<'_, 'storage> {
    async fn load_exact_anchored_chain(
        &mut self,
        cursors: &[MembershipHeadRef],
        owner_pubkey: Option<&str>,
    ) -> Result<MembershipChain, AnchoredChainError> {
        let root = self.root().clone();
        let root_value = self.verified_root().clone();
        if let Some(owner) = owner_pubkey {
            if root_value.descriptor.founder_pubkey != owner {
                return Err(AnchoredChainError::FounderMismatch {
                    founder: Some(root_value.descriptor.founder_pubkey.clone()),
                    owner: owner.to_string(),
                });
            }
        }
        let anchor = &root_value.descriptor.founder_membership;
        let founder_stream = coven_protocol::membership::derive_founder_stream_id(
            &root.store_root_id.to_string(),
            &root_value.descriptor.founder_pubkey,
        );
        let cursor = cursors.iter().find(|cursor| {
            cursor.coord.author_pubkey == root_value.descriptor.founder_pubkey
                && cursor.coord.author_owner_grant == root_value.descriptor.founder_grant
                && cursor.coord.stream_id == founder_stream
        });
        let founder_loaded = Box::pin(self.traverse_exact_membership_stream(
            &root_value.descriptor.founder_pubkey,
            &root_value.descriptor.founder_grant,
            founder_stream,
            anchor,
            cursor,
        ))
        .await?;
        let founder_latest = founder_loaded.heads.last().cloned().ok_or_else(|| {
            AnchoredChainError::LoadFailed("founder membership head is absent".to_string())
        })?;
        let founder = founder_loaded
            .entries
            .first()
            .map(|(_, entry)| entry)
            .ok_or_else(|| {
                AnchoredChainError::LoadFailed("founder membership entry is absent".to_string())
            })?;
        if root_value
            .descriptor
            .validate_merge_founder_entry(founder)
            .is_err()
        {
            return Err(AnchoredChainError::LoadFailed(
                "first exact membership entry differs from the signed Store founder".to_string(),
            ));
        }
        let mut discovered =
            std::collections::BTreeSet::from([founder_latest.0.coord.stream_key()]);
        let mut consumed_cursors = std::collections::BTreeSet::new();
        if let Some(cursor) = cursor {
            consumed_cursors.insert(cursor.clone());
        }
        let mut latest_heads = vec![founder_latest];
        let mut resolutions = founder_loaded.resolutions;

        loop {
            let exact_heads = latest_heads
                .iter()
                .map(|(reference, _)| reference.clone())
                .collect::<Vec<_>>();
            let resolution_refs = resolutions.keys().cloned().collect::<Vec<_>>();
            let chain = Box::pin(self.load_anchored_chain_at_exact_heads(
                &exact_heads,
                &resolution_refs,
                None,
            ))
            .await?;
            let pending = chain
                .activated_membership_streams()
                .into_iter()
                .filter(|(stream, _)| !discovered.contains(stream))
                .collect::<Vec<_>>();
            if pending.is_empty() {
                if consumed_cursors.len() != cursors.len() {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership cursor names a stream that is not activated by the anchored chain"
                            .to_string(),
                    ));
                }
                return Ok(chain);
            }

            for (stream, anchor) in pending {
                let cursor = cursors
                    .iter()
                    .find(|cursor| cursor.coord.stream_key() == stream);
                let loaded = Box::pin(self.traverse_exact_membership_stream(
                    &stream.author_pubkey,
                    &stream.author_owner_grant,
                    stream.stream_id,
                    &anchor,
                    cursor,
                ))
                .await?;
                if let Some(cursor) = cursor {
                    consumed_cursors.insert(cursor.clone());
                }
                resolutions.extend(loaded.resolutions);
                if let Some(latest) = loaded.heads.last().cloned() {
                    latest_heads.push(latest);
                    latest_heads.sort_by_key(|(reference, _)| reference.coord.stream_key());
                }
                discovered.insert(stream);
            }
        }
    }

    async fn load_at_exact_heads(
        &mut self,
        exact_heads: &[MembershipHeadRef],
        exact_resolutions: &[StoreMembershipConflictResolutionRef],
        pending_resolution: Option<
            &crate::sync::store::commit_verification::merge_history::VerifiedMergeConflictResolutionActivation,
        >,
    ) -> Result<MembershipChain, AnchoredChainError> {
        Box::pin(self.load_anchored_chain_at_exact_heads(
            exact_heads,
            exact_resolutions,
            pending_resolution,
        ))
        .await
    }

    fn load_exact_membership_graph_objects<'a>(
        &'a mut self,
        exact_heads: &'a [MembershipHeadRef],
    ) -> LoadedMembershipGraphFuture<'a> {
        Box::pin(async move {
            let mut entries = BTreeMap::new();
            let mut heads = Vec::with_capacity(exact_heads.len());
            let mut path_heads =
                BTreeMap::<MembershipCoord, graph::LoadedExactMembershipHead>::new();
            for requested in exact_heads {
                let mut current = Some(requested.clone());
                let mut requested_head = None;
                while let Some(reference) = current {
                    let node = self.load_exact_membership_head_node(&reference).await?;
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
                                    "membership coordinate selects different exact entries"
                                        .to_string(),
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
            let graph = graph::LoadedExactMembershipGraph {
                entries,
                heads,
                path_heads,
            };
            graph::validate_exact_membership_head_paths(&graph)?;
            Ok(graph)
        })
    }

    fn load_exact_membership_graph<'a>(
        &'a mut self,
        exact_heads: &'a [MembershipHeadRef],
    ) -> LoadedMembershipGraphFuture<'a> {
        Box::pin(async move {
            let graph = self
                .load_exact_membership_graph_objects(exact_heads)
                .await?;
            for node in graph.path_heads.values() {
                if !Box::pin(self.validate_head_activation(
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
            self.validate_provider_admin_records(&entry_values).await?;
            graph::validate_owner_grant_records(self.verified_root(), &entry_values)?;
            Ok(graph)
        })
    }

    fn load_layered_membership_chain<'a>(
        &'a mut self,
        graph: graph::LoadedExactMembershipGraph,
        exact_resolutions: &'a [StoreMembershipConflictResolutionRef],
        provider_admin: &'a coven_protocol::provider::ProviderAdminState,
        pending_resolution: Option<
            &'a crate::sync::store::commit_verification::merge_history::VerifiedMergeConflictResolutionActivation,
        >,
    ) -> LayeredMembershipFuture<'a> {
        Box::pin(async move {
            let root = self.root().clone();
            let exact_heads = graph.head_refs();
            if exact_resolutions.is_empty() {
                if !graph.resolution_cut().is_empty() {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership signed heads name a nonempty resolution cut".to_string(),
                    ));
                }
                return graph::exact_membership_chain_from_graph(
                    &root,
                    graph,
                    provider_admin.clone(),
                );
            }

            let mut resolutions = BTreeMap::new();
            for reference in exact_resolutions {
                let value = Box::pin(self.load_membership_resolution(reference))
                    .await
                    .map_err(map_membership_object_error)?
                    .value;
                match &mut *self {
                    MembershipActivationAuthority::VerifiedPrefix { activations, .. } => {
                        let verified_by_prefix =
                            activations.verifies_conflict_resolution(reference);
                        let verified_by_pending =
                            pending_resolution.is_some_and(|pending| pending.verifies(reference));
                        if !verified_by_prefix && !verified_by_pending {
                            return Err(AnchoredChainError::LoadFailed(
                                "membership conflict resolution is absent from its verified Store authority"
                                    .to_string(),
                            ));
                        }
                    }
                    MembershipActivationAuthority::History { history, .. } => {
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
            let conflict_graph = self.load_exact_membership_graph(conflict_heads).await?;
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
            let mut chain = self
                .load_layered_membership_chain(conflict_graph, &prior_cut, provider_admin, None)
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
            graph.add_exact_suffix(&mut chain)?;
            graph.validate_stream_anchors(&root, &chain)?;
            if chain.head_refs() != exact_heads || chain.resolution_refs() != exact_resolutions {
                return Err(AnchoredChainError::LoadFailed(
                    "membership resolution reconstruction differs from its exact state".to_string(),
                ));
            }
            Ok(chain)
        })
    }

    async fn load_anchored_chain_at_exact_heads(
        &mut self,
        exact_heads: &[MembershipHeadRef],
        exact_resolutions: &[StoreMembershipConflictResolutionRef],
        pending_resolution: Option<
            &crate::sync::store::commit_verification::merge_history::VerifiedMergeConflictResolutionActivation,
        >,
    ) -> Result<MembershipChain, AnchoredChainError> {
        validate_membership_floor(exact_heads).map_err(AnchoredChainError::LoadFailed)?;
        if !exact_resolutions.windows(2).all(|pair| pair[0] < pair[1]) {
            return Err(AnchoredChainError::LoadFailed(
                "membership resolution cut is not canonical".to_string(),
            ));
        }
        let root = self.root().clone();
        let root_value = self.verified_root().clone();
        let owner_pubkey = root_value.descriptor.founder_pubkey.clone();
        let founder_registration = self
            .load_founder_registration()
            .await
            .map_err(map_membership_object_error)?;
        let founder_registration_ref =
            coven_protocol::store_commit::StoreDeviceRegistrationRef::from_registration(
                &founder_registration.value,
                founder_registration.object,
            );
        let provider_admin = coven_protocol::provider::ProviderAdminState::founder_from_root(
            root.clone(),
            founder_registration_ref,
            &root_value.descriptor.founder_provider_admin,
        );
        let graph = Box::pin(self.load_exact_membership_graph(exact_heads)).await?;
        let chain = self
            .load_layered_membership_chain(
                graph,
                exact_resolutions,
                &provider_admin,
                pending_resolution,
            )
            .await?;
        if !chain.is_founded_by(&owner_pubkey) {
            return Err(AnchoredChainError::FounderMismatch {
                founder: chain.founder_pubkey().map(str::to_string),
                owner: owner_pubkey,
            });
        }
        Ok(chain)
    }

    async fn load_registration(
        &self,
        reference: &coven_protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<
            coven_protocol::store_commit::StoreDeviceRegistration,
        >,
        StoreObjectError,
    > {
        match self {
            Self::History { history } => history.commit_verifier.load_registration(reference).await,
            Self::VerifiedPrefix {
                commit_verifier, ..
            } => commit_verifier.load_registration(reference).await,
        }
    }

    async fn validate_provider_admin_records(
        &self,
        entries: &[MembershipEntry],
    ) -> Result<(), AnchoredChainError> {
        for entry in entries {
            let Some(coven_protocol::provider::ProviderAdminMembershipChange {
                change:
                    coven_protocol::provider::ProviderAdminChange::Set {
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
            let registration = self
                .load_registration(administrator)
                .await
                .map_err(map_membership_object_error)?;
            if registration.value.store_root != *self.root()
                || registration.value.provider != *provider
            {
                return Err(AnchoredChainError::LoadFailed(
                    "provider administrator grant does not match its exact device registration"
                        .to_string(),
                ));
            }
            capability
                .verify(&self.verified_root().descriptor.provider, provider)
                .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        }
        Ok(())
    }

    async fn load_founder_registration(
        &self,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<
            coven_protocol::store_commit::StoreDeviceRegistration,
        >,
        StoreObjectError,
    > {
        match self {
            Self::History { history } => history.commit_verifier.load_founder_registration().await,
            Self::VerifiedPrefix {
                commit_verifier, ..
            } => commit_verifier.load_founder_registration().await,
        }
    }

    fn root(&self) -> &StoreRootRef {
        match self {
            Self::History { history } => history.root.reference(),
            Self::VerifiedPrefix { root, .. } => root.reference(),
        }
    }

    fn verified_root(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
        match self {
            Self::History { history } => history.root.protocol(),
            Self::VerifiedPrefix { root, .. } => root.protocol(),
        }
    }

    async fn load_exact_membership_head(
        &self,
        reference: &MembershipHeadRef,
    ) -> Result<AuthorHead, AnchoredChainError> {
        let loaded = match self {
            Self::History { history } => {
                history
                    .commit_verifier
                    .membership_objects()
                    .load_head(reference)
                    .await
            }
            Self::VerifiedPrefix {
                commit_verifier, ..
            } => {
                commit_verifier
                    .membership_objects()
                    .load_head(reference)
                    .await
            }
        };
        loaded
            .map(|loaded| loaded.value)
            .map_err(map_membership_object_error)
    }

    async fn load_exact_membership_head_node(
        &self,
        reference: &MembershipHeadRef,
    ) -> Result<graph::LoadedExactMembershipHead, AnchoredChainError> {
        let head = self.load_exact_membership_head(reference).await?;
        let loaded_entry = self
            .load_membership_entry(&head.body.entry)
            .await
            .map_err(map_membership_object_error)?;
        if loaded_entry.value.resolution_dependencies != head.body.resolutions {
            return Err(AnchoredChainError::LoadFailed(
                "membership head and selected entry carry different resolution cuts".to_string(),
            ));
        }
        Ok(graph::LoadedExactMembershipHead {
            reference: reference.clone(),
            head,
            entry: loaded_entry.value,
        })
    }

    async fn load_membership_entry(
        &self,
        reference: &coven_protocol::membership::MembershipEntryRef,
    ) -> Result<coven_protocol::objects::VerifiedObject<MembershipEntry>, StoreObjectError> {
        match self {
            Self::History { history } => {
                history
                    .commit_verifier
                    .membership_objects()
                    .load_entry(reference)
                    .await
            }
            Self::VerifiedPrefix {
                commit_verifier, ..
            } => {
                commit_verifier
                    .membership_objects()
                    .load_entry(reference)
                    .await
            }
        }
    }

    async fn load_membership_resolution(
        &self,
        reference: &StoreMembershipConflictResolutionRef,
    ) -> Result<
        coven_protocol::objects::VerifiedObject<StoreMembershipConflictResolution>,
        StoreObjectError,
    > {
        match self {
            Self::History { history } => {
                history
                    .commit_verifier
                    .membership_objects()
                    .load_resolution(reference)
                    .await
            }
            Self::VerifiedPrefix {
                commit_verifier, ..
            } => {
                commit_verifier
                    .membership_objects()
                    .load_resolution(reference)
                    .await
            }
        }
    }

    async fn load_membership_head_at_slot(
        &self,
        slot: &coven_protocol::objects::ObjectSlot,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
        sequence: u64,
    ) -> Result<coven_protocol::objects::VerifiedObject<AuthorHead>, StoreObjectError> {
        match self {
            Self::History { history } => {
                history
                    .commit_verifier
                    .membership_objects()
                    .load_head_at_slot(slot, author, grant, stream_id, sequence)
                    .await
            }
            Self::VerifiedPrefix {
                commit_verifier, ..
            } => {
                commit_verifier
                    .membership_objects()
                    .load_head_at_slot(slot, author, grant, stream_id, sequence)
                    .await
            }
        }
    }

    async fn validate_head_activation(
        &mut self,
        reference: &MembershipHeadRef,
        head: &AuthorHead,
        entry: &MembershipEntry,
    ) -> Result<bool, AnchoredChainError> {
        match (
            membership_entry_requires_store_activation(entry),
            &head.activation,
        ) {
            (false, coven_protocol::membership::MembershipHeadActivation::Direct) => Ok(true),
            (
                true,
                coven_protocol::membership::MembershipHeadActivation::StoreCommit { commit },
            ) => match self {
                MembershipActivationAuthority::VerifiedPrefix {
                    activations: verified_activations,
                    ..
                } => {
                    let activation =
                        verified_activations
                            .head_activation(commit)
                            .ok_or_else(|| {
                                AnchoredChainError::LoadFailed(
                            "membership head activation is absent from its verified Store prefix"
                                .to_string(),
                        )
                            })?;
                    if !activation.verifies(reference, head, commit) {
                        return Err(AnchoredChainError::LoadFailed(
                            "membership head differs from its verified Store activation"
                                .to_string(),
                        ));
                    }
                    Ok(true)
                }
                MembershipActivationAuthority::History { history, .. } => {
                    Box::pin(history.verify_membership_head_activation(reference, head, commit))
                        .await
                        .map_err(AnchoredChainError::LoadFailed)
                }
            },
            (true, coven_protocol::membership::MembershipHeadActivation::Direct) => {
                Err(AnchoredChainError::LoadFailed(
                    "membership authority change has no exact Store activation".to_string(),
                ))
            }
            (false, coven_protocol::membership::MembershipHeadActivation::StoreCommit { .. }) => {
                Err(AnchoredChainError::LoadFailed(
                    "direct membership change carries an unrelated Store activation".to_string(),
                ))
            }
        }
    }

    async fn traverse_exact_membership_stream(
        &mut self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
        anchor: &GrantStreamAnchor,
        cursor: Option<&MembershipHeadRef>,
    ) -> Result<ExactMembershipStream, AnchoredChainError> {
        let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
            return Err(AnchoredChainError::LoadFailed(
                "membership stream uses a recovery anchor".to_string(),
            ));
        };
        let mut slot = first_slot.clone();
        let mut expected_sequence = 1_u64;
        let mut predecessor: Option<MembershipHeadRef> = None;
        let mut entries = Vec::new();
        let mut heads = Vec::new();
        let mut resolutions = BTreeMap::new();
        let mut reached_cursor = cursor.is_none();

        loop {
            let loaded = match self
                .load_membership_head_at_slot(&slot, author, grant, stream_id, expected_sequence)
                .await
            {
                Ok(value) => value,
                Err(StoreObjectError::Storage(StorageError::NotFound(_))) => break,
                Err(StoreObjectError::Storage(source)) if source.is_transport() => {
                    return Err(AnchoredChainError::StorageUnavailable {
                        operation: format!(
                            "read membership head {author}/{grant}/{stream_id}/{expected_sequence}"
                        ),
                        source,
                    })
                }
                Err(error) => return Err(map_membership_object_error(error)),
            };
            let object = loaded.object;
            let head = loaded.value;
            let coord = head.entry_coord();
            let reference = MembershipHeadRef {
                coord: coord.clone(),
                head_hash: head.head_hash(),
                object,
            };
            if head.body.predecessor != predecessor
                || head.body.successor.predecessor
                    != predecessor
                        .as_ref()
                        .map(|reference| reference.object.clone())
            {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {coord:?} does not extend its exact predecessor"
                )));
            }
            if head.body.successor.activation
                != coven_protocol::store_commit::StreamActivation::grant_authorized(
                    self.root().store_root_hash,
                    head.body.author_registration.clone(),
                    grant.clone(),
                    anchor.clone(),
                )
                .activation_id()
            {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {coord:?} is not signed by its activated certified device"
                )));
            }
            let loaded_entry = self
                .load_membership_entry(&head.body.entry)
                .await
                .map_err(map_membership_object_error)?;
            if loaded_entry.value.resolution_dependencies != head.body.resolutions {
                return Err(AnchoredChainError::LoadFailed(format!(
                    "membership head {coord:?} carries a resolution cut different from its entry"
                )));
            }
            if !self
                .validate_head_activation(&reference, &head, &loaded_entry.value)
                .await?
            {
                if cursor == Some(&reference) {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership cursor names an unactivated Store-bound head".to_string(),
                    ));
                }
                break;
            }
            for resolution_ref in &head.body.resolutions {
                if !resolutions.contains_key(resolution_ref) {
                    let resolution = self
                        .load_membership_resolution(resolution_ref)
                        .await
                        .map_err(map_membership_object_error)?
                        .value;
                    resolutions.insert(resolution_ref.clone(), resolution);
                }
            }
            if cursor == Some(&reference) {
                reached_cursor = true;
            }
            entries.push((coord, loaded_entry.value));
            heads.push((reference.clone(), head.clone()));
            predecessor = Some(reference);
            slot = head.body.successor.next_slot.clone();
            expected_sequence = expected_sequence.checked_add(1).ok_or_else(|| {
                AnchoredChainError::LoadFailed("membership head sequence overflow".to_string())
            })?;
        }
        if !reached_cursor {
            return Err(AnchoredChainError::LoadFailed(
                "membership head successor chain regressed below its durable cursor".to_string(),
            ));
        }
        Ok(ExactMembershipStream {
            entries,
            heads,
            resolutions,
        })
    }
}

pub(super) fn map_membership_object_error(error: StoreObjectError) -> AnchoredChainError {
    AnchoredChainError::from_store_object(error)
}
