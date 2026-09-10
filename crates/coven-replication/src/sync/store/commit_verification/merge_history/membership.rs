use crate::sync::store::membership::AnchoredChainError;
use coven_protocol::membership::{
    validate_membership_floor, AuthorHead, MembershipChain, MembershipCoord, MembershipEntry,
    MembershipGrantId, MembershipHeadRef, StoreAuthorityChange,
};
use coven_protocol::objects::StorageError;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::store_commit::{GrantStreamAnchor, StoreRootRef};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

mod accepted_device_authority;
mod accepted_membership_authority;
pub use accepted_membership_authority::AcceptedMembershipAuthority;
mod discovery;
mod graph;
mod stream;
use accepted_device_authority::AcceptedDeviceAuthority;
use stream::LoadedHeadAcceptance;

/// One author stream as the anchored walk actually read it: every head from
/// the stream's anchor to its tip, with the entry each head selects.
///
/// This is what a published membership rollup is made of. The walk already
/// holds all of it — collecting it costs nothing and is the only place it
/// exists in one piece, because a `MembershipChain` keeps the reduced state and
/// the entries but not the heads that carried them.
pub(crate) struct TraversedMembershipStream {
    pub(crate) author_pubkey: String,
    pub(crate) author_owner_grant: MembershipGrantId,
    pub(crate) stream_id: coven_protocol::membership::AuthorStreamId,
    pub(crate) heads: Vec<(MembershipHeadRef, AuthorHead, MembershipEntry)>,
}

/// The published form of what the walk read.
pub(crate) async fn membership_rollup_streams(
    traversed: Vec<TraversedMembershipStream>,
    verifier: &crate::sync::store::commit_verification::commit::StoreCommitVerifier<'_>,
) -> Result<Vec<coven_protocol::store_commit::MembershipRollupStream>, AnchoredChainError> {
    let mut streams = Vec::new();
    for stream in traversed {
        if stream.heads.is_empty() {
            continue;
        }
        let mut heads = Vec::with_capacity(stream.heads.len());
        for (index, (reference, head, entry)) in stream.heads.iter().enumerate() {
            let predecessor_acceptance = match &head.body.predecessor {
                Some(previous) => {
                    let (_, previous_head, _) = index
                        .checked_sub(1)
                        .and_then(|previous| stream.heads.get(previous))
                        .ok_or_else(|| {
                            AnchoredChainError::LoadFailed(
                                "traversed membership predecessor is absent".into(),
                            )
                        })?;
                    previous
                        .verify_head(previous_head)
                        .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
                    match previous.acceptance() {
                        Some(object) => Some(
                            verifier
                                .membership_objects()
                                .load_head_acceptance_at(
                                    previous.head(),
                                    previous_head,
                                    Some(object),
                                )
                                .await?
                                .value,
                        ),
                        None => None,
                    }
                }
                None => None,
            };
            heads.push(coven_protocol::store_commit::MembershipRollupHead {
                entry: head.body.entry.clone(),
                head: reference.clone(),
                head_value: head.clone(),
                entry_value: entry.clone(),
                predecessor_acceptance,
            });
        }
        streams.push(coven_protocol::store_commit::MembershipRollupStream {
            author_pubkey: stream.author_pubkey,
            author_owner_grant: stream.author_owner_grant,
            stream_id: stream.stream_id,
            heads,
        });
    }
    Ok(streams)
}

struct ExactMembershipStream {
    entries: Vec<(MembershipCoord, MembershipEntry)>,
    heads: Vec<(MembershipHeadRef, AuthorHead)>,
}

type LoadedMembershipGraphFuture<'a> = Pin<
    Box<
        dyn Future<Output = Result<graph::LoadedExactMembershipGraph, AnchoredChainError>>
            + Send
            + 'a,
    >,
>;

enum MembershipActivationAuthority<'operation, 'storage> {
    History {
        history: &'operation mut crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier<
            'storage,
        >,
    },
    AcceptedHeads {
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
        commit_verifier: &'operation crate::sync::store::commit_verification::commit::StoreCommitVerifier<'storage>,
        device_authority: AcceptedDeviceAuthority,
    },
    VerifiedPrefix {
        root: crate::sync::store::protocol_root::VerifiedStoreRoot,
        commit_verifier: &'operation crate::sync::store::commit_verification::commit::StoreCommitVerifier<'storage>,
        activations:
            &'operation crate::sync::store::commit_verification::merge_history::VerifiedMergeMembershipPrefix,
    },
}

pub(super) struct AcceptedMembershipActivation<'operation, 'storage> {
    authority: MembershipActivationAuthority<'operation, 'storage>,
}

pub(super) struct HistoryMembershipActivation<'operation, 'storage> {
    authority: MembershipActivationAuthority<'operation, 'storage>,
}

pub(super) struct VerifiedPrefixMembershipActivation<'operation, 'storage> {
    authority: MembershipActivationAuthority<'operation, 'storage>,
}

impl<'operation, 'storage> AcceptedMembershipActivation<'operation, 'storage> {
    pub(super) fn new(
        root: &crate::sync::store::protocol_root::VerifiedStoreRoot,
        commit_verifier: &'operation crate::sync::store::commit_verification::commit::StoreCommitVerifier<'storage>,
    ) -> Self {
        Self {
            authority: MembershipActivationAuthority::AcceptedHeads {
                root: root.clone(),
                commit_verifier,
                device_authority: AcceptedDeviceAuthority::default(),
            },
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
            .map(|(membership, _)| membership)
    }

    pub(super) async fn load_anchored_authority(
        mut self,
        cursors: &[MembershipHeadRef],
        owner_pubkey: Option<&str>,
    ) -> Result<AcceptedMembershipAuthority, AnchoredChainError> {
        let (membership, _) = self
            .authority
            .load_exact_anchored_chain(cursors, owner_pubkey)
            .await?;
        let MembershipActivationAuthority::AcceptedHeads {
            root,
            device_authority,
            ..
        } = self.authority
        else {
            unreachable!("accepted membership authority has one construction state")
        };
        Ok(AcceptedMembershipAuthority::new(
            root.reference().clone(),
            membership,
            device_authority,
        ))
    }

    pub(super) async fn load_snapshot_membership(
        &mut self,
        snapshot: &coven_protocol::store_commit::SnapshotMeta,
    ) -> Result<MembershipChain, crate::sync::store::StorePullError> {
        let exact_heads = &snapshot.state.membership.heads;
        let predecessor = &snapshot.publication_predecessor;
        // Discover from root-owned successor slots before accepting the image's
        // chosen heads. An absent acceptance result is an unfinished authority
        // transition and must not let an older authority state authorize a cut.
        let (_, traversed) = self.authority.load_exact_anchored_chain(&[], None).await?;
        let MembershipActivationAuthority::AcceptedHeads {
            device_authority, ..
        } = &self.authority
        else {
            unreachable!("accepted membership authority has one construction state")
        };
        for selected in exact_heads {
            if !device_authority.contains(&selected.coord)
                || !traversed
                    .iter()
                    .flat_map(|stream| &stream.heads)
                    .any(|(rooted, _, _)| rooted == selected)
            {
                return Err(AnchoredChainError::LoadFailed(
                    "snapshot membership does not select an exact rooted head in accepted authority"
                        .into(),
                )
                .into());
            }
        }
        let membership = self.authority.load_at_exact_heads(exact_heads).await?;
        let MembershipActivationAuthority::AcceptedHeads {
            root,
            commit_verifier,
            device_authority,
        } = &self.authority
        else {
            unreachable!("accepted membership authority has one construction state")
        };
        for stream in &traversed {
            for (reference, head, _) in &stream.heads {
                if !device_authority.contains(&reference.coord) {
                    continue;
                }
                let coven_protocol::membership::MembershipHeadActivation::StoreCommit { .. } =
                    &head.activation
                else {
                    continue;
                };
                let accepted = device_authority.receipt(reference)?.publication()?;
                let included = membership.contains_coord(&reference.coord);
                let before_snapshot = predecessor
                    .accepted()
                    .is_some_and(|last| accepted.position <= last.position);
                if predecessor
                    .accepted()
                    .is_some_and(|last| accepted.position == last.position && accepted != last)
                    || included != before_snapshot
                {
                    return Err(AnchoredChainError::LoadFailed(
                        "snapshot membership omits or advances an independently accepted authority transition".into(),
                    ).into());
                }
            }
        }
        device_authority
            .verify_snapshot(root, commit_verifier, snapshot, &membership, &traversed)
            .await?;
        Ok(membership)
    }
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
    ) -> Result<(MembershipChain, Vec<TraversedMembershipStream>), AnchoredChainError> {
        self.authority
            .load_exact_anchored_chain(cursors, owner_pubkey)
            .await
    }

    pub(super) async fn load_at_exact_heads(
        &mut self,
        exact_heads: &[MembershipHeadRef],
    ) -> Result<MembershipChain, AnchoredChainError> {
        self.authority.load_at_exact_heads(exact_heads).await
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
            node.reference.coord = node.entry.coord();
            let head = node.head.body_mut();
            head.body.entry.coord = node.reference.coord.clone();
            head.body.predecessor = predecessor
                .clone()
                .map(|head| coven_protocol::membership::MembershipHeadPredecessor::Direct { head });
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
    ) -> Result<MembershipChain, AnchoredChainError> {
        self.authority.load_at_exact_heads(exact_heads).await
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
        let heads = graph::project_membership_cut_to_store_prefix(&candidate, &prefix)?;
        let projected = self
            .authority
            .load_anchored_chain_at_exact_heads(&heads)
            .await?;
        prefix
            .validate_complete_membership(&projected)
            .map_err(AnchoredChainError::from)?;
        Ok(projected)
    }
}

fn membership_entry_requires_store_activation(entry: &MembershipEntry) -> bool {
    !matches!(entry.change, StoreAuthorityChange::Founder { .. })
}

impl<'storage> MembershipActivationAuthority<'_, 'storage> {
    async fn load_at_exact_heads(
        &mut self,
        exact_heads: &[MembershipHeadRef],
    ) -> Result<MembershipChain, AnchoredChainError> {
        Box::pin(self.load_anchored_chain_at_exact_heads(exact_heads)).await
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
                    current = node.head.body.predecessor_head().cloned();
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
                    None,
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

    async fn load_anchored_chain_at_exact_heads(
        &mut self,
        exact_heads: &[MembershipHeadRef],
    ) -> Result<MembershipChain, AnchoredChainError> {
        validate_membership_floor(exact_heads).map_err(AnchoredChainError::InvalidFloor)?;
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
        let chain = graph::exact_membership_chain_from_graph(&root, graph, provider_admin)?;
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
            }
            | Self::AcceptedHeads {
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
                .map_err(AnchoredChainError::from)?;
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
            }
            | Self::AcceptedHeads {
                commit_verifier, ..
            } => commit_verifier.load_founder_registration().await,
        }
    }

    fn root(&self) -> &StoreRootRef {
        match self {
            Self::History { history } => history.root.reference(),
            Self::VerifiedPrefix { root, .. } | Self::AcceptedHeads { root, .. } => {
                root.reference()
            }
        }
    }

    fn verified_root(&self) -> &coven_protocol::store_commit::StoreProtocolRoot {
        match self {
            Self::History { history } => history.root.protocol(),
            Self::VerifiedPrefix { root, .. } | Self::AcceptedHeads { root, .. } => root.protocol(),
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
            }
            | Self::AcceptedHeads {
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
            }
            | Self::AcceptedHeads {
                commit_verifier, ..
            } => {
                commit_verifier
                    .membership_objects()
                    .load_entry(reference)
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
            }
            | Self::AcceptedHeads {
                commit_verifier, ..
            } => {
                commit_verifier
                    .membership_objects()
                    .load_head_at_slot(slot, author, grant, stream_id, sequence)
                    .await
            }
        }
    }

    /// Run every check `load_membership_head_at_slot` would have run, over
    /// bytes a prefetch already holds.
    async fn verify_membership_head_at_slot(
        &self,
        read: &crate::sync::store::commit_verification::commit::ReadProtocolSlot,
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
                    .verify_head_at_slot(
                        &read.bytes,
                        &read.object,
                        author,
                        grant,
                        stream_id,
                        sequence,
                    )
                    .await
            }
            Self::VerifiedPrefix {
                commit_verifier, ..
            }
            | Self::AcceptedHeads {
                commit_verifier, ..
            } => {
                commit_verifier
                    .membership_objects()
                    .verify_head_at_slot(
                        &read.bytes,
                        &read.object,
                        author,
                        grant,
                        stream_id,
                        sequence,
                    )
                    .await
            }
        }
    }

    async fn prefetch_slot_stream(
        &self,
        context: &coven_protocol::objects::ProtocolObjectContext,
        listing_prefix: &str,
        anchor_slots: Vec<coven_protocol::objects::ObjectSlot>,
    ) -> Result<crate::sync::store::commit_verification::commit::PrefetchedSlotStream, StorageError>
    {
        match self {
            Self::History { history } => {
                history
                    .commit_verifier
                    .prefetch_slot_stream(context, listing_prefix, anchor_slots)
                    .await
            }
            Self::VerifiedPrefix {
                commit_verifier, ..
            }
            | Self::AcceptedHeads {
                commit_verifier, ..
            } => {
                commit_verifier
                    .prefetch_slot_stream(context, listing_prefix, anchor_slots)
                    .await
            }
        }
    }

    /// Fetch every slot the provider holds for one author stream, and the
    /// objects those heads name, before the walk verifies any of them.
    ///
    /// A membership stream is a hash-linked list, so verification has to run in
    /// order: a head means nothing until the head before it has been checked.
    /// Fetching does not — a head's slot is named by its coordinate alone, so
    /// the whole stream shares one provider prefix, and the objects each head
    /// names are readable as soon as its bytes arrive. Following the links to
    /// discover slot k+1 from slot k's bytes therefore spends a round trip per
    /// head purely on discovery, which against a live store is where a joining
    /// device's minutes went.
    ///
    /// Nothing here is trusted. The listing only chooses what to fetch: the
    /// walk still starts at the anchor's first slot, still follows each head's
    /// signed successor link, and still runs every check on every head it
    /// reaches. A slot the walk reaches that this did not fetch is read the way
    /// it always was, so an incomplete or dishonest listing costs round trips
    /// and never truth — and the decode below is a guess at what else to
    /// fetch, so a head that fails it is simply not prefetched.
    async fn prefetch_membership_stream(
        &self,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: coven_protocol::membership::AuthorStreamId,
        first_slot: &coven_protocol::objects::ObjectSlot,
    ) -> Result<crate::sync::store::commit_verification::commit::StreamSlotReads, AnchoredChainError>
    {
        let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            self.root().store_root_hash,
            coven_protocol::objects::ProtocolObjectDomain::StoreMembershipHead,
        );
        let listing_prefix =
            coven_protocol::store_commit::membership_head_stream_prefix(author, grant, stream_id);
        // The founder's first head is written under a prefix of its own, before
        // the founder has a grant to file it under, so the anchor's slot is
        // handed in rather than found in the listing.
        let heads = self
            .prefetch_slot_stream(&context, &listing_prefix, vec![first_slot.clone()])
            .await
            .map_err(|source| {
                if source.is_transport() {
                    AnchoredChainError::StorageUnavailable {
                        operation: format!("fetch membership stream {author}/{grant}/{stream_id}"),
                        source,
                    }
                } else {
                    map_membership_object_error(StoreObjectError::Storage(source))
                }
            })?;

        if let Some(fetched) = heads.freshly_fetched() {
            let mut entries = Vec::new();
            for read in fetched.values() {
                let Ok(head) =
                    coven_protocol::objects::decode_protocol_object::<AuthorHead>(&read.bytes)
                else {
                    continue;
                };
                entries.push(head.body.entry.clone());
            }
            self.prefetch_membership_head_dependencies(entries).await;
        }
        Ok(heads.reads().clone())
    }

    /// Read each prefetched head's entry into the verifier's object memo.
    ///
    /// Content-addressed reads by reference, so a hit still runs every check
    /// the walk would have run — this decides when the bytes are fetched, not
    /// whether they are believed.
    ///
    /// Unlike the head slots, a failure here is dropped rather than reported.
    /// A head's slot is named by the stream's own coordinates, so a slot under
    /// that prefix that will not open is this Store's own object failing. These
    /// references come out of heads nothing has verified yet, so a head that
    /// names an object that is absent or corrupt would otherwise let whoever
    /// wrote it fail every reader's walk. The walk reads what it actually
    /// reaches, and fails there, on a reference it has verified.
    async fn prefetch_membership_head_dependencies(
        &self,
        entries: Vec<coven_protocol::membership::MembershipEntryRef>,
    ) {
        use futures_util::StreamExt;

        let width = crate::sync::store::commit_verification::commit::PROTOCOL_SLOT_READ_WIDTH;
        let entries = futures_util::stream::iter(entries)
            .map(|reference| async move { self.load_membership_entry(&reference).await.map(drop) })
            .buffer_unordered(width);
        for read in entries.collect::<Vec<_>>().await {
            if let Err(error) = read {
                tracing::debug!(%error, "speculative membership object read did not land");
            }
        }
    }

    async fn validate_head_activation(
        &mut self,
        reference: &MembershipHeadRef,
        head: &AuthorHead,
        entry: &MembershipEntry,
        acceptance: Option<LoadedHeadAcceptance>,
    ) -> Result<bool, AnchoredChainError> {
        match (
            membership_entry_requires_store_activation(entry),
            &head.activation,
        ) {
            (false, coven_protocol::membership::MembershipHeadActivation::Direct) => Ok(true),
            (
                true,
                coven_protocol::membership::MembershipHeadActivation::StoreCommit {
                    commit, ..
                },
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
                MembershipActivationAuthority::AcceptedHeads {
                    commit_verifier,
                    device_authority,
                    ..
                } => {
                    device_authority
                        .observe(commit_verifier, reference, head, entry, acceptance)
                        .await
                }
                MembershipActivationAuthority::History { history, .. } => {
                    Box::pin(history.verify_membership_head_activation(reference, head, commit))
                        .await
                        .map_err(AnchoredChainError::from)
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
}

/// Pair each head a traversal read with the entry it selected. The walk pushes
/// both in step, so index `k` of each is one membership change.
fn zip_traversed_heads(
    loaded: &ExactMembershipStream,
) -> Vec<(MembershipHeadRef, AuthorHead, MembershipEntry)> {
    loaded
        .heads
        .iter()
        .zip(loaded.entries.iter())
        .map(|((reference, head), (_, entry))| (reference.clone(), head.clone(), entry.clone()))
        .collect()
}

pub(super) fn map_membership_object_error(error: StoreObjectError) -> AnchoredChainError {
    AnchoredChainError::from_store_object(error)
}
