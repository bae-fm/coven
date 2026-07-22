use super::*;

mod graph;

use graph::load_anchored_chain_at_exact_heads_with_root_impl;
pub(crate) use graph::project_anchored_chain_to_verified_store_prefix;

#[cfg(test)]
pub(super) use graph::{
    load_exact_membership_graph_objects, membership_projection_statuses,
    LoadedExactMembershipGraph, MembershipProjectionStatus,
};

pub(crate) async fn load_current_exact_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: Option<&str>,
    db: Option<&Database>,
) -> Result<MembershipChain, MembershipOpsError> {
    let _membership_load = match db {
        Some(db) => Some(db.lock_membership_load().await),
        None => None,
    };
    let cursors = match db {
        Some(db) => read_head_cursors(db)
            .await
            .map_err(MembershipOpsError::Database)?,
        None => Vec::new(),
    };
    let chain = Box::pin(load_exact_anchored_chain(
        storage,
        root,
        &cursors,
        owner_pubkey,
    ))
    .await?;
    if let Some(db) = db {
        persist_head_cursors(db, chain.head_refs())
            .await
            .map_err(MembershipOpsError::Database)?;
    }
    Ok(chain)
}

/// Why loading an anchored chain failed.
#[derive(Debug, thiserror::Error)]
pub enum AnchoredChainError {
    /// A cloud read required to decide the committed membership prefix was
    /// unavailable. Pull callers retain their position and retry this case.
    #[error("membership storage unavailable while {operation}: {source}")]
    StorageUnavailable {
        operation: String,
        #[source]
        source: StorageError,
    },
    /// The entries didn't download, parse, or validate into a well-formed chain.
    #[error("membership chain failed to load/validate: {0}")]
    LoadFailed(String),
    /// The chain is well-formed but its founder is not the store's pinned owner.
    #[error("chain founder {founder:?} is not the pinned owner {owner}")]
    FounderMismatch {
        founder: Option<String>,
        owner: String,
    },
}

/// The membership role a signed control author must currently hold.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MembershipAuthorRequirement {
    /// The author must be a current Owner.
    Owner,
    /// The author must be a current Owner or Member.
    WriteCapable,
}

impl MembershipAuthorRequirement {
    fn permits(self, chain: &MembershipChain, author_pubkey: &str) -> bool {
        match self {
            MembershipAuthorRequirement::Owner => chain.is_owner_now(author_pubkey),
            MembershipAuthorRequirement::WriteCapable => chain.can_write_now(author_pubkey),
        }
    }

    fn denial_message(self, author_pubkey: &str) -> String {
        match self {
            MembershipAuthorRequirement::Owner => {
                format!("author {author_pubkey} is not a current owner")
            }
            MembershipAuthorRequirement::WriteCapable => {
                format!("author {author_pubkey} is not a current write-capable member")
            }
        }
    }
}

/// Authorize an already-loaded membership chain. `None` exists for
/// pre-initialization bootstrap callers; an initialized sync session always has
/// an owner-anchored chain.
pub(crate) fn authorize_loaded_membership_author(
    chain: Option<&MembershipChain>,
    author_pubkey: &str,
    requirement: MembershipAuthorRequirement,
) -> Result<(), String> {
    let Some(chain) = chain else {
        debug!(
            author = %author_pubkey,
            required_role = ?requirement,
            "membership author authorization skipped before membership initialization"
        );
        return Ok(());
    };

    if !requirement.permits(chain, author_pubkey) {
        return Err(requirement.denial_message(author_pubkey));
    }

    Ok(())
}

struct ExactMembershipStream {
    entries: Vec<(MembershipCoord, MembershipEntry)>,
    heads: Vec<(MembershipHeadRef, AuthorHead)>,
    resolutions: BTreeMap<StoreMembershipConflictResolutionRef, StoreMembershipConflictResolution>,
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
                crate::sync::membership::StoreMembershipRoleGrant::Owner { .. }
            ) || retirement_barriers.values().any(|barrier| {
                matches!(
                    barrier,
                    crate::sync::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                )
            })
        }
        MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } => retirement_barriers.values().any(|barrier| {
            matches!(
                barrier,
                crate::sync::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
            )
        }),
        MembershipChange::ResolutionActivation { .. } => true,
    }
}

async fn validate_membership_head_activation(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &MembershipHeadRef,
    head: &AuthorHead,
    entry: &MembershipEntry,
    verified_activations: Option<
        &crate::sync::store_engine::merge::pull::VerifiedMergeMembershipPrefix,
    >,
) -> Result<bool, AnchoredChainError> {
    match (
        membership_entry_requires_store_activation(entry),
        &head.activation,
    ) {
        (false, crate::sync::membership::MembershipHeadActivation::Direct) => Ok(true),
        (true, crate::sync::membership::MembershipHeadActivation::StoreCommit { commit }) => {
            if let Some(verified_activations) = verified_activations {
                let activation = verified_activations
                    .head_activation(commit)
                    .ok_or_else(|| {
                        AnchoredChainError::LoadFailed(
                            "membership head activation is absent from its verified Store prefix"
                                .to_string(),
                        )
                    })?;
                if !activation.verifies(reference, head, commit) {
                    return Err(AnchoredChainError::LoadFailed(
                        "membership head differs from its verified Store activation".to_string(),
                    ));
                }
                return Ok(true);
            }
            Box::pin(
                crate::sync::store_engine::merge::pull::verify_merge_membership_head_activation(
                    storage, root, reference, head, commit,
                ),
            )
            .await
            .map_err(AnchoredChainError::LoadFailed)
        }
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

async fn traverse_exact_membership_stream(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    author: &str,
    grant: &MembershipGrantId,
    stream_id: crate::sync::membership::AuthorStreamId,
    anchor: &GrantStreamAnchor,
    cursor: Option<&MembershipHeadRef>,
) -> Result<ExactMembershipStream, AnchoredChainError> {
    let GrantStreamAnchor::StoreMembership { first_slot } = anchor else {
        return Err(AnchoredChainError::LoadFailed(
            "membership stream uses a recovery anchor".to_string(),
        ));
    };
    let context = ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreMembershipHead,
    );
    let mut slot = first_slot.clone();
    let mut expected_sequence = 1_u64;
    let mut predecessor: Option<MembershipHeadRef> = None;
    let mut entries = Vec::new();
    let mut heads = Vec::new();
    let mut resolutions = BTreeMap::new();
    let mut reached_cursor = cursor.is_none();

    loop {
        let semantic_prefix = slot.logical_key().strip_suffix(".json").ok_or_else(|| {
            AnchoredChainError::LoadFailed(
                "membership head exact slot has no .json suffix".to_string(),
            )
        })?;
        let (bytes, object) = match storage
            .read_protocol_slot(&context, &slot, semantic_prefix)
            .await
        {
            Ok(value) => value,
            Err(StorageError::NotFound(_)) => break,
            Err(source) if source.is_transport() => {
                return Err(AnchoredChainError::StorageUnavailable {
                    operation: format!(
                        "read membership head {author}/{grant}/{stream_id}/{expected_sequence}"
                    ),
                    source,
                })
            }
            Err(error) => return Err(AnchoredChainError::LoadFailed(error.to_string())),
        };
        let head: AuthorHead = serde_json::from_slice(&bytes)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        let coord = head.entry_coord();
        if coord.author_pubkey != author
            || coord.author_owner_grant != *grant
            || coord.stream_id != stream_id
            || coord.seq != expected_sequence
        {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head at sequence {expected_sequence} selects coordinate {coord:?}"
            )));
        }
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
        let registration = crate::sync::store_objects::load_registration_ref(
            storage,
            root,
            &head.body.author_registration,
        )
        .await
        .map_err(map_membership_object_error)?
        .value;
        if registration.author_pubkey != author
            || !head.verify(&registration)
            || head.body.successor.activation
                != crate::sync::store_commit::StreamActivation::grant_authorized(
                    root.store_root_hash,
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
        let loaded_entry = crate::sync::store_objects::load_membership_entry_ref(
            storage,
            root.store_root_hash,
            &head.body.entry,
        )
        .await
        .map_err(map_membership_object_error)?;
        if loaded_entry.value.resolution_dependencies != head.body.resolutions {
            return Err(AnchoredChainError::LoadFailed(format!(
                "membership head {coord:?} carries a resolution cut different from its entry"
            )));
        }
        if !validate_membership_head_activation(
            storage,
            root,
            &reference,
            &head,
            &loaded_entry.value,
            None,
        )
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
                let resolution = crate::sync::store_objects::load_membership_resolution_ref(
                    storage,
                    root.store_root_hash,
                    resolution_ref,
                )
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

pub(crate) async fn load_exact_anchored_chain(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    cursors: &[MembershipHeadRef],
    owner_pubkey: Option<&str>,
) -> Result<MembershipChain, AnchoredChainError> {
    let root_value = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    if let Some(owner) = owner_pubkey {
        if root_value.descriptor.founder_pubkey != owner {
            return Err(AnchoredChainError::FounderMismatch {
                founder: Some(root_value.descriptor.founder_pubkey),
                owner: owner.to_string(),
            });
        }
    }
    let crate::sync::store_commit::StoreMembershipGenesis::MergeConcurrent {
        founder_membership: anchor,
    } = &root_value.descriptor.membership
    else {
        return Err(AnchoredChainError::LoadFailed(
            "Serial Store has no Merge membership stream".to_string(),
        ));
    };
    let founder_stream = crate::sync::membership::derive_founder_stream_id(
        &root.store_root_id.to_string(),
        &root_value.descriptor.founder_pubkey,
    );
    let cursor = cursors.iter().find(|cursor| {
        cursor.coord.author_pubkey == root_value.descriptor.founder_pubkey
            && cursor.coord.author_owner_grant == root_value.descriptor.founder_grant
            && cursor.coord.stream_id == founder_stream
    });
    let founder_loaded = Box::pin(traverse_exact_membership_stream(
        storage,
        root,
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
    let mut discovered = std::collections::BTreeSet::from([founder_latest.0.coord.stream_key()]);
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
        let chain = Box::pin(load_anchored_chain_at_exact_heads(
            storage,
            root,
            &root_value.descriptor.founder_pubkey,
            &exact_heads,
            &resolution_refs,
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
            let loaded = Box::pin(traverse_exact_membership_stream(
                storage,
                root,
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

pub(crate) async fn load_exact_membership_head(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    reference: &MembershipHeadRef,
) -> Result<AuthorHead, AnchoredChainError> {
    let root_value = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    Box::pin(load_exact_membership_head_with_root(
        storage,
        root,
        &root_value,
        reference,
    ))
    .await
}

type ExactMembershipHeadFuture<'a> =
    Pin<Box<dyn Future<Output = Result<AuthorHead, AnchoredChainError>> + Send + 'a>>;

fn load_exact_membership_head_with_root<'a>(
    storage: &'a dyn SyncStorage,
    root: &'a StoreRootRef,
    root_value: &'a crate::sync::store_commit::StoreProtocolRoot,
    reference: &'a MembershipHeadRef,
) -> ExactMembershipHeadFuture<'a> {
    Box::pin(async move {
        let coord = &reference.coord;
        let context = ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        let semantic_prefix = reference
            .object
            .slot()
            .logical_key()
            .strip_suffix(".json")
            .ok_or_else(|| {
                AnchoredChainError::LoadFailed(
                    "membership head exact slot has no .json suffix".to_string(),
                )
            })?;
        let bytes = storage
            .read_protocol_object(&context, &reference.object, semantic_prefix)
            .await
            .map_err(|source| AnchoredChainError::StorageUnavailable {
                operation: format!("read exact membership head {coord:?}"),
                source,
            })?;
        let head: AuthorHead = serde_json::from_slice(&bytes)
            .map_err(|error| AnchoredChainError::LoadFailed(error.to_string()))?;
        if head.entry_coord() != *coord || head.head_hash() != reference.head_hash {
            return Err(AnchoredChainError::LoadFailed(
                "membership head differs from its exact reference".to_string(),
            ));
        }
        let registration = Box::pin(crate::sync::store_objects::load_registration_ref_with_root(
            storage,
            root,
            root_value,
            &head.body.author_registration,
        ))
        .await
        .map_err(map_membership_object_error)?
        .value;
        if registration.author_pubkey != coord.author_pubkey || !head.verify(&registration) {
            return Err(AnchoredChainError::LoadFailed(
                "membership head is not signed by its exact certified device".to_string(),
            ));
        }
        Ok(head)
    })
}

pub(crate) async fn load_anchored_chain_at_exact_heads(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
) -> Result<MembershipChain, AnchoredChainError> {
    let root_value = crate::sync::store_objects::load_store_protocol_root(storage, root)
        .await
        .map_err(map_membership_object_error)?
        .value;
    Box::pin(load_anchored_chain_at_exact_heads_with_root(
        storage,
        root,
        &root_value,
        owner_pubkey,
        exact_heads,
        exact_resolutions,
    ))
    .await
}

pub(crate) async fn load_anchored_chain_at_exact_heads_with_root(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &crate::sync::store_commit::StoreProtocolRoot,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
) -> Result<MembershipChain, AnchoredChainError> {
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        storage,
        root,
        root_value,
        owner_pubkey,
        exact_heads,
        exact_resolutions,
        None,
        None,
    ))
    .await
}

pub(crate) async fn load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    root_value: &crate::sync::store_commit::StoreProtocolRoot,
    owner_pubkey: &str,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
    verified_activations: &crate::sync::store_engine::merge::pull::VerifiedMergeMembershipPrefix,
    pending_resolution: Option<
        &crate::sync::store_engine::merge::pull::VerifiedMergeConflictResolutionActivation,
    >,
) -> Result<MembershipChain, AnchoredChainError> {
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        storage,
        root,
        root_value,
        owner_pubkey,
        exact_heads,
        exact_resolutions,
        Some(verified_activations),
        pending_resolution,
    ))
    .await
}

pub(super) fn map_membership_object_error(error: StoreObjectError) -> AnchoredChainError {
    match error {
        StoreObjectError::Storage(source @ StorageError::Storage(_))
        | StoreObjectError::Storage(source @ StorageError::RotationPending(_)) => {
            AnchoredChainError::StorageUnavailable {
                operation: "discovering immutable membership objects".to_string(),
                source,
            }
        }
        error => AnchoredChainError::LoadFailed(error.to_string()),
    }
}
