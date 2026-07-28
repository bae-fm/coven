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
    database: Option<&StoreDatabase>,
) -> Result<MembershipChain, MembershipOpsError> {
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, root)
        .await
        .map_err(map_membership_history_error)?;
    load_current_exact_chain_with_history(&mut history_verifier, owner_pubkey, database).await
}

pub(crate) async fn load_current_exact_chain_with_history(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    owner_pubkey: Option<&str>,
    database: Option<&StoreDatabase>,
) -> Result<MembershipChain, MembershipOpsError> {
    let _membership_load = match database {
        Some(database) => Some(database.lock_membership_load().await),
        None => None,
    };
    let cursors = match database {
        Some(database) => read_head_cursors(database.sqlite())
            .await
            .map_err(MembershipOpsError::Database)?,
        None => Vec::new(),
    };
    let chain = Box::pin(load_exact_anchored_chain_with_history(
        history_verifier,
        &cursors,
        owner_pubkey,
    ))
    .await?;
    if let Some(database) = database {
        persist_head_cursors(database.sqlite(), chain.head_refs())
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
}

impl MembershipAuthorRequirement {
    fn permits(self, chain: &MembershipChain, author_pubkey: &str) -> bool {
        match self {
            MembershipAuthorRequirement::Owner => chain.is_owner_now(author_pubkey),
        }
    }

    fn denial_message(self, author_pubkey: &str) -> String {
        match self {
            MembershipAuthorRequirement::Owner => {
                format!("author {author_pubkey} is not a current owner")
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

pub(super) enum MembershipActivationAuthority<'operation, 'storage> {
    History(&'operation mut crate::sync::store::pull::MergeHistoryVerifier<'storage>),
    VerifiedPrefix(&'operation crate::sync::store::pull::VerifiedMergeMembershipPrefix),
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
    authority: &mut MembershipActivationAuthority<'_, '_>,
    reference: &MembershipHeadRef,
    head: &AuthorHead,
    entry: &MembershipEntry,
) -> Result<bool, AnchoredChainError> {
    match (
        membership_entry_requires_store_activation(entry),
        &head.activation,
    ) {
        (false, crate::sync::membership::MembershipHeadActivation::Direct) => Ok(true),
        (true, crate::sync::membership::MembershipHeadActivation::StoreCommit { commit }) => {
            match authority {
                MembershipActivationAuthority::VerifiedPrefix(verified_activations) => {
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
                MembershipActivationAuthority::History(history) => Box::pin(
                    crate::sync::store::pull::verify_merge_membership_head_activation(
                        history, reference, head, commit,
                    ),
                )
                .await
                .map_err(AnchoredChainError::LoadFailed),
            }
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
    authority: &mut MembershipActivationAuthority<'_, '_>,
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
        let head = parse_membership_head_off_stack(bytes, semantic_prefix, &object).await?;
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
        if !validate_membership_head_activation(authority, &reference, &head, &loaded_entry.value)
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
    let mut history_verifier = crate::sync::store::pull::MergeHistoryVerifier::new(storage, root)
        .await
        .map_err(map_membership_history_error)?;
    load_exact_anchored_chain_with_history(&mut history_verifier, cursors, owner_pubkey).await
}

pub(crate) async fn load_exact_anchored_chain_with_history(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    cursors: &[MembershipHeadRef],
    owner_pubkey: Option<&str>,
) -> Result<MembershipChain, AnchoredChainError> {
    let storage = history_verifier.storage();
    let root = history_verifier.root();
    let root_value = history_verifier.verified_root().clone();
    let mut activation_authority = MembershipActivationAuthority::History(history_verifier);
    if let Some(owner) = owner_pubkey {
        if root_value.descriptor.founder_pubkey != owner {
            return Err(AnchoredChainError::FounderMismatch {
                founder: Some(root_value.descriptor.founder_pubkey),
                owner: owner.to_string(),
            });
        }
    }
    let anchor = &root_value.descriptor.founder_membership;
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
        &mut activation_authority,
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
        let chain = Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
            &mut activation_authority,
            storage,
            root,
            &root_value,
            &root_value.descriptor.founder_pubkey,
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
            let loaded = Box::pin(traverse_exact_membership_stream(
                &mut activation_authority,
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

#[cfg(test)]
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

pub(crate) async fn load_exact_membership_head_with_history(
    history_verifier: &crate::sync::store::pull::MergeHistoryVerifier<'_>,
    reference: &MembershipHeadRef,
) -> Result<AuthorHead, AnchoredChainError> {
    load_exact_membership_head_with_root(
        history_verifier.storage(),
        history_verifier.root(),
        history_verifier.verified_root(),
        reference,
    )
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
        let head =
            parse_membership_head_off_stack(bytes, semantic_prefix, &reference.object).await?;
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

pub(crate) async fn load_anchored_chain_at_exact_heads_with_history(
    history_verifier: &mut crate::sync::store::pull::MergeHistoryVerifier<'_>,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
) -> Result<MembershipChain, AnchoredChainError> {
    let storage = history_verifier.storage();
    let root = history_verifier.root();
    let root_value = history_verifier.verified_root().clone();
    let owner_pubkey = root_value.descriptor.founder_pubkey.clone();
    let mut activation_authority = MembershipActivationAuthority::History(history_verifier);
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        &mut activation_authority,
        storage,
        root,
        &root_value,
        &owner_pubkey,
        exact_heads,
        exact_resolutions,
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
    verified_activations: &crate::sync::store::pull::VerifiedMergeMembershipPrefix,
    pending_resolution: Option<
        &crate::sync::store::pull::VerifiedMergeConflictResolutionActivation,
    >,
) -> Result<MembershipChain, AnchoredChainError> {
    let mut activation_authority =
        MembershipActivationAuthority::VerifiedPrefix(verified_activations);
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        &mut activation_authority,
        storage,
        root,
        root_value,
        owner_pubkey,
        exact_heads,
        exact_resolutions,
        pending_resolution,
    ))
    .await
}

/// Deserialize an exact membership head off the async poll stack.
///
/// This runs at the leaves of the nested membership-load chain, where the
/// accumulated async future frames leave little of the default thread stack for
/// serde's monomorphized deserializer of the large `AuthorHead` graph. Running
/// the parse on a fresh blocking-pool stack keeps the deep async descent and the
/// heavy deserialization off the same stack — the same bounded-stack path
/// [`load_exact_object`](crate::sync::store_objects::load_exact_object) uses for
/// entries and heads loaded by reference.
async fn parse_membership_head_off_stack(
    bytes: Vec<u8>,
    semantic_prefix: &str,
    object: &crate::sync::storage::ExactObjectRef,
) -> Result<AuthorHead, AnchoredChainError> {
    crate::sync::store_objects::run_blocking_object_verification(
        semantic_prefix,
        object,
        Box::new(move || {
            serde_json::from_slice::<AuthorHead>(&bytes).map_err(|error| {
                crate::sync::store_commit::StoreProtocolError::Malformed(error.to_string())
            })
        }),
    )
    .await
    .map_err(map_membership_object_error)
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

pub(super) fn map_membership_history_error(
    error: crate::sync::store::pull::StorePullError,
) -> AnchoredChainError {
    match error {
        crate::sync::store::pull::StorePullError::Object(error) => {
            map_membership_object_error(error)
        }
        crate::sync::store::pull::StorePullError::Storage(source) if source.is_transport() => {
            AnchoredChainError::StorageUnavailable {
                operation: "authenticating membership Store history".to_string(),
                source,
            }
        }
        crate::sync::store::pull::StorePullError::Storage(error) => {
            AnchoredChainError::LoadFailed(error.to_string())
        }
        error => AnchoredChainError::LoadFailed(error.to_string()),
    }
}
