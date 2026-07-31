use crate::protocol::membership::{
    validate_membership_floor, AuthorHead, MembershipChain, MembershipChange, MembershipCoord,
    MembershipEntry, MembershipGrantId, MembershipHeadRef, StoreMembershipConflictResolution,
    StoreMembershipConflictResolutionRef,
};
use crate::protocol::store_commit::{GrantStreamAnchor, StoreRootRef};
use crate::storage::StorageError;
use crate::storage::StoreObjectError;
use crate::sync::store::membership::AnchoredChainError;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::pin::Pin;

mod graph;

use graph::load_anchored_chain_at_exact_heads_with_root_impl;

#[cfg(test)]
pub(super) async fn assert_deep_valid_predecessor_path_is_iterative(
    history: &mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'_>,
    heads: &[MembershipHeadRef],
) {
    let mut authority = MembershipActivationAuthority::History(history);
    graph::assert_deep_valid_predecessor_path_is_iterative(&mut authority, heads).await;
}

pub(super) async fn project_anchored_chain_to_verified_store_prefix(
    commit_verifier: &crate::sync::store::owner::StoreCommitVerifier<'_>,
    candidate_heads: &[MembershipHeadRef],
    prefix: &crate::sync::store::owner::verified_history::VerifiedMergeMembershipPrefix,
) -> Result<MembershipChain, AnchoredChainError> {
    graph::project_anchored_chain_to_verified_store_prefix(commit_verifier, candidate_heads, prefix)
        .await
}

struct ExactMembershipStream {
    entries: Vec<(MembershipCoord, MembershipEntry)>,
    heads: Vec<(MembershipHeadRef, AuthorHead)>,
    resolutions: BTreeMap<StoreMembershipConflictResolutionRef, StoreMembershipConflictResolution>,
}

pub(super) enum MembershipActivationAuthority<'operation, 'storage> {
    History(
        &'operation mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
    ),
    VerifiedPrefix {
        commit_verifier: &'operation crate::sync::store::owner::StoreCommitVerifier<'storage>,
        activations:
            &'operation crate::sync::store::owner::verified_history::VerifiedMergeMembershipPrefix,
    },
}

impl<'storage> MembershipActivationAuthority<'_, 'storage> {
    fn commit_verifier(&self) -> &crate::sync::store::owner::StoreCommitVerifier<'storage> {
        match self {
            Self::History(history) => &history.commit_verifier,
            Self::VerifiedPrefix {
                commit_verifier, ..
            } => commit_verifier,
        }
    }

    async fn load_registration(
        &self,
        reference: &crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<
        crate::storage::VerifiedObject<crate::protocol::store_commit::StoreDeviceRegistration>,
        StoreObjectError,
    > {
        self.commit_verifier().load_registration(reference).await
    }

    async fn validate_provider_admin_records(
        &self,
        entries: &[MembershipEntry],
    ) -> Result<(), AnchoredChainError> {
        for entry in entries {
            let Some(crate::protocol::provider::ProviderAdminMembershipChange {
                change:
                    crate::protocol::provider::ProviderAdminChange::Set {
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
        crate::storage::VerifiedObject<crate::protocol::store_commit::StoreDeviceRegistration>,
        StoreObjectError,
    > {
        self.commit_verifier().load_founder_registration().await
    }

    fn root(&self) -> &StoreRootRef {
        self.commit_verifier().root()
    }

    fn verified_root(&self) -> &crate::protocol::store_commit::StoreProtocolRoot {
        self.commit_verifier().verified_root()
    }

    async fn load_exact_membership_head(
        &self,
        reference: &MembershipHeadRef,
    ) -> Result<AuthorHead, AnchoredChainError> {
        self.commit_verifier()
            .membership_objects()
            .load_head(reference)
            .await
            .map(|loaded| loaded.value)
            .map_err(map_membership_object_error)
    }

    async fn load_membership_entry(
        &self,
        reference: &crate::protocol::membership::MembershipEntryRef,
    ) -> Result<crate::storage::VerifiedObject<MembershipEntry>, StoreObjectError> {
        self.commit_verifier()
            .membership_objects()
            .load_entry(reference)
            .await
    }

    async fn load_membership_resolution(
        &self,
        reference: &StoreMembershipConflictResolutionRef,
    ) -> Result<crate::storage::VerifiedObject<StoreMembershipConflictResolution>, StoreObjectError>
    {
        self.commit_verifier()
            .membership_objects()
            .load_resolution(reference)
            .await
    }

    async fn load_membership_head_at_slot(
        &self,
        slot: &crate::storage::cloud::ObjectSlot,
        author: &str,
        grant: &MembershipGrantId,
        stream_id: crate::protocol::membership::AuthorStreamId,
        sequence: u64,
    ) -> Result<crate::storage::VerifiedObject<AuthorHead>, StoreObjectError> {
        self.commit_verifier()
            .membership_objects()
            .load_head_at_slot(slot, author, grant, stream_id, sequence)
            .await
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
                crate::protocol::membership::StoreMembershipRoleGrant::Owner { .. }
            ) || retirement_barriers.values().any(|barrier| {
                matches!(
                    barrier,
                    crate::protocol::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
                )
            })
        }
        MembershipChange::RemoveMember {
            retirement_barriers,
            ..
        } => retirement_barriers.values().any(|barrier| {
            matches!(
                barrier,
                crate::protocol::membership::MergeMembershipGrantRetirementBarrier::Owner { .. }
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
        (false, crate::protocol::membership::MembershipHeadActivation::Direct) => Ok(true),
        (true, crate::protocol::membership::MembershipHeadActivation::StoreCommit { commit }) => {
            match authority {
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
                MembershipActivationAuthority::History(history) => Box::pin(
                    crate::sync::store::owner::pull::verify_merge_membership_head_activation(
                        history, reference, head, commit,
                    ),
                )
                .await
                .map_err(AnchoredChainError::LoadFailed),
            }
        }
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

async fn traverse_exact_membership_stream(
    authority: &mut MembershipActivationAuthority<'_, '_>,
    root: &StoreRootRef,
    author: &str,
    grant: &MembershipGrantId,
    stream_id: crate::protocol::membership::AuthorStreamId,
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
        let loaded = match authority
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
            != crate::protocol::store_commit::StreamActivation::grant_authorized(
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
        let loaded_entry = authority
            .load_membership_entry(&head.body.entry)
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
                let resolution = authority
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

pub(super) async fn load_exact_anchored_chain_with_history(
    history_verifier: &mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'_>,
    cursors: &[MembershipHeadRef],
    owner_pubkey: Option<&str>,
) -> Result<MembershipChain, AnchoredChainError> {
    let root = history_verifier.root().clone();
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
    let founder_stream = crate::protocol::membership::derive_founder_stream_id(
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
        &root,
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
            &root,
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
                &root,
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

pub(super) async fn load_anchored_chain_at_exact_heads_with_history(
    history_verifier: &mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'_>,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
) -> Result<MembershipChain, AnchoredChainError> {
    let root = history_verifier.root().clone();
    let root_value = history_verifier.verified_root().clone();
    let owner_pubkey = root_value.descriptor.founder_pubkey.clone();
    let mut activation_authority = MembershipActivationAuthority::History(history_verifier);
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        &mut activation_authority,
        &root,
        &root_value,
        &owner_pubkey,
        exact_heads,
        exact_resolutions,
        None,
    ))
    .await
}

pub(super) async fn load_anchored_chain_at_exact_heads_with_root_and_verified_activations(
    commit_verifier: &crate::sync::store::owner::StoreCommitVerifier<'_>,
    exact_heads: &[MembershipHeadRef],
    exact_resolutions: &[StoreMembershipConflictResolutionRef],
    verified_activations: &crate::sync::store::owner::verified_history::VerifiedMergeMembershipPrefix,
    pending_resolution: Option<
        &crate::sync::store::owner::verified_history::VerifiedMergeConflictResolutionActivation,
    >,
) -> Result<MembershipChain, AnchoredChainError> {
    let owner_pubkey = commit_verifier
        .verified_root()
        .descriptor
        .founder_pubkey
        .clone();
    let mut activation_authority = MembershipActivationAuthority::VerifiedPrefix {
        commit_verifier,
        activations: verified_activations,
    };
    Box::pin(load_anchored_chain_at_exact_heads_with_root_impl(
        &mut activation_authority,
        commit_verifier.root(),
        commit_verifier.verified_root(),
        &owner_pubkey,
        exact_heads,
        exact_resolutions,
        pending_resolution,
    ))
    .await
}

pub(super) fn map_membership_object_error(error: StoreObjectError) -> AnchoredChainError {
    AnchoredChainError::from_store_object(error)
}
