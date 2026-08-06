use super::*;
use crate::database::VerifiedMergeMembershipObjects;
use crate::storage as store_objects;
use crate::sync::store::membership::InviteError;
use crate::sync::store::owner::load_wrapped_store_key;
use crate::sync::store::owner::verification::StoreMembershipObjectVerifier;
use coven_protocol::membership::{
    self, MembershipChain, MembershipChange, MembershipEntry, MembershipError, MembershipHeadRef,
};
use coven_protocol::membership_mutation::{
    PreparedMembershipPublication, PreparedMembershipTransition,
};
use coven_protocol::objects::{
    ProtocolObjectContext, ProtocolObjectDomain, StorageError, StoreObjectError,
};
use coven_protocol::store_commit::{
    self, commit_semantic_prefix, head_slot_prefix, membership_head_slot_prefix,
    StoreBatchCommitDeletionTarget, StoreDeviceHeadRef,
};
use coven_protocol::wrapped_store_key::{PreparedWrappedStoreKey, WrappedStoreKeyRef};
use std::sync::Arc;

mod abandonment;
pub(crate) mod acknowledgements;
mod blob_lifecycle;
mod blob_preparation;
mod blob_upload;
pub(super) mod membership_mutation;
pub(super) mod membership_mutation_journal;
pub(crate) mod operations;
mod preparation;
pub(crate) mod reclaim;
pub(crate) mod snapshot;

mod commit_publication;
mod facades;
mod membership_commands;
mod membership_publication;
mod operation_test_support;
mod signing;
mod store_writes;

pub(super) use blob_preparation::close_prepared_packages;
pub(crate) use blob_preparation::prepare_partition_blob_locator;

use membership_mutation_journal::{
    decode_membership_mutation, exact_owned_remote, InviteMutationPlan, MembershipMutationPlan,
    MembershipMutationProgress, MutationPersistence, ReplacementWrappedKey, ResolveMutationPlan,
    RevokeMembershipPublication, RevokeMutationPlan,
};

pub(crate) struct MergeConflictResolutionCommitPlan {
    authorship: crate::database::store::OwnStreamAuthorship,
    writer: Arc<LocalStoreWriter>,
    root: coven_protocol::store_commit::StoreRootRef,
    coord: coven_protocol::store_commit::StoreCommitCoord,
    order: coven_protocol::store_commit::StoreCommitOrder,
    membership: coven_protocol::membership::MembershipChain,
    device_state: coven_protocol::store_commit::StoreDeviceStateRef,
    device_state_value: coven_protocol::store_commit::ResolvedStoreDeviceState,
}

impl MergeConflictResolutionCommitPlan {
    #[allow(clippy::too_many_arguments)]
    fn new(
        authorship: crate::database::store::OwnStreamAuthorship,
        writer: Arc<LocalStoreWriter>,
        root: coven_protocol::store_commit::StoreRootRef,
        coord: coven_protocol::store_commit::StoreCommitCoord,
        order: coven_protocol::store_commit::StoreCommitOrder,
        authorization: super::history::MergeConflictResolutionAuthorization,
    ) -> Self {
        Self {
            authorship,
            writer,
            root,
            coord,
            order,
            membership: authorization.membership,
            device_state: authorization.device_state_ref,
            device_state_value: authorization.device_state,
        }
    }

    pub(super) fn root(&self) -> &coven_protocol::store_commit::StoreRootRef {
        &self.root
    }

    pub(super) fn membership(&self) -> &coven_protocol::membership::MembershipChain {
        &self.membership
    }

    pub(super) fn grant_authorized_stream_id(
        &self,
        grant: &coven_protocol::membership::MembershipGrantId,
        domain: coven_protocol::store_commit::StreamAnchorDomain,
    ) -> coven_protocol::membership::AuthorStreamId {
        self.writer
            .grant_authorized_stream_id(self.root.store_root_hash, grant, domain)
    }

    pub(super) fn sign_conflict_resolution(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        selection: coven_protocol::membership::MembershipConflictSelection,
        replacement_grant: coven_protocol::membership::MembershipGrantId,
        membership: coven_protocol::store_commit::GrantStreamAnchor,
        recovery: coven_protocol::store_commit::GrantStreamAnchor,
    ) -> Result<coven_protocol::membership::StoreMembershipConflictResolution, InviteError> {
        self.writer.sign_conflict_resolution(
            chain,
            self.root.store_root_hash,
            selection,
            replacement_grant,
            membership,
            recovery,
            self.device_state.clone(),
        )
    }

    pub(super) fn sign_conflict_resolution_activation(
        &self,
        chain: &coven_protocol::membership::MembershipChain,
        stream_id: coven_protocol::membership::AuthorStreamId,
        reference: coven_protocol::membership::StoreMembershipConflictResolutionRef,
        resolution: &coven_protocol::membership::StoreMembershipConflictResolution,
        created_at: String,
    ) -> Result<
        coven_protocol::membership::MembershipEntry,
        coven_protocol::membership::MembershipError,
    > {
        self.writer.sign_conflict_resolution_activation(
            chain,
            self.root.store_root_hash,
            stream_id,
            reference,
            resolution,
            created_at,
        )
    }

    pub(super) fn finish(
        self,
        membership: &coven_protocol::membership::MembershipChain,
        resolution: &coven_protocol::membership::StoreMembershipConflictResolutionRef,
    ) -> Result<operations::StoreOperationCommitPlan, StoreError> {
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(StoreError::InvalidOutbound(
                "conflict-resolution candidate membership remains conflicted".to_string(),
            ));
        };
        if membership
            .resolution_refs()
            .binary_search(resolution)
            .is_err()
        {
            return Err(StoreError::InvalidOutbound(
                "conflict-resolution candidate membership omits its exact resolution".to_string(),
            ));
        }
        let replacement_grant = coven_protocol::membership::derive_store_resolution_grant(
            &resolution.conflict_hash,
            &resolution.resolver_pubkey,
        );
        let authority =
            coven_protocol::membership::MembershipGrantCreationAuthority::ConflictResolution(
                resolution.clone(),
            );
        if membership
            .active_grant(&replacement_grant)
            .is_none_or(|record| {
                record.member_pubkey != self.writer.author_pubkey()
                    || record.creation_authority != authority
            })
        {
            return Err(StoreError::InvalidOutbound(
                "conflict-resolution candidate is not authorized by its replacement Owner grant"
                    .to_string(),
            ));
        }
        let membership_state = coven_protocol::circle_control::StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            self.device_state.recovery().to_vec(),
            resolved.state_hash,
        )
        .map_err(|error| StoreError::InvalidOutbound(error.to_string()))?;
        let common = operations::StoreOperationPlanCommon::new(
            self.authorship,
            self.writer,
            self.root,
            self.coord,
            self.order,
            membership_state,
            self.device_state,
            coven_protocol::store_commit::StoreOperationMembershipAuthority {
                predecessor: authority,
            },
            Some(replacement_grant),
        );
        Ok(operations::StoreOperationCommitPlan::new(
            common,
            membership.clone(),
            self.device_state_value,
        ))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum StoreWriterAuthorizationError {
    #[error("Store authority: {0}")]
    StoreAuthority(SyncCycleFailure),
    #[error("Store writer registration: {0}")]
    Registration(StoreRegistrationError),
}

#[derive(Debug, thiserror::Error)]
enum AuthorizationRefreshError {
    #[error("select this device's wrapped-key authority: {0}")]
    Membership(#[source] coven_protocol::membership::MembershipError),
    #[error("read this device's wrapped key: {0}")]
    WrappedKey(#[source] crate::sync::store::membership::InviteError),
    #[error("refresh state is invalid: {0}")]
    InvalidState(String),
    #[error("rotation gate database state: {0}")]
    Database(#[source] crate::database::DbError),
    #[error("merge this device's live and selected keyrings: {0}")]
    InvalidKeyring(#[source] coven_keys::encryption::EncryptionError),
    #[error("adopt committed store-key rotation: {0}")]
    KeyAdoption(#[source] coven_keys::keys::KeyError),
}
