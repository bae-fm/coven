//! Verification and materialization of Store-activated Circle state.

use std::collections::BTreeSet;

use crate::database::StoreDatabase;
use crate::protocol::circle::{
    circle_epoch_close_intent_semantic_prefix, circle_semantic_prefix, recipient_slot_with_peer,
    verify_circle_semantic_prefix, AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf,
    CircleControl, CircleControlCoord, CircleControlState, CircleEpochCloseId, CircleId,
    CircleMetadataHeadRef, CircleRosterHeadRef, CircleSemanticSlot, MergeCircleOwnerAuthorityRef,
    PreparedAccessLeaf, PreparedCircleControl, ResolvedCircleRoster,
};
use crate::protocol::objects::{ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain};
use crate::protocol::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    CircleAccessObjectRef, CircleActivationObjects, GrantStreamAnchor, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreRootRef, StreamActivation, StreamActivationId, VerifiedStoreBatchCommit,
};
use crate::storage::SyncStorage;
use crate::sync::store::circle_controls::CircleOperationError;
use coven_keys::encryption::{EncryptionService, MasterKeyring};
use coven_keys::keys::{self, UserKeypair};

mod metadata;
mod roster;

mod access;
mod epoch_close;
mod heads;

pub(crate) use access::LocalCircleAccess;
use heads::{CircleHeadKind, CircleHeadValue};

#[cfg(test)]
use crate::protocol::circle_activation::CircleCurrentState;
#[cfg(test)]
use crate::sync::store::circle_controls::activation::CircleCurrentControl;
use crate::sync::store::circle_controls::activation::{
    read_exact_circle_object, verify_control_context_for_verified_commit,
};
use crate::sync::store::circle_controls::activation::{
    LocalCircleExclusion, VerifiedCircleAccess, VerifiedCircleActivations, VerifiedCircleActive,
    VerifiedCircleImage, VerifiedCircleReference, VerifiedStreamActivationPrefix,
    VerifiedStreamActivations,
};

pub(crate) struct CircleActivationVerifier<'operation, 'storage> {
    database: &'operation StoreDatabase,
    storage: &'storage dyn SyncStorage,
    history:
        &'operation mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<'storage>,
}

impl<'operation, 'storage> CircleActivationVerifier<'operation, 'storage> {
    pub(crate) fn new(
        database: &'operation StoreDatabase,
        storage: &'storage dyn SyncStorage,
        history: &'operation mut crate::sync::store::owner::verified_history::MergeHistoryVerifier<
            'storage,
        >,
    ) -> Self {
        Self {
            database,
            storage,
            history,
        }
    }

    fn root(&self) -> &StoreRootRef {
        self.history.verified_root().reference()
    }

    async fn verify_control_membership(
        &mut self,
        control: &PreparedCircleControl,
    ) -> Result<Vec<(String, crate::protocol::membership::MemberRole)>, CircleOperationError> {
        let state = &control.value.access_epoch().store_membership;
        let chain = self
            .history
            .load_membership_at_exact_heads(&state.heads, &state.resolutions)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        verify_loaded_control_membership(control, chain)
    }

    async fn verify_control_membership_at_verified_prefix(
        &self,
        control: &PreparedCircleControl,
        verified_activations: &crate::sync::store::owner::verified_history::VerifiedMergeMembershipPrefix,
    ) -> Result<Vec<(String, crate::protocol::membership::MemberRole)>, CircleOperationError> {
        let state = &control.value.access_epoch().store_membership;
        let chain = self
            .history
            .load_membership_at_verified_prefix(
                &state.heads,
                &state.resolutions,
                verified_activations,
                None,
            )
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
        verify_loaded_control_membership(control, chain)
    }

    pub(crate) async fn load_control_roster_chain(
        &mut self,
        verified: &VerifiedStoreBatchCommit,
        reference: &crate::protocol::store_commit::CircleControlRef,
        control: &PreparedCircleControl,
        keyring: &str,
    ) -> Result<crate::protocol::circle::CircleRosterChain, CircleOperationError> {
        verify_control_context_for_verified_commit(reference, control, verified)?;
        let commit_ref = verified.reference();
        let commit = verified.value();
        let encryption =
            EncryptionService::from(MasterKeyring::from_serialized(keyring).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse Circle authoring access keyring: {error}"
                ))
            })?);
        let mut consumed_stream_activations = BTreeSet::new();
        self.load_circle_roster_chain(
            &VerifiedStreamActivationPrefix::empty(),
            commit_ref,
            commit,
            reference.circle_id(),
            &control.value.state().access_epoch().roster,
            encryption,
            reference.objects(),
            &mut consumed_stream_activations,
        )
        .await
    }
}

/// The verified epoch-close settlement a successor activation carries: the exact
/// close it finalizes and the device registrations the Owner excluded. Derived
/// only from the verified outcome, never from unverified storage.
struct VerifiedCloseOutcome {
    close_id: CircleEpochCloseId,
    exclusions: Vec<StoreDeviceRegistrationRef>,
}

fn verify_loaded_control_membership(
    control: &PreparedCircleControl,
    chain: crate::protocol::membership::MembershipChain,
) -> Result<Vec<(String, crate::protocol::membership::MemberRole)>, CircleOperationError> {
    let state = &control.value.access_epoch().store_membership;
    if !chain.authorizes_write_authority(
        &control.value.value.membership_authority,
        &control.value.author_pubkey,
    ) {
        return Err(CircleOperationError::InvalidState(
            "Store membership does not authorize circle control author".to_string(),
        ));
    }
    let membership_state_hash = match chain.status() {
        crate::protocol::membership::MembershipStatus::Resolved(resolved) => resolved.state_hash,
        crate::protocol::membership::MembershipStatus::Conflict(_) => {
            return Err(CircleOperationError::InvalidState(
                "Store membership state has an unresolved conflict".to_string(),
            ));
        }
    };
    let expected_state = crate::protocol::circle::StoreMembershipStateRef::from_parts(
        state.heads.clone(),
        state.resolutions.clone(),
        state.recovery.clone(),
        membership_state_hash,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if expected_state != control.value.store_membership_state_ref() {
        return Err(CircleOperationError::InvalidState(
            "circle control Store membership state reference is invalid".to_string(),
        ));
    }
    Ok(chain.current_members())
}

fn verify_merge_circle_owner_authority(
    author_pubkey: &str,
    authority: &MergeCircleOwnerAuthorityRef,
    roster: &ResolvedCircleRoster,
) -> bool {
    match authority {
        MergeCircleOwnerAuthorityRef::Roster {
            grant_id,
            created_at,
            ..
        } => roster.authorizes_owner_grant(author_pubkey, grant_id, created_at),
        MergeCircleOwnerAuthorityRef::ConflictResolution {
            conflict_hash,
            resolution_hash,
        } => {
            let grant_id = crate::protocol::circle_roster::derive_circle_resolution_grant(
                conflict_hash,
                author_pubkey,
            );
            roster.authorizes_resolution_grant(
                author_pubkey,
                &grant_id,
                &crate::protocol::circle_roster::CircleRosterConflictResolutionRef {
                    conflict_hash: *conflict_hash,
                    resolver_pubkey: author_pubkey.to_string(),
                    resolution_hash: *resolution_hash,
                },
            )
        }
    }
}

#[cfg(test)]
mod tests;
