//! Verification and materialization of Store-activated Circle state.

use std::collections::BTreeSet;

use super::CircleOperationError;
use crate::encryption::{EncryptionService, MasterKeyring};
use crate::keys::{self, UserKeypair};
use crate::sync::circle::{
    circle_epoch_close_intent_semantic_prefix, circle_semantic_prefix, recipient_slot_with_peer,
    verify_circle_semantic_prefix, AccessEnvelope, CircleAccessDisposition, CircleAccessLeaf,
    CircleControl, CircleControlCoord, CircleControlState, CircleEpochCloseId, CircleId,
    CircleMetadataHeadRef, CircleRosterHeadRef, CircleSemanticSlot, MergeCircleOwnerAuthorityRef,
    PreparedAccessLeaf, PreparedCircleControl, ResolvedCircleRoster,
};
use crate::sync::circle_roster::CircleMaterializedRoster;
use crate::sync::storage::{
    ExactObjectRef, ProtocolObjectContext, ProtocolObjectDomain, SyncStorage,
};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    circle_access_envelope_semantic_prefix, circle_access_leaf_semantic_prefix,
    CircleAccessObjectRef, CircleActivationObjects, GrantStreamAnchor, ObjectHash,
    StoreBatchCommit, StoreBatchCommitRef, StoreDeviceRegistration, StoreDeviceRegistrationRef,
    StoreRootRef, StreamActivation, StreamActivationId, VerifiedStoreBatchCommit,
};

mod context;
mod metadata;
mod roster;
mod state;

pub(crate) use context::{read_exact_circle_object, verify_control_context_for_verified_commit};
use context::{verify_control_membership, verify_control_membership_with_verified_activations};
use metadata::load_circle_metadata_state;
use roster::{load_circle_authority_roster, load_circle_roster_chain, load_circle_roster_state};
#[cfg(test)]
use state::CircleCurrentControl;
pub(crate) use state::{
    CircleAuthoringState, CircleCurrentState, CirclePackageAccess, LocalCircleExclusion,
    VerifiedCircleAccess, VerifiedCircleActivations, VerifiedCircleActive, VerifiedCircleImage,
    VerifiedCircleReference, VerifiedStreamActivationPrefix, VerifiedStreamActivations,
};

/// The verified epoch-close settlement a successor activation carries: the exact
/// close it finalizes and the device registrations the Owner excluded. Derived
/// only from the verified outcome, never from unverified storage.
struct VerifiedCloseOutcome {
    close_id: CircleEpochCloseId,
    exclusions: Vec<StoreDeviceRegistrationRef>,
}

struct VerifiedAccessPair {
    reference: CircleAccessObjectRef,
    envelope: AccessEnvelope,
    leaf_bytes: Vec<u8>,
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn load_circle_control_roster_chain(
    database: &StoreDatabase,
    commit_verifier: &mut crate::sync::store::owner::pull::StoreCommitVerifier<'_>,
    verified: &VerifiedStoreBatchCommit,
    reference: &crate::sync::store_commit::CircleControlRef,
    control: &PreparedCircleControl,
    keyring: &str,
) -> Result<crate::sync::circle::CircleRosterChain, CircleOperationError> {
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
    load_circle_roster_chain(
        database,
        commit_verifier,
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

async fn load_verified_access_pairs(
    storage: &dyn SyncStorage,
    commit: &StoreBatchCommit,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
) -> Result<Vec<VerifiedAccessPair>, CircleOperationError> {
    let family = commit.candidate_family();
    let mut verified = Vec::with_capacity(objects.access.len());
    for reference in &objects.access {
        if reference.leaf.owner_pubkey != reference.envelope.owner_pubkey
            || reference.leaf.recipient_slot != reference.envelope.recipient_slot
            || reference.leaf.leaf_id != reference.envelope.leaf_id
            || reference.leaf.leaf_hash != reference.envelope.leaf_hash
            || reference.leaf.leaf_hash != reference.leaf.object.stored_hash()
            || reference.envelope.control_hash != control.coord.control_hash()
        {
            return Err(CircleOperationError::InvalidState(
                "paired Circle access references differ".to_string(),
            ));
        }
        let envelope_prefix = circle_access_envelope_semantic_prefix(
            circle_id,
            family,
            &reference.envelope.owner_pubkey,
            &reference.envelope.recipient_slot,
            reference.envelope.control_hash,
        );
        let envelope_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessEnvelope,
            ),
            &reference.envelope.object,
            &envelope_prefix,
        )
        .await?;
        let envelope: AccessEnvelope =
            serde_json::from_slice(&envelope_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse circle access envelope: {error}"))
            })?;
        if envelope.candidate_family != family
            || envelope.circle_id != circle_id
            || envelope.owner_pubkey != reference.envelope.owner_pubkey
            || envelope.recipient_slot != reference.envelope.recipient_slot
            || envelope.control_hash != reference.envelope.control_hash
            || envelope.leaf_id != reference.envelope.leaf_id
            || envelope.leaf_hash != reference.envelope.leaf_hash
            || !envelope.verify(control, family)
        {
            return Err(CircleOperationError::InvalidState(
                "circle access envelope failed verification".to_string(),
            ));
        }
        let leaf_prefix = circle_access_leaf_semantic_prefix(
            circle_id,
            family,
            &reference.leaf.owner_pubkey,
            reference.leaf.epoch_id,
            &reference.leaf.recipient_slot,
            reference.leaf.leaf_id,
        );
        let leaf_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::recipient_sealed(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleAccessLeaf,
            ),
            &reference.leaf.object,
            &leaf_prefix,
        )
        .await?;
        if ObjectHash::digest(&leaf_bytes) != reference.leaf.leaf_hash {
            return Err(CircleOperationError::InvalidState(
                "Circle access leaf bytes differ from the paired leaf hash".to_string(),
            ));
        }
        verified.push(VerifiedAccessPair {
            reference: reference.clone(),
            envelope,
            leaf_bytes,
        });
    }
    Ok(verified)
}

async fn verify_epoch_close(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
    encryption: EncryptionService,
    roster_chain: &crate::sync::circle::CircleRosterChain,
) -> Result<Option<VerifiedCloseOutcome>, CircleOperationError> {
    let CircleControlState::EpochClose(close) = control.value.state() else {
        // The successor is an ActiveEpoch. Dispatch on the settled slot object,
        // never the epoch origin: a reopened founder-origin epoch keeps its
        // `Founder` origin, so origin-based dispatch would accept a forged reopen
        // that carries no cancellation.
        if objects.close_cancellation.is_some() {
            if objects.close_outcome.is_some() || objects.close_intent.is_some() {
                return Err(CircleOperationError::InvalidState(
                    "Circle epoch reopen also carries a close outcome or intent".to_string(),
                ));
            }
            verify_epoch_reopen(database, storage, root, commit, control, objects).await?;
            return Ok(None);
        }
        if objects.close_intent.is_some() {
            return Err(CircleOperationError::InvalidState(
                "active Circle control carries an epoch-close intent".to_string(),
            ));
        }
        return verify_epoch_close_outcome(
            database, storage, root, commit, control, objects, encryption,
        )
        .await;
    };
    if objects.close_outcome.is_some()
        || objects.close_cancellation.is_some()
        || close.frozen_device_state != commit.device_state
        || close.provisional_frontier
            != commit
                .order
                .predecessor_cut()
                .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
                .frontier()
    {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch close differs from its activating Store cut".to_string(),
        ));
    }
    let intent = load_verified_epoch_close_intent(
        storage,
        commit.store_root_hash,
        control,
        objects,
        encryption,
    )
    .await?;
    let remaining = roster_chain
        .resolved_with_successor(intent.removal)
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    if remaining.state_hash() != intent.remaining_roster_state_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close intent names another remaining roster".to_string(),
        ));
    }
    let remaining_members = remaining.members();
    let devices = database
        .resolved_store_device_state(&close.frozen_device_state)
        .await?;
    let mut expected = Vec::new();
    for record in devices.devices.values() {
        if !matches!(
            record.status,
            crate::sync::store_commit::StoreDeviceStatus::Active
        ) {
            continue;
        }
        let registration = database
            .activated_store_device_registration(record.registration.clone())
            .await?;
        if remaining_members.contains_key(&registration.author_pubkey) {
            expected.push(record.registration.clone());
        }
    }
    expected.sort_by_key(|registration| registration.device_id);
    if close
        .participants
        .iter()
        .map(|participant| participant.registration.clone())
        .collect::<Vec<_>>()
        != expected
    {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close participants differ from the frozen active devices".to_string(),
        ));
    }
    Ok(None)
}

async fn load_verified_epoch_close_intent(
    storage: &dyn SyncStorage,
    store_root_hash: ObjectHash,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
    encryption: EncryptionService,
) -> Result<crate::sync::circle::CircleEpochCloseIntent, CircleOperationError> {
    let CircleControlState::EpochClose(close) = control.value.state() else {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close intent has no close control".to_string(),
        ));
    };
    if objects.close_intent.as_ref() != Some(&close.intent) {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close intent is absent from its exact object graph".to_string(),
        ));
    }
    let intent_prefix = circle_epoch_close_intent_semantic_prefix(
        control.value.circle_id,
        close.close_id,
        close.intent.intent_hash,
    );
    let bytes = read_exact_circle_object(
        storage,
        &ProtocolObjectContext::circle(
            store_root_hash,
            ProtocolObjectDomain::CircleEpochCloseIntent,
            encryption,
        ),
        &close.intent.object,
        &intent_prefix,
    )
    .await?;
    let intent: crate::sync::circle::CircleEpochCloseIntent = serde_json::from_slice(&bytes)
        .map_err(|error| {
            CircleOperationError::InvalidState(format!("parse Circle epoch-close intent: {error}"))
        })?;
    if !intent.verify()
        || intent.store_root_hash != store_root_hash
        || intent.circle_id != control.value.circle_id
        || intent.close_id != close.close_id
        || intent.epoch_id != close.frozen_epoch.common.epoch_id
        || intent.predecessor_roster != close.frozen_epoch.roster
        || intent.owner_pubkey != control.value.author_pubkey
        || intent.intent_hash() != close.intent.intent_hash
    {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close intent failed exact verification".to_string(),
        ));
    }
    Ok(intent)
}

async fn verify_epoch_close_outcome(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
    encryption: EncryptionService,
) -> Result<Option<VerifiedCloseOutcome>, CircleOperationError> {
    let active = control.value.active_epoch().ok_or_else(|| {
        CircleOperationError::InvalidState(
            "Circle epoch-close outcome has no active successor".to_string(),
        )
    })?;
    // An epoch's origin describes how the epoch was born, not what each control in
    // it does. Only the control whose exact predecessor is the `EpochClose`
    // finalizes the close and carries the outcome; every later control in the same
    // epoch (a re-add, a further add) inherits the already-settled epoch. Dispatch
    // on the retained predecessor's kind, not on the origin alone.
    let epoch_predecessor = retained_predecessor(database, root, control).await?;
    let predecessor_is_close = epoch_predecessor.as_ref().is_some_and(|predecessor| {
        matches!(
            predecessor.control.value.state(),
            CircleControlState::EpochClose(_)
        )
    });
    let crate::sync::circle::CircleEpochOrigin::Closed {
        closed_epoch_id,
        close_control,
        close_id,
        outcome_hash,
        cutoff,
    } = &active.common.origin
    else {
        if objects.close_outcome.is_some() {
            return Err(CircleOperationError::InvalidState(
                "founder Circle epoch carries a close outcome".to_string(),
            ));
        }
        // A founder-origin active epoch is an ordinary transition only when its
        // predecessor is itself active. An active successor of an epoch close must
        // carry a settlement — a finalize outcome (origin `Closed`, handled above)
        // or a reopen cancellation (dispatched before this function). Reaching here
        // over an `EpochClose` predecessor is the forged reopen the cancellation
        // dispatch key closes: reject it rather than trusting the `Founder` origin.
        if predecessor_is_close {
            return Err(CircleOperationError::InvalidState(
                "Circle active successor of an epoch close carries no settlement".to_string(),
            ));
        }
        return Ok(None);
    };
    if !predecessor_is_close {
        // A closed-origin control whose exact predecessor is already an active
        // epoch operates within an epoch a prior finalize established — the re-add
        // the plan defines as "the same operation with a new active access leaf and
        // current bootstrap." The outcome was verified once, at that finalize; this
        // control neither carries nor re-proves it. Bind it to the retained
        // predecessor's exact epoch so a forged origin (a fabricated cutoff or
        // close reference) cannot ride in on an in-epoch add.
        if objects.close_outcome.is_some() {
            return Err(CircleOperationError::InvalidState(
                "in-epoch Circle control carries a close outcome".to_string(),
            ));
        }
        let epoch_predecessor = epoch_predecessor.ok_or_else(|| {
            CircleOperationError::InvalidState(
                "closed-origin in-epoch Circle control retained no exact predecessor".to_string(),
            )
        })?;
        if control.value.epoch_id() != epoch_predecessor.control.value.epoch_id()
            || control.value.key_fingerprint() != epoch_predecessor.control.value.key_fingerprint()
            || active.common.origin != epoch_predecessor.control.value.active_common().origin
        {
            return Err(CircleOperationError::InvalidState(
                "closed-origin in-epoch Circle control differs from its retained epoch".to_string(),
            ));
        }
        return Ok(None);
    }
    let outcome_ref = objects.close_outcome.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(
            "closed Circle epoch omits its exact outcome".to_string(),
        )
    })?;
    if outcome_ref.close_id != *close_id || outcome_ref.outcome_hash != *outcome_hash {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch origin differs from its outcome reference".to_string(),
        ));
    }
    let predecessor = database
        .verified_circle_activation(root.clone(), control.value.circle_id, close_control.clone())
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle successor retained no exact close activation".to_string(),
            )
        })?;
    let CircleControlState::EpochClose(close) = predecessor.control.value.state() else {
        return Err(CircleOperationError::InvalidState(
            "Circle successor origin names an active predecessor".to_string(),
        ));
    };
    if predecessor.control.coord != *close_control
        || close.close_id != *close_id
        || close.frozen_epoch.common.epoch_id != *closed_epoch_id
        || outcome_ref.object.slot() != &close.outcome_slot
        || !control.value.causally_covers(&predecessor.control.value)
    {
        return Err(CircleOperationError::InvalidState(
            "Circle successor differs from its exact epoch close".to_string(),
        ));
    }
    let intent = load_verified_epoch_close_intent(
        storage,
        commit.store_root_hash,
        &predecessor.control,
        predecessor.reference.objects(),
        encryption,
    )
    .await?;
    let outcome_prefix = crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
        control.value.circle_id,
        *close_id,
    );
    let outcome_bytes = read_exact_circle_object(
        storage,
        &ProtocolObjectContext::store_encrypted(
            commit.store_root_hash,
            ProtocolObjectDomain::CircleEpochCloseOutcome,
        ),
        &outcome_ref.object,
        &outcome_prefix,
    )
    .await?;
    let crate::sync::circle::CircleEpochCloseSlotValue::Outcome(outcome) =
        crate::sync::circle::CircleEpochCloseSlotValue::parse(&outcome_bytes).map_err(|error| {
            CircleOperationError::InvalidState(format!("parse Circle epoch-close outcome: {error}"))
        })?
    else {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close outcome slot holds a cancellation for a finalized successor"
                .to_string(),
        ));
    };
    if outcome.outcome_hash() != *outcome_hash
        || crate::sync::circle::CircleEpochCloseOutcomeRef::from_outcome(
            &outcome,
            outcome_ref.object.clone(),
        )? != *outcome_ref
        || outcome.responses.len() != close.participants.len()
    {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close outcome differs from its exact reference".to_string(),
        ));
    }
    let mut settlements = Vec::with_capacity(close.participants.len());
    for (participant, settlement_ref) in close.participants.iter().zip(&outcome.responses) {
        if settlement_ref.registration() != &participant.registration
            || settlement_ref.object().slot() != &participant.response_slot
        {
            return Err(CircleOperationError::InvalidState(
                "Circle epoch-close outcome settlement differs from its participant".to_string(),
            ));
        }
        let prefix = crate::sync::circle::circle_epoch_close_response_semantic_prefix(
            control.value.circle_id,
            *close_id,
            participant.registration.device_id,
        );
        let bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleEpochCloseResponse,
            ),
            settlement_ref.object(),
            &prefix,
        )
        .await?;
        // Dispatch on the slot's actual settled arm, not the outcome's claim: the
        // outcome's declared settlement must equal the one derived here, which is
        // what refuses an outcome naming an exclusion for a slot holding a response.
        let slot_value = crate::sync::circle::CircleEpochCloseResponseSlotValue::parse(&bytes)
            .map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse Circle epoch-close response slot: {error}"
                ))
            })?;
        let settlement = match &slot_value {
            crate::sync::circle::CircleEpochCloseResponseSlotValue::Response(response) => {
                let registration = database
                    .activated_store_device_registration(participant.registration.clone())
                    .await?;
                if !response.verify_for(&predecessor.control, &registration) {
                    return Err(CircleOperationError::InvalidState(
                        "Circle epoch-close response failed exact verification".to_string(),
                    ));
                }
                crate::sync::circle::CircleEpochCloseSettlement::Response(
                    crate::sync::circle::CircleEpochCloseResponseRef::from_response(
                        response,
                        settlement_ref.object().clone(),
                    )?,
                )
            }
            crate::sync::circle::CircleEpochCloseResponseSlotValue::Exclusion(exclusion) => {
                if !exclusion.verify_for(&predecessor.control)
                    || exclusion.excluded != participant.registration
                {
                    return Err(CircleOperationError::InvalidState(
                        "Circle epoch-close exclusion failed exact verification".to_string(),
                    ));
                }
                crate::sync::circle::CircleEpochCloseSettlement::Exclusion(
                    crate::sync::circle::CircleEpochCloseExclusionRef::from_exclusion(
                        exclusion,
                        settlement_ref.object().clone(),
                    )?,
                )
            }
        };
        settlements.push((settlement, slot_value));
    }
    let successor = crate::sync::circle::CircleEpochSuccessor {
        epoch_id: active.common.epoch_id,
        key_fingerprint: active.common.key_fingerprint,
        owners: active.common.owners.clone(),
        access_root: active.common.access_root,
        metadata: active.metadata.clone(),
        roster: active.roster.clone(),
        store_membership: active.store_membership.clone(),
    };
    if !outcome.verify_for(&predecessor.control, &intent, &settlements)
        || outcome.cutoff != *cutoff
        || outcome.successor != successor
    {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close outcome failed exact verification".to_string(),
        ));
    }
    let exclusions = settlements
        .into_iter()
        .filter_map(|(settlement, _)| match settlement {
            crate::sync::circle::CircleEpochCloseSettlement::Exclusion(reference) => {
                Some(reference.registration)
            }
            crate::sync::circle::CircleEpochCloseSettlement::Response(_) => None,
        })
        .collect();
    Ok(Some(VerifiedCloseOutcome {
        close_id: outcome.close_id,
        exclusions,
    }))
}

/// Load the reopening control's exact predecessor close coordinate. The reopen's
/// same-stream predecessor is named by `previous_control_hash` and carried in the
/// covered control heads.
fn reopen_predecessor_coord(
    control: &PreparedCircleControl,
) -> Result<crate::sync::circle::CircleControlCoord, CircleOperationError> {
    let active = control.value.active_epoch().ok_or_else(|| {
        CircleOperationError::InvalidState("Circle reopen has no active successor".to_string())
    })?;
    let previous = control.value.previous_control_hash().ok_or_else(|| {
        CircleOperationError::InvalidState(
            "Circle active successor of an epoch close names no predecessor".to_string(),
        )
    })?;
    active
        .covered_control_heads
        .iter()
        .find(|head| head.coord.control_hash == previous)
        .map(|head| head.coord.clone())
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle successor predecessor is absent from its covered control heads".to_string(),
            )
        })
}

/// The active successor's exact predecessor as a retained activation, or `None`
/// when the control opens its own stream (founder seq 1) or the predecessor is
/// not retained. Its kind distinguishes the two active successors of a closed
/// epoch — the finalize (predecessor is the `EpochClose`, carries the outcome)
/// from every later in-epoch control (predecessor is already active, inherits the
/// settled epoch) — and rejects a founder-origin successor of an epoch close.
async fn retained_predecessor(
    database: &StoreDatabase,
    root: &StoreRootRef,
    control: &PreparedCircleControl,
) -> Result<Option<VerifiedCircleReference>, CircleOperationError> {
    if control.value.previous_control_hash().is_none() {
        return Ok(None);
    }
    let coord = reopen_predecessor_coord(control)?;
    Ok(database
        .verified_circle_activation(root.clone(), control.value.circle_id, coord)
        .await?)
}

/// Verify an `EpochClose → ActiveEpoch` reopen. The reopen is valid only when the
/// exact-read outcome slot of the named predecessor close holds an Owner-signed
/// cancellation, and the successor restores the frozen epoch's protocol identity
/// exactly — re-issuing only the control-bound access material.
async fn verify_epoch_reopen(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    commit: &StoreBatchCommit,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
) -> Result<(), CircleOperationError> {
    let active = control.value.active_epoch().ok_or_else(|| {
        CircleOperationError::InvalidState(
            "Circle epoch reopen has no active successor".to_string(),
        )
    })?;
    let cancellation_ref = objects.close_cancellation.as_ref().ok_or_else(|| {
        CircleOperationError::InvalidState(
            "Circle epoch reopen omits its exact cancellation".to_string(),
        )
    })?;
    let close_coord = reopen_predecessor_coord(control)?;
    let predecessor = database
        .verified_circle_activation(root.clone(), control.value.circle_id, close_coord.clone())
        .await?
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle reopen retained no exact close activation".to_string(),
            )
        })?;
    let CircleControlState::EpochClose(close) = predecessor.control.value.state() else {
        return Err(CircleOperationError::InvalidState(
            "Circle reopen predecessor is an active epoch, not a close".to_string(),
        ));
    };
    if predecessor.control.coord != close_coord
        || cancellation_ref.close_id != close.close_id
        || cancellation_ref.object.slot() != &close.outcome_slot
        || !control.value.causally_covers(&predecessor.control.value)
    {
        return Err(CircleOperationError::InvalidState(
            "Circle reopen differs from its exact epoch close".to_string(),
        ));
    }
    let cancellation_prefix = crate::sync::circle::circle_epoch_close_outcome_semantic_prefix(
        control.value.circle_id,
        close.close_id,
    );
    let cancellation_bytes = read_exact_circle_object(
        storage,
        &ProtocolObjectContext::store_encrypted(
            commit.store_root_hash,
            ProtocolObjectDomain::CircleEpochCloseOutcome,
        ),
        &cancellation_ref.object,
        &cancellation_prefix,
    )
    .await?;
    let crate::sync::circle::CircleEpochCloseSlotValue::Cancellation(cancellation) =
        crate::sync::circle::CircleEpochCloseSlotValue::parse(&cancellation_bytes).map_err(
            |error| {
                CircleOperationError::InvalidState(format!(
                    "parse Circle epoch-close cancellation: {error}"
                ))
            },
        )?
    else {
        return Err(CircleOperationError::InvalidState(
            "Circle reopen outcome slot holds a final outcome, not a cancellation".to_string(),
        ));
    };
    if !cancellation.verify_for(&predecessor.control)
        || crate::sync::circle::CircleEpochCloseCancellationRef::from_cancellation(
            &cancellation,
            cancellation_ref.object.clone(),
        )? != *cancellation_ref
    {
        return Err(CircleOperationError::InvalidState(
            "Circle epoch-close cancellation failed exact verification".to_string(),
        ));
    }
    let frozen = &close.frozen_epoch;
    if active.common.epoch_id != frozen.common.epoch_id
        || active.common.key_fingerprint != frozen.common.key_fingerprint
        || active.common.owners != frozen.common.owners
        || active.common.origin != frozen.common.origin
        || active.metadata != frozen.metadata
        || active.roster != frozen.roster
    {
        return Err(CircleOperationError::InvalidState(
            "Circle reopen successor differs from its frozen epoch".to_string(),
        ));
    }
    Ok(())
}

fn verify_circle_owner_authority(
    author_pubkey: &str,
    control: &CircleControl,
    roster: &CircleMaterializedRoster,
) -> bool {
    verify_merge_circle_owner_authority(author_pubkey, &control.value.author_authority, roster)
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
            let grant_id = crate::sync::circle_roster::derive_circle_resolution_grant(
                conflict_hash,
                author_pubkey,
            );
            roster.authorizes_resolution_grant(
                author_pubkey,
                &grant_id,
                &crate::sync::circle_roster::CircleRosterConflictResolutionRef {
                    conflict_hash: *conflict_hash,
                    resolver_pubkey: author_pubkey.to_string(),
                    resolution_hash: *resolution_hash,
                },
            )
        }
    }
}

struct CircleStreamAuthority {
    activation_id: StreamActivationId,
    first_slot: crate::storage::cloud::ObjectSlot,
    registration: StoreDeviceRegistration,
    activated_here: bool,
}

#[derive(Clone, Copy)]
enum CircleHeadKind {
    Control,
    Roster,
    Metadata,
}

enum CircleHeadValue {
    Control(crate::sync::circle::CircleControlHead),
    Roster(crate::sync::circle::CircleRosterHead),
    Metadata(crate::sync::circle::CircleMetadataHead),
}

struct CircleHeadPosition<'a> {
    store_root_hash: ObjectHash,
    circle_id: CircleId,
    author_pubkey: &'a str,
    device_id: &'a str,
    stream_id: crate::sync::causal_grants::AuthorStreamId,
    author_owner_grant: &'a crate::sync::membership::MembershipGrantId,
    seq: u64,
    successor: &'a crate::sync::store_commit::SuccessorLink,
}

impl CircleHeadValue {
    fn parse(kind: CircleHeadKind, bytes: &[u8]) -> Result<Self, CircleOperationError> {
        match kind {
            CircleHeadKind::Control => {
                serde_json::from_slice(bytes)
                    .map(Self::Control)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle control head: {error}"
                        ))
                    })
            }
            CircleHeadKind::Roster => {
                serde_json::from_slice(bytes)
                    .map(Self::Roster)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle roster head: {error}"
                        ))
                    })
            }
            CircleHeadKind::Metadata => {
                serde_json::from_slice(bytes)
                    .map(Self::Metadata)
                    .map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse predecessor Circle metadata head: {error}"
                        ))
                    })
            }
        }
    }

    fn position(&self) -> Result<CircleHeadPosition<'_>, CircleOperationError> {
        match self {
            Self::Control(head) => {
                let CircleControlCoord {
                    device_id,
                    stream_id,
                    author_pubkey,
                    author_owner_grant,
                    seq,
                    ..
                } = &head.control;
                Ok(CircleHeadPosition {
                    store_root_hash: head.store_root_hash,
                    circle_id: head.circle_id,
                    author_pubkey,
                    device_id,
                    stream_id: *stream_id,
                    author_owner_grant,
                    seq: *seq,
                    successor: &head.successor,
                })
            }
            Self::Roster(head) => Ok(CircleHeadPosition {
                store_root_hash: head.store_root_hash,
                circle_id: head.circle_id,
                author_pubkey: &head.author_pubkey,
                device_id: &head.device_id,
                stream_id: head.stream_id,
                author_owner_grant: &head.author_owner_grant,
                seq: head.seq,
                successor: &head.successor,
            }),
            Self::Metadata(head) => Ok(CircleHeadPosition {
                store_root_hash: head.store_root_hash,
                circle_id: head.circle_id,
                author_pubkey: &head.author_pubkey,
                device_id: &head.device_id,
                stream_id: head.stream_id,
                author_owner_grant: &head.author_owner_grant,
                seq: head.seq,
                successor: &head.successor,
            }),
        }
    }

    fn verify_for_registration(&self, registration: &StoreDeviceRegistration) -> bool {
        match self {
            Self::Control(head) => head.verify(registration),
            Self::Roster(head) => head.verify_for_registration(registration),
            Self::Metadata(head) => head.verify_for_registration(registration),
        }
    }

    fn semantic_prefix(&self, object: ExactObjectRef) -> String {
        match self {
            Self::Control(head) => circle_semantic_prefix(CircleSemanticSlot::ControlHead {
                circle_id: head.circle_id,
                control: &head.control,
                head_hash: head.head_hash(),
            }),
            Self::Roster(head) => {
                let reference = CircleRosterHeadRef::from_stored_head(head, object);
                circle_semantic_prefix(CircleSemanticSlot::RosterHead {
                    circle_id: head.circle_id,
                    head: &reference,
                })
            }
            Self::Metadata(head) => {
                let reference = CircleMetadataHeadRef::from_stored_head(head, object);
                circle_semantic_prefix(CircleSemanticSlot::MetadataHead {
                    circle_id: head.circle_id,
                    head: &reference,
                })
            }
        }
    }
}

async fn verify_circle_head_chain(
    storage: &dyn SyncStorage,
    context: &ProtocolObjectContext,
    kind: CircleHeadKind,
    current: CircleHeadValue,
    current_object: ExactObjectRef,
    authority: &CircleStreamAuthority,
) -> Result<(), CircleOperationError> {
    let mut current = current;
    let mut current_object = current_object;
    loop {
        let position = current.position()?;
        if !current.verify_for_registration(&authority.registration)
            || position.store_root_hash != authority.registration.store_root.store_root_hash
            || position.author_pubkey != authority.registration.author_pubkey
            || position.device_id != authority.registration.device_id.to_string()
            || position.successor.activation != authority.activation_id
        {
            return Err(CircleOperationError::InvalidState(
                "Circle head differs from its activated registration".to_string(),
            ));
        }
        if position.seq == 1 {
            if position.successor.predecessor.is_some()
                || current_object.slot() != &authority.first_slot
            {
                return Err(CircleOperationError::InvalidState(
                    "first Circle head differs from its activated slot".to_string(),
                ));
            }
            return Ok(());
        }
        let predecessor_object = position.successor.predecessor.clone().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "successor Circle head omits its exact predecessor".to_string(),
            )
        })?;
        let predecessor_prefix = predecessor_object
            .slot()
            .logical_key()
            .strip_suffix(".json")
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle predecessor head has a non-canonical logical key".to_string(),
                )
            })?;
        let predecessor_bytes =
            read_exact_circle_object(storage, context, &predecessor_object, predecessor_prefix)
                .await?;
        let predecessor = CircleHeadValue::parse(kind, &predecessor_bytes)?;
        let predecessor_position = predecessor.position()?;
        if predecessor.semantic_prefix(predecessor_object.clone()) != predecessor_prefix
            || predecessor_position.store_root_hash != position.store_root_hash
            || predecessor_position.circle_id != position.circle_id
            || predecessor_position.author_pubkey != position.author_pubkey
            || predecessor_position.device_id != position.device_id
            || predecessor_position.stream_id != position.stream_id
            || predecessor_position.author_owner_grant != position.author_owner_grant
            || predecessor_position.seq.checked_add(1) != Some(position.seq)
            || predecessor_position.successor.next_slot != *current_object.slot()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle head does not occupy its predecessor-reserved successor slot".to_string(),
            ));
        }
        current = predecessor;
        current_object = predecessor_object;
    }
}

async fn verify_covered_control_heads(
    database: &StoreDatabase,
    commit_verifier: &mut crate::sync::store::owner::pull::StoreCommitVerifier<'_>,
    verified_prefix: &VerifiedStreamActivationPrefix,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    control: &CircleControl,
) -> Result<(), CircleOperationError> {
    let storage = commit_verifier.storage();
    let access_epoch = control.access_epoch();
    let context = ProtocolObjectContext::store_encrypted(
        commit.store_root_hash,
        ProtocolObjectDomain::CircleControl,
    );
    for reference in &access_epoch.covered_control_heads {
        let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id: control.circle_id,
            control: &reference.coord,
            head_hash: reference.head_hash,
        });
        let bytes = read_exact_circle_object(storage, &context, &reference.object, &prefix).await?;
        let head: crate::sync::circle::CircleControlHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse covered Circle control head: {error}"
                ))
            })?;
        let CircleControlCoord {
            stream_id,
            author_owner_grant,
            ..
        } = &head.control;
        let authority = resolve_circle_stream_authority(
            database,
            commit_verifier,
            verified_prefix,
            commit_ref,
            commit,
            head.successor.activation,
            *stream_id,
            control.circle_id,
            author_owner_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                circle_id,
                first_slot,
            },
        )
        .await?;
        if authority.activated_here
            || head.control != reference.coord
            || head.head_hash() != reference.head_hash
        {
            return Err(CircleOperationError::InvalidState(
                "covered Circle control head differs from its exact reference".to_string(),
            ));
        }
        verify_circle_head_chain(
            storage,
            &context,
            CircleHeadKind::Control,
            CircleHeadValue::Control(head),
            reference.object.clone(),
            &authority,
        )
        .await?;
    }
    Ok(())
}

async fn resolve_circle_stream_authority(
    database: &StoreDatabase,
    commit_verifier: &mut crate::sync::store::owner::pull::StoreCommitVerifier<'_>,
    verified_prefix: &VerifiedStreamActivationPrefix,
    commit_ref: &StoreBatchCommitRef,
    commit: &StoreBatchCommit,
    claimed_activation_id: StreamActivationId,
    stream_id: crate::sync::causal_grants::AuthorStreamId,
    circle_id: CircleId,
    grant_id: &crate::sync::membership::MembershipGrantId,
    expected_anchor: fn(CircleId, crate::storage::cloud::ObjectSlot) -> GrantStreamAnchor,
) -> Result<CircleStreamAuthority, CircleOperationError> {
    let root = commit_verifier.root().clone();
    let current = commit
        .stream_activations()
        .iter()
        .find(|activation| activation.activation_id() == claimed_activation_id)
        .cloned();
    let (activation, activating_commit, activated_here) = if let Some(activation) = current {
        (activation, commit_ref.clone(), true)
    } else if let Some((activation, activating_commit)) =
        verified_prefix.activation(claimed_activation_id)
    {
        (activation.clone(), activating_commit.clone(), false)
    } else {
        let registered = database
            .registered_stream_activation(claimed_activation_id)
            .await
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(format!(
                    "Circle author stream {stream_id} has no verified activation"
                ))
            })?;
        (
            registered.activation().clone(),
            registered.activating_commit().clone(),
            false,
        )
    };
    let StreamActivation::GrantAuthorized {
        store_root_hash,
        author_registration,
        grant_id: activation_grant,
        anchor,
    } = &activation
    else {
        return Err(CircleOperationError::InvalidState(
            "Circle author stream uses device authority".to_string(),
        ));
    };
    let expected = expected_anchor(circle_id, anchor.first_slot().clone());
    if *store_root_hash != root.store_root_hash
        || activation.author_stream_id() != stream_id
        || activation_grant != grant_id
        || anchor != &expected
    {
        return Err(CircleOperationError::InvalidState(
            "Circle author stream differs from its activation descriptor".to_string(),
        ));
    }
    if activated_here {
        if activating_commit != *commit_ref {
            return Err(CircleOperationError::InvalidState(
                "same-commit Circle activation names another Store commit".to_string(),
            ));
        }
    } else {
        let reached = crate::sync::store::owner::pull::predecessor_commit_matching(
            commit_verifier,
            &commit.order,
            Box::new(|predecessor| {
                predecessor.reference() == &activating_commit
                    && predecessor
                        .value()
                        .stream_activations()
                        .binary_search(&activation)
                        .is_ok()
            }),
        )
        .await
        .map_err(|error| match error {
            crate::sync::store::owner::pull::RegistrationLoadError::Object(error) => {
                CircleOperationError::Object(error)
            }
            crate::sync::store::owner::pull::RegistrationLoadError::Invalid(error) => {
                CircleOperationError::InvalidState(error)
            }
        })?
        .is_some();
        if !reached {
            return Err(CircleOperationError::InvalidState(
                "Circle author stream activation is outside the commit predecessor history"
                    .to_string(),
            ));
        }
    }
    let registration = commit_verifier
        .load_registration(author_registration)
        .await?
        .value;
    Ok(CircleStreamAuthority {
        activation_id: activation.activation_id(),
        first_slot: anchor.first_slot().clone(),
        registration,
        activated_here,
    })
}

/// One identity's own resolved access at a verified control: the exact access
/// envelope and the decrypted, context-verified access leaf.
struct ResolvedIdentityAccess {
    envelope: AccessEnvelope,
    prepared_leaf: PreparedAccessLeaf,
}

/// Resolve an identity's own access leaf at an already-verified control, given the
/// control's verified access pairs and membership checkpoint. Returns `None` when
/// the identity is not a current member (removed or never added). This is the
/// identity-specific decryption step of Circle activation, split out so a
/// snapshot-restore selection can resolve its own access without re-verifying the
/// control's lineage — the control is already verified in the retained
/// materialization, and the lineage walk touches covered controls a restore may
/// have reclaimed.
fn resolve_identity_access_leaf(
    verified_access: &[VerifiedAccessPair],
    checkpoint_members: &[(String, crate::sync::membership::MemberRole)],
    reference: &crate::sync::store_commit::CircleControlRef,
    control: &PreparedCircleControl,
    commit: &StoreBatchCommit,
    identity: &UserKeypair,
) -> Result<Option<ResolvedIdentityAccess>, CircleOperationError> {
    let own_pubkey = keys::public_key_hex(identity);
    if !checkpoint_members
        .iter()
        .any(|(pubkey, _)| pubkey == &own_pubkey)
    {
        return Ok(None);
    }
    let owner_pubkey = &control.value.author_pubkey;
    let owner = (
        owner_pubkey.clone(),
        recipient_slot_with_peer(identity, owner_pubkey, reference.circle_id()).map_err(
            |error| {
                CircleOperationError::InvalidState(format!(
                    "derive circle Owner recipient slot: {error}"
                ))
            },
        )?,
    );
    let access = verified_access
        .iter()
        .find(|candidate| {
            candidate.reference.envelope.owner_pubkey == owner.0
                && candidate.reference.envelope.recipient_slot == owner.1
                && candidate.reference.envelope.control_hash == reference.control().control_hash()
        })
        .ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle activation lacks the recipient's exact access envelope".to_string(),
            )
        })?;
    let envelope = access.envelope.clone();
    let leaf_bytes = access.leaf_bytes.clone();
    let plaintext =
        keys::seal_box_decrypt(&leaf_bytes, &identity.to_x25519_secret_key()).map_err(|error| {
            CircleOperationError::InvalidState(format!("open circle access leaf: {error}"))
        })?;
    let leaf: CircleAccessLeaf = serde_json::from_slice(&plaintext).map_err(|error| {
        CircleOperationError::InvalidState(format!("parse circle access leaf: {error}"))
    })?;
    let prepared_leaf = PreparedAccessLeaf {
        bytes: leaf_bytes,
        value: leaf,
        leaf_hash: envelope.leaf_hash,
    };
    let leaf = &prepared_leaf.value;
    if leaf.candidate_family != commit.candidate_family()
        || leaf.owner_pubkey != owner.0
        || leaf.recipient_pubkey != own_pubkey
        || leaf.recipient_slot != owner.1
        || leaf.store_membership != control.value.store_membership_state_ref()
        || leaf.epoch_id != access.reference.leaf.epoch_id
        || leaf.leaf_id != access.reference.leaf.leaf_id
        || !prepared_leaf.verify_envelope(control, &envelope, commit.candidate_family())
    {
        return Err(CircleOperationError::InvalidState(
            "circle access leaf failed context verification".to_string(),
        ));
    }
    Ok(Some(ResolvedIdentityAccess {
        envelope,
        prepared_leaf,
    }))
}

/// Download and verify the Circle image named by an access leaf's bootstrap: the
/// recipient's own baseline for a Circle whose accessible content predates their
/// join, which no forward replay reconstructs. Shared by pull activation and
/// snapshot-restore selection so both verify the image against the retained
/// control and routing key identically.
#[allow(clippy::too_many_arguments)]
async fn build_verified_leaf_bootstrap_image(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &StoreRootRef,
    leaf: &CircleAccessLeaf,
    control: &PreparedCircleControl,
    bootstrap: &crate::sync::circle::CircleBootstrapRef,
    epoch_encryption: EncryptionService,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<VerifiedCircleImage, CircleOperationError> {
    if bootstrap.schema_version != database.sqlite().schema_version()
        || bootstrap.sync_routing_hash != database.sqlite().sync_routing_hash()
    {
        return Err(CircleOperationError::InvalidState(
            "Circle bootstrap schema or routing contract differs from the local Store".to_string(),
        ));
    }
    let image_prefix = crate::sync::store_commit::circle_bootstrap_image_semantic_prefix(
        leaf.circle_id,
        leaf.candidate_family,
        &leaf.owner_pubkey,
        leaf.epoch_id,
        &leaf.recipient_slot,
        bootstrap.image.image_hash,
    );
    let image_bytes = read_exact_circle_object(
        storage,
        &ProtocolObjectContext::circle(
            root.store_root_hash,
            ProtocolObjectDomain::CircleBootstrapImage,
            epoch_encryption,
        ),
        &bootstrap.image.object,
        &image_prefix,
    )
    .await?;
    crate::sync::store::snapshot::verify_circle_bootstrap_image(
        &image_bytes,
        bootstrap,
        leaf.circle_id,
        database.sqlite().synced_tables(),
        routing_key,
    )
    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    for binding in &bootstrap.blobs {
        let crate::blob::RowBlobAuthority::Remote(
            crate::sync::audience_package::PackageAudience::Circle {
                circle_id,
                control: blob_control,
                key_fingerprint,
            },
        ) = binding.authority()
        else {
            return Err(CircleOperationError::InvalidState(
                "Circle bootstrap row blob lacks Circle package authority".to_string(),
            ));
        };
        let blob_activation = database
            .verified_circle_activation(root.clone(), *circle_id, blob_control.clone())
            .await?
            .ok_or_else(|| {
                CircleOperationError::InvalidState(
                    "Circle bootstrap blob authority is not retained".to_string(),
                )
            })?;
        let current_control = control.clone();
        let historical_control = blob_control.clone();
        let lineage_circle_id = *circle_id;
        let lineage_root = root.clone();
        let is_in_control_history = database
            .sqlite()
            .call(move |connection| {
                StoreDatabase::verified_circle_control_covers_on(
                    connection,
                    &lineage_root,
                    lineage_circle_id,
                    &current_control,
                    &historical_control,
                )
            })
            .await?;
        if *key_fingerprint != blob_activation.control.value.key_fingerprint()
            || !is_in_control_history
        {
            return Err(CircleOperationError::InvalidState(
                "Circle bootstrap blob authority is outside its control history".to_string(),
            ));
        }
        let stored = binding.stored().ok_or_else(|| {
            CircleOperationError::InvalidState(
                "Circle bootstrap row blob has no exact locator".to_string(),
            )
        })?;
        storage.verify_blob_object(stored).await.map_err(|error| {
            CircleOperationError::InvalidState(format!(
                "verify Circle bootstrap blob {}: {error}",
                crate::sync::remote_object::remote_object_id(stored.object())
            ))
        })?;
    }
    VerifiedCircleImage::new(
        leaf.circle_id,
        control.coord.clone(),
        leaf,
        bootstrap.clone(),
        image_bytes,
    )
}

/// The restoring identity's own access at a Circle's head control: no access (the
/// gate to clear a preserved coverage row it must not retain), or active access
/// with the identity's own leaf-named bootstrap image if the leaf carries one.
pub(crate) enum LocalCircleAccess {
    NoAccess,
    Active {
        /// The Circle epoch key the identity's active leaf carries — the key
        /// standalone Circle snapshots (and the leaf bootstrap) are sealed under,
        /// which the restore selection reads their metadata and image with.
        epoch_encryption: EncryptionService,
        leaf_bootstrap: Option<VerifiedCircleImage>,
    },
}

/// Resolve an identity's own access at an already-verified control, reading only
/// its own access envelope and the Store membership checkpoint from storage, and —
/// when the leaf names a bootstrap — downloading and verifying that image. Used by
/// snapshot-restore selection to decide, per Circle, whether the restoring
/// identity can decrypt it and what baseline image it can stage.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn resolve_local_circle_access(
    history_verifier: &mut crate::sync::store::owner::pull::MergeHistoryVerifier<'_>,
    database: &StoreDatabase,
    commit: &StoreBatchCommit,
    reference: &crate::sync::store_commit::CircleControlRef,
    control: &PreparedCircleControl,
    identity: &UserKeypair,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<LocalCircleAccess, CircleOperationError> {
    let root = history_verifier.root().clone();
    let storage = history_verifier.storage();
    let verified_access = load_verified_access_pairs(
        storage,
        commit,
        reference.circle_id(),
        control,
        reference.objects(),
    )
    .await?;
    let checkpoint_members = Box::pin(verify_control_membership(history_verifier, control)).await?;
    let Some(resolved) = resolve_identity_access_leaf(
        &verified_access,
        checkpoint_members.as_slice(),
        reference,
        control,
        commit,
        identity,
    )?
    else {
        return Ok(LocalCircleAccess::NoAccess);
    };
    let CircleAccessDisposition::Active {
        keyring, bootstrap, ..
    } = &resolved.prepared_leaf.value.disposition
    else {
        return Ok(LocalCircleAccess::NoAccess);
    };
    let epoch_encryption =
        EncryptionService::from(MasterKeyring::from_serialized(keyring).map_err(|error| {
            CircleOperationError::InvalidState(format!("parse circle access keyring: {error}"))
        })?);
    let leaf_bootstrap = match bootstrap {
        Some(bootstrap) => Some(
            build_verified_leaf_bootstrap_image(
                database,
                storage,
                &root,
                &resolved.prepared_leaf.value,
                control,
                bootstrap,
                epoch_encryption.clone(),
                routing_key,
            )
            .await?,
        ),
        None => None,
    };
    Ok(LocalCircleAccess::Active {
        epoch_encryption,
        leaf_bootstrap,
    })
}

pub(crate) async fn load_circle_activations(
    database: &StoreDatabase,
    history_verifier: &mut crate::sync::store::owner::pull::MergeHistoryVerifier<'_>,
    verified: &VerifiedStoreBatchCommit,
    identity: &UserKeypair,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
) -> Result<VerifiedCircleActivations, CircleOperationError> {
    let commit = verified.value();
    history_verifier
        .verify_refs(crate::sync::store::owner::pull::commit_predecessor_references(commit))
        .await
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let verified_membership_prefix =
        crate::sync::store::owner::pull::verified_merge_membership_prefix(
            &history_verifier.history().commits,
            crate::sync::store::owner::pull::commit_predecessor_references(commit),
        )
        .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    let verified_prefix = VerifiedStreamActivationPrefix::empty();
    Box::pin(load_circle_activations_with_prefix(
        database,
        history_verifier.commit_verifier(),
        verified,
        Some(identity),
        routing_key,
        &verified_prefix,
        &verified_membership_prefix,
    ))
    .await
}

pub(crate) async fn load_circle_activations_with_prefix(
    database: &StoreDatabase,
    commit_verifier: &mut crate::sync::store::owner::pull::StoreCommitVerifier<'_>,
    verified: &VerifiedStoreBatchCommit,
    identity: Option<&UserKeypair>,
    routing_key: Option<&crate::sync::circle::RowRoutingKey>,
    verified_prefix: &VerifiedStreamActivationPrefix,
    verified_membership_prefix: &crate::sync::store::owner::pull::VerifiedMergeMembershipPrefix,
) -> Result<VerifiedCircleActivations, CircleOperationError> {
    let root = commit_verifier.root().clone();
    let storage = commit_verifier.storage();
    let commit_ref = verified.reference();
    let commit = verified.value();
    let author = verified.author();
    if commit_verifier.root().store_root_hash != commit.store_root_hash
        || commit
            .author_registration
            .verify_registration(author)
            .is_err()
    {
        return Err(CircleOperationError::InvalidState(
            "Circle activation authority differs from its exact Store commit".to_string(),
        ));
    }
    let mut activations = Vec::with_capacity(commit.circle_controls().len());
    let mut bootstraps = Vec::new();
    let mut local_exclusions = Vec::new();
    let mut bootstrap_pending_exclusions = Vec::new();
    let local_device_id = match identity {
        Some(_) => {
            database
                .sqlite()
                .get_protocol_state(crate::database::LOCAL_DEVICE_ID_STATE_KEY)
                .await?
        }
        None => None,
    };
    let mut consumed_stream_activations = BTreeSet::new();
    for reference in commit.circle_controls() {
        let objects = reference.objects();
        let control_prefix = circle_semantic_prefix(CircleSemanticSlot::Control {
            circle_id: reference.circle_id(),
            control: reference.control(),
        });
        let control_bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            &objects.control,
            &control_prefix,
        )
        .await?;
        if ObjectHash::digest(&control_bytes) != reference.control().control_hash() {
            return Err(CircleOperationError::InvalidState(
                "Circle control bytes differ from the signed control hash".to_string(),
            ));
        }
        let control_value: CircleControl =
            serde_json::from_slice(&control_bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!("parse Circle control: {error}"))
            })?;
        let declared_coord = control_value.coord();
        if !control_value.verify()
            || verify_circle_semantic_prefix(
                &control_prefix,
                CircleSemanticSlot::Control {
                    circle_id: control_value.circle_id,
                    control: &declared_coord,
                },
            )
            .is_err()
        {
            return Err(CircleOperationError::InvalidState(
                "Circle control failed exact verification".to_string(),
            ));
        }
        let control = PreparedCircleControl {
            coord: reference.control().clone(),
            bytes: control_bytes,
            value: control_value,
        };
        let circle_id = reference.circle_id;
        let control_coord = &reference.control;
        let head_hash = reference.head_hash;
        let prefix = circle_semantic_prefix(CircleSemanticSlot::ControlHead {
            circle_id,
            control: control_coord,
            head_hash,
        });
        let head_object = reference.head_object();
        let bytes = read_exact_circle_object(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            head_object,
            &prefix,
        )
        .await?;
        let head: crate::sync::circle::CircleControlHead =
            serde_json::from_slice(&bytes).map_err(|error| {
                CircleOperationError::InvalidState(format!(
                    "parse exact Circle control head: {error}"
                ))
            })?;
        let CircleControlCoord {
            stream_id,
            author_pubkey,
            author_owner_grant,
            seq,
            ..
        } = &head.control;
        let authority = resolve_circle_stream_authority(
            database,
            commit_verifier,
            verified_prefix,
            commit_ref,
            commit,
            head.successor.activation,
            *stream_id,
            circle_id,
            author_owner_grant,
            |circle_id, first_slot| GrantStreamAnchor::CircleControl {
                circle_id,
                first_slot,
            },
        )
        .await?;
        verify_circle_head_chain(
            storage,
            &ProtocolObjectContext::store_encrypted(
                commit.store_root_hash,
                ProtocolObjectDomain::CircleControl,
            ),
            CircleHeadKind::Control,
            CircleHeadValue::Control(head.clone()),
            head_object.clone(),
            &authority,
        )
        .await?;
        if !head.verify(author)
            || !head.verify(&authority.registration)
            || authority.registration.author_pubkey != *author_pubkey
            || (authority.activated_here && *seq != 1)
            || head.successor.activation != authority.activation_id
            || (*seq == 1
                && (head.successor.predecessor.is_some()
                    || head_object.slot() != &authority.first_slot))
            || head.head_hash() != head_hash
            || head.entry != objects.control
            || verify_circle_semantic_prefix(
                &prefix,
                CircleSemanticSlot::ControlHead {
                    circle_id: head.circle_id,
                    control: &head.control,
                    head_hash: head.head_hash(),
                },
            )
            .is_err()
            || head.store_root_hash != commit.store_root_hash
            || head.circle_id != circle_id
        {
            return Err(CircleOperationError::InvalidState(
                "Circle control head failed exact verification".to_string(),
            ));
        }
        if authority.activated_here {
            consumed_stream_activations.insert(authority.activation_id);
        }
        verify_covered_control_heads(
            database,
            commit_verifier,
            verified_prefix,
            commit_ref,
            commit,
            &control.value,
        )
        .await?;
        verify_control_context_for_verified_commit(reference, &control, verified)?;
        consume_public_private_stream_activations(
            commit,
            author,
            reference.circle_id(),
            &control,
            objects,
            &mut consumed_stream_activations,
        )?;
        let verified_access =
            load_verified_access_pairs(storage, commit, reference.circle_id(), &control, objects)
                .await?;
        let checkpoint_members = Box::pin(verify_control_membership_with_verified_activations(
            commit_verifier,
            &control,
            verified_membership_prefix,
        ))
        .await?;
        if control.value.state().is_deleted() {
            // A deletion carries no access material. It activates to the
            // terminal Deleted state with no local access; materialization
            // prunes the Circle's rows and caches from the winning chain.
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: None,
            });
            continue;
        }
        let Some(identity) = identity else {
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: None,
            });
            continue;
        };
        let Some(resolved) = resolve_identity_access_leaf(
            &verified_access,
            &checkpoint_members,
            reference,
            &control,
            commit,
            identity,
        )?
        else {
            activations.push(VerifiedCircleReference {
                reference: reference.clone(),
                circle_id: reference.circle_id(),
                control,
                local_access: None,
            });
            continue;
        };
        let ResolvedIdentityAccess {
            envelope,
            prepared_leaf,
        } = resolved;
        let leaf = &prepared_leaf.value;
        let active = match &leaf.disposition {
            CircleAccessDisposition::Active { keyring, .. } => {
                let encryption = EncryptionService::from(
                    MasterKeyring::from_serialized(keyring).map_err(|error| {
                        CircleOperationError::InvalidState(format!(
                            "parse circle access keyring: {error}"
                        ))
                    })?,
                );
                let authority_roster = load_circle_authority_roster(
                    database,
                    commit_verifier,
                    verified_prefix,
                    commit,
                    reference.circle_id(),
                    &control,
                    encryption.clone(),
                    objects,
                    commit_ref,
                    &mut consumed_stream_activations,
                )
                .await?;
                if !verify_circle_owner_authority(
                    &control.value.author_pubkey,
                    &control.value,
                    &authority_roster,
                ) {
                    return Err(CircleOperationError::InvalidState(
                        "circle control author lacks its exact historical Owner grant".to_string(),
                    ));
                }
                let roster_chain = load_circle_roster_chain(
                    database,
                    commit_verifier,
                    verified_prefix,
                    commit_ref,
                    commit,
                    reference.circle_id(),
                    &control.value.roster_state_ref(),
                    encryption.clone(),
                    objects,
                    &mut consumed_stream_activations,
                )
                .await?;
                let resolved = roster_chain
                    .try_resolved()
                    .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
                let close_outcome = verify_epoch_close(
                    database,
                    storage,
                    &root,
                    commit,
                    &control,
                    objects,
                    encryption.clone(),
                    &roster_chain,
                )
                .await?;
                if let (Some(outcome), Some(local_device_id)) =
                    (&close_outcome, local_device_id.as_deref())
                {
                    if let Some(excluded) = outcome
                        .exclusions
                        .iter()
                        .find(|registration| registration.device_id.to_string() == local_device_id)
                    {
                        local_exclusions.push(LocalCircleExclusion {
                            circle_id: reference.circle_id(),
                            close_id: outcome.close_id,
                            excluded: excluded.clone(),
                            successor_control: control.coord.clone(),
                            activating_commit: commit_ref.clone(),
                        });
                    }
                }
                let resolved_members = resolved.members();
                if !resolved_members.contains_key(&leaf.recipient_pubkey) {
                    return Err(CircleOperationError::InvalidState(
                        "circle Active access recipient is absent from its resolved roster"
                            .to_string(),
                    ));
                }
                let roster_owners = resolved_members
                    .iter()
                    .filter_map(|(pubkey, role)| {
                        (*role == crate::sync::circle::CircleRole::Owner).then_some(pubkey.clone())
                    })
                    .collect::<Vec<_>>();
                if roster_owners != control.value.owners() {
                    return Err(CircleOperationError::InvalidState(
                        "circle control Owners differ from its roster".to_string(),
                    ));
                }
                let metadata_state = control.value.metadata_state_ref();
                let metadata = load_circle_metadata_state(
                    database,
                    commit_verifier,
                    verified_prefix,
                    commit,
                    reference.circle_id(),
                    &metadata_state,
                    encryption.clone(),
                    objects,
                    commit_ref,
                    &mut consumed_stream_activations,
                )
                .await?;
                if let CircleAccessDisposition::Active {
                    bootstrap: Some(bootstrap),
                    ..
                } = &leaf.disposition
                {
                    // An excluded device that cannot yet read its successor bootstrap
                    // image defers the reset: flag the exclusion so the pull records
                    // it (detection is derived from the verified outcome above, not the
                    // bootstrap) and holds the successor. Its publication stays gated
                    // until a later pull reads the image and the reseed records
                    // coverage. The image read is the only source of a `CircleObject`
                    // error here — verification and blob checks fail as `InvalidState`.
                    match build_verified_leaf_bootstrap_image(
                        database,
                        storage,
                        &root,
                        leaf,
                        &control,
                        bootstrap,
                        encryption,
                        routing_key,
                    )
                    .await
                    {
                        Ok(image) => bootstraps.push(image),
                        Err(error @ CircleOperationError::Object(_)) => {
                            if let Some(exclusion) = local_exclusions
                                .iter()
                                .find(|exclusion| exclusion.circle_id == reference.circle_id())
                            {
                                bootstrap_pending_exclusions.push(exclusion.clone());
                                continue;
                            }
                            return Err(error);
                        }
                        Err(error) => return Err(error),
                    }
                }
                Some(VerifiedCircleActive {
                    roster: resolved,
                    metadata,
                })
            }
            CircleAccessDisposition::Inactive => None,
        };
        activations.push(VerifiedCircleReference {
            reference: reference.clone(),
            circle_id: reference.circle_id(),
            control,
            local_access: Some(VerifiedCircleAccess {
                envelope: envelope.clone(),
                leaf: prepared_leaf,
                active,
            }),
        });
    }
    let declared = commit
        .stream_activations()
        .iter()
        .map(StreamActivation::activation_id)
        .collect::<BTreeSet<_>>();
    if consumed_stream_activations != declared {
        return Err(CircleOperationError::InvalidState(
            "Store commit stream activations do not exactly introduce its first Circle heads"
                .to_string(),
        ));
    }
    let stream_activations =
        VerifiedStreamActivations::from_verified_circle_commit(commit, commit_ref)
            .map_err(|error| CircleOperationError::InvalidState(error.to_string()))?;
    Ok(VerifiedCircleActivations {
        circles: activations,
        stream_activations,
        bootstraps,
        local_exclusions,
        bootstrap_pending_exclusions,
    })
}

fn consume_public_private_stream_activations(
    commit: &StoreBatchCommit,
    author: &StoreDeviceRegistration,
    circle_id: CircleId,
    control: &PreparedCircleControl,
    objects: &CircleActivationObjects,
    consumed: &mut BTreeSet<StreamActivationId>,
) -> Result<(), CircleOperationError> {
    let roster = control.value.roster_state_ref();
    let metadata = control.value.metadata_state_ref();
    for activation in commit.stream_activations() {
        let StreamActivation::GrantAuthorized {
            store_root_hash,
            author_registration,
            grant_id,
            anchor,
        } = activation
        else {
            continue;
        };
        let valid = match anchor {
            GrantStreamAnchor::CircleRoster {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => roster.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.roster_heads.contains(head)
            }),
            GrantStreamAnchor::CircleMetadata {
                circle_id: anchor_circle,
                first_slot,
            } if *anchor_circle == circle_id => metadata.heads.iter().any(|head| {
                head.coord.seq == 1
                    && head.coord.author_pubkey == author.author_pubkey
                    && head.coord.device_id == author.device_id.to_string()
                    && head.coord.author_owner_grant == *grant_id
                    && head.coord.stream_id == activation.author_stream_id()
                    && head.object.slot() == first_slot
                    && objects.metadata_heads.contains(head)
            }),
            _ => continue,
        };
        if *store_root_hash != commit.store_root_hash
            || author_registration != &commit.author_registration
            || grant_id != &control.value.author_grant_id()
            || !valid
        {
            return Err(CircleOperationError::InvalidState(
                "private Circle stream activation differs from its signed public first-head reference"
                    .to_string(),
            ));
        }
        consumed.insert(activation.activation_id());
    }
    Ok(())
}

#[cfg(test)]
mod tests;
