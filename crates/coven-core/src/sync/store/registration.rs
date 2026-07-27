//! Durable append-only Store device registration and recovery.

use std::collections::BTreeMap;

#[cfg(test)]
use crate::database::Database;
use crate::keys::UserKeypair;

use crate::sync::membership::MembershipChain;
use crate::sync::storage::{PreparedExactObject, ProtocolObjectDomain, SyncStorage};
use crate::sync::store::database::StoreDatabase;
use crate::sync::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, head_slot_prefix, owner_recovery_semantic_prefix,
    registration_semantic_prefix, snapshot_slot_prefix, ActivatedStoreDeviceRegistrationRef,
    DeviceJoinAttempt, DeviceJoinAttemptDecisionRef, DeviceJoinAttemptRef, DeviceReadinessProof,
    DeviceRecoveryId, DeviceRecoveryReadiness, DeviceStreamAnchor, ObjectHash, OwnerRecoveryNode,
    OwnerRecoveryNodeRef, OwnerRecoveryPosition, StoreAck, StoreAckExclusionState, StoreAckRef,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreCommitOrder, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut,
    StoreOperationMembershipAuthority, SuccessorLink,
};
use crate::sync::store_objects::StoreObjectError;

#[derive(Debug, thiserror::Error)]
pub enum StoreRegistrationError {
    #[error("Store device registration database state: {0}")]
    Database(String),
    #[error("{0}")]
    Object(#[from] StoreObjectError),
    #[error("exact Store root authority is absent")]
    ExactRootAuthorityMissing,
    #[error("Store device registration bytes are invalid: {0}")]
    Invalid(String),
    #[error("this Store installation requires an activated Join or Recovery registration")]
    ActivationRequired,
    #[error("Store device registration activation: {0}")]
    Outbound(#[from] crate::sync::store::StoreError),
}

impl super::AuthorizedStore<'_> {
    pub(crate) async fn ensure_active_registration(
        &self,
    ) -> Result<(), crate::sync::cycle::SyncCycleFailure> {
        ensure_active_registration(self.database(), self.storage())
            .await
            .map_err(|error| {
                crate::sync::cycle::SyncCycleFailure::operation(
                    "publish Store device registration",
                    error,
                )
            })
    }
}

pub(crate) async fn ensure_active_registration(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
) -> Result<(), StoreRegistrationError> {
    drain_registration_outbox(database, storage).await?;
    match database
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
    {
        Some(registration) if registration.is_activated() => {
            require_activated_registration(database, storage, &registration).await?;
            return Ok(());
        }
        Some(_) => return Err(StoreRegistrationError::ActivationRequired),
        None => {}
    }

    database
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    Err(StoreRegistrationError::ActivationRequired)
}

pub(crate) async fn install_existing_founder_device(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    signer: &UserKeypair,
) -> Result<(), StoreRegistrationError> {
    let founder = crate::sync::store_objects::load_founder_registration(storage, root).await?;
    if founder.value.author_pubkey != crate::keys::public_key_hex(signer) {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration belongs to another identity".to_string(),
        ));
    }
    if founder.value.provider
        != storage
            .provider_binding()
            .await
            .map_err(StoreObjectError::from)?
            .device
    {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration belongs to another provider principal".to_string(),
        ));
    }
    founder
        .value
        .device_signer(signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;

    let registration_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let registration_prefix = crate::sync::store_commit::founder_registration_semantic_prefix(
        match founder.value.origin {
            StoreDeviceRegistrationOrigin::Founder { creation_id } => creation_id,
            _ => {
                return Err(StoreRegistrationError::Invalid(
                    "Store founder registration has a non-founder origin".to_string(),
                ))
            }
        },
    );
    let (registration_bytes, registration_prepared) = storage
        .read_prepared_protocol_slot(
            &registration_context,
            founder.object.slot(),
            &registration_prefix,
        )
        .await
        .map_err(StoreObjectError::from)?;
    if registration_bytes != founder.bytes || registration_prepared.reference() != &founder.object {
        return Err(StoreRegistrationError::Invalid(
            "prepared founder registration differs from its verified exact object".to_string(),
        ));
    }
    let registration_ref =
        StoreDeviceRegistrationRef::from_registration(&founder.value, founder.object.clone());
    let DeviceStreamAnchor::StoreAcknowledgements { first_slot } = &founder.value.acknowledgements
    else {
        return Err(StoreRegistrationError::Invalid(
            "Store founder registration has no acknowledgement anchor".to_string(),
        ));
    };
    let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let ack_prefix = ack_slot_prefix(&founder.value.device_id.to_string(), 1);
    let (ack_bytes, ack_prepared) = storage
        .read_prepared_protocol_slot(&ack_context, first_slot, &ack_prefix)
        .await
        .map_err(StoreObjectError::from)?;
    let unverified_ack: StoreAck = serde_json::from_slice(&ack_bytes)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let ack_ref = StoreAckRef {
        registration: registration_ref.clone(),
        sequence: unverified_ack.sequence,
        ack_hash: unverified_ack.ack_hash(),
        object: ack_prepared.reference().clone(),
    };
    let ack = StoreAck::parse_at(&ack_bytes, root, &ack_ref, &founder.value)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    if ack.registration != registration_ref {
        return Err(StoreRegistrationError::Invalid(
            "Store founder acknowledgement names another registration".to_string(),
        ));
    }
    database
        .install_existing_local_founder_device(
            crate::database::ExactProtocolObject {
                value: founder.value,
                bytes: registration_bytes,
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            ack_ref,
            crate::database::ExactProtocolObject {
                value: ack,
                bytes: ack_bytes,
                object: ack_prepared.reference().clone(),
                prepared: ack_prepared,
            },
        )
        .await
        .map_err(database_error)
}

pub(crate) async fn prepare_registration_for_origin(
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    store_root: crate::sync::store_commit::StoreRootRef,
    origin: StoreDeviceRegistrationOrigin,
    reserved_slot: crate::storage::cloud::ObjectSlot,
    expected_provider: crate::sync::storage::ProviderDeviceBinding,
    store_commits: DeviceStreamAnchor,
    acknowledgements: DeviceStreamAnchor,
    snapshots: DeviceStreamAnchor,
) -> Result<(StoreDeviceRegistration, PreparedExactObject), StoreRegistrationError> {
    let provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?
        .device;
    if provider != expected_provider {
        return Err(StoreRegistrationError::Invalid(
            "live provider principal differs from the reserved founder authority".to_string(),
        ));
    }
    let registration = StoreDeviceRegistration::signed(
        store_root,
        origin,
        provider,
        store_commits,
        acknowledgements,
        snapshots,
        identity_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let prepared = prepare_registration_object(storage, &registration, reserved_slot)?;
    Ok((registration, prepared))
}

fn prepare_registration_object(
    storage: &dyn SyncStorage,
    registration: &StoreDeviceRegistration,
    slot: crate::storage::cloud::ObjectSlot,
) -> Result<PreparedExactObject, StoreRegistrationError> {
    let semantic_prefix = slot
        .logical_key()
        .strip_suffix(".json")
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "reserved registration slot has no .json suffix".to_string(),
            )
        })?
        .to_string();
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        registration.store_root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    storage
        .prepare_protocol_object(&context, slot, &semantic_prefix, registration.to_bytes())
        .map_err(StoreObjectError::from)
        .map_err(StoreRegistrationError::from)
}

async fn prepare_or_load_recovery_registration(
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    expected: StoreDeviceRegistration,
    slot: crate::storage::cloud::ObjectSlot,
    semantic_prefix: &str,
) -> Result<
    (
        StoreDeviceRegistration,
        StoreDeviceRegistrationRef,
        PreparedExactObject,
        bool,
    ),
    StoreRegistrationError,
> {
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    match storage
        .read_prepared_protocol_slot(&context, &slot, semantic_prefix)
        .await
    {
        Ok((bytes, prepared)) => {
            let registration = StoreDeviceRegistration::parse_at(&bytes, root, expected.device_id)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if registration != expected {
                return Err(StoreRegistrationError::Invalid(
                    "existing Owner recovery registration differs from its exact authority".into(),
                ));
            }
            let reference = StoreDeviceRegistrationRef::from_registration(
                &registration,
                prepared.reference().clone(),
            );
            Ok((registration, reference, prepared, true))
        }
        Err(crate::sync::storage::StorageError::NotFound(_)) => {
            let prepared = storage
                .prepare_protocol_object(&context, slot, semantic_prefix, expected.to_bytes())
                .map_err(StoreObjectError::from)?;
            let reference = StoreDeviceRegistrationRef::from_registration(
                &expected,
                prepared.reference().clone(),
            );
            Ok((expected, reference, prepared, false))
        }
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

async fn prepared_protocol_object_exists(
    storage: &dyn SyncStorage,
    context: &crate::sync::storage::ProtocolObjectContext,
    prepared: &PreparedExactObject,
    semantic_prefix: &str,
    expected_bytes: &[u8],
) -> Result<bool, StoreRegistrationError> {
    match storage
        .read_prepared_protocol_slot(context, prepared.reference().slot(), semantic_prefix)
        .await
    {
        Ok((bytes, opened))
            if bytes == expected_bytes && opened.reference() == prepared.reference() =>
        {
            Ok(true)
        }
        Ok(_) => Err(StoreRegistrationError::Invalid(format!(
            "exact object {semantic_prefix:?} differs from its staged Owner recovery bytes"
        ))),
        Err(crate::sync::storage::StorageError::NotFound(_)) => Ok(false),
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

async fn prepare_or_load_initial_recovery_ack(
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    registration: &StoreDeviceRegistration,
    registration_ref: &StoreDeviceRegistrationRef,
    first_slot: crate::storage::cloud::ObjectSlot,
    store_cut: StoreHistoryCut,
    device_state: StoreDeviceStateRef,
    published_at: &str,
    device_signer: &UserKeypair,
) -> Result<(StoreAck, Vec<u8>, StoreAckRef, PreparedExactObject, bool), StoreRegistrationError> {
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let prefix = ack_slot_prefix(&registration.device_id.to_string(), 1);
    match storage
        .read_prepared_protocol_slot(&context, &first_slot, &prefix)
        .await
    {
        Ok((bytes, prepared)) => {
            let object = prepared.reference();
            let decoded: StoreAck = serde_json::from_slice(&bytes)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let reference = StoreAckRef {
                registration: registration_ref.clone(),
                sequence: decoded.sequence,
                ack_hash: decoded.ack_hash(),
                object: object.clone(),
            };
            let ack = StoreAck::parse_at(&bytes, root, &reference, registration)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let expected_activation = registration
                .store_acknowledgement_activation(registration_ref)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                .activation_id();
            if ack.sequence != 1
                || ack.successor.predecessor.is_some()
                || ack.registration != *registration_ref
                || ack.store_cut != store_cut
                || ack.device_state != device_state
                || ack.last_sync != published_at
                || ack.successor.activation != expected_activation
                || ack.successor.next_slot == first_slot
            {
                return Err(StoreRegistrationError::Invalid(
                    "existing Owner recovery acknowledgement differs from its exact authority"
                        .into(),
                ));
            }
            Ok((ack, bytes, reference, prepared, true))
        }
        Err(crate::sync::storage::StorageError::NotFound(_)) => {
            let next_slot = storage
                .allocate_protocol_slot(
                    &context,
                    &ack_slot_prefix(&registration.device_id.to_string(), 2),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let ack = StoreAck::signed(
                root.store_root_hash,
                registration_ref.clone(),
                1,
                store_cut,
                device_state,
                None,
                StoreAckExclusionState {
                    proposal_freezes: Vec::new(),
                },
                published_at.to_string(),
                SuccessorLink {
                    activation: registration
                        .store_acknowledgement_activation(registration_ref)
                        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                        .activation_id(),
                    predecessor: None,
                    next_slot,
                },
                device_signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let bytes = ack.to_bytes();
            let prepared = storage
                .prepare_protocol_object(&context, first_slot, &prefix, bytes.clone())
                .map_err(StoreObjectError::from)?;
            let reference = StoreAckRef {
                registration: registration_ref.clone(),
                sequence: 1,
                ack_hash: ack.ack_hash(),
                object: prepared.reference().clone(),
            };
            Ok((ack, bytes, reference, prepared, false))
        }
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn prepare_or_load_owner_recovery_node(
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    recovery_slot: crate::storage::cloud::ObjectSlot,
    owner_pubkey: &str,
    owner_grant: &crate::sync::membership::MembershipGrantId,
    sequence: u64,
    recovery_id: DeviceRecoveryId,
    membership: &crate::sync::circle_control::StoreMembershipStateRef,
    predecessor: &Option<OwnerRecoveryNodeRef>,
    readiness: &DeviceRecoveryReadiness,
    identity_signer: &UserKeypair,
) -> Result<
    (
        OwnerRecoveryNode,
        OwnerRecoveryNodeRef,
        PreparedExactObject,
        bool,
    ),
    StoreRegistrationError,
> {
    let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::OwnerRecoveryNode,
    );
    let prefix = owner_recovery_semantic_prefix(owner_pubkey, owner_grant.clone(), sequence);
    match storage
        .read_prepared_protocol_slot(&context, &recovery_slot, &prefix)
        .await
    {
        Ok((bytes, prepared)) => {
            let object = prepared.reference();
            let decoded: OwnerRecoveryNode = serde_json::from_slice(&bytes)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let reference = OwnerRecoveryNodeRef {
                owner_pubkey: decoded.owner_pubkey.clone(),
                owner_grant: decoded.owner_grant.clone(),
                sequence: decoded.sequence,
                node_hash: decoded.node_hash(),
                object: object.clone(),
            };
            let node = OwnerRecoveryNode::parse_at(&bytes, root, &reference)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if node.recovery_id != recovery_id
                || node.owner_pubkey != owner_pubkey
                || node.owner_grant != *owner_grant
                || node.sequence != sequence
                || node.membership != *membership
                || node.predecessor != *predecessor
                || node.readiness != *readiness
                || node.next_slot == recovery_slot
            {
                return Err(StoreRegistrationError::Invalid(
                    "existing Owner recovery node differs from its exact authority".into(),
                ));
            }
            Ok((node, reference, prepared, true))
        }
        Err(crate::sync::storage::StorageError::NotFound(_)) => {
            let next_sequence = sequence.checked_add(1).ok_or_else(|| {
                StoreRegistrationError::Invalid("Owner recovery sequence overflow".into())
            })?;
            let next_slot = storage
                .allocate_protocol_slot(
                    &context,
                    &owner_recovery_semantic_prefix(
                        owner_pubkey,
                        owner_grant.clone(),
                        next_sequence,
                    ),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let node = OwnerRecoveryNode::signed(
                root.store_root_hash,
                recovery_id,
                owner_grant.clone(),
                sequence,
                membership.clone(),
                predecessor.clone(),
                readiness.clone(),
                next_slot,
                identity_signer,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let prepared = storage
                .prepare_protocol_object(&context, recovery_slot, &prefix, node.to_bytes())
                .map_err(StoreObjectError::from)?;
            let reference = OwnerRecoveryNodeRef {
                owner_pubkey: owner_pubkey.to_string(),
                owner_grant: owner_grant.clone(),
                sequence,
                node_hash: node.node_hash(),
                object: prepared.reference().clone(),
            };
            Ok((node, reference, prepared, false))
        }
        Err(error) => Err(StoreObjectError::from(error).into()),
    }
}

#[allow(clippy::too_many_arguments)]
async fn install_activated_owner_recovery(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    root: &crate::sync::store_commit::StoreRootRef,
    origin: &StoreDeviceRegistrationOrigin,
    device_id: crate::sync::store_commit::StoreDeviceId,
    recovery_id: DeviceRecoveryId,
    recovery_slot: &crate::storage::cloud::ObjectSlot,
    owner_pubkey: &str,
    owner_grant: &crate::sync::membership::MembershipGrantId,
    sequence: u64,
    predecessor: &Option<OwnerRecoveryNodeRef>,
) -> Result<Option<StoreDeviceRegistrationRef>, StoreRegistrationError> {
    let Some((registration_ref, registration, activation)) = database
        .activated_store_device_registration_for_device(device_id)
        .await
        .map_err(database_error)?
    else {
        return Ok(None);
    };
    let StoreDeviceRegistrationActivation::Recovery {
        recovery_id: activated_recovery_id,
        node: node_ref,
    } = activation.clone()
    else {
        return Err(StoreRegistrationError::Invalid(
            "derived Owner recovery device has a non-recovery activation".into(),
        ));
    };
    if registration.origin != *origin
        || registration.author_pubkey != owner_pubkey
        || activated_recovery_id != recovery_id
        || node_ref.owner_pubkey != owner_pubkey
        || node_ref.owner_grant != *owner_grant
        || node_ref.sequence != sequence
        || node_ref.object.slot() != recovery_slot
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery registration differs from the requested authority".into(),
        ));
    }
    let provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?
        .device;
    if registration.provider != provider {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery registration belongs to another provider principal".into(),
        ));
    }
    let node =
        crate::sync::store_objects::load_owner_recovery_node_ref(storage, root, &node_ref).await?;
    if node.value.recovery_id != recovery_id
        || node.value.predecessor != *predecessor
        || node.value.readiness.registration != registration_ref
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery node differs from the requested authority".into(),
        ));
    }
    let initial_ack_ref = node.value.readiness.initial_ack;
    let initial_ack = crate::sync::store_objects::load_store_ack_ref(
        storage,
        root,
        &initial_ack_ref,
        &registration,
    )
    .await?;
    if initial_ack.value.store_cut != node.value.readiness.bootstrap_cut {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery acknowledgement differs from its recovery node".into(),
        ));
    }

    let registration_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreDeviceRegistration,
    );
    let (registration_bytes, registration_prepared) = storage
        .read_prepared_protocol_slot(
            &registration_context,
            registration_ref.object.slot(),
            &registration_semantic_prefix(&registration_ref.device_id.to_string()),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if registration_bytes != registration.to_bytes()
        || registration_prepared.reference() != &registration_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery registration differs from its prepared exact object".into(),
        ));
    }
    let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
        root.store_root_hash,
        ProtocolObjectDomain::StoreAck,
    );
    let (initial_ack_bytes, initial_ack_prepared) = storage
        .read_prepared_protocol_slot(
            &ack_context,
            initial_ack_ref.object.slot(),
            &ack_slot_prefix(
                &registration_ref.device_id.to_string(),
                initial_ack_ref.sequence,
            ),
        )
        .await
        .map_err(StoreObjectError::from)?;
    if initial_ack_bytes != initial_ack.bytes
        || initial_ack_prepared.reference() != &initial_ack_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery acknowledgement differs from its prepared exact object"
                .into(),
        ));
    }
    let already_activated = database
        .stage_owner_recovery_registration(
            crate::database::ExactProtocolObject {
                value: registration,
                bytes: registration_bytes,
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            initial_ack_ref,
            crate::database::ExactProtocolObject {
                value: initial_ack.value,
                bytes: initial_ack_bytes,
                object: initial_ack_prepared.reference().clone(),
                prepared: initial_ack_prepared,
            },
            activation,
        )
        .await
        .map_err(database_error)?;
    if !already_activated {
        return Err(StoreRegistrationError::Invalid(
            "activated Owner recovery disappeared while installing its local journal".into(),
        ));
    }
    Ok(Some(registration_ref))
}

pub async fn recover_owner_device(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    authority: &crate::sync::restore_code::OwnerRecoveryAuthority,
    membership: &MembershipChain,
) -> Result<StoreDeviceRegistrationRef, StoreRegistrationError> {
    let root = database
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let protocol = crate::sync::store_objects::load_store_protocol_root(storage, &root)
        .await?
        .value;
    let owner_pubkey = crate::keys::public_key_hex(identity_signer);
    if owner_pubkey != protocol.descriptor.founder_pubkey
        || authority.owner_grant != protocol.descriptor.founder_grant
        || membership.active_owner_grant(&owner_pubkey).as_ref() != Some(&authority.owner_grant)
        || authority.recovery.owner_grant != authority.owner_grant
    {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery authority differs from the active root founder grant".into(),
        ));
    }
    let crate::sync::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
        &protocol.descriptor.founder_recovery
    else {
        return Err(StoreRegistrationError::Invalid(
            "Store root has no founder recovery stream".into(),
        ));
    };
    let (recovery_slot, predecessor, sequence) = match &authority.recovery.position {
        OwnerRecoveryPosition::BeforeFirst { activation } => {
            let expected = crate::sync::store_commit::OwnerRecoveryActivationId::derive(
                &root,
                &owner_pubkey,
                &authority.owner_grant,
                &protocol.descriptor.founder_recovery,
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            if activation != &expected {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery activation differs from the root anchor".into(),
                ));
            }
            (first_slot.clone(), None, 1)
        }
        OwnerRecoveryPosition::At { node } => {
            let loaded =
                crate::sync::store_objects::load_owner_recovery_node_ref(storage, &root, node)
                    .await?;
            if loaded.value.owner_pubkey != owner_pubkey
                || loaded.value.owner_grant != authority.owner_grant
            {
                return Err(StoreRegistrationError::Invalid(
                    "Owner recovery cursor belongs to another authority".into(),
                ));
            }
            (
                loaded.value.next_slot,
                Some(node.clone()),
                node.sequence.saturating_add(1),
            )
        }
    };
    let recovery_hash = ObjectHash::digest(
        &serde_json::to_vec(&(
            &root,
            &owner_pubkey,
            &authority.owner_grant,
            &authority.recovery,
        ))
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?,
    );
    let recovery_id = DeviceRecoveryId::from_hash(recovery_hash);
    let origin = StoreDeviceRegistrationOrigin::Recovery {
        recovery_id,
        recovery_slot: recovery_slot.clone(),
        owner_grant: authority.owner_grant.clone(),
    };
    let device_id = crate::sync::store_commit::StoreDeviceId::derive(&root, &origin);
    if let Some(registration) = install_activated_owner_recovery(
        database,
        storage,
        &root,
        &origin,
        device_id,
        recovery_id,
        &recovery_slot,
        &owner_pubkey,
        &authority.owner_grant,
        sequence,
        &predecessor,
    )
    .await?
    {
        return Ok(registration);
    }
    let context = |domain| {
        crate::sync::storage::ProtocolObjectContext::signed_plaintext(root.store_root_hash, domain)
    };
    let commit_context = context(ProtocolObjectDomain::StoreCommit);
    let staged = database
        .latest_local_store_device_registration()
        .await
        .map_err(database_error)?
        .filter(|durable| durable.device_id == device_id);
    let (
        registration,
        registration_ref,
        registration_prepared,
        registration_exists,
        initial_ack,
        initial_ack_bytes,
        initial_ack_ref,
        initial_ack_prepared,
        initial_ack_exists,
    ) = if let Some(durable) = staged {
        let registration = StoreDeviceRegistration::parse_at(
            &durable.registration_bytes,
            &root,
            durable.device_id,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if registration.origin != origin
            || registration.to_bytes() != durable.registration_bytes
            || registration.registration_hash() != durable.registration_hash
        {
            return Err(StoreRegistrationError::Invalid(
                "staged Owner recovery registration differs from its exact authority".into(),
            ));
        }
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration,
            durable.prepared.reference().clone(),
        );
        let initial_ack = StoreAck::parse_at(
            &durable.initial_ack.bytes,
            &root,
            &durable.initial_ack_ref,
            &registration,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if initial_ack != durable.initial_ack.value
            || durable.initial_ack.object != *durable.initial_ack.prepared.reference()
        {
            return Err(StoreRegistrationError::Invalid(
                "staged Owner recovery acknowledgement differs from its exact authority".into(),
            ));
        }
        let registration_exists = prepared_protocol_object_exists(
            storage,
            &context(crate::sync::storage::ProtocolObjectDomain::StoreDeviceRegistration),
            &durable.prepared,
            &registration_semantic_prefix(&device_id.to_string()),
            &durable.registration_bytes,
        )
        .await?;
        let initial_ack_exists = prepared_protocol_object_exists(
            storage,
            &context(crate::sync::storage::ProtocolObjectDomain::StoreAck),
            &durable.initial_ack.prepared,
            &ack_slot_prefix(&device_id.to_string(), 1),
            &durable.initial_ack.bytes,
        )
        .await?;
        (
            registration,
            registration_ref,
            durable.prepared,
            registration_exists,
            initial_ack,
            durable.initial_ack.bytes,
            durable.initial_ack_ref,
            durable.initial_ack.prepared,
            initial_ack_exists,
        )
    } else {
        let head_context = context(crate::sync::storage::ProtocolObjectDomain::StoreHead);
        let ack_context = context(crate::sync::storage::ProtocolObjectDomain::StoreAck);
        let snapshot_context =
            context(crate::sync::storage::ProtocolObjectDomain::StoreSnapshotMeta);
        let registration_context =
            context(crate::sync::storage::ProtocolObjectDomain::StoreDeviceRegistration);
        let first_head = storage
            .allocate_protocol_slot(
                &head_context,
                &head_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let first_ack = storage
            .allocate_protocol_slot(
                &ack_context,
                &ack_slot_prefix(&device_id.to_string(), 1),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let first_snapshot = storage
            .allocate_protocol_slot(
                &snapshot_context,
                &snapshot_slot_prefix(&device_id.to_string(), 0),
                ".json",
            )
            .await
            .map_err(StoreObjectError::from)?;
        let registration_prefix = registration_semantic_prefix(&device_id.to_string());
        let registration_slot = storage
            .allocate_protocol_slot(&registration_context, &registration_prefix, ".json")
            .await
            .map_err(StoreObjectError::from)?;
        let provider = storage
            .provider_binding()
            .await
            .map_err(StoreObjectError::from)?
            .device;
        let expected_registration = StoreDeviceRegistration::signed(
            root.clone(),
            origin.clone(),
            provider,
            DeviceStreamAnchor::StoreAnnouncements {
                first_slot: first_head,
            },
            DeviceStreamAnchor::StoreAcknowledgements {
                first_slot: first_ack.clone(),
            },
            DeviceStreamAnchor::StoreSnapshots {
                first_slot: first_snapshot,
            },
            identity_signer,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let (registration, registration_ref, registration_prepared, registration_exists) =
            prepare_or_load_recovery_registration(
                storage,
                &root,
                expected_registration,
                registration_slot,
                &registration_prefix,
            )
            .await?;
        let dependencies = database
            .materialized_frontier()
            .await
            .map_err(database_error)?
            .into_iter()
            .map(|(stream, reference)| {
                stream
                    .parse()
                    .map(|stream| (stream, reference))
                    .map_err(|error| {
                        StoreRegistrationError::Invalid(format!(
                            "Owner recovery frontier stream {stream}: {error}"
                        ))
                    })
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let device_signer = registration
            .device_signer(identity_signer)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let bootstrap_cut = StoreHistoryCut(dependencies);
        let (device_state, _) = database
            .store_device_state_for_history_cut(&bootstrap_cut)
            .await
            .map_err(database_error)?;
        let (
            initial_ack,
            initial_ack_bytes,
            initial_ack_ref,
            initial_ack_prepared,
            initial_ack_exists,
        ) = prepare_or_load_initial_recovery_ack(
            storage,
            &root,
            &registration,
            &registration_ref,
            first_ack,
            bootstrap_cut,
            device_state,
            &authority.published_at,
            &device_signer,
        )
        .await?;
        (
            registration,
            registration_ref,
            registration_prepared,
            registration_exists,
            initial_ack,
            initial_ack_bytes,
            initial_ack_ref,
            initial_ack_prepared,
            initial_ack_exists,
        )
    };
    let dependencies = initial_ack.store_cut.0.clone();
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let bootstrap_cut = initial_ack.store_cut.clone();
    let crate::sync::membership::MembershipStatus::Resolved(resolved) = membership.status() else {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery requires resolved membership".into(),
        ));
    };
    let membership_state = crate::sync::circle_control::StoreMembershipStateRef::from_parts(
        membership.head_refs().to_vec(),
        membership.resolution_refs().to_vec(),
        vec![authority.recovery.clone()],
        resolved.state_hash,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let readiness = DeviceRecoveryReadiness {
        registration: registration_ref.clone(),
        initial_ack: initial_ack_ref.clone(),
        bootstrap_cut: bootstrap_cut.clone(),
    };
    let (_node, node_ref, node_prepared, node_exists) = prepare_or_load_owner_recovery_node(
        storage,
        &root,
        recovery_slot,
        &owner_pubkey,
        &authority.owner_grant,
        sequence,
        recovery_id,
        &membership_state,
        &predecessor,
        &readiness,
        identity_signer,
    )
    .await?;
    let registration_activation = StoreDeviceRegistrationActivation::Recovery {
        recovery_id,
        node: node_ref.clone(),
    };
    let already_activated = database
        .stage_owner_recovery_registration(
            crate::database::ExactProtocolObject {
                value: registration.clone(),
                bytes: registration.to_bytes(),
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared.clone(),
            },
            initial_ack_ref.clone(),
            crate::database::ExactProtocolObject {
                value: initial_ack.clone(),
                bytes: initial_ack.to_bytes(),
                object: initial_ack_prepared.reference().clone(),
                prepared: initial_ack_prepared.clone(),
            },
            registration_activation.clone(),
        )
        .await
        .map_err(database_error)?;
    if already_activated {
        return Ok(registration_ref);
    }
    if !registration_exists {
        storage
            .create_protocol_object(&registration_prepared)
            .await
            .map_err(StoreObjectError::from)?;
    }
    if !initial_ack_exists {
        storage
            .create_protocol_object(&initial_ack_prepared)
            .await
            .map_err(StoreObjectError::from)?;
    }
    if !node_exists {
        storage
            .create_protocol_object(&node_prepared)
            .await
            .map_err(StoreObjectError::from)?;
    }
    database
        .mark_local_store_device_registration_created(
            crate::database::ExactProtocolObject {
                value: registration.clone(),
                bytes: registration.to_bytes(),
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            initial_ack_ref,
            crate::database::ExactProtocolObject {
                value: initial_ack,
                bytes: initial_ack_bytes,
                object: initial_ack_prepared.reference().clone(),
                prepared: initial_ack_prepared,
            },
        )
        .await
        .map_err(database_error)?;

    let stream_id = crate::sync::store_commit::StreamActivation::device_authorized_stream_id(
        root.store_root_hash,
        &registration_ref,
        crate::sync::store_commit::StreamAnchorDomain::StoreAnnouncements,
    );
    let order = StoreCommitOrder {
        seq: 1,
        predecessor: None,
        dependencies,
    };
    let (device_state, predecessor_state) = database
        .store_device_state_for_order(&order)
        .await
        .map_err(database_error)?;
    let coord = StoreCommitCoord {
        stream_id,
        sequence: 1,
    };
    let activation_ref = ActivatedStoreDeviceRegistrationRef {
        registration: registration_ref.clone(),
        authority: StoreDeviceRegistrationActivationRef::Recovery {
            recovery_id,
            node: node_ref.clone(),
        },
    };
    let commit = StoreBatchCommit::signed_with_registrations(
        root.store_root_hash,
        crate::WriteId::from_generated(format!("owner-recovery-{recovery_hash}")),
        coord.clone(),
        registration_ref.clone(),
        &registration,
        order,
        membership_state,
        device_state,
        StoreOperationMembershipAuthority {
            predecessor: membership
                .active_grant(&authority.owner_grant)
                .ok_or_else(|| {
                    StoreRegistrationError::Invalid(
                        "Owner recovery grant is absent from active membership".to_string(),
                    )
                })?
                .creation_authority
                .clone(),
        },
        vec![activation_ref],
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let commit_prefix = commit_semantic_prefix(
        commit.candidate_family(),
        &stream_id.to_string(),
        1,
        commit.commit_hash(),
    );
    let commit_slot = storage
        .allocate_protocol_slot(&commit_context, &commit_prefix, ".json")
        .await
        .map_err(StoreObjectError::from)?;
    let commit_prepared = storage
        .prepare_protocol_object(
            &commit_context,
            commit_slot,
            &commit_prefix,
            commit.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    let commit_ref =
        StoreBatchCommitRef::from_commit(&commit, coord, commit_prepared.reference().clone())
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let verified_commit = crate::sync::store_commit::VerifiedStoreBatchCommit::parse(
        &commit.to_bytes(),
        root.store_root_hash,
        &commit_ref,
        &registration,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let state_after = predecessor_state
        .activate_registration(
            registration_ref.clone(),
            Some(crate::sync::store_commit::OwnerRecoveryCursor {
                owner_grant: authority.owner_grant.clone(),
                position: OwnerRecoveryPosition::At {
                    node: node_ref.clone(),
                },
            }),
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let history = crate::sync::store::pull::prepare_merge_history_successor(
        database,
        &root,
        &verified_commit,
        membership,
        Some(&registration_ref),
        state_after,
        crate::sync::store::pull::MergeHistorySuccessorEvidence {
            registrations: vec![crate::sync::store_commit::RetainedVerifiedRegistration {
                reference: registration_ref.clone(),
                value: registration.clone(),
            }],
            acknowledgement: None,
            membership_proof: None,
        },
    )
    .await
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let head_context = context(crate::sync::storage::ProtocolObjectDomain::StoreHead);
    let DeviceStreamAnchor::StoreAnnouncements { first_slot: _ } = &registration.store_commits
    else {
        return Err(StoreRegistrationError::Invalid(
            "Owner recovery registration has no announcement stream anchor".into(),
        ));
    };
    let next_head = storage
        .allocate_protocol_slot(
            &head_context,
            &head_slot_prefix(&device_id.to_string(), 2),
            ".json",
        )
        .await
        .map_err(StoreObjectError::from)?;
    let head = StoreDeviceHead::signed(
        root.store_root_hash,
        registration_ref.clone(),
        commit_ref.clone(),
        history.summary.digest(),
        SuccessorLink {
            activation: registration
                .store_announcement_activation(&registration_ref)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                .activation_id(),
            predecessor: None,
            next_slot: next_head,
        },
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let head_prepared = storage
        .prepare_protocol_object(
            &head_context,
            history.head_slot,
            &head_slot_prefix(&device_id.to_string(), 1),
            head.to_bytes(),
        )
        .map_err(StoreObjectError::from)?;
    storage
        .create_protocol_object(&commit_prepared)
        .await
        .map_err(StoreObjectError::from)?;
    storage
        .create_protocol_object(&head_prepared)
        .await
        .map_err(StoreObjectError::from)?;
    database
        .complete_owner_recovery(
            verified_commit,
            head,
            head_prepared.reference().clone(),
            history.summary,
            registration,
            registration_activation,
        )
        .await
        .map_err(database_error)?;
    Ok(registration_ref)
}

pub(crate) async fn bootstrap_pending_device(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    identity_signer: &UserKeypair,
    attempt_ref: DeviceJoinAttemptRef,
    verified_attempt: crate::sync::store_objects::VerifiedObject<DeviceJoinAttempt>,
    bootstrap_plan: crate::sync::store::pull::DeviceJoinBootstrapPlan,
    attempt_activation: StoreBatchCommitRef,
    owner: &StoreDeviceRegistration,
    published_at: &str,
) -> Result<DeviceReadinessProof, StoreRegistrationError> {
    if verified_attempt.semantic_hash != attempt_ref.attempt_hash
        || verified_attempt.object != attempt_ref.object
    {
        return Err(StoreRegistrationError::Invalid(
            "verified device join attempt differs from its exact reference".to_string(),
        ));
    }
    let attempt = verified_attempt.value;
    let activation_stream = attempt_activation.coord.stream_id.to_string();
    let verified_activation = bootstrap_plan
        .verified_commit(&attempt_activation)
        .cloned()
        .ok_or_else(|| {
            StoreRegistrationError::Invalid(
                "device join bootstrap omits its attempt activation".to_string(),
            )
        })?;
    Box::pin(database.install_device_join_bootstrap(attempt.store_root.clone(), bootstrap_plan))
        .await
        .map_err(database_error)?;
    if Box::pin(
        database.exact_materialized_ref(&activation_stream, attempt_activation.coord.sequence()),
    )
    .await
    .map_err(database_error)?
    .as_ref()
        != Some(&attempt_activation)
    {
        return Err(StoreRegistrationError::ActivationRequired);
    }
    let activation_commit = verified_activation.value();
    if verified_activation.author() != owner
        || activation_commit.author_registration != attempt.owner_registration
        || !activation_commit
            .device_join_attempt_decisions()
            .iter()
            .any(|decision| {
                matches!(
                    decision,
                    DeviceJoinAttemptDecisionRef::Attempt(reference)
                        if reference == &attempt_ref
                )
            })
        || activation_commit
            .order
            .predecessor_cut()
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
            != attempt.bootstrap_cut
        || activation_commit.membership_state != attempt.membership
    {
        return Err(StoreRegistrationError::Invalid(
            "device join attempt is not activated by the named exact Store commit".to_string(),
        ));
    }
    let provider = Box::pin(storage.provider_binding())
        .await
        .map_err(StoreObjectError::from)?;
    if provider.device != attempt.expected_registration.provider {
        return Err(StoreRegistrationError::Invalid(
            "joiner provider principal differs from the signed device join attempt".to_string(),
        ));
    }
    let expected_registration = attempt.expected_registration.clone();
    if expected_registration.author_pubkey != crate::keys::public_key_hex(identity_signer) {
        return Err(StoreRegistrationError::Invalid(
            "joiner identity differs from the signed device registration request".to_string(),
        ));
    }
    let existing = Box::pin(database.latest_local_store_device_registration())
        .await
        .map_err(database_error)?;
    if let Some(existing) = existing.as_ref() {
        if existing.registration_bytes != expected_registration.to_bytes()
            || existing.prepared.reference().slot() != &attempt.registration_slot
            || existing.initial_ack.value.store_cut != attempt.bootstrap_cut
        {
            return Err(StoreRegistrationError::Invalid(
                "local join journal owns different exact registration bytes".to_string(),
            ));
        }
    } else {
        let registration_prepared = prepare_registration_object(
            storage,
            &expected_registration,
            attempt.registration_slot.clone(),
        )?;
        let registration_ref =
            crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
                &expected_registration,
                registration_prepared.reference().clone(),
            );
        let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
            &expected_registration.acknowledgements
        else {
            return Err(StoreRegistrationError::Invalid(
                "join registration has no acknowledgement anchor".to_string(),
            ));
        };
        let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            attempt.store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let next_slot = Box::pin(storage.allocate_protocol_slot(
            &ack_context,
            &ack_slot_prefix(&expected_registration.device_id.to_string(), 2),
            ".json",
        ))
        .await
        .map_err(StoreObjectError::from)?;
        let device_signer = expected_registration
            .device_signer(identity_signer)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let (device_state, _) =
            Box::pin(database.store_device_state_for_history_cut(&attempt.bootstrap_cut))
                .await
                .map_err(database_error)?;
        let initial_ack = StoreAck::signed(
            attempt.store_root.store_root_hash,
            registration_ref.clone(),
            1,
            attempt.bootstrap_cut.clone(),
            device_state,
            None,
            StoreAckExclusionState {
                proposal_freezes: Vec::new(),
            },
            published_at.to_string(),
            SuccessorLink {
                activation: expected_registration
                    .store_acknowledgement_activation(&registration_ref)
                    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?
                    .activation_id(),
                predecessor: None,
                next_slot,
            },
            &device_signer,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let ack_prepared = storage
            .prepare_protocol_object(
                &ack_context,
                first_slot.clone(),
                &ack_slot_prefix(&expected_registration.device_id.to_string(), 1),
                initial_ack.to_bytes(),
            )
            .map_err(StoreObjectError::from)?;
        let initial_ack_ref = StoreAckRef {
            registration: registration_ref,
            sequence: 1,
            ack_hash: initial_ack.ack_hash(),
            object: ack_prepared.reference().clone(),
        };
        Box::pin(database.stage_local_store_device_registration(
            crate::database::ExactProtocolObject {
                value: expected_registration.clone(),
                bytes: expected_registration.to_bytes(),
                object: registration_prepared.reference().clone(),
                prepared: registration_prepared,
            },
            initial_ack_ref,
            crate::database::ExactProtocolObject {
                value: initial_ack.clone(),
                bytes: initial_ack.to_bytes(),
                object: ack_prepared.reference().clone(),
                prepared: ack_prepared,
            },
        ))
        .await
        .map_err(database_error)?;
    }
    Box::pin(drain_registration_outbox(database, storage)).await?;
    let durable = Box::pin(database.latest_local_store_device_registration())
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ActivationRequired)?;
    if !matches!(
        durable.state,
        crate::database::LocalDeviceRegistrationState::Created
            | crate::database::LocalDeviceRegistrationState::Activated { .. }
    ) {
        return Err(StoreRegistrationError::ActivationRequired);
    }
    let registration = StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        &attempt.store_root,
        durable.device_id,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    let registration_ref = crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let device_signer = registration
        .device_signer(identity_signer)
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    DeviceReadinessProof::signed(
        attempt_ref,
        registration_ref,
        durable.initial_ack_ref,
        attempt.bootstrap_cut,
        &registration,
        &device_signer,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))
}

async fn drain_registration_outbox(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
) -> Result<u64, StoreRegistrationError> {
    let store_root = database
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let mut published = 0_u64;
    while let Some(outbound) = database
        .oldest_unpublished_store_device_registration()
        .await
        .map_err(database_error)?
    {
        let registration = StoreDeviceRegistration::parse_at(
            &outbound.registration_bytes,
            &store_root,
            outbound.device_id,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        if registration.registration_hash() != outbound.registration_hash {
            return Err(StoreRegistrationError::Invalid(
                "durable registration columns differ from its exact signed bytes".to_string(),
            ));
        }
        let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            store_root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let semantic_prefix = registration_semantic_prefix(&outbound.device_id.to_string());
        storage
            .create_protocol_object(&outbound.prepared)
            .await
            .map_err(StoreObjectError::from)?;
        let opened = storage
            .read_protocol_object(&context, outbound.prepared.reference(), &semantic_prefix)
            .await
            .map_err(StoreObjectError::from)?;
        if opened != outbound.registration_bytes {
            return Err(StoreRegistrationError::Invalid(
                "Store registration exact readback differs from its durable bytes".to_string(),
            ));
        }
        let ack_context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
            store_root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        storage
            .create_protocol_object(&outbound.initial_ack.prepared)
            .await
            .map_err(StoreObjectError::from)?;
        let opened_ack = storage
            .read_protocol_object(
                &ack_context,
                &outbound.initial_ack_ref.object,
                &ack_slot_prefix(&outbound.device_id.to_string(), 1),
            )
            .await
            .map_err(StoreObjectError::from)?;
        if opened_ack != outbound.initial_ack.bytes {
            return Err(StoreRegistrationError::Invalid(
                "Store initial acknowledgement exact readback differs from its durable bytes"
                    .to_string(),
            ));
        }
        database
            .mark_local_store_device_registration_created(
                crate::database::ExactProtocolObject {
                    value: registration,
                    bytes: outbound.registration_bytes,
                    object: outbound.prepared.reference().clone(),
                    prepared: outbound.prepared,
                },
                outbound.initial_ack_ref,
                outbound.initial_ack,
            )
            .await
            .map_err(database_error)?;
        published = published.checked_add(1).ok_or_else(|| {
            StoreRegistrationError::Database("registration publish count exceeded u64".to_string())
        })?;
    }
    Ok(published)
}

async fn require_activated_registration(
    database: &StoreDatabase,
    storage: &dyn SyncStorage,
    durable: &crate::database::DurableDeviceRegistration,
) -> Result<(), StoreRegistrationError> {
    let root_ref = database
        .local_store_root_ref()
        .await
        .map_err(database_error)?
        .ok_or(StoreRegistrationError::ExactRootAuthorityMissing)?;
    let registration = StoreDeviceRegistration::parse_at(
        &durable.registration_bytes,
        &root_ref,
        durable.device_id,
    )
    .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
    if registration.registration_hash() != durable.registration_hash {
        return Err(StoreRegistrationError::Invalid(
            "local registration differs from its durable hash".to_string(),
        ));
    }
    let exact_ref = crate::sync::store_commit::StoreDeviceRegistrationRef::from_registration(
        &registration,
        durable.prepared.reference().clone(),
    );
    let crate::database::LocalDeviceRegistrationState::Activated { authority } = &durable.state
    else {
        return Err(StoreRegistrationError::ActivationRequired);
    };
    let activated = database
        .activated_store_device_registration_with_authority(exact_ref)
        .await
        .map_err(database_error)?;
    if activated != (registration.clone(), authority.clone()) {
        return Err(StoreRegistrationError::Invalid(
            "local registration differs from its exact activation authority".to_string(),
        ));
    }
    let live_provider = storage
        .provider_binding()
        .await
        .map_err(StoreObjectError::from)?;
    if live_provider.device != registration.provider {
        return Err(StoreRegistrationError::Invalid(
            "live provider principal differs from the founder registration".to_string(),
        ));
    }
    Ok(())
}

fn database_error(error: crate::database::DbError) -> StoreRegistrationError {
    StoreRegistrationError::Database(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::test_helpers::{open_test_db, TestStore};

    fn founder_recovery_authority(
        store: &TestStore,
    ) -> crate::sync::restore_code::OwnerRecoveryAuthority {
        let owner_grant = store.protocol_root.descriptor.founder_grant.clone();
        let activation = crate::sync::store_commit::OwnerRecoveryActivationId::derive(
            &store.root,
            &crate::keys::public_key_hex(&store.signer),
            &owner_grant,
            &store.protocol_root.descriptor.founder_recovery,
        )
        .expect("derive founder recovery activation");
        crate::sync::restore_code::OwnerRecoveryAuthority {
            owner_identity_secret: hex::encode(store.signer.to_keypair_bytes()),
            owner_grant: owner_grant.clone(),
            recovery: crate::sync::store_commit::OwnerRecoveryCursor {
                owner_grant,
                position: OwnerRecoveryPosition::BeforeFirst { activation },
            },
            published_at: "2026-07-17T00:00:00Z".to_string(),
        }
    }

    async fn initialized() -> (TestStore, Database) {
        let signer = UserKeypair::generate();
        let db = open_test_db();
        let store = TestStore::create(&db, "registration-store-test", signer)
            .await
            .expect("create exact registration test Store");
        (store, db)
    }

    async fn recovered_author() -> (
        TestStore,
        Database,
        StoreDeviceRegistrationRef,
        StoreBatchCommitRef,
    ) {
        let (store, db) = initialized().await;
        let membership = crate::sync::store::pull::load_cycle_membership(
            &store.storage,
            &crate::sync::store::database::StoreDatabase::new(&db),
        )
        .await
        .expect("load exact membership");
        let authority = founder_recovery_authority(&store);
        let database = StoreDatabase::new(&db);
        let registration = recover_owner_device(
            &database,
            &store.storage,
            &store.signer,
            &authority,
            &membership,
        )
        .await
        .expect("recover Owner device");
        let mut commit_verifier =
            crate::sync::store::pull::StoreCommitVerifier::new(&store.storage, &store.root)
                .await
                .expect("create Store commit verifier");
        for reference in database
            .materialized_frontier()
            .await
            .expect("load materialized Store frontier")
            .into_values()
        {
            let commit = commit_verifier
                .load_ref(&reference)
                .await
                .expect("load materialized recovery commit");
            if commit.value().author_registration == registration {
                return (store, db, registration, reference);
            }
        }
        panic!("recovery commit is materialized")
    }

    #[derive(Clone, Copy)]
    enum RetainedRegistrationTamper {
        CanonicalRegistration,
        ActivationAuthority,
    }

    async fn tamper_retained_recovery_registration(
        db: &Database,
        reference: &StoreBatchCommitRef,
        tamper: RetainedRegistrationTamper,
    ) {
        let StoreCommitCoord {
            stream_id,
            sequence,
        } = &reference.coord;
        let stream_id = stream_id.to_string();
        let sequence = i64::try_from(*sequence).expect("recovery sequence fits SQLite");
        db.call(move |conn| {
            let (commit_ref, canonical_input): (String, Vec<u8>) = conn
                .query_row(
                    "SELECT commit_ref, canonical_input
                     FROM retained_merge_materializations
                     WHERE device_id = ?1 AND seq = ?2",
                    (&stream_id, sequence),
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .map_err(crate::database::DbError::from)?;
            let mut input: serde_json::Value = serde_json::from_slice(&canonical_input)
                .expect("parse retained recovery materialization");
            let registration = input
                .get_mut("activation")
                .and_then(|value| value.get_mut("registrations"))
                .and_then(|value| value.get_mut("registrations"))
                .and_then(serde_json::Value::as_array_mut)
                .and_then(|values| values.first_mut())
                .expect("retained recovery registration");
            match tamper {
                RetainedRegistrationTamper::CanonicalRegistration => registration
                    .get_mut("canonical_registration")
                    .and_then(serde_json::Value::as_array_mut)
                    .expect("canonical recovery registration bytes")
                    .push(serde_json::Value::from(b' ')),
                RetainedRegistrationTamper::ActivationAuthority => {
                    let recovery = registration
                        .get_mut("authority")
                        .and_then(|value| value.get_mut("recovery"))
                        .and_then(serde_json::Value::as_object_mut)
                        .expect("retained recovery authority");
                    recovery.insert(
                        "recovery_id".to_string(),
                        serde_json::Value::String("0".repeat(64)),
                    );
                }
            }
            let canonical_input = serde_json::to_vec(&input)
                .expect("serialize tampered retained recovery materialization");
            let input_hash = ObjectHash::digest(&canonical_input).to_string();
            let tx = conn
                .unchecked_transaction()
                .map_err(crate::database::DbError::from)?;
            tx.execute(
                "DELETE FROM materialized_commits WHERE device_id = ?1 AND seq = ?2",
                (&stream_id, sequence),
            )
            .map_err(crate::database::DbError::from)?;
            tx.execute(
                "UPDATE retained_merge_materializations
                 SET input_hash = ?3, canonical_input = ?4
                 WHERE device_id = ?1 AND seq = ?2",
                rusqlite::params![&stream_id, sequence, &input_hash, &canonical_input],
            )
            .map_err(crate::database::DbError::from)?;
            tx.execute(
                "INSERT INTO materialized_commits
                 (device_id, seq, commit_ref, retained_commit_ref, retained_input_hash)
                 VALUES (?1, ?2, ?3, ?3, ?4)",
                rusqlite::params![&stream_id, sequence, &commit_ref, &input_hash],
            )
            .map_err(crate::database::DbError::from)?;
            tx.commit().map_err(crate::database::DbError::from)
        })
        .await
        .expect("install tampered retained recovery registration");
    }

    #[tokio::test]
    async fn store_root_state_failures_keep_registration_error_variants() {
        let db = open_test_db();
        let database = StoreDatabase::new(&db);
        let store = TestStore::for_store("registration-missing-root-storage").await;

        assert!(matches!(
            drain_registration_outbox(&database, &store.storage).await,
            Err(StoreRegistrationError::ExactRootAuthorityMissing)
        ));
    }

    #[tokio::test]
    async fn exact_founder_registration_is_already_activated() {
        let (store, db) = initialized().await;
        let database = StoreDatabase::new(&db);
        ensure_active_registration(&database, &store.storage)
            .await
            .expect("founder registration remains active");
        let activated = database
            .activated_store_device_registrations()
            .await
            .unwrap();
        assert_eq!(activated.len(), 1);
        assert_eq!(activated[0].store_root, store.root);
    }

    #[tokio::test]
    async fn owner_recovery_publishes_and_activates_replacement_device() {
        let (store, db) = initialized().await;
        let membership = crate::sync::store::pull::load_cycle_membership(
            &store.storage,
            &crate::sync::store::database::StoreDatabase::new(&db),
        )
        .await
        .expect("load exact membership");
        let authority = founder_recovery_authority(&store);
        let database = StoreDatabase::new(&db);
        let registration = recover_owner_device(
            &database,
            &store.storage,
            &store.signer,
            &authority,
            &membership,
        )
        .await
        .expect("recover Owner device");

        let durable = database
            .latest_local_store_device_registration()
            .await
            .expect("load replacement registration")
            .expect("replacement registration exists");
        assert_eq!(durable.device_id, registration.device_id);
        assert!(durable.is_activated());
        ensure_active_registration(&database, &store.storage)
            .await
            .expect("replacement registration is usable");
    }

    #[tokio::test]
    async fn recovery_materialization_reopens_its_retained_introduced_author() {
        let (_store, db, registration, reference) = recovered_author().await;
        let device_id = registration.device_id.to_string();
        let registration_hash = registration.registration_hash.to_string();
        db.call(move |conn| {
            conn.execute(
                "UPDATE store_device_registration_activations
                 SET registration_bytes = X'00'
                 WHERE device_id = ?1 AND registration_hash = ?2",
                (&device_id, &registration_hash),
            )
            .map_err(crate::database::DbError::from)?;
            Ok(())
        })
        .await
        .expect("corrupt activated recovery registration fixture");

        let frontier = crate::sync::store::database::StoreDatabase::new(&db)
            .materialized_frontier()
            .await
            .expect("retained recovery author does not depend on mutable registration rows");
        let StoreCommitCoord { stream_id, .. } = &reference.coord;
        assert_eq!(frontier.get(&stream_id.to_string()), Some(&reference));
    }

    #[tokio::test]
    async fn recovery_materialization_rejects_tampered_retained_registration_bytes() {
        let (_store, db, _registration, reference) = recovered_author().await;
        tamper_retained_recovery_registration(
            &db,
            &reference,
            RetainedRegistrationTamper::CanonicalRegistration,
        )
        .await;

        db.call(|conn| StoreDatabase::load_retained_merge_replay_inputs_on(conn).map(drop))
            .await
            .expect_err(
                "tampered retained recovery registration bytes must fail durable history verification",
            );
    }

    #[tokio::test]
    async fn recovery_materialization_rejects_tampered_retained_registration_authority() {
        let (_store, db, _registration, reference) = recovered_author().await;
        tamper_retained_recovery_registration(
            &db,
            &reference,
            RetainedRegistrationTamper::ActivationAuthority,
        )
        .await;

        db.call(|conn| StoreDatabase::load_retained_merge_replay_inputs_on(conn).map(drop))
            .await
            .expect_err(
                "tampered retained recovery registration authority must fail durable history verification",
            );
    }

    #[tokio::test]
    async fn owner_recovery_retry_reuses_each_published_readiness_prefix() {
        for failed_call in [2, 3, 4] {
            let signer = UserKeypair::generate();
            let db = open_test_db();
            let store = TestStore::create(&db, &format!("recovery-prefix-{failed_call}"), signer)
                .await
                .expect("create recovery prefix Store");
            let membership = crate::sync::store::pull::load_cycle_membership(
                &store.storage,
                &crate::sync::store::database::StoreDatabase::new(&db),
            )
            .await
            .expect("load exact membership");
            let authority = founder_recovery_authority(&store);
            let database = StoreDatabase::new(&db);
            store.home.fail_exact_create_before_call(failed_call);
            assert!(
                recover_owner_device(
                    &database,
                    &store.storage,
                    &store.signer,
                    &authority,
                    &membership,
                )
                .await
                .is_err(),
                "failure before exact create {failed_call} interrupts recovery",
            );

            let interrupted = database
                .latest_local_store_device_registration()
                .await
                .expect("read interrupted recovery journal")
                .expect("interrupted recovery journal exists");
            let interrupted_node = if failed_call == 4 {
                let registration = StoreDeviceRegistration::parse_at(
                    &interrupted.registration_bytes,
                    &store.root,
                    interrupted.device_id,
                )
                .expect("parse interrupted recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin
                else {
                    panic!("interrupted registration is not a Recovery registration");
                };
                let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                Some(
                    store
                        .storage
                        .read_prepared_protocol_slot(
                            &context,
                            &recovery_slot,
                            &owner_recovery_semantic_prefix(
                                &crate::keys::public_key_hex(&store.signer),
                                authority.owner_grant.clone(),
                                1,
                            ),
                        )
                        .await
                        .expect("read published recovery node")
                        .1,
                )
            } else {
                None
            };

            recover_owner_device(
                &database,
                &store.storage,
                &store.signer,
                &authority,
                &membership,
            )
            .await
            .expect("retry completes absent recovery suffix");
            assert_eq!(
                store.home.exact_create_count(),
                6,
                "retry after boundary {failed_call} creates only the absent suffix",
            );
            let completed = database
                .latest_local_store_device_registration()
                .await
                .expect("read completed recovery journal")
                .expect("completed recovery journal exists");
            assert_eq!(
                completed.prepared.reference(),
                interrupted.prepared.reference(),
            );
            assert_eq!(
                completed.prepared.stored_bytes(),
                interrupted.prepared.stored_bytes(),
            );
            if failed_call >= 3 {
                assert_eq!(
                    completed.initial_ack.prepared.reference(),
                    interrupted.initial_ack.prepared.reference(),
                );
                assert_eq!(
                    completed.initial_ack.prepared.stored_bytes(),
                    interrupted.initial_ack.prepared.stored_bytes(),
                );
            }
            if let Some(interrupted_node) = interrupted_node {
                let registration = StoreDeviceRegistration::parse_at(
                    &completed.registration_bytes,
                    &store.root,
                    completed.device_id,
                )
                .expect("parse completed recovery registration");
                let StoreDeviceRegistrationOrigin::Recovery { recovery_slot, .. } =
                    registration.origin
                else {
                    panic!("completed registration is not a Recovery registration");
                };
                let context = crate::sync::storage::ProtocolObjectContext::signed_plaintext(
                    store.root.store_root_hash,
                    ProtocolObjectDomain::OwnerRecoveryNode,
                );
                let completed_node = store
                    .storage
                    .read_prepared_protocol_slot(
                        &context,
                        &recovery_slot,
                        &owner_recovery_semantic_prefix(
                            &crate::keys::public_key_hex(&store.signer),
                            authority.owner_grant.clone(),
                            1,
                        ),
                    )
                    .await
                    .expect("read completed recovery node")
                    .1;
                assert_eq!(completed_node.reference(), interrupted_node.reference());
                assert_eq!(
                    completed_node.stored_bytes(),
                    interrupted_node.stored_bytes(),
                );
            }
        }
    }
}
