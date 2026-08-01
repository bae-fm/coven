use std::collections::BTreeMap;
use std::path::PathBuf;

use rusqlite::Connection;
use tokio::sync::watch;
use tracing::{info, warn};

use super::*;
use crate::protocol::store_commit::{
    ack_slot_prefix, commit_semantic_prefix, head_slot_prefix, owner_recovery_semantic_prefix,
    registration_semantic_prefix, snapshot_slot_prefix, ActivatedStoreDeviceRegistrationRef,
    DeviceRecoveryId, DeviceRecoveryReadiness, DeviceStreamAnchor, ObjectHash, OwnerRecoveryNode,
    OwnerRecoveryNodeRef, OwnerRecoveryPosition, StoreAck, StoreAckExclusionState, StoreAckRef,
    StoreBatchCommit, StoreBatchCommitRef, StoreCommitCoord, StoreCommitOrder, StoreDeviceHead,
    StoreDeviceRegistration, StoreDeviceRegistrationActivation,
    StoreDeviceRegistrationActivationRef, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut,
    StoreOperationMembershipAuthority, SuccessorLink,
};
use crate::storage::StoreObjectError;
use crate::storage::{PreparedExactObject, ProtocolObjectDomain};

/// A snapshot-installed Store that retains the exact remote authority used to
/// verify the image. Restore-only operations consume this authority directly
/// instead of reconstructing it from database rows and cloud objects.
pub(crate) struct RestoringStore<'storage> {
    history: AuthorizedStoreHistory<'storage>,
    database: StoreDatabase,
    storage: &'storage dyn SyncStorage,
    root: StoreRootRef,
    protocol: StoreProtocolRoot,
    membership: crate::protocol::membership::MembershipChain,
    identity: UserKeypair,
    target_path: PathBuf,
}

impl<'storage> RestoringStore<'storage> {
    async fn prepare_or_load_recovery_registration(
        &self,
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
        let root = &self.root;
        let context = crate::storage::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        match self
            .storage
            .read_prepared_protocol_slot(&context, &slot, semantic_prefix)
            .await
        {
            Ok((bytes, prepared)) => {
                let registration =
                    StoreDeviceRegistration::parse_at(&bytes, root, expected.device_id)
                        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
                if registration != expected {
                    return Err(StoreRegistrationError::Invalid(
                        "existing Owner recovery registration differs from its exact authority"
                            .into(),
                    ));
                }
                let reference = StoreDeviceRegistrationRef::from_registration(
                    &registration,
                    prepared.reference().clone(),
                );
                Ok((registration, reference, prepared, true))
            }
            Err(crate::storage::StorageError::NotFound(_)) => {
                let prepared = self
                    .storage
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
        &self,
        context: &crate::storage::ProtocolObjectContext,
        prepared: &PreparedExactObject,
        semantic_prefix: &str,
        expected_bytes: &[u8],
    ) -> Result<bool, StoreRegistrationError> {
        match self
            .storage
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
            Err(crate::storage::StorageError::NotFound(_)) => Ok(false),
            Err(error) => Err(StoreObjectError::from(error).into()),
        }
    }

    async fn prepare_or_load_initial_recovery_ack(
        &self,
        registration: &StoreDeviceRegistration,
        registration_ref: &StoreDeviceRegistrationRef,
        first_slot: crate::storage::cloud::ObjectSlot,
        store_cut: StoreHistoryCut,
        device_state: StoreDeviceStateRef,
        published_at: &str,
        device_signer: &UserKeypair,
    ) -> Result<(StoreAck, Vec<u8>, StoreAckRef, PreparedExactObject, bool), StoreRegistrationError>
    {
        let root = &self.root;
        let storage = self.storage;
        let context = crate::storage::ProtocolObjectContext::signed_plaintext(
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
            Err(crate::storage::StorageError::NotFound(_)) => {
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
        &self,
        recovery_slot: crate::storage::cloud::ObjectSlot,
        owner_pubkey: &str,
        owner_grant: &crate::protocol::membership::MembershipGrantId,
        sequence: u64,
        recovery_id: DeviceRecoveryId,
        membership: &crate::protocol::circle_control::StoreMembershipStateRef,
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
        let root = &self.root;
        let storage = self.storage;
        let context = crate::storage::ProtocolObjectContext::signed_plaintext(
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
            Err(crate::storage::StorageError::NotFound(_)) => {
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
        &self,
        origin: &StoreDeviceRegistrationOrigin,
        device_id: crate::protocol::store_commit::StoreDeviceId,
        recovery_id: DeviceRecoveryId,
        recovery_slot: &crate::storage::cloud::ObjectSlot,
        owner_pubkey: &str,
        owner_grant: &crate::protocol::membership::MembershipGrantId,
        sequence: u64,
        predecessor: &Option<OwnerRecoveryNodeRef>,
    ) -> Result<Option<StoreDeviceRegistrationRef>, StoreRegistrationError> {
        let database = &self.database;
        let history = self.history.restore_history();
        let storage = self.storage;
        let root = &self.root;
        let Some((registration_ref, registration, activation)) = database
            .activated_store_device_registration_for_device(device_id)
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?
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
                "activated Owner recovery registration belongs to another provider principal"
                    .into(),
            ));
        }
        let node = history.load_owner_recovery_node(&node_ref).await?;
        if node.value.recovery_id != recovery_id
            || node.value.predecessor != *predecessor
            || node.value.readiness.registration != registration_ref
        {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery node differs from the requested authority".into(),
            ));
        }
        let initial_ack_ref = node.value.readiness.initial_ack;
        let initial_ack = history
            .load_store_ack(&initial_ack_ref, &registration)
            .await?;
        if initial_ack.value.store_cut != node.value.readiness.bootstrap_cut {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery acknowledgement differs from its recovery node".into(),
            ));
        }

        let registration_context = crate::storage::ProtocolObjectContext::signed_plaintext(
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
                "activated Owner recovery registration differs from its prepared exact object"
                    .into(),
            ));
        }
        let ack_context = crate::storage::ProtocolObjectContext::signed_plaintext(
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
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
        if !already_activated {
            return Err(StoreRegistrationError::Invalid(
                "activated Owner recovery disappeared while installing its local journal".into(),
            ));
        }
        Ok(Some(registration_ref))
    }

    pub(crate) async fn recover_owner_device(
        &mut self,
        authority: &crate::restoration::OwnerRecoveryAuthority,
    ) -> Result<StoreDeviceRegistrationRef, StoreRegistrationError> {
        let database = self.database.clone();
        let storage = self.storage;
        let identity_signer = &self.identity;
        let membership = &self.membership;
        let root = self.root.clone();
        let protocol = self.protocol.clone();
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
        let crate::protocol::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
            &protocol.descriptor.founder_recovery
        else {
            return Err(StoreRegistrationError::Invalid(
                "Store root has no founder recovery stream".into(),
            ));
        };
        let (recovery_slot, predecessor, sequence) = match &authority.recovery.position {
            OwnerRecoveryPosition::BeforeFirst { activation } => {
                let expected = crate::protocol::store_commit::OwnerRecoveryActivationId::derive(
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
                let loaded = self
                    .history
                    .restore_history()
                    .load_owner_recovery_node(node)
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
        let device_id = crate::protocol::store_commit::StoreDeviceId::derive(&root, &origin);
        if let Some(registration) = self
            .install_activated_owner_recovery(
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
            crate::storage::ProtocolObjectContext::signed_plaintext(root.store_root_hash, domain)
        };
        let commit_context = context(ProtocolObjectDomain::StoreCommit);
        let staged = database
            .latest_local_store_device_registration()
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?
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
            let registration_exists = self
                .prepared_protocol_object_exists(
                    &context(crate::storage::ProtocolObjectDomain::StoreDeviceRegistration),
                    &durable.prepared,
                    &registration_semantic_prefix(&device_id.to_string()),
                    &durable.registration_bytes,
                )
                .await?;
            let initial_ack_exists = self
                .prepared_protocol_object_exists(
                    &context(crate::storage::ProtocolObjectDomain::StoreAck),
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
            let head_context = context(crate::storage::ProtocolObjectDomain::StoreHead);
            let ack_context = context(crate::storage::ProtocolObjectDomain::StoreAck);
            let snapshot_context = context(crate::storage::ProtocolObjectDomain::StoreSnapshotMeta);
            let registration_context =
                context(crate::storage::ProtocolObjectDomain::StoreDeviceRegistration);
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
            let (registration, registration_ref, registration_prepared, registration_exists) = self
                .prepare_or_load_recovery_registration(
                    expected_registration,
                    registration_slot,
                    &registration_prefix,
                )
                .await?;
            let dependencies = database
                .materialized_frontier()
                .await
                .map_err(|error| StoreRegistrationError::Database(error.to_string()))?
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
                .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
            let (
                initial_ack,
                initial_ack_bytes,
                initial_ack_ref,
                initial_ack_prepared,
                initial_ack_exists,
            ) = self
                .prepare_or_load_initial_recovery_ack(
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
        let crate::protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(StoreRegistrationError::Invalid(
                "Owner recovery requires resolved membership".into(),
            ));
        };
        let membership_state =
            crate::protocol::circle_control::StoreMembershipStateRef::from_parts(
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
        let (_node, node_ref, node_prepared, node_exists) = self
            .prepare_or_load_owner_recovery_node(
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
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
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
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;

        let stream_id =
            crate::protocol::store_commit::StreamActivation::device_authorized_stream_id(
                root.store_root_hash,
                &registration_ref,
                crate::protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
            );
        let order = StoreCommitOrder {
            seq: 1,
            predecessor: None,
            dependencies,
        };
        let (device_state, predecessor_state) = database
            .store_device_state_for_order(&order)
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
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
        let verified_commit = crate::protocol::store_commit::VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            root.store_root_hash,
            &commit_ref,
            &registration,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let state_after = predecessor_state
            .activate_registration(
                registration_ref.clone(),
                Some(crate::protocol::store_commit::OwnerRecoveryCursor {
                    owner_grant: authority.owner_grant.clone(),
                    position: OwnerRecoveryPosition::At {
                        node: node_ref.clone(),
                    },
                }),
            )
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let prepared_history = self
            .history
            .prepare_merge_history_successor(
                &verified_commit,
                membership,
                Some(&registration_ref),
                state_after,
                crate::sync::store::owner::verified_history::MergeHistorySuccessorEvidence {
                    registrations: vec![
                        crate::protocol::store_commit::RetainedVerifiedRegistration {
                            reference: registration_ref.clone(),
                            value: registration.clone(),
                        },
                    ],
                    acknowledgement: None,
                    membership_proof: None,
                },
            )
            .await
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let head_context = context(crate::storage::ProtocolObjectDomain::StoreHead);
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
            prepared_history.summary.digest(),
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
                prepared_history.head_slot,
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
                prepared_history.summary,
                registration,
                registration_activation,
            )
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
        Ok(registration_ref)
    }

    pub(crate) async fn begin_device_join(
        self,
        pending: &crate::sync::store::DeviceJoinJournalDatabase,
        offer: crate::sync::store::DeviceJoinOffer,
    ) -> Result<crate::sync::store::JoiningStore<'storage>, crate::sync::store::DeviceJoinError>
    {
        crate::sync::store::JoiningStore::begin_from_restored_history(
            self.history,
            self.identity,
            pending,
            offer,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn from_parts(
        history: AuthorizedStoreHistory<'storage>,
        database: StoreDatabase,
        storage: &'storage dyn SyncStorage,
        root: StoreRootRef,
        protocol: StoreProtocolRoot,
        membership: crate::protocol::membership::MembershipChain,
        identity: UserKeypair,
        target_path: PathBuf,
    ) -> Self {
        Self {
            history,
            database,
            storage,
            root,
            protocol,
            membership,
            identity,
            target_path,
        }
    }

    #[cfg(test)]
    pub(crate) async fn install_device_join_bootstrap_for_test(
        &self,
        plan: crate::sync::DeviceJoinBootstrapPlan,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .install_device_join_bootstrap(self.root.clone(), plan)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn installed_store_root_for_test(
        &self,
    ) -> Result<Option<StoreRootRef>, crate::database::DbError> {
        self.database.local_store_root_ref().await
    }

    #[cfg(test)]
    pub(crate) async fn generation_zero_replay_baseline_for_test(
        &self,
    ) -> Result<crate::sync::RetainedReplayBaseline, crate::database::DbError> {
        self.database
            .generation_zero_replay_baseline_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn replace_generation_zero_replay_authority_for_test(
        &self,
        authority_bytes: Vec<u8>,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .replace_generation_zero_replay_authority_for_test(authority_bytes)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn read_local_blob_for_test(
        &self,
        store_dir: &crate::store_dir::StoreDir,
        table: &str,
        row_id: &str,
    ) -> Result<Vec<u8>, crate::sync::BlobCacheError> {
        let reference = self.database.row_blob_ref(table, row_id).await?;
        crate::sync::test_owner_graph::TestOwnerGraph::new(self.database.clone(), store_dir.clone())
            .local_access()
            .read(&reference)
            .await
    }

    #[cfg(test)]
    pub(crate) fn schema_version_for_test(&self) -> u32 {
        self.database.schema_version()
    }

    #[cfg(test)]
    pub(crate) async fn scoped_snapshot_counts_for_test(
        &self,
    ) -> Result<(i64, i64, i64), crate::database::DbError> {
        self.database.scoped_snapshot_counts_for_test().await
    }

    #[cfg(test)]
    pub(crate) async fn migrated_scoped_snapshot_facts_for_test(
        &self,
    ) -> Result<(i64, i64, String), crate::database::DbError> {
        self.database
            .migrated_scoped_snapshot_facts_for_test()
            .await
    }

    #[cfg(test)]
    pub(crate) async fn materialized_frontier_for_test(
        &self,
    ) -> Result<
        BTreeMap<String, crate::protocol::store_commit::StoreBatchCommitRef>,
        crate::database::DbError,
    > {
        self.database.materialized_frontier().await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_coverage_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<Option<crate::protocol::circle::CircleBootstrapCoverageRef>, crate::database::DbError>
    {
        self.database.circle_bootstrap_coverage_ref(circle_id).await
    }

    #[cfg(test)]
    pub(crate) async fn circle_control_activation_count_for_test(
        &self,
        circle_id: crate::protocol::circle::CircleId,
    ) -> Result<i64, crate::database::DbError> {
        self.database
            .circle_control_activation_count_for_test(circle_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn circle_bootstrap_replay_inputs_for_test(
        &self,
    ) -> Result<
        Vec<(
            crate::protocol::store_commit::StoreBatchCommitRef,
            crate::sync::VerifiedCircleImage,
        )>,
        crate::database::DbError,
    > {
        self.database.circle_bootstrap_replay_inputs().await
    }

    #[cfg(test)]
    pub(crate) async fn transfer_prepared_write_from_for_test(
        &self,
        source: &StoreDatabase,
        write_id: &crate::WriteId,
    ) -> Result<(), crate::database::DbError> {
        source
            .transfer_prepared_write_to_for_test(&self.database, write_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn blocked_merge_candidate_for_test(
        &self,
        write_id: crate::WriteId,
    ) -> Result<Option<crate::database::BlockedMergeCandidate>, crate::database::DbError> {
        self.database.blocked_merge_candidate(write_id).await
    }

    #[cfg(test)]
    pub(crate) async fn tamper_author_exclusion_locator_for_test(
        &self,
        exclusion: &crate::protocol::store_commit::StoreDeviceExclusionRef,
        candidate: &crate::protocol::store_commit::StoreBatchCommitRef,
        tamper: crate::database::AuthorExclusionLocatorTamper,
    ) -> Result<(), crate::database::DbError> {
        self.database
            .tamper_author_exclusion_locator_for_test(exclusion, candidate, tamper)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn author_exclusion_activation_for_candidate_for_test(
        &self,
        candidate: crate::protocol::store_commit::StoreBatchCommitRef,
        author: crate::protocol::store_commit::StoreDeviceRegistrationRef,
    ) -> Result<Option<crate::database::AuthorExclusionActivationLocator>, crate::database::DbError>
    {
        self.database
            .author_exclusion_activation_for_candidate(self.root.clone(), candidate, author)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn author_exclusion_activation_evidence_for_test(
        &self,
        exclusion: &crate::protocol::store_commit::StoreDeviceExclusionRef,
    ) -> Result<(String, String), crate::database::DbError> {
        self.database
            .author_exclusion_activation_evidence_for_test(exclusion)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn merge_candidate_cleanup_pending_for_test(
        &self,
        write_id: &crate::WriteId,
    ) -> Result<bool, crate::database::DbError> {
        self.database
            .merge_candidate_cleanup_pending(write_id)
            .await
    }

    #[cfg(test)]
    pub(crate) async fn abandon_merge_candidate_for_test(
        &mut self,
        write_id: crate::WriteId,
    ) -> Result<
        crate::sync::store::owner::history::abandonment::MergeCandidateAbandonment,
        StoreError,
    > {
        self.history
            .abandon_excluded_merge_candidate(write_id)
            .await
            .and_then(|result| {
                result.ok_or_else(|| {
                    StoreError::InvalidOutbound(
                        "restored candidate has no verified exclusion authority".to_string(),
                    )
                })
            })
    }

    pub(crate) async fn pull(
        &mut self,
        store_dir: &crate::store_dir::StoreDir,
        routing_encryption: Option<&crate::encryption::EncryptionService>,
    ) -> Result<pull::StorePullResult, pull::StorePullError> {
        let execution = self
            .history
            .pull(
                store_dir,
                &self.membership,
                Some(&self.identity),
                routing_encryption,
            )
            .await?;
        self.membership = execution.membership;
        Ok(execution.result)
    }

    pub(crate) async fn install_activated_device_continuation(
        &self,
        continuation: crate::restoration::ActivatedContinuation,
    ) -> Result<(), StoreRegistrationError> {
        let registration = crate::protocol::store_commit::StoreDeviceRegistration::parse_at(
            &continuation.registration_bytes,
            &self.root,
            continuation.registration.device_id,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let device_signer = registration
            .device_signer(&self.identity)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let history = self.history.restore_history();
        let latest = history
            .load_store_ack(&continuation.latest_ack, &registration)
            .await?;
        let chain = history
            .load_acknowledgement_proof_chain(
                continuation.latest_ack.clone(),
                latest.value,
                &registration,
            )
            .await
            .map_err(|error| match error {
                super::verified_history::registration::RegistrationLoadError::Object(error) => {
                    StoreRegistrationError::Object(error)
                }
                super::verified_history::registration::RegistrationLoadError::Invalid(error) => {
                    StoreRegistrationError::Invalid(error)
                }
            })?
            .into_iter()
            .rev()
            .map(|(_, value)| value)
            .collect();
        let latest_snapshot = match &continuation.latest_snapshot {
            Some(reference) => Some(
                history
                    .load_store_snapshot(&continuation.registration, &registration, reference)
                    .await?,
            ),
            None => None,
        };
        self.database
            .install_activated_device_continuation(
                continuation,
                &self.identity,
                &device_signer,
                chain,
                latest_snapshot,
            )
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))
    }

    pub(crate) async fn reconcile_snapshot_blobs(
        &self,
        store_dir: &crate::store_dir::StoreDir,
        cancel: &watch::Receiver<bool>,
    ) -> Result<super::writer::snapshot::SnapshotBlobReconcile, crate::database::DbError> {
        let db = &self.database;
        let row_ids: Vec<(String, String)> = {
            let conn =
                Connection::open(&self.target_path).map_err(crate::database::DbError::from)?;
            let mut row_ids = Vec::new();
            for table in db.synced_tables() {
                let Some(declaration) = table.blob() else {
                    continue;
                };
                if declaration.fill != crate::blob::CacheFill::CacheEager {
                    continue;
                }
                let sql = format!(
                    "SELECT id FROM {} WHERE {} IS NOT NULL ORDER BY id",
                    crate::database::quote_ident(table.name()),
                    crate::database::quote_ident(&declaration.id_column),
                );
                let mut statement = conn.prepare(&sql).map_err(crate::database::DbError::from)?;
                let ids = statement
                    .query_map([], |row| row.get::<_, String>(0))
                    .map_err(crate::database::DbError::from)?
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(crate::database::DbError::from)?;
                row_ids.extend(ids.into_iter().map(|id| (table.name().to_string(), id)));
            }
            row_ids
        };

        let mut blobs = Vec::with_capacity(row_ids.len());
        for (table, row_id) in row_ids {
            let reference = db.row_blob_ref(&table, &row_id).await?;
            blobs.push(
                BlobDownload::from_row(reference).map_err(crate::database::DbError::Message)?,
            );
        }

        if blobs.is_empty() {
            return Ok(super::writer::snapshot::SnapshotBlobReconcile::Complete);
        }

        let total = blobs.len();
        let blob_cache =
            crate::sync::store::blob::StoreBlobCache::new(self.database.clone(), store_dir.clone());
        let mut all_ok = true;
        for blob in blobs {
            if *cancel.borrow() {
                info!(total, "snapshot blob reconciliation cancelled");
                return Ok(super::writer::snapshot::SnapshotBlobReconcile::Cancelled);
            }
            if self.download_blob(blob, &blob_cache).await.is_err() {
                all_ok = false;
            }
        }
        if all_ok {
            info!(total, "snapshot blob reconciliation complete");
            Ok(super::writer::snapshot::SnapshotBlobReconcile::Complete)
        } else {
            warn!(total, "some snapshot blob files are not local");
            Ok(super::writer::snapshot::SnapshotBlobReconcile::Incomplete)
        }
    }

    async fn download_blob(
        &self,
        download: BlobDownload,
        blob_cache: &crate::sync::store::blob::StoreBlobCache,
    ) -> Result<(), pull::BlobDownloadFailure> {
        let BlobDownload { authority, stored } = download;
        let namespace = stored.locator().namespace();
        let id = stored.locator().blob_id();
        self.history
            .verify_blob_plaintext(blob_cache, &authority, &stored, true)
            .await
            .map_err(|cause| {
                warn!(id, namespace, error = %cause, "failed to verify snapshot blob");
                pull::BlobDownloadFailure {
                    namespace: namespace.to_string(),
                    id: id.to_string(),
                    cause,
                }
            })
    }
}
