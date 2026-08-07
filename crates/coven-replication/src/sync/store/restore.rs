use std::collections::BTreeMap;

use tokio::sync::watch;
use tracing::{info, warn};

use super::owner::authorized_history::AuthorizedStoreHistory;
use super::pull;
use super::*;
use coven_protocol::objects::StoreObjectError;
use coven_protocol::objects::{PreparedExactObject, ProtocolObjectDomain};
use coven_protocol::store_commit::StoreRootRef;
use coven_protocol::store_commit::{
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

/// A snapshot-installed Store that retains the exact remote authority used to
/// verify the image. Restore-only operations consume this authority directly
/// instead of reconstructing it from database rows and cloud objects.
mod continuation;
mod history;
use recovery_preparation::*;
mod recovery_preparation;
mod restore_test_support;

pub(crate) use history::RestoreHistory;

pub struct RestoringStore<'storage> {
    history: AuthorizedStoreHistory<'storage>,
    database: StoreDatabase,
    storage: &'storage dyn SyncStorage,
    root: StoreRootRef,
    protocol: StoreProtocolRoot,
    membership: coven_protocol::membership::MembershipChain,
    identity: UserKeypair,
}

impl<'storage> RestoringStore<'storage> {
    pub async fn recover_owner_device(
        &mut self,
        authority: &coven_protocol::recovery::OwnerRecoveryAuthority,
    ) -> Result<StoreDeviceRegistrationRef, StoreRegistrationError> {
        let database = self.database.clone();
        let storage = self.storage;
        let identity_signer = &self.identity;
        let membership = &self.membership;
        let root = self.root.clone();
        let protocol = self.protocol.clone();
        let owner_pubkey = coven_keys::keys::public_key_hex(identity_signer);
        if owner_pubkey != protocol.descriptor.founder_pubkey
            || authority.owner_grant != protocol.descriptor.founder_grant
            || membership.active_owner_grant(&owner_pubkey).as_ref() != Some(&authority.owner_grant)
            || authority.recovery.owner_grant != authority.owner_grant
        {
            return Err(StoreRegistrationError::Invalid(
                "Owner recovery authority differs from the active root founder grant".into(),
            ));
        }
        let coven_protocol::store_commit::GrantStreamAnchor::OwnerRecovery { first_slot } =
            &protocol.descriptor.founder_recovery
        else {
            return Err(StoreRegistrationError::Invalid(
                "Store root has no founder recovery stream".into(),
            ));
        };
        let (recovery_slot, predecessor, sequence) = match &authority.recovery.position {
            OwnerRecoveryPosition::BeforeFirst { activation } => {
                let expected = coven_protocol::store_commit::OwnerRecoveryActivationId::derive(
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
                    loaded.value.next_slot.clone(),
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
        let device_id = coven_protocol::store_commit::StoreDeviceId::derive(&root, &origin);
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
            coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                root.store_root_hash,
                domain,
            )
        };
        let commit_context = context(ProtocolObjectDomain::StoreCommit);
        let staged = database
            .latest_local_store_device_registration()
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?
            .filter(|durable| durable.device_id == device_id);
        let readiness = if let Some(durable) = staged {
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
                    &context(
                        coven_protocol::objects::ProtocolObjectDomain::StoreDeviceRegistration,
                    ),
                    &durable.prepared,
                    &registration_semantic_prefix(&device_id.to_string()),
                    &durable.registration_bytes,
                )
                .await?;
            let initial_ack_exists = self
                .prepared_protocol_object_exists(
                    &context(coven_protocol::objects::ProtocolObjectDomain::StoreAck),
                    &durable.initial_ack.prepared,
                    &ack_slot_prefix(&device_id.to_string(), 1),
                    &durable.initial_ack.bytes,
                )
                .await?;
            let registration_object = durable.prepared.reference().clone();
            PreparedRecoveryReadiness {
                registration: RecoveryProtocolObject::from_remote_state(
                    coven_protocol::objects::ExactProtocolObject {
                        value: registration,
                        bytes: durable.registration_bytes,
                        object: registration_object,
                        prepared: durable.prepared,
                    },
                    registration_ref,
                    registration_exists,
                ),
                initial_ack: RecoveryProtocolObject::from_remote_state(
                    durable.initial_ack,
                    durable.initial_ack_ref,
                    initial_ack_exists,
                ),
            }
        } else {
            let head_context = context(coven_protocol::objects::ProtocolObjectDomain::StoreHead);
            let ack_context = context(coven_protocol::objects::ProtocolObjectDomain::StoreAck);
            let snapshot_context =
                context(coven_protocol::objects::ProtocolObjectDomain::StoreSnapshotMeta);
            let registration_context =
                context(coven_protocol::objects::ProtocolObjectDomain::StoreDeviceRegistration);
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
            let registration = self
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
                .exact()
                .value
                .device_signer(identity_signer)
                .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
            let bootstrap_cut = StoreHistoryCut(dependencies);
            let (device_state, _) = database
                .store_device_state_for_history_cut(&bootstrap_cut)
                .await
                .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
            let initial_ack = self
                .prepare_or_load_initial_recovery_ack(
                    &registration.exact().value,
                    registration.reference(),
                    first_ack,
                    bootstrap_cut,
                    device_state,
                    &authority.published_at,
                    &device_signer,
                )
                .await?;
            PreparedRecoveryReadiness {
                registration,
                initial_ack,
            }
        };
        let registration_ref = readiness.registration.reference().clone();
        let initial_ack_ref = readiness.initial_ack.reference().clone();
        let dependencies = readiness.initial_ack.exact().value.store_cut.0.clone();
        let device_signer = readiness
            .registration
            .exact()
            .value
            .device_signer(identity_signer)
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let bootstrap_cut = readiness.initial_ack.exact().value.store_cut.clone();
        let coven_protocol::membership::MembershipStatus::Resolved(resolved) = membership.status()
        else {
            return Err(StoreRegistrationError::Invalid(
                "Owner recovery requires resolved membership".into(),
            ));
        };
        let membership_state = coven_protocol::circle_control::StoreMembershipStateRef::from_parts(
            membership.head_refs().to_vec(),
            membership.resolution_refs().to_vec(),
            vec![authority.recovery.clone()],
            resolved.state_hash,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let recovery_readiness = DeviceRecoveryReadiness {
            registration: registration_ref.clone(),
            initial_ack: initial_ack_ref.clone(),
            bootstrap_cut: bootstrap_cut.clone(),
        };
        let node = self
            .prepare_or_load_owner_recovery_node(
                recovery_slot,
                &owner_pubkey,
                &authority.owner_grant,
                sequence,
                recovery_id,
                &membership_state,
                &predecessor,
                &recovery_readiness,
                identity_signer,
            )
            .await?;
        let node_ref = node.reference().clone();
        let registration_activation = StoreDeviceRegistrationActivation::Recovery {
            recovery_id,
            node: node_ref.clone(),
        };
        let registration = readiness.registration.exact().value.clone();
        let already_activated = database
            .stage_owner_recovery_registration(
                readiness.registration.exact().clone(),
                initial_ack_ref.clone(),
                readiness.initial_ack.exact().clone(),
                registration_activation.clone(),
            )
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
        if already_activated {
            return Ok(registration_ref);
        }
        if let Some(prepared) = readiness.registration.prepared_for_creation() {
            storage
                .create_protocol_object(prepared)
                .await
                .map_err(StoreObjectError::from)?;
        }
        if let Some(prepared) = readiness.initial_ack.prepared_for_creation() {
            storage
                .create_protocol_object(prepared)
                .await
                .map_err(StoreObjectError::from)?;
        }
        if let Some(prepared) = node.prepared_for_creation() {
            storage
                .create_protocol_object(prepared)
                .await
                .map_err(StoreObjectError::from)?;
        }
        let PreparedRecoveryReadiness {
            registration: prepared_registration,
            initial_ack: prepared_initial_ack,
        } = readiness;
        database
            .mark_local_store_device_registration_created(
                prepared_registration.into_exact(),
                initial_ack_ref,
                prepared_initial_ack.into_exact(),
            )
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;

        let stream_id = coven_protocol::store_commit::StreamActivation::device_authorized_stream_id(
            root.store_root_hash,
            &registration_ref,
            coven_protocol::store_commit::StreamAnchorDomain::StoreAnnouncements,
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
        let commit = StoreBatchCommit::signed_operations(
            root.store_root_hash,
            coven_protocol::write::WriteId::from_generated(format!(
                "owner-recovery-{recovery_hash}"
            )),
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
            coven_protocol::store_commit::StoreCommitOperationsInput {
                device_registrations: vec![activation_ref],
                ..coven_protocol::store_commit::StoreCommitOperationsInput::empty()
            },
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
        let verified_commit = coven_protocol::store_commit::VerifiedStoreBatchCommit::parse(
            &commit.to_bytes(),
            root.store_root_hash,
            &commit_ref,
            &registration,
        )
        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let state_after = predecessor_state
            .activate_registration(
                registration_ref.clone(),
                Some(coven_protocol::store_commit::OwnerRecoveryCursor {
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
                crate::sync::store::commit_verification::merge_history::MergeHistorySuccessorEvidence {
                    registrations: vec![
                        coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                            registration_ref.clone(),
                            registration.clone(),
                        )
                        .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?,
                    ],
                    acknowledgement: None,
                    membership_proof: None,
                },
            )
            .await
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        let head_context = context(coven_protocol::objects::ProtocolObjectDomain::StoreHead);
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
        let registration =
            coven_protocol::store_commit::ReferencedStoreDeviceRegistration::verified(
                registration_ref.clone(),
                registration,
            )
            .and_then(|registration| {
                coven_protocol::store_commit::ActivatedStoreDeviceRegistration::verified(
                    registration,
                    registration_activation,
                )
            })
            .map_err(|error| StoreRegistrationError::Invalid(error.to_string()))?;
        database
            .complete_owner_recovery(
                verified_commit,
                head,
                head_prepared.reference().clone(),
                prepared_history.summary,
                registration,
            )
            .await
            .map_err(|error| StoreRegistrationError::Database(error.to_string()))?;
        Ok(registration_ref)
    }
}
