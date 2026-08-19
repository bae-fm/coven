use super::*;
use crate::sync::store::protocol_root::*;
use crate::sync::store::registration_object::prepare_registration_object;
use coven_protocol::membership::{AuthorHead, MembershipHeadRef};
use coven_protocol::objects::{ProtocolObjectDomain, StoreObjectError};
use coven_protocol::store_commit::{
    ack_slot_prefix, membership_head_slot_prefix, owner_recovery_semantic_prefix, CommitFrontier,
    DeviceStreamAnchor, GrantStreamAnchor, ObjectHash, ResolvedStoreDeviceState, StoreAck,
    StoreAckExclusionState, StoreAckRef, StoreCreationId, StoreDeviceRegistrationOrigin,
    StoreDeviceRegistrationRef, StoreDeviceStateRef, StoreHistoryCut, StoreProtocolRoot,
    SuccessorLink,
};
use coven_protocol::store_creation::*;
use std::sync::Arc;

use super::authorization::history::AuthorizedStoreHistory;
use super::authorization::InitializedStore;
use super::{HistoryConstructionAuthority, StoreKeyrings};
use crate::sync::store::commit_verification::commit::StoreCommitVerifier;
use crate::sync::store::commit_verification::merge_history::MergeHistoryVerifier;
use coven_protocol::store_commit::StoreRootRef;

pub(crate) struct FounderStoreCreation<'operation> {
    database: StoreDatabase,
    storage: Arc<dyn CloudSyncObjectStorage>,
    store_dir: &'operation StoreDir,
    blob_cache: crate::sync::store::blob::StoreBlobCache,
    founder_timestamp: &'operation str,
    identity: &'operation UserKeypair,
    _permit: coven_database::store::StoreCreationPermit,
}

struct StagedFounderStoreCreation<'operation> {
    creation: FounderStoreCreation<'operation>,
    graph: Box<coven_database::DurableFounderGraph>,
    rollback_allowed: bool,
}

#[derive(Debug, thiserror::Error)]
#[error("delete founder object {}: {source}", object.slot().logical_key())]
pub struct FounderObjectDeleteError {
    object: coven_protocol::objects::ExactObjectRef,
    #[source]
    source: coven_protocol::objects::StorageError,
}

#[derive(Debug, thiserror::Error)]
pub enum FounderRollbackError {
    #[error("founder object rollback failed: {0:?}")]
    ObjectDeletes(Vec<FounderObjectDeleteError>),
    #[error("reset founder publication database state: {0}")]
    Database(#[from] coven_database::DbError),
}

#[derive(Debug, thiserror::Error)]
#[error("{operation}; Store founder rollback failed: {rollback}")]
pub struct FounderPublicationRollback {
    #[source]
    operation: StoreProtocolRootError,
    rollback: FounderRollbackError,
}

fn creation_authority(attempt: &StoreCreationAttempt) -> &StoreCreationAuthority {
    match attempt {
        StoreCreationAttempt::Initialized(authority) => authority,
        StoreCreationAttempt::RootReserved(reservation) => &reservation.authority,
        StoreCreationAttempt::FounderRegistrationReserved(reservation) => {
            &reservation.root.authority
        }
        StoreCreationAttempt::MembershipReserved(reservation) => {
            &reservation.founder.root.authority
        }
        StoreCreationAttempt::DescriptorReserved(reservation) => {
            &reservation.membership.founder.root.authority
        }
        StoreCreationAttempt::FounderStoreCommitsReserved(reservation) => {
            &reservation.descriptor.membership.founder.root.authority
        }
        StoreCreationAttempt::FounderAcknowledgementsReserved(reservation) => {
            &reservation
                .store_commits
                .descriptor
                .membership
                .founder
                .root
                .authority
        }
        StoreCreationAttempt::FounderSnapshotsReserved(reservation) => {
            &reservation
                .acknowledgements
                .store_commits
                .descriptor
                .membership
                .founder
                .root
                .authority
        }
        StoreCreationAttempt::FounderNextAckReserved(reservation) => {
            &reservation
                .snapshots
                .acknowledgements
                .store_commits
                .descriptor
                .membership
                .founder
                .root
                .authority
        }
        StoreCreationAttempt::FounderGraphReserved(reservation) => {
            &reservation.descriptor.membership.founder.root.authority
        }
    }
}

impl<'operation> FounderStoreCreation<'operation> {
    pub(crate) async fn begin(
        database: StoreDatabase,
        storage: Arc<dyn CloudSyncObjectStorage>,
        store_dir: &'operation StoreDir,
        blob_cache: crate::sync::store::blob::StoreBlobCache,
        founder_timestamp: &'operation str,
        identity: &'operation UserKeypair,
    ) -> Self {
        let permit = database.store_creation_permit().await;
        Self {
            database,
            storage,
            store_dir,
            blob_cache,
            founder_timestamp,
            identity,
            _permit: permit,
        }
    }

    async fn durable_descriptor_reservation(
        &self,
    ) -> Result<DescriptorReservation, StoreProtocolRootError> {
        let db = &self.database;
        let storage = self.storage.as_ref();
        let founder_timestamp = self.founder_timestamp;
        let signer = self.identity;
        let binding = coven_storage::CloudSyncObjectStorage::provider_binding(storage)
            .await
            .map_err(StoreProtocolRootError::Provider)?;
        let probes =
            StoreCreationProbeIds::new(coven_protocol::provider::ProviderProbeId::from_bytes(
                coven_keys::encryption::generate_random_key(),
            ));
        let access =
            coven_protocol::provider::ProviderAccessLocator::for_current_administrator(&binding)
                .map_err(StoreProtocolRootError::Provider)?;
        let initialized = StoreCreationAttempt::Initialized(StoreCreationAuthority {
            creation_id: StoreCreationId::from_random_bytes(
                coven_keys::encryption::generate_random_key(),
            ),
            founder_grant: coven_protocol::membership::MembershipGrantId(ObjectHash::from_digest(
                coven_keys::encryption::generate_random_key(),
            )),
            provider_admin_grant: coven_protocol::provider::ProviderAdminGrantId::from_random_bytes(
                coven_keys::encryption::generate_random_key(),
            ),
            probes,
            binding: binding.clone(),
            access,
            founder_pubkey: coven_keys::keys::public_key_hex(signer),
            founder_timestamp: founder_timestamp.to_string(),
            schema_version: db.schema_version(),
            sync_routing_hash: db.sync_routing_hash(),
        });
        let mut attempt = db
            .begin_store_creation_attempt(initialized)
            .await
            .map_err(StoreProtocolRootError::Database)?;
        let authority = creation_authority(&attempt);
        if authority.binding != binding
            || authority.founder_pubkey != coven_keys::keys::public_key_hex(signer)
            || authority.schema_version != db.schema_version()
            || authority.sync_routing_hash != db.sync_routing_hash()
        {
            return Err(StoreProtocolRootError::Invariant(
                "durable Store creation authority differs from this creation request".to_string(),
            ));
        }
        let allocation_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            ObjectHash::digest(authority.creation_id.to_string().as_bytes()),
            ProtocolObjectDomain::StoreProtocolRoot,
        );
        if let StoreCreationAttempt::Initialized(authority) = &attempt {
            let root_slot = storage
                .allocate_protocol_slot(
                    &allocation_context,
                    coven_protocol::store_commit::store_protocol_root_logical_key(),
                    ".json",
                )
                .await
                .map_err(StoreObjectError::from)?;
            let next = StoreCreationAttempt::RootReserved(StoreRootReservation {
                authority: authority.clone(),
                root_slot,
            });
            db.advance_store_creation_attempt(attempt.clone(), next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            attempt = next;
        }
        if let StoreCreationAttempt::RootReserved(root) = &attempt {
            let prefix = coven_protocol::store_commit::founder_registration_semantic_prefix(
                root.authority.creation_id,
            );
            let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                allocation_context.store_root_hash(),
                ProtocolObjectDomain::StoreDeviceRegistration,
            );
            let registration_slot = storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let next =
                StoreCreationAttempt::FounderRegistrationReserved(FounderRegistrationReservation {
                    root: root.clone(),
                    registration_slot,
                });
            db.advance_store_creation_attempt(attempt.clone(), next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            attempt = next;
        }
        if let StoreCreationAttempt::FounderRegistrationReserved(founder) = &attempt {
            let prefix = coven_protocol::store_commit::founder_membership_head_semantic_prefix(
                founder.root.authority.creation_id,
            );
            let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                allocation_context.store_root_hash(),
                coven_protocol::objects::ProtocolObjectDomain::StoreMembershipHead,
            );
            let first_slot = storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let membership = MembershipReservation {
                founder: founder.clone(),
                first_slot,
            };
            let next = StoreCreationAttempt::MembershipReserved(membership);
            db.advance_store_creation_attempt(attempt.clone(), next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            attempt = next;
        }
        if let StoreCreationAttempt::MembershipReserved(membership) = &attempt {
            let authority = &membership.founder.root.authority;
            let prefix = owner_recovery_semantic_prefix(
                &authority.founder_pubkey,
                authority.founder_grant.clone(),
                1,
            );
            let context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                allocation_context.store_root_hash(),
                ProtocolObjectDomain::OwnerRecoveryNode,
            );
            let recovery_slot = storage
                .allocate_protocol_slot(&context, &prefix, ".json")
                .await
                .map_err(StoreObjectError::from)?;
            let next = StoreCreationAttempt::DescriptorReserved(DescriptorReservation {
                membership: membership.clone(),
                recovery_slot,
            });
            db.advance_store_creation_attempt(attempt, next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            return Ok(match next {
                StoreCreationAttempt::DescriptorReserved(reservation) => reservation,
                _ => unreachable!("constructed descriptor reservation variant"),
            });
        }
        match attempt {
            StoreCreationAttempt::DescriptorReserved(reservation) => Ok(reservation),
            StoreCreationAttempt::FounderStoreCommitsReserved(reservation) => {
                Ok(reservation.descriptor)
            }
            StoreCreationAttempt::FounderAcknowledgementsReserved(reservation) => {
                Ok(reservation.store_commits.descriptor)
            }
            StoreCreationAttempt::FounderSnapshotsReserved(reservation) => {
                Ok(reservation.acknowledgements.store_commits.descriptor)
            }
            StoreCreationAttempt::FounderNextAckReserved(reservation) => Ok(reservation
                .snapshots
                .acknowledgements
                .store_commits
                .descriptor),
            StoreCreationAttempt::FounderGraphReserved(reservation) => Ok(reservation.descriptor),
            _ => Err(StoreProtocolRootError::Invariant(
                "Store creation attempt did not reach descriptor reservation".to_string(),
            )),
        }
    }

    async fn durable_founder_graph_reservation(
        &self,
        descriptor: &DescriptorReservation,
        root: &StoreRootRef,
    ) -> Result<FounderGraphReservation, StoreProtocolRootError> {
        let db = &self.database;
        let storage = self.storage.as_ref();
        let mut attempt = db
            .load_store_creation_attempt()
            .await
            .map_err(StoreProtocolRootError::Database)?
            .ok_or_else(|| {
                StoreProtocolRootError::Invariant("Store creation attempt is absent".to_string())
            })?;
        if creation_authority(&attempt) != &descriptor.membership.founder.root.authority {
            return Err(StoreProtocolRootError::Invariant(
                "founder graph reservation belongs to another descriptor".to_string(),
            ));
        }
        let authority = &descriptor.membership.founder.root.authority;
        let origin = StoreDeviceRegistrationOrigin::Founder {
            creation_id: authority.creation_id,
        };
        let device = coven_protocol::store_commit::StoreDeviceId::derive(root, &origin).to_string();
        let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );

        if let StoreCreationAttempt::DescriptorReserved(current) = &attempt {
            if current != descriptor {
                return Err(StoreProtocolRootError::Invariant(
                    "founder graph reservation belongs to another descriptor".to_string(),
                ));
            }
            let store_commits = DeviceStreamAnchor::StoreAnnouncements {
                first_slot: storage
                    .allocate_protocol_slot(
                        &coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                            root.store_root_hash,
                            ProtocolObjectDomain::StoreHead,
                        ),
                        &coven_protocol::store_commit::head_slot_prefix(&device, 1),
                        ".json",
                    )
                    .await
                    .map_err(StoreObjectError::from)?,
            };
            let next =
                StoreCreationAttempt::FounderStoreCommitsReserved(FounderStoreCommitsReservation {
                    descriptor: descriptor.clone(),
                    store_commits,
                });
            db.advance_store_creation_attempt(attempt.clone(), next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            attempt = next;
        }
        if let StoreCreationAttempt::FounderStoreCommitsReserved(current) = &attempt {
            let next = StoreCreationAttempt::FounderAcknowledgementsReserved(
                FounderAcknowledgementsReservation {
                    store_commits: current.clone(),
                    acknowledgements: DeviceStreamAnchor::StoreAcknowledgements {
                        first_slot: storage
                            .allocate_protocol_slot(
                                &ack_context,
                                &ack_slot_prefix(&device, 1),
                                ".json",
                            )
                            .await
                            .map_err(StoreObjectError::from)?,
                    },
                },
            );
            db.advance_store_creation_attempt(attempt.clone(), next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            attempt = next;
        }
        if let StoreCreationAttempt::FounderAcknowledgementsReserved(current) = &attempt {
            let next =
                StoreCreationAttempt::FounderSnapshotsReserved(FounderSnapshotsReservation {
                    acknowledgements: current.clone(),
                    snapshots: DeviceStreamAnchor::StoreSnapshots {
                        first_slot: storage
                            .allocate_protocol_slot(
                                &coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                                    root.store_root_hash,
                                    ProtocolObjectDomain::StoreSnapshotMeta,
                                ),
                                &coven_protocol::store_commit::snapshot_slot_prefix(&device, 0),
                                ".json",
                            )
                            .await
                            .map_err(StoreObjectError::from)?,
                    },
                });
            db.advance_store_creation_attempt(attempt.clone(), next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            attempt = next;
        }
        if let StoreCreationAttempt::FounderSnapshotsReserved(current) = &attempt {
            let next = StoreCreationAttempt::FounderNextAckReserved(FounderNextAckReservation {
                snapshots: current.clone(),
                next_ack_slot: storage
                    .allocate_protocol_slot(&ack_context, &ack_slot_prefix(&device, 2), ".json")
                    .await
                    .map_err(StoreObjectError::from)?,
            });
            db.advance_store_creation_attempt(attempt.clone(), next.clone())
                .await
                .map_err(StoreProtocolRootError::Database)?;
            attempt = next;
        }
        if let StoreCreationAttempt::FounderNextAckReserved(current) = &attempt {
            let founder_stream = coven_protocol::membership::derive_founder_stream_id(
                &root.store_root_id.to_string(),
                &authority.founder_pubkey,
            );
            let prefix = membership_head_slot_prefix(
                &authority.founder_pubkey,
                &authority.founder_grant,
                founder_stream,
                2,
            );
            let membership = FounderMembershipPublicationReservation {
                next_head_slot: storage
                    .allocate_protocol_slot(
                        &coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                            root.store_root_hash,
                            coven_protocol::objects::ProtocolObjectDomain::StoreMembershipHead,
                        ),
                        &prefix,
                        ".json",
                    )
                    .await
                    .map_err(StoreObjectError::from)?,
            };
            let reservation = FounderGraphReservation {
                descriptor: current
                    .snapshots
                    .acknowledgements
                    .store_commits
                    .descriptor
                    .clone(),
                store_commits: current
                    .snapshots
                    .acknowledgements
                    .store_commits
                    .store_commits
                    .clone(),
                acknowledgements: current.snapshots.acknowledgements.acknowledgements.clone(),
                snapshots: current.snapshots.snapshots.clone(),
                next_ack_slot: current.next_ack_slot.clone(),
                membership,
            };
            let next = StoreCreationAttempt::FounderGraphReserved(reservation.clone());
            db.advance_store_creation_attempt(attempt, next)
                .await
                .map_err(StoreProtocolRootError::Database)?;
            return Ok(reservation);
        }
        match attempt {
            StoreCreationAttempt::FounderGraphReserved(reservation) => Ok(reservation),
            _ => Err(StoreProtocolRootError::Invariant(
                "Store creation attempt regressed before founder graph reservation".to_string(),
            )),
        }
    }

    async fn prepare_founder_graph(
        &self,
    ) -> Result<Box<coven_database::DurableFounderGraph>, StoreProtocolRootError> {
        let db = &self.database;
        let storage = self.storage.as_ref();
        let signer = self.identity;
        let reservation = self.durable_descriptor_reservation().await?;
        let authority = &reservation.membership.founder.root.authority;
        let founder_timestamp = authority.founder_timestamp.as_str();
        let exact_slots = storage
            .probe_exact_slots(db, authority.probes.exact_slots(), &authority.binding)
            .await
            .map_err(StoreProtocolRootError::ProviderProbe)?;
        let provider_admin = coven_protocol::provider::FounderProviderAdminGrant {
            grant_id: authority.provider_admin_grant.clone(),
            provider: authority.binding.device.clone(),
            access: authority.access.clone(),
            capability: coven_protocol::provider::ProviderCapabilityProof { exact_slots },
        };
        let founder_anchor = GrantStreamAnchor::StoreMembership {
            first_slot: reservation.membership.first_slot.clone(),
        };
        let founder_recovery = GrantStreamAnchor::OwnerRecovery {
            first_slot: reservation.recovery_slot.clone(),
        };
        let key_confirmation = storage
            .create_store_key_confirmation(authority.creation_id)
            .map_err(StoreProtocolRootError::Provider)?;
        let descriptor = coven_protocol::store_commit::StoreCreationDescriptor {
            creation_id: authority.creation_id,
            key_confirmation,
            provider: authority.binding.store.clone(),
            schema_version: authority.schema_version,
            sync_routing_hash: authority.sync_routing_hash,
            founder_pubkey: authority.founder_pubkey.clone(),
            founder_grant: authority.founder_grant.clone(),
            root_slot: reservation.membership.founder.root.root_slot.clone(),
            founder_registration: reservation.membership.founder.registration_slot.clone(),
            founder_provider_admin: provider_admin.clone(),
            founder_membership: founder_anchor.clone(),
            founder_recovery,
        };
        let root_value = StoreProtocolRoot::signed(descriptor, signer)
            .map_err(StoreProtocolRootError::Protocol)?;
        let root_id = root_value.descriptor.store_root_id();
        let root_hash = root_value.object_hash();
        let root_prefix = coven_protocol::store_commit::store_protocol_root_logical_key();
        let root_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root_hash,
            ProtocolObjectDomain::StoreProtocolRoot,
        );
        let root_bytes = root_value.to_bytes();
        let root_prepared = storage
            .prepare_protocol_object(
                &root_context,
                root_value.descriptor.root_slot.clone(),
                root_prefix,
                root_bytes.clone(),
            )
            .map_err(StoreObjectError::from)?;
        let root_ref = StoreRootRef {
            store_root_id: root_id,
            store_root_hash: root_hash,
            object: root_prepared.reference().clone(),
        };
        let graph_reservation = self
            .durable_founder_graph_reservation(&reservation, &root_ref)
            .await?;
        let origin = StoreDeviceRegistrationOrigin::Founder {
            creation_id: authority.creation_id,
        };
        let registration_value = coven_protocol::store_commit::StoreDeviceRegistration::signed(
            root_ref.clone(),
            origin,
            authority.binding.device.clone(),
            graph_reservation.store_commits.clone(),
            graph_reservation.acknowledgements.clone(),
            graph_reservation.snapshots.clone(),
            signer,
        )
        .map_err(StoreProtocolRootError::Protocol)?;
        let registration_prepared = prepare_registration_object(
            storage,
            &registration_value,
            root_value.descriptor.founder_registration.clone(),
        )
        .map_err(|error| StoreProtocolRootError::Registration(Box::new(error)))?;
        let registration_bytes = registration_value.to_bytes();
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &registration_value,
            registration_prepared.reference().clone(),
        );
        let device_signer = registration_value
            .device_signer(signer)
            .map_err(StoreProtocolRootError::Protocol)?;
        let DeviceStreamAnchor::StoreAcknowledgements { first_slot } =
            &registration_value.acknowledgements
        else {
            return Err(StoreProtocolRootError::Invariant(
                "founder registration has no acknowledgement anchor".to_string(),
            ));
        };
        let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        let next_ack_slot = graph_reservation.next_ack_slot.clone();
        let frontier = StoreHistoryCut::from_commits(Default::default());
        let resolved_devices = ResolvedStoreDeviceState::founder(
            &root_ref,
            registration_ref.clone(),
            &root_value.descriptor.founder_pubkey,
            root_value.descriptor.founder_grant.clone(),
            &root_value.descriptor.founder_recovery,
        )
        .map_err(StoreProtocolRootError::Protocol)?;
        let device_state = StoreDeviceStateRef::from_resolved(
            CommitFrontier(frontier.0.clone()),
            &resolved_devices,
        )
        .map_err(StoreProtocolRootError::Protocol)?;
        let exclusions = StoreAckExclusionState {
            proposal_freezes: Vec::new(),
        };
        let acknowledgement_activation = registration_value
            .store_acknowledgement_activation(&registration_ref)
            .map_err(StoreProtocolRootError::Protocol)?
            .activation_id();
        let initial_ack_value = StoreAck::signed(
            root_hash,
            1,
            coven_protocol::store_commit::StoreAckAssertion {
                registration: registration_ref.clone(),
                store_cut: frontier,
                device_state,
                snapshot: None,
                exclusions,
            },
            founder_timestamp.to_string(),
            SuccessorLink {
                activation: acknowledgement_activation,
                predecessor: None,
                next_slot: next_ack_slot,
            },
            &device_signer,
        )
        .map_err(StoreProtocolRootError::Protocol)?;
        let initial_ack_bytes = initial_ack_value.to_bytes();
        let initial_ack_prepared = storage
            .prepare_protocol_object(
                &ack_context,
                first_slot.clone(),
                &ack_slot_prefix(&registration_value.device_id.to_string(), 1),
                initial_ack_bytes.clone(),
            )
            .map_err(StoreObjectError::from)?;
        let initial_ack_ref = StoreAckRef {
            registration: registration_ref.clone(),
            sequence: 1,
            ack_hash: initial_ack_value.ack_hash(),
            object: initial_ack_prepared.reference().clone(),
        };
        let membership = {
            let founder_head_slot = reservation.membership.first_slot.clone();
            let founder_grant = authority.founder_grant.clone();
            let founder = coven_protocol::membership::founder_entry_for_creation(
                &root_id.to_string(),
                root_value.descriptor.creation_id,
                signer,
                founder_grant.clone(),
                founder_timestamp,
                founder_anchor.clone(),
                provider_admin,
            );
            let (entry_prepared, entry_ref) =
                coven_storage::prepare_membership_entry(storage, root_hash, &founder).await?;
            let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
                root_hash,
                coven_protocol::objects::ProtocolObjectDomain::StoreMembershipHead,
            );
            let next_head_slot = &graph_reservation.membership.next_head_slot;
            let head_value = AuthorHead::signed(
                root_id.to_string(),
                coven_protocol::membership::MembershipHeadBody {
                    author_registration: registration_ref.clone(),
                    entry: entry_ref.clone(),
                    predecessor: None,
                    resolutions: Vec::new(),
                    successor: SuccessorLink {
                        activation:
                            coven_protocol::store_commit::StreamActivation::grant_authorized(
                                root_ref.store_root_hash,
                                registration_ref.clone(),
                                founder_grant.clone(),
                                founder_anchor.clone(),
                            )
                            .activation_id(),
                        predecessor: None,
                        next_slot: next_head_slot.clone(),
                    },
                },
                coven_protocol::membership::MembershipHeadActivation::Direct,
                &device_signer,
            );
            let head_bytes = serde_json::to_vec(&head_value)
                .expect("founder membership head serialization cannot fail");
            let founder_head_prefix =
                coven_protocol::store_commit::founder_membership_head_semantic_prefix(
                    authority.creation_id,
                );
            let head_prepared = storage
                .prepare_protocol_object(
                    &head_context,
                    founder_head_slot,
                    &founder_head_prefix,
                    head_bytes.clone(),
                )
                .map_err(StoreObjectError::from)?;
            let head_ref = MembershipHeadRef {
                coord: founder.coord(),
                head_hash: head_value.head_hash(),
                object: head_prepared.reference().clone(),
            };
            let founder_bytes = serde_json::to_vec(&founder)
                .expect("founder membership entry serialization cannot fail");
            coven_database::DurableFounderMembership {
                entry: coven_protocol::objects::ExactProtocolObject {
                    value: founder,
                    bytes: founder_bytes,
                    prepared: entry_prepared,
                },
                entry_ref,
                head: coven_protocol::objects::ExactProtocolObject {
                    value: head_value,
                    bytes: head_bytes,
                    prepared: head_prepared,
                },
                head_ref,
            }
        };
        Ok(Box::new(coven_database::DurableFounderGraph {
            root: coven_protocol::objects::ExactProtocolObject {
                value: root_value,
                bytes: root_bytes,
                prepared: root_prepared,
            },
            registration: coven_protocol::objects::ExactProtocolObject {
                value: registration_value,
                bytes: registration_bytes,
                prepared: registration_prepared,
            },
            initial_ack: coven_protocol::objects::ExactProtocolObject {
                value: initial_ack_value,
                bytes: initial_ack_bytes,
                prepared: initial_ack_prepared,
            },
            initial_ack_ref,
            membership,
            registration_state: coven_database::LocalDeviceRegistrationState::Prepared,
        }))
    }

    async fn rollback_founder_publication(
        &self,
        graph: &coven_database::DurableFounderGraph,
    ) -> Result<(), FounderRollbackError> {
        let mut objects = vec![
            graph.membership.head.prepared.reference().clone(),
            graph.membership.entry.prepared.reference().clone(),
        ];
        objects.extend([
            graph.initial_ack.prepared.reference().clone(),
            graph.registration.prepared.reference().clone(),
            graph.root.prepared.reference().clone(),
        ]);
        let mut failures = Vec::new();
        for object in objects {
            match self.storage.delete_protocol_object(&object).await {
                Ok(()) | Err(coven_protocol::objects::StorageError::SlotCollision(_)) => {}
                Err(source) => failures.push(FounderObjectDeleteError { object, source }),
            }
        }
        if !failures.is_empty() {
            return Err(FounderRollbackError::ObjectDeletes(failures));
        }
        self.database
            .reset_store_founder_graph_publication(graph)
            .await
            .map_err(FounderRollbackError::Database)
    }

    async fn stage(
        self,
    ) -> Result<StagedFounderStoreCreation<'operation>, StoreInitializationError> {
        let existing = self.database.local_store_founder_graph().await?;
        let resumed = existing.is_some();
        let graph = match existing {
            Some(graph) => graph,
            None => {
                let graph = Box::pin(self.prepare_founder_graph()).await?;
                self.database.stage_store_founder_graph(graph).await?;
                self.database
                    .local_store_founder_graph()
                    .await?
                    .ok_or_else(|| {
                        StoreInitializationError::FounderState(
                            "staged Store founder graph is absent".to_string(),
                        )
                    })?
            }
        };
        let rollback_allowed = match &graph.registration_state {
            coven_database::LocalDeviceRegistrationState::Prepared
            | coven_database::LocalDeviceRegistrationState::RegistrationPublished
            | coven_database::LocalDeviceRegistrationState::Created => true,
            coven_database::LocalDeviceRegistrationState::RegistrationActivated { .. }
            | coven_database::LocalDeviceRegistrationState::Activated { .. } => false,
        };
        let mut staged = StagedFounderStoreCreation {
            creation: self,
            graph,
            rollback_allowed,
        };
        if resumed && staged.rollback_allowed {
            staged.reset_partial_publication().await?;
        }
        Ok(staged)
    }

    pub(crate) async fn execute(self) -> Result<InitializedStore, StoreInitializationError> {
        self.stage().await?.publish().await
    }

    async fn reload_founder_graph(
        &self,
    ) -> Result<Box<coven_database::DurableFounderGraph>, StoreInitializationError> {
        self.database
            .local_store_founder_graph()
            .await?
            .ok_or_else(|| {
                StoreInitializationError::FounderState(
                    "rolled-back Store founder graph is absent".to_string(),
                )
            })
    }

    async fn publish_history<'creation>(
        &'creation self,
        graph: &coven_database::DurableFounderGraph,
    ) -> Result<AuthorizedStoreHistory<'creation>, StoreProtocolRootError> {
        let database = &self.database;
        let storage = &self.storage;
        let storage_access = storage.as_ref();
        let identity = self.identity;
        let root = StoreRootRef {
            store_root_id: graph.root.value.descriptor.store_root_id(),
            store_root_hash: graph.root.value.object_hash(),
            object: graph.root.prepared.reference().clone(),
        };
        let protocol_root = StoreProtocolRoot::parse_expected(
            &graph.root.bytes,
            &root,
            database.sync_routing_hash(),
        )
        .map_err(StoreProtocolRootError::Protocol)?;
        if protocol_root.descriptor.founder_pubkey != coven_keys::keys::public_key_hex(identity) {
            return Err(StoreProtocolRootError::Invariant(
                "durable Store founder differs from the creation signer".to_string(),
            ));
        }
        if protocol_root.descriptor.schema_version > database.schema_version() {
            return Err(StoreProtocolRootError::SchemaTooNew {
                root_schema: protocol_root.descriptor.schema_version,
                local: database.schema_version(),
            });
        }
        let registration_ref = StoreDeviceRegistrationRef::from_registration(
            &graph.registration.value,
            graph.registration.prepared.reference().clone(),
        );
        let root_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreProtocolRoot,
        );
        storage_access
            .create_verified_protocol_object(
                &root_context,
                &graph.root.prepared,
                coven_protocol::store_commit::store_protocol_root_logical_key(),
                &graph.root.bytes,
            )
            .await
            .map_err(StoreObjectError::from)?;
        let verified_root = VerifiedStoreRoot::from_verified_object(
            root.clone(),
            coven_protocol::objects::VerifiedObject {
                value: protocol_root.clone(),
                bytes: graph.root.bytes.clone(),
                semantic_hash: root.store_root_hash,
                object: graph.root.prepared.reference().clone(),
            },
        )
        .map_err(StoreProtocolRootError::Protocol)?;
        let authority = HistoryConstructionAuthority::founder();
        let commit_verifier = StoreCommitVerifier::from_verified_root(
            authority,
            storage_access,
            verified_root.clone(),
        );
        let registration_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreDeviceRegistration,
        );
        let registration_prefix = graph
            .registration
            .prepared
            .reference()
            .slot()
            .logical_key()
            .strip_suffix(".json")
            .ok_or_else(|| {
                StoreProtocolRootError::Invariant(
                    "founder registration slot has no .json suffix".to_string(),
                )
            })?;
        storage_access
            .create_verified_protocol_object(
                &registration_context,
                &graph.registration.prepared,
                registration_prefix,
                &graph.registration.bytes,
            )
            .await
            .map_err(StoreObjectError::from)?;
        let registration = graph.registration.value.clone();
        let ack_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreAck,
        );
        storage_access
            .create_verified_protocol_object(
                &ack_context,
                &graph.initial_ack.prepared,
                &ack_slot_prefix(&registration.device_id.to_string(), 1),
                &graph.initial_ack.bytes,
            )
            .await
            .map_err(StoreObjectError::from)?;
        if !matches!(
            &graph.registration_state,
            coven_database::LocalDeviceRegistrationState::Activated { .. }
        ) {
            database
                .mark_local_store_device_registration_published(
                    graph.registration.clone(),
                    graph.initial_ack_ref.clone(),
                    graph.initial_ack.clone(),
                )
                .await
                .map_err(StoreProtocolRootError::Database)?;
            database
                .mark_local_store_device_ack_published(
                    graph.registration.clone(),
                    graph.initial_ack_ref.clone(),
                    graph.initial_ack.clone(),
                )
                .await
                .map_err(StoreProtocolRootError::Database)?;
        }
        let membership = &graph.membership;
        let entry_coord = membership.entry.value.coord();
        let entry_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipEntry,
        );
        storage_access
            .create_verified_protocol_object(
                &entry_context,
                &membership.entry.prepared,
                &coven_protocol::store_commit::membership_entry_semantic_prefix(
                    &entry_coord.author_pubkey,
                    &entry_coord.author_owner_grant,
                    entry_coord.stream_id,
                    entry_coord.seq,
                    entry_coord.entry_hash,
                ),
                &membership.entry.bytes,
            )
            .await
            .map_err(StoreObjectError::from)?;
        let head_context = coven_protocol::objects::ProtocolObjectContext::signed_plaintext(
            root.store_root_hash,
            ProtocolObjectDomain::StoreMembershipHead,
        );
        storage_access
            .create_verified_protocol_object(
                &head_context,
                &membership.head.prepared,
                &coven_protocol::store_commit::founder_membership_head_semantic_prefix(
                    protocol_root.descriptor.creation_id,
                ),
                &membership.head.bytes,
            )
            .await
            .map_err(StoreObjectError::from)?;
        database
            .complete_store_founder_graph(
                root.clone(),
                registration_ref,
                graph.initial_ack_ref.clone(),
                coven_database::FounderMembershipRefs {
                    entry: membership.entry_ref.clone(),
                    head: membership.head_ref.clone(),
                },
            )
            .await
            .map_err(StoreProtocolRootError::Database)?;
        let history_verifier = MergeHistoryVerifier::from_commit_verifier(
            authority,
            verified_root.clone(),
            commit_verifier,
        )
        .await
        .map_err(|error| StoreProtocolRootError::History(Box::new(error)))?;
        let blob_source = crate::sync::store::blob::RemoteBlobSource::authorized(
            database.clone(),
            storage_access,
            root.clone(),
        );
        let keyrings = StoreKeyrings::new(storage_access, root);
        Ok(AuthorizedStoreHistory::new(
            database.clone(),
            storage,
            self.store_dir,
            self.blob_cache.clone(),
            history_verifier,
            blob_source,
            keyrings,
        ))
    }

    async fn finish_published(
        &self,
        history: AuthorizedStoreHistory<'_>,
    ) -> Result<InitializedStore, StoreInitializationError> {
        let durable_root = self.database.local_store_root_ref().await?.ok_or_else(|| {
            StoreInitializationError::FounderState(
                "published Store founder graph has no durable exact root".to_string(),
            )
        })?;
        if history.root() != &durable_root {
            return Err(StoreInitializationError::FounderState(
                "published Store founder history differs from its durable exact root".to_string(),
            ));
        }
        history.finish_initialization(self.identity).await
    }
}

impl StagedFounderStoreCreation<'_> {
    async fn reset_partial_publication(&mut self) -> Result<(), StoreInitializationError> {
        Box::pin(self.creation.rollback_founder_publication(&self.graph)).await?;
        self.graph = self.creation.reload_founder_graph().await?;
        Ok(())
    }

    async fn publish(self) -> Result<InitializedStore, StoreInitializationError> {
        let history = match self.creation.publish_history(&self.graph).await {
            Ok(history) => history,
            Err(operation) if self.rollback_allowed => {
                match Box::pin(self.creation.rollback_founder_publication(&self.graph)).await {
                    Ok(()) => {
                        return Err(StoreInitializationError::ProtocolRoot(operation));
                    }
                    Err(rollback) => {
                        return Err(FounderPublicationRollback {
                            operation,
                            rollback,
                        }
                        .into());
                    }
                }
            }
            Err(operation) => {
                return Err(StoreInitializationError::ProtocolRoot(operation));
            }
        };
        self.creation.finish_published(history).await
    }
}

#[cfg(test)]
mod tests;
