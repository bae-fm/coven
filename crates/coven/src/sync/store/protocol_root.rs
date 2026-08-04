//! Durable creation and exact opening of the Store protocol root.

use crate::protocol::objects::ProtocolObjectDomain;
use crate::protocol::objects::StoreObjectError;
use crate::protocol::store_commit::{
    DeviceStreamAnchor, ObjectHash, StoreCreationId, StoreProtocolError, StoreProtocolRoot,
    StoreRootRef,
};
use crate::storage::SyncStorage;

pub(crate) const STORE_CREATION_ATTEMPT_STATE_KEY: &str = "store_creation_attempt_v1";

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreCreationProbeIds {
    exact_slots: crate::protocol::provider::ProviderProbeId,
}

impl StoreCreationProbeIds {
    pub(crate) fn new(exact_slots: crate::protocol::provider::ProviderProbeId) -> Self {
        Self { exact_slots }
    }

    pub(crate) fn exact_slots(&self) -> crate::protocol::provider::ProviderProbeId {
        self.exact_slots
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreCreationAuthority {
    pub creation_id: StoreCreationId,
    pub founder_grant: crate::protocol::membership::MembershipGrantId,
    pub provider_admin_grant: crate::protocol::provider::ProviderAdminGrantId,
    pub probes: StoreCreationProbeIds,
    pub binding: crate::protocol::objects::ResolvedProviderBinding,
    pub access: crate::protocol::provider::ProviderAccessLocator,
    pub founder_pubkey: String,
    pub founder_timestamp: String,
    pub schema_version: u32,
    pub sync_routing_hash: ObjectHash,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreRootReservation {
    pub authority: StoreCreationAuthority,
    pub root_slot: crate::protocol::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderRegistrationReservation {
    pub root: StoreRootReservation,
    pub registration_slot: crate::protocol::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MembershipReservation {
    pub founder: FounderRegistrationReservation,
    pub first_slot: crate::protocol::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DescriptorReservation {
    pub membership: MembershipReservation,
    pub recovery_slot: crate::protocol::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderMembershipPublicationReservation {
    pub next_head_slot: crate::protocol::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderGraphReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: DeviceStreamAnchor,
    pub acknowledgements: DeviceStreamAnchor,
    pub snapshots: DeviceStreamAnchor,
    pub next_ack_slot: crate::protocol::objects::ObjectSlot,
    pub membership: FounderMembershipPublicationReservation,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderStoreCommitsReservation {
    pub descriptor: DescriptorReservation,
    pub store_commits: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderAcknowledgementsReservation {
    pub store_commits: FounderStoreCommitsReservation,
    pub acknowledgements: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderSnapshotsReservation {
    pub acknowledgements: FounderAcknowledgementsReservation,
    pub snapshots: DeviceStreamAnchor,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FounderNextAckReservation {
    pub snapshots: FounderSnapshotsReservation,
    pub next_ack_slot: crate::protocol::objects::ObjectSlot,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum StoreCreationAttempt {
    Initialized(StoreCreationAuthority),
    RootReserved(StoreRootReservation),
    FounderRegistrationReserved(FounderRegistrationReservation),
    MembershipReserved(MembershipReservation),
    DescriptorReserved(DescriptorReservation),
    FounderStoreCommitsReserved(FounderStoreCommitsReservation),
    FounderAcknowledgementsReserved(FounderAcknowledgementsReservation),
    FounderSnapshotsReserved(FounderSnapshotsReservation),
    FounderNextAckReserved(FounderNextAckReservation),
    FounderGraphReserved(FounderGraphReservation),
}

#[derive(Debug, thiserror::Error)]
pub(super) enum StoreProtocolRootError {
    #[error(transparent)]
    Object(#[from] StoreObjectError),
    #[error("Store protocol root database state: {0}")]
    Database(String),
    #[error("Store protocol root schema version {root_schema} is newer than local schema {local}")]
    SchemaTooNew { root_schema: u32, local: u32 },
    #[error("Store protocol root is missing at {0}")]
    Missing(ObjectHash),
    #[error("Store provider check failed: {0}")]
    Provider(String),
}

#[derive(Clone)]
pub(crate) struct VerifiedStoreRoot {
    reference: StoreRootRef,
    object: crate::protocol::objects::VerifiedObject<StoreProtocolRoot>,
}

impl VerifiedStoreRoot {
    pub(super) async fn open(
        database: &crate::database::StoreDatabase,
        storage: &dyn SyncStorage,
        expected: &StoreRootRef,
    ) -> Result<Self, StoreProtocolRootError> {
        let object =
            load_exact_store_protocol_root(storage, expected, database.sync_routing_hash()).await?;
        let live_binding = storage
            .provider_binding()
            .await
            .map_err(|error| StoreProtocolRootError::Provider(error.to_string()))?;
        if live_binding.store != object.value.descriptor.provider {
            return Err(StoreProtocolRootError::Database(
                "live provider namespace differs from the signed Store root".to_string(),
            ));
        }
        if let Some(local) = database
            .latest_local_store_device_registration()
            .await
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?
            .filter(|registration| registration.is_activated())
        {
            let registration = crate::protocol::store_commit::StoreDeviceRegistration::parse_at(
                &local.registration_bytes,
                expected,
                local.device_id,
            )
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))?;
            if registration.provider != live_binding.device {
                return Err(StoreProtocolRootError::Database(
                    "live provider principal differs from the active Store registration"
                        .to_string(),
                ));
            }
        }
        if object.value.descriptor.schema_version > database.schema_version() {
            return Err(StoreProtocolRootError::SchemaTooNew {
                root_schema: object.value.descriptor.schema_version,
                local: database.schema_version(),
            });
        }
        Self::from_verified_object(expected.clone(), object)
            .map_err(|error| StoreProtocolRootError::Database(error.to_string()))
    }

    pub(crate) fn from_verified_object(
        reference: StoreRootRef,
        object: crate::protocol::objects::VerifiedObject<StoreProtocolRoot>,
    ) -> Result<Self, StoreProtocolError> {
        let verified_reference = StoreRootRef {
            store_root_id: object.value.descriptor.store_root_id(),
            store_root_hash: object.semantic_hash,
            object: object.object.clone(),
        };
        if verified_reference != reference {
            return Err(StoreProtocolError::Malformed(
                "verified Store root belongs to another exact reference".to_string(),
            ));
        }
        Ok(Self { reference, object })
    }

    pub(crate) fn reference(&self) -> &StoreRootRef {
        &self.reference
    }

    pub(crate) fn protocol(&self) -> &StoreProtocolRoot {
        &self.object.value
    }

    pub(crate) fn object(&self) -> &crate::protocol::objects::VerifiedObject<StoreProtocolRoot> {
        &self.object
    }
}

pub(super) async fn load_pinned_store_protocol_root(
    storage: &dyn SyncStorage,
    expected: &StoreRootRef,
) -> Result<crate::protocol::objects::VerifiedObject<StoreProtocolRoot>, StoreProtocolRootError> {
    let context = crate::protocol::objects::ProtocolObjectContext::signed_plaintext(
        expected.store_root_hash,
        ProtocolObjectDomain::StoreProtocolRoot,
    );
    let bytes = storage
        .read_protocol_object(
            &context,
            &expected.object,
            crate::protocol::store_commit::store_protocol_root_logical_key(),
        )
        .await
        .map_err(StoreObjectError::from)?;
    let verified = StoreProtocolRoot::parse_pinned(&bytes, expected).map_err(|source| {
        StoreObjectError::InvalidObject {
            semantic_prefix: crate::protocol::store_commit::store_protocol_root_logical_key()
                .to_string(),
            key: expected.object.slot().logical_key().to_string(),
            source: Box::new(source),
        }
    })?;
    Ok(crate::protocol::objects::VerifiedObject {
        value: verified,
        bytes,
        semantic_hash: expected.store_root_hash,
        object: expected.object.clone(),
    })
}

pub(super) async fn load_exact_store_protocol_root(
    storage: &dyn SyncStorage,
    expected: &StoreRootRef,
    expected_sync_routing_hash: ObjectHash,
) -> Result<crate::protocol::objects::VerifiedObject<StoreProtocolRoot>, StoreProtocolRootError> {
    let verified = load_pinned_store_protocol_root(storage, expected).await?;
    if verified.value.descriptor.sync_routing_hash != expected_sync_routing_hash {
        return Err(StoreObjectError::InvalidObject {
            semantic_prefix: crate::protocol::store_commit::store_protocol_root_logical_key()
                .to_string(),
            key: expected.object.slot().logical_key().to_string(),
            source: Box::new(
                crate::protocol::store_commit::StoreProtocolError::SyncRoutingMismatch {
                    expected: expected_sync_routing_hash,
                    actual: verified.value.descriptor.sync_routing_hash,
                },
            ),
        }
        .into());
    }
    Ok(verified)
}
