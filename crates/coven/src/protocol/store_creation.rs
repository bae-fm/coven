//! Durable record of an in-flight Store creation: the probe identities and
//! outcome states persisted in `protocol_state` so an interrupted creation is
//! resumed or abandoned exactly.

use crate::protocol::store_commit::{DeviceStreamAnchor, ObjectHash, StoreCreationId};

pub(crate) const STORE_CREATION_ATTEMPT_STATE_KEY: &str = "store_creation_attempt_v1";

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
