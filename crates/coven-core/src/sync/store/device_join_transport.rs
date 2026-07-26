//! Storage-mediated delivery for the device-join exchange.
//!
//! The join protocol in [`crate::sync::store::device_join`] produces nine signed
//! artifacts plus the unwind artifacts, and hands each to the host as a
//! [`DeviceJoinAction`] to deliver however it likes. This module is the delivery
//! coven ships by default: each artifact travels as one create-once object in
//! the store's cloud home, under a per-attempt namespace, sealed with a key
//! minted for that attempt alone.
//!
//! The layer carries bytes and nothing else. It never inspects an artifact
//! beyond naming which slot it belongs in, and unsealing is not part of the
//! trust story — the artifact's own signature and hash chaining, checked by the
//! protocol when it accepts the artifact, are.
//!
//! The offer does not travel here. It is the out-of-band kickoff: the host
//! encodes a [`DeviceJoinOfferBundle`] (the offer plus the slots and seal key
//! this module needs) as a QR, a link, or a typed code, and the joiner's copy of
//! that bundle is what bootstraps everything below.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::encryption::{EncryptionService, MasterKeyring, SealError};
use crate::keys::UserKeypair;
use crate::storage::cloud::ObjectSlot;
use crate::sync::storage::{
    ProtocolObjectContext, ProtocolObjectDomain, StorageError, SyncStorage,
};
use crate::sync::store::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinCleanupReceipt, DeviceJoinError, DeviceJoinOffer,
    DeviceJoinReadiness, DeviceJoinRole, DeviceJoinStatus, DeviceProviderAccessAdministrator,
    DeviceProviderAccessRequest, DeviceProviderAdmissionApproval,
    DeviceProviderAdmissionCompletion, DeviceRegistrationRequest, JoinerJoinTerminal,
    ProviderAdminJoinTerminal, ProvisionalDeviceBootstrap, Store,
};
use crate::sync::store_commit::{DeviceJoinAttemptId, ObjectHash, STORE_PROTOCOL_VERSION};

/// The prefix every transport slot's logical key starts with.
const TRANSPORT_ROOT: &str = "store-v1/device-join-transport";

/// Domain separation for the per-attempt seal, so a sealed artifact cannot be
/// opened as anything but the kind and attempt it was written for.
const SEAL_AAD_LABEL: &[u8] = b"coven.device-join-transport.v1";

/// One artifact kind in transit. Every kind has exactly one producing role in
/// the protocol and exactly one slot per attempt.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case", deny_unknown_fields)]
pub enum DeviceJoinTransportKind {
    ProviderAccessRequest,
    ProviderAdmissionApproval,
    RegistrationRequest,
    ProvisionalBootstrap,
    ProviderReadyBootstrap,
    Readiness,
    ProviderAdmissionCompletion,
    Activation,
    Abandonment,
    Cancellation,
    ProviderAdminTerminal,
    JoinerTerminal,
    CleanupReceipt,
    CleanupActivation,
}

impl DeviceJoinTransportKind {
    /// Every kind, in protocol order. An attempt's namespace holds one slot per
    /// entry — allocated together, deleted together.
    pub const ALL: [Self; 14] = [
        Self::ProviderAccessRequest,
        Self::ProviderAdmissionApproval,
        Self::RegistrationRequest,
        Self::ProvisionalBootstrap,
        Self::ProviderReadyBootstrap,
        Self::Readiness,
        Self::ProviderAdmissionCompletion,
        Self::Activation,
        Self::Abandonment,
        Self::Cancellation,
        Self::ProviderAdminTerminal,
        Self::JoinerTerminal,
        Self::CleanupReceipt,
        Self::CleanupActivation,
    ];

    /// The last path component of this kind's slot.
    fn slug(self) -> &'static str {
        match self {
            Self::ProviderAccessRequest => "provider-access-request",
            Self::ProviderAdmissionApproval => "provider-admission-approval",
            Self::RegistrationRequest => "registration-request",
            Self::ProvisionalBootstrap => "provisional-bootstrap",
            Self::ProviderReadyBootstrap => "provider-ready-bootstrap",
            Self::Readiness => "readiness",
            Self::ProviderAdmissionCompletion => "provider-admission-completion",
            Self::Activation => "activation",
            Self::Abandonment => "abandonment",
            Self::Cancellation => "cancellation",
            Self::ProviderAdminTerminal => "provider-admin-terminal",
            Self::JoinerTerminal => "joiner-terminal",
            Self::CleanupReceipt => "cleanup-receipt",
            Self::CleanupActivation => "cleanup-activation",
        }
    }

    /// The one role the protocol lets produce this kind. A publish from any
    /// other role is refused before it reaches storage.
    fn producer(self) -> DeviceJoinRole {
        match self {
            Self::ProviderAccessRequest
            | Self::RegistrationRequest
            | Self::Readiness
            | Self::JoinerTerminal => DeviceJoinRole::Joiner,
            Self::ProviderAdmissionApproval
            | Self::ProviderReadyBootstrap
            | Self::ProviderAdmissionCompletion
            | Self::ProviderAdminTerminal => DeviceJoinRole::ProviderAdministrator,
            Self::ProvisionalBootstrap
            | Self::Activation
            | Self::Abandonment
            | Self::Cancellation
            | Self::CleanupReceipt
            | Self::CleanupActivation => DeviceJoinRole::Owner,
        }
    }

    /// The kind an action's artifact belongs in, or `None` for the actions that
    /// name local work rather than a transfer (`CompleteJoin`,
    /// `CompleteCleanup`, `ResumeOperation`) and for the offer, which travels
    /// out of band.
    fn of(action: &DeviceJoinAction) -> Option<Self> {
        match action {
            DeviceJoinAction::TransferProviderAccessRequest(_) => Some(Self::ProviderAccessRequest),
            DeviceJoinAction::TransferProviderAdmissionApproval(_) => {
                Some(Self::ProviderAdmissionApproval)
            }
            DeviceJoinAction::TransferRegistrationRequest(_) => Some(Self::RegistrationRequest),
            DeviceJoinAction::TransferProvisionalBootstrap(_) => Some(Self::ProvisionalBootstrap),
            DeviceJoinAction::TransferProviderReadyBootstrap(_) => {
                Some(Self::ProviderReadyBootstrap)
            }
            DeviceJoinAction::TransferReadiness(_) => Some(Self::Readiness),
            DeviceJoinAction::TransferProviderAdmissionCompletion(_) => {
                Some(Self::ProviderAdmissionCompletion)
            }
            DeviceJoinAction::TransferActivation(_) => Some(Self::Activation),
            DeviceJoinAction::TransferAbandonment(_) => Some(Self::Abandonment),
            DeviceJoinAction::TransferCancellation(_) => Some(Self::Cancellation),
            DeviceJoinAction::TransferProviderAdminTerminal(_) => Some(Self::ProviderAdminTerminal),
            DeviceJoinAction::TransferJoinerTerminal(_) => Some(Self::JoinerTerminal),
            DeviceJoinAction::TransferCleanupReceipt(_) => Some(Self::CleanupReceipt),
            DeviceJoinAction::TransferCleanupActivation(_) => Some(Self::CleanupActivation),
            DeviceJoinAction::TransferOffer(_)
            | DeviceJoinAction::CompleteJoin(_)
            | DeviceJoinAction::CompleteCleanup(_)
            | DeviceJoinAction::ResumeOperation { .. } => None,
        }
    }
}

/// The artifact type a kind carries. Awaiting a kind yields exactly this type,
/// so a caller never re-matches the action enum it just asked for by kind.
pub trait DeviceJoinArtifact: Sized {
    const KIND: DeviceJoinTransportKind;

    fn from_action(action: DeviceJoinAction) -> Option<Self>;
}

macro_rules! device_join_artifact {
    ($type:ty, $kind:ident, $variant:ident) => {
        impl DeviceJoinArtifact for $type {
            const KIND: DeviceJoinTransportKind = DeviceJoinTransportKind::$kind;

            fn from_action(action: DeviceJoinAction) -> Option<Self> {
                match action {
                    DeviceJoinAction::$variant(value) => Some(value),
                    _ => None,
                }
            }
        }
    };
}

device_join_artifact!(
    DeviceProviderAccessRequest,
    ProviderAccessRequest,
    TransferProviderAccessRequest
);
device_join_artifact!(
    DeviceProviderAdmissionApproval,
    ProviderAdmissionApproval,
    TransferProviderAdmissionApproval
);
device_join_artifact!(
    DeviceRegistrationRequest,
    RegistrationRequest,
    TransferRegistrationRequest
);
device_join_artifact!(
    ProvisionalDeviceBootstrap,
    ProvisionalBootstrap,
    TransferProvisionalBootstrap
);
device_join_artifact!(
    crate::sync::store::ProviderReadyDeviceBootstrap,
    ProviderReadyBootstrap,
    TransferProviderReadyBootstrap
);
device_join_artifact!(DeviceJoinReadiness, Readiness, TransferReadiness);
device_join_artifact!(
    DeviceProviderAdmissionCompletion,
    ProviderAdmissionCompletion,
    TransferProviderAdmissionCompletion
);
device_join_artifact!(DeviceJoinActivation, Activation, TransferActivation);
device_join_artifact!(DeviceJoinAbandonment, Abandonment, TransferAbandonment);
device_join_artifact!(DeviceJoinCancellation, Cancellation, TransferCancellation);
device_join_artifact!(
    ProviderAdminJoinTerminal,
    ProviderAdminTerminal,
    TransferProviderAdminTerminal
);
device_join_artifact!(JoinerJoinTerminal, JoinerTerminal, TransferJoinerTerminal);
device_join_artifact!(
    DeviceJoinCleanupReceipt,
    CleanupReceipt,
    TransferCleanupReceipt
);
device_join_artifact!(
    DeviceJoinCleanupActivation,
    CleanupActivation,
    TransferCleanupActivation
);

/// The slots and seal key one attempt's artifacts travel through.
///
/// The owner allocates the slots when it begins the join, because on providers
/// whose exact slots carry an opaque provider locator (Google Drive) a reader
/// cannot derive a slot from its logical key — the same reason the protocol's
/// own attempt, outcome, and registration slots are reserved up front and named
/// in the signed artifact that precedes them.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinTransportParams {
    pub version: u32,
    pub attempt_namespace: String,
    pub slots: BTreeMap<DeviceJoinTransportKind, ObjectSlot>,
    #[serde(with = "seal_key")]
    pub seal_key: MasterKeyring,
}

/// `MasterKeyring` is the codebase's symmetric-key carrier and travels as its
/// own serialized form; the transport adds no second key encoding.
mod seal_key {
    use super::MasterKeyring;
    use serde::{Deserialize, Deserializer, Serializer};

    pub(super) fn serialize<S: Serializer>(
        keyring: &MasterKeyring,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&keyring.to_serialized())
    }

    pub(super) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<MasterKeyring, D::Error> {
        let encoded = String::deserialize(deserializer)?;
        MasterKeyring::from_serialized(&encoded).map_err(serde::de::Error::custom)
    }
}

impl DeviceJoinTransportParams {
    /// Reserve one slot per kind under this attempt's namespace and mint the
    /// attempt's seal key.
    async fn allocate(
        storage: &dyn SyncStorage,
        offer: &DeviceJoinOffer,
    ) -> Result<Self, DeviceJoinTransportError> {
        let attempt_namespace = attempt_namespace(offer.attempt_id);
        let context = slot_context(offer.store_root.store_root_hash);
        let mut slots = BTreeMap::new();
        for kind in DeviceJoinTransportKind::ALL {
            let slot = storage
                .allocate_protocol_slot(
                    &context,
                    &semantic_prefix(&attempt_namespace, kind),
                    ".json",
                )
                .await?;
            slots.insert(kind, slot);
        }
        Ok(Self {
            version: STORE_PROTOCOL_VERSION,
            attempt_namespace,
            slots,
            seal_key: MasterKeyring::generate(),
        })
    }

    fn slot(&self, kind: DeviceJoinTransportKind) -> Result<&ObjectSlot, DeviceJoinTransportError> {
        self.slots
            .get(&kind)
            .ok_or(DeviceJoinTransportError::MissingSlot { kind })
    }

    fn validate_for(&self, offer: &DeviceJoinOffer) -> Result<(), DeviceJoinTransportError> {
        if self.version != STORE_PROTOCOL_VERSION
            || self.attempt_namespace != attempt_namespace(offer.attempt_id)
        {
            return Err(DeviceJoinTransportError::BundleMismatch);
        }
        let context = slot_context(offer.store_root.store_root_hash);
        for kind in DeviceJoinTransportKind::ALL {
            context.validate_slot(
                self.slot(kind)?,
                &semantic_prefix(&self.attempt_namespace, kind),
            )?;
        }
        Ok(())
    }
}

/// The out-of-band kickoff: the offer plus everything the transport needs to
/// carry the rest of the exchange. The host encodes this however it delivers a
/// join code; coven does not choose that encoding.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinOfferBundle {
    pub version: u32,
    pub offer: DeviceJoinOffer,
    pub transport: DeviceJoinTransportParams,
}

impl DeviceJoinOfferBundle {
    /// Mint a bundle for an offer the owner has just made: allocate the
    /// attempt's slots and its seal key.
    pub async fn allocate(
        storage: &dyn SyncStorage,
        offer: DeviceJoinOffer,
    ) -> Result<Self, DeviceJoinTransportError> {
        let transport = DeviceJoinTransportParams::allocate(storage, &offer).await?;
        Ok(Self {
            version: STORE_PROTOCOL_VERSION,
            offer,
            transport,
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("device join offer bundle serialization cannot fail")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DeviceJoinTransportError> {
        let bundle: Self = serde_json::from_slice(bytes)?;
        if bundle.version != STORE_PROTOCOL_VERSION {
            return Err(DeviceJoinTransportError::BundleMismatch);
        }
        bundle.transport.validate_for(&bundle.offer)?;
        Ok(bundle)
    }
}

/// What a joining device found while waiting for its next artifact: the
/// artifact, or the owner's abandonment of the attempt.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeviceJoinStep<T> {
    Continue(T),
    Abandoned(DeviceJoinAbandonment),
}

/// How a driven join ended for the admitting side.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DeviceJoinDriveOutcome {
    Activated(DeviceJoinActivation),
    Abandoned(DeviceJoinAbandonment),
}

/// How often to look for a counterpart's artifact, and how long to keep
/// looking before giving up on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceJoinTransportTiming {
    pub poll: Duration,
    pub deadline: Duration,
}

/// Why a transfer through the transport failed.
#[derive(Debug, thiserror::Error)]
pub enum DeviceJoinTransportError {
    #[error("storage: {0}")]
    Storage(#[from] StorageError),
    #[error("device join: {0}")]
    DeviceJoin(#[from] DeviceJoinError),
    #[error("transport artifact is not valid JSON: {0}")]
    Malformed(#[from] serde_json::Error),
    #[error("transport artifact could not be unsealed: {0}")]
    Unsealable(#[from] SealError),
    #[error("the offer bundle does not describe this attempt's transport")]
    BundleMismatch,
    #[error("this attempt's transport has no {kind:?} slot")]
    MissingSlot { kind: DeviceJoinTransportKind },
    /// The action carries no transferable artifact — the offer travels out of
    /// band, and `CompleteJoin`/`CompleteCleanup`/`ResumeOperation` name local
    /// work rather than a transfer.
    #[error("{0:?} carries nothing for the transport to deliver")]
    NotTransferable(Box<DeviceJoinAction>),
    /// Only one role produces each kind, and this device does not hold it.
    #[error("a {kind:?} artifact is the {role:?}'s to publish, not this device's")]
    WrongProducer {
        kind: DeviceJoinTransportKind,
        role: DeviceJoinRole,
    },
    /// The slot already holds a different artifact of this kind. Republishing
    /// the same artifact after a crash succeeds; a different one never
    /// overwrites what a counterpart may already have read.
    #[error("the {kind:?} slot already holds a different artifact")]
    ArtifactConflict { kind: DeviceJoinTransportKind },
    /// The slot's stored bytes are not the ones this write produced — a
    /// concurrent writer reached it first.
    #[error("the {kind:?} slot was written concurrently with different bytes")]
    SlotConflict { kind: DeviceJoinTransportKind },
    /// The unsealed bytes decode as a different kind than the slot they sat in.
    #[error("the {kind:?} slot holds an artifact of another kind")]
    KindMismatch { kind: DeviceJoinTransportKind },
    #[error("the {producer:?} never published its {kind:?} artifact")]
    Timeout {
        kind: DeviceJoinTransportKind,
        producer: DeviceJoinRole,
    },
}

/// The roles one device plays in an attempt. Both admitting roles are usually
/// the same device; the joining device holds only its own.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceJoinRoles {
    pub owner: bool,
    pub provider_administrator: bool,
    pub joiner: bool,
}

impl DeviceJoinRoles {
    pub fn joiner() -> Self {
        Self {
            owner: false,
            provider_administrator: false,
            joiner: true,
        }
    }

    pub fn admitting(owner: bool, provider_administrator: bool) -> Self {
        Self {
            owner,
            provider_administrator,
            joiner: false,
        }
    }

    fn holds(self, role: DeviceJoinRole) -> bool {
        match role {
            DeviceJoinRole::Owner => self.owner,
            DeviceJoinRole::ProviderAdministrator => self.provider_administrator,
            DeviceJoinRole::Joiner => self.joiner,
        }
    }

    fn any(self) -> bool {
        self.owner || self.provider_administrator || self.joiner
    }
}

/// One attempt's slot namespace, bound to the roles this device plays in it.
pub struct DeviceJoinTransport<'a> {
    storage: &'a dyn SyncStorage,
    params: &'a DeviceJoinTransportParams,
    store_root_hash: ObjectHash,
    seal: EncryptionService,
    roles: DeviceJoinRoles,
}

impl<'a> DeviceJoinTransport<'a> {
    /// Open the transport described by `bundle` against `storage`, for the
    /// roles this device plays. It may publish only the kinds those roles
    /// produce; it may read every kind.
    pub fn open(
        storage: &'a dyn SyncStorage,
        bundle: &'a DeviceJoinOfferBundle,
        roles: DeviceJoinRoles,
    ) -> Result<Self, DeviceJoinTransportError> {
        bundle.transport.validate_for(&bundle.offer)?;
        Ok(Self {
            storage,
            params: &bundle.transport,
            store_root_hash: bundle.offer.store_root.store_root_hash,
            seal: EncryptionService::from(bundle.transport.seal_key.clone()),
            roles,
        })
    }

    /// Seal an artifact and create it at its slot.
    ///
    /// Republishing an artifact already at its slot succeeds — that is what a
    /// crash between the durable journal advance and the create resumes into.
    /// The seal draws a fresh nonce per call, so sameness is decided on the
    /// artifact, not on the stored ciphertext; the first write's bytes stay.
    /// A *different* artifact at an occupied slot is refused: a counterpart may
    /// already have read what is there.
    pub async fn publish(&self, action: &DeviceJoinAction) -> Result<(), DeviceJoinTransportError> {
        let kind = DeviceJoinTransportKind::of(action)
            .ok_or_else(|| DeviceJoinTransportError::NotTransferable(Box::new(action.clone())))?;
        let producer = kind.producer();
        if !self.roles.holds(producer) {
            return Err(DeviceJoinTransportError::WrongProducer {
                kind,
                role: producer,
            });
        }
        if let Some(existing) = self.read(kind).await? {
            return if existing == *action {
                Ok(())
            } else {
                Err(DeviceJoinTransportError::ArtifactConflict { kind })
            };
        }
        let sealed = self
            .seal
            .seal_app_data(&serde_json::to_vec(action)?, &self.seal_aad(kind));
        let prepared = self.storage.prepare_protocol_object(
            &slot_context(self.store_root_hash),
            self.params.slot(kind)?.clone(),
            &self.semantic_prefix(kind),
            sealed,
        )?;
        self.storage
            .create_protocol_object(&prepared)
            .await
            .map_err(|error| match error {
                StorageError::SlotCollision(_) => DeviceJoinTransportError::SlotConflict { kind },
                other => other.into(),
            })
    }

    /// Read one kind's artifact, or `None` while its slot is still empty.
    pub async fn read(
        &self,
        kind: DeviceJoinTransportKind,
    ) -> Result<Option<DeviceJoinAction>, DeviceJoinTransportError> {
        let sealed = match self
            .storage
            .read_protocol_slot(
                &slot_context(self.store_root_hash),
                self.params.slot(kind)?,
                &self.semantic_prefix(kind),
            )
            .await
        {
            Ok((sealed, _)) => sealed,
            Err(StorageError::NotFound(_)) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let opened = self.seal.open_app_data(&sealed, &self.seal_aad(kind))?;
        let action: DeviceJoinAction = serde_json::from_slice(&opened)?;
        if DeviceJoinTransportKind::of(&action) != Some(kind) {
            return Err(DeviceJoinTransportError::KindMismatch { kind });
        }
        Ok(Some(action))
    }

    /// Poll for the counterpart's artifact of type `T` until the deadline. The
    /// timeout names the role that never published, so a host can tell the user
    /// which device it is waiting on.
    pub async fn await_artifact<T: DeviceJoinArtifact>(
        &self,
        timing: DeviceJoinTransportTiming,
    ) -> Result<T, DeviceJoinTransportError> {
        let kind = T::KIND;
        let polled = tokio::time::timeout(timing.deadline, async {
            loop {
                if let Some(action) = self.read(kind).await? {
                    return T::from_action(action)
                        .ok_or(DeviceJoinTransportError::KindMismatch { kind });
                }
                tokio::time::sleep(timing.poll).await;
            }
        })
        .await;
        match polled {
            Ok(artifact) => artifact,
            Err(_) => Err(DeviceJoinTransportError::Timeout {
                kind,
                producer: kind.producer(),
            }),
        }
    }

    /// Poll for the next artifact of type `T`, or for the owner's abandonment
    /// of the whole attempt, whichever appears first.
    ///
    /// The owner may give up on an attempt while the joining device is waiting
    /// for the next step, so every joiner wait watches both slots. A wait that
    /// watched only its own kind would sit until its deadline against an
    /// abandonment already published.
    pub async fn await_step<T: DeviceJoinArtifact>(
        &self,
        timing: DeviceJoinTransportTiming,
    ) -> Result<DeviceJoinStep<T>, DeviceJoinTransportError> {
        let kind = T::KIND;
        let polled = tokio::time::timeout(timing.deadline, async {
            loop {
                if let Some(action) = self.read(DeviceJoinTransportKind::Abandonment).await? {
                    return DeviceJoinAbandonment::from_action(action)
                        .map(DeviceJoinStep::Abandoned)
                        .ok_or(DeviceJoinTransportError::KindMismatch {
                            kind: DeviceJoinTransportKind::Abandonment,
                        });
                }
                if let Some(action) = self.read(kind).await? {
                    return T::from_action(action)
                        .map(DeviceJoinStep::Continue)
                        .ok_or(DeviceJoinTransportError::KindMismatch { kind });
                }
                tokio::time::sleep(timing.poll).await;
            }
        })
        .await;
        match polled {
            Ok(step) => step,
            Err(_) => Err(DeviceJoinTransportError::Timeout {
                kind,
                producer: kind.producer(),
            }),
        }
    }

    /// Remove every slot this attempt reserved.
    ///
    /// Called once the exchange has reached an end the joining device has
    /// consumed — its completed join, its accepted abandonment, or its accepted
    /// cleanup activation. The joining device is the last reader on all three,
    /// which is why the deletion is its to make: the owner has no artifact by
    /// which it could learn that the joiner read the last thing it published.
    /// There is no sweep behind this — an attempt that reaches none of those
    /// ends keeps its slots until its cancellation removes them.
    pub async fn delete_attempt_slots(&self) -> Result<(), DeviceJoinTransportError> {
        for kind in DeviceJoinTransportKind::ALL {
            let prepared = match self
                .storage
                .read_prepared_protocol_slot(
                    &slot_context(self.store_root_hash),
                    self.params.slot(kind)?,
                    &self.semantic_prefix(kind),
                )
                .await
            {
                Ok((_, prepared)) => prepared,
                Err(StorageError::NotFound(_)) => continue,
                Err(error) => return Err(error.into()),
            };
            self.storage
                .delete_protocol_object(prepared.reference())
                .await?;
        }
        Ok(())
    }

    fn semantic_prefix(&self, kind: DeviceJoinTransportKind) -> String {
        semantic_prefix(&self.params.attempt_namespace, kind)
    }

    /// Bind a sealed artifact to its store, its attempt, and its kind, so bytes
    /// lifted from one slot cannot be opened as another.
    fn seal_aad(&self, kind: DeviceJoinTransportKind) -> Vec<u8> {
        let prefix = self.semantic_prefix(kind);
        let mut aad = SEAL_AAD_LABEL.to_vec();
        aad.extend_from_slice(self.store_root_hash.as_bytes());
        aad.extend_from_slice(&(prefix.len() as u64).to_le_bytes());
        aad.extend_from_slice(prefix.as_bytes());
        aad
    }
}

fn attempt_namespace(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{TRANSPORT_ROOT}/{attempt_id}")
}

fn semantic_prefix(attempt_namespace: &str, kind: DeviceJoinTransportKind) -> String {
    format!("{attempt_namespace}/{}", kind.slug())
}

fn slot_context(store_root_hash: ObjectHash) -> ProtocolObjectContext {
    ProtocolObjectContext::recipient_sealed(
        store_root_hash,
        ProtocolObjectDomain::DeviceJoinTransport,
    )
}

/// Whether the driver approves an access request, and on whose say-so.
pub enum DeviceJoinApprovalPolicy<'a> {
    /// Approve requests against an attempt this device itself issued: its own
    /// owner journal holds the attempt, and the request carries the offer this
    /// bundle names. Anything else is refused. The host opts into this; it is
    /// never the implicit behavior.
    AutoApproveSelfIssued,
    /// Ask the host, which prompts whoever is at the device.
    Ask(&'a (dyn Fn(&DeviceProviderAccessRequest) -> DeviceJoinApproval + Send + Sync)),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeviceJoinApproval {
    Approve,
    Refuse,
}

/// Advance the Owner and Provider Administrator journals as the joiner's
/// artifacts arrive, publishing each artifact this device produces.
///
/// The two roles are usually the same device, and then this drives the whole
/// admitting side. When they are separate devices, each runs this with its own
/// keys: the role checks below find only that device's work, and every artifact
/// it does not produce it waits for in the transport instead.
///
/// Returns when the join reaches its activation. The joiner's completion is the
/// joiner's own step.
pub async fn drive_device_join(
    store: &Store,
    identity_signer: &UserKeypair,
    bundle: &DeviceJoinOfferBundle,
    policy: DeviceJoinApprovalPolicy<'_>,
    access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    timing: DeviceJoinTransportTiming,
) -> Result<DeviceJoinDriveOutcome, DeviceJoinTransportError> {
    let roles = driver_roles(store, &bundle.offer).await?;
    let transport = DeviceJoinTransport::open(&**store.storage(), bundle, roles)?;
    let attempt_id = bundle.offer.attempt_id;

    // An abandoned attempt has no further step to drive. Publishing it here is
    // what lets a driver started after the abandonment still deliver it to a
    // joining device that has not seen it yet.
    if let Some(DeviceJoinStatus::Abandoned { abandonment }) =
        owner_status(store, attempt_id).await?
    {
        transport
            .publish(&DeviceJoinAction::TransferAbandonment(abandonment.clone()))
            .await?;
        return Ok(DeviceJoinDriveOutcome::Abandoned(abandonment));
    }

    // The admitting side produces four artifacts in a fixed order, each after
    // one of the joiner's. Every phase below starts from its role journal's
    // durable state rather than from the beginning: a journal already holding
    // the artifact republishes it (the crash between producing and publishing),
    // and a journal past it does nothing (its artifact was published, which is
    // how the journal got past it).
    if roles.provider_administrator {
        let approval = match admin_status(store, attempt_id).await? {
            Some(DeviceJoinStatus::AwaitingRegistrationRequest { approval }) => Some(approval),
            Some(
                DeviceJoinStatus::AwaitingChallengePublication { .. }
                | DeviceJoinStatus::AwaitingReadiness { .. }
                | DeviceJoinStatus::AwaitingProviderCompletion { .. }
                | DeviceJoinStatus::AwaitingActivation { .. },
            ) => None,
            _ => {
                let request = transport
                    .await_artifact::<DeviceProviderAccessRequest>(timing)
                    .await?;
                approve_access_request(store, roles, &bundle.offer, &request, &policy).await?;
                Some(
                    store
                        .authorize_device_provider_access(
                            identity_signer,
                            request,
                            access_administrator,
                        )
                        .await?,
                )
            }
        };
        if let Some(approval) = approval {
            transport
                .publish(&DeviceJoinAction::TransferProviderAdmissionApproval(
                    approval,
                ))
                .await?;
        }
    }

    if roles.owner {
        let provisional = match owner_status(store, attempt_id).await? {
            Some(DeviceJoinStatus::AwaitingChallengePublication { bootstrap }) => Some(bootstrap),
            Some(
                DeviceJoinStatus::AwaitingActivation { .. }
                | DeviceJoinStatus::AwaitingCompletion { .. },
            ) => None,
            Some(DeviceJoinStatus::AwaitingBootstrap { request }) => Some(
                store
                    .accept_device_registration_request(identity_signer, request)
                    .await?,
            ),
            _ => {
                let request = transport
                    .await_artifact::<DeviceRegistrationRequest>(timing)
                    .await?;
                Some(
                    store
                        .accept_device_registration_request(identity_signer, request)
                        .await?,
                )
            }
        };
        if let Some(provisional) = provisional {
            transport
                .publish(&DeviceJoinAction::TransferProvisionalBootstrap(provisional))
                .await?;
        }
    }

    if roles.provider_administrator {
        let ready = match admin_status(store, attempt_id).await? {
            Some(DeviceJoinStatus::AwaitingReadiness { bootstrap }) => Some(bootstrap),
            Some(
                DeviceJoinStatus::AwaitingProviderCompletion { .. }
                | DeviceJoinStatus::AwaitingActivation { .. },
            ) => None,
            Some(DeviceJoinStatus::AwaitingChallengePublication { bootstrap }) => {
                Some(store.publish_device_provider_challenge(bootstrap).await?)
            }
            _ => {
                let provisional = transport
                    .await_artifact::<ProvisionalDeviceBootstrap>(timing)
                    .await?;
                Some(store.publish_device_provider_challenge(provisional).await?)
            }
        };
        if let Some(ready) = ready {
            transport
                .publish(&DeviceJoinAction::TransferProviderReadyBootstrap(ready))
                .await?;
        }

        let completion = match admin_status(store, attempt_id).await? {
            Some(DeviceJoinStatus::AwaitingActivation { completion }) => completion,
            Some(DeviceJoinStatus::AwaitingProviderCompletion { readiness }) => {
                store
                    .complete_device_provider_admission(identity_signer, readiness)
                    .await?
            }
            _ => {
                let readiness = transport
                    .await_artifact::<DeviceJoinReadiness>(timing)
                    .await?;
                store
                    .complete_device_provider_admission(identity_signer, readiness)
                    .await?
            }
        };
        transport
            .publish(&DeviceJoinAction::TransferProviderAdmissionCompletion(
                completion,
            ))
            .await?;
    }

    if !roles.owner {
        return Ok(DeviceJoinDriveOutcome::Activated(
            transport
                .await_artifact::<DeviceJoinActivation>(timing)
                .await?,
        ));
    }

    let activation = match owner_status(store, attempt_id).await? {
        Some(DeviceJoinStatus::AwaitingCompletion { activation }) => activation,
        Some(DeviceJoinStatus::AwaitingActivation { completion }) => {
            store
                .finalize_device_join(identity_signer, completion)
                .await?
        }
        _ => {
            let completion = transport
                .await_artifact::<DeviceProviderAdmissionCompletion>(timing)
                .await?;
            store
                .finalize_device_join(identity_signer, completion)
                .await?
        }
    };
    transport
        .publish(&DeviceJoinAction::TransferActivation(activation.clone()))
        .await?;
    Ok(DeviceJoinDriveOutcome::Activated(activation))
}

/// Give up on an attempt and publish the abandonment, so a joining device
/// waiting on its next artifact learns the join is over instead of waiting out
/// its deadline.
pub async fn abandon_device_join_via_transport(
    store: &Store,
    identity_signer: &UserKeypair,
    bundle: &DeviceJoinOfferBundle,
) -> Result<DeviceJoinAbandonment, DeviceJoinTransportError> {
    let roles = driver_roles(store, &bundle.offer).await?;
    let transport = DeviceJoinTransport::open(&**store.storage(), bundle, roles)?;
    let abandonment = store
        .abandon_device_join(identity_signer, bundle.offer.clone())
        .await?;
    transport
        .publish(&DeviceJoinAction::TransferAbandonment(abandonment.clone()))
        .await?;
    Ok(abandonment)
}

async fn admin_status(
    store: &Store,
    attempt_id: DeviceJoinAttemptId,
) -> Result<Option<DeviceJoinStatus>, DeviceJoinTransportError> {
    Ok(store
        .database()
        .device_join_status(attempt_id, DeviceJoinRole::ProviderAdministrator)
        .await?)
}

async fn owner_status(
    store: &Store,
    attempt_id: DeviceJoinAttemptId,
) -> Result<Option<DeviceJoinStatus>, DeviceJoinTransportError> {
    Ok(store
        .database()
        .device_join_status(attempt_id, DeviceJoinRole::Owner)
        .await?)
}

/// Cancel an attempt and carry the unwind to its activated cleanup.
///
/// The joiner's own closure travels through the transport: the owner publishes
/// the cancellation, waits for the joiner's terminal, and settles the receipt
/// against both terminals. Like the admitting driver, every step starts from
/// the role journal's durable state, so a run that died mid-unwind resumes
/// where it stopped rather than replaying a step its journal is already past.
///
/// The attempt's slots are not deleted here. The joining device reads the
/// cleanup activation this publishes last, and the owner has no artifact by
/// which it could learn that read had happened — so the deletion belongs to the
/// joiner, at the point it accepts that activation.
pub async fn cancel_device_join_via_transport(
    store: &Store,
    identity_signer: &UserKeypair,
    bundle: &DeviceJoinOfferBundle,
    attempt: crate::sync::store_commit::DeviceJoinAttemptRef,
    timing: DeviceJoinTransportTiming,
) -> Result<DeviceJoinCleanupActivation, DeviceJoinTransportError> {
    let roles = driver_roles(store, &bundle.offer).await?;
    let transport = DeviceJoinTransport::open(&**store.storage(), bundle, roles)?;
    let attempt_id = bundle.offer.attempt_id;

    let receipt = match owner_status(store, attempt_id).await? {
        // The unwind already reached its end; republish what it settled on.
        Some(DeviceJoinStatus::CleanupActivated { activation }) => {
            transport
                .publish(&DeviceJoinAction::TransferCleanupActivation(
                    activation.clone(),
                ))
                .await?;
            store
                .complete_owner_device_join_cleanup(activation.clone())
                .await?;
            return Ok(activation);
        }
        Some(DeviceJoinStatus::AwaitingCleanupActivation { receipt }) => receipt,
        _ => {
            let cancellation =
                cancel_and_publish(store, identity_signer, &transport, attempt_id, attempt).await?;
            let administrator_terminal = store
                .close_device_provider_admission(identity_signer, cancellation.clone())
                .await?;
            transport
                .publish(&DeviceJoinAction::TransferProviderAdminTerminal(
                    administrator_terminal.clone(),
                ))
                .await?;
            let joiner_terminal = transport
                .await_artifact::<JoinerJoinTerminal>(timing)
                .await?;
            store
                .prepare_device_join_cleanup(
                    identity_signer,
                    cancellation,
                    administrator_terminal,
                    joiner_terminal,
                )
                .await?
        }
    };
    transport
        .publish(&DeviceJoinAction::TransferCleanupReceipt(receipt.clone()))
        .await?;

    let activation = store
        .activate_device_join_cleanup(identity_signer, receipt)
        .await?;
    transport
        .publish(&DeviceJoinAction::TransferCleanupActivation(
            activation.clone(),
        ))
        .await?;
    store
        .complete_owner_device_join_cleanup(activation.clone())
        .await?;
    Ok(activation)
}

/// The attempt's cancellation, taken from the owner journal when it already
/// holds one — `cancel_device_join` refuses to run again once the unwind has
/// moved on to preparing the cleanup receipt.
async fn cancel_and_publish(
    store: &Store,
    identity_signer: &UserKeypair,
    transport: &DeviceJoinTransport<'_>,
    attempt_id: DeviceJoinAttemptId,
    attempt: crate::sync::store_commit::DeviceJoinAttemptRef,
) -> Result<DeviceJoinCancellation, DeviceJoinTransportError> {
    let cancellation = match owner_status(store, attempt_id).await? {
        Some(
            DeviceJoinStatus::CleanupPending { cancellation, .. }
            | DeviceJoinStatus::CleanupReceiptCreatePending { cancellation, .. },
        ) => cancellation,
        _ => store.cancel_device_join(identity_signer, attempt).await?,
    };
    transport
        .publish(&DeviceJoinAction::TransferCancellation(
            cancellation.clone(),
        ))
        .await?;
    Ok(cancellation)
}

async fn driver_roles(
    store: &Store,
    offer: &DeviceJoinOffer,
) -> Result<DeviceJoinRoles, DeviceJoinTransportError> {
    let local = store
        .database()
        .local_activated_registration_ref()
        .await
        .map_err(|error| DeviceJoinError::Store(error.to_string()))?
        .ok_or(DeviceJoinError::ActiveDeviceRequired)?;
    let roles = DeviceJoinRoles::admitting(
        local == offer.owner_registration,
        local == offer.provider_admin.administrator,
    );
    if !roles.any() {
        return Err(DeviceJoinError::ActiveDeviceRequired.into());
    }
    Ok(roles)
}

async fn approve_access_request(
    store: &Store,
    roles: DeviceJoinRoles,
    offer: &DeviceJoinOffer,
    request: &DeviceProviderAccessRequest,
    policy: &DeviceJoinApprovalPolicy<'_>,
) -> Result<(), DeviceJoinTransportError> {
    let approval = match policy {
        DeviceJoinApprovalPolicy::AutoApproveSelfIssued => {
            if self_issued(store, roles, offer).await? && *request.offer == *offer {
                DeviceJoinApproval::Approve
            } else {
                DeviceJoinApproval::Refuse
            }
        }
        DeviceJoinApprovalPolicy::Ask(ask) => ask(request),
    };
    match approval {
        DeviceJoinApproval::Approve => Ok(()),
        DeviceJoinApproval::Refuse => Err(DeviceJoinError::OfferMismatch.into()),
    }
}

/// Whether this device issued the offer being admitted — the bound
/// `AutoApproveSelfIssued` keeps to.
///
/// Two facts decide it, both authoritative: this device is the offer's owner,
/// and its own owner journal holds a record for this attempt. That record
/// exists only because this device ran `begin_device_join` for it. A provider
/// administrator that is a *different* device never satisfies this, so it
/// prompts rather than admitting an offer it did not make.
async fn self_issued(
    store: &Store,
    roles: DeviceJoinRoles,
    offer: &DeviceJoinOffer,
) -> Result<bool, DeviceJoinTransportError> {
    if !roles.owner {
        return Ok(false);
    }
    Ok(store
        .database()
        .device_join_status(offer.attempt_id, DeviceJoinRole::Owner)
        .await?
        .is_some())
}
