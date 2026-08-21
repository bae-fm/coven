//! Storage-mediated delivery for the device-join exchange.
//!
//! The join protocol owned by [`crate::sync::store::Store`] produces signed
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

use crate::sync::store::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinError,
    DeviceJoinOffer, DeviceJoinReadiness, DeviceJoinRole, DeviceJoinStatus,
    DeviceProviderAccessAdministrator, DeviceProviderAccessRequest,
    DeviceProviderAdmissionApproval, DeviceRegistrationRequest, SamePrincipalDeviceJoin, Store,
};
use coven_keys::encryption::{EncryptionService, MasterKeyring, SealError};
use coven_protocol::objects::ObjectSlot;
use coven_protocol::objects::{ProtocolObjectContext, ProtocolObjectDomain, StorageError};
use coven_protocol::store_commit::device_join_exchange::DeviceProviderAdmission;
use coven_protocol::store_commit::device_join_exchange::DeviceProviderChallengePublication;
use coven_protocol::store_commit::{DeviceJoinAttemptId, ObjectHash, STORE_PROTOCOL_VERSION};
use coven_storage::CloudSyncObjectStorage;

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
    ProviderReadyBootstrap,
    Readiness,
    SamePrincipalJoin,
    Activation,
    Abandonment,
}

impl DeviceJoinTransportKind {
    /// Every kind, in protocol order. An attempt's namespace holds one slot per
    /// entry — allocated together, deleted together.
    pub const ALL: [Self; 8] = [
        Self::ProviderAccessRequest,
        Self::ProviderAdmissionApproval,
        Self::RegistrationRequest,
        Self::ProviderReadyBootstrap,
        Self::Readiness,
        Self::SamePrincipalJoin,
        Self::Activation,
        Self::Abandonment,
    ];

    /// The last path component of this kind's slot.
    fn slug(self) -> &'static str {
        match self {
            Self::ProviderAccessRequest => "provider-access-request",
            Self::ProviderAdmissionApproval => "provider-admission-approval",
            Self::RegistrationRequest => "registration-request",
            Self::ProviderReadyBootstrap => "provider-ready-bootstrap",
            Self::Readiness => "readiness",
            Self::SamePrincipalJoin => "same-principal-join",
            Self::Activation => "activation",
            Self::Abandonment => "abandonment",
        }
    }

    /// The one role the protocol lets produce this kind. A publish from any
    /// other role is refused before it reaches storage.
    fn producer(self) -> DeviceJoinRole {
        match self {
            Self::ProviderAccessRequest | Self::RegistrationRequest | Self::Readiness => {
                DeviceJoinRole::Joiner
            }
            Self::ProviderAdmissionApproval
            | Self::ProviderReadyBootstrap
            | Self::SamePrincipalJoin
            | Self::Activation
            | Self::Abandonment => DeviceJoinRole::Owner,
        }
    }

    /// The kind an action's artifact belongs in, or `None` for the actions that
    /// name local work rather than a transfer (`CompleteJoin`,
    /// `ResumeOperation`) and for the offer, which travels out of band.
    fn of(action: &DeviceJoinAction) -> Option<Self> {
        match action {
            DeviceJoinAction::TransferProviderAccessRequest(_) => Some(Self::ProviderAccessRequest),
            DeviceJoinAction::TransferProviderAdmissionApproval(_) => {
                Some(Self::ProviderAdmissionApproval)
            }
            DeviceJoinAction::TransferRegistrationRequest(_) => Some(Self::RegistrationRequest),
            DeviceJoinAction::TransferProviderReadyBootstrap(_) => {
                Some(Self::ProviderReadyBootstrap)
            }
            DeviceJoinAction::TransferReadiness(_) => Some(Self::Readiness),
            DeviceJoinAction::TransferSamePrincipalJoin(_) => Some(Self::SamePrincipalJoin),
            DeviceJoinAction::TransferActivation(_) => Some(Self::Activation),
            DeviceJoinAction::TransferAbandonment(_) => Some(Self::Abandonment),
            DeviceJoinAction::TransferOffer(_)
            | DeviceJoinAction::CompleteJoin(_)
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
    coven_protocol::store_commit::device_join_exchange::ProviderReadyDeviceBootstrap,
    ProviderReadyBootstrap,
    TransferProviderReadyBootstrap
);
device_join_artifact!(DeviceJoinReadiness, Readiness, TransferReadiness);
device_join_artifact!(
    SamePrincipalDeviceJoin,
    SamePrincipalJoin,
    TransferSamePrincipalJoin
);
device_join_artifact!(DeviceJoinActivation, Activation, TransferActivation);
device_join_artifact!(DeviceJoinAbandonment, Abandonment, TransferAbandonment);

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
    seal_key: MasterKeyring,
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
    pub(crate) fn new(
        attempt_namespace: String,
        slots: BTreeMap<DeviceJoinTransportKind, ObjectSlot>,
        seal_key: MasterKeyring,
    ) -> Self {
        Self {
            version: STORE_PROTOCOL_VERSION,
            attempt_namespace,
            slots,
            seal_key,
        }
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

/// The joining device's current user-visible operation. These values describe
/// the work actually executing or the exact counterpart artifact being
/// awaited; hosts render them directly instead of collapsing the whole join
/// into one indeterminate state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum JoiningDeviceJoinProgress {
    WaitingForApproval,
    RequestingProviderAccess,
    WaitingForProviderAccess,
    RegisteringDevice,
    WaitingForLibrary,
    DownloadingSnapshot { bytes_done: u64, bytes_total: u64 },
    InstallingSnapshot,
    WaitingForActivation,
    CatchingUp,
    SavingLibrary,
}

/// A joining device's retained progress sink. Provider reads keep a clone while
/// their response stream is active, so every received buffer reaches the host.
pub type JoiningDeviceJoinProgressObserver =
    std::sync::Arc<dyn Fn(JoiningDeviceJoinProgress) + Send + Sync>;

/// The existing device's current user-visible operation while admitting the
/// joining device.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmittingDeviceJoinProgress {
    PreparingInvitation,
    WaitingForProviderAccessRequest,
    GrantingProviderAccess,
    WaitingForRegistrationRequest,
    RegisteringDevice,
    PreparingLibrary,
    WaitingForJoiningDevice,
    ActivatingDevice,
}

/// How often to look for a counterpart's artifact, and how long to keep
/// looking before giving up on it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DeviceJoinTransportTiming {
    pub poll: Duration,
    pub deadline: Duration,
}

impl DeviceJoinTransportTiming {
    /// Pairing's product timing. Hosts render the states; coven decides how
    /// frequently storage and the local pairing endpoint are observed and when
    /// an absent counterpart becomes a failure.
    pub const fn interactive() -> Self {
        Self {
            poll: Duration::from_millis(100),
            deadline: Duration::from_secs(180),
        }
    }

    /// The cadence a wait on this timing uses.
    fn polls(self) -> JoinPollBackoff {
        JoinPollBackoff {
            next: self.poll,
            ceiling: JOIN_POLL_CEILING.max(self.poll),
        }
    }
}

/// The longest a wait ever sleeps between looks.
///
/// A wait on a counterpart is a wait on a person — an owner reading an approval
/// prompt — or on that device's next sync cycle, which is tens of seconds away.
/// Looking every hundred milliseconds for all of it is hundreds of provider
/// reads that answer "not yet", and a provider that rate-limits them makes the
/// join slower, not faster. The first look is immediate and the cadence backs
/// off to this, so a counterpart that answers at once is still seen at once.
const JOIN_POLL_CEILING: Duration = Duration::from_secs(2);

struct JoinPollBackoff {
    next: Duration,
    ceiling: Duration,
}

impl JoinPollBackoff {
    fn next(&mut self) -> Duration {
        let current = self.next;
        self.next = (current * 2).min(self.ceiling);
        current
    }
}

/// Time one owner-side device-join step and report it the way every other
/// staged run reports.
///
/// Each of these is one transition in the Add-a-device flow — approve the
/// provider access, accept the registration, activate — and each is one or more
/// provider round trips, which is what `requests` counts. Two flows reach them:
/// the discrete command API a host drives itself, and the pairing driver's
/// `drive_once`. They share this function so a run through either one reads the
/// same in the log, and each passes the counter of the home it drives.
pub async fn timed_owner_join_step<T>(
    step: &'static str,
    requests: Option<std::sync::Arc<dyn coven_foundation::stage_timing::ProviderRequests>>,
    work: impl std::future::Future<Output = T>,
) -> T {
    let mut timings =
        coven_foundation::stage_timing::StageTimings::counting("Device join owner step", requests);
    let outcome = timings.stage(step, work).await;
    timings.report();
    outcome
}

/// One wait on the counterpart, reported when it ends.
///
/// A join that took four minutes is either waiting on the other device or
/// fetching, and until these lines existed the logs could not say which. The
/// poll count separates a wait that sat through the owner's next sync cycle
/// from one that answered immediately.
struct JoinWait {
    kind: DeviceJoinTransportKind,
    started: coven_foundation::clock::Stopwatch,
    polls: std::sync::atomic::AtomicU64,
}

impl JoinWait {
    fn begin(kind: DeviceJoinTransportKind) -> Self {
        Self {
            kind,
            started: coven_foundation::clock::Stopwatch::start(),
            polls: std::sync::atomic::AtomicU64::new(0),
        }
    }

    fn polled(&self) {
        self.polls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    }

    fn report(self) {
        tracing::info!(
            kind = ?self.kind,
            produced_by = ?self.kind.producer(),
            waited_ms = self.started.elapsed().as_millis() as u64,
            looks = self.polls.load(std::sync::atomic::Ordering::Relaxed),
            "Device join waited for its counterpart"
        );
    }
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

/// One attempt's slot namespace, bound to the side of the exchange this device
/// is on.
pub struct DeviceJoinTransport<'a> {
    storage: &'a dyn CloudSyncObjectStorage,
    params: &'a DeviceJoinTransportParams,
    store_root_hash: ObjectHash,
    seal: EncryptionService,
    role: DeviceJoinRole,
}

impl<'a> DeviceJoinTransport<'a> {
    /// Open the transport described by `bundle` against `storage`, for the role
    /// this device plays. It may publish only the kinds that role produces; it
    /// may read every kind.
    pub fn open(
        storage: &'a dyn CloudSyncObjectStorage,
        bundle: &'a DeviceJoinOfferBundle,
        role: DeviceJoinRole,
    ) -> Result<Self, DeviceJoinTransportError> {
        bundle.transport.validate_for(&bundle.offer)?;
        Ok(Self {
            storage,
            params: &bundle.transport,
            store_root_hash: bundle.offer.store_root.store_root_hash,
            seal: EncryptionService::from(bundle.transport.seal_key.clone()),
            role,
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
        if self.role != producer {
            return Err(DeviceJoinTransportError::WrongProducer {
                kind,
                role: producer,
            });
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
        match self.storage.create_protocol_object(&prepared).await {
            Ok(()) => Ok(()),
            Err(StorageError::SlotCollision(_)) => match self.read(kind).await? {
                Some(existing) if existing == *action => Ok(()),
                Some(_) => Err(DeviceJoinTransportError::ArtifactConflict { kind }),
                None => Err(DeviceJoinTransportError::SlotConflict { kind }),
            },
            Err(error) => Err(error.into()),
        }
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
        let wait = JoinWait::begin(kind);
        let polled = tokio::time::timeout(timing.deadline, async {
            let mut poll = timing.polls();
            loop {
                wait.polled();
                if let Some(action) = self.read(kind).await? {
                    return T::from_action(action)
                        .ok_or(DeviceJoinTransportError::KindMismatch { kind });
                }
                tokio::time::sleep(poll.next()).await;
            }
        })
        .await;
        wait.report();
        match polled {
            Ok(artifact) => artifact,
            Err(_) => Err(DeviceJoinTransportError::Timeout {
                kind,
                producer: kind.producer(),
            }),
        }
    }

    /// Observe one artifact without imposing a phase deadline. A concurrent
    /// operation owns the deadline; this observation exists to interrupt that
    /// operation when a terminal artifact appears.
    ///
    /// This is the longest-running wait in a join and the one least likely to
    /// find anything: it watches for the owner cancelling, for the whole join,
    /// alongside the snapshot download and the install. At the asked-for
    /// cadence that is a provider read every hundred milliseconds for minutes
    /// to answer "not yet" — which is exactly what the poll backoff was
    /// introduced to stop for the phase waits, and this one was left behind
    /// because it takes a bare interval rather than a timing. It takes the
    /// timing now and backs off like the others: the first look is immediate,
    /// so a cancellation still interrupts promptly, and the cadence settles at
    /// the same ceiling instead of running flat out under a several-second
    /// download.
    pub async fn observe_artifact<T: DeviceJoinArtifact>(
        &self,
        timing: DeviceJoinTransportTiming,
    ) -> Result<T, DeviceJoinTransportError> {
        let kind = T::KIND;
        let mut poll = timing.polls();
        loop {
            if let Some(action) = self.read(kind).await? {
                return T::from_action(action)
                    .ok_or(DeviceJoinTransportError::KindMismatch { kind });
            }
            tokio::time::sleep(poll.next()).await;
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
        let wait = JoinWait::begin(kind);
        let polled = tokio::time::timeout(timing.deadline, async {
            let mut poll = timing.polls();
            loop {
                wait.polled();
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
                tokio::time::sleep(poll.next()).await;
            }
        })
        .await;
        wait.report();
        match polled {
            Ok(step) => step,
            Err(_) => Err(DeviceJoinTransportError::Timeout {
                kind,
                producer: kind.producer(),
            }),
        }
    }

    /// Remove everything under this attempt's namespace.
    ///
    /// Called once the exchange has reached an end the joining device has
    /// consumed — its completed join or its accepted abandonment. The joining
    /// device is the last reader on both, which is why the deletion is its to
    /// make: the admitting device has no artifact by which it could learn that
    /// the joiner read the last thing it published. There is no sweep behind
    /// this.
    ///
    /// The namespace is listed rather than probed kind by kind. Probing asks
    /// for every name this build knows and so leaves behind anything written
    /// under a name it does not — an artifact from a different version, or from
    /// anyone else who can write to the provider. A listing names what is
    /// actually there, which is what "remove the namespace" has to mean.
    ///
    /// Each object is still deleted by the exact reference its own stored bytes
    /// produce, so a delete cannot race a concurrent write: the reference
    /// carries the size and hash observed, and the delete refuses if what sits
    /// there no longer matches. Nothing is opened — this is removing a
    /// namespace, not reading it, and an object this device cannot decrypt is
    /// exactly as much garbage as one it can.
    pub async fn delete_attempt_slots(&self) -> Result<(), DeviceJoinTransportError> {
        let context = slot_context(self.store_root_hash);
        let listed = self
            .storage
            .list_protocol_slots(&context, &format!("{}/", self.params.attempt_namespace))
            .await?;
        let deletions = futures_util::future::join_all(listed.iter().map(|slot| async move {
            let Some(object) = self.storage.observe_exact_slot(slot).await? else {
                return Ok(());
            };
            self.storage
                .delete_protocol_object(&object)
                .await
                .map_err(DeviceJoinTransportError::from)
        }))
        .await;
        for result in deletions {
            result?;
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

pub(crate) fn attempt_namespace(attempt_id: DeviceJoinAttemptId) -> String {
    format!("{TRANSPORT_ROOT}/{attempt_id}")
}

pub(crate) fn semantic_prefix(attempt_namespace: &str, kind: DeviceJoinTransportKind) -> String {
    format!("{attempt_namespace}/{}", kind.slug())
}

pub(crate) fn slot_context(store_root_hash: ObjectHash) -> ProtocolObjectContext {
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

pub struct StoreDeviceJoinTransport<'store> {
    store: &'store Store,
}

impl<'store> StoreDeviceJoinTransport<'store> {
    pub(crate) fn new(store: &'store Store) -> Self {
        Self { store }
    }

    pub async fn allocate_bundle(
        &self,
        offer: DeviceJoinOffer,
    ) -> Result<DeviceJoinOfferBundle, DeviceJoinTransportError> {
        self.store
            .allocate_device_join_transport_bundle(offer)
            .await
    }

    pub async fn drive(
        &self,
        bundle: &DeviceJoinOfferBundle,
        policy: DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
        on_progress: &(dyn Fn(AdmittingDeviceJoinProgress) + Send + Sync),
        timing: DeviceJoinTransportTiming,
    ) -> Result<DeviceJoinDriveOutcome, DeviceJoinTransportError> {
        retrying_activation_conflicts(|| async {
            AttemptTransport::open(self.store, bundle)
                .await?
                .drive_once(&policy, access_administrator, on_progress, timing)
                .await
        })
        .await
    }

    pub async fn abandon(
        &self,
        bundle: &DeviceJoinOfferBundle,
    ) -> Result<DeviceJoinAbandonment, DeviceJoinTransportError> {
        let attempt = AttemptTransport::open(self.store, bundle).await?;
        let abandonment = self.store.abandon_device_join(bundle.offer.clone()).await?;
        attempt
            .publish(DeviceJoinAction::TransferAbandonment(abandonment.clone()))
            .await?;
        Ok(abandonment)
    }

    /// Give up on an attempt this device offered.
    ///
    /// Only an attempt that has not reached its Store commit can be given up on:
    /// up to that point nothing is published about the joining device, so an
    /// abandonment is the whole story. Past it the device has been approved and
    /// holds storage access, and taking that back is member removal with a key
    /// rotation — not something a pairing window can do.
    pub async fn abort(
        &self,
        bundle: &DeviceJoinOfferBundle,
    ) -> Result<(), DeviceJoinTransportError> {
        let attempt = AttemptTransport::open(self.store, bundle).await?;
        match attempt.owner_status().await? {
            None
            | Some(
                DeviceJoinStatus::AwaitingAccessRequest { .. }
                | DeviceJoinStatus::AwaitingProviderAdmission { .. }
                | DeviceJoinStatus::ProviderAccessGrantCreatePending { .. }
                | DeviceJoinStatus::AwaitingRegistrationRequest { .. }
                | DeviceJoinStatus::AwaitingBootstrap { .. }
                | DeviceJoinStatus::AbandonmentCreatePending { .. }
                | DeviceJoinStatus::Abandoned { .. },
            ) => {
                self.abandon(bundle).await?;
                Ok(())
            }
            status => Err(DeviceJoinError::Store(format!(
                "device join {} is past the point it could be given up on: {status:?}",
                bundle.offer.attempt_id
            ))
            .into()),
        }
    }
}

/// One attempt in flight: the bundle naming its transport slots and the attempt
/// every status read addresses. Every step of a drive shares both.
struct AttemptTransport<'attempt> {
    store: &'attempt Store,
    bundle: &'attempt DeviceJoinOfferBundle,
    attempt_id: DeviceJoinAttemptId,
}

impl<'attempt> AttemptTransport<'attempt> {
    async fn open(
        store: &'attempt Store,
        bundle: &'attempt DeviceJoinOfferBundle,
    ) -> Result<Self, DeviceJoinTransportError> {
        store.require_device_join_admitter(&bundle.offer).await?;
        Ok(Self {
            store,
            bundle,
            attempt_id: bundle.offer.attempt_id,
        })
    }

    /// Put an artifact at its transport slot. An artifact already at its slot is
    /// the same transfer, so a step that produced its artifact and died before
    /// publishing it republishes here for nothing.
    async fn publish(&self, action: DeviceJoinAction) -> Result<(), DeviceJoinTransportError> {
        self.step(
            "publish artifact",
            self.store
                .publish_device_join_transport_artifact(self.bundle, &action),
        )
        .await
    }

    /// Time one step of the driven exchange under the shared owner-step line.
    ///
    /// The work is boxed: `drive_once` holds a dozen of these, and leaving each
    /// one inline grows its already-large state machine past the stack a test
    /// runner gives it.
    async fn step<T>(&self, step: &'static str, work: impl std::future::Future<Output = T>) -> T {
        timed_owner_join_step(step, self.store.provider_requests(), Box::pin(work)).await
    }

    /// Read the artifact the other side owes this step, waiting for it to appear.
    async fn await_artifact<T: DeviceJoinArtifact>(
        &self,
        timing: DeviceJoinTransportTiming,
    ) -> Result<T, DeviceJoinTransportError> {
        self.store
            .await_device_join_transport_artifact::<T>(self.bundle, timing)
            .await
    }

    async fn owner_status(&self) -> Result<Option<DeviceJoinStatus>, DeviceJoinTransportError> {
        self.store
            .device_join_transport_status(self.attempt_id, DeviceJoinRole::Owner)
            .await
    }

    /// Carry the admitting side of one attempt as far as it will go.
    ///
    /// One device admits, so there is one journal and one status to read: every
    /// pass takes the durable state and performs the step that follows it. A
    /// step that produced its artifact and died before publishing republishes
    /// here for nothing, and a step already past does nothing.
    async fn drive_once(
        &self,
        policy: &DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
        on_progress: &(dyn Fn(AdmittingDeviceJoinProgress) + Send + Sync),
        timing: DeviceJoinTransportTiming,
    ) -> Result<DeviceJoinDriveOutcome, DeviceJoinTransportError> {
        loop {
            match self.owner_status().await? {
                // An abandoned attempt has no further step to drive. Publishing
                // it here is what lets a driver started after the abandonment
                // still deliver it to a joining device that has not seen it yet.
                Some(DeviceJoinStatus::Abandoned { abandonment }) => {
                    self.publish(DeviceJoinAction::TransferAbandonment(abandonment.clone()))
                        .await?;
                    return Ok(DeviceJoinDriveOutcome::Abandoned(abandonment));
                }
                Some(DeviceJoinStatus::SamePrincipalCompleted { join }) => {
                    self.publish(DeviceJoinAction::TransferSamePrincipalJoin(join.clone()))
                        .await?;
                    return Ok(DeviceJoinDriveOutcome::Activated(join.activation));
                }
                Some(DeviceJoinStatus::AwaitingCompletion { activation }) => {
                    self.publish(DeviceJoinAction::TransferActivation(activation.clone()))
                        .await?;
                    return Ok(DeviceJoinDriveOutcome::Activated(activation));
                }
                None | Some(DeviceJoinStatus::AwaitingAccessRequest { .. }) => {
                    on_progress(AdmittingDeviceJoinProgress::WaitingForProviderAccessRequest);
                    let request = self
                        .await_artifact::<DeviceProviderAccessRequest>(timing)
                        .await?;
                    self.step(
                        "approve access request",
                        self.approve_access_request(&request, policy),
                    )
                    .await?;
                    if request.offer.provider_admin.provider == request.peer_provider {
                        on_progress(AdmittingDeviceJoinProgress::RegisteringDevice);
                        let join = self
                            .step(
                                "activate same-provider device",
                                self.activate_same_principal(request, access_administrator),
                            )
                            .await?;
                        self.publish(DeviceJoinAction::TransferSamePrincipalJoin(join.clone()))
                            .await?;
                        return Ok(DeviceJoinDriveOutcome::Activated(join.activation));
                    }
                    on_progress(AdmittingDeviceJoinProgress::GrantingProviderAccess);
                    let approval = self
                        .step(
                            "authorize provider access",
                            self.store
                                .authorize_device_provider_access(request, access_administrator),
                        )
                        .await?;
                    self.publish(DeviceJoinAction::TransferProviderAdmissionApproval(
                        approval,
                    ))
                    .await?;
                }
                Some(
                    DeviceJoinStatus::AwaitingProviderAdmission { request }
                    | DeviceJoinStatus::ProviderAccessGrantCreatePending { request, .. },
                ) => {
                    on_progress(AdmittingDeviceJoinProgress::GrantingProviderAccess);
                    let approval = self
                        .step(
                            "authorize provider access",
                            self.store
                                .authorize_device_provider_access(request, access_administrator),
                        )
                        .await?;
                    self.publish(DeviceJoinAction::TransferProviderAdmissionApproval(
                        approval,
                    ))
                    .await?;
                }
                Some(DeviceJoinStatus::AwaitingRegistrationRequest { approval }) => {
                    self.publish(DeviceJoinAction::TransferProviderAdmissionApproval(
                        approval.clone(),
                    ))
                    .await?;
                    if matches!(approval.admission, DeviceProviderAdmission::SamePrincipal) {
                        let request = DeviceRegistrationRequest::same_principal(approval)
                            .map_err(DeviceJoinError::from)?;
                        on_progress(AdmittingDeviceJoinProgress::RegisteringDevice);
                        let join = self
                            .step(
                                "activate same-provider device",
                                self.store.resume_same_principal_device_join(request),
                            )
                            .await?;
                        self.publish(DeviceJoinAction::TransferSamePrincipalJoin(join.clone()))
                            .await?;
                        return Ok(DeviceJoinDriveOutcome::Activated(join.activation));
                    }
                    on_progress(AdmittingDeviceJoinProgress::WaitingForRegistrationRequest);
                    let request = self
                        .await_artifact::<DeviceRegistrationRequest>(timing)
                        .await?;
                    on_progress(AdmittingDeviceJoinProgress::RegisteringDevice);
                    self.accept_registration(request).await?;
                }
                Some(DeviceJoinStatus::AwaitingBootstrap { request }) => {
                    on_progress(AdmittingDeviceJoinProgress::RegisteringDevice);
                    if matches!(request, DeviceRegistrationRequest::SamePrincipal { .. }) {
                        let join = self
                            .step(
                                "activate same-provider device",
                                self.store.resume_same_principal_device_join(request),
                            )
                            .await?;
                        self.publish(DeviceJoinAction::TransferSamePrincipalJoin(join.clone()))
                            .await?;
                        return Ok(DeviceJoinDriveOutcome::Activated(join.activation));
                    }
                    self.accept_registration(request).await?;
                }
                Some(DeviceJoinStatus::SamePrincipalActivationCreatePending { request }) => {
                    on_progress(AdmittingDeviceJoinProgress::RegisteringDevice);
                    let join = self
                        .step(
                            "activate same-provider device",
                            self.store.resume_same_principal_device_join(request),
                        )
                        .await?;
                    self.publish(DeviceJoinAction::TransferSamePrincipalJoin(join.clone()))
                        .await?;
                    return Ok(DeviceJoinDriveOutcome::Activated(join.activation));
                }
                Some(DeviceJoinStatus::AwaitingChallengePublication { bootstrap }) => {
                    on_progress(AdmittingDeviceJoinProgress::PreparingLibrary);
                    let ready = self
                        .step(
                            "publish provider challenge",
                            self.store.publish_device_provider_challenge(bootstrap),
                        )
                        .await?;
                    self.publish(DeviceJoinAction::TransferProviderReadyBootstrap(ready))
                        .await?;
                }
                Some(DeviceJoinStatus::AwaitingReadiness { bootstrap }) => {
                    self.publish(DeviceJoinAction::TransferProviderReadyBootstrap(
                        bootstrap.clone(),
                    ))
                    .await?;
                    if matches!(
                        bootstrap.challenge_publication,
                        DeviceProviderChallengePublication::SamePrincipal
                    ) {
                        self.step(
                            "complete same-provider admission",
                            self.store
                                .complete_same_principal_device_admission(bootstrap),
                        )
                        .await?;
                        continue;
                    }
                    on_progress(AdmittingDeviceJoinProgress::WaitingForJoiningDevice);
                    let readiness = self.await_artifact::<DeviceJoinReadiness>(timing).await?;
                    on_progress(AdmittingDeviceJoinProgress::ActivatingDevice);
                    self.step(
                        "complete provider admission",
                        self.store.complete_device_provider_admission(readiness),
                    )
                    .await?;
                }
                Some(DeviceJoinStatus::AwaitingProviderCompletion { readiness }) => {
                    on_progress(AdmittingDeviceJoinProgress::ActivatingDevice);
                    self.step(
                        "complete provider admission",
                        self.store.complete_device_provider_admission(readiness),
                    )
                    .await?;
                }
                Some(DeviceJoinStatus::AwaitingActivation { completion }) => {
                    on_progress(AdmittingDeviceJoinProgress::ActivatingDevice);
                    let activation = self
                        .step(
                            "publish activation",
                            self.store.finalize_device_join(completion),
                        )
                        .await?;
                    self.publish(DeviceJoinAction::TransferActivation(activation.clone()))
                        .await?;
                    return Ok(DeviceJoinDriveOutcome::Activated(activation));
                }
                status => {
                    return Err(DeviceJoinError::Store(format!(
                        "device join {} has no admitting step from {status:?}",
                        self.attempt_id
                    ))
                    .into());
                }
            }
        }
    }

    async fn accept_registration(
        &self,
        request: DeviceRegistrationRequest,
    ) -> Result<(), DeviceJoinTransportError> {
        self.step(
            "accept registration",
            self.store.accept_device_registration_request(request),
        )
        .await?;
        Ok(())
    }

    /// Admit a device that uses this Store's provider account through one
    /// authorized writer. Each protocol transition is still journaled before
    /// the next begins, so a failure resumes through `drive_once`; keeping the
    /// writer open avoids reconstructing and re-verifying the same Store
    /// authority between consecutive transitions.
    async fn activate_same_principal(
        &self,
        request: DeviceProviderAccessRequest,
        access_administrator: Option<&dyn DeviceProviderAccessAdministrator>,
    ) -> Result<SamePrincipalDeviceJoin, DeviceJoinTransportError> {
        // Sixty-eight seconds hid behind this one step in a live run. It is
        // three provider-facing pieces, and they report as three.
        let mut timings = coven_foundation::stage_timing::StageTimings::counting(
            "Device join same-provider activation",
            self.store.provider_requests(),
        );
        let outcome = async {
            let mut writer = timings
                .stage("authorize writer", self.store.authorize_writer())
                .await
                .map_err(DeviceJoinError::from)?;
            let approval = timings
                .stage(
                    "authorize provider access",
                    writer
                        .join_operation()
                        .authorize_access(request, access_administrator),
                )
                .await?;
            let registration = DeviceRegistrationRequest::same_principal(approval)
                .map_err(DeviceJoinError::from)?;
            timings
                .stage(
                    "activate the join",
                    writer
                        .join_operation()
                        .activate_same_principal_join(registration),
                )
                .await
                .map_err(DeviceJoinTransportError::from)
        }
        .await;
        timings.report();
        outcome
    }

    async fn approve_access_request(
        &self,
        request: &DeviceProviderAccessRequest,
        policy: &DeviceJoinApprovalPolicy<'_>,
    ) -> Result<(), DeviceJoinTransportError> {
        let offer = &self.bundle.offer;
        let approval = match policy {
            DeviceJoinApprovalPolicy::AutoApproveSelfIssued => {
                if self.self_issued().await? && request.offer.as_ref() == offer {
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
    async fn self_issued(&self) -> Result<bool, DeviceJoinTransportError> {
        Ok(self.owner_status().await?.is_some())
    }
}

/// How many times a driver re-derives after losing an activation slot, and how
/// long it waits before each retry.
///
/// A device holding the join also runs its sync loop, so the two publish Store
/// operations against the same positions. Losing that race persists nothing, so
/// the answer is to re-derive and go again — but only so many times: a store
/// that keeps refusing is not a race, and has to surface.
const ACTIVATION_CONFLICT_RETRIES: usize = 8;
const ACTIVATION_CONFLICT_BACKOFF: Duration = Duration::from_millis(25);

/// Whether this failure is another writer having taken the activation slot
/// first — which persisted nothing, so the operation can simply be re-derived.
fn is_activation_conflict(error: &DeviceJoinTransportError) -> bool {
    matches!(
        error,
        DeviceJoinTransportError::DeviceJoin(DeviceJoinError::Outbound(
            crate::sync::store::StoreError::ActivationConflict
        ))
    )
}

/// Run a driver pass, re-entering it when it loses an activation slot.
///
/// Every pass starts from the role journals, so a re-entry resumes rather than
/// repeating: the phases already settled are skipped and the one that lost the
/// race is re-derived against whatever the winner just committed. The backoff
/// grows so a busy store is not hammered, and the last failure propagates
/// unchanged once the budget is spent — this retries a lost race, it does not
/// paper over a wedged store.
async fn retrying_activation_conflicts<Pass, Fut, T>(
    mut pass: Pass,
) -> Result<T, DeviceJoinTransportError>
where
    Pass: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, DeviceJoinTransportError>>,
{
    // Each pass is boxed: a driver pass composes many large generators, and
    // holding one inline here would add its whole frame to this loop's own.
    for attempt in 0..ACTIVATION_CONFLICT_RETRIES {
        match Box::pin(pass()).await {
            Err(error) if is_activation_conflict(&error) => {
                tokio::time::sleep(ACTIVATION_CONFLICT_BACKOFF * (attempt as u32 + 1)).await;
            }
            settled => return settled,
        }
    }
    Box::pin(pass()).await
}

impl From<coven_database::DeviceJoinJournalError> for DeviceJoinTransportError {
    fn from(error: coven_database::DeviceJoinJournalError) -> Self {
        DeviceJoinTransportError::from(super::DeviceJoinError::from(error))
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
