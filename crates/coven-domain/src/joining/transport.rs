//! The joining device's side of the storage-mediated device-join transport.
//!
//! The admitting side's driver lives beside the Store it advances; this is its
//! counterpart on the device being admitted, where the join steps hang off
//! [`DeviceJoinClient`] rather than an open Store.
//!
//! Each step is the same call a host driving the join by hand would make. The
//! transport only replaces handing the artifacts across: publish what the step
//! produced, wait for what the next step needs.

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use tokio::sync::watch;

use crate::joining::client::{
    enrollment_oauth_tokens, BootstrapError, DeviceJoinClient, EnrollmentProviderAccess,
};
use coven_foundation::config::Config;
use coven_replication::sync::store::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinOfferBundle,
    DeviceJoinRole, DeviceJoinStatus, DeviceJoinStep, DeviceJoinTransport,
    DeviceJoinTransportTiming, DeviceProviderAdmissionApproval, ProviderReadyDeviceBootstrap,
};

use coven_replication::sync::MemberAdmission;

/// Complete the joining side after scanning the existing device's one pairing
/// code. The local session returns the invitation sealed to this attempt; the
/// existing Store transport then performs registration and bootstrap.
#[allow(clippy::too_many_arguments)]
pub async fn join_with_device_pairing(
    pairing: &crate::joining::PreparedDevicePairing,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<coven_database::Migration>,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    transfer_limits: coven_protocol::blob::TransferLimits,
    key_custody: coven_keys::custody::KeyCustody,
    identity_custody: coven_keys::identity_custody::IdentityCustody,
    oauth_clients: coven_storage::oauth::OAuthClients,
    oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
    cloudkit_ops: Option<std::sync::Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    clock: coven_foundation::clock::ClockRef,
    on_progress: coven_replication::sync::JoiningDeviceJoinProgressObserver,
    cancel: &watch::Receiver<bool>,
) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
    let timing = DeviceJoinTransportTiming::interactive();
    let continuation = durable_invitation(
        pairing,
        &layout,
        timing,
        clock.clone(),
        &on_progress,
        cancel,
    )
    .await?;
    let (pairing, invitation, provider_access) = match continuation {
        EnrollmentContinuation::ProviderAccessPending {
            pairing,
            invitation,
        } => (
            pairing,
            invitation,
            EnrollmentProviderAccess::Supplied(oauth_tokens),
        ),
        EnrollmentContinuation::LibraryInstallationPending {
            pairing,
            invitation,
        } => (pairing, invitation, EnrollmentProviderAccess::Stored),
    };
    let invite = DeviceJoinInvite::from_bytes(&invitation)?;
    let client = invitation_client(
        &invite,
        pairing.request().public_key(),
        layout.clone(),
        synced_tables,
        migrations,
        exact_upload_verification,
        transfer_limits,
        key_custody,
        identity_custody,
        oauth_clients,
        provider_access,
        cloudkit_ops,
        clock,
    )?;
    let pairing = pairing.record_library_installation_pending(&layout)?;
    let outcome = client
        .join_via_transport(&invite.bundle, timing, on_progress, cancel)
        .await?;
    pairing.finish(&layout)?;
    Ok(outcome)
}

enum EnrollmentContinuation {
    ProviderAccessPending {
        pairing: crate::joining::PreparedDevicePairing,
        invitation: Vec<u8>,
    },
    LibraryInstallationPending {
        pairing: crate::joining::PreparedDevicePairing,
        invitation: Vec<u8>,
    },
}

async fn durable_invitation(
    pairing: &crate::joining::PreparedDevicePairing,
    layout: &coven_foundation::store_dir::StoreLayout,
    timing: DeviceJoinTransportTiming,
    clock: coven_foundation::clock::ClockRef,
    on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
    cancel: &watch::Receiver<bool>,
) -> Result<EnrollmentContinuation, BootstrapError> {
    let pairing = match pairing.phase() {
        crate::joining::DevicePairingPhase::AwaitingInvitation => {
            on_progress(coven_replication::sync::JoiningDeviceJoinProgress::WaitingForApproval);
            let invitation = crate::joining::receive_device_invitation(
                pairing.offer(),
                pairing.sealed_request(),
                timing,
                clock,
                cancel,
            )
            .await
            .map_err(BootstrapError::Pairing)?;
            pairing.record_invitation_received(layout, &invitation)?
        }
        crate::joining::DevicePairingPhase::ProviderAccessPending
        | crate::joining::DevicePairingPhase::LibraryInstallationPending => pairing.clone(),
    };
    let invitation = pairing
        .pending_invitation()
        .expect("a durable invitation phase carries its invitation")
        .to_vec();
    Ok(match pairing.phase() {
        crate::joining::DevicePairingPhase::ProviderAccessPending => {
            EnrollmentContinuation::ProviderAccessPending {
                pairing,
                invitation,
            }
        }
        crate::joining::DevicePairingPhase::LibraryInstallationPending => {
            EnrollmentContinuation::LibraryInstallationPending {
                pairing,
                invitation,
            }
        }
        crate::joining::DevicePairingPhase::AwaitingInvitation => {
            unreachable!("recording the invitation advances the durable phase")
        }
    })
}

/// Everything a joining device needs, sealed to the pending identity named by
/// its signed pairing request. The transport bundle is public kickoff data; the member
/// admission inside `sealed_invitation` carries provider credentials and can
/// be opened only by that joining device.
#[derive(Clone, Debug)]
pub struct DeviceJoinInvite {
    sealed_invitation: Vec<u8>,
    pub bundle: DeviceJoinOfferBundle,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct DeviceJoinInviteWire {
    version: u32,
    sealed_invitation: String,
    bundle: DeviceJoinOfferBundle,
}

impl DeviceJoinInvite {
    pub fn new(
        admission: MemberAdmission,
        bundle: DeviceJoinOfferBundle,
    ) -> Result<Self, DeviceInviteError> {
        validate_admission(&admission)?;
        require_admission_matches_bundle(&admission, &bundle)?;
        let recipient =
            coven_keys::keys::ed25519_hex_to_x25519_public_key(&bundle.offer.member_pubkey)?;
        let plaintext =
            serde_json::to_vec(&admission).expect("member admission serialization cannot fail");
        Ok(Self {
            sealed_invitation: coven_keys::keys::seal_box_encrypt(&plaintext, &recipient),
            bundle,
        })
    }

    pub(crate) fn open_admission(
        &self,
        member_pubkey: &str,
    ) -> Result<MemberAdmission, DeviceInviteError> {
        if member_pubkey != self.bundle.offer.member_pubkey {
            return Err(DeviceInviteError::RecipientMismatch);
        }
        let recipient = coven_keys::keys::peek_pending_identity(member_pubkey)?;
        let plaintext = coven_keys::keys::seal_box_decrypt(
            &self.sealed_invitation,
            &recipient.to_x25519_secret_key(),
        )?;
        let admission: MemberAdmission =
            serde_json::from_slice(&plaintext).map_err(DeviceInviteError::AdmissionJson)?;
        validate_admission(&admission)?;
        require_admission_matches_bundle(&admission, &self.bundle)?;
        Ok(admission)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&DeviceJoinInviteWire {
            version: coven_protocol::store_commit::STORE_PROTOCOL_VERSION,
            sealed_invitation: URL_SAFE_NO_PAD.encode(&self.sealed_invitation),
            bundle: self.bundle.clone(),
        })
        .expect("device join invite serialization cannot fail")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BootstrapError> {
        let wire: DeviceJoinInviteWire =
            serde_json::from_slice(bytes).map_err(DeviceInviteError::WireJson)?;
        if wire.version != coven_protocol::store_commit::STORE_PROTOCOL_VERSION {
            return Err(BootstrapError::UnsupportedDeviceInviteVersion(wire.version));
        }
        Ok(Self {
            sealed_invitation: URL_SAFE_NO_PAD
                .decode(wire.sealed_invitation)
                .map_err(DeviceInviteError::Ciphertext)?,
            bundle: wire.bundle,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DeviceInviteError {
    #[error("device invitation wire is not valid JSON: {0}")]
    WireJson(#[source] serde_json::Error),
    #[error("device invitation ciphertext is not valid base64: {0}")]
    Ciphertext(#[source] base64::DecodeError),
    #[error("device invitation admission payload is not valid JSON: {0}")]
    AdmissionJson(#[source] serde_json::Error),
    #[error("device invitation key: {0}")]
    Key(#[from] coven_keys::keys::KeyError),
    #[error("device invitation is for a different pairing identity")]
    RecipientMismatch,
    #[error("device invitation does not match its signed join offer")]
    OfferMismatch,
    #[error("device invitation admission payload is invalid: {0}")]
    Admission(#[from] AdmissionPayloadError),
}

#[derive(Debug, thiserror::Error)]
pub enum AdmissionPayloadError {
    #[error("store id: {0}")]
    StoreId(#[from] coven_foundation::store_dir::PathTokenError),
    #[error("owner public key: {0}")]
    OwnerPublicKey(#[source] coven_foundation::code_envelope::FixedHexError),
    #[error("wrapped-key material: {0}")]
    WrappedKeyMaterial(#[source] coven_foundation::code_envelope::FixedHexError),
    #[error("wrapped-key identity: {0}")]
    WrappedKeyIdentity(#[source] coven_protocol::objects::StorageError),
    #[error("membership floor is empty")]
    EmptyMembershipFloor,
    #[error("membership floor: {0}")]
    MembershipFloor(#[source] coven_protocol::membership::MembershipFloorError),
}

fn validate_admission(admission: &MemberAdmission) -> Result<(), AdmissionPayloadError> {
    coven_foundation::store_dir::validate_path_token(&admission.store_id)?;
    coven_foundation::code_envelope::decode_fixed_hex(
        "owner public key",
        &admission.owner_pubkey,
        32,
    )
    .map_err(AdmissionPayloadError::OwnerPublicKey)?;
    for (subject, value) in [
        (
            "wrapped-key author public key",
            &admission.wrapped_key.owner_pubkey,
        ),
        (
            "wrapped-key recipient public key",
            &admission.wrapped_key.recipient_pubkey,
        ),
    ] {
        coven_foundation::code_envelope::decode_fixed_hex(subject, value, 32)
            .map_err(AdmissionPayloadError::WrappedKeyMaterial)?;
    }
    admission
        .wrapped_key
        .validate_identity()
        .map_err(AdmissionPayloadError::WrappedKeyIdentity)?;
    if admission.membership_floor.0.is_empty() {
        return Err(AdmissionPayloadError::EmptyMembershipFloor);
    }
    admission
        .membership_floor
        .validate()
        .map_err(AdmissionPayloadError::MembershipFloor)
}

fn require_admission_matches_bundle(
    admission: &MemberAdmission,
    bundle: &DeviceJoinOfferBundle,
) -> Result<(), DeviceInviteError> {
    if admission.wrapped_key.recipient_pubkey != bundle.offer.member_pubkey
        || admission.store_root != bundle.offer.store_root
    {
        return Err(DeviceInviteError::OfferMismatch);
    }
    Ok(())
}

/// Build the joining device's client from a scanned payload.
///
/// The arguments after the payload are this device's own — its pending public
/// key, and the same custody, schema, and provider wiring
/// [`DeviceJoinClient::new`] takes, since that is what this constructs.
#[allow(clippy::too_many_arguments)]
fn invitation_client(
    invite: &DeviceJoinInvite,
    member_pubkey: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<coven_database::Migration>,
    exact_upload_verification: coven_foundation::config::ExactUploadVerification,
    transfer_limits: coven_protocol::blob::TransferLimits,
    key_custody: coven_keys::custody::KeyCustody,
    identity_custody: coven_keys::identity_custody::IdentityCustody,
    oauth_clients: coven_storage::oauth::OAuthClients,
    provider_access: EnrollmentProviderAccess,
    cloudkit_ops: Option<std::sync::Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    clock: coven_foundation::clock::ClockRef,
) -> Result<DeviceJoinClient, BootstrapError> {
    let admission = invite.open_admission(member_pubkey)?;
    let store_keys = coven_keys::keys::StoreKeys::bind(admission.store_id.clone());
    let oauth_tokens = enrollment_oauth_tokens(&admission.join_info, &store_keys, provider_access)?;
    DeviceJoinClient::new(
        admission,
        member_pubkey.to_string(),
        layout,
        synced_tables,
        migrations,
        exact_upload_verification,
        transfer_limits,
        key_custody,
        identity_custody,
        oauth_clients,
        oauth_tokens,
        cloudkit_ops,
        clock,
    )
}

/// Test-only: the joining device's client over an injected cloud home, the way
/// the host's own test entry point injects one for the admitting side. The
/// provider knobs a real device reads from its invitation are fixed here,
/// since the home is supplied outright — including the exact-slot capability,
/// which the injected home has by construction.
#[cfg(any(test, feature = "test-utils"))]
fn invitation_test_client(
    invite: &DeviceJoinInvite,
    member_pubkey: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<coven_database::Migration>,
    clock: coven_foundation::clock::ClockRef,
    home: std::sync::Arc<dyn coven_storage::cloud::ExactCloudHome>,
) -> Result<DeviceJoinClient, BootstrapError> {
    Ok(invitation_client(
        invite,
        member_pubkey,
        layout,
        synced_tables,
        migrations,
        coven_foundation::config::ExactUploadVerification::MetadataHash,
        coven_protocol::blob::TransferLimits::one_at_a_time(),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        coven_storage::oauth::OAuthClients::empty(),
        EnrollmentProviderAccess::InjectedHome,
        None,
        clock,
    )?
    .with_test_bootstrap_home(home))
}

/// Test-only counterpart of [`join_with_device_pairing`].
#[cfg(any(test, feature = "test-utils"))]
#[allow(clippy::too_many_arguments)]
pub async fn join_with_device_pairing_over_test_home(
    pairing: &crate::joining::PreparedDevicePairing,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<coven_database::Migration>,
    clock: coven_foundation::clock::ClockRef,
    home: std::sync::Arc<dyn coven_storage::cloud::ExactCloudHome>,
    timing: DeviceJoinTransportTiming,
    on_progress: coven_replication::sync::JoiningDeviceJoinProgressObserver,
    cancel: &watch::Receiver<bool>,
) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
    let continuation = durable_invitation(
        pairing,
        &layout,
        timing,
        clock.clone(),
        &on_progress,
        cancel,
    )
    .await?;
    let (pairing, invitation) = match continuation {
        EnrollmentContinuation::ProviderAccessPending {
            pairing,
            invitation,
        }
        | EnrollmentContinuation::LibraryInstallationPending {
            pairing,
            invitation,
        } => (pairing, invitation),
    };
    let invite = DeviceJoinInvite::from_bytes(&invitation)?;
    let client = invitation_test_client(
        &invite,
        pairing.request().public_key(),
        layout.clone(),
        synced_tables,
        migrations,
        clock,
        home,
    )?;
    let pairing = pairing.record_library_installation_pending(&layout)?;
    let outcome = client
        .join_via_transport(&invite.bundle, timing, on_progress, cancel)
        .await?;
    pairing.finish(&layout)?;
    Ok(outcome)
}

/// How a join driven through the transport ended for the joining device.
#[derive(Clone, Debug)]
pub enum DeviceJoinTransportOutcome {
    /// The device is a member: its store is saved and its config returned.
    Joined(Config),
    /// The owner gave up on the attempt before it completed.
    Abandoned(DeviceJoinAbandonment),
}

async fn publish_once(
    transport: &DeviceJoinTransport<'_>,
    published: &mut Vec<DeviceJoinAction>,
    action: DeviceJoinAction,
) -> Result<(), coven_replication::sync::DeviceJoinTransportError> {
    if published.contains(&action) {
        return Ok(());
    }
    transport.publish(&action).await?;
    published.push(action);
    Ok(())
}

impl DeviceJoinClient {
    /// Join through the transport: one call from the scanned offer bundle to a
    /// saved member [`Config`], or to the owner's abandonment of the attempt.
    ///
    /// Every step resumes from the joiner journal, so calling this again after
    /// a crash picks up where the last durable step left off — a republished
    /// artifact that is already at its slot is accepted as the same transfer,
    /// and an awaited artifact is simply read again.
    ///
    /// The admitting side can give up until it approves the join, and every
    /// wait below watches for that. After it approves there is nothing to watch
    /// for: the approval is what grants this device storage access, and taking
    /// it back is member removal and a key rotation, not a message.
    pub(crate) async fn join_via_transport(
        &self,
        bundle: &DeviceJoinOfferBundle,
        timing: DeviceJoinTransportTiming,
        on_progress: coven_replication::sync::JoiningDeviceJoinProgressObserver,
        cancel: &watch::Receiver<bool>,
    ) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
        let storage = self.transport_storage().await?;
        let transport = DeviceJoinTransport::open(&storage, bundle, DeviceJoinRole::Joiner)?;
        self.drive_join_via_transport(&transport, bundle, timing, &on_progress, cancel)
            .await
    }

    async fn drive_join_via_transport(
        &self,
        transport: &DeviceJoinTransport<'_>,
        bundle: &DeviceJoinOfferBundle,
        timing: DeviceJoinTransportTiming,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
        cancel: &watch::Receiver<bool>,
    ) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
        let attempt_id = bundle.offer.attempt_id;
        let mut published = Vec::new();

        // Each pass takes the joiner journal's durable state and performs the
        // one step that follows it — never an earlier step, which the journal
        // refuses once it is past. A step that produced an artifact but died
        // before publishing it republishes here; a step whose artifact is
        // already at its slot publishes the same transfer again for nothing.
        loop {
            match self.device_join_status(attempt_id)? {
                None | Some(DeviceJoinStatus::AwaitingAccessRequest { .. }) => {
                    on_progress(
                        coven_replication::sync::JoiningDeviceJoinProgress::RequestingProviderAccess,
                    );
                    let request = self
                        .prepare_provider_access_request(bundle.offer.clone())
                        .await?;
                    publish_once(
                        transport,
                        &mut published,
                        DeviceJoinAction::TransferProviderAccessRequest(request),
                    )
                    .await?;
                }
                Some(DeviceJoinStatus::Abandoned { abandonment }) => {
                    return self.accept_abandonment(transport, abandonment).await;
                }
                Some(DeviceJoinStatus::AwaitingProviderAdmission { request }) => {
                    let same_principal =
                        request.offer.provider_admin.provider == request.peer_provider;
                    publish_once(
                        transport,
                        &mut published,
                        DeviceJoinAction::TransferProviderAccessRequest(request),
                    )
                    .await?;
                    if same_principal {
                        on_progress(
                            coven_replication::sync::JoiningDeviceJoinProgress::WaitingForLibrary,
                        );
                        let join = match transport
                            .await_step::<coven_replication::sync::SamePrincipalDeviceJoin>(timing)
                            .await?
                        {
                            DeviceJoinStep::Continue(join) => join,
                            DeviceJoinStep::Abandoned(abandonment) => {
                                return self.accept_abandonment(transport, abandonment).await;
                            }
                        };
                        self.record_same_principal_registration_request(
                            join.bootstrap.bootstrap.request.approval().clone(),
                        )?;
                        return self
                            .finish_same_principal(transport, join, on_progress, cancel)
                            .await;
                    } else {
                        on_progress(
                            coven_replication::sync::JoiningDeviceJoinProgress::WaitingForProviderAccess,
                        );
                        // The owner may give up on the attempt while this device
                        // waits, so the wait watches the abandonment slot alongside
                        // the approval rather than sitting out its deadline.
                        let approval = match transport
                            .await_step::<DeviceProviderAdmissionApproval>(timing)
                            .await?
                        {
                            DeviceJoinStep::Continue(approval) => approval,
                            DeviceJoinStep::Abandoned(abandonment) => {
                                return self.accept_abandonment(transport, abandonment).await;
                            }
                        };
                        on_progress(
                            coven_replication::sync::JoiningDeviceJoinProgress::RegisteringDevice,
                        );
                        let registration_request =
                            self.prepare_registration_request(approval).await?;
                        publish_once(
                            transport,
                            &mut published,
                            DeviceJoinAction::TransferRegistrationRequest(registration_request),
                        )
                        .await?;
                    }
                }
                Some(DeviceJoinStatus::AwaitingRegistrationRequest { approval }) => {
                    on_progress(
                        coven_replication::sync::JoiningDeviceJoinProgress::RegisteringDevice,
                    );
                    let registration_request = self.prepare_registration_request(approval).await?;
                    publish_once(
                        transport,
                        &mut published,
                        DeviceJoinAction::TransferRegistrationRequest(registration_request),
                    )
                    .await?;
                }
                Some(DeviceJoinStatus::AwaitingBootstrap { request }) => {
                    let same_principal = matches!(
                        &request,
                        coven_replication::sync::DeviceRegistrationRequest::SamePrincipal { .. }
                    );
                    publish_once(
                        transport,
                        &mut published,
                        DeviceJoinAction::TransferRegistrationRequest(request),
                    )
                    .await?;
                    on_progress(
                        coven_replication::sync::JoiningDeviceJoinProgress::WaitingForLibrary,
                    );
                    if same_principal {
                        let join = match transport
                            .await_step::<coven_replication::sync::SamePrincipalDeviceJoin>(timing)
                            .await?
                        {
                            DeviceJoinStep::Continue(join) => join,
                            DeviceJoinStep::Abandoned(abandonment) => {
                                return self.accept_abandonment(transport, abandonment).await;
                            }
                        };
                        return self
                            .finish_same_principal(transport, join, on_progress, cancel)
                            .await;
                    }
                    let provider_ready = match transport
                        .await_step::<ProviderReadyDeviceBootstrap>(timing)
                        .await?
                    {
                        DeviceJoinStep::Continue(provider_ready) => provider_ready,
                        DeviceJoinStep::Abandoned(abandonment) => {
                            return self.accept_abandonment(transport, abandonment).await;
                        }
                    };
                    let readiness = Box::pin(self.bootstrap_pending_device(
                        provider_ready,
                        on_progress,
                        cancel,
                    ))
                    .await?;
                    publish_once(
                        transport,
                        &mut published,
                        DeviceJoinAction::TransferReadiness(readiness),
                    )
                    .await?;
                }
                Some(DeviceJoinStatus::AwaitingProviderCompletion { readiness }) => {
                    if !matches!(
                        &readiness.provider,
                        coven_replication::sync::DeviceProviderReadiness::SamePrincipal
                    ) {
                        publish_once(
                            transport,
                            &mut published,
                            DeviceJoinAction::TransferReadiness(readiness),
                        )
                        .await?;
                    }
                    on_progress(
                        coven_replication::sync::JoiningDeviceJoinProgress::WaitingForActivation,
                    );
                    let activation = transport
                        .await_artifact::<DeviceJoinActivation>(timing)
                        .await?;
                    return self.finish(transport, activation, on_progress).await;
                }
                Some(DeviceJoinStatus::AwaitingCompletion { activation }) => {
                    return self.finish(transport, activation, on_progress).await;
                }
                Some(DeviceJoinStatus::Activated { store }) => {
                    return self.finish(transport, store.activation, on_progress).await;
                }
                Some(_) => {
                    return Err(coven_replication::sync::DeviceJoinError::JournalConflict.into())
                }
            }
        }
    }

    /// Save the store and clear the attempt's namespace: the join is complete,
    /// so every artifact has been consumed and neither side has anything left
    /// to read.
    async fn finish(
        &self,
        transport: &DeviceJoinTransport<'_>,
        activation: DeviceJoinActivation,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
    ) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
        let config = self.complete_device_join(activation, on_progress).await?;
        transport.delete_attempt_slots().await?;
        Ok(DeviceJoinTransportOutcome::Joined(config))
    }

    async fn finish_same_principal(
        &self,
        transport: &DeviceJoinTransport<'_>,
        join: coven_replication::sync::SamePrincipalDeviceJoin,
        on_progress: &coven_replication::sync::JoiningDeviceJoinProgressObserver,
        cancel: &watch::Receiver<bool>,
    ) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
        let config = self
            .install_same_principal_device_join(join, on_progress, cancel)
            .await?;
        transport.delete_attempt_slots().await?;
        Ok(DeviceJoinTransportOutcome::Joined(config))
    }

    /// Record the owner's abandonment and clear the attempt's namespace: the
    /// abandonment is the last artifact either side publishes, and this device
    /// has just read it.
    async fn accept_abandonment(
        &self,
        transport: &DeviceJoinTransport<'_>,
        abandonment: DeviceJoinAbandonment,
    ) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
        let accepted = self.accept_device_join_abandonment(abandonment).await?;
        transport.delete_attempt_slots().await?;
        Ok(DeviceJoinTransportOutcome::Abandoned(accepted))
    }
}
