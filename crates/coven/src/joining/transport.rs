//! The joining device's side of the storage-mediated device-join transport.
//!
//! The admitting side's driver lives beside the Store it advances; this is its
//! counterpart on the device being admitted, where the join steps hang off
//! [`DeviceJoinClient`] rather than an open Store.
//!
//! Each step is the same call a host driving the join by hand would make. The
//! transport only replaces handing the artifacts across: publish what the step
//! produced, wait for what the next step needs.

use tokio::sync::watch;

use crate::joining::{BootstrapError, DeviceJoinClient};
use crate::sync::store::{
    DeviceJoinAbandonment, DeviceJoinAction, DeviceJoinActivation, DeviceJoinCancellation,
    DeviceJoinCleanupActivation, DeviceJoinOfferBundle, DeviceJoinRoles, DeviceJoinStatus,
    DeviceJoinStep, DeviceJoinTransport, DeviceJoinTransportTiming,
    DeviceProviderAdmissionApproval, ProviderReadyDeviceBootstrap,
};
use coven_foundation::config::Config;

/// Everything a joining device needs, in the one blob a host renders as a join
/// code.
///
/// Two things come from different places and neither is derivable from the
/// other: the invite code carries the cloud home's credentials and the store
/// key wrapped to this member, and the bundle carries the attempt's slots and
/// seal key. A device that scanned only one of them cannot join.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeviceJoinInvite {
    pub version: u32,
    pub invite_code: String,
    pub bundle: DeviceJoinOfferBundle,
}

impl DeviceJoinInvite {
    pub(crate) fn new(invite_code: String, bundle: DeviceJoinOfferBundle) -> Self {
        Self {
            version: coven_protocol::store_commit::STORE_PROTOCOL_VERSION,
            invite_code,
            bundle,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("device join invite serialization cannot fail")
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, BootstrapError> {
        let invite: Self = serde_json::from_slice(bytes)
            .map_err(|error| BootstrapError::InvalidCode(error.to_string()))?;
        if invite.version != coven_protocol::store_commit::STORE_PROTOCOL_VERSION {
            return Err(BootstrapError::InvalidCode(format!(
                "device join invite version {} is not supported",
                invite.version
            )));
        }
        Ok(invite)
    }
}

/// Build the joining device's client from a scanned payload.
///
/// The arguments after the payload are this device's own — the join request it
/// generated, and the same custody, schema, and provider wiring
/// [`DeviceJoinClient::new`] takes, since that is what this constructs.
#[allow(clippy::too_many_arguments)]
fn scanned_invite_client(
    invite: &DeviceJoinInvite,
    join_request_code: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<crate::Migration>,
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
    key_custody: coven_keys::custody::KeyCustody,
    identity_custody: coven_keys::identity_custody::IdentityCustody,
    oauth_clients: coven_storage::oauth::OAuthClients,
    oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
    cloudkit_ops: Option<std::sync::Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    clock: coven_foundation::clock::ClockRef,
) -> Result<DeviceJoinClient, BootstrapError> {
    DeviceJoinClient::new(
        &invite.invite_code,
        join_request_code,
        layout,
        synced_tables,
        migrations,
        custom_s3_exact_slots,
        key_custody,
        identity_custody,
        oauth_clients,
        oauth_tokens,
        cloudkit_ops,
        clock,
    )
}

/// Join a store from a scanned invite: one call from the payload the owner's
/// device displayed to this device's saved [`crate::Config`], or to the owner's
/// abandonment of the attempt.
#[allow(clippy::too_many_arguments)]
pub async fn join_with_scanned_invite(
    invite: &[u8],
    join_request_code: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<crate::Migration>,
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
    key_custody: coven_keys::custody::KeyCustody,
    identity_custody: coven_keys::identity_custody::IdentityCustody,
    oauth_clients: coven_storage::oauth::OAuthClients,
    oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
    cloudkit_ops: Option<std::sync::Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    clock: coven_foundation::clock::ClockRef,
    timing: DeviceJoinTransportTiming,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
    let invite = DeviceJoinInvite::from_bytes(invite)?;
    scanned_invite_client(
        &invite,
        join_request_code,
        layout,
        synced_tables,
        migrations,
        custom_s3_exact_slots,
        key_custody,
        identity_custody,
        oauth_clients,
        oauth_tokens,
        cloudkit_ops,
        clock,
    )?
    .join_via_transport(&invite.bundle, timing, on_status, cancel)
    .await
}

/// Close this device's side of a join the owner cancelled, and discard the
/// pending join state the attempt left behind.
#[allow(clippy::too_many_arguments)]
pub async fn close_scanned_invite_join(
    invite: &[u8],
    join_request_code: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<crate::Migration>,
    custom_s3_exact_slots: Option<crate::CustomS3ExactSlots>,
    key_custody: coven_keys::custody::KeyCustody,
    identity_custody: coven_keys::identity_custody::IdentityCustody,
    oauth_clients: coven_storage::oauth::OAuthClients,
    oauth_tokens: Option<coven_storage::oauth::OAuthTokens>,
    cloudkit_ops: Option<std::sync::Arc<dyn coven_storage::cloud::cloudkit::CloudKitOps>>,
    clock: coven_foundation::clock::ClockRef,
    timing: DeviceJoinTransportTiming,
) -> Result<(), BootstrapError> {
    let invite = DeviceJoinInvite::from_bytes(invite)?;
    scanned_invite_client(
        &invite,
        join_request_code,
        layout,
        synced_tables,
        migrations,
        custom_s3_exact_slots,
        key_custody,
        identity_custody,
        oauth_clients,
        oauth_tokens,
        cloudkit_ops,
        clock,
    )?
    .close_device_join_via_transport(&invite.bundle, timing)
    .await
}

/// Test-only: the joining device's client over an injected cloud home, the way
/// [`CovenHandle::connect_sync_with_test_home`](crate::CovenHandle) injects one
/// for the admitting side. The provider knobs a real device reads from its
/// invite code are fixed here, since the home is supplied outright — including
/// the exact-slot capability, which the injected home has by construction.
#[cfg(test)]
fn scanned_invite_test_client(
    invite: &DeviceJoinInvite,
    join_request_code: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<crate::Migration>,
    clock: coven_foundation::clock::ClockRef,
    home: std::sync::Arc<dyn coven_storage::cloud::CloudHome>,
) -> Result<DeviceJoinClient, BootstrapError> {
    Ok(scanned_invite_client(
        invite,
        join_request_code,
        layout,
        synced_tables,
        migrations,
        Some(crate::CustomS3ExactSlots::StandardConditionalRequests),
        coven_keys::custody::KeyCustody::Keyring,
        coven_keys::identity_custody::IdentityCustody::Keyring,
        coven_storage::oauth::OAuthClients::empty(),
        None,
        None,
        clock,
    )?
    .with_test_bootstrap_home(home))
}

/// Test-only counterpart of [`join_with_scanned_invite`].
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn join_with_scanned_invite_over_test_home(
    invite: &[u8],
    join_request_code: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<crate::Migration>,
    clock: coven_foundation::clock::ClockRef,
    home: std::sync::Arc<dyn coven_storage::cloud::CloudHome>,
    timing: DeviceJoinTransportTiming,
    on_status: impl Fn(&str),
    cancel: &watch::Receiver<bool>,
) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
    let invite = DeviceJoinInvite::from_bytes(invite)?;
    scanned_invite_test_client(
        &invite,
        join_request_code,
        layout,
        synced_tables,
        migrations,
        clock,
        home,
    )?
    .join_via_transport(&invite.bundle, timing, on_status, cancel)
    .await
}

/// Test-only counterpart of [`close_scanned_invite_join`].
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn close_scanned_invite_join_over_test_home(
    invite: &[u8],
    join_request_code: &str,
    layout: coven_foundation::store_dir::StoreLayout,
    synced_tables: Vec<coven_protocol::synced_schema::SyncedTable>,
    migrations: Vec<crate::Migration>,
    clock: coven_foundation::clock::ClockRef,
    home: std::sync::Arc<dyn coven_storage::cloud::CloudHome>,
    timing: DeviceJoinTransportTiming,
) -> Result<(), BootstrapError> {
    let invite = DeviceJoinInvite::from_bytes(invite)?;
    scanned_invite_test_client(
        &invite,
        join_request_code,
        layout,
        synced_tables,
        migrations,
        clock,
        home,
    )?
    .close_device_join_via_transport(&invite.bundle, timing)
    .await
}

/// How a join driven through the transport ended for the joining device.
#[derive(Clone, Debug)]
pub enum DeviceJoinTransportOutcome {
    /// The device is a member: its store is saved and its config returned.
    Joined(Config),
    /// The owner gave up on the attempt before it completed.
    Abandoned(DeviceJoinAbandonment),
}

impl DeviceJoinClient {
    /// Join through the transport: one call from the scanned offer bundle to a
    /// saved member [`Config`], or to the owner's abandonment of the attempt.
    ///
    /// Every step resumes from the joiner journal, so calling this again after
    /// a crash picks up where the last durable step left off — a republished
    /// artifact that is already at its slot is accepted as the same transfer,
    /// and an awaited artifact is simply read again.
    pub async fn join_via_transport(
        &self,
        bundle: &DeviceJoinOfferBundle,
        timing: DeviceJoinTransportTiming,
        on_status: impl Fn(&str),
        cancel: &watch::Receiver<bool>,
    ) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
        let storage = self.transport_storage().await?;
        let transport = DeviceJoinTransport::open(&storage, bundle, DeviceJoinRoles::joiner())?;
        let attempt_id = bundle.offer.attempt_id;

        // Each pass takes the joiner journal's durable state and performs the
        // one step that follows it — never an earlier step, which the journal
        // refuses once it is past. A step that produced an artifact but died
        // before publishing it republishes here; a step whose artifact is
        // already at its slot publishes the same transfer again for nothing.
        loop {
            match self.device_join_status(attempt_id)? {
                None | Some(DeviceJoinStatus::AwaitingAccessRequest { .. }) => {
                    let request = self
                        .prepare_provider_access_request(bundle.offer.clone())
                        .await?;
                    transport
                        .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
                        .await?;
                }
                Some(DeviceJoinStatus::Abandoned { abandonment }) => {
                    return self.accept_abandonment(&transport, abandonment).await;
                }
                Some(DeviceJoinStatus::AwaitingProviderAdmission { request }) => {
                    transport
                        .publish(&DeviceJoinAction::TransferProviderAccessRequest(request))
                        .await?;
                    // The owner may give up on the attempt while this device
                    // waits, so the wait watches the abandonment slot alongside
                    // the approval rather than sitting out its deadline.
                    let approval = match transport
                        .await_step::<DeviceProviderAdmissionApproval>(timing)
                        .await?
                    {
                        DeviceJoinStep::Continue(approval) => approval,
                        DeviceJoinStep::Abandoned(abandonment) => {
                            return self.accept_abandonment(&transport, abandonment).await;
                        }
                    };
                    let registration_request = self.prepare_registration_request(approval).await?;
                    transport
                        .publish(&DeviceJoinAction::TransferRegistrationRequest(
                            registration_request,
                        ))
                        .await?;
                }
                Some(DeviceJoinStatus::AwaitingRegistrationRequest { approval }) => {
                    let registration_request = self.prepare_registration_request(approval).await?;
                    transport
                        .publish(&DeviceJoinAction::TransferRegistrationRequest(
                            registration_request,
                        ))
                        .await?;
                }
                Some(DeviceJoinStatus::AwaitingBootstrap { request }) => {
                    transport
                        .publish(&DeviceJoinAction::TransferRegistrationRequest(request))
                        .await?;
                    let provider_ready = transport
                        .await_artifact::<ProviderReadyDeviceBootstrap>(timing)
                        .await?;
                    let readiness =
                        Box::pin(self.bootstrap_pending_device(provider_ready, &on_status, cancel))
                            .await?;
                    transport
                        .publish(&DeviceJoinAction::TransferReadiness(readiness))
                        .await?;
                }
                Some(DeviceJoinStatus::AwaitingProviderCompletion { readiness }) => {
                    transport
                        .publish(&DeviceJoinAction::TransferReadiness(readiness))
                        .await?;
                    let activation = transport
                        .await_artifact::<DeviceJoinActivation>(timing)
                        .await?;
                    return self.finish(&transport, activation, &on_status).await;
                }
                Some(DeviceJoinStatus::AwaitingCompletion { activation }) => {
                    return self.finish(&transport, activation, &on_status).await;
                }
                Some(DeviceJoinStatus::Activated { store }) => {
                    return self.finish(&transport, store.activation, &on_status).await;
                }
                Some(_) => return Err(crate::DeviceJoinError::JournalConflict.into()),
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
        on_status: &impl Fn(&str),
    ) -> Result<DeviceJoinTransportOutcome, BootstrapError> {
        let config = self.complete_device_join(activation, on_status).await?;
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

    /// Carry a cancelled attempt through the transport to its activated
    /// cleanup, then remove the attempt's transport slots.
    ///
    /// The joiner is the last reader in the unwind — it consumes the cleanup
    /// activation the owner publishes last — so the deletion belongs here, at
    /// the same point the joiner discards the rest of its pending join state.
    pub async fn close_device_join_via_transport(
        &self,
        bundle: &DeviceJoinOfferBundle,
        timing: DeviceJoinTransportTiming,
    ) -> Result<(), BootstrapError> {
        let storage = self.transport_storage().await?;
        let transport = DeviceJoinTransport::open(&storage, bundle, DeviceJoinRoles::joiner())?;
        let attempt_id = bundle.offer.attempt_id;

        match self.device_join_status(attempt_id)? {
            // The cleanup already landed; all that is left is clearing the
            // namespace, which a run that died before it would not have done.
            Some(DeviceJoinStatus::CleanupActivated { activation }) => {
                if self
                    .resume_device_joins()?
                    .contains(&DeviceJoinAction::CompleteCleanup(activation.clone()))
                {
                    self.complete_cancelled_device_join(activation).await?;
                }
                transport.delete_attempt_slots().await?;
                return Ok(());
            }
            Some(DeviceJoinStatus::JoinerClosed { terminal }) => {
                transport
                    .publish(&DeviceJoinAction::TransferJoinerTerminal(terminal))
                    .await?;
            }
            _ => {
                let cancellation = transport
                    .await_artifact::<DeviceJoinCancellation>(timing)
                    .await?;
                let terminal = self.close_pending_device_join(cancellation).await?;
                transport
                    .publish(&DeviceJoinAction::TransferJoinerTerminal(terminal))
                    .await?;
            }
        }

        let activation = transport
            .await_artifact::<DeviceJoinCleanupActivation>(timing)
            .await?;
        self.complete_cancelled_device_join(activation).await?;
        transport.delete_attempt_slots().await?;
        Ok(())
    }
}
