use coven_domain::joining::{
    DevicePairingHost, DevicePairingOffer, DevicePairingRequest, DevicePairingTransportError,
};
use coven_protocol::membership::MemberRole;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use crate::store_joining::StoreJoining;
use crate::store_sync::{ConfigProvider, StoreSync};

const DEVICE_PAIRING_PORT: u16 = 24_821;
const DEVICE_PAIRING_LIFETIME: chrono::Duration = chrono::Duration::minutes(15);

#[derive(Debug, thiserror::Error)]
pub enum StartDevicePairingError {
    #[error("this Store has no cloud provider")]
    NoCloudProvider,
    #[error("no active local network interface can receive the joining device")]
    NoLocalInterface,
    #[error("local interfaces: {0}")]
    Interfaces(#[source] std::io::Error),
    #[error("pairing listener: {0}")]
    Listen(#[from] std::io::Error),
    #[error("pairing offer: {0}")]
    Pairing(#[from] coven_domain::joining::DevicePairingError),
    #[error("pairing host: {0}")]
    Host(#[from] DevicePairingTransportError),
}

#[derive(Debug, thiserror::Error)]
pub enum ApproveDevicePairingError {
    #[error("device pairing was cancelled")]
    Cancelled,
    #[error("pairing transport: {0}")]
    Pairing(#[from] DevicePairingTransportError),
    #[error("device invitation: {0}")]
    Invitation(#[from] crate::DeviceAdmissionError),
    #[error("persisted device invitation: {0}")]
    PersistedInvitation(#[from] coven_domain::joining::BootstrapError),
    #[error("device join: {0}")]
    Join(#[from] crate::SyncError),
    #[error("the invitation was created for another pairing request")]
    RequestMismatch,
}

#[derive(Clone)]
pub(crate) struct StoreDevicePairing {
    config_provider: ConfigProvider,
    journal_path: std::path::PathBuf,
    clock: coven_foundation::clock::ClockRef,
    joining: StoreJoining,
    sync: StoreSync,
}

impl StoreDevicePairing {
    pub(crate) fn new(
        config_provider: ConfigProvider,
        journal_path: std::path::PathBuf,
        clock: coven_foundation::clock::ClockRef,
        joining: StoreJoining,
        sync: StoreSync,
    ) -> Self {
        Self {
            config_provider,
            journal_path,
            clock,
            joining,
            sync,
        }
    }

    /// Start the one pairing session this process can present on the LAN. The
    /// returned code is the only value the joining device scans.
    pub(crate) async fn start(&self) -> Result<DevicePairingHost, StartDevicePairingError> {
        let config = (self.config_provider)();
        let cloud_provider = config
            .cloud_home
            .provider
            .clone()
            .ok_or(StartDevicePairingError::NoCloudProvider)?;
        let endpoints = local_pairing_endpoints(DEVICE_PAIRING_PORT)?;
        let bind_address = if endpoints[0].is_ipv4() {
            SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), DEVICE_PAIRING_PORT)
        } else {
            SocketAddr::new(IpAddr::V6(Ipv6Addr::UNSPECIFIED), DEVICE_PAIRING_PORT)
        };
        let listener = tokio::net::TcpListener::bind(bind_address).await?;
        let pairing_key = coven_keys::keys::UserKeypair::generate();
        let offer = DevicePairingOffer::new(
            &pairing_key,
            endpoints,
            config.store_name,
            cloud_provider,
            (self.clock.now() + DEVICE_PAIRING_LIFETIME).timestamp(),
        )?;
        Ok(DevicePairingHost::start_or_resume(
            listener,
            offer,
            pairing_key,
            self.journal_path.clone(),
            self.clock.clone(),
        )
        .await?)
    }

    /// Admit the exact signed request the owner reviewed, return its sealed
    /// invitation over the local pairing session, and drive the Store
    /// registration protocol to its terminal outcome.
    pub(crate) async fn approve(
        &self,
        host: &DevicePairingHost,
        request: &DevicePairingRequest,
        role: MemberRole,
        policy: crate::DeviceJoinApprovalPolicy<'_>,
        access_administrator: Option<&dyn crate::DeviceProviderAccessAdministrator>,
        on_progress: &(dyn Fn(crate::AdmittingDeviceJoinProgress) + Send + Sync),
        cancel: tokio::sync::watch::Receiver<bool>,
    ) -> Result<crate::DeviceJoinDriveOutcome, ApproveDevicePairingError> {
        let timing = crate::DeviceJoinTransportTiming::interactive();
        if let Some(bytes) = host.cancellation_invitation(request)? {
            let invitation = coven_domain::joining::DeviceJoinInvite::from_bytes(&bytes)?;
            self.sync
                .abort_device_join_transport(&invitation.bundle, timing)
                .await?;
            host.finish()?;
            return Err(ApproveDevicePairingError::Cancelled);
        }
        on_progress(crate::AdmittingDeviceJoinProgress::PreparingInvitation);
        let invitation = match host.invitation(request)? {
            Some(bytes) => coven_domain::joining::DeviceJoinInvite::from_bytes(&bytes)?,
            None => self.joining.begin_invite(request, role).await?,
        };
        if invitation.bundle.offer.member_pubkey != request.public_key() {
            return Err(ApproveDevicePairingError::RequestMismatch);
        }
        host.deliver_invitation(request, invitation.to_bytes())?;
        let drive = self.sync.drive_device_join(
            &invitation.bundle,
            policy,
            access_administrator,
            on_progress,
            timing,
        );
        let cancellation = cancellation_requested(cancel);
        tokio::pin!(drive);
        tokio::pin!(cancellation);
        let outcome = tokio::select! {
            outcome = &mut drive => outcome?,
            () = &mut cancellation => {
                host.cancel()?;
                self.sync
                    .abort_device_join_transport(&invitation.bundle, timing)
                    .await?;
                host.finish()?;
                return Err(ApproveDevicePairingError::Cancelled);
            }
        };
        host.finish()?;
        Ok(outcome)
    }

    /// Persist cancellation, unwind the exact Store attempt retained by the
    /// pairing journal, and close the local pairing session.
    pub(crate) async fn cancel(
        &self,
        host: &DevicePairingHost,
    ) -> Result<(), ApproveDevicePairingError> {
        let timing = crate::DeviceJoinTransportTiming::interactive();
        if let Some(bytes) = host.cancel()? {
            let invitation = coven_domain::joining::DeviceJoinInvite::from_bytes(&bytes)?;
            self.sync
                .abort_device_join_transport(&invitation.bundle, timing)
                .await?;
        }
        host.finish()?;
        Ok(())
    }
}

async fn cancellation_requested(mut cancel: tokio::sync::watch::Receiver<bool>) {
    while !*cancel.borrow() {
        if cancel.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

fn local_pairing_endpoints(port: u16) -> Result<Vec<SocketAddr>, StartDevicePairingError> {
    let addresses = if_addrs::get_if_addrs()
        .map_err(StartDevicePairingError::Interfaces)?
        .into_iter()
        .filter(|interface| interface.is_oper_up() && !interface.is_loopback())
        .filter(|interface| !interface.is_link_local())
        .map(|interface| interface.ip());
    select_pairing_endpoints(addresses, port)
}

fn select_pairing_endpoints(
    addresses: impl IntoIterator<Item = IpAddr>,
    port: u16,
) -> Result<Vec<SocketAddr>, StartDevicePairingError> {
    let mut ipv4 = Vec::new();
    let mut ipv6 = Vec::new();
    for address in addresses {
        match address {
            IpAddr::V4(address) if !address.is_loopback() => ipv4.push(IpAddr::V4(address)),
            IpAddr::V6(address) if !address.is_loopback() => ipv6.push(IpAddr::V6(address)),
            _ => {}
        }
    }
    let selected = if ipv4.is_empty() { ipv6 } else { ipv4 };
    let mut endpoints: Vec<_> = selected
        .into_iter()
        .map(|address| SocketAddr::new(address, port))
        .collect();
    endpoints.sort();
    endpoints.dedup();
    if endpoints.is_empty() {
        return Err(StartDevicePairingError::NoLocalInterface);
    }
    Ok(endpoints)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_discovery_never_advertises_loopback() {
        match local_pairing_endpoints(DEVICE_PAIRING_PORT) {
            Ok(endpoints) => assert!(endpoints
                .iter()
                .all(|endpoint| !endpoint.ip().is_loopback())),
            Err(StartDevicePairingError::NoLocalInterface) => {}
            Err(error) => panic!("interface discovery failed: {error}"),
        }
    }

    #[test]
    fn endpoint_selection_uses_one_listener_family_and_supports_ipv6_only_networks() {
        let ipv4 = "192.0.2.4".parse().expect("IPv4 address");
        let ipv6 = "2001:db8::4".parse().expect("IPv6 address");
        assert_eq!(
            select_pairing_endpoints([ipv6, ipv4], 7).expect("select IPv4 endpoints"),
            vec!["192.0.2.4:7".parse().expect("IPv4 endpoint")]
        );
        assert_eq!(
            select_pairing_endpoints([ipv6], 7).expect("select IPv6 endpoints"),
            vec!["[2001:db8::4]:7".parse().expect("IPv6 endpoint")]
        );
    }
}
