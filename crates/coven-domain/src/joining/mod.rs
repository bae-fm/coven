mod client;
pub mod config;
mod pairing;
mod pairing_transport;
mod transport;

#[cfg(any(test, feature = "test-utils"))]
pub mod test_runtime;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;

pub use client::{
    BootstrapCleanupFailure, BootstrapCleanupFailures, BootstrapError, SigningKeyError,
};
pub use pairing::{
    DevicePairingError, DevicePairingOffer, DevicePairingRequest, PreparedDevicePairing,
    SealedDevicePairingRequest,
};
pub use pairing_transport::{
    receive_device_invitation, DevicePairingHost, DevicePairingTransportError,
};
#[cfg(any(test, feature = "test-utils"))]
pub use transport::join_with_device_pairing_over_test_home;
pub use transport::{
    join_with_device_pairing, DeviceInviteError, DeviceJoinInvite, DeviceJoinTransportOutcome,
};

// What `restoration` shares with the join flow it grew out of.
pub(crate) use client::{derive_credentials, BootstrapCleanup};
pub(crate) use config::build_config;
