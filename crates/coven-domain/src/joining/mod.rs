mod client;
pub mod config;
mod request;
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
pub use request::{
    abandon_join_request, decode_join_request, encode_join_request, generate_join_request,
    generate_join_request_for_keypair, JoinRequest,
};
pub use transport::{
    close_scanned_invite_join, join_with_scanned_invite, DeviceInviteError, DeviceInviteInfo,
    DeviceJoinInvite, DeviceJoinTransportOutcome,
};
#[cfg(any(test, feature = "test-utils"))]
pub use transport::{
    close_scanned_invite_join_over_test_home, join_with_scanned_invite_over_test_home,
};

// What `restoration` shares with the join flow it grew out of.
pub(crate) use client::{derive_credentials, BootstrapCleanup};
pub(crate) use config::build_config;
