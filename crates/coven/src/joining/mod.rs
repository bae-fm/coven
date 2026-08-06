mod client;
mod transport;

#[cfg(test)]
pub(crate) mod test_runtime;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;

pub use crate::join_code::{
    abandon_join_request, decode_invite_code_info, decode_join_request, generate_join_request,
    InviteCodeInfo, JoinCodeError, JoinRequestCode,
};
pub use client::{BootstrapError, DeviceJoinClient};
pub use transport::{
    close_scanned_invite_join, join_with_scanned_invite, DeviceJoinInvite,
    DeviceJoinTransportOutcome,
};
#[cfg(any(test, feature = "test-utils"))]
pub use transport::{
    close_scanned_invite_join_over_test_home, join_with_scanned_invite_over_test_home,
};

pub(crate) use crate::join_code::*;
pub(crate) use client::*;
