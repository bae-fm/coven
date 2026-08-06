mod client;
mod transport;

#[cfg(test)]
pub(crate) mod test_runtime;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod transport_tests;

pub use client::{BootstrapError, DeviceJoinClient};
pub use coven_storage::join_code::{
    abandon_join_request, decode_invite_code_info, decode_join_request, generate_join_request,
    InviteCodeInfo, JoinCodeError, JoinRequestCode,
};
pub use transport::{
    close_scanned_invite_join, join_with_scanned_invite, DeviceJoinInvite,
    DeviceJoinTransportOutcome,
};
#[cfg(test)]
pub(crate) use transport::{
    close_scanned_invite_join_over_test_home, join_with_scanned_invite_over_test_home,
};

pub(crate) use client::*;
pub(crate) use coven_storage::join_code::*;
