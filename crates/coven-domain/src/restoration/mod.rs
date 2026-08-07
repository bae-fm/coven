mod restore;

#[cfg(test)]
mod tests;

pub use coven_protocol::recovery::{
    ActivatedContinuation, OwnerRecoveryAuthority, RestoreAuthority,
};
pub use coven_storage::restore_code::{
    decode_restore_code_info, RestoreCodeError, RestoreCodeInfo,
};
pub use restore::{restore_from_cloud, restore_from_code, RestoreSource};
