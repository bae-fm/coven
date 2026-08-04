mod restore;

#[cfg(test)]
mod tests;

pub use crate::protocol::recovery::{
    ActivatedContinuation, OwnerRecoveryAuthority, RestoreAuthority,
};
pub use crate::restore_code::{
    decode_restore_code_info, RestoreCode, RestoreCodeError, RestoreCodeInfo,
};
pub use restore::{restore_from_cloud, restore_from_code, RestoreSource};

pub(crate) use crate::restore_code::*;
