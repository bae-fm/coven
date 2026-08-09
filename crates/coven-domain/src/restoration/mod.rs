mod code;
mod restore;

#[cfg(test)]
mod tests;

pub use code::{
    decode_restore_code, decode_restore_code_info, encode_restore_code, RestoreCode,
    RestoreCodeError, RestoreCodeInfo, RESTORE_CODE_VERSION,
};
pub use coven_protocol::recovery::{
    ActivatedContinuation, OwnerRecoveryAuthority, RestoreAuthority,
};
pub use restore::{restore_from_cloud, restore_from_code, RestoreSource};
