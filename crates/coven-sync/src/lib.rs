//! Device logs, store-log replay, keys, snapshots, files and durable operations (§§6–18).

mod access_keys;
pub use access_keys::DeleteAccessKeyError;
mod carried_access_keys;
pub use carried_access_keys::{carry_access_keys, discarded_access_keys, CarriedAccessKey};
mod conflicts;
mod device_log_sync;
mod effects;
mod entry_signature;
mod finality;
mod identity;
mod pass_reads;
mod recent_return;
mod retry_waits;
pub use retry_waits::RetryWaits;
mod send_gate;
pub use send_gate::{PublicationScope, SendGate};
mod replay;
mod replay_cache;
mod stream_input;
mod write_object;
mod write_seal;

pub use replay::{replay, replay_entry};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

mod error;
mod key_introductions;
mod member_credentials;
mod object_author;
mod object_input;
mod posted_positions;
mod store_log_keys;
mod store_log_landing;
mod store_log_object;
mod store_log_sync;
pub use coven_database::{DropReason, EntryTarget};
pub use device_log_sync::{DeviceLogSync, WriteDiscovery};
pub use error::{DamagedObject, SyncError, SyncFailure};
mod pending;
mod pending_encoding;
mod pending_logs;
pub use pending::{
    FileMissingReason, FileSourceFailure, LocalFailure, PendingReason, PendingRecord,
    PendingSubject, Prerequisite, ProviderAccessAction, RequiredUpdate, Retry,
};
mod refusal;
pub use refusal::Refusal;
pub use store_log_sync::{JoinOutcome, StoreLogSync};

mod operation_types;
pub use operation_types::*;
mod operation_data;
mod operations;
pub use operations::Operations;
mod files;
pub use files::{
    DrainOutcome, EagerCacheFillStatus, FileRangeStream, FileReadError, FileStatus, FileStream,
    Files, PinProgress, QueuedUpload, TransferLimits, UploadPhase, UploadQueue,
};

mod join_admission;
mod joining_identity;
pub use join_admission::BindingSource;
mod recorded_upload;
mod snapshot_data;

mod sync_loop;
pub use sync_loop::{SyncLoop, SyncStatus};

/// Compensation retained until setup has committed its keys and applied origin.
type StorageRollback = Box<dyn FnOnce() -> Result<(), SyncError> + Send>;
/// Credential/settings commit performed after remote setup has succeeded.
pub type StorageCommit = Box<dyn FnOnce() -> Result<StorageRollback, SyncError> + Send>;
pub use joining_identity::JoiningIdentity;
mod codes;
pub use codes::{
    decode_code_info, read_invite_code, read_restore_code, CodeError, CodeInfo, CodeKind,
};
mod credential_custody;
pub use credential_custody::{commit_credentials, CredentialRollback};
mod restore_codes;
pub use restore_codes::{read_connection, RestoreCodes};
mod sign_in;
pub use sign_in::{DeviceCredentials, SessionSignIn};

mod discovery;
