//! The shared store sequence, durable operations, snapshots and file transfers.

mod chunk_reader;
mod effects;
mod error;
mod floor_object;
mod history;
mod identity;
mod joining_identity;
mod object_input;
mod operation_rejection;
mod operation_types;
mod pending;
mod pending_encoding;
mod recorded_upload;
mod refusal;
mod restore_codes;
mod sealed_stream;
mod sequence_sync;
mod sign_in;
mod slot_object;
mod snapshot_authorship;
mod snapshot_object;
mod stream_input;
mod write_object;
mod write_seal;
pub use sequence_sync::sync_loop::SyncLoop;
mod codes;
mod connection_retry;
pub use connection_retry::{connection_failure, ConnectionRetry};
mod credential_custody;
mod files;
mod retry_waits;
mod sequence_state;
pub use files::{
    upload_queue_from_state, DrainOutcome, EagerCacheFillStatus, FileRangeStream, FileReadError,
    FileStatus, FileStream, Files, PinProgress, QueuedUpload, TransferLimits, UploadPhase,
    UploadQueue,
};
pub use retry_waits::RetryWaits;

pub use codes::{
    decode_code_info, read_invite_code, read_restore_code, CodeError, CodeInfo, CodeKind,
    CodeMetadata,
};
pub use coven_database::WriteCutoff;
pub use credential_custody::{commit_credentials, CredentialRollback};
pub use error::{DamagedObject, SyncError, SyncFailure};
pub use identity::DeviceIdEvidence;
pub use joining_identity::JoiningIdentity;
pub use operation_rejection::{EntryTarget, OperationRejection};
pub use operation_types::*;
pub use pending::{
    FileMissingReason, FileSourceFailure, LocalFailure, PendingReason, PendingRecord,
    PendingSubject, Prerequisite, ProviderAccessAction, RequiredUpdate, Retry,
};
pub use refusal::Refusal;
pub use restore_codes::{read_connection, RestoreCodes};
pub use sequence_state::{SequenceStatus, SyncState, SyncStatus};
pub use sequence_sync::access_keys::DeleteAccessKeyError;
pub use sequence_sync::sequence_join::JoinOutcome;
pub use sequence_sync::SequenceSync;
pub use sign_in::{DeviceCredentials, SessionSignIn};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

/// Compensation retained until setup has committed its keys and applied origin.
pub type StorageRollback = Box<dyn FnOnce() -> Result<(), SyncError> + Send>;
/// Credential and settings commit performed after remote setup has succeeded.
pub type StorageCommit = Box<dyn FnOnce() -> Result<StorageRollback, SyncError> + Send>;
