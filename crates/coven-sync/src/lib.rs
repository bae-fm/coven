//! The shared store sequence, durable operations, snapshots and file transfers.

mod admin_steps;
mod chunk_reader;
mod effects;
mod error;
mod failure_log;
mod floor_object;
mod history;
mod identity;
mod joining_identity;
mod last_pass;
mod object_input;
mod operation_outcome;
mod operation_types;
mod pending;
mod pending_encoding;
mod recorded_upload;
mod refusal;
mod restore_codes;
mod saved_code;
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
mod network_follow;
pub use network_follow::NetworkFollow;
mod retry_waits;
mod sequence_state;
mod sync_often;
mod sync_status;
pub use files::{
    DeviceOnlyError, DeviceOnlyStep, DrainOutcome, FileRangeStream, FileReadError, FileStatus,
    FileStream, Files, PinProgress, QueuedUpload, ReadProgress, TransferLimits, UploadPhase,
    UploadProgress, UploadQueue,
};
pub use retry_waits::{
    NetworkOwner, PassRequests, RetryAnswer, RetryEffect, RetryOutcome, RetryWaits,
};
pub use sync_often::SyncOftenGuard;

pub use admin_steps::{administer, create_circle, replace_member_keys_for};
pub use codes::{
    decode_code_info, read_invite_code, read_restore_code, CodeError, CodeInfo, CodeKind,
    CodeMetadata, InviteInfo,
};
pub use coven_database::WriteCutoff;
pub use coven_database::{DeviceStop, EntryTarget, OperationOutcome, OperationRejection};
pub use credential_custody::{commit_credentials, CredentialKeychains, CredentialRollback};
pub use error::{DamagedObject, SyncError, SyncFailure};
pub use identity::DeviceIdEvidence;
pub use joining_identity::JoiningIdentity;
pub use last_pass::{LastPass, PassFailure, StatusFailure};
pub use operation_types::*;
pub use pending::{
    FileMissingReason, FileSourceFailure, LocalFailure, PendingReason, PendingRecord,
    PendingSubject, Prerequisite, ProviderAccessAction, RequiredUpdate, Retry,
};
pub use refusal::Refusal;
pub use restore_codes::{read_connection, RestoreCodes};
pub use saved_code::{
    follow_name, read_saved_code, replacement_code, SavedName, UnreadableSavedCode,
};
pub use sequence_state::{SequenceState, SequenceStatus};
pub use sequence_sync::sequence_invites::expiry::InviteExpiry;
pub use sequence_sync::sequence_invites::{
    approve_join_request, cancel_invite, create_invite, decline_join_request, NewInvite,
};
pub use sequence_sync::sequence_join::{JoinOutcome, NextCheck};
pub use sequence_sync::{retry_pending_operation, SequenceSync};
pub use sign_in::{DeviceCredentials, SessionSignIn};
pub use sync_status::{
    record_location, record_opened, subscribe_sync_status, sync_status, SyncCause, SyncState,
    SyncStatus, SyncWait,
};

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

/// Compensation retained until setup has committed its keys and applied origin.
pub type StorageRollback = Box<dyn FnOnce() -> Result<(), SyncError> + Send>;
/// Credential and settings commit performed after remote setup has succeeded.
pub type StorageCommit = Box<dyn FnOnce() -> Result<StorageRollback, SyncError> + Send>;
