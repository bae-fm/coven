//! Failures of sequence publication, file uploads and sync steps (E5, E7).

use crate::OperationRejection;
use coven_crypto::{custody::KeyError, CryptoError, MaterialError};
use coven_database::DbError;
use coven_foundation::files::{FileError, ObservationError, StoreLockError};
use coven_foundation::id_source::KeyId;
use coven_merge::Audience;
use coven_storage::{StorageError, StorageFailure};
use std::sync::Arc;

/// Why `confirm_device_cut_off` refused (§14.2, §20.9). Each is a reason the
/// call can't change anything; the app tells the person which one.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CutOffRefusal {
    /// The applied history doesn't have the device gone for good: its
    /// removal hasn't reached this device yet, or it was never removed.
    /// After the next pass has read the removal the call can succeed.
    DeviceNotGone,
    /// The member whose device this is doesn't hold the store: another
    /// member's account does, and only their devices change sharing. The
    /// call can't succeed here.
    NotStoreOwner,
    /// The store is on S3, where no account is shared, so there is nothing to
    /// take back. The call can't succeed.
    NoAccountShared,
}

/// A synchronization request failed; its durable queue, if any, remains retryable.
#[derive(Debug, thiserror::Error)]
pub enum SyncError {
    /// This call requires work that cannot yet advance.
    #[error("pending: {0:?}")]
    Pending(Box<crate::PendingRecord>),
    /// A local invariant or resource failed, classified without erasing its
    /// native cause. One failure delivered to several callers shares one cause.
    #[error("{failure:?}: {source}")]
    Failed {
        /// Durable local category.
        failure: crate::LocalFailure,
        /// Original failure retained by the initiating call.
        #[source]
        source: Arc<dyn std::error::Error + Send + Sync>,
    },
    /// A later step, such as a rollback or closing another owner, failed
    /// too; neither failure is hidden.
    #[error("{operation}; then: {cleanup}")]
    Cleanup {
        /// The original failure.
        operation: Box<SyncError>,
        /// The later failure, such as restoring the original state.
        #[source]
        cleanup: Box<SyncError>,
    },
    /// This call requires connected storage; durable work remains queued.
    #[error("storage is not connected")]
    NoStorage,
    /// The member whose account holds the store can't be removed, and their
    /// access can't move to another account (§10.1): OneDrive and CloudKit
    /// can't hand a store to another account, and coven doesn't use Dropbox's
    /// ownership transfer (bae-fm/coven#237).
    #[error("the store owner cannot be removed or leave the account that holds the store")]
    StoreOwner,
    /// The request or invitation is absent, expired, or no longer matches.
    #[error("invitation or join request is no longer current")]
    InvitationChanged,
    /// The requested journal row is not failed app work available for retry or discard.
    #[error("operation {0:?} is not pending")]
    NotPending(crate::OperationId),
    /// Administrative and snapshot publication wait for the retained reload (§11).
    #[error("snapshot reload {0:?} must finish before publishing entries or snapshots")]
    ReloadPending(crate::OperationId),
    /// Deleting stored files waits for this unfinished snapshot, whose
    /// journal still owns references no row holds (§17.10).
    #[error("snapshot {0:?} must finish before stored files are deleted")]
    SnapshotPending(crate::OperationId),
    /// Decoding persisted operation data failed; its cause remains available.
    #[error("invalid operation data: {0}")]
    OperationData(#[from] serde_json::Error),
    /// The sequence has no next slot: slot numbers are `u64`s, and the
    /// slot it would follow is the last one.
    #[error("the store sequence is exhausted")]
    SequenceExhausted,
    /// Storage refused or failed a request.
    #[error(transparent)]
    Storage(StorageError),
    /// The local database failed.
    #[error(transparent)]
    Database(DbError),
    /// Unlocking or keeping keys failed.
    #[error(transparent)]
    SecureStorage(KeyError),
    /// Making encrypted or signed bytes failed.
    #[error(transparent)]
    Crypto(CryptoError),
    /// The proposed entry violates its byte format.
    #[error(transparent)]
    Format(coven_format::Error),
    /// A required key has not arrived or the local key material conflicts.
    #[error(transparent)]
    Key(#[from] MaterialError),
    /// A required key's sealed copy has not arrived.
    #[error("key {key} is not available yet")]
    KeyUnavailable {
        /// The audience the key belongs to.
        audience: Audience,
        /// The missing key.
        key: KeyId,
    },
    /// The proposed change is already invalid in the author's applied view.
    #[error("administrative change rejected: {0:?}")]
    Rejected(OperationRejection),
    /// `confirm_device_cut_off` changed nothing, for this reason.
    #[error("the device cut-off was refused: {0:?}")]
    CutOffRefused(CutOffRefusal),
    /// A required object failed authentication or parsing.
    #[error(transparent)]
    Damaged(#[from] crate::DamagedObject),
    /// This device or its member was removed; syncing stops for good (§14.1).
    #[error("this device or its member was removed")]
    Removed,
    /// A joined store read to the end of the sequence with its pair bound
    /// nowhere in its history: the admission that published it was refused,
    /// and its device never became a member. Syncing stops for good (§13.2,
    /// §14.1).
    #[error("the admission of this device was refused")]
    NotAdmitted,
    /// A replacement of this member's S3 key by another key is still under
    /// way: its id isn't recorded yet, or custody doesn't connect with it yet
    /// (§14.3). Retrying with that key finishes it; once it ends, another key
    /// can replace it.
    #[error("the replacement by S3 key {access_key_id} is still under way")]
    ReplacementPending {
        /// The key the replacement under way records.
        access_key_id: String,
    },
    /// Another copy uses this device's id, as this evidence shows. This copy
    /// sends nothing more; nothing is recorded, and `reset_device` is the
    /// app's option (§19.3).
    #[error("another copy uses this device's id: {0:?}")]
    DeviceIdInUse(crate::DeviceIdEvidence),
    /// Held signing credentials are no longer current for this stable id, or
    /// an object was fixed under credentials that are no longer current.
    /// Sending stops; it never re-signs an old object with newer keys (§14.1).
    #[error("member {member:?} has newer keys; held credentials cannot send")]
    MemberKeysReplaced {
        /// The stable member whose keys were replaced.
        member: coven_foundation::id_source::MemberId,
    },
    /// This call needs a newer app schema or coven format (§18).
    #[error("an update is required: {0:?}")]
    UpdateRequired(crate::RequiredUpdate),
    /// A code can't be used for this call, as one naming another store.
    #[error(transparent)]
    Code(#[from] crate::CodeError),
    /// The call's runtime is shutting down and dropped its work (E15). A
    /// database call the blocking pool dropped unstarted changed nothing; a
    /// call that runs as its own task, as closing and replacing credentials
    /// do, may have stopped partway.
    #[error(transparent)]
    Shutdown(#[from] coven_foundation::task::Shutdown),
    /// The call's work panicked: a bug in coven, which the caller reports
    /// (E15). Its category is `LocalFailure::Bug`.
    #[error(transparent)]
    Bug(#[from] coven_foundation::task::Bug),
    /// The worker this call needs ended on a panic: the sync loop, a
    /// connection attempt or the upload worker. The store's bug sink heard
    /// the bug as it happened, so the app shows this and doesn't report it
    /// again (E15). Its category is `LocalFailure::Bug`.
    #[error("{0}; it has stopped")]
    WorkerFailed(coven_foundation::task::Bug),
}

impl From<CutOffRefusal> for SyncError {
    fn from(refusal: CutOffRefusal) -> Self {
        Self::CutOffRefused(refusal)
    }
}

impl coven_foundation::task::CarriesBug for SyncError {
    fn bug(&self) -> Option<&coven_foundation::task::Bug> {
        match self {
            Self::Bug(bug) => Some(bug),
            // An earlier bug, which the sink heard as it happened.
            Self::WorkerFailed(_) => None,
            Self::Database(error) => error.bug(),
            Self::SecureStorage(error) => error.bug(),
            Self::Storage(error) => error.bug(),
            Self::Cleanup { operation, cleanup } => operation.bug().or_else(|| cleanup.bug()),
            // The cause `into_shared` shares is the error itself.
            Self::Failed { source, .. } => {
                source.downcast_ref::<Self>().and_then(coven_foundation::task::CarriesBug::bug)
            }
            _ => None,
        }
    }
}

impl From<coven_foundation::task::TaskFailed> for SyncError {
    fn from(failed: coven_foundation::task::TaskFailed) -> Self {
        failed.into_error()
    }
}

impl From<coven_database::SqliteError> for SyncError {
    fn from(error: coven_database::SqliteError) -> Self {
        DbError::from(error).into()
    }
}

/// Sync's own writes keep their error: a database failure as itself, and
/// any other with a failed rollback or byte removal as a later step that
/// failed too (§4, Appendix C.3).
impl coven_database::WriteFailure for SyncError {
    fn with_rollback(self, rollback: coven_database::SqliteError) -> Self {
        match self {
            Self::Database(error) => Self::Database(error.with_rollback(rollback)),
            error => Self::cleanup(error, rollback.into()),
        }
    }

    fn with_cleanup(write: Self, failures: Vec<DbError>) -> Self {
        match write {
            Self::Database(error) => Self::Database(DbError::with_cleanup(error, failures)),
            error => failures
                .into_iter()
                .fold(error, |error, failure| Self::cleanup(error, failure.into())),
        }
    }
}

impl From<DbError> for SyncError {
    fn from(error: DbError) -> Self {
        match error {
            // The database refusing sync's own requests against changed state
            // is not a database failure an app can act on.
            error @ DbError::OperationChanged(_) => {
                Self::Failed { failure: crate::LocalFailure::Database, source: Arc::new(error) }
            }
            // Coven broke its own rule: a bug, as a panic is.
            error @ DbError::Invariant { .. } => {
                Self::Failed { failure: crate::LocalFailure::Bug, source: Arc::new(error) }
            }
            // The process is ending the call; the database did not fail.
            DbError::Shutdown(shutdown) => Self::Shutdown(shutdown),
            DbError::Bug(bug) => Self::Bug(bug),
            DbError::StagingCleanup { operation, cleanup } => {
                Self::cleanup((*operation).into(), (*cleanup).into())
            }
            error => Self::Database(error),
        }
    }
}

impl SyncError {
    /// `operation` failed, and a later step (a rollback, closing another
    /// owner) failed too; neither failure is hidden.
    pub fn cleanup(operation: SyncError, cleanup: SyncError) -> Self {
        Self::Cleanup { operation: Box::new(operation), cleanup: Box::new(cleanup) }
    }

    /// The durable reason for a failed request, without losing its typed cause here.
    /// Every variant is matched by name, so a new one is classified here.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub fn pending_reason(&self, provider: coven_storage::CloudProvider) -> crate::PendingReason {
        use crate::PendingReason;
        let error = self;
        match error {
            SyncError::Pending(record) => record.reason.clone(),
            SyncError::Storage(error) => PendingReason::Storage {
                provider,
                failure: error.failure(),
                answered: error.answered(),
            },
            SyncError::Damaged(object) => PendingReason::Refused(object.failure.clone()),
            SyncError::UpdateRequired(update) => PendingReason::UpdateRequired(*update),
            SyncError::Removed => PendingReason::Removed,
            SyncError::NotAdmitted => PendingReason::NotAdmitted,
            SyncError::MemberKeysReplaced { member } => {
                PendingReason::MemberKeysReplaced { member: *member }
            }
            SyncError::KeyUnavailable { audience, key } => {
                PendingReason::KeyUnavailable { audience: audience.clone(), key: *key }
            }
            SyncError::Rejected(reason) => PendingReason::Rejected(reason.clone()),
            SyncError::Cleanup { operation, cleanup } => PendingReason::Cleanup {
                operation: Box::new(operation.pending_reason(provider)),
                cleanup: Box::new(cleanup.pending_reason(provider)),
            },
            SyncError::Failed { .. }
            | SyncError::NoStorage
            | SyncError::StoreOwner
            | SyncError::InvitationChanged
            | SyncError::CutOffRefused(_)
            | SyncError::ReplacementPending { .. }
            | SyncError::NotPending(_)
            | SyncError::ReloadPending(_)
            | SyncError::SnapshotPending(_)
            | SyncError::OperationData(_)
            | SyncError::SequenceExhausted
            | SyncError::Database(_)
            | SyncError::SecureStorage(_)
            | SyncError::Crypto(_)
            | SyncError::Format(_)
            | SyncError::Key(_)
            | SyncError::DeviceIdInUse(_)
            | SyncError::Code(_)
            | SyncError::Shutdown(_)
            | SyncError::Bug(_)
            | SyncError::WorkerFailed(_) => PendingReason::Failed(error.local_failure()),
        }
    }

    /// A local file, directory or store-lock failure, retaining its cause.
    pub(crate) fn disk(error: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Failed { failure: crate::LocalFailure::of_disk(&error), source: Arc::new(error) }
    }

    /// The local category recorded for a failure that names no other condition.
    #[deny(clippy::wildcard_enum_match_arm)]
    pub(crate) fn local_failure(&self) -> crate::LocalFailure {
        match self {
            Self::Failed { failure, .. } => *failure,
            Self::Database(_) => crate::LocalFailure::Database,
            Self::SecureStorage(_) => crate::LocalFailure::KeyCustody,
            Self::Crypto(_) | Self::Key(_) => crate::LocalFailure::Crypto,
            Self::Cleanup { operation, .. } => operation.local_failure(),
            Self::Pending(_)
            | Self::NoStorage
            | Self::StoreOwner
            | Self::InvitationChanged
            | Self::ReplacementPending { .. }
            | Self::NotPending(_)
            | Self::ReloadPending(_)
            | Self::SnapshotPending(_)
            | Self::OperationData(_)
            | Self::SequenceExhausted
            | Self::Storage(_)
            | Self::Format(_)
            | Self::KeyUnavailable { .. }
            | Self::Rejected(_)
            | Self::CutOffRefused(_)
            | Self::Damaged(_)
            | Self::Removed
            | Self::NotAdmitted
            | Self::DeviceIdInUse(_)
            | Self::MemberKeysReplaced { .. }
            | Self::UpdateRequired(_)
            | Self::Code(_) => crate::LocalFailure::Operation,
            Self::Shutdown(_) => crate::LocalFailure::Shutdown,
            Self::Bug(_) | Self::WorkerFailed(_) => crate::LocalFailure::Bug,
        }
    }

    /// A local database, disk or custody failure that can clear by itself,
    /// such as a locked keychain or a failed disk write (E5), or the runtime
    /// shutting down, which the next process clears (E15). It fails only
    /// this attempt and says nothing about the slot being applied (§19.1).
    pub(crate) fn local_failure_clears_by_itself(&self) -> bool {
        matches!(
            self,
            Self::Database(_)
                | Self::SecureStorage(_)
                | Self::Failed { .. }
                | Self::Shutdown(_)
                | Self::Bug(_)
                | Self::WorkerFailed(_)
        ) && crate::PendingReason::Failed(self.local_failure()).clears()
            == Some(crate::pending::Clears::ByItself)
    }

    /// The error a pending record keeps as its cause, the one `into_shared`
    /// shares: a local failure's source, any other error itself.
    pub(crate) fn cause(&self) -> &(dyn std::error::Error + 'static) {
        match self {
            Self::Failed { source, .. } => source.as_ref(),
            error => error,
        }
    }

    /// The local category and one shared cause, for a status and a waiting
    /// caller failed by the same error.
    pub fn into_shared(self) -> (crate::LocalFailure, Arc<dyn std::error::Error + Send + Sync>) {
        match self {
            Self::Failed { failure, source } => (failure, source),
            error => (error.local_failure(), Arc::new(error)),
        }
    }
}

impl From<StorageError> for SyncError {
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Bug(bug) => Self::Bug(bug),
            error => Self::Storage(error),
        }
    }
}

impl From<KeyError> for SyncError {
    fn from(error: KeyError) -> Self {
        match error {
            KeyError::Bug(bug) => Self::Bug(bug),
            error => Self::SecureStorage(error),
        }
    }
}

impl From<FileError> for SyncError {
    fn from(error: FileError) -> Self {
        match error {
            FileError::Bug(bug) => Self::Bug(bug),
            error => Self::disk(error),
        }
    }
}

impl From<ObservationError> for SyncError {
    fn from(error: ObservationError) -> Self {
        Self::disk(error)
    }
}

impl From<coven_foundation::files::WriterError> for SyncError {
    fn from(error: coven_foundation::files::WriterError) -> Self {
        match error {
            coven_foundation::files::WriterError::Lock(error) => error.into(),
            coven_foundation::files::WriterError::File(error) => error.into(),
        }
    }
}

impl From<StoreLockError> for SyncError {
    fn from(error: StoreLockError) -> Self {
        match error {
            StoreLockError::Bug(bug) => Self::Bug(bug),
            error => Self::disk(error),
        }
    }
}

/// A sync step stopped as a whole (E5).
#[derive(Clone, Debug, thiserror::Error)]
pub enum SyncFailure {
    /// A schema or stored object format is newer than this implementation.
    #[error("an update is required")]
    UpdateRequired(crate::RequiredUpdate),
    /// This device or its member has been removed.
    #[error("this device or its member was removed")]
    Removed,
    /// The admission that published this joined store was refused (§13.2).
    #[error("the admission of this device was refused")]
    NotAdmitted,
    /// Another copy uses this device's id; this copy sends nothing more (§19.3).
    #[error("another copy uses this device's id: {0:?}")]
    DeviceIdInUse(crate::DeviceIdEvidence),
    /// The provider refused or failed a request.
    #[error("storage failed: {0}")]
    Storage(#[source] Arc<StorageError>),
    /// The pass finished, but this work of coven's own failed its last
    /// attempt; it retries under backoff (E5). The record carries the cause
    /// it keeps, and the text names it.
    #[error("coven's own work failed its last attempt: {0}")]
    Operation(Box<crate::PendingRecord>),
    /// Any other failure, retaining its typed cause.
    #[error("sync failed: {0}")]
    Other(#[source] Arc<dyn std::error::Error + Send + Sync>),
}

impl From<OperationRejection> for SyncError {
    fn from(reason: OperationRejection) -> Self {
        match reason {
            OperationRejection::StoreOwner => Self::StoreOwner,
            reason => Self::Rejected(reason),
        }
    }
}

impl From<StorageFailure> for SyncError {
    fn from(failure: StorageFailure) -> Self {
        Self::Storage(failure.into())
    }
}

impl From<SyncError> for SyncFailure {
    fn from(error: SyncError) -> Self {
        match error {
            SyncError::Removed => Self::Removed,
            SyncError::NotAdmitted => Self::NotAdmitted,
            SyncError::DeviceIdInUse(evidence) => Self::DeviceIdInUse(evidence),
            SyncError::UpdateRequired(update) => Self::UpdateRequired(update),
            SyncError::Storage(error) => Self::Storage(Arc::new(error)),
            error => Self::Other(Arc::new(error)),
        }
    }
}

impl From<coven_format::Error> for SyncError {
    fn from(error: coven_format::Error) -> Self {
        match required_format(&error) {
            Some(update) => Self::UpdateRequired(update),
            None => Self::Format(error),
        }
    }
}

impl From<CryptoError> for SyncError {
    fn from(error: CryptoError) -> Self {
        Self::Crypto(error)
    }
}

/// An object whose checks failed (§19.1).
#[derive(Clone, Debug, thiserror::Error)]
#[error("damaged object at {path}: {failure}")]
pub struct DamagedObject {
    /// The exact path read from storage.
    pub path: String,
    /// The failed check and its cause.
    #[source]
    pub failure: crate::Refusal,
}

pub(crate) fn required_format(
    error: &(dyn std::error::Error + 'static),
) -> Option<crate::RequiredUpdate> {
    let version = match error.downcast_ref::<coven_format::Error>() {
        Some(coven_format::Error::UnsupportedVersion(version)) => Some(*version),
        _ => None,
    }?;
    (version > coven_format::FORMAT_VERSION)
        .then_some(crate::RequiredUpdate::CovenFormat { version })
}

/// Older unsupported formats are damaged inputs; only newer ones require an update.
pub(crate) fn newer_format(error: &(dyn std::error::Error + 'static)) -> bool {
    required_format(error).is_some()
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
