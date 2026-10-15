//! The store's SQLite database, protected internal tables and app migrations.
//!
//! [`DatabaseBuilder`] opens a store with one locked writer and read-only
//! connections, or with read-only connections alone. App SQL runs inside the
//! borrowed [`MigrationContext`] or [`SqlContext`]; no raw connection leaves its
//! owner. A local write commits its app rows, unsigned record and merge metadata
//! together on the writer connection.

mod authorization;
mod authorship;
mod change_capture;
mod database;
mod declaration;
mod device_reset;
mod dismissal;
mod download;
mod download_checks;
mod download_stage;
mod download_stream;
mod entry_effects;
mod entry_inputs;
mod error;
mod execution_capture;
mod file_authorization;
mod file_hashes;
mod file_queue;
mod file_ref;
mod file_removals;
mod file_row;
mod file_write;
mod fingerprint;
mod internal_schema;
mod interpretation;
mod judge;
mod judge_view;
mod key_scope;
mod key_upload;
pub use key_upload::{KeyUpload, KeyUploadBatch};
mod live_query;
mod local_effects;
mod log_input;
mod log_refusal;
pub use log_input::{LogInput, LogKind};
mod loss_record;
mod lost;
mod members;
mod merge_store;
mod migration;
mod migration_change;
mod migration_hidden;
mod migration_names;
mod migration_receive;
mod migration_references;
mod migration_run;
mod migration_snapshot;
mod migration_state;
mod observation;
mod owner_permission;
mod ownership;
mod pending;
mod provider_access;
mod raise_capture;
pub use provider_access::{ProviderAccess, ProviderAccessChange, ProviderAccessRequest};
pub use raise_capture::RaiseCapture;
mod cache_progress;
mod eager_work;
mod read;
pub use cache_progress::{EagerCacheFillStatus, PinProgress};
mod removal;
mod removal_schema;
mod removal_sql;
mod removal_view;
mod retained_writes;
mod retirement_drain;
mod retry;
pub use retirement_drain::{RetirementFinality, DRAIN_WAIT};
pub use retry::{RetrySubject, RetryWait};
mod row_key;
mod row_queries;
mod schema;
mod schema_local_children;
mod schema_source;
mod snapshot_coverage;
mod snapshot_error;
mod snapshot_load;
mod snapshot_metadata;
mod snapshot_reload;
mod snapshot_state;
mod snapshot_witnesses;
pub use snapshot_witnesses::SnapshotWitnesses;
mod snapshot_write;
mod snapshot_writes;
mod sql;
mod sql_value;
mod sqlite;
mod storage_time;
mod store_log;
mod store_log_check;
mod store_log_tables;
mod store_log_upload;
mod written_values;
pub use pending::PendingValue;
mod file_protection;
mod source_names;
mod upload;
mod user_file;
mod write;
mod write_apply;
mod write_batch;
mod write_boundary;
mod write_capture;
mod write_commit;
mod write_encoding;
mod write_failure;
mod write_headers;
mod write_landing;
mod write_record;
mod write_retirement;
mod write_rows;
mod write_schema;
mod write_sources;

pub use authorship::{Authorship, HeaderEvidence};
pub use coven_format::value::EntryId;
pub use coven_merge::WriteId;
pub use database::file_database::{CacheReservation, FileDatabase};
pub use database::file_retention::{FileCandidate, FileRetention};
pub use database::input_database::{
    FileEvidence, InputBudget, RetainedInput, RetainedRange, UnprovenFiles,
};
pub use database::{
    discarded_operations, Database, DatabaseBuilder, DatabaseReadHandle, IdentityReservations,
};
pub use declaration::{CacheFill, FileDecl, Provenance, RowIdentity, SyncedTable};
pub use download::{ApplyOutcome, DownloadedPart, DownloadedWrite, SyncState, WriteWait};
pub use download_stream::{DownloadedPartStream, DownloadedWriteStream, WriteAuthorship};
pub use error::{
    CovenError, CovenMigrationError, CovenResult, DbError, MigrationError, SchemaError,
};
pub use file_queue::{FileUpload, UploadQueueState};
pub use file_ref::{FileRef, LocalFileError, LocalFileStream};
pub use live_query::{
    LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent,
};
pub use log_refusal::{LogObject, LogRefusal};
pub use lost::{
    ChangeSource, Lost, LostCell, LostValue, RemovalRule, Replacement, RowAuthorizationFailure,
};
pub use members::{Circle, CircleMemberInfo, DeviceInfo, MemberInfo};
pub use migration::{Migration, MigrationChange, MigrationContext, MigrationOutcome};
pub use migration_change::{ChangeOp, ColumnChange, RowChange};
pub use read::{Read, SqlReadContext};
pub use row_key::RowKey;
pub use snapshot_error::SnapshotError;
pub use snapshot_reload::{LoadedSnapshot, SnapshotReload, SnapshotSource};
pub use snapshot_write::{CapturedState, SnapshotWriteError};
pub use store_log::{
    DeviceState, DropReason, EntryOutcome, EntryTarget, StoreCircle, StoreDevice, StoreIdentity,
    StoreLog, StoreLogReplay, StoreLogState, StoreMember, StoreVersion,
};
pub use store_log_check::{CircleRemoval, ReplayEntry, StoreLogCheck};
pub use store_log_upload::{LocalStoreLog, StoreLogSealing, StoreLogUpload};
pub use upload::{UploadBytes, UploadParts, UploadReadError, WaitingUpload};
pub use user_file::{prepare_user_file, PreparedUserFile, UserFile};
pub use write::SqlContext;
pub use write_batch::{FileSource, WriteBatch};
pub use write_boundary::{Boundary, BoundaryCoverage};
pub use write_failure::WriteFailure;
pub use write_landing::{Published, VerdictWait, WRITE_WINDOW};

// SQL values and parameters are part of the API. Connections remain private.
pub use rusqlite::{named_params, params, types, Params, Row, ToSql};

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

mod operation;
pub use operation::{
    AccessKeyDeletion, AccessKeyToDelete, NewOperation, OperationId, OperationRecord,
    OperationUpdate,
};

mod commit_scope;
pub use commit_scope::CommitScope;
mod prerequisite_rows;
pub use prerequisite_rows::{AttemptCommits, PrerequisiteRows};
mod database_changes;
pub use database_changes::DatabaseChanges;
