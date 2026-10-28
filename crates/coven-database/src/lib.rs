//! The store's SQLite database, protected internal tables and app migrations.
//!
//! [`DatabaseBuilder`] opens a store with one locked writer and read-only
//! connections, or with read-only connections alone. App SQL runs inside the
//! borrowed [`MigrationContext`] or [`SqlContext`]; no raw connection leaves its
//! owner. A local write commits its app rows, unsigned record and merge metadata
//! together on the writer connection.

mod author_judgement;
mod authorization;
mod authorship;
mod change_capture;
mod coven_functions;
mod sql_functions;
pub use sql_functions::{Determinism, FunctionArguments, SqlFunctions};
mod database;
mod declaration;
mod device_reset;
mod dismissal;
mod download;
mod download_checks;
mod download_stage;
mod download_stream;
mod error;
mod execution_capture;
mod file_authorization;
mod file_hashes;
mod file_queue;
mod file_ref;
mod file_removals;
mod file_row;
mod file_write;
mod internal_schema;
mod interpretation;
mod judge;
mod judge_view;
mod key_scope;
mod live_query;
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
mod ownership;
mod pending;
mod pending_attachment;
mod provider_access;
mod raise_capture;
pub use provider_access::{ProviderAccess, ProviderAccessChange, ProviderAccessRequest};
pub use raise_capture::RaiseCapture;
mod cache_progress;
mod read;
pub use cache_progress::PinProgress;
mod removal;
mod removal_schema;
mod removal_sql;
mod removal_view;
mod retry;
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
mod snapshot_preparation;
mod snapshot_reload;
mod snapshot_state;
mod snapshot_write;
mod snapshot_writes;
mod sql;
mod sql_read;
mod sql_value;
mod sqlite;
mod sqlite_settings;
pub use sqlite_settings::{
    AnalysisLimit, AutoVacuum, CacheSize, CacheSpill, CheckpointMode, IncrementalVacuum,
    JournalSizeLimit, PageSize, SecureDelete, SqliteSettings, SqliteSettingsInEffect, TempStore,
    WalAutocheckpoint, WalCheckpoint,
};
mod written_values;
pub use pending::PendingValue;
mod file_protection;
mod source_names;
mod transfers_in_flight;
mod upload;
mod wal_checkpoints;
pub use transfers_in_flight::{InFlight, Transferring};
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
mod write_record;
mod write_rows;
mod write_schema;
mod write_sources;

pub use authorship::{Authorship, HeaderEvidence};
pub use coven_merge::Slot;
pub use coven_merge::WriteId;
pub use database::file_database::{CacheReservation, FileDatabase, PinnedFile};
pub use database::file_retention::FileCandidate;
pub use database::input_database::{
    FileEvidence, InputBudget, RetainedBytes, RetainedInput, RetainedRange, UnprovenFiles,
};
pub use database::{
    discarded_operations, discarded_store_name, read_store_name, Database, DatabaseBuilder,
    DatabaseReadHandle,
};
pub use declaration::{FileDecl, Provenance, RowIdentity, SyncedTable};
pub use download::{ApplyOutcome, WriteWait};
#[cfg(test)]
use download::{DownloadedPart, DownloadedWrite};
pub use download_stream::{DownloadedPartStream, DownloadedWriteStream, WriteAuthorship};
pub use error::{
    CovenError, CovenMigrationError, CovenResult, DbError, MigrationError, SchemaError,
};
pub use file_queue::{FileUpload, UnsentWork, UploadProgress, UploadQueueState};
pub use file_ref::{FileRef, FileRefJournal, LocalFileError, LocalFileStream};
pub use live_query::{
    LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent,
};
pub use lost::{
    ChangeSource, Lost, LostCell, LostValue, RemovalRule, Replacement, RowAuthorizationFailure,
};
pub use members::{Circle, CircleMemberInfo, DeviceInfo, MemberInfo};
pub use migration::{Migration, MigrationChange, MigrationContext, MigrationOutcome, OldWrites};
pub use migration_change::{ChangeOp, ColumnChange, RowChange};
pub use pending_attachment::{AbandonReason, AbandonedAttachment};
pub use read::{Read, SqlReadContext};
pub use row_key::RowKey;
pub use snapshot_error::SnapshotError;
pub use snapshot_reload::{LoadedSnapshot, SnapshotReload, SnapshotSource};
pub use snapshot_write::SnapshotWriteError;
pub use sql_read::SqlRead;
#[cfg(any(test, feature = "test-utils"))]
pub use upload::UploadReadError;
pub use upload::{UploadBytes, UploadParts, WaitingUpload};
pub use user_file::{prepare_user_file, PreparedUserFile, UserFile};
pub use write::SqlContext;
pub use write_batch::{FileSource, WriteBatch};
pub use write_boundary::{Boundary, BoundaryCoverage};
pub use write_failure::WriteFailure;

// SQL values, parameters and callback results are public. Connections remain private.
pub use rusqlite::{
    named_params, params, types, Error as SqliteError, Params, Result as SqliteResult, Row, ToSql,
};

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
#[path = "lib_tests.rs"]
pub(crate) mod tests;

mod operation;
pub use operation::{NewOperation, OperationId, OperationRecord, OperationUpdate};

mod commit_scope;
pub use commit_scope::CommitScope;
mod prerequisite_rows;
pub use prerequisite_rows::{AttemptCommits, PrerequisiteRows};
mod database_changes;
pub use database_changes::DatabaseChanges;

mod captured_changes;
mod claim;
mod published;
mod published_migration;
mod queued_writes;
mod reload_gate;
mod sequence;
mod sequence_apply;
mod snapshot_view;
mod store_state;
mod store_state_tables;
mod write_metadata;
pub use claim::{
    Claim, ClaimAttempt, ClaimBytes, ClaimRead, ClaimSettlement, ClaimUpload, ClaimWork,
};
pub use sequence::WriteCutoff;
pub use sequence_apply::{AdminEffect, IncomingSlot, SlotContent};
pub use store_state::{
    DeviceState, StoreCircle, StoreDevice, StoreIdentity, StoreMember, StoreState, StoreVersion,
};

mod provider_changes;
pub use provider_changes::ProviderChanges;

mod claim_preparation;
mod input_preparation;
