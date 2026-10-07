//! The store's SQLite database, protected internal tables and app migrations.
//!
//! [`DatabaseBuilder`] opens a store with one locked writer and read-only
//! connections, or with read-only connections alone. App SQL runs inside the
//! borrowed [`MigrationContext`] or [`SqlContext`]; no raw connection leaves its
//! owner. A local write commits its app rows, unsigned record and merge metadata
//! together on the writer connection.

mod authorization;
mod change_capture;
mod database;
mod declaration;
mod dismissal;
mod download;
mod download_checks;
mod download_stage;
mod download_stream;
mod error;
mod excluded_write;
mod file_authorization;
mod file_location;
mod file_queue;
mod file_ref;
mod file_removals;
mod file_row;
mod file_write;
mod fingerprint;
mod internal_schema;
mod key_scope;
mod key_upload;
mod live_query;
mod lost;
mod merge_store;
mod migration;
mod migration_change;
mod migration_names;
mod migration_references;
mod migration_run;
mod migration_snapshot;
mod migration_state;
mod migration_writes;
mod observation;
mod read;
mod reference_values;
mod removal;
mod removal_schema;
mod removal_sql;
mod removal_view;
mod row_key;
mod row_queries;
mod schema;
mod schema_source;
mod snapshot_coverage;
mod snapshot_error;
mod snapshot_load;
mod snapshot_loss;
mod snapshot_metadata;
mod snapshot_state;
mod snapshot_write;
mod snapshot_writes;
mod sql;
mod sql_value;
mod sqlite;
mod store_log;
mod store_log_check;
mod store_log_tables;
mod store_log_upload;
mod upload;
mod user_file;
mod waiting_write;
mod write;
mod write_apply;
mod write_batch;
mod write_boundary;
mod write_capture;
mod write_commit;
mod write_encoding;
mod write_failure;
mod write_record;
mod write_rows;
mod write_schema;

pub use coven_format::value::EntryId;
pub use coven_merge::WriteId;
pub use database::file_database::{CacheReservation, FileChanges, FileDatabase, FileReservation};
pub use database::{Database, DatabaseBuilder, DatabaseReadHandle};
pub use declaration::{CacheFill, FileDecl, Provenance, RowIdentity, SyncedTable, Uploads};
pub use download::{ApplyOutcome, DownloadedPart, DownloadedWrite, SyncState, WriteWait};
pub use download_stream::{DownloadedPartStream, DownloadedWriteStream};
pub use error::{
    CovenError, CovenMigrationError, CovenResult, DbError, MigrationError, SchemaError,
};
pub use file_location::FileLocation;
pub use file_queue::{FileUpload, FixedFileUpload};
pub use file_ref::{FileRef, LocalFileError, LocalFileStream};
pub use live_query::{
    LiveQuery, LiveQueryCause, LiveQueryClosed, LiveQueryRequests, LiveQueryRevision,
    ReconfigurableLiveQuery, ReconfigurableLiveQueryEvent,
};
pub use lost::{Lost, LostCell, LostValue, RemovalRule, Replacement};
pub use migration::{
    CovenMigrationPolicy, Migration, MigrationChange, MigrationContext, MigrationOutcome,
};
pub use migration_change::{ChangeOp, ColumnChange, RowChange};
pub use read::{Read, SqlReadContext};
pub use row_key::RowKey;
pub use snapshot_error::SnapshotError;
pub use snapshot_write::SnapshotWriteError;
pub use store_log::{
    DropReason, EntryOutcome, StoreCircle, StoreDevice, StoreIdentity, StoreLog, StoreLogReplay,
    StoreLogState, StoreMember, StoreVersion,
};
pub use store_log_check::{ReplayEntry, StoreLogCheck};
pub use store_log_upload::{LocalStoreLog, SealedStoreLog, StoreLogKeyUpload, StoreLogUpload};
pub use upload::{UploadBytes, UploadParts, UploadReadError, WaitingUpload};
pub use user_file::{prepare_user_file, PreparedUserFile, UserFile};
pub use write::SqlContext;
pub use write_batch::{FileSource, WriteBatch};
pub use write_failure::WriteFailure;

// SQL values and parameters are part of the API. Connections remain private.
pub use rusqlite::{named_params, params, types, Params, Row, ToSql};

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils;

#[cfg(test)]
#[path = "lib_tests.rs"]
mod tests;

mod operation;
pub use operation::{
    AccessKeyToDelete, NewOperation, OperationCommit, OperationId, OperationRecord, OperationUpdate,
};

mod circle_deletion;
