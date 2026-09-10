//! Apply a changeset to the connection, resolving conflicts as each row lands.
//!
//! Prepare column merges in the changeset before applying any live rows. When
//! an incoming UPDATE loses row arbitration, preserve columns it changed from a
//! base the local row still holds, together with the local winning values and
//! timestamp. SQLite applies the combined changeset as one operation, including
//! constraint retries. Other collisions use `arbitrate_row_conflict`.
//!
//! Captured row effects include their synced trigger and foreign-key effects.
//! Native constraint retries must not run those actions again: an UPDATE cycle
//! may temporarily delete and reinsert a parent while its children survive.
//! The production materializer validates foreign keys after its whole atomic
//! replay step; the test wrapper also reports unresolved foreign keys so isolated
//! changeset tests can roll back. The canonical scheduler handles dependencies
//! between changesets. A non-FK
//! constraint conflict marks the whole changeset rejected; the caller rolls its
//! transaction back instead of committing the rows that happened not to conflict.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetItem, ChangesetIter, ConflictAction, ConflictType};
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params_from_iter, Connection, OptionalExtension};
use tracing::warn;

use super::conflict::{
    arbitrate_row_conflict, compare_lww_stamps, IncomingTimestampPolicy, LwwComparison, TableSchema,
};
use crate::changeset::{value_ref_to_string, UpdateValue};
use crate::changeset_identity::validate_changeset_row_identities;
use crate::store::store_session::replay_sql::ReplaySql;
use crate::{quote_ident, ChangesetIdentityError, DbError};
use coven_protocol::hlc::Timestamp;
#[cfg(any(test, feature = "test-utils"))]
use coven_protocol::synced_schema::SyncedTable;

use super::MergeMaterializationTransaction;

#[path = "column_merge.rs"]
mod column_merge;
use column_merge::ColumnMergeEncoder;

#[path = "recorded_changeset.rs"]
mod recorded_changeset;
#[cfg(test)]
#[path = "recorded_changeset_tests.rs"]
mod recorded_changeset_tests;
#[cfg(test)]
#[path = "three_way_tests.rs"]
mod three_way_tests;

/// Result of applying a changeset.
#[cfg_attr(
    not(any(test, feature = "test-utils")),
    allow(unreachable_pub),
    doc = "Public when the `test-utils` feature exposes changeset application."
)]
pub struct ApplyResult {
    /// True if the resulting database has unresolved foreign keys. The caller
    /// may retry after applying changesets that contain the missing parent
    /// rows.
    #[cfg(any(test, feature = "test-utils"))]
    pub had_fk_violations: bool,
    /// Tables that hit non-retryable SQLite constraint conflicts. The caller must
    /// roll back the transaction when this is non-empty.
    pub constraint_conflict_tables: Vec<String>,
    /// Incoming rows whose exact row value won arbitration. A missing stamp
    /// identifies a winning deletion.
    #[cfg(any(test, feature = "test-utils"))]
    pub winning_rows: Vec<WinningRow>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WinningRow {
    pub table: String,
    pub row_id: String,
    pub row_stamp: Option<String>,
}

#[derive(Clone, Debug)]
struct IncomingRow {
    table: String,
    row_id: String,
    row_stamp: Option<String>,
}

/// Changeset bytes paired with the exact synced schema that validated their row
/// identities. Construction is the one identity-validation boundary; apply only
/// accepts this type, so callers cannot parse once for classification and then
/// parse the same bytes again during mutation.
pub struct ValidatedChangeset<B> {
    bytes: B,
    schema: Arc<TableSchema>,
}

impl<B: AsRef<[u8]>> ValidatedChangeset<B> {
    pub fn new(bytes: B, schema: Arc<TableSchema>) -> Result<Self, ChangesetIdentityError> {
        validate_changeset_row_identities(bytes.as_ref(), schema.synced_tables())?;
        Ok(Self { bytes, schema })
    }

    pub fn bytes(&self) -> &[u8] {
        self.bytes.as_ref()
    }

    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    pub fn validate_subset<C: AsRef<[u8]>>(
        &self,
        bytes: C,
    ) -> Result<ValidatedChangeset<C>, ChangesetIdentityError> {
        ValidatedChangeset::new(bytes, self.schema.clone())
    }
}

/// Apply `bytes` to `conn`, resolving column and row conflicts,
/// building the [`TableSchema`] from `tables` once. A convenience wrapper over
/// `resolve_and_apply_changeset_with_schema` for callers that apply a single
/// changeset and don't already hold a schema (tests, snapshot round-trips).
///
/// `receiver_wall_ms` is the receiver's current wall-clock millis, against which
/// a grossly-future incoming `_updated_at` is refused (see `arbitrate_row_conflict`).
#[cfg(any(test, feature = "test-utils"))]
pub fn resolve_and_apply_changeset(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    bytes: &[u8],
    tables: &[SyncedTable],
    receiver_wall_ms: u64,
) -> Result<ApplyResult, DbError> {
    let schema = Arc::new(TableSchema::from_db(conn, tables)?);
    resolve_and_apply_changeset_with_schema(conn, store_dir, bytes, schema, receiver_wall_ms)
}

/// Apply `bytes` to `conn`, resolving conflicts against a pre-built
/// [`TableSchema`]: prepare losing UPDATE column merges in the changeset, then
/// apply once with a conflict closure for remaining row collisions.
///
/// The schema's per-table `_updated_at` column index map is derived once (from
/// the live schema, so future migrations that add columns are safe) and reused
/// across every changeset in a pull, rather than re-querying `PRAGMA table_info`
/// per changeset. The conflict closure resolves each conflicting row's table from
/// its operation and decides REPLACE/OMIT by comparing `_updated_at`;
/// Non-FK constraint conflicts are collected so the caller can surface the
/// rejected changeset; unresolved foreign keys are checked after application.
///
/// `schema` is an `Arc` so the same map moves into the `'static` conflict closure
/// without re-deriving it per call. `receiver_wall_ms` is the receiver's current
/// wall-clock millis, read once by the caller and moved into the closure to bound
/// a grossly-future incoming `_updated_at` (see `arbitrate_row_conflict`).
#[cfg(any(test, feature = "test-utils"))]
pub(crate) fn resolve_and_apply_changeset_with_schema(
    conn: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    bytes: &[u8],
    schema: Arc<TableSchema>,
    receiver_wall_ms: u64,
) -> Result<ApplyResult, DbError> {
    let changeset = ValidatedChangeset::new(bytes, schema).map_err(DbError::from)?;
    let tx = conn.unchecked_transaction().map_err(DbError::from)?;
    let result = MergeMaterializationTransaction::from_store(
        crate::store::store_session::StoreTransaction::new(&tx, store_dir),
    )
    .apply_changeset(
        changeset,
        IncomingTimestampPolicy::Received { receiver_wall_ms },
    )?;
    if result.had_fk_violations || !result.constraint_conflict_tables.is_empty() {
        tx.rollback().map_err(DbError::from)?;
    } else {
        tx.commit().map_err(DbError::from)?;
    }
    Ok(result)
}

impl MergeMaterializationTransaction<'_, '_> {
    pub(crate) fn apply_changeset<B: AsRef<[u8]>>(
        &self,
        changeset: ValidatedChangeset<B>,
        timestamp_policy: IncomingTimestampPolicy,
    ) -> Result<ApplyResult, DbError> {
        let conn = self.store.transaction;
        let ValidatedChangeset { bytes, schema } = changeset;
        let bytes = bytes.as_ref();
        #[cfg(any(test, feature = "test-utils"))]
        let incoming_rows = incoming_rows(bytes, &schema)?;

        let constraint_conflict_tables = Arc::new(Mutex::new(Vec::new()));
        let (prepared_bytes, merged_updates) =
            prepare_column_merges(conn, bytes, &schema, timestamp_policy)?;

        let closure_constraint_conflict_tables = constraint_conflict_tables.clone();
        let closure_schema = schema.clone();
        ReplaySql::begin(conn)?.run(|| {
            conn.apply_strm(
                &mut prepared_bytes.as_ref(),
                None::<fn(&str) -> bool>,
                move |conflict_type, item| {
                    // A FOREIGN_KEY conflict's iterator supports ONLY `fk_conflicts()`;
                    // calling `op()`/`new_value()`/`conflict()` on it is undefined (it
                    // crashes the process). Resolve it first, without touching the row.
                    if conflict_type == ConflictType::SQLITE_CHANGESET_FOREIGN_KEY {
                        return ConflictAction::SQLITE_CHANGESET_OMIT;
                    }
                    // Every other conflict type exposes the operation, so the table name
                    // (needed to find the `_updated_at` column) is readable.
                    let (table, op_code) = match item.op() {
                        Ok(op) => (op.table_name().to_string(), op.code()),
                        Err(error) => {
                            warn!(error = %error, "failed to read changeset conflict operation; aborting apply");
                            return ConflictAction::SQLITE_CHANGESET_ABORT;
                        }
                    };
                    if conflict_type == ConflictType::SQLITE_CHANGESET_CONSTRAINT {
                        warn!(
                            table = %table,
                            "changeset hit a non-retryable SQLite constraint conflict; rejecting changeset"
                        );
                        match closure_constraint_conflict_tables.lock() {
                            Ok(mut tables) => tables.push(table),
                            Err(error) => {
                                warn!(error = %error, "failed to record changeset constraint conflict; aborting apply");
                                return ConflictAction::SQLITE_CHANGESET_ABORT;
                            }
                        }
                        return ConflictAction::SQLITE_CHANGESET_OMIT;
                    }
                    if conflict_type == ConflictType::SQLITE_CHANGESET_DATA
                        && op_code == Action::SQLITE_UPDATE
                    {
                        match update_pk_key(&item, &table).map(|pk| {
                            merged_updates.contains(&RowKey {
                                table: table.clone(),
                                pk,
                            })
                        }) {
                            Ok(true) => return ConflictAction::SQLITE_CHANGESET_REPLACE,
                            Ok(false) => {}
                            Err(error) => {
                                warn!(table, error = %error, "failed to read merged UPDATE primary key; aborting apply");
                                return ConflictAction::SQLITE_CHANGESET_ABORT;
                            }
                        }
                    }
                    arbitrate_row_conflict(
                        conflict_type,
                        item,
                        &table,
                        &closure_schema,
                        timestamp_policy,
                    )
                },
            )
            .map_err(DbError::from)
        })?;
        #[cfg(any(test, feature = "test-utils"))]
        let had_fk_violations = self.has_foreign_key_violations()?;
        let constraint_conflict_tables = constraint_conflict_tables
            .lock()
            .map_err(|_| {
                DbError::Message("constraint conflict table collection is poisoned".to_string())
            })?
            .clone();
        #[cfg(any(test, feature = "test-utils"))]
        let winning_rows = resolve_winning_rows(conn, &schema, incoming_rows)?;

        Ok(ApplyResult {
            #[cfg(any(test, feature = "test-utils"))]
            had_fk_violations,
            constraint_conflict_tables,
            #[cfg(any(test, feature = "test-utils"))]
            winning_rows,
        })
    }

    pub(crate) fn current_winning_rows<B: AsRef<[u8]>>(
        &self,
        schema: &TableSchema,
        changeset: B,
    ) -> Result<Vec<WinningRow>, DbError> {
        resolve_winning_rows(
            self.store.transaction,
            schema,
            incoming_rows(changeset.as_ref(), schema)?,
        )
    }

    pub(crate) fn apply_changeset_strict<B: AsRef<[u8]>>(
        &self,
        changeset: ValidatedChangeset<B>,
        blob_decls: &crate::BlobDecls,
    ) -> Result<(), DbError> {
        let bytes = changeset.bytes();
        let old_changes = crate::walk_old_changeset(bytes).map_err(DbError::Changeset)?;
        let new_changes = crate::walk_changeset(bytes).map_err(DbError::Changeset)?;
        let old_exact_bindings = super::exact_blob_bindings_on(self.store.transaction)?;
        let obsolete = crate::local_blob_cleanup_intents::intents_from_changes(
            blob_decls,
            &old_changes,
            &new_changes,
        )?;
        self.store
            .transaction
            .apply_strm(
                &mut &bytes[..],
                None::<fn(&str) -> bool>,
                |_conflict_type, _item| ConflictAction::SQLITE_CHANGESET_ABORT,
            )
            .map_err(DbError::from)?;
        for intent in obsolete {
            super::record_obsolete_copy_intents_from_bindings_on(
                self.store.transaction,
                blob_decls,
                &intent,
                &old_exact_bindings,
            )?;
        }
        Ok(())
    }
}

fn incoming_rows(bytes: &[u8], schema: &TableSchema) -> Result<Vec<IncomingRow>, DbError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let input: &mut dyn std::io::Read = &mut &bytes[..];
    let mut iter = ChangesetIter::start_strm(&input).map_err(DbError::from)?;
    let mut rows = Vec::new();
    while let Some(item) = iter.next().map_err(DbError::from)? {
        let op = item.op().map_err(DbError::from)?;
        let table = op.table_name();
        let updated_at = schema.updated_at(table).ok_or_else(|| {
            DbError::Message(format!("changeset contains undeclared table {table:?}"))
        })?;
        let (id_side, stamp_side) = match op.code() {
            Action::SQLITE_INSERT => (UpdateValue::New, Some(UpdateValue::New)),
            Action::SQLITE_UPDATE => (UpdateValue::Old, Some(UpdateValue::New)),
            Action::SQLITE_DELETE => (UpdateValue::Old, None),
            code => {
                return Err(DbError::Message(format!(
                    "changeset for {table:?} contains unsupported operation {code:?}"
                )));
            }
        };
        let row_id = required_text_changeset_value(item, table, 0, id_side, "row id")?;
        let row_stamp = stamp_side
            .map(|side| required_text_changeset_value(item, table, updated_at, side, "row stamp"))
            .transpose()?;
        rows.push(IncomingRow {
            table: table.to_string(),
            row_id,
            row_stamp,
        });
    }
    Ok(rows)
}

fn required_text_changeset_value(
    item: &ChangesetItem,
    table: &str,
    column: usize,
    side: UpdateValue,
    field: &str,
) -> Result<String, DbError> {
    let value = changeset_value(item, column, side)?.ok_or_else(|| {
        DbError::Message(format!("changeset for {table:?} has no {side:?} {field}"))
    })?;
    let Value::Text(value) = value else {
        return Err(DbError::Message(format!(
            "changeset for {table:?} has non-TEXT {side:?} {field}"
        )));
    };
    Ok(value)
}

fn resolve_winning_rows(
    conn: &Connection,
    schema: &TableSchema,
    incoming: Vec<IncomingRow>,
) -> Result<Vec<WinningRow>, DbError> {
    let mut winners = Vec::new();
    for row in incoming {
        let columns = schema.columns(&row.table).ok_or_else(|| {
            DbError::Message(format!("synced table {:?} has no column map", row.table))
        })?;
        let updated_at = schema.updated_at(&row.table).ok_or_else(|| {
            DbError::Message(format!(
                "synced table {:?} has no _updated_at column index",
                row.table
            ))
        })?;
        let sql = format!(
            "SELECT {} FROM {} WHERE {} = ?1",
            quote_ident(&columns[updated_at]),
            quote_ident(&row.table),
            quote_ident(&columns[0])
        );
        let live_stamp = conn
            .query_row(&sql, [&row.row_id], |result| result.get::<_, String>(0))
            .optional()
            .map_err(DbError::from)?;
        let incoming_won = match (&row.row_stamp, &live_stamp) {
            (None, None) => true,
            (Some(expected), Some(actual)) => expected == actual,
            _ => false,
        };
        if incoming_won {
            winners.push(WinningRow {
                table: row.table,
                row_id: row.row_id,
                row_stamp: row.row_stamp,
            });
        }
    }
    Ok(winners)
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct RowKey {
    table: String,
    pk: String,
}

struct UpdateColumn {
    index: usize,
    base: Value,
    incoming: Value,
}

struct IncomingUpdate {
    table: String,
    pk: String,
    columns: Vec<UpdateColumn>,
    incoming_updated_at: Timestamp,
    incoming_updated_at_value: Value,
}

fn prepare_column_merges<'bytes>(
    conn: &Connection,
    bytes: &'bytes [u8],
    schema: &TableSchema,
    timestamp_policy: IncomingTimestampPolicy,
) -> Result<(Cow<'bytes, [u8]>, HashSet<RowKey>), DbError> {
    if bytes.is_empty() {
        return Ok((Cow::Borrowed(bytes), HashSet::new()));
    }
    let input: &mut dyn std::io::Read = &mut &bytes[..];
    let mut iter = ChangesetIter::start_strm(&input)?;
    let mut handled = HashSet::new();
    let mut encoder = None;
    while let Some(item) = iter.next()? {
        let Some(update) = incoming_update(item, schema)? else {
            continue;
        };
        if prepare_losing_update(
            conn,
            schema,
            &update,
            timestamp_policy,
            item.op()?.indirect(),
            &mut encoder,
            bytes,
        )? {
            handled.insert(RowKey {
                table: update.table,
                pk: update.pk,
            });
        }
    }
    let prepared = match encoder {
        Some(encoder) => Cow::Owned(encoder.finish()?),
        None => Cow::Borrowed(bytes),
    };
    Ok((prepared, handled))
}

fn incoming_update(
    item: &ChangesetItem,
    schema: &TableSchema,
) -> Result<Option<IncomingUpdate>, DbError> {
    let op = item.op().map_err(DbError::from)?;
    if op.code() != Action::SQLITE_UPDATE {
        return Ok(None);
    }

    let table = op.table_name();
    let Some(updated_at) = schema.updated_at(table) else {
        warn!(
            table,
            "UPDATE changeset table is not in the local synced schema"
        );
        return Ok(None);
    };

    let Some(incoming_updated_at_value) = changeset_value(item, updated_at, UpdateValue::New)?
    else {
        warn!(table, "UPDATE changeset has no incoming _updated_at value");
        return Ok(None);
    };
    let Some(incoming_updated_at) = timestamp_from_value(&incoming_updated_at_value) else {
        warn!(
            table,
            "UPDATE changeset has an incoming _updated_at value that does not parse"
        );
        return Ok(None);
    };

    let pk = update_pk_key(item, table)?;

    let columns = update_columns(item, updated_at)?;
    if let Some(blob) = schema.blob_columns(table) {
        let edits_blob = columns.iter().any(|column| {
            blob.iter().any(|index| index == column.index) && column.base != column.incoming
        });
        if edits_blob {
            for index in blob.iter().filter(|index| *index != 0) {
                if !columns.iter().any(|column| column.index == index) {
                    return Err(DbError::Message(format!(
                        "blob UPDATE for {table} omits content column {index}"
                    )));
                }
            }
        }
    }

    Ok(Some(IncomingUpdate {
        table: table.to_string(),
        pk,
        columns,
        incoming_updated_at,
        incoming_updated_at_value,
    }))
}

fn update_columns(item: &ChangesetItem, updated_at: usize) -> Result<Vec<UpdateColumn>, DbError> {
    let op = item.op()?;
    let table = op.table_name();
    let mut columns = Vec::new();
    for index in 0..op.number_of_columns() as usize {
        if index == 0 || index == updated_at {
            continue;
        }
        let base = changeset_value(item, index, UpdateValue::Old)?;
        let incoming = changeset_value(item, index, UpdateValue::New)?;
        match (base, incoming) {
            (Some(base), Some(incoming)) => columns.push(UpdateColumn {
                index,
                base,
                incoming,
            }),
            (None, None) => {}
            _ => {
                return Err(DbError::Message(format!(
                    "UPDATE changeset for {table} has only one side for column {index}"
                )));
            }
        }
    }

    Ok(columns)
}

fn prepare_losing_update(
    conn: &Connection,
    schema: &TableSchema,
    update: &IncomingUpdate,
    timestamp_policy: IncomingTimestampPolicy,
    indirect: bool,
    encoder: &mut Option<ColumnMergeEncoder>,
    bytes: &[u8],
) -> Result<bool, DbError> {
    let columns = schema.columns(&update.table).ok_or_else(|| {
        DbError::Message(format!("synced table {} has no column map", update.table))
    })?;
    let updated_at = schema.updated_at(&update.table).ok_or_else(|| {
        DbError::Message(format!(
            "synced table {} has no _updated_at column",
            update.table
        ))
    })?;
    if update.columns.iter().any(|c| c.index >= columns.len()) || updated_at >= columns.len() {
        return Err(DbError::Message(format!(
            "UPDATE changeset for {} names a column outside the local schema",
            update.table
        )));
    }
    let sql = format!(
        "SELECT {} FROM {} WHERE {} = ?1",
        columns
            .iter()
            .map(|column| quote_ident(column))
            .collect::<Vec<_>>()
            .join(", "),
        quote_ident(&update.table),
        quote_ident(&columns[0]),
    );
    let local = conn
        .query_row(&sql, [&update.pk], |row| {
            (0..columns.len())
                .map(|index| row.get::<_, Value>(index))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .optional()?;
    let Some(local) = local else { return Ok(false) };
    let local_stamp = timestamp_from_value(&local[updated_at]).ok_or_else(|| {
        DbError::Message(format!(
            "local row in {} has no parseable _updated_at",
            update.table
        ))
    })?;
    match compare_lww_stamps(
        &update.table,
        update.incoming_updated_at.clone(),
        local_stamp,
        timestamp_policy,
    ) {
        LwwComparison::IncomingWins | LwwComparison::IncomingGrossFuture => return Ok(false),
        LwwComparison::LocalWins => {}
    }
    // Encode incoming -> merged as a correction to the original changeset. The
    // live rows stay untouched until SQLite applies the combined changeset, so
    // its constraint retry sees every update and deletion in the same operation.
    let mut incoming = local.clone();
    let mut merged = local.clone();
    incoming[updated_at] = update.incoming_updated_at_value.clone();
    let blob = schema.blob_columns(&update.table);
    // Content fields describe one value: an older edit can replace them only
    // while the whole value still matches its captured base.
    let merge_blob = blob.is_none_or(|blob| {
        update
            .columns
            .iter()
            .filter(|column| blob.iter().any(|index| index == column.index))
            .all(|column| local[column.index] == column.base)
    });
    for column in &update.columns {
        incoming[column.index] = column.incoming.clone();
        let belongs_to_blob =
            blob.is_some_and(|blob| blob.iter().any(|index| index == column.index));
        if (belongs_to_blob && merge_blob)
            || (!belongs_to_blob && local[column.index] == column.base)
        {
            merged[column.index] = column.incoming.clone();
        }
    }
    if merged == local {
        return Ok(false);
    }
    let encoder = match encoder {
        Some(encoder) => encoder,
        slot @ None => slot.insert(ColumnMergeEncoder::new(bytes)?),
    };
    encoder.record(&update.table, columns, &incoming, &merged, indirect)?;
    Ok(true)
}

fn changeset_value(
    item: &ChangesetItem,
    column: usize,
    side: UpdateValue,
) -> Result<Option<Value>, DbError> {
    let value = match side {
        UpdateValue::Old => item.old_value(column),
        UpdateValue::New => item.new_value(column),
    };
    match value {
        Ok(value) => Value::try_from(value).map(Some).map_err(|error| {
            DbError::context(
                format!("changeset {side:?} value conversion failed for column {column}"),
                error,
            )
        }),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Ok(None),
        Err(error) => Err(DbError::context(
            format!("changeset {side:?} value read failed for column {column}"),
            error,
        )),
    }
}

fn update_pk_key(item: &ChangesetItem, table: &str) -> Result<String, DbError> {
    match item.old_value(0) {
        Ok(value) => text_id_from_value_ref(table, value),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Err(DbError::Message(format!(
            "UPDATE changeset for {table} has no old-side primary key"
        ))),
        Err(error) => Err(DbError::context(
            format!("UPDATE changeset for {table} primary key read failed"),
            error,
        )),
    }
}

fn text_id_from_value_ref(table: &str, value: ValueRef<'_>) -> Result<String, DbError> {
    let ValueRef::Text(bytes) = value else {
        return Err(DbError::Message(format!(
            "UPDATE changeset for {table} primary key is not TEXT"
        )));
    };
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|error| {
            DbError::context(
                format!("UPDATE changeset for {table} primary key is not UTF-8"),
                error,
            )
        })
}

fn timestamp_from_value(value: &Value) -> Option<Timestamp> {
    value_ref_to_string(ValueRef::from(value)).and_then(|s| Timestamp::parse(&s))
}
