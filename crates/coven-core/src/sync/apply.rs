//! Apply a changeset to the connection, resolving conflicts as each row lands.
//!
//! Two stages. First a column-level three-way premerge
//! ([`premerge_losing_update_columns`]): when an incoming UPDATE loses row
//! arbitration, the columns it moved away from a base the local row still holds
//! are folded into the local row, so concurrent edits to *different* columns of
//! one row both survive. Then the changeset is applied with
//! [`arbitrate_row_conflict`] as the conflict handler, which picks the winning row
//! by `_updated_at` (remove-wins for deletes, a future-skew bound on the
//! comparison) for every collision the premerge did not already fold in.
//!
//! Within a single changeset, SQLite defers FK checks — parent and child rows in
//! the same changeset are applied in recording order. Cross-changeset FK
//! dependencies are handled by applying changesets in seq order (parents are
//! always in earlier changesets than children).
//!
//! If a FK violation remains after applying a changeset, the conflict handler
//! reports it via `FOREIGN_KEY` and the returned flag notes it for the caller,
//! which retries the changeset once its parents have landed. A non-FK constraint
//! conflict marks the whole changeset rejected; the caller rolls its transaction
//! back instead of committing the rows that happened not to conflict.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use fallible_streaming_iterator::FallibleStreamingIterator;
use rusqlite::hooks::Action;
use rusqlite::session::{ChangesetItem, ChangesetIter, ConflictAction, ConflictType};
use rusqlite::types::{Value, ValueRef};
use rusqlite::{params_from_iter, Connection, OptionalExtension, ToSql};
use tracing::warn;

use super::conflict::{arbitrate_row_conflict, compare_lww_stamps, LwwComparison, TableSchema};
use super::hlc::Timestamp;
use super::session::quote_ident;
#[cfg(any(test, feature = "test-utils"))]
use super::session::SyncedTable;
use crate::changeset::{value_ref_to_string, UpdateValue};
use crate::database::DbError;

/// Result of applying a changeset.
pub struct ApplyResult {
    /// True if any FK violations were reported. The caller may retry this
    /// changeset after applying other changesets that contain the missing parent
    /// rows.
    pub had_fk_violations: bool,
    /// Tables that hit non-retryable SQLite constraint conflicts. The caller must
    /// roll back the transaction when this is non-empty.
    pub constraint_conflict_tables: Vec<String>,
}

/// Apply `bytes` to `conn`, resolving conflicts (premerge + row arbitration),
/// building the [`TableSchema`] from `tables` once. A convenience wrapper over
/// [`resolve_and_apply_changeset_with_schema`] for callers that apply a single
/// changeset and don't already hold a schema (tests, snapshot round-trips).
///
/// `receiver_wall_ms` is the receiver's current wall-clock millis, against which
/// a grossly-future incoming `_updated_at` is refused (see [`arbitrate_row_conflict`]).
#[cfg(any(test, feature = "test-utils"))]
pub fn resolve_and_apply_changeset(
    conn: &Connection,
    bytes: &[u8],
    tables: &[SyncedTable],
    receiver_wall_ms: u64,
) -> Result<ApplyResult, DbError> {
    let table_refs: Vec<&str> = tables.iter().map(|t| t.name()).collect();
    let schema = Arc::new(TableSchema::from_db(conn, &table_refs)?);
    resolve_and_apply_changeset_with_schema(conn, bytes, schema, receiver_wall_ms, &[])
}

/// Apply `bytes` to `conn`, resolving conflicts against a pre-built
/// [`TableSchema`]: a column-level premerge of losing UPDATEs
/// ([`premerge_losing_update_columns`]) followed by an apply whose conflict
/// closure arbitrates every remaining row collision.
///
/// The schema's per-table `_updated_at` column index map is derived once (from
/// the live schema, so future migrations that add columns are safe) and reused
/// across every changeset in a pull, rather than re-querying `PRAGMA table_info`
/// per changeset. The conflict closure resolves each conflicting row's table from
/// its operation and decides REPLACE/OMIT by comparing `_updated_at`;
/// FK violations flip a shared flag for the caller to retry; non-FK constraint
/// conflicts are collected so the caller can surface the dropped rows.
///
/// `schema` is an `Arc` so the same map moves into the `'static` conflict closure
/// without re-deriving it per call. `receiver_wall_ms` is the receiver's current
/// wall-clock millis, read once by the caller and moved into the closure to bound
/// a grossly-future incoming `_updated_at` (see [`arbitrate_row_conflict`]).
/// `blob_uploads` records, atomically with the applied rows, which device uploaded
/// each blob the changeset introduces (`(namespace, blob_id, uploader)`): the read
/// dispatch later keys a blob under its uploader's cloud prefix. Writing it inside
/// this transaction is what keeps the index consistent with the rows that reference
/// the blobs — a committed row always has its uploader recorded, never a later
/// repair. Callers with no blobs to record (a test, a snapshot round-trip) pass an
/// empty slice.
pub fn resolve_and_apply_changeset_with_schema(
    conn: &Connection,
    bytes: &[u8],
    schema: Arc<TableSchema>,
    receiver_wall_ms: u64,
    blob_uploads: &[(String, String, String)],
) -> Result<ApplyResult, DbError> {
    let tx = conn.unchecked_transaction().map_err(DbError::from)?;
    let result = resolve_and_apply_changeset_with_schema_on(
        &tx,
        bytes,
        schema,
        receiver_wall_ms,
        blob_uploads,
    )?;
    if result.had_fk_violations || !result.constraint_conflict_tables.is_empty() {
        tx.rollback().map_err(DbError::from)?;
    } else {
        tx.commit().map_err(DbError::from)?;
    }
    Ok(result)
}

/// Apply one changeset on a connection whose transaction boundary belongs to the
/// caller. Rows, blob-uploader records, durable cleanup intents, and the receiver's
/// cursor can therefore commit as one database operation. The caller must roll
/// back when the returned result reports an FK or non-FK constraint conflict.
pub(crate) fn resolve_and_apply_changeset_with_schema_on(
    conn: &Connection,
    bytes: &[u8],
    schema: Arc<TableSchema>,
    receiver_wall_ms: u64,
    blob_uploads: &[(String, String, String)],
) -> Result<ApplyResult, DbError> {
    validate_changeset_tables(bytes, &schema)?;

    let fk_flag = Arc::new(AtomicBool::new(false));
    let constraint_conflict_tables = Arc::new(Mutex::new(Vec::new()));
    let premerged_updates = premerge_losing_update_columns(conn, bytes, &schema, receiver_wall_ms)?;

    let closure_flag = fk_flag.clone();
    let closure_constraint_conflict_tables = constraint_conflict_tables.clone();
    conn.apply_strm(
        &mut &bytes[..],
        None::<fn(&str) -> bool>,
        move |conflict_type, item| {
            // A FOREIGN_KEY conflict's iterator supports ONLY `fk_conflicts()`;
            // calling `op()`/`new_value()`/`conflict()` on it is undefined (it
            // crashes the process). Resolve it first, without touching the row.
            if conflict_type == ConflictType::SQLITE_CHANGESET_FOREIGN_KEY {
                closure_flag.store(true, Ordering::Relaxed);
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
                match is_premerged_update(&item, &table, &premerged_updates) {
                    Ok(true) => return ConflictAction::SQLITE_CHANGESET_OMIT,
                    Ok(false) => {}
                    Err(error) => {
                        warn!(table, error = %error, "failed to read premerged UPDATE primary key; aborting apply");
                        return ConflictAction::SQLITE_CHANGESET_ABORT;
                    }
                }
            }
            arbitrate_row_conflict(conflict_type, item, &table, &schema, receiver_wall_ms)
        },
    )
    .map_err(DbError::from)?;
    let had_fk_violations = fk_flag.load(Ordering::Relaxed);
    if !had_fk_violations {
        // Record the uploader of each blob these rows introduce on the caller's
        // transaction, so a committed blob-bearing row always carries its
        // uploader. The caller rolls these records back with the rows on any
        // rejected apply; a deferred retry re-records the same fact idempotently.
        for (namespace, blob_id, uploader) in blob_uploads {
            crate::database::Database::record_blob_uploader_on(conn, namespace, blob_id, uploader)?;
        }
    }

    let constraint_conflict_tables = constraint_conflict_tables
        .lock()
        .map_err(|error| {
            DbError(format!(
                "constraint conflict table collection poisoned: {error}"
            ))
        })?
        .clone();

    Ok(ApplyResult {
        had_fk_violations,
        constraint_conflict_tables,
    })
}

fn validate_changeset_tables(bytes: &[u8], schema: &TableSchema) -> Result<(), DbError> {
    if bytes.is_empty() {
        return Ok(());
    }

    let input: &mut dyn std::io::Read = &mut &bytes[..];
    let mut iter = ChangesetIter::start_strm(&input).map_err(DbError::from)?;
    while let Some(item) = iter.next().map_err(DbError::from)? {
        let op = item.op().map_err(DbError::from)?;
        let table = op.table_name();
        if schema.columns(table).is_none() {
            return Err(DbError(format!(
                "changeset contains undeclared table {table}"
            )));
        }
    }

    Ok(())
}

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
struct RowKey {
    table: String,
    pk: String,
}

struct ChangedColumn {
    index: usize,
    base: Value,
    incoming: Value,
}

struct IncomingUpdate {
    table: String,
    pk: String,
    changed_columns: Vec<ChangedColumn>,
    incoming_updated_at: Timestamp,
}

fn premerge_losing_update_columns(
    conn: &Connection,
    bytes: &[u8],
    schema: &TableSchema,
    receiver_wall_ms: u64,
) -> Result<HashSet<RowKey>, DbError> {
    if bytes.is_empty() {
        return Ok(HashSet::new());
    }

    let input: &mut dyn std::io::Read = &mut &bytes[..];
    let mut iter = ChangesetIter::start_strm(&input).map_err(DbError::from)?;
    let mut handled = HashSet::new();

    while let Some(item) = iter.next().map_err(DbError::from)? {
        let Some(update) = incoming_update(item, schema)? else {
            continue;
        };
        if merge_losing_update(conn, schema, &update, receiver_wall_ms)? {
            handled.insert(RowKey {
                table: update.table,
                pk: update.pk,
            });
        }
    }

    Ok(handled)
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

    let pk = update_pk_value(item, table)?;

    let mut changed_columns = Vec::new();
    for index in 0..op.number_of_columns() as usize {
        if index == 0 || index == updated_at {
            continue;
        }
        let base = changeset_value(item, index, UpdateValue::Old)?;
        let incoming = changeset_value(item, index, UpdateValue::New)?;
        match (base, incoming) {
            (Some(base), Some(incoming)) => changed_columns.push(ChangedColumn {
                index,
                base,
                incoming,
            }),
            (None, None) => {}
            _ => {
                return Err(DbError(format!(
                    "UPDATE changeset for {table} has only one side for column {index}"
                )));
            }
        }
    }

    Ok(Some(IncomingUpdate {
        table: table.to_string(),
        pk,
        changed_columns,
        incoming_updated_at,
    }))
}

fn merge_losing_update(
    conn: &Connection,
    schema: &TableSchema,
    update: &IncomingUpdate,
    receiver_wall_ms: u64,
) -> Result<bool, DbError> {
    let columns = schema
        .columns(&update.table)
        .ok_or_else(|| DbError(format!("synced table {} has no column map", update.table)))?;
    let updated_at_index = schema.updated_at(&update.table).ok_or_else(|| {
        DbError(format!(
            "synced table {} has no _updated_at column index",
            update.table
        ))
    })?;
    if update
        .changed_columns
        .iter()
        .any(|c| c.index >= columns.len())
        || updated_at_index >= columns.len()
    {
        return Err(DbError(format!(
            "UPDATE changeset for {} names a column outside the local schema",
            update.table
        )));
    }

    let mut selected_indices = update
        .changed_columns
        .iter()
        .map(|column| column.index)
        .collect::<Vec<_>>();
    selected_indices.push(updated_at_index);
    let select_columns = selected_indices
        .iter()
        .map(|index| quote_ident(&columns[*index]))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT {select_columns} FROM {} WHERE {} = ?1",
        quote_ident(&update.table),
        quote_ident(&columns[0])
    );
    let local_values = conn
        .query_row(&sql, rusqlite::params![&update.pk], |row| {
            (0..selected_indices.len())
                .map(|index| row.get::<_, Value>(index))
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .optional()
        .map_err(DbError::from)?;
    let Some(local_values) = local_values else {
        return Ok(false);
    };

    let local_updated_at = local_values
        .last()
        .and_then(timestamp_from_value)
        .ok_or_else(|| {
            DbError(format!(
                "local row in {} has no parseable _updated_at",
                update.table
            ))
        })?;
    match compare_lww_stamps(
        &update.table,
        update.incoming_updated_at.clone(),
        local_updated_at,
        receiver_wall_ms,
    ) {
        LwwComparison::IncomingWins | LwwComparison::IncomingGrossFuture => return Ok(false),
        LwwComparison::LocalWins => {}
    }

    let mut applied = Vec::new();
    for (column, local_value) in update.changed_columns.iter().zip(local_values.iter()) {
        if *local_value == column.base {
            applied.push(column);
        }
    }
    if !applied.is_empty() {
        // Fold the losing writer's columns into the local row WITHOUT bumping its
        // `_updated_at`. The local row won row arbitration, so its stamp already
        // dominates the incoming one; re-stamping the merged row could only lower
        // it, and the row winner's stamp is what future arbitration must compare
        // against. The merge changes a losing column's value, not the row's clock.
        apply_columns(conn, columns, update, &applied)?;
    }

    Ok(true)
}

fn apply_columns(
    conn: &Connection,
    columns: &[String],
    update: &IncomingUpdate,
    applied: &[&ChangedColumn],
) -> Result<(), DbError> {
    let assignments = applied
        .iter()
        .enumerate()
        .map(|(offset, column)| {
            format!("{} = ?{}", quote_ident(&columns[column.index]), offset + 1)
        })
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "UPDATE {} SET {assignments} WHERE {} = ?{}",
        quote_ident(&update.table),
        quote_ident(&columns[0]),
        applied.len() + 1
    );

    let mut params: Vec<&dyn ToSql> = applied
        .iter()
        .map(|column| &column.incoming as &dyn ToSql)
        .collect();
    params.push(&update.pk);
    conn.execute(&sql, params_from_iter(params))
        .map(|_| ())
        .map_err(DbError::from)
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
            DbError(format!(
                "changeset {side:?} value conversion failed for column {column}: {error}"
            ))
        }),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Ok(None),
        Err(error) => Err(DbError(format!(
            "changeset {side:?} value read failed for column {column}: {error}"
        ))),
    }
}

fn update_pk_value(item: &ChangesetItem, table: &str) -> Result<String, DbError> {
    update_pk_key(item, table).map_err(DbError)
}

fn is_premerged_update(
    item: &ChangesetItem,
    table: &str,
    premerged: &HashSet<RowKey>,
) -> Result<bool, String> {
    let pk = update_pk_key(item, table)?;
    Ok(premerged.contains(&RowKey {
        table: table.to_string(),
        pk,
    }))
}

fn update_pk_key(item: &ChangesetItem, table: &str) -> Result<String, String> {
    match item.old_value(0) {
        Ok(value) => text_id_from_value_ref(table, value),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Err(format!(
            "UPDATE changeset for {table} has no old-side primary key"
        )),
        Err(error) => Err(format!(
            "UPDATE changeset for {table} primary key read failed: {error}"
        )),
    }
}

fn text_id_from_value_ref(table: &str, value: ValueRef<'_>) -> Result<String, String> {
    let ValueRef::Text(bytes) = value else {
        return Err(format!(
            "UPDATE changeset for {table} primary key is not TEXT"
        ));
    };
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|error| format!("UPDATE changeset for {table} primary key is not UTF-8: {error}"))
}

fn timestamp_from_value(value: &Value) -> Option<Timestamp> {
    value_ref_to_string(ValueRef::from(value)).and_then(|s| Timestamp::parse(&s))
}
