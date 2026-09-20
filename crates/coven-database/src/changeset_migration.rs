//! Explicit interpretation of historical application changesets.

use crate::{DbError, Migration};
use coven_foundation::changeset::ChangeOp;
use rusqlite::{types::Value, Connection};
use std::{collections::BTreeMap, sync::Arc};

/// The two sides of a changed column. An absent cell is unchanged; SQL NULL is
/// represented by `Some(Value::Null)`.
#[derive(Clone, Debug, PartialEq)]
pub struct ChangesetColumn {
    pub name: String,
    pub old: Option<Value>,
    pub new: Option<Value>,
    pub primary_key: bool,
}

/// One operation whose cells retain their SQLite types and update presence.
#[derive(Clone, Debug, PartialEq)]
pub struct ChangesetRow {
    pub table: String,
    pub operation: ChangeOp,
    pub columns: Vec<ChangesetColumn>,
    pub indirect: bool,
}

type Transform =
    Arc<dyn Fn(&mut ChangesetRow, &BTreeMap<String, Value>) -> Result<(), DbError> + Send + Sync>;

/// A host-declared change in one table's historical row meaning.
#[derive(Clone)]
pub struct TableChangesetMigration {
    table: &'static str,
    immutable: &'static [&'static str],
    transform: Transform,
}

impl TableChangesetMigration {
    pub fn new<F>(table: &'static str, immutable: &'static [&'static str], transform: F) -> Self
    where
        F: Fn(&mut ChangesetRow, &BTreeMap<String, Value>) -> Result<(), DbError>
            + Send
            + Sync
            + 'static,
    {
        Self {
            table,
            immutable,
            transform: Arc::new(transform),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ChangesetMigrationError {
    #[error("changeset schema {source_version} is newer than supported schema {supported}")]
    FutureSchema { source_version: u32, supported: u32 },
    #[error("changeset table {table} does not match schema {version}: {reason}")]
    Layout {
        table: String,
        version: u32,
        reason: String,
    },
    #[error("historical UPDATE changes immutable identity {table}.{column}")]
    MutableIdentity { table: String, column: String },
    #[error("changeset authoring schema cannot be recovered unambiguously: {versions:?}")]
    AmbiguousVersion { versions: Vec<u32> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ColumnLayout {
    name: String,
    primary_key: bool,
}
type Layout = BTreeMap<String, Vec<ColumnLayout>>;

/// Registered application schemas and the transformations between them.
/// Layouts come from executing the actual migration ladder on disposable SQLite.
pub(crate) struct ApplicationSchemaHistory {
    migrations: Arc<[Migration]>,
    layouts: Vec<Layout>,
}

impl ApplicationSchemaHistory {
    pub(crate) fn new(migrations: Arc<[Migration]>) -> Result<Self, DbError> {
        let database = Connection::open_in_memory()?;
        let mut layouts = vec![Layout::new()];
        for (position, migration) in migrations.iter().enumerate() {
            if migration.version as usize != position + 1 {
                return Err(DbError::Message(
                    "application migration ladder is not contiguous".into(),
                ));
            }
            migration.up.apply(&database)?;
            database.pragma_update(None, "user_version", migration.version)?;
            layouts.push(read_layout(&database)?);
        }
        Ok(Self {
            migrations,
            layouts,
        })
    }

    pub(crate) fn migrations(&self) -> &[Migration] {
        &self.migrations
    }

    fn layout(&self, version: u32) -> Result<&Layout, DbError> {
        self.layouts.get(version as usize).ok_or_else(|| {
            ChangesetMigrationError::FutureSchema {
                source_version: version,
                supported: self.migrations.len() as u32,
            }
            .into()
        })
    }

    pub(crate) fn validate_source(
        &self,
        connection: &Connection,
        version: u32,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        self.decode(connection, version, bytes).map(|_| ())
    }

    pub(crate) fn migrate(
        &self,
        connection: &Connection,
        source_version: u32,
        bytes: &[u8],
    ) -> Result<Vec<u8>, DbError> {
        let mut rows = self.decode(connection, source_version, bytes)?;
        let mut converted = Vec::with_capacity(rows.len());
        for mut row in rows.drain(..) {
            let identity = row
                .columns
                .iter()
                .filter(|column| column.primary_key)
                .cloned()
                .collect::<Vec<_>>();
            let operation = row.operation;
            let table_name = row.table.clone();
            let indirect = row.indirect;
            let clock = row
                .columns
                .iter()
                .find(|column| column.name == "_updated_at")
                .cloned();
            let mut available = true;
            for migration in self.migrations.iter().skip(source_version as usize) {
                let table = row.table.clone();
                for adapter in migration
                    .changesets
                    .iter()
                    .filter(|adapter| adapter.table == table)
                {
                    let Some(context) = immutable_context(connection, &row, adapter.immutable)?
                    else {
                        tracing::debug!(table = %row.table, "historical UPDATE has no surviving immutable row context; retaining delete-wins");
                        available = false;
                        break;
                    };
                    (adapter.transform)(&mut row, &context)?;
                }
                if !available {
                    break;
                }
                if !crate::is_routing_table(&row.table) {
                    validate_layout(&row, self.layout(migration.version)?, migration.version)?;
                }
            }
            if available {
                let actual_identity = row
                    .columns
                    .iter()
                    .filter(|column| column.primary_key)
                    .cloned()
                    .collect::<Vec<_>>();
                if actual_identity != identity {
                    return Err(ChangesetMigrationError::MutableIdentity {
                        table: row.table,
                        column: "primary key".into(),
                    }
                    .into());
                }
                if row.operation != operation
                    || row.table != table_name
                    || row.indirect != indirect
                    || row
                        .columns
                        .iter()
                        .find(|column| column.name == "_updated_at")
                        != clock.as_ref()
                {
                    return Err(DbError::Message(format!(
                        "changeset migration altered operation, table, indirect flag or row clock for {table_name}"
                    )));
                }
                converted.push(row);
            }
        }
        encode(connection, &converted)
    }

    fn decode(
        &self,
        connection: &Connection,
        version: u32,
        bytes: &[u8],
    ) -> Result<Vec<ChangesetRow>, DbError> {
        use fallible_streaming_iterator::FallibleStreamingIterator;
        use rusqlite::{hooks::Action, session::ChangesetIter};
        let layout = self.layout(version)?;
        if bytes.is_empty() {
            return Ok(Vec::new());
        }
        let routing = read_layout(connection)?;
        let input: &mut dyn std::io::Read = &mut &bytes[..];
        let mut iterator =
            ChangesetIter::start_strm(&input).map_err(crate::ChangesetError::Start)?;
        let mut rows = Vec::new();
        while let Some(item) = iterator.next().map_err(crate::ChangesetError::Next)? {
            let op = item.op().map_err(crate::ChangesetError::Operation)?;
            let table = op.table_name();
            let layout = if crate::is_routing_table(table) {
                &routing
            } else {
                layout
            };
            let columns = layout
                .get(table)
                .ok_or_else(|| ChangesetMigrationError::Layout {
                    table: table.into(),
                    version,
                    reason: "table is absent".into(),
                })?;
            let keys = item.pk().map_err(crate::ChangesetError::Operation)?;
            if columns.len() != op.number_of_columns() as usize
                || keys.len() != columns.len()
                || columns
                    .iter()
                    .zip(keys)
                    .any(|(column, key)| column.primary_key != (*key != 0))
            {
                return Err(ChangesetMigrationError::Layout {
                    table: table.into(),
                    version,
                    reason: "column count or primary key differs".into(),
                }
                .into());
            }
            let operation = match op.code() {
                Action::SQLITE_INSERT => ChangeOp::Insert,
                Action::SQLITE_UPDATE => ChangeOp::Update,
                Action::SQLITE_DELETE => ChangeOp::Delete,
                _ => return Err(DbError::Message("unsupported changeset operation".into())),
            };
            let mut values = Vec::with_capacity(columns.len());
            for (index, column) in columns.iter().enumerate() {
                values.push(ChangesetColumn {
                    name: column.name.clone(),
                    primary_key: column.primary_key,
                    old: if operation == ChangeOp::Insert {
                        None
                    } else {
                        owned_cell(item.old_value(index), index, "old")?
                    },
                    new: if operation == ChangeOp::Delete {
                        None
                    } else {
                        owned_cell(item.new_value(index), index, "new")?
                    },
                });
            }
            rows.push(ChangesetRow {
                table: table.into(),
                operation,
                columns: values,
                indirect: op.indirect(),
            });
        }
        Ok(rows)
    }

    pub(crate) fn recover_version(
        &self,
        connection: &Connection,
        changesets: &[&[u8]],
        authenticated_version: Option<u32>,
    ) -> Result<u32, DbError> {
        let current: u32 = connection.pragma_query_value(None, "user_version", |row| row.get(0))?;
        if let Some(version) = authenticated_version {
            for bytes in changesets {
                self.decode(connection, version, bytes)?;
            }
            return Ok(version);
        }
        let mut candidates = Vec::new();
        let mut interpretation = None;
        for version in 0..=current {
            let mut decoded = Vec::new();
            let mut matches = true;
            for bytes in changesets {
                match self.decode(connection, version, bytes) {
                    Ok(rows) => decoded.extend(rows),
                    Err(DbError::ChangesetMigration(ChangesetMigrationError::Layout {
                        ..
                    })) => {
                        matches = false;
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if !matches {
                continue;
            }
            let transformations = self
                .migrations
                .iter()
                .skip(version as usize)
                .filter(|migration| {
                    migration
                        .changesets
                        .iter()
                        .any(|adapter| decoded.iter().any(|row| row.table == adapter.table))
                })
                .map(|migration| migration.version)
                .collect::<Vec<_>>();
            let meaning = (decoded, transformations);
            if interpretation
                .as_ref()
                .is_some_and(|prior| prior != &meaning)
            {
                candidates.push(version);
                return Err(ChangesetMigrationError::AmbiguousVersion {
                    versions: candidates,
                }
                .into());
            }
            interpretation = Some(meaning);
            candidates.push(version);
        }
        candidates.first().copied().ok_or_else(|| {
            ChangesetMigrationError::AmbiguousVersion {
                versions: candidates,
            }
            .into()
        })
    }
}

fn owned_cell(
    value: rusqlite::Result<rusqlite::types::ValueRef<'_>>,
    column: usize,
    side: &'static str,
) -> Result<Option<Value>, DbError> {
    let error = |source| {
        DbError::Changeset(crate::ChangesetError::Value {
            side,
            column,
            source,
        })
    };
    match value {
        Ok(value) => Value::try_from(value).map(Some).map_err(|source| {
            error(rusqlite::Error::FromSqlConversionFailure(
                column,
                value.data_type(),
                Box::new(source),
            ))
        }),
        Err(rusqlite::Error::InvalidColumnIndex(_)) => Ok(None),
        Err(source) => Err(error(source)),
    }
}

fn read_layout(connection: &Connection) -> Result<Layout, DbError> {
    let names = crate::query_mapped_rows(
        connection,
        "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name",
        [],
        |row| row.get::<_, String>(0),
    )?;
    names
        .into_iter()
        .map(|table| {
            let sql = format!("PRAGMA table_info({})", crate::quote_ident(&table));
            let columns = crate::query_mapped_rows(connection, &sql, [], |row| {
                Ok(ColumnLayout {
                    name: row.get(1)?,
                    primary_key: row.get::<_, i64>(5)? != 0,
                })
            })?;
            Ok((table, columns))
        })
        .collect()
}

fn validate_layout(row: &ChangesetRow, layout: &Layout, version: u32) -> Result<(), DbError> {
    let columns = row
        .columns
        .iter()
        .map(|column| ColumnLayout {
            name: column.name.clone(),
            primary_key: column.primary_key,
        })
        .collect::<Vec<_>>();
    if layout.get(&row.table) != Some(&columns) {
        return Err(ChangesetMigrationError::Layout {
            table: row.table.clone(),
            version,
            reason: "schema change requires an explicit matching changeset transformation".into(),
        }
        .into());
    }
    Ok(())
}

fn immutable_context(
    connection: &Connection,
    row: &ChangesetRow,
    names: &[&str],
) -> Result<Option<BTreeMap<String, Value>>, DbError> {
    use rusqlite::OptionalExtension;
    let columns = names
        .iter()
        .map(|name| {
            let column = row
                .columns
                .iter()
                .find(|column| column.name == *name)
                .ok_or_else(|| {
                    DbError::Message(format!("immutable column {}.{name} is absent", row.table))
                })?;
            if row.operation == ChangeOp::Update && column.new.is_some() && column.old != column.new
            {
                return Err(ChangesetMigrationError::MutableIdentity {
                    table: row.table.clone(),
                    column: (*name).into(),
                }
                .into());
            }
            Ok((*name, column))
        })
        .collect::<Result<Vec<_>, DbError>>()?;
    let mut context = BTreeMap::new();
    for (name, column) in columns {
        if let Some(value) = column.old.as_ref().or(column.new.as_ref()) {
            context.insert(name.into(), value.clone());
            continue;
        }
        let keys = row
            .columns
            .iter()
            .filter(|column| column.primary_key)
            .collect::<Vec<_>>();
        let predicates = keys
            .iter()
            .map(|key| format!("{} = ?", crate::quote_ident(&key.name)))
            .collect::<Vec<_>>()
            .join(" AND ");
        let values =
            keys.iter()
                .map(|key| {
                    key.old.as_ref().or(key.new.as_ref()).ok_or_else(|| {
                        DbError::Message("changeset primary key has no value".into())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
        let sql = format!(
            "SELECT {} FROM {} WHERE {predicates}",
            crate::quote_ident(name),
            crate::quote_ident(&row.table)
        );
        let value = connection
            .query_row(&sql, rusqlite::params_from_iter(values), |record| {
                record.get::<_, Value>(0)
            })
            .optional()?;
        match value {
            Some(value) => {
                context.insert(name.into(), value);
            }
            None => return Ok(None),
        }
    }
    Ok(Some(context))
}

fn encode(connection: &Connection, rows: &[ChangesetRow]) -> Result<Vec<u8>, DbError> {
    use rusqlite::ffi;
    let group = crate::gate::Changegroup::new()?;
    // The connection outlives the native group and supplies only its final schema.
    unsafe {
        group.set_schema(connection.handle())?;
    }
    for row in rows {
        let operation = match row.operation {
            ChangeOp::Insert => ffi::SQLITE_INSERT,
            ChangeOp::Update => ffi::SQLITE_UPDATE,
            ChangeOp::Delete => ffi::SQLITE_DELETE,
        };
        let old = row
            .columns
            .iter()
            .map(|column| column.old.clone())
            .collect::<Vec<_>>();
        let new = row
            .columns
            .iter()
            .map(|column| column.new.clone())
            .collect::<Vec<_>>();
        group.add_typed_change(&row.table, operation, &old, &new, row.indirect)?;
    }
    Ok(group.output()?)
}

#[cfg(test)]
pub(crate) fn migrate_changeset(
    connection: &Connection,
    migrations: &[Migration],
    source_version: u32,
    bytes: &[u8],
) -> Result<Vec<u8>, DbError> {
    ApplicationSchemaHistory::new(Arc::from(migrations.to_vec()))?.migrate(
        connection,
        source_version,
        bytes,
    )
}

pub(crate) fn recover_changeset_version(
    connection: &Connection,
    migrations: &[Migration],
    changesets: &[&[u8]],
    authenticated_version: Option<u32>,
) -> Result<u32, DbError> {
    ApplicationSchemaHistory::new(Arc::from(migrations.to_vec()))?.recover_version(
        connection,
        changesets,
        authenticated_version,
    )
}

/// Exercise registered historical conversion and the ordinary row-conflict
/// application path without constructing a remote provider fixture.
#[cfg(any(test, feature = "test-utils"))]
pub fn resolve_and_apply_historical_changeset(
    connection: &Connection,
    store_dir: &coven_foundation::store_dir::StoreDir,
    migrations: Vec<Migration>,
    source_version: u32,
    bytes: &[u8],
    tables: &[coven_protocol::synced_schema::SyncedTable],
    receiver_wall_ms: u64,
) -> Result<crate::ApplyResult, DbError> {
    let history = ApplicationSchemaHistory::new(Arc::from(migrations))?;
    let converted = history.migrate(connection, source_version, bytes)?;
    crate::resolve_and_apply_changeset(connection, store_dir, &converted, tables, receiver_wall_ms)
}

#[cfg(test)]
#[path = "changeset_migration_tests.rs"]
mod tests;
