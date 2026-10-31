//! Coven's own files (§17.2): the synced table holding each file's record,
//! and the app's columns that reference those records.
//!
//! A file's record is a row of `coven_files` in the file's audience, merged
//! like any synced row. An app column declared with `FileColumn` holds a
//! record's id; coven reads it as a foreign key into `coven_files` that takes
//! its row out, as restrict does, once the record is deleted.

use crate::file_location::Place;
use crate::internal_schema::FILES;
use crate::sqlite::DatabaseConnection;
use crate::write_encoding::{audience_text, encoded, sql_value};
use crate::write_rows::AppValues;
use crate::{DbError, RowIdentity, SyncedTable};
use coven_crypto::ContentHash;
use coven_format::value::Value;
use coven_foundation::id_source::FileId;
use coven_merge::{Audience, RowId};
use rusqlite::params;

pub(crate) const ID: &str = "id";
pub(crate) const AUDIENCE: &str = "audience";
pub(crate) const SIZE: &str = "size";
pub(crate) const HASH: &str = "hash";
pub(crate) const COORDINATES: &str = "coordinates";
pub(crate) const NAMESPACE: &str = "namespace";
/// The columns saying where a file is, which every change sets together.
pub(crate) const PLACE: [&str; 2] = [HASH, COORDINATES];

/// The synced table of files, as coven declares it.
pub(crate) fn declaration() -> SyncedTable {
    SyncedTable::new(FILES, RowIdentity::IndependentUuid).audience_column(AUDIENCE)
}

/// Every table that syncs: the app's, then coven's files table.
pub(crate) fn synced(tables: &[SyncedTable]) -> Vec<SyncedTable> {
    let mut synced = tables.to_vec();
    synced.push(declaration());
    synced
}

/// Whether `table` is coven's files table.
pub(crate) fn is_files(table: &str) -> bool {
    table.eq_ignore_ascii_case(FILES)
}

/// Each file reference these declarations make, as a table and column.
pub(crate) fn declared(tables: &[SyncedTable]) -> Vec<(String, String)> {
    tables
        .iter()
        .flat_map(|table| table.files.iter().map(|file| (table.name.clone(), file.column.clone())))
        .collect()
}

/// The file references last registered, with whether each is write-once.
pub(crate) fn registered(db: &DatabaseConnection) -> Result<Vec<(String, String, bool)>, DbError> {
    db.query(
        "SELECT table_name,column_name,write_once FROM _coven_file_columns
         ORDER BY table_name,column_name",
        [],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )
}

/// File references under a migration step's renames; a dropped table or
/// column's go.
pub(crate) fn renamed(
    files: &[(String, String)],
    names: &crate::migration_names::MigrationMatch,
) -> Vec<(String, String)> {
    files
        .iter()
        .filter_map(|(table, column)| {
            let new_table = names.tables.get(table)?;
            let new_column = names.columns.get(&(table.clone(), column.clone()))?;
            Some((new_table.clone(), new_column.clone()))
        })
        .collect()
}

/// The registered file references, as a table and column.
pub(crate) fn registered_columns(
    db: &DatabaseConnection,
) -> Result<Vec<(String, String)>, DbError> {
    Ok(registered(db)?.into_iter().map(|(table, column, _)| (table, column)).collect())
}

/// Each declared file reference under the schema's own spelling of its
/// table and column, with whether it is write-once.
fn spelled(schema: &crate::schema::Schema, tables: &[SyncedTable]) -> Vec<(String, String, bool)> {
    let mut spelled = Vec::new();
    for declaration in tables {
        let Some(table) = schema.tables.get(&declaration.name.to_ascii_lowercase()) else {
            continue;
        };
        for file in &declaration.files {
            if let Some(column) =
                table.columns.iter().find(|c| c.name.eq_ignore_ascii_case(&file.column))
            {
                spelled.push((table.name.clone(), column.name.clone(), file.write_once));
            }
        }
    }
    spelled.sort();
    spelled
}

/// Record the declared file references, which a later open or migration
/// compares against (§18.1).
pub(crate) fn register(
    db: &DatabaseConnection,
    schema: &crate::schema::Schema,
    tables: &[SyncedTable],
) -> Result<(), DbError> {
    db.internal_execute("DELETE FROM _coven_file_columns", [])?;
    for (table, column, write_once) in spelled(schema, tables) {
        db.internal_execute(
            "INSERT INTO _coven_file_columns(table_name,column_name,write_once) VALUES(?1,?2,?3)",
            params![table, column, write_once],
        )?;
    }
    Ok(())
}

/// The first registered table whose file references differ from the
/// declarations'. A table never registered has none to change.
pub(crate) fn changed(
    db: &DatabaseConnection,
    schema: &crate::schema::Schema,
    tables: &[SyncedTable],
) -> Result<Option<String>, DbError> {
    let declared = spelled(schema, tables);
    let registered = registered(db)?;
    for declaration in tables {
        let Some(table) = schema.tables.get(&declaration.name.to_ascii_lowercase()) else {
            continue;
        };
        let known: bool = db.query_row(
            "SELECT EXISTS(SELECT 1 FROM _coven_columns WHERE table_name=?1)",
            [&table.name],
            |r| r.get(0),
        )?;
        let of = |files: &[(String, String, bool)]| {
            files.iter().filter(|(name, _, _)| *name == table.name).cloned().collect::<Vec<_>>()
        };
        if known && of(&declared) != of(&registered) {
            return Ok(Some(table.name.clone()));
        }
    }
    Ok(None)
}

/// A file's record as a merged row's key.
pub(crate) fn key(file: FileId) -> Result<Vec<u8>, DbError> {
    encoded(coven_format::key::encode_key(&[Value::Text(file.0.to_string())]))
}

/// The file a record's key names.
pub(crate) fn file_of_key(key: &[u8]) -> Result<FileId, DbError> {
    match coven_format::key::decode_key(key)?.as_slice() {
        [value] => id(value).ok_or(DbError::DamagedDatabase),
        _ => Err(DbError::DamagedDatabase),
    }
}

/// A file's record in its audience.
pub(crate) fn row(file: FileId, audience: Audience) -> Result<RowId, DbError> {
    Ok(RowId { table: FILES.into(), key: key(file)?, audience })
}

/// A file id as a reference cell holds it, or `None` for text that names
/// no file in canonical UUID form.
pub(crate) fn id(value: &Value) -> Option<FileId> {
    let Value::Text(text) = value else {
        return None;
    };
    uuid::Uuid::try_parse(text).ok().filter(|id| id.to_string() == *text).map(FileId)
}

/// Check an incoming record's cells (§17.2). A new record carries all of
/// them; a later change carries exactly its coordinates and content hash,
/// which move together: a file never changes its id, size or namespace, and
/// it is in storage exactly when its record has a content hash.
pub(crate) fn check_cells(
    columns: &std::collections::BTreeMap<&str, &coven_merge::ColumnValue<Value>>,
    complete: bool,
) -> Result<(), DbError> {
    use crate::snapshot_error::invalid;
    let value = |name: &str| columns.get(name).map(|column| &column.value);
    if complete {
        match value(SIZE) {
            Some(Value::Integer(size)) if *size >= 0 => {}
            _ => return Err(invalid("file record has no size")),
        }
        if !matches!(value(NAMESPACE), Some(Value::Text(_))) {
            return Err(invalid("file record has no namespace"));
        }
        if !matches!(value(ID), Some(Value::Text(_))) {
            return Err(invalid("file record has no id"));
        }
    } else if columns.keys().any(|name| *name != HASH && *name != COORDINATES) {
        return Err(invalid("file record changes more than where its file is"));
    }
    if complete || !columns.is_empty() {
        let (Some(coordinates), Some(hash)) = (value(COORDINATES), value(HASH)) else {
            return Err(invalid("file record lacks its coordinates or content hash"));
        };
        let cloud = matches!(
            Place::decode(coordinates)
                .map_err(|_damaged| invalid("file coordinates are invalid"))?,
            Place::Cloud(_)
        );
        let stored = match hash {
            Value::Null => false,
            Value::Blob(bytes) if bytes.len() == 32 => true,
            _ => return Err(invalid("file content hash is invalid")),
        };
        if cloud != stored {
            return Err(invalid("file record's content hash disagrees with its coordinates"));
        }
    }
    Ok(())
}

/// One file's record: its audience, size, content hash once stored, where
/// it is, and its namespace (§17.2).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct Record {
    pub(crate) id: FileId,
    pub(crate) audience: Audience,
    pub(crate) size: u64,
    pub(crate) hash: Option<ContentHash>,
    pub(crate) place: Place,
    pub(crate) namespace: String,
}

impl Record {
    /// The record this device shows for `file`, if it has one.
    pub(crate) fn read(db: &DatabaseConnection, file: FileId) -> Result<Option<Self>, DbError> {
        db.query(
            "SELECT audience,size,hash,coordinates,namespace FROM main.coven_files WHERE id=?1",
            [file],
            |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, Option<Vec<u8>>>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            },
        )?
        .pop()
        .map(|(audience, size, hash, coordinates, namespace)| {
            Self::decode(&AppValues::from([
                (ID.into(), Value::Text(file.0.to_string())),
                (AUDIENCE.into(), Value::Text(audience)),
                (SIZE.into(), Value::Integer(size)),
                (HASH.into(), hash.map_or(Value::Null, Value::Blob)),
                (COORDINATES.into(), Value::Text(coordinates)),
                (NAMESPACE.into(), Value::Text(namespace)),
            ]))
        })
        .transpose()
    }

    /// A record from its row's values. A file is in storage exactly when its
    /// record has a content hash; anything else is damage.
    #[expect(
        clippy::map_err_ignore,
        reason = "a stored value that won't convert or decode is DamagedDatabase"
    )]
    pub(crate) fn decode(values: &AppValues) -> Result<Self, DbError> {
        let damaged = || DbError::DamagedDatabase;
        let id = values.get(ID).and_then(id).ok_or_else(damaged)?;
        let audience = match values.get(AUDIENCE) {
            Some(Value::Text(text)) => {
                crate::write_encoding::canonical_audience(text).ok_or_else(damaged)?
            }
            _ => return Err(damaged()),
        };
        let size = match values.get(SIZE) {
            Some(Value::Integer(size)) => u64::try_from(*size).map_err(|_| damaged())?,
            _ => return Err(damaged()),
        };
        let hash = match values.get(HASH) {
            Some(Value::Null) => None,
            Some(Value::Blob(bytes)) => {
                Some(ContentHash::from_bytes(bytes.as_slice().try_into().map_err(|_| damaged())?))
            }
            _ => return Err(damaged()),
        };
        let place = Place::decode(values.get(COORDINATES).ok_or_else(damaged)?)?;
        let namespace = match values.get(NAMESPACE) {
            Some(Value::Text(text)) => text.clone(),
            _ => return Err(damaged()),
        };
        if matches!(place, Place::Cloud(_)) != hash.is_some() {
            return Err(damaged());
        }
        Ok(Self { id, audience, size, hash, place, namespace })
    }

    /// Show this new record in `coven_files`, as coven's own statement in
    /// the write that makes it.
    #[expect(
        clippy::map_err_ignore,
        reason = "a size past the target integer is TooLarge, which names the field and size"
    )]
    pub(crate) fn insert(&self, db: &DatabaseConnection) -> Result<(), DbError> {
        let size = i64::try_from(self.size).map_err(|_| DbError::TooLarge {
            field: "file size",
            actual: self.size,
            maximum: i64::MAX as u64,
        })?;
        db.internal_execute(
            "INSERT INTO main.coven_files(id,audience,size,hash,coordinates,namespace)
             VALUES(?1,?2,?3,?4,?5,?6)",
            params![
                self.id,
                audience_text(&self.audience),
                size,
                self.hash.map(|hash| hash.as_bytes().to_vec()),
                sql_value(&self.place.value()),
                self.namespace
            ],
        )?;
        Ok(())
    }

    /// Switch this record to its stored bytes (§17.9).
    pub(crate) fn store(
        db: &DatabaseConnection,
        file: FileId,
        location: &crate::FileLocation,
        hash: ContentHash,
    ) -> Result<(), DbError> {
        changed_once(db.internal_execute(
            "UPDATE main.coven_files SET coordinates=?2,hash=?3 WHERE id=?1",
            params![file, location.text(), hash.as_bytes().as_slice()],
        )?)
    }

    /// Switch this record off its stored bytes, to `device` only (§17.3).
    pub(crate) fn device_only(
        db: &DatabaseConnection,
        file: FileId,
        device: coven_foundation::id_source::DeviceId,
    ) -> Result<(), DbError> {
        changed_once(db.internal_execute(
            "UPDATE main.coven_files SET coordinates=?2,hash=NULL WHERE id=?1",
            params![file, sql_value(&Place::DeviceOnly(device).value())],
        )?)
    }

    /// Delete a file's record, as coven's own statement in the write that
    /// removes its last reference (§17.2).
    pub(crate) fn delete(db: &DatabaseConnection, file: FileId) -> Result<(), DbError> {
        changed_once(db.internal_execute("DELETE FROM main.coven_files WHERE id=?1", [file])?)
    }
}

fn changed_once(count: usize) -> Result<(), DbError> {
    if count == 1 {
        Ok(())
    } else {
        Err(rusqlite::Error::StatementChangedRows(count).into())
    }
}

#[cfg(test)]
#[path = "file_table_tests.rs"]
mod tests;
