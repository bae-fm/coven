//! Original authored changes and their admission, independent of dismissible losses.

use crate::{
    merge_store::MergeStore, sqlite::DatabaseConnection, write_encoding::audience_text, DbError,
};
use coven_format::{
    merge_fields,
    snapshot_rows::LostWriteCause,
    value::Value,
    write::{RowChange, WriteHeader},
    write_sources::{CellSource, ChangeId, WriteSources},
};
use coven_merge::{Change, Operation, RowId, WriteId};
use rusqlite::params;
use std::collections::BTreeMap;

pub(crate) struct Evidence {
    pub(crate) header: WriteHeader,
    pub(crate) original: RowChange,
}

fn invalid(field: &'static str) -> DbError {
    coven_format::Error::Invalid {
        field,
        rule: coven_format::error::Rule::Coverage,
    }
    .into()
}

/// Read the named original, never a current winner or an equal scalar. A known
/// complete part can prove absence; otherwise the evidence is still unavailable.
pub(crate) fn evidence(db: &DatabaseConnection, source: &ChangeId) -> Result<Evidence, DbError> {
    let write = merge_fields::encode_write_id(&source.write)?;
    let audience = audience_text(&source.row.audience);
    let part = db
        .query(
            "SELECT header,complete FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2",
            params![write, audience],
            |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, bool>(1)?)),
        )?
        .pop();
    let Some((header, complete)) = part else {
        return Err(DbError::MissingWriteSources(vec![source.clone()]));
    };
    let row = db.query("SELECT record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",params![write,audience,source.row.table,source.row.key],|r| r.get::<_,Vec<u8>>(0))?.pop();
    let Some(record) = row else {
        return Err(if complete {
            invalid("source row")
        } else {
            DbError::MissingWriteSources(vec![source.clone()])
        });
    };
    Ok(Evidence {
        header: merge_fields::decode_write_header(&header)?,
        original: RowChange::decode(&record)?,
    })
}

/// Capture the actual setters in the author's pre-write merge state. Renames
/// resolve their original names through the recorded schema identities.
pub(crate) fn authored(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    row: RowId,
    change: Change<Value>,
    old: BTreeMap<String, Value>,
) -> Result<RowChange, DbError> {
    let state = store.row(&row)?.state;
    let version = db.schema_version()?;
    let source = |write: WriteId| -> Result<(ChangeId, u32), DbError> {
        let bytes: Option<Vec<u8>> = db.query_row(
            "SELECT (SELECT header FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2)",
            params![
                merge_fields::encode_write_id(&write)?,
                audience_text(&row.audience)
            ],
            |r| r.get(0),
        )?;
        let Some(bytes) = bytes else {
            return Err(DbError::MissingWriteSources(vec![ChangeId {
                write,
                row: row.clone(),
            }]));
        };
        let header = merge_fields::decode_write_header(&bytes)?;
        let table = crate::source_names::table(db, version, &row.table, header.schema_version)?
            .ok_or(DbError::DamagedDatabase)?;
        Ok((
            ChangeId {
                write,
                row: RowId {
                    table,
                    ..row.clone()
                },
            },
            header.schema_version,
        ))
    };
    let generation = if change.generation == 0 {
        None
    } else {
        Some(
            source(
                *state
                    .generations()
                    .get(&change.generation)
                    .ok_or(DbError::DamagedDatabase)?,
            )?
            .0,
        )
    };
    let mut setters = BTreeMap::new();
    for (column, cell) in state.cells() {
        let replaces = match &change.operation {
            Operation::Insert(_) => false,
            Operation::Update(columns) => columns.contains_key(column),
            Operation::Delete => true,
        };
        if replaces {
            let (change, source_version) = source(cell.write)?;
            let original = crate::source_names::column(
                db,
                version,
                &row.table,
                column,
                source_version,
                &change.row.table,
            )?
            .ok_or(DbError::DamagedDatabase)?;
            setters.insert(
                column.clone(),
                CellSource {
                    change,
                    column: original,
                },
            );
        }
    }
    Ok(RowChange {
        row,
        change,
        old,
        sources: WriteSources {
            generation,
            setters,
        },
    })
}

/// Checks the original role in its original incarnation. Ordinary merge losses
/// remain source evidence: admission is retained but does not establish validity.
pub(crate) fn validate(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowChange,
) -> Result<(), DbError> {
    row.encode().map_err(|error| match error {
        coven_format::Error::Merge(error) => DbError::InvalidWrite {
            write: header.position,
            error,
        },
        error => error.into(),
    })?;
    row.sources.validate_past(header)?;
    let check = |source: &ChangeId| -> Result<Evidence, DbError> {
        if source.row.key != row.row.key || source.row.audience != row.row.audience {
            return Err(invalid("source row identity"));
        }
        let evidence = evidence(db, source)?;
        if evidence.header.timestamp >= header.timestamp {
            return Err(DbError::InvalidWrite {
                write: header.position,
                error: coven_merge::MergeError::CausalTimestamp(header.position),
            });
        }
        if crate::source_names::table(
            db,
            evidence.header.schema_version,
            &source.row.table,
            header.schema_version,
        )?
        .as_ref()
            != Some(&row.row.table)
        {
            return Err(invalid("source row identity"));
        }
        Ok(evidence)
    };
    if let Some(source) = &row.sources.generation {
        let evidence = check(source)?;
        if matches!(evidence.original.change.operation, Operation::Update(_))
            || evidence.original.change.generation.checked_add(1) != Some(row.change.generation)
        {
            return Err(invalid("source generation advancement"));
        }
    }
    for (column, source) in &row.sources.setters {
        let evidence = check(&source.change)?;
        let columns = match &evidence.original.change.operation {
            Operation::Insert(columns) | Operation::Update(columns) => columns,
            Operation::Delete => return Err(invalid("source setter operation")),
        };
        if !columns.contains_key(&source.column)
            || crate::source_names::column(
                db,
                evidence.header.schema_version,
                &source.change.row.table,
                &source.column,
                header.schema_version,
                &row.row.table,
            )?
            .as_ref()
                != Some(column)
        {
            return Err(invalid("source cell identity"));
        }
        if evidence
            .original
            .change
            .incarnation()
            .map_err(coven_format::Error::Merge)?
            != row
                .change
                .incarnation()
                .map_err(coven_format::Error::Merge)?
        {
            return Err(invalid("source setter incarnation"));
        }
    }
    Ok(())
}

fn part(
    db: &DatabaseConnection,
    header: &WriteHeader,
    audience: &coven_merge::Audience,
) -> Result<(), DbError> {
    let write = merge_fields::encode_write_id(&header.position)?;
    let audience = audience_text(audience);
    let bytes = merge_fields::encode_write_header(header)?;
    let original: Option<Vec<u8>> = db.query_row(
        "SELECT (SELECT header FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2)",
        params![write, audience],
        |r| r.get(0),
    )?;
    if let Some(original) = original {
        if original != bytes {
            return Err(invalid("original source header changed"));
        }
    } else {
        db.internal_execute(
            "INSERT INTO _coven_authored_parts VALUES(?1,?2,?3,0)",
            params![write, audience, bytes],
        )?;
    }
    Ok(())
}

pub(crate) fn retain(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowChange,
) -> Result<(), DbError> {
    part(db, header, &row.row.audience)?;
    let write = merge_fields::encode_write_id(&header.position)?;
    let audience = audience_text(&row.row.audience);
    let bytes = row.encode()?;
    let original: Option<Vec<u8>> = db.query_row("SELECT (SELECT record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4)",params![write,audience,row.row.table,row.row.key],|r|r.get(0))?;
    if original.is_none()
        && db.query_row(
            "SELECT complete FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2",
            params![write, audience],
            |r| r.get::<_, bool>(0),
        )?
    {
        return Err(invalid("original source part changed"));
    }
    if original.as_ref().is_some_and(|original| *original != bytes) {
        return Err(invalid("original source change changed"));
    }
    db.internal_execute("INSERT INTO _coven_authored_changes VALUES(?1,?2,?3,?4,?5,NULL) ON CONFLICT(write_id,audience,table_name,key) DO NOTHING",params![write,audience,row.row.table,row.row.key,bytes])?;
    Ok(())
}

pub(crate) fn complete(
    db: &DatabaseConnection,
    header: &WriteHeader,
    audience: &coven_merge::Audience,
) -> Result<(), DbError> {
    part(db, header, audience)?;
    db.internal_execute("UPDATE _coven_authored_parts SET complete=1 WHERE write_id=?1 AND audience=?2 AND complete=0",params![merge_fields::encode_write_id(&header.position)?,audience_text(audience)])?;
    Ok(())
}

/// Record an actual interpretation under the original identity. NULL means
/// unjudged, an empty blob means admitted, and a cause means excluded. Receiving
/// covered bytes alone never judges them; an exclusion survives later receipt.
pub(crate) fn admit(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowId,
    cause: Option<LostWriteCause>,
) -> Result<(), DbError> {
    let write = merge_fields::encode_write_id(&header.position)?;
    let audience = audience_text(&row.audience);
    let original: Vec<u8> = db.query_row(
        "SELECT header FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2",
        params![write, audience],
        |r| r.get(0),
    )?;
    let original = merge_fields::decode_write_header(&original)?;
    let table = crate::source_names::table(
        db,
        header.schema_version,
        &row.table,
        original.schema_version,
    )?
    .ok_or(DbError::DamagedDatabase)?;
    let admission = match cause {
        Some(cause) => merge_fields::encode_lost_write_cause(&cause)?,
        None => Vec::new(),
    };
    db.internal_execute("UPDATE _coven_authored_changes SET admission=?5 WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4 AND (admission IS NULL OR (length(admission)=0 AND length(?5)>0))",
        params![write, audience, table, row.key, admission])?;
    Ok(())
}

#[cfg(test)]
#[path = "write_sources_tests.rs"]
mod tests;
