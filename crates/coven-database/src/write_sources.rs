//! Original authored changes and their admission, independent of dismissible losses.

use crate::{
    merge_store::MergeStore, sqlite::DatabaseConnection, write_encoding::audience_text, DbError,
};
use coven_format::{
    merge_fields,
    snapshot_rows::{AuthoredChange, ChangeVerdict},
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

/// Record this device's current interpretation under the original identity,
/// replacing an earlier one: replay rebuilds interpret retained inputs again.
/// NULL means unjudged; receiving covered bytes alone never judges them.
pub(crate) fn admit(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowId,
    verdict: &ChangeVerdict,
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
    db.internal_execute(
        "UPDATE _coven_authored_changes SET admission=?5 WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
        params![write, audience, table, row.key, stored_verdict(verdict)?],
    )?;
    Ok(())
}

/// Whether an original change survives as data for later changes: applied
/// here, or never judged here because a loaded snapshot carries its effects.
pub(crate) fn retained(db: &DatabaseConnection, source: &ChangeId) -> Result<bool, DbError> {
    let admission: Option<Option<Vec<u8>>> = db
        .query(
            "SELECT admission FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
            params![
                merge_fields::encode_write_id(&source.write)?,
                audience_text(&source.row.audience),
                source.row.table,
                source.row.key
            ],
            |r| r.get(0),
        )?
        .pop();
    match admission {
        None => Err(DbError::MissingWriteSources(vec![source.clone()])),
        Some(admission) => Ok(read_verdict(admission)?.retained()),
    }
}

/// NULL stands for a change never judged here.
fn stored_verdict(verdict: &ChangeVerdict) -> Result<Option<Vec<u8>>, DbError> {
    Ok(match verdict {
        ChangeVerdict::Unjudged => None,
        verdict => Some(merge_fields::encode_change_verdict(verdict)?),
    })
}

fn read_verdict(stored: Option<Vec<u8>>) -> Result<ChangeVerdict, DbError> {
    stored.map_or(Ok(ChangeVerdict::Unjudged), |bytes| {
        merge_fields::decode_change_verdict(&bytes).map_err(|_| DbError::DamagedDatabase)
    })
}

/// Every original change of an audience whose write these positions cover,
/// with its part header and this device's verdict, in D7 section 5 order.
pub(crate) fn visit_evidence<E: From<DbError>>(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
    covered: &coven_format::value::WritePositions,
    mut visit: impl FnMut(AuthoredChange) -> Result<(), E>,
) -> Result<(), E> {
    db.for_each(
        "SELECT p.header,p.complete,c.record,c.admission FROM _coven_authored_changes c
         JOIN _coven_authored_parts p ON p.write_id=c.write_id AND p.audience=c.audience
         WHERE c.audience=?1 ORDER BY c.write_id,c.table_name,c.key",
        [audience_text(audience)],
        |r| {
            let header =
                merge_fields::decode_write_header(&r.get::<_, Vec<u8>>(0).map_err(DbError::from)?)
                    .map_err(DbError::from)?;
            if !covered.covers(header.position) {
                return Ok(());
            }
            let change = RowChange::decode(&r.get::<_, Vec<u8>>(2).map_err(DbError::from)?)
                .map_err(DbError::from)?;
            visit(AuthoredChange {
                header,
                complete: r.get(1).map_err(DbError::from)?,
                change,
                verdict: read_verdict(r.get(3).map_err(DbError::from)?)?,
            })
        },
    )
}

/// Install a loaded snapshot's source evidence (D7). An original already held
/// must have the same bytes; the snapshot's verdict replaces this device's,
/// since the loaded data is what that verdict produced. The caller marks
/// complete parts once all their changes are installed.
pub(crate) fn load_evidence(
    db: &DatabaseConnection,
    authored: &AuthoredChange,
) -> Result<(), DbError> {
    let header = &authored.header;
    part(db, header, &authored.change.row.audience)?;
    retain(db, header, &authored.change)?;
    db.internal_execute(
        "UPDATE _coven_authored_changes SET admission=?5 WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
        params![
            merge_fields::encode_write_id(&header.position)?,
            audience_text(&authored.change.row.audience),
            authored.change.row.table,
            authored.change.row.key,
            stored_verdict(&authored.verdict)?
        ],
    )?;
    Ok(())
}

/// This device's migration write changes rows only through its own raise's
/// snapshot (§17.1). A loaded snapshot that covers it without carrying those
/// changes was made from another raise's data, in which they never counted:
/// their evidence goes with the data they described.
pub(crate) fn forget_own_migration(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
    covered: &coven_format::value::WritePositions,
    carried: &std::collections::BTreeSet<(WriteId, String, Vec<u8>)>,
) -> Result<(), DbError> {
    let own = db.pending_device().ok_or(DbError::DamagedDatabase)?;
    let audience = audience_text(audience);
    let first = merge_fields::encode_write_id(&WriteId {
        device: own,
        number: 1,
    })?;
    let last = merge_fields::encode_write_id(&WriteId {
        device: own,
        number: u64::MAX,
    })?;
    for (write_id, header) in db.query(
        "SELECT write_id,header FROM _coven_authored_parts WHERE write_id BETWEEN ?2 AND ?3 AND audience=?1",
        params![audience, first, last],
        |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
    )? {
        let header = merge_fields::decode_write_header(&header)?;
        if header.disposition != coven_format::write::WriteDisposition::Migration
            || !covered.covers(header.position)
        {
            continue;
        }
        for (table, key) in db.query(
            "SELECT table_name,key FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2",
            params![write_id, audience],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )? {
            if !carried.contains(&(header.position, table.clone(), key.clone())) {
                db.internal_execute(
                    "DELETE FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
                    params![write_id, audience, table, key],
                )?;
            }
        }
        db.internal_execute(
            "DELETE FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2
             AND NOT EXISTS(SELECT 1 FROM _coven_authored_changes c WHERE c.write_id=?1 AND c.audience=?2)",
            params![write_id, audience],
        )?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "write_sources_tests.rs"]
mod tests;
