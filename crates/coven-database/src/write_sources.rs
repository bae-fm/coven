//! Original authored changes and their admission, independent of dismissible losses.

use crate::{
    merge_store::MergeStore, sqlite::DatabaseConnection, write_encoding::audience_text, DbError,
};
use coven_format::{
    merge_fields,
    snapshot_rows::{AuthoredChange, ChangeVerdict, OriginalRow},
    value::Value,
    write::{PartRecord, RowChange, WriteHeader},
    write_sources::{CellSource, ChangeId, WriteSources},
};
use coven_merge::{Change, Operation, RowId, WriteId};
use rusqlite::params;
use std::collections::BTreeMap;

pub(crate) struct Evidence {
    pub(crate) header: WriteHeader,
    pub(crate) original: RowChange,
}

/// A retained original record, a net change or an execution alone.
pub(crate) struct Original {
    pub(crate) header: WriteHeader,
    pub(crate) record: OriginalRow,
}

fn invalid(field: &'static str) -> DbError {
    coven_format::Error::Invalid {
        field,
        rule: coven_format::error::Rule::Coverage,
    }
    .into()
}

/// Read the named original net change, never a current winner or an equal
/// scalar. A row with operations but no net change set no generation or cell,
/// so it is never a source.
pub(crate) fn evidence(db: &DatabaseConnection, source: &ChangeId) -> Result<Evidence, DbError> {
    let Original { header, record } = original(db, source)?;
    match record {
        OriginalRow::Change(original) => Ok(Evidence { header, original }),
        OriginalRow::Execution(_) => Err(invalid("source has no net change")),
    }
}

/// Read the named original record. A known complete part can prove absence;
/// otherwise the evidence is still unavailable.
pub(crate) fn original(db: &DatabaseConnection, source: &ChangeId) -> Result<Original, DbError> {
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
    Ok(Original {
        header: merge_fields::decode_write_header(&header)?,
        record: OriginalRow::decode(&record)?,
    })
}

/// The author's pre-write generation and cell setters of one row, named as
/// their original changes. Renames resolve their original names through the
/// recorded schema identities.
struct PreWrite<'a> {
    db: &'a DatabaseConnection,
    row: RowId,
    state: coven_merge::RowState<Value>,
    version: u32,
}

impl<'a> PreWrite<'a> {
    fn new(
        db: &'a DatabaseConnection,
        store: &MergeStore<'_>,
        row: RowId,
    ) -> Result<Self, DbError> {
        Ok(Self {
            state: store.row(&row)?.state,
            version: db.schema_version()?,
            db,
            row,
        })
    }

    fn source(&self, write: WriteId) -> Result<(ChangeId, u32), DbError> {
        let bytes: Option<Vec<u8>> = self.db.query_row(
            "SELECT (SELECT header FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2)",
            params![
                merge_fields::encode_write_id(&write)?,
                audience_text(&self.row.audience)
            ],
            |r| r.get(0),
        )?;
        let Some(bytes) = bytes else {
            return Err(DbError::MissingWriteSources(vec![ChangeId {
                write,
                row: self.row.clone(),
            }]));
        };
        let header = merge_fields::decode_write_header(&bytes)?;
        let table = crate::source_names::table(
            self.db,
            self.version,
            &self.row.table,
            header.schema_version,
        )?
        .ok_or(DbError::DamagedDatabase)?;
        Ok((
            ChangeId {
                write,
                row: RowId {
                    table,
                    ..self.row.clone()
                },
            },
            header.schema_version,
        ))
    }

    /// The change that established `generation`, absent at generation zero.
    fn generation(&self, generation: u64) -> Result<Option<ChangeId>, DbError> {
        if generation == 0 {
            return Ok(None);
        }
        let write = *self
            .state
            .generations()
            .get(&generation)
            .ok_or(DbError::DamagedDatabase)?;
        Ok(Some(self.source(write)?.0))
    }

    /// The original setter of a pre-write cell, if it was ever set.
    fn setter(&self, column: &str) -> Result<Option<CellSource>, DbError> {
        let Some(cell) = self.state.cells().get(column) else {
            return Ok(None);
        };
        let (change, source_version) = self.source(cell.write)?;
        let original = crate::source_names::column(
            self.db,
            self.version,
            &self.row.table,
            column,
            source_version,
            &change.row.table,
        )?
        .ok_or(DbError::DamagedDatabase)?;
        Ok(Some(CellSource {
            change,
            column: original,
        }))
    }

    fn cells(&self) -> impl Iterator<Item = &String> {
        self.state.cells().keys()
    }
}

/// Capture the actual setters in the author's pre-write merge state.
pub(crate) fn authored(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    row: RowId,
    change: Change<Value>,
    old: BTreeMap<String, Value>,
) -> Result<RowChange, DbError> {
    let pre = PreWrite::new(db, store, row.clone())?;
    let mut setters = BTreeMap::new();
    for column in pre.cells() {
        let replaces = match &change.operation {
            Operation::Insert(_) => false,
            Operation::Update(columns) => columns.contains_key(column),
            Operation::Delete => true,
        };
        if replaces {
            setters.extend(pre.setter(column)?.map(|setter| (column.clone(), setter)));
        }
    }
    Ok(RowChange {
        row,
        sources: WriteSources {
            generation: pre.generation(change.generation)?,
            setters,
        },
        change,
        old,
        creator: None,
        execution: Default::default(),
    })
}

/// The pre-write sources a row's operations used beyond its net change's own
/// sources (§9.5, D5): the generation the first operation started from, and the
/// setter of each pre-write cell an operation replaced before the row's
/// incarnation ended. Cells this write itself set are not sources.
pub(crate) fn executed(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    row: &RowId,
    steps: Vec<coven_format::write::Step>,
    net: Option<&WriteSources>,
) -> Result<coven_format::write::Execution, DbError> {
    let Some(first) = steps.first() else {
        return Ok(Default::default());
    };
    let pre = PreWrite::new(db, store, row.clone())?;
    // A row that moved into this audience earlier in the write starts from
    // the incarnation the move began: nothing before the write is replaced.
    if first.change.generation != pre.state.generation() {
        return Ok(coven_format::write::Execution {
            steps,
            sources: Default::default(),
        });
    }
    let mut remaining: std::collections::BTreeSet<String> = pre.cells().cloned().collect();
    let mut replaced = std::collections::BTreeSet::new();
    for step in &steps {
        match &step.change.operation {
            Operation::Update(columns) => {
                for column in columns.keys() {
                    if remaining.remove(column) {
                        replaced.insert(column.clone());
                    }
                }
            }
            Operation::Delete => replaced.append(&mut remaining),
            Operation::Insert(_) => remaining.clear(),
        }
    }
    let generation = match net {
        Some(net) if net.generation.is_some() => None,
        _ => pre.generation(first.change.generation)?,
    };
    let mut setters = BTreeMap::new();
    for column in replaced {
        if net.is_some_and(|net| net.setters.contains_key(&column)) {
            continue;
        }
        setters.extend(pre.setter(&column)?.map(|setter| (column, setter)));
    }
    Ok(coven_format::write::Execution {
        steps,
        sources: WriteSources {
            generation,
            setters,
        },
    })
}

/// Checks a record's original roles in their original incarnation, and a
/// copy's named creation. Ordinary merge losses remain source evidence:
/// admission is retained but does not establish validity.
pub(crate) fn validate(
    db: &DatabaseConnection,
    header: &WriteHeader,
    record: PartRecord<'_>,
) -> Result<(), DbError> {
    let merge = |error| match error {
        coven_format::Error::Merge(error) => DbError::InvalidWrite {
            write: header.position,
            error,
        },
        error => error.into(),
    };
    match record {
        PartRecord::Change(row) => {
            row.encode().map_err(merge)?;
            validate_past(db, header, &row.sources)?;
            let incarnation = row
                .change
                .incarnation()
                .map_err(coven_format::Error::Merge)?;
            roles(
                db,
                header,
                &row.row,
                row.change.generation,
                incarnation,
                &row.sources,
            )?;
            if let Some(creator) = &row.creator {
                creation(db, header, &row.row, creator)?;
            }
        }
        PartRecord::Execution(execution) => {
            execution.encode().map_err(merge)?;
        }
    }
    let execution = record.execution();
    validate_past(db, header, &execution.sources)?;
    if let Some(first) = execution.steps.first() {
        let incarnation = first
            .change
            .incarnation()
            .map_err(coven_format::Error::Merge)?;
        roles(
            db,
            header,
            record.row(),
            first.change.generation,
            incarnation,
            &execution.sources,
        )?;
    }
    Ok(())
}

pub(crate) fn validate_past(
    db: &DatabaseConnection,
    header: &WriteHeader,
    sources: &WriteSources,
) -> Result<(), DbError> {
    for source in sources.changes() {
        if !coven_merge::had_read(
            header.position,
            header.read_through,
            source.write,
            crate::sequence::write_slot(db, source.write)?,
        ) {
            return Err(invalid("write source is outside authored past"));
        }
    }
    Ok(())
}
/// A copy names an original generation-zero creation of the same row in
/// another audience (§9.6). A part this device opened proves or refutes it;
/// a part it could not read leaves the authenticated claim as written.
fn creation(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowId,
    creator: &ChangeId,
) -> Result<(), DbError> {
    let opened: bool = db.query_row(
        "SELECT EXISTS(SELECT 1 FROM _coven_authored_parts WHERE write_id=?1 AND audience=?2)",
        params![
            merge_fields::encode_write_id(&creator.write)?,
            audience_text(&creator.row.audience)
        ],
        |r| r.get(0),
    )?;
    if !opened {
        return Ok(());
    }
    let Original {
        header: original,
        record,
    } = self::original(db, creator)?;
    let OriginalRow::Change(change) = record else {
        return Err(invalid("copied creation"));
    };
    let table = crate::source_names::table(
        db,
        original.schema_version,
        &creator.row.table,
        header.schema_version,
    )?;
    if change.change.generation != 0
        || !matches!(change.change.operation, Operation::Insert(_))
        || change.creator.is_some()
        || table.as_ref() != Some(&row.table)
        || original.timestamp >= header.timestamp
    {
        return Err(invalid("copied creation"));
    }
    Ok(())
}

/// The generation source and each replaced setter name changes at this row
/// that established that generation, or set that cell in this incarnation.
fn roles(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowId,
    generation: u64,
    incarnation: u64,
    sources: &WriteSources,
) -> Result<(), DbError> {
    let check = |source: &ChangeId| -> Result<Evidence, DbError> {
        if source.row.key != row.key || source.row.audience != row.audience {
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
            != Some(&row.table)
        {
            return Err(invalid("source row identity"));
        }
        Ok(evidence)
    };
    if let Some(source) = &sources.generation {
        let evidence = check(source)?;
        if matches!(evidence.original.change.operation, Operation::Update(_))
            || evidence.original.change.generation.checked_add(1) != Some(generation)
        {
            return Err(invalid("source generation advancement"));
        }
    }
    for (column, source) in &sources.setters {
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
                &row.table,
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
            != incarnation
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
    record: PartRecord<'_>,
) -> Result<(), DbError> {
    let row = record.row();
    part(db, header, &row.audience)?;
    let write = merge_fields::encode_write_id(&header.position)?;
    let audience = audience_text(&row.audience);
    let bytes = match record {
        PartRecord::Change(change) => change.encode()?,
        PartRecord::Execution(execution) => execution.encode()?,
    };
    let original: Option<Vec<u8>> = db.query_row("SELECT (SELECT record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4)",params![write,audience,row.table,row.key],|r|r.get(0))?;
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
    db.internal_execute("INSERT INTO _coven_authored_changes VALUES(?1,?2,?3,?4,?5,NULL) ON CONFLICT(write_id,audience,table_name,key) DO NOTHING",params![write,audience,row.table,row.key,bytes])?;
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
/// replacing an earlier one when a reload judges the original under its new data.
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

#[expect(
    clippy::map_err_ignore,
    reason = "a stored value that won't convert or decode is DamagedDatabase"
)]
fn read_verdict(stored: Option<Vec<u8>>) -> Result<ChangeVerdict, DbError> {
    stored.map_or(Ok(ChangeVerdict::Unjudged), |bytes| {
        merge_fields::decode_change_verdict(&bytes).map_err(|_| DbError::DamagedDatabase)
    })
}

/// Every original change of an audience whose containing slot is covered,
/// with its part header and this device's verdict, in D7 section 5 order.
pub(crate) fn visit_evidence<E: From<DbError>>(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
    covered: u64,
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
            if crate::sequence::write_slot(db, header.position)?.is_none_or(|slot| slot.0 > covered)
            {
                return Ok(());
            }
            let change = OriginalRow::decode(&r.get::<_, Vec<u8>>(2).map_err(DbError::from)?)
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
    let row = authored.change.row();
    part(db, header, &row.audience)?;
    retain(db, header, authored.change.record())?;
    db.internal_execute(
        "UPDATE _coven_authored_changes SET admission=?5 WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
        params![
            merge_fields::encode_write_id(&header.position)?,
            audience_text(&row.audience),
            row.table,
            row.key,
            stored_verdict(&authored.verdict)?
        ],
    )?;
    Ok(())
}

/// This device's migration write changes rows only through its own raise's
/// snapshot (§18.4). A loaded snapshot that covers it without carrying those
/// changes was made from another raise's data, in which they never counted:
/// their evidence goes with the data they described. A loaded snapshot at
/// the marker's version or later, covering it or not, comes from the raise
/// that established that version: the marker's own results are replaced and
/// it changes nothing anywhere.
pub(crate) fn forget_own_migration(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
    covered: u64,
    version: u32,
    carried: &std::collections::BTreeSet<(WriteId, String, Vec<u8>)>,
) -> Result<(), DbError> {
    let own = db.local_device().ok_or(DbError::Invariant {
        what: "a writer connection observes this device",
    })?;
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
            || (header.schema_version > version
                && crate::sequence::write_slot(db, header.position)?
                    .is_none_or(|slot| slot.0 > covered))
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
