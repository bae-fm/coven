//! Original authored changes and their admission, independent of dismissible losses.

use crate::{merge_store::MergeStore, sqlite::DatabaseConnection, DbError};
use coven_format::{
    merge_fields,
    snapshot_rows::{AuthoredChange, ChangeVerdict, OriginalRow},
    value::Value,
    write::{PartRecord, RowChange, WriteDisposition, WriteHeader},
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
    coven_format::Error::Invalid { field, rule: coven_format::error::Rule::Coverage }.into()
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
    let audience = &source.row.audience;
    let Some((header, complete)) = db.authored_part(&write, audience)? else {
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

/// Whether `source` names a change of a migration write that no history
/// holds. A migration write's changes reach other devices only in its own
/// raise's snapshot (§18.4), and a write built on them goes out only after a
/// raise to its version is established. So when a device judges that write
/// without holding the migration write's part, another device's raise was
/// established instead: the migrating device forgot those changes on
/// loading it, and no other device ever had them. Such a source was dropped
/// (§9.5), so the change built on it is lost on every device instead of
/// waiting forever.
///
/// A device that loaded a snapshot covering the migration write's slot has
/// neither its part nor its header: the snapshot carries every change of a
/// covered write its author held, and no device held these. That write is
/// known only as a covered one, by its slot. Any write with a part holding
/// the change would have brought it along, so its absence is the proof.
pub(crate) fn uncarried(db: &DatabaseConnection, source: &ChangeId) -> Result<bool, DbError> {
    let write = merge_fields::encode_write_id(&source.write)?;
    if db.authored_part(&write, &source.row.audience)?.is_some() {
        return Ok(false);
    }
    // A received marker keeps its header; the migrating device's own marker
    // may still wait in its queue.
    let header = match crate::write_headers::read(db, source.write)? {
        Some(frame) => Some(frame.header),
        None => crate::upload::queued(db, source.write)?.map(|frame| frame.header),
    };
    match header {
        Some(header) => Ok(header.disposition == WriteDisposition::Migration),
        None => Ok(crate::sequence::write_slot(db, source.write)?.is_some()),
    }
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
        Self::over(db, store.row(&row)?.state)
    }

    /// The pre-write merge state `state`, of its own row.
    fn over(
        db: &'a DatabaseConnection,
        state: coven_merge::RowState<Value>,
    ) -> Result<Self, DbError> {
        Ok(Self { row: state.row().clone(), state, version: db.schema_version()?, db })
    }

    fn source(&self, write: WriteId) -> Result<(ChangeId, u32), DbError> {
        let part =
            self.db.authored_part(&merge_fields::encode_write_id(&write)?, &self.row.audience)?;
        let Some((bytes, _)) = part else {
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
        Ok((ChangeId { write, row: RowId { table, ..self.row.clone() } }, header.schema_version))
    }

    /// The change that established `generation`, absent at generation zero.
    fn generation(&self, generation: u64) -> Result<Option<ChangeId>, DbError> {
        if generation == 0 {
            return Ok(None);
        }
        let write = *self.state.generations().get(&generation).ok_or(DbError::DamagedDatabase)?;
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
        Ok(Some(CellSource { change, column: original }))
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
    authored_over(db, store.row(&row)?.state, change, old)
}

/// [`authored`] over the pre-write merge state `state`, of its own row.
pub(crate) fn authored_over(
    db: &DatabaseConnection,
    state: coven_merge::RowState<Value>,
    change: Change<Value>,
    old: BTreeMap<String, Value>,
) -> Result<RowChange, DbError> {
    let row = state.row().clone();
    let pre = PreWrite::over(db, state)?;
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
        sources: WriteSources { generation: pre.generation(change.generation)?, setters },
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
    Ok(coven_format::write::Execution { steps, sources: WriteSources { generation, setters } })
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
        coven_format::Error::Merge(error) => {
            DbError::InvalidWrite { write: header.position, error }
        }
        error => error.into(),
    };
    match record {
        PartRecord::Change(row) => {
            row.encode().map_err(merge)?;
            validate_past(db, header, &row.sources)?;
            let incarnation = row.change.incarnation().map_err(coven_format::Error::Merge)?;
            roles(db, header, &row.row, row.change.generation, incarnation, &row.sources)?;
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
        let incarnation = first.change.incarnation().map_err(coven_format::Error::Merge)?;
        roles(db, header, record.row(), first.change.generation, incarnation, &execution.sources)?;
    }
    Ok(())
}

/// Whether `earlier` is in the authored past of the write `header` heads:
/// the slots through its `read_through`, and its device's earlier writes
/// (§7.1).
pub(crate) fn had_read(
    db: &DatabaseConnection,
    header: &WriteHeader,
    earlier: coven_merge::WriteId,
) -> Result<bool, DbError> {
    Ok(coven_merge::had_read(
        header.position,
        header.read_through,
        earlier,
        crate::sequence::write_slot(db, earlier)?,
    ))
}

pub(crate) fn validate_past(
    db: &DatabaseConnection,
    header: &WriteHeader,
    sources: &WriteSources,
) -> Result<(), DbError> {
    for source in sources.changes() {
        if !had_read(db, header, source.write)? {
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
        params![merge_fields::encode_write_id(&creator.write)?, &creator.row.audience],
        |r| r.get(0),
    )?;
    if !opened {
        return Ok(());
    }
    let Original { header: original, record } = self::original(db, creator)?;
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
    // A source no history holds has no original to check its role against:
    // it was dropped, and the change built on it is lost (§9.5).
    let check = |source: &ChangeId| -> Result<Option<Evidence>, DbError> {
        if source.row.key != row.key || source.row.audience != row.audience {
            return Err(invalid("source row identity"));
        }
        if uncarried(db, source)? {
            return Ok(None);
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
        Ok(Some(evidence))
    };
    if let Some(evidence) = sources.generation.as_ref().map(check).transpose()?.flatten() {
        if matches!(evidence.original.change.operation, Operation::Update(_))
            || evidence.original.change.generation.checked_add(1) != Some(generation)
        {
            return Err(invalid("source generation advancement"));
        }
    }
    for (column, source) in &sources.setters {
        let Some(evidence) = check(&source.change)? else {
            continue;
        };
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
        if evidence.original.change.incarnation().map_err(coven_format::Error::Merge)?
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
    let bytes = merge_fields::encode_write_header(header)?;
    if let Some((original, _)) = db.authored_part(&write, audience)? {
        if original != bytes {
            return Err(invalid("original source header changed"));
        }
    } else {
        db.internal_execute(
            "INSERT INTO _coven_authored_parts VALUES(?1,?2,?3,0)",
            params![write, audience, bytes],
        )?;
        db.authored_part_changed(&write, audience, Some((bytes, false)));
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
    let audience = &row.audience;
    let bytes = match record {
        PartRecord::Change(change) => change.encode()?,
        PartRecord::Execution(execution) => execution.encode()?,
    };
    let original: Option<Vec<u8>> = db.query_row("SELECT (SELECT record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4)",params![write,audience,row.table,row.key],|r|r.get(0))?;
    let complete =
        db.authored_part(&write, audience)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?.1;
    if original.is_none() && complete {
        return Err(invalid("original source part changed"));
    }
    if original.as_ref().is_some_and(|original| *original != bytes) {
        return Err(invalid("original source change changed"));
    }
    db.internal_execute("INSERT INTO _coven_authored_changes VALUES(?1,?2,?3,?4,?5) ON CONFLICT(write_id,audience,table_name,key) DO NOTHING",params![write,audience,row.table,row.key,bytes])?;
    Ok(())
}

pub(crate) fn complete(
    db: &DatabaseConnection,
    header: &WriteHeader,
    audience: &coven_merge::Audience,
) -> Result<(), DbError> {
    part(db, header, audience)?;
    let write = merge_fields::encode_write_id(&header.position)?;
    db.internal_execute("UPDATE _coven_authored_parts SET complete=1 WHERE write_id=?1 AND audience=?2 AND complete=0",params![write,audience])?;
    db.authored_part_changed(
        &write,
        audience,
        Some((merge_fields::encode_write_header(header)?, true)),
    );
    Ok(())
}

/// Install a snapshot's source records for `audience` from the copies its
/// install staged in `temp._coven_install_parts` and
/// `temp._coven_install_authored`, once they are checked against this
/// device's: a part the device lacks starts incomplete, an original the
/// device holds stays, and the snapshot's verdict replaces the device's, an
/// unjudged one too.
pub(crate) fn install(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_authored_parts(write_id,audience,header,complete)
         SELECT write_id,audience,header,0 FROM temp._coven_install_parts s WHERE s.audience=?1
         AND NOT EXISTS(SELECT 1 FROM _coven_authored_parts p WHERE p.write_id=s.write_id AND p.audience=s.audience)",
        params![audience],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_authored_changes(write_id,audience,table_name,key,record)
         SELECT write_id,audience,table_name,key,record FROM temp._coven_install_authored WHERE audience=?1
         ON CONFLICT(write_id,audience,table_name,key) DO NOTHING",
        params![audience],
    )?;
    db.internal_execute(
        "DELETE FROM _coven_authored_admissions WHERE (write_id,audience,table_name,key) IN
         (SELECT write_id,audience,table_name,key FROM temp._coven_install_authored WHERE audience=?1 AND admission IS NULL)",
        params![audience],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_authored_admissions(write_id,audience,table_name,key,admission)
         SELECT write_id,audience,table_name,key,admission FROM temp._coven_install_authored WHERE audience=?1 AND admission IS NOT NULL
         ON CONFLICT(write_id,audience,table_name,key) DO UPDATE SET admission=excluded.admission",
        params![audience],
    )?;
    Ok(())
}

/// Mark the parts the installed snapshot holds whole, once all their changes
/// are in.
pub(crate) fn complete_installed(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
) -> Result<(), DbError> {
    db.internal_execute(
        "UPDATE _coven_authored_parts SET complete=1 WHERE complete=0 AND (write_id,audience) IN (
           SELECT write_id,audience FROM temp._coven_install_parts WHERE audience=?1 AND complete=1)",
        params![audience],
    )?;
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
    let audience = &row.audience;
    let (original, _) =
        db.authored_part(&write, audience)?.ok_or(rusqlite::Error::QueryReturnedNoRows)?;
    let original = merge_fields::decode_write_header(&original)?;
    let table =
        crate::source_names::table(db, header.schema_version, &row.table, original.schema_version)?
            .ok_or(DbError::DamagedDatabase)?;
    match stored_verdict(verdict)? {
        Some(admission) => db.internal_execute(
            "INSERT INTO _coven_authored_admissions VALUES(?1,?2,?3,?4,?5)
             ON CONFLICT(write_id,audience,table_name,key) DO UPDATE SET admission=excluded.admission",
            params![write, audience, table, row.key, admission],
        )?,
        None => db.internal_execute(
            "DELETE FROM _coven_authored_admissions WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
            params![write, audience, table, row.key],
        )?,
    };
    Ok(())
}

/// Whether an original change survives as data for later changes: applied
/// here, or never judged here because a loaded snapshot carries its effects.
pub(crate) fn retained(db: &DatabaseConnection, source: &ChangeId) -> Result<bool, DbError> {
    let admission: Option<Option<Vec<u8>>> = db
        .query(
            "SELECT a.admission FROM _coven_authored_changes c
             LEFT JOIN _coven_authored_admissions a ON a.write_id=c.write_id AND a.audience=c.audience AND a.table_name=c.table_name AND a.key=c.key
             WHERE c.write_id=?1 AND c.audience=?2 AND c.table_name=?3 AND c.key=?4",
            params![
                merge_fields::encode_write_id(&source.write)?,
                &source.row.audience,
                source.row.table,
                source.row.key
            ],
            |r| r.get(0),
        )?
        .pop();
    match admission {
        None if uncarried(db, source)? => Ok(false),
        None => Err(DbError::MissingWriteSources(vec![source.clone()])),
        Some(admission) => Ok(read_verdict(admission)?.retained()),
    }
}

/// None stands for a change never judged here, which keeps no admission row.
pub(crate) fn stored_verdict(verdict: &ChangeVerdict) -> Result<Option<Vec<u8>>, DbError> {
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
        "SELECT p.header,p.complete,c.record,a.admission FROM _coven_authored_changes c
         JOIN _coven_authored_parts p ON p.write_id=c.write_id AND p.audience=c.audience
         LEFT JOIN _coven_authored_admissions a ON a.write_id=c.write_id AND a.audience=c.audience AND a.table_name=c.table_name AND a.key=c.key
         WHERE c.audience=?1 ORDER BY c.write_id,c.table_name,c.key",
        [audience],
        |r| {
            let header =
                merge_fields::decode_write_header(&r.get::<_, Vec<u8>>(0).map_err(DbError::from)?)
                    .map_err(DbError::from)?;
            if crate::sequence::write_slot(db, header.position)?.is_none_or(|slot| slot.get() > covered)
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
    carried: impl Fn(&[u8], &coven_merge::Audience, &str, &[u8]) -> Result<bool, DbError>,
) -> Result<(), DbError> {
    let own = db
        .local_device()
        .ok_or(DbError::Invariant { what: "a writer connection observes this device" })?;
    let first = merge_fields::encode_write_id(&WriteId { device: own, number: 1 })?;
    let last = merge_fields::encode_write_id(&WriteId { device: own, number: u64::MAX })?;
    for (write_id, header) in db.query(
        "SELECT write_id,header FROM _coven_authored_parts WHERE write_id BETWEEN ?2 AND ?3 AND audience=?1",
        params![audience, first, last],
        |r| Ok((r.get::<_, Vec<u8>>(0)?, r.get::<_, Vec<u8>>(1)?)),
    )? {
        let header = merge_fields::decode_write_header(&header)?;
        if header.disposition != coven_format::write::WriteDisposition::Migration
            || (header.schema_version > version
                && crate::sequence::write_slot(db, header.position)?
                    .is_none_or(|slot| slot.get() > covered))
        {
            continue;
        }
        for (table, key) in db.query(
            "SELECT table_name,key FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2",
            params![write_id, audience],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, Vec<u8>>(1)?)),
        )? {
            if !carried(&write_id, audience, &table, &key)? {
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
        db.forget_authored_part(&write_id, audience);
    }
    Ok(())
}

#[cfg(test)]
#[path = "write_sources_tests.rs"]
pub(crate) mod tests;
