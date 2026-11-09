//! Original authored changes and their admission, independent of dismissible losses.

use crate::{
    merge_store::MergeStore, sqlite::DatabaseConnection, write_encoding::counter, DbError,
};
use coven_format::{
    merge_fields,
    snapshot_rows::{
        AuthoredChange, AuthoredPart, AuthoredRecord, ChangeVerdict, KeptChange, KeptRecord,
    },
    value::Value,
    write::{PartRecord, RowChange, WriteDisposition, WriteHeader},
    write_sources::{CellSource, ChangeId, WriteSources},
};
use coven_merge::{Change, Operation, RowId, WriteId};
use rusqlite::params;
use std::collections::BTreeMap;

pub(crate) struct Evidence {
    pub(crate) header: WriteHeader,
    pub(crate) original: KeptChange,
}

/// A kept record, a net change or an execution alone, and its part's header.
pub(crate) struct Original {
    pub(crate) header: WriteHeader,
    pub(crate) record: KeptRecord,
}

fn invalid(field: &'static str) -> DbError {
    coven_format::Error::Invalid { field, rule: coven_format::error::Rule::Coverage }.into()
}

/// What this device holds of a source a write names, as far as it decides
/// whether the source counts for that write (§9.5).
pub(crate) enum Standing<T> {
    /// Its record is held, and the source counts.
    Counts(T),
    /// It doesn't count: it stopped being current at or below the write's
    /// `read_through`, or its write landed while this device holds no record
    /// of it. Nothing more of it is read: a device that forgot the record
    /// couldn't, so one that holds it doesn't either.
    Dropped,
    /// This device holds no record of it and its write hasn't landed: the
    /// evidence is still unavailable.
    Unknown,
}

/// The standing of `source` for the write `header` heads, from `held`: its
/// record's stop slot and what the caller read beside it, when held.
///
/// A held source that stopped being current within the write's past counts
/// as dropped: the write claims to build on a value its author had already
/// seen replaced, which no genuine write does. One whose write landed but
/// whose record this device doesn't hold counts as dropped too: devices
/// forget records at different slots, and a holder drops the same source by
/// the first rule (§16.7). A migration write whose raise lost to another
/// device's is such a write: no history holds its changes (§18.8), the
/// migrating device's own marker among them while it still waits in its
/// queue.
fn standing<T>(
    db: &DatabaseConnection,
    header: &WriteHeader,
    source: &ChangeId,
    held: Option<(Option<u64>, T)>,
) -> Result<Standing<T>, DbError> {
    match held {
        Some((stop, _)) if stop.is_some_and(|stop| stop <= header.read_through) => {
            Ok(Standing::Dropped)
        }
        Some((_, held)) => Ok(Standing::Counts(held)),
        None if crate::write_metadata::landed_before(db, source.write, header.position)? => {
            Ok(Standing::Dropped)
        }
        None if crate::upload::queued(db, source.write)?
            .is_some_and(|frame| frame.header.disposition == WriteDisposition::Migration) =>
        {
            Ok(Standing::Dropped)
        }
        None => Ok(Standing::Unknown),
    }
}

/// The named original record and its part's header, for the write `header`
/// heads: never a current winner or an equal scalar.
pub(crate) fn source(
    db: &DatabaseConnection,
    header: &WriteHeader,
    source: &ChangeId,
) -> Result<Standing<Original>, DbError> {
    let write = merge_fields::encode_write_id(&source.write)?;
    let audience = &source.row.audience;
    let held = db
        .query(
            "SELECT stop,record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4",
            params![write, audience, source.row.table, source.row.key],
            |r| {
                Ok((
                    r.get::<_, Option<Vec<u8>>>(0)?.map(counter).transpose()?,
                    r.get::<_, Vec<u8>>(1)?,
                ))
            },
        )?
        .pop();
    Ok(match standing(db, header, source, held)? {
        Standing::Counts(record) => {
            let part = db.authored_part(&write, audience)?.ok_or(DbError::DamagedDatabase)?;
            Standing::Counts(Original {
                header: merge_fields::decode_write_header(&part)?,
                record: KeptRecord::decode(&record)?,
            })
        }
        Standing::Dropped => Standing::Dropped,
        Standing::Unknown => Standing::Unknown,
    })
}

/// The named original net change, for the write `header` heads, or none when
/// the source doesn't count; an unknown one waits. A row with operations but
/// no net change set no generation or cell, so it is never a source.
pub(crate) fn counting(
    db: &DatabaseConnection,
    header: &WriteHeader,
    named: &ChangeId,
) -> Result<Option<Evidence>, DbError> {
    match source(db, header, named)? {
        Standing::Counts(Original { header, record: KeptRecord::Change(original) }) => {
            Ok(Some(Evidence { header, original }))
        }
        Standing::Counts(Original { record: KeptRecord::Execution { .. }, .. }) => {
            Err(invalid("source has no net change"))
        }
        Standing::Dropped => Ok(None),
        Standing::Unknown => Err(DbError::MissingWriteSources(vec![named.clone()])),
    }
}

/// The part header of `write`'s change at `row`, which names its table as
/// it is now, with that table's name as the write named it, when this device
/// holds the part. Renames resolve through the recorded schema identities.
pub(crate) fn kept_part(
    db: &DatabaseConnection,
    write: WriteId,
    row: &RowId,
) -> Result<Option<(WriteHeader, String)>, DbError> {
    let Some(bytes) = db.authored_part(&merge_fields::encode_write_id(&write)?, &row.audience)?
    else {
        return Ok(None);
    };
    let header = merge_fields::decode_write_header(&bytes)?;
    let table =
        crate::source_names::table(db, db.schema_version()?, &row.table, header.schema_version)?
            .ok_or(DbError::DamagedDatabase)?;
    Ok(Some((header, table)))
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
        let Some((header, table)) = kept_part(self.db, write, &self.row)? else {
            return Err(DbError::MissingWriteSources(vec![ChangeId {
                write,
                row: self.row.clone(),
            }]));
        };
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

/// Capture the actual setters in the author's pre-write merge state. A
/// delete carries the cells it replaces, as their setters wrote them: what
/// receivers check (D5.6), never a displayed substitution such as a stale
/// reference's NULL. An insert or update carries no replaced value.
pub(crate) fn authored(
    db: &DatabaseConnection,
    store: &MergeStore<'_>,
    row: RowId,
    change: Change<Value>,
) -> Result<RowChange, DbError> {
    authored_over(db, store.row(&row)?.state, change)
}

/// [`authored`] over the pre-write merge state `state`, of its own row.
pub(crate) fn authored_over(
    db: &DatabaseConnection,
    state: coven_merge::RowState<Value>,
    change: Change<Value>,
) -> Result<RowChange, DbError> {
    let row = state.row().clone();
    let old = match change.operation {
        Operation::Delete => state
            .cells()
            .iter()
            .map(|(column, cell)| (column.clone(), cell.value.value.clone()))
            .collect(),
        Operation::Insert(_) | Operation::Update(_) => BTreeMap::new(),
    };
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
    if let PartRecord::Change(row) = record {
        validate_past(db, header, &row.sources)?;
        let incarnation = row.change.incarnation().map_err(coven_format::Error::Merge)?;
        roles(db, header, &row.row, row.change.generation, incarnation, &row.sources)?;
        if let Some(creator) = &row.creator {
            creation(db, header, &row.row, creator)?;
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
/// (§7.1). A landed write whose entry this device forgot was read: its
/// entry goes only once every write still to be judged has read past its
/// slot (§16.7).
pub(crate) fn had_read(
    db: &DatabaseConnection,
    header: &WriteHeader,
    earlier: coven_merge::WriteId,
) -> Result<bool, DbError> {
    let slot = match crate::write_metadata::stored_write(db, earlier)? {
        Some(stored) => stored.slot,
        None if crate::write_metadata::landed(db, earlier)? => return Ok(true),
        None => None,
    };
    Ok(coven_merge::had_read(header.position, header.read_through, earlier, slot))
}

/// Each record of `part` encoded, which validates it: the bytes a record is
/// retained and queued as, produced once for a write. A merge error is the
/// write's own invalid change.
pub(crate) fn encode_records(
    header: &WriteHeader,
    part: &coven_format::write::WritePart,
) -> Result<Vec<Vec<u8>>, DbError> {
    part.encode_records().map_err(|error| match error {
        coven_format::Error::Merge(error) => {
            DbError::InvalidWrite { write: header.position, error }
        }
        error => error.into(),
    })
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
/// another audience (§9.6, D5.7). A kept record of it proves or refutes it.
/// A creation that doesn't count as a source would, and one in a part this
/// device could not read, leave the authenticated claim as written.
fn creation(
    db: &DatabaseConnection,
    header: &WriteHeader,
    row: &RowId,
    creator: &ChangeId,
) -> Result<(), DbError> {
    let Standing::Counts(Original { header: original, record }) = source(db, header, creator)?
    else {
        return Ok(());
    };
    let KeptRecord::Change(change) = record else {
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
    // A source that doesn't count has no original to check its role
    // against: it was dropped, and the change built on it is lost (§9.5).
    let check = |source: &ChangeId| -> Result<Option<Evidence>, DbError> {
        if source.row.key != row.key || source.row.audience != row.audience {
            return Err(invalid("source row identity"));
        }
        let Some(evidence) = counting(db, header, source)? else {
            return Ok(None);
        };
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
    if let Some(original) = db.authored_part(&write, audience)? {
        if original != bytes {
            return Err(invalid("original source header changed"));
        }
    } else {
        db.internal_execute(
            "INSERT INTO _coven_authored_parts VALUES(?1,?2,?3)",
            params![write, audience, bytes],
        )?;
        db.authored_part_changed(&write, audience, Some(bytes));
    }
    Ok(())
}

/// Which columns of a write's rows were references under the schema the
/// write was made with: a kept record holds exactly those in full (D7.3).
pub(crate) struct KeptForm<'a> {
    schema: &'a crate::schema::Schema,
}

impl<'a> KeptForm<'a> {
    /// The kept form of the rows of a write at schema `version`.
    pub(crate) fn new(schema: &'a crate::write_schema::WriteSchema, version: u32) -> Self {
        Self { schema: schema.retained_before(version).unwrap_or(&schema.schema) }
    }

    /// Whether `column` of `table` was a reference under the write's schema.
    pub(crate) fn reference(&self, table: &str, column: &str) -> bool {
        self.schema
            .tables
            .get(&table.to_ascii_lowercase())
            .is_some_and(|table| table.foreign_keys_of(column).next().is_some())
    }

    /// The kept record of one of the write's rows, encoded.
    pub(crate) fn keep(&self, record: PartRecord<'_>) -> Result<Vec<u8>, DbError> {
        let table = &record.row().table;
        Ok(KeptRecord::keep(record, |column| self.reference(table, column))?.encode()?)
    }

    /// Whether a kept record holds in full exactly the columns that were
    /// references under the write's schema.
    pub(crate) fn conforms(&self, record: &KeptRecord) -> bool {
        let table = &record.row().table;
        record.conforms(|column| self.reference(table, column))
    }
}

/// Keep the record of a write this device just allocated the number of,
/// whose parts and changes nothing has kept yet: the guard `retain` reads to
/// compare a repeated change has nothing to compare, and a row repeated
/// within the write is refused by the table's key.
pub(crate) fn retain_authored(
    db: &DatabaseConnection,
    form: &KeptForm<'_>,
    header: &WriteHeader,
    record: PartRecord<'_>,
) -> Result<(), DbError> {
    let row = record.row();
    part(db, header, &row.audience)?;
    let write = merge_fields::encode_write_id(&header.position)?;
    db.internal_execute(
        "INSERT INTO _coven_authored_changes(write_id,audience,table_name,key,record) VALUES(?1,?2,?3,?4,?5)",
        params![write, &row.audience, row.table, row.key, form.keep(record)?],
    )?;
    Ok(())
}

/// Keep a received record: its kept form, which must equal any this device
/// already keeps for the same change, and the whole record in the slot's
/// staging, which judging the write and applying it read (D7.3). A reload
/// that stages a slot again keeps again a record this device forgot.
pub(crate) fn retain(
    db: &DatabaseConnection,
    form: &KeptForm<'_>,
    header: &WriteHeader,
    record: PartRecord<'_>,
) -> Result<(), DbError> {
    let row = record.row();
    part(db, header, &row.audience)?;
    let write = merge_fields::encode_write_id(&header.position)?;
    let audience = &row.audience;
    let kept = form.keep(record)?;
    let original: Option<Vec<u8>> = db.query_row("SELECT (SELECT record FROM _coven_authored_changes WHERE write_id=?1 AND audience=?2 AND table_name=?3 AND key=?4)",params![write,audience,row.table,row.key],|r|r.get(0))?;
    if original.as_ref().is_some_and(|original| *original != kept) {
        return Err(invalid("original source change changed"));
    }
    db.internal_execute("INSERT INTO _coven_authored_changes(write_id,audience,table_name,key,record) VALUES(?1,?2,?3,?4,?5) ON CONFLICT(write_id,audience,table_name,key) DO NOTHING",params![write,audience,row.table,row.key,kept])?;
    crate::download_stage::stage_record(db, header, record)
}

/// Install a snapshot's source records for `audience` from the copies its
/// install staged in `temp._coven_install_parts` and
/// `temp._coven_install_authored`, once they are checked against this
/// device's: an original the device holds stays, with the snapshot's stop
/// slot, and the snapshot's verdict replaces the device's, an unjudged one
/// too. A record the device holds and the snapshot doesn't stays until the
/// device forgets it (§16.7).
pub(crate) fn install(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
) -> Result<(), DbError> {
    db.internal_execute(
        "INSERT INTO _coven_authored_parts(write_id,audience,header)
         SELECT write_id,audience,header FROM temp._coven_install_parts s WHERE s.audience=?1
         AND NOT EXISTS(SELECT 1 FROM _coven_authored_parts p WHERE p.write_id=s.write_id AND p.audience=s.audience)",
        params![audience],
    )?;
    db.internal_execute(
        "INSERT INTO _coven_authored_changes(write_id,audience,table_name,key,record,stop)
         SELECT write_id,audience,table_name,key,record,stop FROM temp._coven_install_authored WHERE audience=?1
         ON CONFLICT(write_id,audience,table_name,key) DO UPDATE SET stop=excluded.stop",
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
    crate::forget_records::look_again(db)
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
    let original =
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

/// Whether an original change survives as data for the write `header`
/// heads: it counts as a source for it, and was applied here, or never
/// judged here because a loaded snapshot carries its effects.
pub(crate) fn retained(
    db: &DatabaseConnection,
    header: &WriteHeader,
    source: &ChangeId,
) -> Result<bool, DbError> {
    let held = db
        .query(
            "SELECT c.stop,a.admission FROM _coven_authored_changes c
             LEFT JOIN _coven_authored_admissions a ON a.write_id=c.write_id AND a.audience=c.audience AND a.table_name=c.table_name AND a.key=c.key
             WHERE c.write_id=?1 AND c.audience=?2 AND c.table_name=?3 AND c.key=?4",
            params![
                merge_fields::encode_write_id(&source.write)?,
                &source.row.audience,
                source.row.table,
                source.row.key
            ],
            |r| {
                Ok((
                    r.get::<_, Option<Vec<u8>>>(0)?.map(counter).transpose()?,
                    r.get::<_, Option<Vec<u8>>>(1)?,
                ))
            },
        )?
        .pop();
    match standing(db, header, source, held)? {
        Standing::Counts(admission) => Ok(read_verdict(admission)?.retained()),
        Standing::Dropped => Ok(false),
        Standing::Unknown => Err(DbError::MissingWriteSources(vec![source.clone()])),
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

/// Every kept change of an audience whose write landed, each part's header
/// before its changes, with this device's verdict on each and the slot at
/// which it stopped being current, in D7 section 4 order. A snapshot covers
/// the slots this device applied, so every landed write; this device's own
/// queued writes are left out.
pub(crate) fn visit_evidence<E: From<DbError>>(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
    mut visit: impl FnMut(AuthoredRecord) -> Result<(), E>,
) -> Result<(), E> {
    let mut part: Option<Vec<u8>> = None;
    db.for_each(
        "SELECT c.write_id,p.header,c.stop,c.record,a.admission FROM _coven_authored_changes c
         JOIN _coven_authored_parts p ON p.write_id=c.write_id AND p.audience=c.audience
         LEFT JOIN _coven_authored_admissions a ON a.write_id=c.write_id AND a.audience=c.audience AND a.table_name=c.table_name AND a.key=c.key
         WHERE c.audience=?1 ORDER BY c.write_id,c.table_name,c.key",
        [audience],
        |r| {
            let write: Vec<u8> = r.get(0).map_err(DbError::from)?;
            let header =
                merge_fields::decode_write_header(&r.get::<_, Vec<u8>>(1).map_err(DbError::from)?)
                    .map_err(DbError::from)?;
            if !crate::write_metadata::landed(db, header.position)? {
                return Ok(());
            }
            if part.as_ref() != Some(&write) {
                visit(AuthoredRecord::Part(AuthoredPart { header }))?;
                part = Some(write);
            }
            let record = KeptRecord::decode(&r.get::<_, Vec<u8>>(3).map_err(DbError::from)?)
                .map_err(DbError::from)?;
            let stop = r
                .get::<_, Option<Vec<u8>>>(2)
                .map_err(DbError::from)?
                .map(crate::write_encoding::slot_counter)
                .transpose()
                .map_err(DbError::from)?;
            visit(AuthoredRecord::Change(AuthoredChange {
                record,
                verdict: read_verdict(r.get(4).map_err(DbError::from)?)?,
                stop,
            }))
        },
    )
}

/// This device's migration write changes rows only through its own raise's
/// snapshot (§18.4). A loaded snapshot that covers it without carrying those
/// changes was made from another raise's data, in which they never counted:
/// their evidence goes with the data they described. A loaded snapshot at
/// the marker's version or later, covering it or not, comes from the raise
/// that established that version: the marker's own results are replaced and
/// it changes nothing anywhere. `covers` tells whether the loaded snapshot
/// covers a write: it lists its device's writes through its coverage by
/// number, whatever this device has forgotten of them (§16.7).
pub(crate) fn forget_own_migration(
    db: &DatabaseConnection,
    audience: &coven_merge::Audience,
    covers: impl Fn(WriteId) -> Result<bool, DbError>,
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
            || (header.schema_version > version && !covers(header.position)?)
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
                crate::migration_run::forget_record(db, &write_id, audience, &table, &key)?;
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
