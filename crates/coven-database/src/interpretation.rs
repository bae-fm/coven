//! The received interpretation of authenticated original changes: each
//! part's direct admission by the selected boundaries and the slot cut-off, then every admitted change's permission and data sources (§8,
//! §8.8, §10, §17.1, §19.3). Original bytes stay retained; only this
//! interpretation reaches the merge.
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::DbError;
use coven_format::{
    loss::LossCause,
    snapshot_rows::{ChangeVerdict, OriginalRow},
    write::{RowChange, WriteDisposition, WriteHeader, WritePart},
    write_sources::ChangeId,
};
use coven_merge::{Audience, ColumnValue, Operation, Provenance, RowId};
use std::collections::{BTreeMap, BTreeSet};

/// How a whole part is admitted before its changes are judged (§10, §17.1,
/// §19.3).
#[derive(Clone, Debug)]
enum PartAdmission {
    /// A preceding removal or key replacement refuses the batch.
    Refused,
    /// A kept reset ignores the part, dismissals included, with no loss.
    Ignored(coven_merge::Slot),
    /// A breaking change excluded it, and no converter reaches its version:
    /// each original change is a frozen schema loss.
    Excluded(u32),
    /// Its changes are judged, then converted to this version if given.
    Judged(Interpretation),
}

/// How admitted original columns reach the local schema.
#[derive(Clone, Debug)]
enum Interpretation {
    Native,
    Convert(u32),
    Captured,
}

/// One write's received interpretation: every opened change's verdict,
/// decided for the whole write before any part merges, since a foreign-key
/// action and its causes can be in different parts (§8, §8.8).
pub(crate) struct Interpreter {
    header: WriteHeader,
    parts: BTreeMap<Audience, PartAdmission>,
    verdicts: BTreeMap<RowId, ChangeVerdict>,
}

impl Interpreter {
    /// Judge every change of the write's parts in `audiences`, whose original
    /// records the caller has retained. Changes in other parts this device
    /// holds keep the verdicts already recorded for them.
    pub(crate) fn new(
        database: &DatabaseConnection,
        schema: &WriteSchema,
        header: &WriteHeader,
        audiences: &[Audience],
    ) -> Result<Self, DbError> {
        let mut interpreter = Self {
            header: header.clone(),
            parts: BTreeMap::new(),
            verdicts: BTreeMap::new(),
        };
        // A write after its cut-off is consumed without effects or losses (§10).
        if crate::sequence::refused(database, header.position)? {
            for audience in audiences {
                interpreter
                    .parts
                    .insert(audience.clone(), PartAdmission::Refused);
            }
            return Ok(interpreter);
        }
        let boundaries = crate::write_boundary::WriteBoundary::judging(database)?;
        for audience in audiences {
            // The part's direct admission before its changes' sources are followed.
            let captured = crate::captured_changes::includes(database, header)?;
            let admission = match crate::write_boundary::WriteBoundary::part_verdict(
                &boundaries,
                header,
                audience,
                crate::sequence::write_slot(database, header.position)?,
            ) {
                Some(ChangeVerdict::Ignored(entry)) => PartAdmission::Ignored(entry),
                _ if captured => PartAdmission::Judged(Interpretation::Captured),
                _ if header.disposition == WriteDisposition::Migration => {
                    PartAdmission::Judged(Interpretation::Native)
                }
                // A late old-schema part the winning raise's converters reach is a
                // candidate for admission at that version; without them it is excluded.
                Some(ChangeVerdict::Excluded(version)) => {
                    if crate::migration_receive::available(
                        &schema.conversions,
                        header.schema_version,
                        version,
                    ) {
                        PartAdmission::Judged(Interpretation::Convert(version))
                    } else {
                        PartAdmission::Excluded(version)
                    }
                }
                _ => PartAdmission::Judged(Interpretation::Native),
            };
            interpreter.parts.insert(audience.clone(), admission);
        }
        let judged: BTreeSet<&Audience> = interpreter
            .parts
            .iter()
            .filter(|(_, admission)| matches!(admission, PartAdmission::Judged(_)))
            .map(|(audience, _)| audience)
            .collect();
        // An admitted part's creations are offered before any check decides
        // about its changes (§8.8).
        let mut sources = BTreeMap::new();
        let mut fixed = BTreeMap::new();
        crate::judge::visit_write(database, header, |record| {
            let row = record.row().clone();
            match interpreter.parts.get(&row.audience) {
                Some(PartAdmission::Judged(_)) => {
                    if let OriginalRow::Change(change) = &record {
                        crate::ownership::offer_change(database, header, change)?;
                    }
                    sources.insert(row, provenance(&record));
                }
                Some(PartAdmission::Excluded(version)) => {
                    fixed.insert(row, ChangeVerdict::Excluded(*version));
                }
                Some(PartAdmission::Ignored(entry)) => {
                    fixed.insert(row, ChangeVerdict::Ignored(*entry));
                }
                Some(PartAdmission::Refused) => {
                    fixed.insert(row, ChangeVerdict::Refused);
                }
                None => {
                    let verdict = crate::judge::stored(database, header, &row)?;
                    fixed.insert(row, verdict);
                }
            }
            Ok(())
        })?;
        if judged.is_empty() {
            return Ok(interpreter);
        }
        let judgement = crate::judge::judge(database, schema, header)?;
        interpreter.verdicts = verdicts(database, header, &judgement, sources, &fixed)?;
        Ok(interpreter)
    }

    /// Admit one opened part's changes, or any slice of them in row order,
    /// under the write's verdicts: frozen losses for changes that do not
    /// apply, and the changes that do, converted where a raise requires it.
    pub(crate) fn part(
        &self,
        database: &DatabaseConnection,
        schema: &WriteSchema,
        mut part: WritePart,
    ) -> Result<Option<WritePart>, DbError> {
        let header = &self.header;
        let admission = self.parts.get(&part.audience).ok_or(DbError::Invariant {
            what: "an interpreted part was judged with its write",
        })?;
        if header.disposition == WriteDisposition::Migration
            && !matches!(admission, PartAdmission::Judged(Interpretation::Captured))
        {
            return Ok(None);
        }
        match admission {
            PartAdmission::Refused => {
                for row in part.records() {
                    crate::write_sources::admit(
                        database,
                        header,
                        row.row(),
                        &ChangeVerdict::Refused,
                    )?;
                }
                Ok(None)
            }
            PartAdmission::Ignored(entry) => {
                for row in part.records() {
                    crate::write_sources::admit(
                        database,
                        header,
                        row.row(),
                        &ChangeVerdict::Ignored(*entry),
                    )?;
                }
                Ok(None)
            }
            PartAdmission::Excluded(version) => {
                // Operations alone have no values to keep.
                for execution in &part.executions {
                    crate::write_sources::admit(
                        database,
                        header,
                        &execution.row,
                        &ChangeVerdict::Excluded(*version),
                    )?;
                }
                for change in &part.rows {
                    crate::write_sources::admit(
                        database,
                        header,
                        &change.row,
                        &ChangeVerdict::Excluded(*version),
                    )?;
                    freeze(
                        database,
                        schema,
                        header,
                        change,
                        LossCause::Excluded {
                            write: header.position,
                            version: *version,
                        },
                    )?;
                }
                Ok((!part.dismissals.is_empty()).then(|| WritePart {
                    rows: Vec::new(),
                    executions: Vec::new(),
                    ..part
                }))
            }
            PartAdmission::Judged(converts) => {
                for execution in &part.executions {
                    crate::write_sources::admit(
                        database,
                        header,
                        &execution.row,
                        self.verdict(&execution.row)?,
                    )?;
                }
                // Verdicts are decided on the original changes; a change that
                // does not apply stays frozen in its original shape and never
                // reaches a converter (E13).
                let mut applied = Vec::new();
                for change in std::mem::take(&mut part.rows) {
                    let verdict = self.verdict(&change.row)?;
                    crate::write_sources::admit(database, header, &change.row, verdict)?;
                    let cause = match verdict {
                        ChangeVerdict::NotAuthorized(reason) => LossCause::NotAuthorized {
                            write: header.position,
                            reason: reason.clone(),
                        },
                        ChangeVerdict::Dependent(sources) => LossCause::Dependency {
                            write: header.position,
                            sources: sources.clone(),
                        },
                        ChangeVerdict::Applied | ChangeVerdict::Derived(_) => {
                            applied.push(change);
                            continue;
                        }
                        _ => {
                            return Err(DbError::Invariant {
                                what: "a judged change is applied, dependent or unauthorized",
                            })
                        }
                    };
                    freeze(database, schema, header, &change, cause)?;
                }
                part.rows = applied;
                match converts {
                    Interpretation::Convert(version) => crate::migration_receive::convert(
                        &schema.conversions,
                        header.schema_version,
                        *version,
                        &mut part,
                    )?,
                    Interpretation::Captured => {
                        crate::captured_changes::project(database, schema, header, &mut part)?
                    }
                    Interpretation::Native => (),
                }
                if !matches!(converts, Interpretation::Captured) {
                    crate::download_checks::part(schema, &part)?;
                }
                Ok(Some(part))
            }
        }
    }

    fn verdict(&self, row: &RowId) -> Result<&ChangeVerdict, DbError> {
        self.verdicts.get(row).ok_or(DbError::Invariant {
            what: "every judged change was retained before its write was judged",
        })
    }
}

/// A change's data sources in role order (§8, Lean `Admission.dependencies`):
/// generation, replaced setters, then the further sources its operations used.
fn provenance(record: &OriginalRow) -> Provenance<ChangeId> {
    let (generation, setters) = match record {
        OriginalRow::Change(change) => (
            change.sources.generation.clone(),
            change
                .sources
                .setters
                .values()
                .map(|setter| setter.change.clone())
                .collect(),
        ),
        OriginalRow::Execution(_) => (None, Vec::new()),
    };
    Provenance {
        generation,
        setters,
        execution: record
            .record()
            .execution()
            .sources
            .changes()
            .cloned()
            .collect(),
        causes: Vec::new(),
    }
}

/// Every judged change's verdict (Lean `Admission.guard`): a failed
/// permission check first, then a fixed admission, then dependence on a
/// change that does not survive, followed within the write as a least
/// closure, so a cycle without a failed cause drops nothing.
fn verdicts(
    database: &DatabaseConnection,
    header: &WriteHeader,
    judgement: &crate::judge::Judgement,
    mut sources: BTreeMap<RowId, Provenance<ChangeId>>,
    fixed: &BTreeMap<RowId, ChangeVerdict>,
) -> Result<BTreeMap<RowId, ChangeVerdict>, DbError> {
    let checked = |row: &RowId| {
        judgement.rows.get(row).ok_or(DbError::Invariant {
            what: "every retained change of a write is judged",
        })
    };
    let mut retained = BTreeMap::new();
    let mut dropped: BTreeSet<RowId> = fixed
        .iter()
        .filter(|(_, verdict)| !verdict.retained())
        .map(|(row, _)| row.clone())
        .collect();
    for (row, provenance) in &mut sources {
        let checked = checked(row)?;
        provenance.causes = checked.causes(header.position);
        if checked.check.is_err() {
            dropped.insert(row.clone());
        }
        for source in provenance.sources() {
            if source.write != header.position && !retained.contains_key(source) {
                retained.insert(
                    source.clone(),
                    crate::write_sources::retained(database, source)?,
                );
            }
        }
    }
    let failed = |source: &ChangeId, dropped: &BTreeSet<RowId>| {
        if source.write == header.position {
            dropped.contains(&source.row)
        } else {
            !retained[source]
        }
    };
    loop {
        let mut grew = false;
        for (row, provenance) in &sources {
            if !dropped.contains(row) && provenance.sources().any(|s| failed(s, &dropped)) {
                dropped.insert(row.clone());
                grew = true;
            }
        }
        if !grew {
            break;
        }
    }
    let mut verdicts = BTreeMap::new();
    for (row, provenance) in sources {
        let checked = checked(&row)?;
        let verdict = match &checked.check {
            Err(reason) => ChangeVerdict::NotAuthorized(reason.clone()),
            Ok(()) if dropped.contains(&row) => ChangeVerdict::Dependent(
                provenance
                    .sources()
                    .filter(|s| failed(s, &dropped))
                    .cloned()
                    .collect(),
            ),
            Ok(()) if checked.derived => ChangeVerdict::Derived(checked.causes(header.position)),
            Ok(()) => ChangeVerdict::Applied,
        };
        verdicts.insert(row, verdict);
    }
    Ok(verdicts)
}

/// Keep one original row change as a frozen loss with its written values:
/// new values for an insert or update, old scalar values for a delete.
fn freeze(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    change: &RowChange,
    cause: LossCause,
) -> Result<(), DbError> {
    let mut values = match &change.change.operation {
        Operation::Insert(values) | Operation::Update(values) => values.clone(),
        Operation::Delete => change
            .old
            .iter()
            .map(|(name, value)| {
                (
                    name.clone(),
                    ColumnValue {
                        value: value.clone(),
                        parents: BTreeMap::new(),
                    },
                )
            })
            .collect(),
    };
    // Frozen display values have no live parent links (D5).
    for value in values.values_mut() {
        value.parents.clear();
    }
    let loss = coven_format::loss::Loss {
        row: change.row.clone(),
        generation: change.change.generation,
        frozen: true,
        values: coven_format::loss::LossValues::Row(
            values
                .into_iter()
                .map(|(name, value)| {
                    (
                        name,
                        coven_merge::Cell {
                            write: header.position,
                            value,
                        },
                    )
                })
                .collect(),
        ),
        cause,
    };
    crate::loss_record::put(database, &loss, None)?;
    crate::file_protection::retain_loss(database, schema, &loss)
}
