//! The received interpretation of authenticated original changes: each
//! part's direct admission by the selected boundaries and its own retirement
//! window, then every admitted change's data sources (§8, §10, §17.1, §19.3).
//! Original bytes stay retained; only this interpretation reaches the merge.
use crate::sqlite::DatabaseConnection;
use crate::write_schema::WriteSchema;
use crate::{DbError, DownloadedPart, DownloadedWrite};
use coven_format::{
    loss::LossCause,
    snapshot_rows::ChangeVerdict,
    write::{RowChange, WriteDisposition, WriteHeader, WritePart, WriteRecord},
};
use coven_merge::{ColumnValue, Operation};
use std::collections::BTreeMap;

/// The part's direct admission before its changes' data sources are followed.
fn part_verdict(
    boundaries: &[crate::write_boundary::WriteBoundary],
    header: &WriteHeader,
    part: &WritePart,
) -> Option<ChangeVerdict> {
    let boundary = crate::write_boundary::WriteBoundary::part_verdict(boundaries, header, part);
    match (header.disposition, boundary) {
        (_, Some(ChangeVerdict::Ignored(entry))) => Some(ChangeVerdict::Ignored(entry)),
        (WriteDisposition::Lost(version), _) => Some(ChangeVerdict::Excluded(version)),
        (_, boundary) => boundary,
    }
}

pub(crate) fn accepted(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
) -> Result<WriteRecord, DbError> {
    crate::download::retain_sources(database, &download)?;
    interpret(database, schema, download)
}

pub(crate) fn interpret(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    download: DownloadedWrite,
) -> Result<WriteRecord, DbError> {
    let mut record = WriteRecord {
        header: download.header,
        parts: download
            .parts
            .into_iter()
            .filter_map(|part| match part {
                DownloadedPart::Opened(part) => Some(part),
                DownloadedPart::Skipped(_) => None,
            })
            .collect(),
    };
    // A write outside its own retirement window is consumed without effects
    // or losses; later readers never rescue it (§10).
    if crate::write_landing::late(database, record.header.position)? {
        for part in &record.parts {
            for row in &part.rows {
                crate::write_sources::admit(
                    database,
                    &record.header,
                    &row.row,
                    &ChangeVerdict::Dropped,
                )?;
            }
        }
        record.parts.clear();
        return Ok(record);
    }
    let boundaries = crate::write_boundary::WriteBoundary::load(database)?;
    let mut kept = Vec::new();
    for mut part in record.parts {
        if record.header.disposition == WriteDisposition::Migration {
            continue;
        }
        let originals: Vec<_> = part.rows.iter().map(|row| row.row.clone()).collect();
        let mut verdict = part_verdict(&boundaries, &record.header, &part);
        let mut converted = false;
        if let Some(ChangeVerdict::Excluded(version)) = verdict {
            if record.header.disposition == WriteDisposition::Apply
                && crate::migration_receive::available(
                    &schema.conversions,
                    record.header.schema_version,
                    version,
                )
            {
                crate::migration_receive::convert(
                    &schema.conversions,
                    record.header.schema_version,
                    version,
                    &mut part,
                )?;
                verdict = None;
                converted = true;
            }
        }
        match verdict {
            // A reset ignores the whole part, dismissals included, with no
            // loss; its position still passes (§19.3).
            Some(ChangeVerdict::Ignored(entry)) => {
                for row in &originals {
                    crate::write_sources::admit(
                        database,
                        &record.header,
                        row,
                        &ChangeVerdict::Ignored(entry),
                    )?;
                }
                continue;
            }
            // A schema change keeps each original change as a frozen loss (§17.1).
            Some(ChangeVerdict::Excluded(version)) => {
                for row in &originals {
                    crate::write_sources::admit(
                        database,
                        &record.header,
                        row,
                        &ChangeVerdict::Excluded(version),
                    )?;
                }
                for change in &part.rows {
                    freeze(
                        database,
                        schema,
                        &record.header,
                        change,
                        LossCause::Excluded {
                            write: record.header.position,
                            version,
                        },
                    )?;
                }
                if !part.dismissals.is_empty() {
                    kept.push(WritePart {
                        rows: Vec::new(),
                        ..part
                    });
                }
                continue;
            }
            _ => {}
        }
        if converted {
            for row in &originals {
                crate::write_sources::admit(
                    database,
                    &record.header,
                    row,
                    &ChangeVerdict::Applied,
                )?;
            }
        } else {
            part.rows = classify(database, schema, &record.header, part.rows)?;
        }
        crate::download_checks::part(schema, &part)?;
        kept.push(part);
    }
    record.parts = kept;
    Ok(record)
}

/// The shared data rule (§8): an admitted change whose generation source or
/// replaced setter does not survive becomes a frozen loss naming those
/// sources; its write's other changes still apply.
fn classify(
    database: &DatabaseConnection,
    schema: &WriteSchema,
    header: &WriteHeader,
    rows: Vec<RowChange>,
) -> Result<Vec<RowChange>, DbError> {
    let mut applied = Vec::new();
    for row in rows {
        let provenance = coven_merge::Provenance {
            generation: row.sources.generation.clone(),
            setters: row
                .sources
                .setters
                .values()
                .map(|setter| setter.change.clone())
                .collect(),
        };
        match coven_merge::classify(true, &provenance, |source| {
            crate::write_sources::retained(database, source)
        })? {
            coven_merge::DataVerdict::Dependent(sources) => {
                crate::write_sources::admit(
                    database,
                    header,
                    &row.row,
                    &ChangeVerdict::Dependent(sources.clone()),
                )?;
                freeze(
                    database,
                    schema,
                    header,
                    &row,
                    LossCause::Dependency {
                        write: header.position,
                        sources,
                    },
                )?;
            }
            _ => {
                crate::write_sources::admit(database, header, &row.row, &ChangeVerdict::Applied)?;
                applied.push(row);
            }
        }
    }
    Ok(applied)
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
        retired: true,
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

#[cfg(test)]
#[path = "interpretation_tests.rs"]
mod tests;
